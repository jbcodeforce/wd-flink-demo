"""Kafka producer for raw_activities with Avro serialization."""

import logging
from datetime import datetime, timezone
from typing import Dict, Optional

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient, topic_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from activity_producer.config import ProducerConfig

logger = logging.getLogger(__name__)


# Avro schemas for raw_activities (must match Flink table DDL)
KEY_SCHEMA = """
{
  "type": "record",
  "name": "Key",
  "namespace": "raw_activities",
  "fields": [
    {"name": "activity_id", "type": "int"}
  ]
}
"""

VALUE_SCHEMA = """
{
  "type": "record",
  "name": "Value",
  "namespace": "raw_activities",
  "fields": [
    {"name": "activity_id", "type": "int"},
    {"name": "lead_id", "type": "int"},
    {"name": "activity_type", "type": "string"},
    {"name": "primary_attribute_value", "type": "string"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "metadata", "type": {"type": "map", "values": "string"}}
  ]
}
"""


class ActivityProducer:
    """Producer for raw_activities topic."""

    def __init__(self, config: ProducerConfig):
        self.config = config
        self.topic = config.topic

        # Schema Registry client
        sr_config = {
            "url": config.schema_registry_url,
            "basic.auth.user.info": config.schema_registry_auth,
        }
        schema_registry = SchemaRegistryClient(sr_config)

        # Avro serializers with .flink-dev context (matching Flink table DDL)
        key_serializer = AvroSerializer(
            schema_registry_client=schema_registry,
            schema_str=KEY_SCHEMA,
            conf={"auto.register.schemas": True, "subject.name.strategy": topic_subject_name_strategy},
        )

        value_serializer = AvroSerializer(
            schema_registry_client=schema_registry,
            schema_str=VALUE_SCHEMA,
            conf={"auto.register.schemas": True, "subject.name.strategy": topic_subject_name_strategy},
        )

        # Kafka producer config
        producer_config = {
            "bootstrap.servers": config.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": config.sasl_username,
            "sasl.password": config.sasl_password,
            "key.serializer": key_serializer,
            "value.serializer": value_serializer,
            "compression.type": "snappy",
            "acks": "all",
            "retries": 3,
        }

        self.producer = SerializingProducer(producer_config)
        logger.info(f"Producer initialized for topic: {self.topic}")

    def produce_activity(
        self,
        activity_id: int,
        lead_id: int,
        activity_type: str,
        primary_attribute_value: str,
        timestamp: Optional[datetime] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Produce a single activity record.

        Args:
            activity_id: Unique activity identifier
            lead_id: Lead identifier
            activity_type: Type of activity (e.g., "Open Email", "Fill Out Form")
            primary_attribute_value: Main attribute value (e.g., campaign name, form URL)
            timestamp: Activity timestamp (defaults to now)
            metadata: Additional metadata as string-to-string map
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        if metadata is None:
            metadata = {}

        # Convert datetime to milliseconds since epoch for Avro timestamp-millis
        timestamp_ms = int(timestamp.timestamp() * 1000)

        key = {"activity_id": activity_id}

        value = {
            "activity_id": activity_id,
            "lead_id": lead_id,
            "activity_type": activity_type,
            "primary_attribute_value": primary_attribute_value,
            "timestamp": timestamp_ms,
            "metadata": metadata,
        }

        def delivery_callback(err, msg):
            if err:
                logger.error(f"Delivery failed for activity {activity_id}: {err}")
            else:
                logger.info(
                    f"Activity {activity_id} delivered to {msg.topic()} "
                    f"[{msg.partition()}] @ offset {msg.offset()}"
                )

        self.producer.produce(
            topic=self.topic,
            key=key,
            value=value,
            on_delivery=delivery_callback,
        )

        # Trigger callbacks
        self.producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        """
        Flush pending messages.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            Number of messages still in queue
        """
        logger.info("Flushing producer...")
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            logger.warning(f"{remaining} messages still in queue after flush")
        else:
            logger.info("All messages delivered")
        return remaining

    def close(self):
        """Close the producer."""
        self.flush()
        logger.info("Producer closed")
