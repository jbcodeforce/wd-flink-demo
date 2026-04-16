"""Background DeserializingConsumer loop for Avro key/value on Confluent Cloud."""

from __future__ import annotations

import logging
import threading

from confluent_kafka import DeserializingConsumer, KafkaException
from confluent_kafka.error import KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from consumer_dashboard.config import Settings
from consumer_dashboard.state import MetricsStore

logger = logging.getLogger(__name__)


def _schema_registry_client(settings: Settings) -> SchemaRegistryClient:
    """Create the Schema Registry client used by Avro deserializers (URL and optional basic auth)."""
    conf: dict[str, str] = {"url": settings.schema_registry_url.strip()}
    if settings.schema_registry_basic_auth_user_info.strip():
        conf["basic.auth.user.info"] = settings.schema_registry_basic_auth_user_info.strip()
    return SchemaRegistryClient(conf)


def consumer_loop(settings: Settings, store: MetricsStore, stop: threading.Event) -> None:
    """
    Run the Kafka poll loop: subscribe, deserialize Avro key/value, and update ``store`` until ``stop`` is set.
    The consumer  deserializes fct_nb_act_per_pgm rows into MetricsStore._rows; each row has window_start, program_name, and nb_activities.


    """
    sr = _schema_registry_client(settings)
    key_deserializer = AvroDeserializer(sr)
    value_deserializer = AvroDeserializer(sr)

    conf: dict = {
        "bootstrap.servers": settings.kafka_bootstrap_servers.strip(),
        "group.id": settings.kafka_group_id,
        "security.protocol": settings.kafka_security_protocol,
        "sasl.mechanisms": settings.kafka_sasl_mechanism,
        "sasl.username": settings.kafka_sasl_username,
        "sasl.password": settings.kafka_sasl_password,
        "auto.offset.reset": settings.kafka_auto_offset_reset,
        "enable.auto.commit": True,
        "key.deserializer": key_deserializer,
        "value.deserializer": value_deserializer,
    }
    logger.info(f"consumer conf: {conf}")
    consumer = DeserializingConsumer(conf)
    consumer.subscribe([settings.kafka_topic])
    store.set_consumer_running(True)
    logger.info("Subscribed to topic %s group=%s", settings.kafka_topic, settings.kafka_group_id)

    try:
        while not stop.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                err = msg.error()
                store.record_error(str(err))
                logger.warning("Consumer error: %s", err)
                continue
            try:
                logger.info(f"key: {msg.key()}, value: {msg.value()}")
                store.apply_message(msg.key(), msg.value())
            except Exception as e:
                store.record_error(str(e))
                logger.exception("Failed to apply message")
    except KafkaException as e:
        store.record_error(str(e))
        logger.exception("KafkaException in consumer loop")
    except Exception as e:
        store.record_error(str(e))
        logger.exception("Unexpected error in consumer loop")
    finally:
        store.set_consumer_running(False)
        try:
            consumer.close()
        except Exception:
            logger.exception("Error closing consumer")
        logger.info("Consumer closed")


def start_consumer_thread(settings: Settings, store: MetricsStore, stop: threading.Event) -> threading.Thread:
    """Start ``consumer_loop`` on a daemon thread so the dashboard process can exit with the main app."""
    t = threading.Thread(
        target=consumer_loop,
        args=(settings, store, stop),
        name="kafka-consumer",
        daemon=True,
    )
    t.start()
    return t
