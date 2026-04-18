"""Configuration management for activity producer."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class ProducerConfig:
    """Configuration for Kafka producer."""

    bootstrap_servers: str
    sasl_username: str
    sasl_password: str
    schema_registry_url: str
    schema_registry_auth: str
    topic: str = "raw_activities"

    @classmethod
    def from_env(cls) -> "ProducerConfig":
        """Load configuration from environment variables."""
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        username = os.getenv("KAFKA_SASL_USERNAME")
        password = os.getenv("KAFKA_SASL_PASSWORD")
        sr_url = os.getenv("SCHEMA_REGISTRY_URL")
        sr_auth = os.getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO")
        topic = os.getenv("KAFKA_TOPIC", "raw_activities")

        missing = []
        if not bootstrap:
            missing.append("KAFKA_BOOTSTRAP_SERVERS")
        if not username:
            missing.append("KAFKA_SASL_USERNAME")
        if not password:
            missing.append("KAFKA_SASL_PASSWORD")
        if not sr_url:
            missing.append("SCHEMA_REGISTRY_URL")
        if not sr_auth:
            missing.append("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO")

        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

        return cls(
            bootstrap_servers=bootstrap,
            sasl_username=username,
            sasl_password=password,
            schema_registry_url=sr_url,
            schema_registry_auth=sr_auth,
            topic=topic,
        )

    def is_complete(self) -> bool:
        """Check if all required configuration is present."""
        return all(
            [
                self.bootstrap_servers,
                self.sasl_username,
                self.sasl_password,
                self.schema_registry_url,
                self.schema_registry_auth,
            ]
        )
