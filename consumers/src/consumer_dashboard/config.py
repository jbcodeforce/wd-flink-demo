"""Environment-driven settings for Kafka and Schema Registry."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from uuid import uuid4

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    kafka_bootstrap_servers: str = Field(default="", validation_alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_sasl_username: str = Field(default="", validation_alias="KAFKA_SASL_USERNAME")
    kafka_sasl_password: str = Field(default="", validation_alias="KAFKA_SASL_PASSWORD")
    kafka_security_protocol: str = Field(default="SASL_SSL", validation_alias="KAFKA_SECURITY_PROTOCOL")
    kafka_sasl_mechanism: str = Field(default="PLAIN", validation_alias="KAFKA_SASL_MECHANISM")
    kafka_topic: str = Field(default="fct_nb_act_per_pgm", validation_alias="KAFKA_TOPIC")
    kafka_group_id: str = Field(default="wd-fct-nb-act-dashboard", validation_alias="KAFKA_GROUP_ID")
    kafka_auto_offset_reset: str = Field(default="earliest", validation_alias="KAFKA_AUTO_OFFSET_RESET")

    schema_registry_url: str = Field(default="", validation_alias="SCHEMA_REGISTRY_URL")
    schema_registry_basic_auth_user_info: str = Field(
        default="",
        validation_alias="SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO",
    )

    def consumer_enabled(self) -> bool:
        return bool(
            self.kafka_bootstrap_servers.strip()
            and self.schema_registry_url.strip()
            and self.kafka_sasl_username.strip()
            and self.kafka_sasl_password.strip()
        )


def get_settings() -> Settings:
    s = Settings()
    s.kafka_group_id = s.kafka_group_id + "-" + uuid4().hex[:5]
    return s
