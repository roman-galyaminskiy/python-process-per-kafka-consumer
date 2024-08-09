from datetime import timedelta
from typing import Type, Tuple

from pydantic import BaseModel
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
    PydanticBaseSettingsSource,
    JsonConfigSettingsSource,
)


class KafkaBootstrapConfig(BaseModel):
    servers: str


class KafkaGroupConfig(BaseModel):
    id: str


class KafkaPollConfig(BaseModel):
    timeout: timedelta


class KafkaIncomingConfig(BaseModel):
    poll: KafkaPollConfig
    topic: str
    group: KafkaGroupConfig
    partitions: int


class KafkaBindingConfig(BaseModel):
    bootstrap: KafkaBootstrapConfig
    incoming: KafkaIncomingConfig


class BindingConfig(BaseModel):
    kafka: KafkaBindingConfig


class Config(BaseSettings):
    binding: BindingConfig

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        json_file="config.json",
        env_nested_delimiter="__",
        json_file_encoding="utf-8",
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
            cls,
            settings_cls: Type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            JsonConfigSettingsSource(settings_cls),
            file_secret_settings,
        )
