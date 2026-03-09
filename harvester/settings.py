from typing import Literal
from pydantic_settings import BaseSettings
from pydantic import AnyHttpUrl
import os

Environment = Literal["production", "staging", "dev", "local"]


class BaseAppSettings(BaseSettings):
    ENVIRONMENT: Environment = "dev"
    WAREHOUSE_API_URL: AnyHttpUrl
    FINBIF_ACCESS_TOKEN: str

    LOG_DIR: str = "./logs"
    LOG_LEVEL: str = "INFO"
    WAREHOUSE_API_TIMEOUT: int = 30

    class Config:
        env_file = ".env"
        case_sensitive = True
        url_preserve_empty_path = True

class ProductionSettings(BaseAppSettings):
    """Production settings"""
    pass


class StagingSettings(BaseAppSettings):
    WAREHOUSE_API_URL: AnyHttpUrl = "http://192.168.10.6:8080"


class DevSettings(BaseAppSettings):
    WAREHOUSE_API_URL: AnyHttpUrl = "http://localhost:8080"


class LocalSettings(BaseAppSettings):
    pass # set in .env


def get_settings() -> BaseAppSettings:
    env: Environment = os.getenv("ENVIRONMENT", "dev")

    if env == "production":
        return ProductionSettings()
    elif env == "staging":
        return StagingSettings()
    elif env == "local":
        return LocalSettings()
    else:
        return DevSettings()


settings = get_settings()