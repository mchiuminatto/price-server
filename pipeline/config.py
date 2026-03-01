"""
Pipeline configuration loaded from environment variables / .env file.
"""

from typing import Literal

from pydantic_settings import BaseSettings


class PipelineSettings(BaseSettings):
    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Storage
    storage_backend: Literal["local", "s3"] = "local"
    input_base_path: str = "/data/raw"
    output_base_path: str = "/data/processed"

    # AWS (only required when storage_backend = "s3")
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    aws_session_token: str = ""
    aws_region: str = "us-east-1"

    # Worker concurrency
    worker_count: int = 2

    model_config = {"env_file": ".env", "env_prefix": "PIPELINE_"}


settings = PipelineSettings()
