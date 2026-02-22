from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+asyncpg://postgres:password@localhost:5432/priceserver"
    DEBUG: bool = False
    APP_NAME: str = "Price Server"
    APP_VERSION: str = "0.1.0"
    ASYNC_EXPORT_DIR: str = "/tmp/exports"
    MAX_UPLOAD_SIZE_MB: int = 100
    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
