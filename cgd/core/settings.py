from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings.

    Required:
      - DATABASE_URL

    Optional:
      - DB_SCHEMA: used for prefixing table names in raw SQL: "{schema}.{table}"
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    db_schema: Optional[str] = None

    # Back-compat toggle for CGI-style dispatch endpoint
    allow_search_dispatch: bool = True

    # API prefix (kept constant for reverse-proxy routing)
    api_prefix: str = "/api"


settings = Settings()
