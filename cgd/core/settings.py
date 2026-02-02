from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings.

    Required:
      - DATABASE_URL

    Optional:
      - DB_SCHEMA: used for prefixing table names in raw SQL: "{schema}.{table}"
      - CGD_DATA_DIR: path to CGD data files (homology alignments, trees, etc.)
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    db_schema: Optional[str] = None

    # Path to CGD data files (default matches typical production setup)
    cgd_data_dir: str = "/data"

    # Back-compat toggle for CGI-style dispatch endpoint
    allow_search_dispatch: bool = True

    # API prefix (kept constant for reverse-proxy routing)
    api_prefix: str = "/api"

    # Elasticsearch configuration
    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_index: str = "cgd"


settings = Settings()
