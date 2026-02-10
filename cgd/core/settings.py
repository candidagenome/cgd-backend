import secrets
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings.

    Required:
      - DATABASE_URL
      - JWT_SECRET_KEY (or auto-generated for development)

    Optional:
      - DB_SCHEMA: used for prefixing table names in raw SQL: "{schema}.{table}"
      - CGD_DATA_DIR: path to CGD data files (homology alignments, trees, etc.)
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    db_schema: Optional[str] = None

    # JWT Authentication settings
    jwt_secret_key: str = Field(
        default_factory=lambda: secrets.token_urlsafe(32),
        validation_alias="JWT_SECRET_KEY",
        description="Secret key for JWT signing. MUST be set in production.",
    )
    jwt_access_token_expire_minutes: int = Field(
        default=240,
        validation_alias="JWT_ACCESS_TOKEN_EXPIRE_MINUTES",
        description="Access token expiration in minutes (default 4 hours)",
    )
    jwt_refresh_token_expire_days: int = Field(
        default=7,
        validation_alias="JWT_REFRESH_TOKEN_EXPIRE_DAYS",
        description="Refresh token expiration in days",
    )

    # Path to CGD data files (default matches typical production setup)
    cgd_data_dir: str = "/data"

    # Back-compat toggle for CGI-style dispatch endpoint
    allow_search_dispatch: bool = True

    # API prefix (kept constant for reverse-proxy routing)
    api_prefix: str = "/api"

    # Elasticsearch configuration
    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_index: str = "cgd"

    # BLAST configuration
    blast_bin_path: str = Field(
        default="/tools/ncbi/blast/bin/",
        validation_alias="BLAST_BIN"
    )
    blast_db_path: str = Field(
        default="/data/blast_datasets/",
        validation_alias="BLAST_DB_DIR"
    )
    fasta_dir: str = Field(
        default="/data/fasta_files/",
        validation_alias="FASTA_DIR"
    )
    blast_timeout: int = Field(
        default=300,
        description="BLAST search timeout in seconds"
    )
    blast_clade_conf: Optional[str] = Field(
        default=None,
        validation_alias="BLAST_CLADE_CONF",
        description="Path to external blast_clade.conf file"
    )

    # JBrowse configuration
    jbrowse_base_url: str = Field(
        default="http://www.candidagenome.org/jbrowse/index.html",
        validation_alias="JBROWSE_BASE_URL"
    )
    jbrowse_flank: int = Field(
        default=1000,
        description="Flanking base pairs for JBrowse coordinates"
    )


settings = Settings()
