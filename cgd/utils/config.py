"""
Environment and configuration management.

This module provides centralized configuration loading and access
for CGD scripts and applications.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv


@dataclass
class Config:
    """
    Configuration container for CGD applications.

    This class holds all standard configuration values loaded from
    environment variables with sensible defaults.
    """

    # Database
    database_url: str = ""
    db_schema: str = "MULTI"

    # Project
    project_acronym: str = "CGD"
    organism_name: str = "Candida albicans"
    strain_name: str = "SC5314"

    # Directories
    data_dir: Path = field(default_factory=lambda: Path("/var/data/cgd"))
    ftp_dir: Path = field(default_factory=lambda: Path("/var/ftp/cgd"))
    log_dir: Path = field(default_factory=lambda: Path("/var/log/cgd"))
    tmp_dir: Path = field(default_factory=lambda: Path("/tmp"))
    html_root_dir: Path = field(default_factory=lambda: Path("/var/www/html"))

    # URLs
    html_root_url: str = "http://www.candidagenome.org"
    ftp_root_url: str = "ftp://ftp.candidagenome.org/"

    # Email
    curator_email: str = ""
    help_email: str = ""

    # Sequence source
    seq_source: str = "Stanford"

    @classmethod
    def from_env(cls) -> "Config":
        """
        Create a Config instance from environment variables.

        Returns:
            Config instance with values from environment
        """
        load_dotenv()

        return cls(
            database_url=os.getenv("DATABASE_URL", ""),
            db_schema=os.getenv("DB_SCHEMA", "MULTI"),
            project_acronym=os.getenv("PROJECT_ACRONYM", "CGD"),
            organism_name=os.getenv("ORGANISM_NAME", "Candida albicans"),
            strain_name=os.getenv("STRAIN_NAME", "SC5314"),
            data_dir=Path(os.getenv("DATA_DIR", "/var/data/cgd")),
            ftp_dir=Path(os.getenv("FTP_DIR", "/var/ftp/cgd")),
            log_dir=Path(os.getenv("LOG_DIR", "/var/log/cgd")),
            tmp_dir=Path(os.getenv("TMP_DIR", "/tmp")),
            html_root_dir=Path(os.getenv("HTML_ROOT_DIR", "/var/www/html")),
            html_root_url=os.getenv("HTML_ROOT_URL", "http://www.candidagenome.org"),
            ftp_root_url=os.getenv("FTP_ROOT_URL", "ftp://ftp.candidagenome.org/"),
            curator_email=os.getenv("CURATOR_EMAIL", ""),
            help_email=os.getenv("HELP_EMAIL", ""),
            seq_source=os.getenv("SEQ_SOURCE", "Stanford"),
        )


# Module-level cached config instance
_config: Optional[Config] = None


def load_config(reload: bool = False) -> Config:
    """
    Load configuration from environment variables.

    This function caches the configuration to avoid repeated
    environment lookups.

    Args:
        reload: Force reload of configuration

    Returns:
        Config instance
    """
    global _config
    if _config is None or reload:
        load_dotenv()
        _config = Config.from_env()
    return _config


def get_config_value(key: str, default: Any = None) -> Any:
    """
    Get a single configuration value.

    This is a convenience function for accessing individual
    configuration values without loading the full Config object.

    Args:
        key: Configuration key (environment variable name)
        default: Default value if not set

    Returns:
        Configuration value
    """
    load_dotenv()
    return os.getenv(key, default)


def get_path_config(key: str, default: str = "") -> Path:
    """
    Get a path configuration value.

    Args:
        key: Configuration key (environment variable name)
        default: Default path string if not set

    Returns:
        Path object
    """
    load_dotenv()
    return Path(os.getenv(key, default))


# Common configuration shortcuts
def get_db_schema() -> str:
    """Get database schema name."""
    return get_config_value("DB_SCHEMA", "MULTI")


def get_project_acronym() -> str:
    """Get project acronym."""
    return get_config_value("PROJECT_ACRONYM", "CGD")


def get_data_dir() -> Path:
    """Get data directory path."""
    return get_path_config("DATA_DIR", "/var/data/cgd")


def get_ftp_dir() -> Path:
    """Get FTP directory path."""
    return get_path_config("FTP_DIR", "/var/ftp/cgd")


def get_log_dir() -> Path:
    """Get log directory path."""
    return get_path_config("LOG_DIR", "/var/log/cgd")
