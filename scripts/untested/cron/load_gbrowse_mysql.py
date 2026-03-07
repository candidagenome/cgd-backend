#!/usr/bin/env python3
"""
Load GFF and FASTA data into GBrowse MySQL database.

This script loads genomic data (GFF annotations and FASTA sequences) into
a MySQL database used by GBrowse genome browser. It wraps the Bio::DB::SeqFeature
bulk loader (bp_bulk_load_gff.pl).

Based on loadGBrowseMysqlDatabase.pl by CGD team.

Usage:
    python load_gbrowse_mysql.py <assembly_name>
    python load_gbrowse_mysql.py candida_22
    python load_gbrowse_mysql.py cauris_b8441

Environment Variables:
    GBROWSE_MYSQL_HOST: MySQL host (default: localhost)
    GBROWSE_MYSQL_USER: MySQL username
    GBROWSE_MYSQL_PASSWORD: MySQL password
    GBROWSE_DATA_DIR: Base directory for GBrowse data files
    GBROWSE_CONF_DIR: Directory for GBrowse config files
    BULK_LOADER_PATH: Path to bp_bulk_load_gff.pl script
    LOG_DIR: Directory for log files
    CURATOR_EMAIL: Email address for error notifications
    ADMIN_EMAIL: From email address for notifications
"""

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration from environment
GBROWSE_MYSQL_HOST = os.getenv("GBROWSE_MYSQL_HOST", "localhost")
GBROWSE_MYSQL_USER = os.getenv("GBROWSE_MYSQL_USER")
GBROWSE_MYSQL_PASSWORD = os.getenv("GBROWSE_MYSQL_PASSWORD")

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/var/www/cgd"))
GBROWSE_DATA_DIR = Path(os.getenv("GBROWSE_DATA_DIR", PROJECT_ROOT / "html/gbrowse2/databases"))
GBROWSE_CONF_DIR = Path(os.getenv("GBROWSE_CONF_DIR", PROJECT_ROOT / "conf/gbrowse2.conf"))
BULK_LOADER_PATH = Path(os.getenv("BULK_LOADER_PATH", PROJECT_ROOT / "bin/bp_bulk_load_gff.pl"))

LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL", "")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Assembly configurations
# Maps assembly name to (config_file, data_dir, data_files_pattern)
ASSEMBLY_CONFIG = {
    # Candida albicans assemblies
    "candida": {
        "patterns": ["*.fasta", "*.gff"],
    },
    "candida_21": {
        "patterns": ["*.fasta", "*.gff"],
    },
    "candida_21_prot": {
        "patterns": ["*.gff"],
    },
    "candida_22": {
        "patterns": ["*.fasta", "*.gff"],
    },
    "candida_22_prot": {
        "patterns": ["*.gff"],
    },
    # Candida dubliniensis
    "cdub_cd36": {
        "patterns": ["*.fasta", "*.gff"],
    },
    "cdub_cd36_prot": {
        "patterns": ["*.gff"],
    },
    # Candida glabrata
    "cglab_cbs138": {
        "patterns": ["*.fasta", "*.gff"],
    },
    "cglab_cbs138_prot": {
        "patterns": ["*.gff"],
    },
    # Candida parapsilosis
    "cpar_cdc317": {
        "patterns": ["*.fasta", "*.gff"],
    },
    "cpar_cdc317_prot": {
        "patterns": ["*.gff"],
    },
    # Candida auris
    "cauris_b8441": {
        "patterns": ["*.fasta", "*.gff"],
    },
    "cauris_b8441_prot": {
        "patterns": ["*.gff"],
    },
    # Aspergillus nidulans
    "nidulans_4": {
        "patterns": ["*.fasta", "*.gff"],
    },
    "nidulans_4_prot": {
        "patterns": ["*.gff"],
    },
    # Aspergillus fumigatus
    "afum_af293": {
        "patterns": ["*.fasta", "*.gff"],
    },
    "afum_af293_prot": {
        "patterns": ["*.gff"],
    },
    # Aspergillus niger
    "anig_cbs513_88": {
        "patterns": ["*.fasta", "*.gff"],
    },
    "anig_cbs513_88_prot": {
        "patterns": ["*.gff"],
    },
    # Aspergillus oryzae
    "aory_rib40": {
        "patterns": ["*.fasta", "*.gff"],
    },
    "aory_rib40_prot": {
        "patterns": ["*.gff"],
    },
}


def send_error_email(subject: str, message: str) -> None:
    """Send error notification email."""
    if not CURATOR_EMAIL:
        logger.warning("CURATOR_EMAIL not set, skipping email notification")
        return

    # Log the error - in production, implement actual email sending
    logger.error(f"Email notification: {subject}")
    logger.error(f"Message: {message}")

    # Example using sendmail (uncomment and modify for production):
    # import smtplib
    # from email.mime.text import MIMEText
    # msg = MIMEText(message)
    # msg['Subject'] = subject
    # msg['From'] = ADMIN_EMAIL or 'noreply@localhost'
    # msg['To'] = CURATOR_EMAIL
    # with smtplib.SMTP('localhost') as s:
    #     s.send_message(msg)


def read_config_file(config_file: Path) -> dict[str, str]:
    """Read key=value config file."""
    values = {}
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_file}")

    with open(config_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()

    return values


def get_data_files(data_dir: Path, patterns: list[str]) -> list[Path]:
    """Get list of data files matching patterns."""
    files = []
    for pattern in patterns:
        files.extend(data_dir.glob(pattern))
    return sorted(files)


def load_gbrowse_database(assembly: str) -> bool:
    """
    Load data into GBrowse MySQL database for an assembly.

    Returns True on success, False on error.
    """
    if assembly not in ASSEMBLY_CONFIG:
        logger.error(f"Unknown assembly: {assembly}")
        logger.error(f"Supported assemblies: {', '.join(sorted(ASSEMBLY_CONFIG.keys()))}")
        return False

    config = ASSEMBLY_CONFIG[assembly]

    # Set up paths
    config_file = GBROWSE_CONF_DIR / f"mysql_conf.{assembly}"
    data_dir = GBROWSE_DATA_DIR / assembly

    # Set up logging to file
    log_file = LOG_DIR / f"gbrowse_loadMysql_{assembly}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Loading GBrowse database for assembly: {assembly}")
    logger.info(f"Config file: {config_file}")
    logger.info(f"Data directory: {data_dir}")

    try:
        # Read config file
        db_config = read_config_file(config_file)

        # Allow environment variables to override config values
        db_user = GBROWSE_MYSQL_USER or db_config.get("user")
        db_password = GBROWSE_MYSQL_PASSWORD or db_config.get("password")
        db_name = db_config.get("database")

        if not db_user or not db_password:
            raise ValueError("MySQL user or password not configured")

        if not db_name:
            raise ValueError("Database name not found in config file")

        # Get data files
        data_files = get_data_files(data_dir, config["patterns"])
        if not data_files:
            raise FileNotFoundError(f"No data files found in {data_dir}")

        logger.info(f"Found {len(data_files)} data files")
        for f in data_files:
            logger.info(f"  {f.name}")

        # Build bp_bulk_load_gff.pl command
        dsn = f"DBI:mysql:{db_name};host={GBROWSE_MYSQL_HOST}"
        data_files_str = " ".join(str(f) for f in data_files)

        command = [
            str(BULK_LOADER_PATH),
            "--create",
            "--local",
            f"--database={dsn}",
            f"--user={db_user}",
            "--gff3_munge",
        ] + [str(f) for f in data_files]

        # Log command (without password)
        safe_command = " ".join(command)
        logger.info(f"Running: {safe_command}")

        # Run the command with password in environment
        env = os.environ.copy()
        env["MYSQL_PWD"] = db_password

        result = subprocess.run(
            command,
            env=env,
            capture_output=True,
            text=True,
        )

        # Log output
        if result.stdout:
            logger.info(f"Output:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"Stderr:\n{result.stderr}")

        if result.returncode != 0:
            raise RuntimeError(f"bp_bulk_load_gff.pl failed with code {result.returncode}")

        logger.info(f"Successfully loaded data for {assembly}")
        return True

    except Exception as e:
        error_msg = f"Error loading GBrowse database for {assembly}: {e}"
        logger.error(error_msg)
        send_error_email(
            "Error loading gbrowse data",
            f"{error_msg}\n\nSee {log_file} for more details.",
        )
        return False

    finally:
        logger.removeHandler(file_handler)
        file_handler.close()


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load GFF/FASTA data into GBrowse MySQL database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Supported assemblies:
  {', '.join(sorted(ASSEMBLY_CONFIG.keys()))}

Examples:
  python load_gbrowse_mysql.py candida_22
  python load_gbrowse_mysql.py cauris_b8441
""",
    )
    parser.add_argument(
        "assembly",
        help="GBrowse database/assembly name",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List supported assemblies and exit",
    )

    args = parser.parse_args()

    if args.list:
        print("Supported assemblies:")
        for name in sorted(ASSEMBLY_CONFIG.keys()):
            print(f"  {name}")
        return 0

    success = load_gbrowse_database(args.assembly)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
