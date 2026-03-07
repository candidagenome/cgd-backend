#!/usr/bin/env python3
"""
Load GBrowse MySQL database (new version).

This script loads GFF and FASTA files into a MySQL database for GBrowse2
using bp_seqfeature_load.pl.

Based on loadGBrowseMysqlDatabase_new.pl.

Usage:
    python load_gbrowse_mysql_new.py candida_21
    python load_gbrowse_mysql_new.py nidulans_4
    python load_gbrowse_mysql_new.py --help

Arguments:
    assembly: GBrowse database name (e.g., candida_21, nidulans_4)

Environment Variables:
    GBROWSE_MYSQL_USER: MySQL username for GBrowse
    GBROWSE_MYSQL_PASSWORD: MySQL password for GBrowse
    LOG_DIR: Directory for log files
    CONF_DIR: Directory for configuration files
    HTML_ROOT_DIR: HTML root directory
"""

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Load environment variables
load_dotenv()

# Configuration from environment
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
CONF_DIR = Path(os.getenv("CONF_DIR", "/etc/cgd"))
HTML_ROOT = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
GBROWSE_MYSQL_USER = os.getenv("GBROWSE_MYSQL_USER", "")
GBROWSE_MYSQL_PASSWORD = os.getenv("GBROWSE_MYSQL_PASSWORD", "")

# Path to bp_seqfeature_load.pl
BP_SEQFEATURE_LOAD = os.getenv(
    "BP_SEQFEATURE_LOAD",
    "/tools/perl/current/bin/bp_seqfeature_load.pl"
)

# GBrowse2 configuration and data directories
GBROWSE_CONF_DIR = CONF_DIR / "gbrowse2.conf"
GBROWSE_DATA_DIR = HTML_ROOT / "gbrowse2" / "databases"

# Assembly configurations
ASSEMBLY_CONFIG = {
    "candida_19": {
        "conf_file": "mysql_conf.candida_19",
        "data_dir": "candida",
    },
    "candida_21": {
        "conf_file": "mysql_conf.candida_21",
        "data_dir": "candida_21",
    },
    "nidulans_4": {
        "conf_file": "mysql_conf.nidulans_4",
        "data_dir": "nidulans_4",
    },
    "aory_rib40": {
        "conf_file": "mysql_conf.aory_rib40",
        "data_dir": "aory_rib40",
    },
    "afum_af293": {
        "conf_file": "mysql_conf.afum_af293",
        "data_dir": "afum_af293",
    },
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def read_mysql_config(conf_file: Path) -> dict[str, str]:
    """
    Read MySQL configuration from config file.

    Returns dict with database, user, password, mysql_socket.
    """
    config = {}

    if not conf_file.exists():
        raise FileNotFoundError(f"Config file not found: {conf_file}")

    with open(conf_file) as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue

            param, value = line.split("=", 1)
            config[param.strip()] = value.strip()

    return config


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load GBrowse MySQL database"
    )
    parser.add_argument(
        "assembly",
        choices=list(ASSEMBLY_CONFIG.keys()),
        help="GBrowse database name",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show commands without executing",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    assembly = args.assembly

    # Set up log file
    log_file = LOG_DIR / f"gbrowse_loadMysql_{assembly}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    logger.info(f"Starting GBrowse MySQL load for {assembly} at {datetime.now()}")

    # Get assembly configuration
    if assembly not in ASSEMBLY_CONFIG:
        logger.error(f"Unknown assembly: {assembly}")
        return 1

    config = ASSEMBLY_CONFIG[assembly]
    conf_file = GBROWSE_CONF_DIR / config["conf_file"]
    data_dir = GBROWSE_DATA_DIR / config["data_dir"]

    # Read MySQL configuration
    try:
        mysql_config = read_mysql_config(conf_file)
    except FileNotFoundError as e:
        logger.error(str(e))
        return 1

    # Allow environment variables to override config file values
    mysql_user = GBROWSE_MYSQL_USER or mysql_config.get("user", "")
    mysql_password = GBROWSE_MYSQL_PASSWORD or mysql_config.get("password", "")
    mysql_database = mysql_config.get("database", "")
    mysql_socket = mysql_config.get("mysql_socket", "/var/lib/mysql/mysql.sock")

    if not mysql_user or not mysql_password or not mysql_database:
        logger.error("Missing MySQL configuration (user, password, or database)")
        return 1

    # Find data files
    fasta_files = list(data_dir.glob("*.fasta"))
    gff_files = list(data_dir.glob("*.gff"))

    if not fasta_files and not gff_files:
        logger.error(f"No FASTA or GFF files found in {data_dir}")
        return 1

    data_files = [str(f) for f in fasta_files + gff_files]
    logger.info(f"Found {len(fasta_files)} FASTA and {len(gff_files)} GFF files")

    # Build DSN
    dsn = f"dbi:mysql:{mysql_database};host=localhost;mysql_socket={mysql_socket}"

    # Build command
    command = [
        BP_SEQFEATURE_LOAD,
        "--create",
        f"--dsn={dsn}",
        f"--user={mysql_user}",
    ] + data_files

    logger.info(f"Command: {' '.join(command)} (password hidden)")

    if args.dry_run:
        logger.info("Dry run - not executing")
        return 0

    # Execute command with password in environment
    try:
        env = os.environ.copy()
        env["MYSQL_PWD"] = mysql_password

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            env=env,
            timeout=3600,  # 1 hour timeout
        )

        logger.info(result.stdout)

        if result.returncode != 0:
            logger.error(f"Command failed with exit code {result.returncode}")
            logger.error(result.stderr)
            return 1

    except subprocess.TimeoutExpired:
        logger.error("Command timed out after 1 hour")
        return 1
    except Exception as e:
        logger.error(f"Error executing command: {e}")
        return 1

    logger.info(f"GBrowse MySQL load completed at {datetime.now()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
