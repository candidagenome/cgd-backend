#!/usr/bin/env python3
"""
Load GFF/FASTA data into GBrowse MySQL database.

This script loads genomic data (GFF and FASTA files) into a MySQL database
for use with GBrowse genome browser.

Usage:
    python load_gbrowse_mysql.py <assembly>
    python load_gbrowse_mysql.py candida_21
    python load_gbrowse_mysql.py --list  # List available assemblies

Environment Variables:
    GBROWSE_MYSQL_HOST: MySQL host (default: localhost)
    GBROWSE_MYSQL_USER: MySQL username (overrides config file)
    GBROWSE_MYSQL_PASSWORD: MySQL password (overrides config file)
    GBROWSE_DATA_DIR: Base directory for GBrowse data files
    GBROWSE_CONF_DIR: Directory containing mysql_conf.* files
    LOG_DIR: Directory for log files (default: /tmp)
    CURATOR_EMAIL: Email for error notifications
"""

import argparse
import glob
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Assembly configurations
# Maps assembly name to (config_file_suffix, data_subdir, file_patterns)
ASSEMBLIES = {
    # Candida assemblies
    "candida": ("candida", "candida", ["*.fasta", "*.gff"]),
    "candida_21": ("candida_21", "candida_21", ["*.fasta", "*.gff"]),
    "candida_21_prot": ("candida_21_prot", "candida_21_prot", ["*.gff"]),
    "candida_22": ("candida_22", "candida_22", ["*.fasta", "*.gff"]),
    "candida_22_prot": ("candida_22_prot", "candida_22_prot", ["*.gff"]),
    "cdub_cd36": ("cdub_cd36", "cdub_cd36", ["*.fasta", "*.gff"]),
    "cdub_cd36_prot": ("cdub_cd36_prot", "cdub_cd36_prot", ["*.gff"]),
    "cglab_cbs138": ("cglab_cbs138", "cglab_cbs138", ["*.fasta", "*.gff"]),
    "cglab_cbs138_prot": ("cglab_cbs138_prot", "cglab_cbs138_prot", ["*.gff"]),
    "cpar_cdc317": ("cpar_cdc317", "cpar_cdc317", ["*.fasta", "*.gff"]),
    "cpar_cdc317_prot": ("cpar_cdc317_prot", "cpar_cdc317_prot", ["*.gff"]),
    "cauris_b8441": ("cauris_b8441", "cauris_b8441", ["*.fasta", "*.gff"]),
    "cauris_b8441_prot": ("cauris_b8441_prot", "cauris_b8441_prot", ["*.gff"]),
    # Aspergillus assemblies
    "nidulans_4": ("nidulans_4", "nidulans_4", ["*.fasta", "*.gff"]),
    "nidulans_4_prot": ("nidulans_4_prot", "nidulans_4_prot", ["*.gff"]),
    "afum_af293": ("afum_af293", "afum_af293", ["*.fasta", "*.gff"]),
    "afum_af293_prot": ("afum_af293_prot", "afum_af293_prot", ["*.gff"]),
    "anig_cbs513_88": ("anig_cbs513_88", "anig_cbs513_88", ["*.fasta", "*.gff"]),
    "anig_cbs513_88_prot": ("anig_cbs513_88_prot", "anig_cbs513_88_prot", ["*.gff"]),
    "aory_rib40": ("aory_rib40", "aory_rib40", ["*.fasta", "*.gff"]),
    "aory_rib40_prot": ("aory_rib40_prot", "aory_rib40_prot", ["*.gff"]),
}


def setup_logging(assembly: str) -> logging.Logger:
    """Set up logging for the script."""
    log_dir = Path(os.getenv("LOG_DIR", "/tmp"))
    log_file = log_dir / f"gbrowse_loadMysql_{assembly}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger(__name__)


def read_config_file(config_path: Path) -> dict:
    """Read MySQL configuration from a config file."""
    config = {}
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, value = line.split("=", 1)
            config[key.strip()] = value.strip()

    return config


def get_data_files(data_dir: Path, patterns: list[str]) -> list[str]:
    """Get list of data files matching the patterns."""
    files = []
    for pattern in patterns:
        files.extend(glob.glob(str(data_dir / pattern)))
    return files


def send_error_email(message: str, logger: logging.Logger) -> None:
    """Send error notification email."""
    curator_email = os.getenv("CURATOR_EMAIL")
    if not curator_email:
        logger.warning("CURATOR_EMAIL not set, skipping email notification")
        return

    logger.info(f"Would send error email to {curator_email}: {message}")


def load_gbrowse_data(
    assembly: str,
    bp_bulk_load_script: Optional[str] = None,
) -> bool:
    """
    Load GFF/FASTA data into GBrowse MySQL database.

    Args:
        assembly: Name of the assembly to load
        bp_bulk_load_script: Path to bp_bulk_load_gff.pl script

    Returns:
        True on success, False on failure
    """
    logger = setup_logging(assembly)
    logger.info(f"Starting GBrowse MySQL load for assembly: {assembly}")
    logger.info(f"Start time: {datetime.now()}")

    if assembly not in ASSEMBLIES:
        logger.error(f"Unknown assembly: {assembly}")
        logger.error(f"Available assemblies: {', '.join(sorted(ASSEMBLIES.keys()))}")
        return False

    config_suffix, data_subdir, file_patterns = ASSEMBLIES[assembly]

    # Get paths from environment or use defaults
    conf_dir = Path(os.getenv("GBROWSE_CONF_DIR", "/opt/cgd/conf/gbrowse2.conf"))
    data_base_dir = Path(os.getenv("GBROWSE_DATA_DIR", "/opt/cgd/html/gbrowse2/databases"))
    db_host = os.getenv("GBROWSE_MYSQL_HOST", "localhost")

    if bp_bulk_load_script is None:
        bp_bulk_load_script = os.getenv(
            "BP_BULK_LOAD_SCRIPT",
            "/opt/cgd/bin/bp_bulk_load_gff.pl"
        )

    # Read config file
    config_file = conf_dir / f"mysql_conf.{config_suffix}"
    try:
        config = read_config_file(config_file)
    except FileNotFoundError as e:
        logger.error(str(e))
        return False

    # Get credentials (environment variables override config file)
    db_user = os.getenv("GBROWSE_MYSQL_USER", config.get("user", ""))
    db_password = os.getenv("GBROWSE_MYSQL_PASSWORD", config.get("password", ""))
    db_name = config.get("database", "")

    if not all([db_user, db_password, db_name]):
        logger.error("Missing database credentials. Check config file or environment variables.")
        return False

    # Get data files
    data_dir = data_base_dir / data_subdir
    data_files = get_data_files(data_dir, file_patterns)

    if not data_files:
        logger.error(f"No data files found in {data_dir} matching {file_patterns}")
        return False

    logger.info(f"Found {len(data_files)} data files to load")

    # Build command
    dsn = f"DBI:mysql:{db_name};host={db_host}"
    cmd = [
        bp_bulk_load_script,
        "--create",
        "--local",
        f'--database={dsn}',
        f"--user={db_user}",
        "--gff3_munge",
    ] + data_files

    # Log command (without password)
    logger.info(f"Running: {' '.join(cmd)}")

    # Execute with password in environment (not on command line)
    env = os.environ.copy()
    env["MYSQL_PWD"] = db_password

    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout
        )

        logger.info(f"Command output:\n{result.stdout}")

        if result.returncode != 0:
            error_msg = f"Command failed with return code {result.returncode}\n{result.stderr}"
            logger.error(error_msg)
            send_error_email(error_msg, logger)
            return False

        logger.info(f"Successfully loaded data for {assembly}")
        return True

    except subprocess.TimeoutExpired:
        error_msg = f"Command timed out after 1 hour for assembly {assembly}"
        logger.error(error_msg)
        send_error_email(error_msg, logger)
        return False

    except Exception as e:
        error_msg = f"Error loading data: {e}"
        logger.error(error_msg)
        send_error_email(error_msg, logger)
        return False

    finally:
        logger.info(f"Complete: {datetime.now()}")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load GFF/FASTA data into GBrowse MySQL database"
    )
    parser.add_argument(
        "assembly",
        nargs="?",
        help="Assembly name to load (e.g., candida_21, nidulans_4)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available assemblies",
    )
    parser.add_argument(
        "--script",
        help="Path to bp_bulk_load_gff.pl script",
    )

    args = parser.parse_args()

    if args.list:
        print("Available assemblies:")
        for name in sorted(ASSEMBLIES.keys()):
            print(f"  {name}")
        return 0

    if not args.assembly:
        parser.print_help()
        print("\nError: assembly name is required")
        return 1

    success = load_gbrowse_data(args.assembly, args.script)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
