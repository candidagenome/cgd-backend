#!/usr/bin/env python3
"""
Generate config package to path mapping file.

This script recursively searches the config directory and creates a mapping
of package names to their file paths. This is used by the configuration
factory to locate packages.

Based on makeConfigPackage2PathMappingFile.pl.

Usage:
    python make_config_mapping_file.py
    python make_config_mapping_file.py --debug

Environment Variables:
    LIB_DIR: Directory containing library modules
    LOG_DIR: Directory for log files
"""

import argparse
import hashlib
import logging
import os
import shutil
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
LIB_DIR = Path(os.getenv("LIB_DIR", "/var/lib/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
IS_PRODUCTION = os.getenv("IS_PRODUCTION", "false").lower() == "true"

# Config directory
CONFIG_DIR = LIB_DIR / "Config"

# Mapping file paths
REAL_MAPPING_FILE = CONFIG_DIR / "configPackage2path_mapping"
TMP_MAPPING_FILE = CONFIG_DIR / "configPackage2path_mapping.tmp"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def calculate_checksum(filepath: Path) -> str:
    """Calculate checksum of a file."""
    if not filepath.exists():
        return ""

    hasher = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def find_config_packages(config_dir: Path, lib_dir: Path) -> dict[str, str]:
    """
    Find all Python config packages in the config directory.

    Returns dict mapping package name (without extension) to relative path.
    """
    packages = {}

    for filepath in config_dir.rglob("*.py"):
        # Skip __pycache__ directories
        if "__pycache__" in str(filepath):
            continue

        # Skip __init__.py files
        if filepath.name == "__init__.py":
            continue

        # Get package name (filename without .py)
        package_name = filepath.stem

        # Get relative path from lib directory
        try:
            relative_path = filepath.relative_to(lib_dir)
        except ValueError:
            relative_path = filepath

        # Only add if not already processed (first occurrence wins)
        if package_name not in packages:
            packages[package_name] = str(relative_path)

    return packages


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate config package to path mapping file"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    # Set up log file
    log_file = LOG_DIR / "makeConfigPackage2PathMappingFile.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    logger.info(f"Starting config mapping generation at {datetime.now()}")

    # Check config directory exists
    if not CONFIG_DIR.exists():
        logger.error(f"Config directory does not exist: {CONFIG_DIR}")
        return 1

    # Find all config packages
    packages = find_config_packages(CONFIG_DIR, LIB_DIR)
    logger.info(f"Found {len(packages)} config packages")

    # Write temporary mapping file
    with open(TMP_MAPPING_FILE, "w") as f:
        for package_name, relative_path in sorted(packages.items()):
            f.write(f"{package_name}\t{relative_path}\n")

    # Compare checksums
    old_checksum = calculate_checksum(REAL_MAPPING_FILE)
    new_checksum = calculate_checksum(TMP_MAPPING_FILE)

    if old_checksum == new_checksum:
        logger.info(
            f"{REAL_MAPPING_FILE} and {TMP_MAPPING_FILE} are the same."
        )
        TMP_MAPPING_FILE.unlink()
        return 0

    # Files are different, update the real file
    logger.info(f"Updated {REAL_MAPPING_FILE}")
    shutil.move(str(TMP_MAPPING_FILE), str(REAL_MAPPING_FILE))

    # Commit to version control if production
    if IS_PRODUCTION:
        logger.info("Committing changes to version control...")

        timestamp = datetime.now().strftime("%Y%m%d")
        filename = REAL_MAPPING_FILE.name

        try:
            # Change to config directory
            original_dir = os.getcwd()
            os.chdir(CONFIG_DIR)

            # Git commit (modern replacement for CVS)
            subprocess.run(
                ["git", "add", filename],
                capture_output=True,
                text=True,
                timeout=300,
            )
            subprocess.run(
                ["git", "commit", "-m", timestamp],
                capture_output=True,
                text=True,
                timeout=300,
            )

            os.chdir(original_dir)
            logger.info("Version control commit successful")

        except subprocess.TimeoutExpired:
            logger.error("Timeout during version control commit")
        except Exception as e:
            logger.error(f"Error during version control commit: {e}")
    else:
        logger.info("Version control commit skipped on non-production system")

    logger.info(f"Config mapping generation completed at {datetime.now()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
