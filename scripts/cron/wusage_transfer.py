#!/usr/bin/env python3
"""
Transfer web access logs to wusage reporting server.

This script transfers Apache access logs from the main web server to a
separate server that processes them for wusage (web usage) reports.

It handles:
- Current month's logs (compressed or uncompressed)
- Previous month's logs (compressed)
- Embedding project name in destination filenames

Environment Variables:
    WUSAGE_ADMIN_USER: SSH user for destination server (default: cgdadmin)
    WUSAGE_DEST_HOST: Destination hostname (default: fafner)
    WUSAGE_SRC_DIR: Source directory for logs (default: /var/log/httpd)
    WUSAGE_DEST_DIR: Destination directory on remote (default: wusage)
    PROJECT_NAME: Project name to embed in filenames (default: cgd)
    LOG_DIR: Directory for script logs (default: /tmp)
    CURATOR_EMAIL: Email for error notifications
"""

import gzip
import logging
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration from environment
ADMIN_USER = os.getenv("WUSAGE_ADMIN_USER", "cgdadmin")
DEST_HOST = os.getenv("WUSAGE_DEST_HOST", "fafner")
SRC_DIR = Path(os.getenv("WUSAGE_SRC_DIR", "/var/log/httpd"))
DEST_DIR = os.getenv("WUSAGE_DEST_DIR", "wusage")
PROJECT_NAME = os.getenv("PROJECT_NAME", "cgd")
LOG_DIR = Path(os.getenv("LOG_DIR", "/tmp"))

# Log file settings
ACCESS_LOG_BASE = "access_log"
LOG_FILE = LOG_DIR / "wusage-transfer.log"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def send_error_email(message: str) -> None:
    """Send error notification email and exit."""
    curator_email = os.getenv("CURATOR_EMAIL")
    if curator_email:
        logger.info(f"Would send error email to {curator_email}: {message}")
    logger.error(message)
    sys.exit(1)


def make_dest_filename(src_filename: str, project: str, current_date: str) -> str:
    """
    Create destination filename with project name embedded.

    Args:
        src_filename: Source filename (e.g., access_log.202402.gz)
        project: Project name to embed
        current_date: Current month date string (YYYYMM)

    Returns:
        Destination filename with project name
    """
    # Match pattern: access_log.YYYYMM.gz
    match = re.search(r"\.(\d{6})\.gz$", src_filename)

    if match:
        yyyymm = match.group(1)
        if yyyymm == current_date:
            # Current month: access_log.YYYYMM.gz -> access_log.project
            return re.sub(r"\.\d{6}\.gz$", f".{project}", src_filename)
        else:
            # Previous month: access_log.YYYYMM.gz -> access_log.project.YYYYMM.gz
            return re.sub(r"^([^.]+)\.", rf"\1.{project}.", src_filename)
    else:
        # No date in filename: access_log -> access_log.project
        return f"{src_filename}.{project}"


def decompress_file(src_path: Path) -> Path:
    """
    Decompress a gzipped file.

    Args:
        src_path: Path to the gzipped file

    Returns:
        Path to the decompressed file
    """
    dest_path = src_path.with_suffix("")  # Remove .gz

    logger.info(f"Decompressing {src_path}")

    with gzip.open(src_path, "rb") as f_in:
        with open(dest_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    return dest_path


def copy_file(src: Path, dest: str) -> bool:
    """
    Copy file to remote server using scp.

    Args:
        src: Source file path
        dest: Destination in format user@host:path

    Returns:
        True on success, False on failure
    """
    if not src.exists():
        logger.error(f"Log file {src} not found!")
        return False

    logger.info(f"Copying {src} -> {dest}")

    try:
        result = subprocess.run(
            ["scp", "-q", str(src), dest],
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
        )

        if result.returncode != 0:
            logger.error(
                f"scp failed with return code {result.returncode}: {result.stderr}"
            )
            return False

        return True

    except subprocess.TimeoutExpired:
        logger.error(f"scp timed out copying {src}")
        return False
    except Exception as e:
        logger.error(f"Error copying file: {e}")
        return False


def transfer_logs() -> bool:
    """
    Transfer current and previous month's access logs.

    Returns:
        True on success, False on failure
    """
    logger.info("-" * 54)
    logger.info("Starting wusage_transfer.py")

    # Calculate current and previous month dates
    now = datetime.now()
    current_date = now.strftime("%Y%m")
    prev_month = now - relativedelta(months=1)
    prev_date = prev_month.strftime("%Y%m")

    logger.info(f"Current date: {current_date}")

    # Build destination base
    copy_dest = f"{ADMIN_USER}@{DEST_HOST}:{DEST_DIR}"

    # Determine current month's log file
    compressed_current = SRC_DIR / f"{ACCESS_LOG_BASE}.{current_date}.gz"
    uncompressed_current = SRC_DIR / ACCESS_LOG_BASE

    temp_decompressed = None  # Track if we created a temp file

    if compressed_current.exists():
        # Current month is compressed - need to decompress
        access_log_to_copy = compressed_current
        log_dest_name = make_dest_filename(
            compressed_current.name, PROJECT_NAME, current_date
        )

        # Decompress for transfer
        temp_decompressed = decompress_file(compressed_current)
        access_log_to_copy = temp_decompressed
        # Update dest name to not have .gz since we decompressed
        log_dest_name = log_dest_name.replace(".gz", "")

    elif uncompressed_current.exists():
        access_log_to_copy = uncompressed_current
        log_dest_name = make_dest_filename(
            uncompressed_current.name, PROJECT_NAME, current_date
        )
    else:
        logger.warning(f"No current month log found at {compressed_current} or {uncompressed_current}")
        access_log_to_copy = None

    # Copy current month's log
    if access_log_to_copy:
        if not copy_file(access_log_to_copy, f"{copy_dest}/{log_dest_name}"):
            send_error_email(f"Failed to copy {access_log_to_copy}")

    # Clean up temp decompressed file
    if temp_decompressed and temp_decompressed.exists():
        try:
            temp_decompressed.unlink()
            logger.info(f"Cleaned up temp file: {temp_decompressed}")
        except OSError as e:
            logger.warning(f"Could not remove temp file {temp_decompressed}: {e}")

    # Copy previous month's log (should be compressed)
    prev_log = SRC_DIR / f"{ACCESS_LOG_BASE}.{prev_date}.gz"
    if prev_log.exists():
        log_dest_name = make_dest_filename(prev_log.name, PROJECT_NAME, current_date)
        if not copy_file(prev_log, f"{copy_dest}/{log_dest_name}"):
            logger.warning(f"Failed to copy previous month's log: {prev_log}")
    else:
        logger.warning(f"Previous month's log not found: {prev_log}")

    logger.info("File transfer complete")
    return True


def main() -> int:
    """Main entry point."""
    try:
        success = transfer_logs()
        return 0 if success else 1
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        send_error_email(f"Unexpected error in wusage_transfer: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
