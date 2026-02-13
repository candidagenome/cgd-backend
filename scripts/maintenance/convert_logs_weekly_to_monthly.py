#!/usr/bin/env python3
"""
Convert weekly Apache access logs to monthly logs.

This script is a pre-cursor to wusage_transfer.py for servers where Apache
logs are rotated weekly. It consolidates weekly log files into monthly files
for web usage statistics processing.

The script reads rotated log files (access_log.N.gz), extracts the date from
each log entry, and groups them into monthly files (access_log.YYYYMM.gz).

Environment Variables:
    WEB_LOG_DIR: Directory containing Apache log files
    LOG_DIR: Directory for script logs (default: /tmp)
    CURATOR_EMAIL: Email for error notifications
"""

import gzip
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
WEB_LOG_DIR = Path(os.getenv("WEB_LOG_DIR", "/var/log/httpd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/tmp"))
LOG_FILE = LOG_DIR / "convert_access_log.log"
ACCESS_LOG_BASE = "access_log"

# Month name to number mapping
MONTH_TO_NUM = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}

# Pattern to extract date from Apache log entry
# Example: 192.168.1.1 - - [01/Aug/2024:12:34:56 -0700] "GET / HTTP/1.1"
LOG_DATE_PATTERN = re.compile(
    r"^\d+\.\d+\.\d+\.\d+ - - \[\d+/(\w+)/(\d+):"
)

# Configure logging
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


def find_rotated_logs(src_dir: Path) -> list[tuple[int, Path]]:
    """
    Find all rotated gzipped log files.

    Args:
        src_dir: Directory to search

    Returns:
        List of (rotation_number, path) tuples, sorted by rotation number descending
    """
    logs = []

    for path in src_dir.iterdir():
        match = re.match(rf"^{ACCESS_LOG_BASE}\.(\d+)\.gz$", path.name)
        if match and path.is_file():
            rotation_num = int(match.group(1))
            logs.append((rotation_num, path))

    # Sort by rotation number descending (most recent first)
    logs.sort(key=lambda x: x[0], reverse=True)
    return logs


def process_log_file(log_path: Path) -> dict[str, list[str]]:
    """
    Process a single log file and group entries by month.

    Args:
        log_path: Path to gzipped log file

    Returns:
        Dictionary mapping YYYYMM to list of log lines
    """
    month_data: dict[str, list[str]] = defaultdict(list)

    logger.info(f"Reading {log_path}")

    try:
        with gzip.open(log_path, "rt", encoding="utf-8", errors="replace") as f:
            for line in f:
                match = LOG_DATE_PATTERN.match(line)
                if match:
                    month_name = match.group(1)
                    year = match.group(2)

                    if month_name in MONTH_TO_NUM:
                        key = f"{year}{MONTH_TO_NUM[month_name]}"
                        month_data[key].append(line)

    except Exception as e:
        logger.error(f"Error reading {log_path}: {e}")

    return dict(month_data)


def convert_logs() -> bool:
    """
    Convert weekly logs to monthly logs.

    Returns:
        True on success, False on failure
    """
    logger.info("-" * 54)
    logger.info("Starting convert_logs_weekly_to_monthly.py")
    logger.info(f"Source directory: {WEB_LOG_DIR}")

    if not WEB_LOG_DIR.exists():
        send_error_email(f"Web log directory not found: {WEB_LOG_DIR}")
        return False

    # Find all rotated logs
    rotated_logs = find_rotated_logs(WEB_LOG_DIR)

    if not rotated_logs:
        logger.info("No rotated log files found")
        return True

    logger.info(f"Found {len(rotated_logs)} rotated log files")

    # Collect all log data by month
    all_month_data: dict[str, list[str]] = defaultdict(list)

    for rotation_num, log_path in rotated_logs:
        month_data = process_log_file(log_path)

        for yyyymm, lines in month_data.items():
            all_month_data[yyyymm].extend(lines)

    # Write monthly log files
    logger.info(f"Writing {len(all_month_data)} monthly log files")

    for yyyymm, lines in sorted(all_month_data.items()):
        output_file = WEB_LOG_DIR / f"{ACCESS_LOG_BASE}.{yyyymm}.gz"

        logger.info(f"Writing monthly log for {yyyymm} ({len(lines)} entries)")

        try:
            with gzip.open(output_file, "wt", encoding="utf-8") as f:
                f.writelines(lines)

            logger.info(f"Created {output_file}")

        except Exception as e:
            logger.error(f"Error writing {output_file}: {e}")
            return False

    logger.info("Program complete")
    return True


def main() -> int:
    """Main entry point."""
    try:
        success = convert_logs()
        return 0 if success else 1

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        send_error_email(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
