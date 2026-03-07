#!/usr/bin/env python3
"""
Convert weekly Apache access logs to monthly logs.

This script reads rotated weekly Apache access logs and consolidates them
into monthly log files for web usage statistics analysis.

Based on convert_logs_weekly_to_monthly.pl by Prachi Shah (Aug 2009).

Usage:
    python convert_logs_weekly_to_monthly.py
    python convert_logs_weekly_to_monthly.py --log-dir /var/log/apache2

Environment Variables:
    WEB_LOG_DIR: Directory containing Apache access logs
    LOG_DIR: Directory for script logs
"""

import argparse
import gzip
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Load environment variables
load_dotenv()

# Configuration from environment
WEB_LOG_DIR = Path(os.getenv("WEB_LOG_DIR", "/var/log/apache2"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))

# Month name to number mapping
MONTH_TO_NUM = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}

# Apache log date pattern: [DD/Mon/YYYY:HH:MM:SS
LOG_DATE_PATTERN = re.compile(r"^\d+\.\d+\.\d+\.\d+ - - \[(\d+)/(\w+)/(\d+):")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def find_rotated_log_files(log_dir: Path, base_name: str = "access_log") -> list[Path]:
    """Find all rotated gzipped log files."""
    files = []
    for path in log_dir.iterdir():
        if path.name.startswith(base_name) and path.suffix == ".gz":
            # Match patterns like access_log.1.gz, access_log.52.gz
            match = re.match(rf"{base_name}\.(\d+)\.gz", path.name)
            if match:
                files.append((int(match.group(1)), path))

    # Sort by rotation number (highest first = oldest)
    files.sort(key=lambda x: x[0], reverse=True)
    return [f[1] for f in files]


def extract_month_from_line(line: str) -> str | None:
    """
    Extract YYYYMM from an Apache access log line.

    Returns None if the line doesn't match the expected format.
    """
    match = LOG_DATE_PATTERN.match(line)
    if match:
        day, month_name, year = match.groups()
        month_num = MONTH_TO_NUM.get(month_name)
        if month_num:
            return f"{year}{month_num}"
    return None


def process_log_file(log_file: Path, month_data: dict[str, list[str]]):
    """Process a single log file and add lines to appropriate month buckets."""
    logger.info(f"Processing {log_file}")

    try:
        with gzip.open(log_file, "rt", encoding="utf-8", errors="replace") as f:
            for line in f:
                yyyymm = extract_month_from_line(line)
                if yyyymm:
                    month_data[yyyymm].append(line)
    except Exception as e:
        logger.error(f"Error reading {log_file}: {e}")


def write_monthly_logs(month_data: dict[str, list[str]], output_dir: Path, base_name: str = "access_log"):
    """Write consolidated monthly log files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    for yyyymm, lines in sorted(month_data.items()):
        output_file = output_dir / f"{base_name}.{yyyymm}.gz"
        logger.info(f"Writing {len(lines)} lines to {output_file}")

        try:
            with gzip.open(output_file, "wt", encoding="utf-8") as f:
                f.writelines(lines)
        except Exception as e:
            logger.error(f"Error writing {output_file}: {e}")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Convert weekly Apache access logs to monthly logs"
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=WEB_LOG_DIR,
        help=f"Directory containing Apache access logs (default: {WEB_LOG_DIR})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for monthly logs (default: same as log-dir)",
    )
    parser.add_argument(
        "--base-name",
        default="access_log",
        help="Base name for log files (default: access_log)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write output files, just show what would be done",
    )

    args = parser.parse_args()

    log_dir = args.log_dir
    output_dir = args.output_dir or log_dir
    base_name = args.base_name

    if not log_dir.exists():
        logger.error(f"Log directory does not exist: {log_dir}")
        return 1

    logger.info(f"Processing logs in {log_dir}")

    try:
        # Find all rotated log files
        log_files = find_rotated_log_files(log_dir, base_name)
        if not log_files:
            logger.warning(f"No rotated log files found matching {base_name}.*.gz")
            return 0

        logger.info(f"Found {len(log_files)} rotated log files")

        # Process all log files
        month_data: dict[str, list[str]] = defaultdict(list)

        for log_file in log_files:
            process_log_file(log_file, month_data)

        # Summary
        logger.info(f"Consolidated into {len(month_data)} monthly files:")
        for yyyymm in sorted(month_data.keys()):
            logger.info(f"  {yyyymm}: {len(month_data[yyyymm])} lines")

        # Write monthly logs
        if not args.dry_run:
            write_monthly_logs(month_data, output_dir, base_name)
            logger.info("Monthly log files written successfully")
        else:
            logger.info("Dry run - no files written")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
