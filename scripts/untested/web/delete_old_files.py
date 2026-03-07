#!/usr/bin/env python3
"""
Delete files older than a specified age.

This script checks the age of files and deletes them if they exceed
the specified age limit. Originally used to clean up NewUpdate.gif files.

Original Perl: deleteNewUpdate.pl (Shuai Weng, Mike Cherry, Christopher Lane, June 2003)
Converted to Python: 2024

Usage:
    python delete_old_files.py /path/to/file.gif --days 7
    python delete_old_files.py /path/to/directory/*.tmp --days 30
"""

import argparse
import os
import sys
import time
from pathlib import Path


def check_and_delete(file_path: Path, limit_days: int, dry_run: bool = False) -> bool:
    """
    Check file age and delete if older than limit.

    Args:
        file_path: Path to file to check
        limit_days: Age limit in days
        dry_run: If True, don't actually delete

    Returns:
        True if file was deleted (or would be in dry run)
    """
    if not file_path.exists():
        return False

    # Get file modification time
    mtime = file_path.stat().st_mtime
    current_time = time.time()

    # Calculate age in days
    age_days = (current_time - mtime) / (60 * 60 * 24)

    if age_days > limit_days:
        if dry_run:
            print(f"Would delete: {file_path} (age: {age_days:.1f} days)")
        else:
            file_path.unlink()
            print(f"Deleted: {file_path} (age: {age_days:.1f} days)")
        return True

    return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Delete files older than specified age"
    )
    parser.add_argument(
        "files",
        nargs='+',
        type=Path,
        help="File(s) to check and potentially delete",
    )
    parser.add_argument(
        "-d", "--days",
        type=int,
        default=7,
        help="Age limit in days (default: 7)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting",
    )

    args = parser.parse_args()

    deleted_count = 0
    checked_count = 0

    for file_path in args.files:
        # Handle glob patterns
        if '*' in str(file_path):
            parent = file_path.parent
            pattern = file_path.name
            files = list(parent.glob(pattern))
        else:
            files = [file_path]

        for f in files:
            if f.is_file():
                checked_count += 1
                if check_and_delete(f, args.days, args.dry_run):
                    deleted_count += 1

    print(f"\nChecked {checked_count} files, deleted {deleted_count}")


if __name__ == "__main__":
    main()
