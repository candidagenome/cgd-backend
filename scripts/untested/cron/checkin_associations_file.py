#!/usr/bin/env python3
"""
Check in gene associations file to GO repository.

This script checks if the gene_association file has changed and if so,
commits it to the GO svn repository. It filters out IEA annotations
older than one year before committing.

Based on checkInAssociationsFile.pl

Usage:
    python checkin_associations_file.py

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (e.g., CGD)
    HTML_ROOT_DIR: HTML root directory
    GO_CVS_DIR: GO SVN checkout directory
    TMP_DIR: Temporary directory
    LOG_DIR: Log directory
    ADMIN_USER: Admin username
    SVN_PATH: Path to svn executable
"""

import argparse
import gzip
import hashlib
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Load environment variables
load_dotenv()

# Configuration
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
GO_CVS_DIR = Path(os.getenv("GO_CVS_DIR", "/var/data/go"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
SVN_PATH = os.getenv("SVN_PATH", "svn")
IS_PRODUCTION = os.getenv("IS_PRODUCTION", "false").lower() == "true"

# Timeout for SVN operations (seconds)
SVN_TIMEOUT = 300

# GAF column indices (0-based)
GAF_EVIDENCE_COL = 6
GAF_DATE_COL = 13

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def calculate_checksum(file_path: Path) -> str:
    """Calculate MD5 checksum of a file."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    hash_md5 = hashlib.md5()

    # Handle gzipped files
    if str(file_path).endswith(".gz"):
        with gzip.open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
    else:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)

    return hash_md5.hexdigest()


def run_svn_command(cmd: list[str], cwd: Path, timeout: int = SVN_TIMEOUT) -> tuple[int, str]:
    """
    Run an SVN command with timeout.

    Args:
        cmd: Command and arguments
        cwd: Working directory
        timeout: Timeout in seconds

    Returns:
        Tuple of (return_code, output)
    """
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.returncode, result.stdout + result.stderr

    except subprocess.TimeoutExpired:
        logger.error(f"SVN command timed out: {' '.join(cmd)}")
        return -1, "Timeout"


def remove_old_ieas(input_file: Path, output_file: Path) -> None:
    """
    Remove IEA annotations older than one year.

    Args:
        input_file: Input gzipped gene association file
        output_file: Output gzipped file with old IEAs removed
    """
    # Calculate date limit (one year ago)
    one_year_ago = datetime.now() - timedelta(days=365)
    limit_date = int(one_year_ago.strftime("%Y%m%d"))

    logger.info(f"Filtering out IEA annotations before {limit_date}")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(input_file, "rt") as f_in:
        with gzip.open(output_file, "wt") as f_out:
            for line in f_in:
                # Pass through comment lines
                if line.startswith("!"):
                    f_out.write(line)
                    continue

                cols = line.strip().split("\t")

                # Skip if not enough columns
                if len(cols) <= GAF_DATE_COL:
                    f_out.write(line)
                    continue

                evidence = cols[GAF_EVIDENCE_COL] if len(cols) > GAF_EVIDENCE_COL else ""
                date_str = cols[GAF_DATE_COL] if len(cols) > GAF_DATE_COL else ""

                # Skip old IEA annotations
                if evidence == "IEA":
                    try:
                        annotation_date = int(date_str)
                        if annotation_date <= limit_date:
                            continue
                    except ValueError:
                        pass  # Keep if date parsing fails

                f_out.write(line)


def validate_gaf_file(gaf_file: Path, go_dir: Path) -> tuple[bool, str]:
    """
    Validate GAF file using GO filter script.

    Args:
        gaf_file: Path to GAF file
        go_dir: GO repository directory

    Returns:
        Tuple of (is_valid, error_message)
    """
    filter_script = go_dir / "software" / "utilities" / "filter-gene-association.pl"

    if not filter_script.exists():
        logger.warning(f"Filter script not found: {filter_script}")
        return True, ""  # Skip validation if script not available

    try:
        result = subprocess.run(
            ["perl", str(filter_script), "-i", str(gaf_file), "-e"],
            capture_output=True,
            text=True,
            timeout=300,
        )

        output = result.stdout + result.stderr

        # Check for errors in output
        for line in output.split("\n"):
            if "TOTAL ERRORS =" in line:
                import re
                match = re.search(r"TOTAL ERRORS = (\d+)", line)
                if match and int(match.group(1)) > 0:
                    return False, output

        return True, ""

    except Exception as e:
        logger.error(f"Error running filter script: {e}")
        return True, ""  # Don't block if script fails


def checkin_associations_file(dry_run: bool = False) -> bool:
    """
    Main function to check in gene associations file.

    Args:
        dry_run: If True, don't actually commit

    Returns:
        True on success, False on failure
    """
    # Set up logging
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "GO_checkin.log"

    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Starting gene association file check-in for {PROJECT_ACRONYM}")

    # Check if running in production
    if not IS_PRODUCTION and not dry_run:
        logger.warning("Not running in production mode, skipping actual commit")
        dry_run = True

    # Check current user
    current_user = os.getenv("USER", "")
    if current_user != ADMIN_USER and not dry_run:
        logger.error(f"Must be run as {ADMIN_USER}, current user: {current_user}")
        return False

    try:
        # Define file paths
        file_name = f"gene_association.{PROJECT_ACRONYM.lower()}.gz"
        go_dir = GO_CVS_DIR / "go" / PROJECT_ACRONYM.lower()
        old_file = go_dir / "gene-associations" / "submission" / file_name
        new_file = HTML_ROOT_DIR / "download" / "go" / file_name

        if not new_file.exists():
            logger.error(f"New gene association file not found: {new_file}")
            return False

        if not old_file.exists():
            logger.error(f"Old gene association file not found: {old_file}")
            return False

        # Change to GO directory
        os.chdir(go_dir)

        # Update SVN files
        for update_path in [
            "ontology/gene_ontology.obo",
            "doc/GO.xrf_abbs",
            "software/utilities/filter-gene-association.pl",
        ]:
            logger.info(f"Updating {update_path}")
            ret, output = run_svn_command(
                [SVN_PATH, "update", update_path],
                go_dir,
            )
            if ret != 0:
                logger.error(f"SVN update failed for {update_path}: {output}")
                # Continue anyway, not critical

        # Create filtered file (remove old IEAs)
        tmp_file = TMP_DIR / file_name
        remove_old_ieas(new_file, tmp_file)

        # Validate the filtered file
        is_valid, error_msg = validate_gaf_file(tmp_file, GO_CVS_DIR / "go")
        if not is_valid:
            logger.error(f"GAF validation failed:\n{error_msg}")
            return False

        # Compare checksums
        old_checksum = calculate_checksum(old_file)
        new_checksum = calculate_checksum(tmp_file)

        logger.info(f"Old checksum: {old_checksum}")
        logger.info(f"New checksum: {new_checksum}")

        if old_checksum == new_checksum:
            logger.info("Files are identical, no commit needed")
            tmp_file.unlink()
            return True

        # Check file modification times
        new_mtime = new_file.stat().st_mtime
        old_mtime = old_file.stat().st_mtime

        if old_mtime >= new_mtime:
            logger.info("Old file is newer or same age, skipping commit")
            tmp_file.unlink()
            return True

        # Copy filtered file to old location
        if not dry_run:
            shutil.copy(tmp_file, old_file)
            logger.info(f"Copied {tmp_file} to {old_file}")

        # Commit the file
        date_str = datetime.now().strftime("%Y%m%d")
        commit_path = f"gene-associations/submission/{file_name}"

        if dry_run:
            logger.info(f"DRY RUN: Would commit {commit_path} with message: {date_str}")
        else:
            ret, output = run_svn_command(
                [SVN_PATH, "commit", "-m", date_str, commit_path],
                go_dir,
            )
            if ret != 0:
                logger.error(f"SVN commit failed: {output}")
                return False

            logger.info(f"Successfully committed {commit_path}")

        # Clean up
        if tmp_file.exists():
            tmp_file.unlink()

        return True

    except Exception as e:
        logger.exception(f"Error during check-in: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Check in gene associations file to GO repository"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually commit, just show what would be done",
    )

    args = parser.parse_args()

    success = checkin_associations_file(dry_run=args.dry_run)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
