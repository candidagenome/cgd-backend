#!/usr/bin/env python3
"""
Update Ocelot file for download.

This script updates the Ocelot file from version control and copies it
to the download directory as a gzipped file.

Original Perl: updateOcelotFileForDownload.pl (Prachi Shah, Oct 2008)
Converted to Python: 2024

Usage:
    python update_ocelot_file.py
    python update_ocelot_file.py --dry-run
    python update_ocelot_file.py --ptools-dir /path/to/ptools-local
"""

import argparse
import gzip
import logging
import os
import shutil
import smtplib
import subprocess
import sys
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_PTOOLS_DIR = os.getenv("PTOOLS_LOCAL_DIR", "/share/cgd/ptools-local")
DEFAULT_OCELOT_FILE = "pgdbs/user/calbicyc/12.0/kb/calbibase.ocelot"
DEFAULT_DOWNLOAD_DIR = os.getenv("HTML_ROOT_DIR", "/share/www-data_cgd/prod/html")
DEFAULT_LOG_DIR = os.getenv("LOG_DIR", "/share/www-data_cgd/prod/logs")
DEFAULT_CURATORS_EMAIL = os.getenv("CURATORS_EMAIL", "cgd-curators@lists.stanford.edu")
DEFAULT_SMTP_SERVER = os.getenv("SMTP_SERVER", "localhost")
DEFAULT_VCS_TIMEOUT = 300  # 5 minutes


def setup_logging(verbose: bool = False, log_file: Path = None) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler()]

    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def send_error_email(
    message: str,
    curators_email: str = DEFAULT_CURATORS_EMAIL,
    smtp_server: str = DEFAULT_SMTP_SERVER,
) -> None:
    """
    Send error notification email.

    Args:
        message: Error message
        curators_email: Email recipient
        smtp_server: SMTP server
    """
    try:
        msg = MIMEText(message)
        msg['Subject'] = 'updateOcelotFileForDownload Error'
        msg['From'] = 'CGD Admin <cgd-admin@stanford.edu>'
        msg['To'] = curators_email

        with smtplib.SMTP(smtp_server) as server:
            server.sendmail(msg['From'], [curators_email], msg.as_string())
        logger.info(f"Error notification sent to {curators_email}")
    except Exception as e:
        logger.error(f"Failed to send error email: {e}")


def update_from_vcs(
    ptools_dir: Path,
    ocelot_file: str,
    log_file: Path,
    timeout: int = DEFAULT_VCS_TIMEOUT,
) -> bool:
    """
    Update ocelot file from version control.

    Args:
        ptools_dir: Path to ptools-local directory
        ocelot_file: Relative path to ocelot file
        log_file: Log file for VCS output
        timeout: Timeout in seconds

    Returns:
        True if successful
    """
    original_dir = os.getcwd()

    try:
        os.chdir(ptools_dir)

        # Set CVS_RSH for SSH
        os.environ['CVS_RSH'] = 'ssh'

        # Try git first (modern), fall back to cvs
        git_dir = ptools_dir / '.git'
        if git_dir.exists():
            logger.info("Updating via git...")
            cmd = ['git', 'pull']
        else:
            logger.info("Updating via cvs...")
            cvs_cmd = os.getenv('CVS_CMD', 'cvs -q')
            cmd = cvs_cmd.split() + ['update', ocelot_file]

        with open(log_file, 'w') as log:
            result = subprocess.run(
                cmd,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=timeout,
            )

        if result.returncode != 0:
            logger.error(f"VCS update failed. See {log_file} for details.")
            return False

        logger.info("VCS update successful")
        return True

    except subprocess.TimeoutExpired:
        logger.error(f"VCS process timed out after {timeout} seconds. See {log_file}")
        return False
    except Exception as e:
        logger.error(f"Error during VCS update: {e}")
        return False
    finally:
        os.chdir(original_dir)


def compress_and_copy(
    source_file: Path,
    dest_file: Path,
) -> bool:
    """
    Compress source file and copy to destination.

    Args:
        source_file: Source ocelot file
        dest_file: Destination gzipped file

    Returns:
        True if successful
    """
    try:
        # Ensure destination directory exists
        dest_file.parent.mkdir(parents=True, exist_ok=True)

        # Compress and copy
        with open(source_file, 'rb') as f_in:
            with gzip.open(dest_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        logger.info(f"Compressed and copied to {dest_file}")
        return True

    except Exception as e:
        logger.error(f"Error compressing file: {e}")
        return False


def update_ocelot_file(
    ptools_dir: Path = Path(DEFAULT_PTOOLS_DIR),
    ocelot_file: str = DEFAULT_OCELOT_FILE,
    download_dir: Path = Path(DEFAULT_DOWNLOAD_DIR),
    log_dir: Path = Path(DEFAULT_LOG_DIR),
    curators_email: str = DEFAULT_CURATORS_EMAIL,
    dry_run: bool = False,
) -> bool:
    """
    Update ocelot file for download.

    Args:
        ptools_dir: Path to ptools-local directory
        ocelot_file: Relative path to ocelot file
        download_dir: HTML download directory
        log_dir: Log directory
        curators_email: Email for error notifications
        dry_run: If True, don't actually update

    Returns:
        True if successful
    """
    source_path = ptools_dir / ocelot_file
    dest_path = download_dir / 'download' / 'pathways' / 'calbibase.ocelot.gz'
    vcs_log = log_dir / 'ocelot_cvsupdate.log'

    if dry_run:
        logger.info("Dry run mode - would perform:")
        logger.info(f"  1. Update {source_path} from VCS")
        logger.info(f"  2. Compress and copy to {dest_path}")
        return True

    # Update from VCS
    if not update_from_vcs(ptools_dir, ocelot_file, vcs_log):
        send_error_email(
            f"VCS update of {source_path} failed.\n\nSee {vcs_log} for details.",
            curators_email,
        )
        return False

    # Verify source file exists
    if not source_path.exists():
        msg = f"Ocelot file not found: {source_path}"
        logger.error(msg)
        send_error_email(msg, curators_email)
        return False

    # Compress and copy
    if not compress_and_copy(source_path, dest_path):
        send_error_email(
            f"Error compressing/copying ocelot file to {dest_path}",
            curators_email,
        )
        return False

    logger.info("Ocelot file updated successfully")
    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update Ocelot file for download"
    )
    parser.add_argument(
        "--ptools-dir",
        type=Path,
        default=Path(DEFAULT_PTOOLS_DIR),
        help=f"ptools-local directory (default: {DEFAULT_PTOOLS_DIR})",
    )
    parser.add_argument(
        "--ocelot-file",
        default=DEFAULT_OCELOT_FILE,
        help=f"Relative path to ocelot file (default: {DEFAULT_OCELOT_FILE})",
    )
    parser.add_argument(
        "--download-dir",
        type=Path,
        default=Path(DEFAULT_DOWNLOAD_DIR),
        help=f"HTML root directory (default: {DEFAULT_DOWNLOAD_DIR})",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=Path(DEFAULT_LOG_DIR),
        help=f"Log directory (default: {DEFAULT_LOG_DIR})",
    )
    parser.add_argument(
        "--email",
        default=DEFAULT_CURATORS_EMAIL,
        help=f"Error notification email (default: {DEFAULT_CURATORS_EMAIL})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without doing it",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    success = update_ocelot_file(
        ptools_dir=args.ptools_dir,
        ocelot_file=args.ocelot_file,
        download_dir=args.download_dir,
        log_dir=args.log_dir,
        curators_email=args.email,
        dry_run=args.dry_run,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
