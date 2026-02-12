#!/usr/bin/env python3
"""
Archive website pages for historical record.

This script periodically archives key pages from the website so that
there's a historical record of how the site looked at various intervals.

It downloads sample gene pages and the home page using wget, organizing
them into date-stamped directories.

Environment Variables:
    HTML_ROOT_DIR: Root directory for HTML files
    HTML_ROOT_URL: Base URL of the website (e.g., http://www.candidagenome.org/)
    CGI_ROOT_URL: Base URL for CGI scripts
    LOG_DIR: Directory for log files (default: /tmp)
    CURATOR_EMAIL: Email for error notifications
"""

import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
HTML_ROOT_URL = os.getenv("HTML_ROOT_URL", "http://localhost/")
CGI_ROOT_URL = os.getenv("CGI_ROOT_URL", "http://localhost/cgi-bin/")
LOG_DIR = Path(os.getenv("LOG_DIR", "/tmp"))
LOG_FILE = LOG_DIR / "wget_archive.log"

# Sample genes to archive (orf19 ID -> gene name)
SAMPLE_GENES = {
    "orf19.1321": "HWP1",
    "orf19.7247": "RIM101",
    "orf19.2423": "ZCF11",
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def send_error_email(message: str) -> None:
    """Send error notification email."""
    curator_email = os.getenv("CURATOR_EMAIL")
    if curator_email:
        logger.info(f"Would send error email to {curator_email}: {message}")


def wget(url: str, cwd: Path) -> bool:
    """
    Download a URL using wget.

    Args:
        url: URL to download
        cwd: Working directory for wget

    Returns:
        True on success, False on failure
    """
    logger.info(f"Retrieving {url}")

    cmd = [
        "wget",
        "--page-requisites",
        "--no-clobber",
        "--no-directories",
        "--no-host-directories",
        "--convert-links",
        url,
    ]

    logger.info(f"Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=120,
        )

        if result.returncode != 0:
            logger.error(f"wget failed for {url}: {result.stderr}")
            return False

        return True

    except subprocess.TimeoutExpired:
        logger.error(f"wget timed out for {url}")
        return False
    except Exception as e:
        logger.error(f"Error running wget: {e}")
        return False


def archive_website() -> bool:
    """
    Archive key website pages.

    Returns:
        True on success, False on failure
    """
    today = datetime.now()
    date_str = today.strftime("%Y%m%d")

    archive_dir = HTML_ROOT_DIR / "archive" / date_str
    logger.info(f"Archiving website to {archive_dir}")

    try:
        # Create archive directory
        archive_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory {archive_dir}")

        # Archive home page
        home_url = HTML_ROOT_URL.rstrip("/") + "/index.shtml"
        if wget(home_url, archive_dir):
            # Rename index.shtml to home.shtml
            index_file = archive_dir / "index.shtml"
            if index_file.exists():
                shutil.move(str(index_file), str(archive_dir / "home.shtml"))
                logger.info("Renamed index.shtml to home.shtml")

        # Archive sample gene pages
        cgi_base = CGI_ROOT_URL.rstrip("/")

        for orf19, gene_name in SAMPLE_GENES.items():
            logger.info(f"Retrieving pages for {orf19} ({gene_name})")

            # Define pages to retrieve
            pages = [
                (f"{cgi_base}/locus.pl?locus={gene_name}", f"locus_{gene_name}.shtml"),
                (f"{cgi_base}/singlepageformat?locus={gene_name}", f"singlepageformat_{gene_name}.shtml"),
                (f"{cgi_base}/GO/goAnnotation.pl?locus={gene_name}", f"goAnnotation_{gene_name}.shtml"),
                (f"{cgi_base}/phenotype/phenotype.pl?locus={orf19}", f"phenotype_{gene_name}.shtml"),
                (f"{cgi_base}/reference/litGuide.pl?locus={gene_name}", f"litGuide_{gene_name}.shtml"),
            ]

            for url, new_name in pages:
                if wget(url, archive_dir):
                    # Try to rename the downloaded file
                    # wget saves files based on URL, so we need to find and rename
                    old_name = url.split("/")[-1]
                    old_file = archive_dir / old_name
                    if old_file.exists():
                        shutil.move(str(old_file), str(archive_dir / new_name))
                        logger.info(f"Renamed {old_name} to {new_name}")

        logger.info("Website archiving complete")
        return True

    except Exception as e:
        logger.exception(f"Error archiving website: {e}")
        send_error_email(f"Error archiving website: {e}")
        return False


def main() -> int:
    """Main entry point."""
    success = archive_website()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
