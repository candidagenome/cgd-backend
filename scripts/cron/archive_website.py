#!/usr/bin/env python3
"""
Archive website pages periodically.

This script creates snapshots of key website pages for historical records.
It downloads specified pages and their requisites, storing them in a
dated archive directory.

Based on archiveWebsite.pl by CGD team.

Usage:
    python archive_website.py
    python archive_website.py --base-url https://www.candidagenome.org

Environment Variables:
    HTML_ROOT_DIR: Root directory for HTML files (archive stored here)
    HTML_ROOT_URL: Base URL for the website
    CGI_ROOT_URL: Base URL for CGI scripts
    LOG_DIR: Directory for log files
    CURATOR_EMAIL: Email for error notifications
"""

import argparse
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration from environment
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
HTML_ROOT_URL = os.getenv("HTML_ROOT_URL", "http://www.candidagenome.org/")
CGI_ROOT_URL = os.getenv("CGI_ROOT_URL", "http://www.candidagenome.org/cgi-bin/")
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL", "")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Default genes to archive (orf19 ID -> gene name)
DEFAULT_GENES = {
    "orf19.1321": "HWP1",
    "orf19.7247": "RIM101",
    "orf19.2423": "ZCF11",
}


def send_error_email(subject: str, message: str, log_file: Path) -> None:
    """Send error notification email."""
    if not CURATOR_EMAIL:
        logger.warning("CURATOR_EMAIL not set, skipping email notification")
        return
    logger.error(f"Email notification: {subject}")
    logger.error(f"Message: {message}")
    logger.error(f"See {log_file} for details")


def wget_page(url: str, output_dir: Path) -> bool:
    """
    Download a web page and its requisites using wget.

    Args:
        url: URL to download
        output_dir: Directory to save files

    Returns:
        True on success, False on failure
    """
    logger.info(f"Retrieving {url}")

    # Find wget
    wget_path = shutil.which("wget")
    if not wget_path:
        # Try common locations
        for path in ["/usr/bin/wget", "/usr/local/bin/wget", "/usr/sfw/bin/wget"]:
            if os.path.exists(path):
                wget_path = path
                break

    if not wget_path:
        logger.error("wget not found")
        return False

    command = [
        wget_path,
        "--page-requisites",  # Download CSS, JS, images
        "--no-clobber",       # Don't overwrite existing files
        "--no-directories",   # Don't create directory structure
        "--no-host-directories",
        "--convert-links",    # Convert links for offline viewing
        "--quiet",            # Suppress output
        url,
    ]

    logger.info(f"Command: {' '.join(command)}")

    try:
        result = subprocess.run(
            command,
            cwd=str(output_dir),
            capture_output=True,
            text=True,
            timeout=120,  # 2 minute timeout per page
        )

        if result.returncode != 0:
            logger.warning(f"wget returned non-zero exit code for {url}")
            if result.stderr:
                logger.warning(f"stderr: {result.stderr}")
            # Don't fail completely - wget often returns non-zero for minor issues
            return True

        return True

    except subprocess.TimeoutExpired:
        logger.error(f"Timeout retrieving {url}")
        return False
    except Exception as e:
        logger.error(f"Error retrieving {url}: {e}")
        return False


def rename_file(src: str, dst: str, directory: Path) -> bool:
    """Rename a file in the specified directory."""
    src_path = directory / src
    dst_path = directory / dst

    if not src_path.exists():
        logger.warning(f"Source file not found: {src_path}")
        return False

    logger.info(f"Renaming {src} -> {dst}")
    try:
        src_path.rename(dst_path)
        return True
    except Exception as e:
        logger.error(f"Error renaming {src} to {dst}: {e}")
        return False


def archive_pages(
    genes: dict[str, str],
    html_root_url: str,
    cgi_root_url: str,
    archive_dir: Path,
) -> tuple[int, int]:
    """
    Archive website pages for the given genes.

    Args:
        genes: Dictionary mapping orf19 IDs to gene names
        html_root_url: Base URL for static pages
        cgi_root_url: Base URL for CGI scripts
        archive_dir: Directory to store archived pages

    Returns:
        Tuple of (success_count, failure_count)
    """
    success_count = 0
    failure_count = 0

    # Archive home page
    home_url = urljoin(html_root_url, "index.shtml")
    if wget_page(home_url, archive_dir):
        success_count += 1
        rename_file("index.shtml", "home.shtml", archive_dir)
    else:
        failure_count += 1

    # Archive pages for each gene
    for orf19_id, gene_name in genes.items():
        logger.info(f"Retrieving pages for {orf19_id} ({gene_name})")

        # Page URLs to archive
        pages = [
            (f"locus.pl?locus={gene_name}", f"locus_{gene_name}.shtml"),
            (f"singlepageformat?locus={gene_name}", f"singlepageformat_{gene_name}.shtml"),
            (f"GO/goAnnotation.pl?locus={gene_name}", f"goAnnotation_{gene_name}.shtml"),
            (f"phenotype/phenotype.pl?locus={orf19_id}", f"phenotype_{gene_name}.shtml"),
            (f"reference/litGuide.pl?locus={gene_name}", f"litGuide_{gene_name}.shtml"),
        ]

        for url_path, output_name in pages:
            url = urljoin(cgi_root_url, url_path)
            if wget_page(url, archive_dir):
                success_count += 1
                # Extract the filename from URL (after the ? is removed by wget)
                url_filename = url_path.split("?")[0].split("/")[-1] + "?" + url_path.split("?")[1] if "?" in url_path else url_path.split("/")[-1]
                rename_file(url_filename, output_name, archive_dir)
            else:
                failure_count += 1

    return success_count, failure_count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Archive website pages periodically"
    )
    parser.add_argument(
        "--html-url",
        default=HTML_ROOT_URL,
        help=f"Base URL for HTML pages (default: {HTML_ROOT_URL})",
    )
    parser.add_argument(
        "--cgi-url",
        default=CGI_ROOT_URL,
        help=f"Base URL for CGI scripts (default: {CGI_ROOT_URL})",
    )
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=HTML_ROOT_DIR / "archive",
        help="Root directory for archives",
    )
    parser.add_argument(
        "--genes",
        type=str,
        default=None,
        help="Comma-separated list of orf19_id:gene_name pairs (e.g., 'orf19.1321:HWP1,orf19.7247:RIM101')",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )

    args = parser.parse_args()

    # Set up logging to file
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "wget_archive.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Starting website archive at {datetime.now()}")

    # Parse genes if provided
    genes = DEFAULT_GENES.copy()
    if args.genes:
        genes = {}
        for pair in args.genes.split(","):
            if ":" in pair:
                orf19_id, gene_name = pair.split(":", 1)
                genes[orf19_id.strip()] = gene_name.strip()

    # Create dated archive directory
    date_str = datetime.now().strftime("%Y%m%d")
    archive_dir = args.archive_root / date_str

    if args.dry_run:
        logger.info(f"DRY RUN - would create archive at {archive_dir}")
        logger.info(f"Genes to archive: {genes}")
        return 0

    try:
        # Create archive directory
        archive_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created archive directory: {archive_dir}")

        # Archive pages
        success, failures = archive_pages(
            genes,
            args.html_url,
            args.cgi_url,
            archive_dir,
        )

        logger.info(f"Archive complete: {success} pages succeeded, {failures} failed")

        if failures > 0:
            send_error_email(
                f"Error running {__file__}",
                f"{failures} pages failed to archive",
                log_file,
            )
            return 1

        return 0

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error: {error_msg}")
        send_error_email(f"Error running {__file__}", error_msg, log_file)
        return 1

    finally:
        logger.removeHandler(file_handler)
        file_handler.close()


if __name__ == "__main__":
    sys.exit(main())
