#!/usr/bin/env python3
"""
Download NCBI journal file (J_Medline.txt).

This script downloads the NCBI journal abbreviations file from the PubMed FTP
server. This file is used for journal name lookups when processing references.

Environment Variables:
    NCBI_FTP_URL: Base URL for NCBI FTP (default: ftp://ftp.ncbi.nih.gov/)
    DATA_DIR: Directory to store downloaded file
    LOG_DIR: Directory for log files (default: /tmp)
"""

import logging
import os
import shutil
import sys
import tempfile
import urllib.request
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
NCBI_FTP_URL = os.getenv("NCBI_FTP_URL", "ftp://ftp.ncbi.nih.gov/")
DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/tmp"))
LOG_FILE = LOG_DIR / "NCBI_J.log"

SOURCE_FILE = NCBI_FTP_URL.rstrip("/") + "/pubmed/J_Medline.txt"
LOCAL_FILE = DATA_DIR / "J_Medline.txt"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def download_file(source_url: str, dest_path: Path) -> bool:
    """
    Download a file from URL to local path.

    Args:
        source_url: URL to download from
        dest_path: Local path to save file

    Returns:
        True on success, False on failure
    """
    logger.info(f"Downloading {source_url}")

    # Download to temp file first
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, dir=DATA_DIR) as tmp:
            temp_path = Path(tmp.name)

            with urllib.request.urlopen(source_url, timeout=300) as response:
                shutil.copyfileobj(response, tmp)

        # Move temp file to final destination
        shutil.move(str(temp_path), str(dest_path))
        logger.info(f"Downloaded successfully to {dest_path}")
        return True

    except urllib.error.URLError as e:
        logger.error(f"Failed to download {source_url}: {e}")
        return False

    except OSError as e:
        logger.error(f"File operation error: {e}")
        return False

    finally:
        # Clean up temp file if it still exists
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def main() -> int:
    """Main entry point."""
    logger.info(f"Program {__file__}: Starting {datetime.now()}")
    logger.info(f"Updating {LOCAL_FILE} from {SOURCE_FILE}")

    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    success = download_file(SOURCE_FILE, LOCAL_FILE)

    logger.info(f"Exiting: {datetime.now()}")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
