#!/usr/bin/env python3
"""
Download CGOB and related files.

This script downloads files needed for CGOB (Candida Gene Order Browser) analysis:
- CGOB cluster files and protein sequences
- YGOB (Yeast Gene Order Browser) cluster files and protein sequences
- SGD protein, coding, gene, and feature files

Based on downloadCGOBfiles.pl.

Usage:
    python download_cgob_files.py
    python download_cgob_files.py --debug

Environment Variables:
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
"""

import argparse
import logging
import os
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

# Load environment variables
load_dotenv()

# Configuration from environment
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))

# CGOB configuration
CGOB_DATA_DIR = DATA_DIR / "CGOB"
CGOB_SEQ_DIR = CGOB_DATA_DIR / "sequences"

# Source URLs (these would typically come from configuration)
CGOB_BASE_URL = os.getenv("CGOB_BASE_URL", "http://cgob.ucd.ie/")
YGOB_BASE_URL = os.getenv("YGOB_BASE_URL", "http://ygob.ucd.ie/")
SGD_BASE_URL = os.getenv("SGD_BASE_URL", "https://downloads.yeastgenome.org/")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# Source and local file mappings
CGOB_FILES = {
    "cgob_clusters": {
        "source": "data/Pillars.tab",
        "local": "cgob_clusters.tab",
    },
    "cgob_proteins": {
        "source": "data/proteins.fasta",
        "local": "cgob_proteins.fasta",
    },
    "cgob_rnas": {
        "source": "data/rnas.fasta",
        "local": "cgob_rnas.fasta",
    },
}

YGOB_FILES = {
    "ygob_clusters": {
        "source": "data/Pillars.tab",
        "local": "ygob_clusters.tab",
    },
    "ygob_proteins": {
        "source": "data/proteins.fasta",
        "local": "ygob_proteins.fasta",
    },
}

SGD_FILES = {
    "sgd_proteins": {
        "source": "sequence/S288C_reference/orf_protein/orf_trans_all.fasta.gz",
        "local": "S_cerevisiae/S_cerevisiae_protein.fasta.gz",
    },
    "sgd_coding": {
        "source": "sequence/S288C_reference/orf_dna/orf_coding_all.fasta.gz",
        "local": "S_cerevisiae/S_cerevisiae_coding.fasta.gz",
    },
    "sgd_genes": {
        "source": "sequence/S288C_reference/orf_dna/orf_genomic_all.fasta.gz",
        "local": "S_cerevisiae/S_cerevisiae_gene.fasta.gz",
    },
    "sgd_g1000": {
        "source": "sequence/S288C_reference/orf_dna/orf_genomic_1000_all.fasta.gz",
        "local": "S_cerevisiae/S_cerevisiae_g1000.fasta.gz",
    },
    "sgd_features": {
        "source": "curation/chromosomal_feature/SGD_features.tab",
        "local": "S_cerevisiae/SGD_features.tab",
    },
}


def download_file(url: str, local_path: Path) -> bool:
    """Download a file from URL."""
    try:
        logger.info(f"Downloading {url}")
        logger.info(f"  -> {local_path}")

        # Create parent directory if needed
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Remove existing file
        if local_path.exists():
            local_path.unlink()

        urllib.request.urlretrieve(url, local_path)

        logger.info(f"  Downloaded successfully")
        return True

    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Download CGOB and related files"
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
    log_file = LOG_DIR / "CGOB_download.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Add file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    logger.info(f"Starting CGOB file downloads at {datetime.now()}")

    # Create data directories
    CGOB_DATA_DIR.mkdir(parents=True, exist_ok=True)
    CGOB_SEQ_DIR.mkdir(parents=True, exist_ok=True)
    (CGOB_SEQ_DIR / "S_cerevisiae").mkdir(parents=True, exist_ok=True)

    errors = []

    # Download CGOB files
    logger.info("Downloading CGOB files...")
    for name, info in CGOB_FILES.items():
        url = CGOB_BASE_URL + info["source"]
        local_path = CGOB_DATA_DIR / info["local"]

        if not download_file(url, local_path):
            errors.append(f"Failed to download {name}")

    # Download YGOB files
    logger.info("Downloading YGOB files...")
    for name, info in YGOB_FILES.items():
        url = YGOB_BASE_URL + info["source"]
        local_path = CGOB_DATA_DIR / info["local"]

        if not download_file(url, local_path):
            errors.append(f"Failed to download {name}")

    # Download SGD files
    logger.info("Downloading SGD files...")
    for name, info in SGD_FILES.items():
        url = SGD_BASE_URL + info["source"]
        local_path = CGOB_SEQ_DIR / info["local"]

        if not download_file(url, local_path):
            errors.append(f"Failed to download {name}")

    logger.info(f"Downloads complete at {datetime.now()}")

    if errors:
        logger.error("Errors encountered:")
        for error in errors:
            logger.error(f"  {error}")
        return 1

    logger.info("All downloads completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
