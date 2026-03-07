#!/usr/bin/env python3
"""
Update dbxref descriptions with SGD gene names.

This script downloads the SGD features file and updates the description
field in the dbxref table with the gene name or feature name from SGD.

Based on updateWithSGD.pl by CGD team.

Usage:
    python update_with_sgd.py --sgd-file /tmp/SGD_features.tab
    python update_with_sgd.py --download --update

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    SGD_DOWNLOAD_URL: URL for SGD features download
    LOG_DIR: Directory for log files
"""

import argparse
import csv
import logging
import os
import sys
from pathlib import Path
from urllib.request import urlretrieve

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
SGD_DOWNLOAD_URL = os.getenv(
    "SGD_DOWNLOAD_URL",
    "https://downloads.yeastgenome.org/curation/chromosomal_feature/SGD_features.tab"
)
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
ADMIN_USER = os.getenv("ADMIN_USER", "admin")

# SGD features file columns
SGD_COLUMNS = [
    "sgdid",
    "type",
    "qualifier",
    "feature_name",
    "gene_name",
    "alias",
    "parent_feature_name",
    "sgdid2",
    "chromosome",
    "start_coordinate",
    "stop_coordinate",
    "strand",
    "position",
    "coordinate_version",
    "sequence_version",
    "description",
]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def download_sgd_features(output_file: Path) -> bool:
    """Download SGD features file."""
    try:
        logger.info(f"Downloading SGD features from {SGD_DOWNLOAD_URL}")
        urlretrieve(SGD_DOWNLOAD_URL, output_file)
        logger.info(f"Downloaded to {output_file}")
        return True
    except Exception as e:
        logger.error(f"Error downloading SGD features: {e}")
        return False


def read_sgd_features(sgd_file: Path) -> list[dict]:
    """Read SGD features file."""
    features = []

    with open(sgd_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            if len(row) < len(SGD_COLUMNS):
                # Pad with empty strings
                row.extend([""] * (len(SGD_COLUMNS) - len(row)))

            feature = dict(zip(SGD_COLUMNS, row))
            features.append(feature)

    return features


def check_dbxref_exists(session, sgdid: str) -> bool:
    """Check if a dbxref exists for the given SGDID."""
    query = text(f"""
        SELECT 1 FROM {DB_SCHEMA}.dbxref
        WHERE source LIKE 'SGD%' AND dbxref_id = :sgdid
    """)
    result = session.execute(query, {"sgdid": sgdid}).fetchone()
    return result is not None


def update_dbxref_description(session, sgdid: str, description: str) -> bool:
    """Update the description for a dbxref."""
    query = text(f"""
        UPDATE {DB_SCHEMA}.dbxref
        SET description = :description
        WHERE source LIKE 'SGD%' AND dbxref_id = :sgdid
    """)
    result = session.execute(query, {"sgdid": sgdid, "description": description})
    return result.rowcount > 0


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update dbxref descriptions with SGD gene names"
    )
    parser.add_argument(
        "--sgd-file",
        type=Path,
        default=None,
        help="Path to SGD features file (will download if not provided)",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download fresh SGD features file",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Actually update the database (default: dry run)",
    )

    args = parser.parse_args()

    # Determine SGD file path
    if args.sgd_file:
        sgd_file = args.sgd_file
    else:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        sgd_file = DATA_DIR / "SGD_features.tab"

    # Download if requested or file doesn't exist
    if args.download or not sgd_file.exists():
        if not download_sgd_features(sgd_file):
            return 1

    if not sgd_file.exists():
        logger.error(f"SGD features file not found: {sgd_file}")
        return 1

    logger.info(f"Reading SGD features from {sgd_file}")

    try:
        # Read features
        features = read_sgd_features(sgd_file)
        logger.info(f"Read {len(features)} features")

        with SessionLocal() as session:
            update_count = 0
            not_found_count = 0

            for feature in features:
                sgdid = feature.get("sgdid", "")
                gene_name = feature.get("gene_name", "")
                feature_name = feature.get("feature_name", "")

                if not sgdid:
                    continue

                # Use gene_name if available, otherwise feature_name
                description = gene_name if gene_name else feature_name

                if not description:
                    continue

                # Check if exists
                if check_dbxref_exists(session, sgdid):
                    if args.update:
                        if update_dbxref_description(session, sgdid, description):
                            update_count += 1
                            logger.debug(f"Updated {sgdid} with {description}")
                    else:
                        logger.debug(f"Would update {sgdid} with {description}")
                        update_count += 1
                else:
                    not_found_count += 1
                    logger.debug(f"{sgdid} not present in database")

            if args.update:
                session.commit()
                logger.info(f"Updated {update_count} rows")
            else:
                logger.info(f"Dry run: would update {update_count} rows")

            logger.info(f"{not_found_count} SGDIDs not found in database")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
