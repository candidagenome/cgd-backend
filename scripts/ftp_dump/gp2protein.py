#!/usr/bin/env python3
"""
Generate gp2protein mapping file.

This script creates a mapping file from gene/protein IDs to UniProt and
RefSeq accessions for features with GO annotations.

Based on SGD-gp2protein.pl.

Usage:
    python gp2protein.py
    python gp2protein.py --test
    python gp2protein.py --help

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files
    FTP_DIR: FTP directory for output files
    PROJECT_ACRONYM: Project acronym (e.g., CGD, SGD)
"""

import argparse
import gzip
import hashlib
import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
FTP_DIR = Path(os.getenv("FTP_DIR", "/var/ftp/cgd"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")

# Output configuration
DATABASE_NAME = f"Candida Genome Database ({PROJECT_ACRONYM})"
URL = os.getenv("HTM_ROOT_URL", "http://www.candidagenome.org")
EMAIL = os.getenv("CURATORS_EMAIL", "candida-curator@lists.stanford.edu")
FUNDING = "NHGRI at US NIH, grant number 5-P41-HG001315"

# File names
NEW_FILE = f"gp2protein.{PROJECT_ACRONYM.lower()}"
ZIP_FILE = f"{NEW_FILE}.gz"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def get_feature_qualifiers(session) -> dict[str, str]:
    """Get feature qualifiers for all features."""
    query = text(f"""
        SELECT UPPER(f.feature_name), fp.property_value
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_property fp ON f.feature_no = fp.feature_no
        WHERE fp.property_type = 'Feature Qualifier'
    """)

    qualifiers = {}
    for row in session.execute(query).fetchall():
        qualifiers[row[0]] = row[1]

    return qualifiers


def get_dbxref_mappings(session) -> tuple[dict[str, str], dict[str, str]]:
    """
    Get UniProt and RefSeq mappings for features.

    Returns tuple of (uniprot_map, refseq_map) where keys are feature dbxref_ids.
    """
    # Get dbxref IDs (UniProt and RefSeq)
    query = text(f"""
        SELECT d.dbxref_id, d.dbxref_type, f.dbxref_id
        FROM {DB_SCHEMA}.dbxref d
        JOIN {DB_SCHEMA}.dbxref_feat df ON d.dbxref_no = df.dbxref_no
        JOIN {DB_SCHEMA}.feature f ON df.feature_no = f.feature_no
        WHERE d.dbxref_type IN (
            'UniProt/Swiss-Prot ID',
            'UniProt/TrEMBL ID',
            'RefSeq protein version ID'
        )
        AND f.feature_type != 'pseudogene'
        AND f.feature_type NOT LIKE '%RNA'
    """)

    uniprot_map = {}
    refseq_map = {}

    for row in session.execute(query).fetchall():
        dbxref_id, dbxref_type, feat_dbxref_id = row

        if dbxref_type == "RefSeq protein version ID":
            # Remove version number
            dbxref_id = dbxref_id.rsplit(".", 1)[0]
            refseq_map[feat_dbxref_id] = dbxref_id
        else:
            # UniProt
            uniprot_map[feat_dbxref_id] = dbxref_id

    return uniprot_map, refseq_map


def get_annotated_features(session, qualifiers: dict[str, str]) -> set[str]:
    """Get feature dbxref_ids that have non-computational GO annotations."""
    query = text(f"""
        SELECT f.feature_name, f.dbxref_id
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.go_annotation ga ON f.feature_no = ga.feature_no
        WHERE ga.annotation_type != 'computational'
    """)

    annotated = set()
    for row in session.execute(query).fetchall():
        feat_name, dbxref_id = row

        # Skip deleted/merged/dubious
        qualifier = qualifiers.get(feat_name.upper() if feat_name else "", "")
        if any(x in qualifier.lower() for x in ["deleted", "merged", "dubious"]):
            continue

        if dbxref_id:
            annotated.add(dbxref_id)

    return annotated


def compute_checksum(file_path: Path) -> str:
    """Compute checksum of sorted data lines in file."""
    lines = []

    with open(file_path) as f:
        for line in f:
            # Only include lines with tabs (data lines)
            if "\t" in line:
                lines.append(line)

    # Sort and compute checksum
    lines.sort()
    content = "".join(lines)

    return hashlib.md5(content.encode()).hexdigest()


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate gp2protein mapping file"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode - create file but don't update FTP",
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
    log_file = DATA_DIR / "logs" / "gp2protein.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    logger.info("-" * 15)
    logger.info(datetime.now().isoformat())
    logger.info("-" * 15)

    # Output paths
    data_file = DATA_DIR / NEW_FILE
    ftp_dir = FTP_DIR / "go" / "gp2protein"
    ftp_dir.mkdir(parents=True, exist_ok=True)
    zip_file = ftp_dir / ZIP_FILE

    try:
        with SessionLocal() as session:
            # Get reference data
            logger.info("Loading reference data...")
            qualifiers = get_feature_qualifiers(session)
            uniprot_map, refseq_map = get_dbxref_mappings(session)
            annotated = get_annotated_features(session, qualifiers)

            logger.info(f"Found {len(annotated)} annotated features")
            logger.info(f"Found {len(uniprot_map)} UniProt mappings")
            logger.info(f"Found {len(refseq_map)} RefSeq mappings")

            # Write data file
            date_str = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

            with open(data_file, "w") as f:
                # Header
                f.write(f"!Version: 1.0\n")
                f.write(f"!Date: {date_str}\n")
                f.write(f"!{PROJECT_ACRONYM}ID mapped to UniProtKB and RefSeq Accessions\n")
                f.write(f"!FROM: {DATABASE_NAME}\n")
                f.write(f"!URL: {URL}\n")
                f.write(f"!Contact Email: {EMAIL}\n")
                f.write(f"!Funding: {FUNDING}\n")
                f.write("!\n")

                # Data
                for dbxref_id in sorted(annotated):
                    dbxref_upper = dbxref_id.upper()

                    if dbxref_upper in refseq_map:
                        f.write(f"{PROJECT_ACRONYM}:{dbxref_id}\tNCBI_NP:{refseq_map[dbxref_upper]}\n")
                    elif dbxref_upper in uniprot_map:
                        f.write(f"{PROJECT_ACRONYM}:{dbxref_id}\tUniProt:{uniprot_map[dbxref_upper]}\n")

            logger.info(f"Wrote data to {data_file}")

            # Test mode - don't update FTP
            if args.test:
                logger.info("Test mode - skipping FTP update")
                return 0

            # Compare with existing file
            if zip_file.exists():
                # Decompress existing file
                old_file = DATA_DIR / f"{NEW_FILE}_old"
                with gzip.open(zip_file, "rt") as f_in:
                    with open(old_file, "w") as f_out:
                        f_out.write(f_in.read())

                # Compare checksums
                old_checksum = compute_checksum(old_file)
                new_checksum = compute_checksum(data_file)

                old_file.unlink()

                if old_checksum == new_checksum:
                    logger.info("No changes, update not necessary")
                    data_file.unlink()
                    return 0

                logger.info("Changes detected, updating FTP")

            # Gzip and copy to FTP
            gz_data_file = data_file.with_suffix(f".{PROJECT_ACRONYM.lower()}.gz")
            with open(data_file, "rb") as f_in:
                with gzip.open(gz_data_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            shutil.copy(str(gz_data_file), str(zip_file))
            logger.info(f"Updated {zip_file}")

            # Clean up
            data_file.unlink()
            gz_data_file.unlink()

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    logger.info("Exiting...")
    return 0


if __name__ == "__main__":
    sys.exit(main())
