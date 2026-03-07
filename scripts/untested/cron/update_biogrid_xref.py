#!/usr/bin/env python3
"""
Update BioGRID cross-references.

This script downloads interaction data from BioGRID web services and updates
the database cross-references (DBXREF) for features.

Usage:
    python update_biogrid_xref.py --strain C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
    BIOGRID_API_KEY: BioGRID API access key
    BIOGRID_API_URL: BioGRID web service URL
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/tmp"))
BIOGRID_API_KEY = os.getenv("BIOGRID_API_KEY", "")
BIOGRID_API_URL = os.getenv(
    "BIOGRID_API_URL",
    "https://webservice.thebiogrid.org/interactions"
)
PROJECT_NAME = os.getenv("PROJECT_NAME", "CGD")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_strain_config(session, strain_abbrev: str) -> dict | None:
    """
    Get strain configuration from database.

    Args:
        session: Database session
        strain_abbrev: Strain abbreviation

    Returns:
        Dictionary with strain config or None
    """
    query = text(f"""
        SELECT organism_no, taxon_id
        FROM {DB_SCHEMA}.organism
        WHERE organism_abbrev = :strain_abbrev
    """)

    result = session.execute(query, {"strain_abbrev": strain_abbrev}).first()

    if result:
        return {
            "organism_no": result[0],
            "taxon_id": result[1],
        }
    return None


def get_valid_features(session, strain_abbrev: str) -> set[str]:
    """
    Get set of valid feature names for the strain.

    Args:
        session: Database session
        strain_abbrev: Strain abbreviation

    Returns:
        Set of feature names
    """
    query = text(f"""
        SELECT f.feature_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
        JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
        WHERE o.organism_abbrev = :strain_abbrev
        AND fl.is_loc_current = 'Y'
    """)

    result = session.execute(query, {"strain_abbrev": strain_abbrev})
    return {row[0] for row in result if row[0]}


def download_biogrid_data(taxon_id: int, output_file: Path) -> bool:
    """
    Download BioGRID data for a taxon.

    Args:
        taxon_id: NCBI Taxon ID
        output_file: Path to save downloaded data

    Returns:
        True on success, False on failure
    """
    if not BIOGRID_API_KEY:
        logger.error("BIOGRID_API_KEY not set")
        return False

    url = f"{BIOGRID_API_URL}?taxId={taxon_id}&accesskey={BIOGRID_API_KEY}"

    logger.info(f"Downloading data from BioGRID for taxon {taxon_id}")

    try:
        response = requests.get(url, timeout=300)
        response.raise_for_status()

        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w") as f:
            f.write(response.text)

        logger.info(f"Data saved to {output_file}")
        return True

    except requests.RequestException as e:
        logger.error(f"Error downloading BioGRID data: {e}")
        return False


def parse_biogrid_data(
    data_file: Path,
    valid_features: set[str],
) -> tuple[dict[str, int], dict[int, str]]:
    """
    Parse BioGRID data file.

    Args:
        data_file: Path to BioGRID data file
        valid_features: Set of valid feature names

    Returns:
        Tuple of (feature_to_bgid, bgid_to_feature) dictionaries
    """
    feat_to_bgid: dict[str, int] = {}
    bgid_to_feat: dict[int, str] = {}

    if not data_file.exists():
        logger.error(f"Data file not found: {data_file}")
        return feat_to_bgid, bgid_to_feat

    with open(data_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) < 7:
                continue

            # Parse BioGRID data format
            # Columns: ?, ?, ?, BioGRID_ID_1, BioGRID_ID_2, Feature_1, Feature_2
            try:
                bg1 = int(parts[3])
                bg2 = int(parts[4])
                f1 = parts[5]
                f2 = parts[6]

                # Only include features that exist in our database
                if f1 in valid_features:
                    if f1 not in feat_to_bgid:
                        feat_to_bgid[f1] = bg1
                    if bg1 not in bgid_to_feat:
                        bgid_to_feat[bg1] = f1

                if f2 in valid_features:
                    if f2 not in feat_to_bgid:
                        feat_to_bgid[f2] = bg2
                    if bg2 not in bgid_to_feat:
                        bgid_to_feat[bg2] = f2

            except (ValueError, IndexError):
                continue

    logger.info(f"Parsed {len(feat_to_bgid)} feature-BioGRID mappings")
    return feat_to_bgid, bgid_to_feat


def write_xref_file(
    feat_to_bgid: dict[str, int],
    output_file: Path,
) -> None:
    """Write cross-reference file."""
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w") as f:
        f.write(f"{PROJECT_NAME}\tBioGRID\n")
        for feat, bgid in sorted(feat_to_bgid.items()):
            f.write(f"{feat}\t{bgid}\n")

    logger.info(f"Wrote {len(feat_to_bgid)} entries to {output_file}")


def delete_defunct_refs(
    session,
    feat_to_bgid: dict[str, int],
) -> int:
    """
    Delete defunct BioGRID references from database.

    Args:
        session: Database session
        feat_to_bgid: Current feature to BioGRID ID mapping

    Returns:
        Number of deleted references
    """
    # Get existing BioGRID associations
    query = text(f"""
        SELECT df.dbxref_no, f.feature_name, d.dbxref_id
        FROM {DB_SCHEMA}.dbxref_feat df
        JOIN {DB_SCHEMA}.feature f ON df.feature_no = f.feature_no
        JOIN {DB_SCHEMA}.dbxref d ON df.dbxref_no = d.dbxref_no
        WHERE d.source = 'BioGRID'
        AND d.dbxref_type = 'BioGRID ID'
    """)

    result = session.execute(query)

    to_delete = []
    for dbxref_no, feature_name, dbxref_id in result:
        if feature_name in feat_to_bgid:
            # Feature still has BioGRID entry
            if str(feat_to_bgid[feature_name]) != str(dbxref_id):
                # But ID has changed
                to_delete.append(dbxref_no)
                logger.info(f"Deleting: {feature_name} - {dbxref_id} (ID changed)")
        else:
            # Feature no longer in BioGRID
            to_delete.append(dbxref_no)
            logger.info(f"Deleting: {feature_name} - {dbxref_id} (no longer in BioGRID)")

    # Delete from dbxref_feat and dbxref_url
    if to_delete:
        delete_feat = text(f"DELETE FROM {DB_SCHEMA}.dbxref_feat WHERE dbxref_no = :dbxref_no")
        delete_url = text(f"DELETE FROM {DB_SCHEMA}.dbxref_url WHERE dbxref_no = :dbxref_no")

        for dbxref_no in to_delete:
            session.execute(delete_feat, {"dbxref_no": dbxref_no})
            session.execute(delete_url, {"dbxref_no": dbxref_no})

        session.commit()

    return len(to_delete)


def update_biogrid_xref(strain_abbrev: str) -> bool:
    """
    Main function to update BioGRID cross-references.

    Args:
        strain_abbrev: Strain abbreviation

    Returns:
        True on success, False on failure
    """
    # Set up logging for this strain
    log_file = LOG_DIR / f"{strain_abbrev}_BioGRID_update.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Starting BioGRID update for {strain_abbrev}")
    logger.info(f"Start time: {datetime.now()}")

    try:
        with SessionLocal() as session:
            # Get strain config
            strain_config = get_strain_config(session, strain_abbrev)
            if not strain_config:
                logger.error(f"Strain {strain_abbrev} not found in database")
                return False

            taxon_id = strain_config["taxon_id"]
            if not taxon_id:
                logger.error(f"No taxon ID for strain {strain_abbrev}")
                return False

            # Get valid features
            valid_features = get_valid_features(session, strain_abbrev)
            logger.info(f"Found {len(valid_features)} valid features")

            # Download BioGRID data
            biogrid_dir = DATA_DIR / "BioGRID"
            data_file = biogrid_dir / f"{strain_abbrev}_BioGRID_data.tab"

            if not download_biogrid_data(taxon_id, data_file):
                return False

            # Parse data
            feat_to_bgid, bgid_to_feat = parse_biogrid_data(data_file, valid_features)

            # Write xref file
            xref_file = biogrid_dir / f"{strain_abbrev}_BioGRID_DBXREF.tab"
            write_xref_file(feat_to_bgid, xref_file)

            # Delete defunct refs
            deleted = delete_defunct_refs(session, feat_to_bgid)
            logger.info(f"Deleted {deleted} defunct references")

            logger.info(f"Complete: {datetime.now()}")
            return True

    except Exception as e:
        logger.exception(f"Error updating BioGRID xrefs: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update BioGRID cross-references"
    )
    parser.add_argument(
        "--strain",
        required=True,
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )

    args = parser.parse_args()

    success = update_biogrid_xref(args.strain)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
