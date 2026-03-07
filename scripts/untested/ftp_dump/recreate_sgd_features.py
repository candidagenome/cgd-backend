#!/usr/bin/env python3
"""
Recreate SGD features file.

This script runs after sequence update to recreate the SGD_features.tab
file on the FTP site.

Based on recreate_sgdfeatures.pl.

Usage:
    python recreate_sgd_features.py
    python recreate_sgd_features.py --debug
    python recreate_sgd_features.py --help

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    FTP_DIR: FTP directory for output files
    LOG_DIR: Directory for log files
    PROJECT_ACRONYM: Project acronym (e.g., CGD, SGD)

Output Files:
    SGD_features.tab - Tab-delimited feature information file
"""

import argparse
import logging
import os
import sys
import time
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
FTP_DIR = Path(os.getenv("FTP_DIR", "/var/ftp/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")

# Output directory
DATA_DIR = FTP_DIR / "data_download" / "chromosomal_feature"

# Maximum retry attempts for NFS issues
MAX_ATTEMPTS = 3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def get_sgd_features(session) -> list[dict]:
    """
    Get SGD feature information from the database.

    Returns list of dicts with feature information including:
    - feature_name, gene_name, aliases, feature_type
    - chromosome, start_coord, stop_coord, strand
    - sgdid, qualifier, headline, gene_product
    """
    # Get feature qualifiers first
    qualifier_query = text(f"""
        SELECT f.feature_name, fp.property_value
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_property fp ON f.feature_no = fp.feature_no
        WHERE fp.property_type = 'Feature Qualifier'
    """)

    qualifiers = {}
    for row in session.execute(qualifier_query).fetchall():
        feat_name, qualifier = row
        if feat_name:
            qualifiers[feat_name.upper()] = qualifier

    # Get aliases
    alias_query = text(f"""
        SELECT f.feature_name, a.alias_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_alias fa ON f.feature_no = fa.feature_no
        JOIN {DB_SCHEMA}.alias a ON fa.alias_no = a.alias_no
    """)

    aliases = {}
    for row in session.execute(alias_query).fetchall():
        feat_name, alias = row
        if feat_name:
            key = feat_name.upper()
            if key not in aliases:
                aliases[key] = []
            aliases[key].append(alias)

    # Get gene products
    gp_query = text(f"""
        SELECT f.feature_name, gp.gene_product
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_gp fg ON f.feature_no = fg.feature_no
        JOIN {DB_SCHEMA}.gene_product gp ON fg.gene_product_no = gp.gene_product_no
    """)

    gene_products = {}
    for row in session.execute(gp_query).fetchall():
        feat_name, gp = row
        if feat_name:
            key = feat_name.upper()
            if key not in gene_products:
                gene_products[key] = []
            gene_products[key].append(gp)

    # Get main feature data
    feature_query = text(f"""
        SELECT DISTINCT f.feature_name, f.gene_name, f.feature_type,
               f.dbxref_id, f.headline,
               fl.start_coord, fl.stop_coord, fl.strand,
               p.feature_name as parent_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
        JOIN {DB_SCHEMA}.feat_relationship fr ON f.feature_no = fr.child_feature_no
        JOIN {DB_SCHEMA}.feature p ON fr.parent_feature_no = p.feature_no
        WHERE p.feature_type = 'chromosome'
        AND fr.rank = 1
        ORDER BY p.feature_name, fl.start_coord
    """)

    features = []
    for row in session.execute(feature_query).fetchall():
        (feat_name, gene_name, feat_type, dbxref_id, headline,
         start_coord, stop_coord, strand, parent_name) = row

        if not feat_name:
            continue

        key = feat_name.upper()

        # Get qualifier for this feature
        qualifier = qualifiers.get(key, "")

        # Get aliases
        feat_aliases = aliases.get(key, [])
        alias_str = "|".join(sorted(feat_aliases)) if feat_aliases else ""

        # Get gene products
        feat_gps = gene_products.get(key, [])
        gp_str = "|".join(feat_gps) if feat_gps else ""

        features.append({
            "feature_name": feat_name,
            "gene_name": gene_name or "",
            "aliases": alias_str,
            "feature_type": feat_type,
            "chromosome": parent_name,
            "start_coord": start_coord,
            "stop_coord": stop_coord,
            "strand": strand,
            "sgdid": dbxref_id or "",
            "qualifier": qualifier,
            "headline": headline or "",
            "gene_product": gp_str,
        })

    return features


def write_features_file(features: list[dict], output_file: Path) -> int:
    """
    Write features to tab-delimited file.

    Returns count of features written.
    """
    count = 0
    with open(output_file, "w") as f:
        for feat in features:
            # Build row matching SGD features format
            # Columns: SGDID, feature_type, qualifier, feature_name, gene_name,
            #          aliases, chromosome, start, stop, strand, gene_product, headline
            row = [
                feat["sgdid"],
                feat["feature_type"],
                feat["qualifier"],
                feat["feature_name"],
                feat["gene_name"],
                feat["aliases"],
                feat["chromosome"],
                str(feat["start_coord"]) if feat["start_coord"] else "",
                str(feat["stop_coord"]) if feat["stop_coord"] else "",
                feat["strand"] or "",
                feat["gene_product"],
                feat["headline"],
            ]
            f.write("\t".join(row) + "\n")
            count += 1

    return count


def retrieve_data(session, output_file: Path) -> bool:
    """
    Retrieve data with retry logic for NFS issues.

    Returns True on success, False on failure.
    """
    attempts = MAX_ATTEMPTS

    while attempts > 0:
        try:
            logger.info(f"Retrieving feature data (attempt {MAX_ATTEMPTS - attempts + 1})...")
            features = get_sgd_features(session)
            logger.info(f"Found {len(features)} features")

            count = write_features_file(features, output_file)
            logger.info(f"Wrote {count} features to {output_file}")

            return True

        except Exception as e:
            attempts -= 1
            logger.error(f"Problem generating {output_file}: {e}")

            if attempts > 0:
                logger.info(f"Will retry {attempts} more time(s)")
                time.sleep(5)
            else:
                logger.error("All retry attempts exhausted")
                return False

    return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Recreate SGD features file"
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
    log_file = LOG_DIR / "ftp_data_dump_features.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    logger.info("*" * 50)
    logger.info(datetime.now().isoformat())

    # Create output directory
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Output file
    output_file = DATA_DIR / f"{PROJECT_ACRONYM}_features.tab"

    try:
        with SessionLocal() as session:
            success = retrieve_data(session, output_file)

            if not success:
                return 1

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    logger.info(f"Finished executing {__file__}")
    logger.info(datetime.now().isoformat())

    return 0


if __name__ == "__main__":
    sys.exit(main())
