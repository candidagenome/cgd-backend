#!/usr/bin/env python3
"""
Update ORF classifications (Dubious, Verified, Uncharacterized).

This script updates the ORF classification qualifiers for features based on
GO annotation evidence and other criteria:

Classification Criteria:
------------------------
Dubious:
    The ORF is NOT dubious if it has:
    - Non-IEA GO terms characterization
    - Phenotype curation (excluding non-informative wild-type phenotypes)
    - Included in a protein family
    - Has an SGD ortholog or Best Hit per InParanoid analysis

Verified:
    - Not dubious
    - Not deleted
    - Has GO annotation (excluding IEA, ISS, RCA, ISA, ISM, ISO, NAS, ND)

Uncharacterized:
    - Not dubious
    - Not deleted
    - Not verified

Based on updateORFclassifications.pl by Prachi Shah (Sep 10, 2008).

Usage:
    python update_orf_classifications.py [strain_abbrev]
    python update_orf_classifications.py C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    ADMIN_USER: Admin username for database operations
    LOG_DIR: Directory for log files
    CURATOR_EMAIL: Email for error notifications
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL", "")

# GO evidence codes to exclude when determining "Verified" status
AVOID_GO_EVIDENCE_CODES = ["IEA", "ISS", "RCA", "ISA", "ISM", "ISO", "NAS", "ND"]

# Feature types to process
FEATURE_TYPES = ["ORF", "ncRNA", "rRNA", "snRNA", "snoRNA", "tRNA", "uORF"]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def send_error_email(subject: str, message: str) -> None:
    """Send error notification email."""
    if not CURATOR_EMAIL:
        logger.warning("CURATOR_EMAIL not set, skipping email notification")
        return
    logger.error(f"Email notification: {subject}")
    logger.error(f"Message: {message}")


def get_organism(session, strain_abbrev: str) -> dict | None:
    """Get organism info from database."""
    query = text(f"""
        SELECT organism_no, organism_abbrev, organism_name
        FROM {DB_SCHEMA}.organism
        WHERE organism_abbrev = :strain_abbrev
    """)
    result = session.execute(query, {"strain_abbrev": strain_abbrev}).fetchone()
    if not result:
        return None
    return {
        "organism_no": result[0],
        "organism_abbrev": result[1],
        "organism_name": result[2],
    }


def get_dubious_orfs(session, organism_no: int) -> set[str]:
    """Get list of currently dubious ORFs from database."""
    query = text(f"""
        SELECT DISTINCT f.feature_name
        FROM {DB_SCHEMA}.feat_property p
        JOIN {DB_SCHEMA}.feature f ON f.feature_no = p.feature_no
        WHERE p.property_value = 'Dubious'
        AND f.organism_no = :organism_no
    """)
    results = session.execute(query, {"organism_no": organism_no}).fetchall()
    return {row[0] for row in results}


def get_deleted_features(session, organism_no: int) -> set[str]:
    """Get list of deleted features."""
    query = text(f"""
        SELECT DISTINCT f.feature_name
        FROM {DB_SCHEMA}.feat_property p
        JOIN {DB_SCHEMA}.feature f ON f.feature_no = p.feature_no
        WHERE p.property_value LIKE 'Deleted%'
        AND f.organism_no = :organism_no
    """)
    results = session.execute(query, {"organism_no": organism_no}).fetchall()
    return {row[0] for row in results}


def get_verified_orfs(session, organism_no: int) -> set[str]:
    """
    Get list of features that should be Verified.

    Verified = has GO annotation with evidence codes other than
    IEA, ISS, RCA, ISA, ISM, ISO, NAS, ND.
    """
    # Build placeholders for evidence codes and feature types
    evidence_placeholders = ", ".join(f":evd_{i}" for i in range(len(AVOID_GO_EVIDENCE_CODES)))
    type_placeholders = ", ".join(f":type_{i}" for i in range(len(FEATURE_TYPES)))

    query = text(f"""
        SELECT DISTINCT f.feature_name
        FROM {DB_SCHEMA}.go_annotation g
        JOIN {DB_SCHEMA}.feature f ON g.feature_no = f.feature_no
        WHERE g.go_evidence NOT IN ({evidence_placeholders})
        AND f.feature_type IN ({type_placeholders})
        AND f.organism_no = :organism_no
    """)

    params = {"organism_no": organism_no}
    for i, code in enumerate(AVOID_GO_EVIDENCE_CODES):
        params[f"evd_{i}"] = code
    for i, ftype in enumerate(FEATURE_TYPES):
        params[f"type_{i}"] = ftype

    results = session.execute(query, params).fetchall()
    return {row[0] for row in results}


def get_all_features(session, organism_no: int) -> set[str]:
    """Get all features of relevant types for the organism."""
    type_placeholders = ", ".join(f":type_{i}" for i in range(len(FEATURE_TYPES)))

    query = text(f"""
        SELECT DISTINCT feature_name
        FROM {DB_SCHEMA}.feature
        WHERE feature_type IN ({type_placeholders})
        AND organism_no = :organism_no
    """)

    params = {"organism_no": organism_no}
    for i, ftype in enumerate(FEATURE_TYPES):
        params[f"type_{i}"] = ftype

    results = session.execute(query, params).fetchall()
    return {row[0] for row in results}


def get_feature_info(session, feature_name: str) -> dict | None:
    """Get feature info including current qualifier."""
    query = text(f"""
        SELECT f.feature_no, f.feature_name,
               (SELECT LISTAGG(p.property_value, ', ') WITHIN GROUP (ORDER BY p.property_value)
                FROM {DB_SCHEMA}.feat_property p
                WHERE p.feature_no = f.feature_no
                AND p.property_type = 'feature_qualifier') as feature_qualifier
        FROM {DB_SCHEMA}.feature f
        WHERE f.feature_name = :feature_name
    """)
    result = session.execute(query, {"feature_name": feature_name}).fetchone()
    if not result:
        return None
    return {
        "feature_no": result[0],
        "feature_name": result[1],
        "feature_qualifier": result[2] or "",
    }


def feat_property_exists(session, feature_no: int, prop_type: str, prop_value: str) -> bool:
    """Check if a feature property exists."""
    query = text(f"""
        SELECT 1 FROM {DB_SCHEMA}.feat_property
        WHERE feature_no = :feature_no
        AND source = :source
        AND property_type = :prop_type
        AND property_value = :prop_value
    """)
    result = session.execute(
        query,
        {
            "feature_no": feature_no,
            "source": PROJECT_ACRONYM,
            "prop_type": prop_type,
            "prop_value": prop_value,
        },
    ).fetchone()
    return result is not None


def add_feat_property(session, feature_no: int, prop_type: str, prop_value: str) -> bool:
    """Add a feature property if it doesn't exist."""
    if feat_property_exists(session, feature_no, prop_type, prop_value):
        return False

    query = text(f"""
        INSERT INTO {DB_SCHEMA}.feat_property
            (feature_no, source, property_type, property_value, created_by)
        VALUES
            (:feature_no, :source, :prop_type, :prop_value, :created_by)
    """)

    session.execute(
        query,
        {
            "feature_no": feature_no,
            "source": PROJECT_ACRONYM,
            "prop_type": prop_type,
            "prop_value": prop_value,
            "created_by": ADMIN_USER.upper()[:12],
        },
    )
    return True


def remove_feat_property(session, feature_no: int, prop_type: str, prop_value: str) -> bool:
    """Remove a feature property if it exists."""
    if not feat_property_exists(session, feature_no, prop_type, prop_value):
        return False

    query = text(f"""
        DELETE FROM {DB_SCHEMA}.feat_property
        WHERE feature_no = :feature_no
        AND source = :source
        AND property_type = :prop_type
        AND property_value = :prop_value
    """)

    session.execute(
        query,
        {
            "feature_no": feature_no,
            "source": PROJECT_ACRONYM,
            "prop_type": prop_type,
            "prop_value": prop_value,
        },
    )
    return True


def update_classification(
    session,
    feature_name: str,
    new_classification: str,
    log_handler: logging.FileHandler,
) -> bool:
    """
    Update a feature's classification to the specified value.

    Removes conflicting classifications and adds the new one.
    Returns True if any changes were made.
    """
    feat_info = get_feature_info(session, feature_name)
    if not feat_info:
        logger.error(f"No feature found for {feature_name}")
        send_error_email(
            "Error updating ORF classification",
            f"No feature found for {feature_name}",
        )
        return False

    feature_no = feat_info["feature_no"]
    current_qualifier = feat_info["feature_qualifier"]
    changed = False

    # Classifications that should be removed for each new classification
    remove_for = {
        "Dubious": ["Verified", "Uncharacterized"],
        "Verified": ["Dubious", "Uncharacterized"],
        "Uncharacterized": ["Dubious", "Verified"],
    }

    # Remove conflicting classifications
    for classification in remove_for.get(new_classification, []):
        if classification in current_qualifier:
            if remove_feat_property(session, feature_no, "feature_qualifier", classification):
                logger.info(f"Removed {classification} classification from {feature_name}")
                changed = True

    # Add new classification if not already present
    if new_classification not in current_qualifier:
        if add_feat_property(session, feature_no, "feature_qualifier", new_classification):
            logger.info(f"Added {new_classification} classification to {feature_name}")
            changed = True

    return changed


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update ORF classifications (Dubious, Verified, Uncharacterized)"
    )
    parser.add_argument(
        "strain_abbrev",
        nargs="?",
        default=None,
        help="Strain abbreviation (e.g., C_albicans_SC5314). If not provided, uses default.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without making changes",
    )

    args = parser.parse_args()

    strain_abbrev = args.strain_abbrev
    dry_run = args.dry_run

    # Set up file logging
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"{strain_abbrev or 'default'}_update_ORF_classifications.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Starting ORF classification update at {datetime.now()}")
    if dry_run:
        logger.info("DRY RUN - no changes will be made")

    try:
        with SessionLocal() as session:
            # If no strain specified, we'd need a default - for now, require it
            if not strain_abbrev:
                logger.error("Strain abbreviation is required")
                return 1

            # Get organism info
            organism = get_organism(session, strain_abbrev)
            if not organism:
                logger.error(f"No organism found in database for {strain_abbrev}")
                return 1

            organism_no = organism["organism_no"]
            logger.info(f"Processing organism: {organism['organism_name']} ({strain_abbrev})")

            # Get current dubious ORFs from database
            dubious_orfs = get_dubious_orfs(session, organism_no)
            logger.info(f"Found {len(dubious_orfs)} dubious ORFs")

            # Get deleted features
            deleted_features = get_deleted_features(session, organism_no)
            logger.info(f"Found {len(deleted_features)} deleted features")

            # Get features that should be verified (have good GO evidence)
            verified_orfs = get_verified_orfs(session, organism_no)
            logger.info(f"Found {len(verified_orfs)} potential verified ORFs")

            # Get all features
            all_features = get_all_features(session, organism_no)
            logger.info(f"Found {len(all_features)} total features")

            # Counters
            dubious_updates = 0
            verified_updates = 0
            uncharacterized_updates = 0

            # Process dubious ORFs
            logger.info("Processing dubious ORFs...")
            for orf in dubious_orfs:
                if not dry_run:
                    if update_classification(session, orf, "Dubious", file_handler):
                        dubious_updates += 1
                else:
                    logger.info(f"Would update {orf} to Dubious")

            # Process verified ORFs (exclude deleted and dubious)
            logger.info("Processing verified ORFs...")
            for orf in verified_orfs:
                if orf in deleted_features or orf in dubious_orfs:
                    continue
                if not dry_run:
                    if update_classification(session, orf, "Verified", file_handler):
                        verified_updates += 1
                else:
                    logger.info(f"Would update {orf} to Verified")

            # Process uncharacterized ORFs (everything else that's not deleted/dubious/verified)
            logger.info("Processing uncharacterized ORFs...")
            for orf in all_features:
                if orf in deleted_features or orf in dubious_orfs or orf in verified_orfs:
                    continue
                if not dry_run:
                    if update_classification(session, orf, "Uncharacterized", file_handler):
                        uncharacterized_updates += 1
                else:
                    logger.info(f"Would update {orf} to Uncharacterized")

            # Commit changes
            if not dry_run:
                session.commit()
                logger.info("Changes committed to database")

            # Summary
            logger.info(f"Summary:")
            logger.info(f"  Dubious updates: {dubious_updates}")
            logger.info(f"  Verified updates: {verified_updates}")
            logger.info(f"  Uncharacterized updates: {uncharacterized_updates}")

        logger.info(f"Completed at {datetime.now()}")
        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        send_error_email(
            "Error updating ORF classification",
            str(e),
        )
        return 1

    finally:
        logger.removeHandler(file_handler)
        file_handler.close()


if __name__ == "__main__":
    sys.exit(main())
