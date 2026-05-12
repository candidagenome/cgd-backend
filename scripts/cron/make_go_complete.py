#!/usr/bin/env python3
from __future__ import annotations

"""
Make CGD GO-complete by assigning root GO terms to features without annotations.

This script finds features that do not have any GO annotations for a particular
aspect and assigns the root term with 'ND' evidence code. This signifies that
a curator has examined the literature and that as of the date of the annotation,
there was no information available for that aspect.

Safety checks before modifying database:
---------------------------------------------------------------------------
- Validates that deletion count is within expected range
- Validates that insertion count is within expected range
- Checks annotation count change < 50% vs previous run
- Sends Slack notifications on success or failure
- Use --dry-run to preview changes without modifying database

Based on make_GO_complete.pl by Prachi Shah (March 2010)

Usage:
    python make_go_complete.py C_albicans_SC5314
    python make_go_complete.py --all
    python make_go_complete.py --dry-run C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    SLACK_WEBHOOK_URL: Slack webhook URL for notifications
    ENV_STATE: Environment state (dev/prod) for Slack labels
"""

import argparse
import json
import logging
import os
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Project root directory (cgd-backend/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load environment variables BEFORE importing cgd modules (settings validation)
load_dotenv(PROJECT_ROOT / ".env")

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
LOG_DIR = Path(os.getenv("LOG_DIR", str(PROJECT_ROOT / "logs")))
ADMIN_USER = os.getenv("ADMIN_USER", "CGDADMIN").upper()
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")

# Slack webhook for notifications
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
ENV_STATE = os.getenv("ENV_STATE", "dev")

# Validation thresholds
MIN_ND_ANNOTATIONS = 100  # Minimum ND annotations expected per strain
MAX_ND_CHANGE_PERCENT = 50.0  # Maximum allowed change from previous count

# CGD reference_no for ND annotations (internal reference for auto-generated descriptions)
ND_REFERENCE_NO = 53556  # CGD production

# Root GO terms for each aspect
ASPECT_TO_ROOT_TERM = {
    "P": "biological_process",
    "F": "molecular_function",
    "C": "cellular_component",
}

# Strain configurations
STRAIN_ABBREVS = [
    "C_albicans_SC5314",
    "C_auris_B8441",
    "C_dubliniensis_CD36",
    "C_glabrata_CBS138",
    "C_parapsilosis_CDC317",
    "C_tropicalis",
]

# Configure logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def send_slack_message(message: str, is_error: bool = False) -> None:
    """Send a message to Slack webhook."""
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set, skipping notification")
        return

    emoji = ":x:" if is_error else ":white_check_mark:"
    env_prefix = f"[{ENV_STATE.upper()}] " if ENV_STATE != "prod" else ""

    payload = {
        "text": f"{emoji} {env_prefix}GO-Complete: {message}"
    }

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status != 200:
                logger.warning(f"Slack notification failed: {response.status}")
    except Exception as e:
        logger.warning(f"Failed to send Slack notification: {e}")


def validate_nd_counts(
    strain_abbrev: str,
    deleted_count: int,
    new_count: int,
) -> tuple[bool, str]:
    """
    Validate ND annotation counts.

    Returns:
        Tuple of (is_valid, message)
    """
    # Check minimum new annotations
    if new_count < MIN_ND_ANNOTATIONS:
        return False, (
            f"Too few ND annotations for {strain_abbrev}: {new_count} "
            f"(minimum: {MIN_ND_ANNOTATIONS})"
        )

    # Check against deleted count (previous run) if present
    if deleted_count > 0:
        change_pct = abs(new_count - deleted_count) / deleted_count * 100
        if change_pct > MAX_ND_CHANGE_PERCENT:
            return False, (
                f"ND count changed too much for {strain_abbrev}: "
                f"{deleted_count} -> {new_count} ({change_pct:.1f}% "
                f"change, max: {MAX_ND_CHANGE_PERCENT}%)"
            )

    return True, f"Validation passed for {strain_abbrev}: {new_count} ND annotations"


def get_organism_no(session, strain_abbrev: str) -> int | None:
    """Get organism_no for a strain."""
    query = text(f"""
        SELECT organism_no
        FROM {DB_SCHEMA}.organism
        WHERE organism_abbrev = :strain_abbrev
    """)
    result = session.execute(query, {"strain_abbrev": strain_abbrev}).fetchone()
    return result[0] if result else None


def get_root_go_no(session, go_term: str, go_aspect: str) -> int | None:
    """Get go_no for a root GO term."""
    query = text(f"""
        SELECT go_no
        FROM {DB_SCHEMA}.go
        WHERE go_term = :go_term
        AND go_aspect = :go_aspect
    """)
    result = session.execute(query, {"go_term": go_term, "go_aspect": go_aspect}).fetchone()
    return result[0] if result else None


def get_next_go_annotation_no(session) -> int:
    """Get next go_annotation_no."""
    query = text(f"SELECT MAX(go_annotation_no) FROM {DB_SCHEMA}.go_annotation")
    result = session.execute(query).scalar()
    return (result or 0) + 1


def get_next_go_ref_no(session) -> int:
    """Get next go_ref_no."""
    query = text(f"SELECT MAX(go_ref_no) FROM {DB_SCHEMA}.go_ref")
    result = session.execute(query).scalar()
    return (result or 0) + 1


def check_existing_annotation(session, feature_no: int, go_no: int) -> int | None:
    """Check if annotation already exists, return go_annotation_no if so."""
    query = text(f"""
        SELECT go_annotation_no
        FROM {DB_SCHEMA}.go_annotation
        WHERE go_no = :go_no
        AND feature_no = :feature_no
        AND go_evidence = 'ND'
        AND source = :source
        AND annotation_type = 'manually curated'
    """)
    result = session.execute(query, {
        "go_no": go_no,
        "feature_no": feature_no,
        "source": PROJECT_ACRONYM,
    }).fetchone()
    return result[0] if result else None


def check_existing_go_ref(session, go_annotation_no: int, reference_no: int) -> int | None:
    """Check if go_ref already exists, return go_ref_no if so."""
    query = text(f"""
        SELECT go_ref_no
        FROM {DB_SCHEMA}.go_ref
        WHERE go_annotation_no = :go_annotation_no
        AND reference_no = :reference_no
    """)
    result = session.execute(query, {
        "go_annotation_no": go_annotation_no,
        "reference_no": reference_no,
    }).fetchone()
    return result[0] if result else None


def delete_nd_annotations(session, organism_no: int, dry_run: bool = False) -> int:
    """
    Delete all previous ND annotations for a strain.

    Returns count of deleted annotations.
    """
    # First count
    count_query = text(f"""
        SELECT COUNT(*)
        FROM {DB_SCHEMA}.go_annotation
        WHERE go_evidence = 'ND'
        AND feature_no IN (
            SELECT feature_no FROM {DB_SCHEMA}.feature WHERE organism_no = :organism_no
        )
    """)
    count = session.execute(count_query, {"organism_no": organism_no}).scalar() or 0

    if dry_run:
        logger.info(f"  [DRY RUN] Would delete {count} existing ND annotations")
        return count

    if count > 0:
        # Delete go_ref entries first (foreign key)
        delete_refs = text(f"""
            DELETE FROM {DB_SCHEMA}.go_ref
            WHERE go_annotation_no IN (
                SELECT go_annotation_no
                FROM {DB_SCHEMA}.go_annotation
                WHERE go_evidence = 'ND'
                AND feature_no IN (
                    SELECT feature_no FROM {DB_SCHEMA}.feature WHERE organism_no = :organism_no
                )
            )
        """)
        session.execute(delete_refs, {"organism_no": organism_no})

        # Delete annotations
        delete_annot = text(f"""
            DELETE FROM {DB_SCHEMA}.go_annotation
            WHERE go_evidence = 'ND'
            AND feature_no IN (
                SELECT feature_no FROM {DB_SCHEMA}.feature WHERE organism_no = :organism_no
            )
        """)
        session.execute(delete_annot, {"organism_no": organism_no})

        logger.info(f"  Deleted {count} existing ND annotations")

    return count


def get_features_without_aspect(session, organism_no: int, aspect: str) -> list[tuple]:
    """
    Get features without GO annotations for a specific aspect.

    Returns list of (feature_no, feature_name) tuples.
    """
    query = text(f"""
        SELECT DISTINCT f.feature_no, f.feature_name
        FROM {DB_SCHEMA}.feature f
        WHERE f.feature_type IN ('ORF', 'ncRNA', 'rRNA', 'snRNA', 'snoRNA', 'tRNA')
        AND f.feature_no NOT IN (
            SELECT DISTINCT fp.feature_no
            FROM {DB_SCHEMA}.feat_property fp
            WHERE fp.property_type = 'feature_qualifier'
            AND fp.property_value LIKE 'Deleted%'
        )
        AND f.feature_no NOT IN (
            SELECT DISTINCT ga.feature_no
            FROM {DB_SCHEMA}.go_annotation ga
            WHERE ga.go_no IN (
                SELECT go_no FROM {DB_SCHEMA}.go WHERE go_aspect = :aspect
            )
        )
        AND f.organism_no = :organism_no
    """)

    result = session.execute(query, {"organism_no": organism_no, "aspect": aspect})
    return [(row[0], row[1]) for row in result]


def insert_go_annotation(
    session,
    feature_no: int,
    go_no: int,
    dry_run: bool = False
) -> int | None:
    """Insert GO annotation with ND evidence."""
    # Check if already exists
    existing = check_existing_annotation(session, feature_no, go_no)
    if existing:
        return existing

    if dry_run:
        return -1  # Placeholder for dry run

    go_annotation_no = get_next_go_annotation_no(session)

    insert_query = text(f"""
        INSERT INTO {DB_SCHEMA}.go_annotation
        (go_annotation_no, go_no, feature_no, go_evidence, source, annotation_type, created_by)
        VALUES (:go_annotation_no, :go_no, :feature_no, 'ND', :source, 'manually curated', :user)
    """)

    session.execute(insert_query, {
        "go_annotation_no": go_annotation_no,
        "go_no": go_no,
        "feature_no": feature_no,
        "source": PROJECT_ACRONYM,
        "user": ADMIN_USER,
    })

    return go_annotation_no


def insert_go_ref(
    session,
    go_annotation_no: int,
    reference_no: int,
    dry_run: bool = False
) -> int | None:
    """Insert GO reference."""
    # Check if already exists
    existing = check_existing_go_ref(session, go_annotation_no, reference_no)
    if existing:
        return existing

    if dry_run:
        return -1  # Placeholder for dry run

    go_ref_no = get_next_go_ref_no(session)

    insert_query = text(f"""
        INSERT INTO {DB_SCHEMA}.go_ref
        (go_ref_no, go_annotation_no, reference_no, created_by, has_qualifier, has_supporting_evidence)
        VALUES (:go_ref_no, :go_annotation_no, :reference_no, :user, 'N', 'N')
    """)

    session.execute(insert_query, {
        "go_ref_no": go_ref_no,
        "go_annotation_no": go_annotation_no,
        "reference_no": reference_no,
        "user": ADMIN_USER,
    })

    return go_ref_no


def make_go_complete(strain_abbrev: str, dry_run: bool = False) -> bool:
    """
    Make a strain GO-complete by adding ND annotations.

    Args:
        strain_abbrev: Strain abbreviation
        dry_run: If True, don't actually modify database

    Returns:
        True on success, False on failure
    """
    log_file = LOG_DIR / f"GO_complete_{strain_abbrev}.log"
    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Making {strain_abbrev} GO-complete")
    logger.info(f"Started: {datetime.now()}")

    try:
        with SessionLocal() as session:
            # Get organism_no
            organism_no = get_organism_no(session, strain_abbrev)
            if not organism_no:
                error_msg = f"Organism not found: {strain_abbrev}"
                logger.error(error_msg)
                send_slack_message(error_msg, is_error=True)
                return False

            logger.info(f"Organism: {strain_abbrev} (organism_no={organism_no})")

            # Delete existing ND annotations (store count for validation)
            logger.info("Deleting existing ND annotations...")
            deleted_count = delete_nd_annotations(session, organism_no, dry_run)

            # Process each aspect
            total_annotations = 0

            for aspect, root_term in ASPECT_TO_ROOT_TERM.items():
                logger.info(f"\nProcessing aspect {aspect} ({root_term})...")

                # Get root GO term
                go_no = get_root_go_no(session, root_term, aspect)
                if not go_no:
                    logger.error(f"Root GO term not found: {root_term} ({aspect})")
                    continue

                # Get features without this aspect
                features = get_features_without_aspect(session, organism_no, aspect)
                logger.info(f"  Found {len(features)} features without {aspect} annotations")

                # Add ND annotations
                for feature_no, feature_name in features:
                    go_annotation_no = insert_go_annotation(
                        session, feature_no, go_no, dry_run
                    )

                    if go_annotation_no:
                        go_ref_no = insert_go_ref(
                            session, go_annotation_no, ND_REFERENCE_NO, dry_run
                        )

                        if go_ref_no:
                            total_annotations += 1
                            if not dry_run:
                                logger.debug(f"  Added ND for {feature_name} ({aspect})")

                logger.info(f"  Added {len(features)} ND annotations for aspect {aspect}")

            # Validate counts before committing
            is_valid, validation_msg = validate_nd_counts(
                strain_abbrev, deleted_count, total_annotations
            )

            if not is_valid and not dry_run:
                error_msg = f"Validation failed: {validation_msg}"
                logger.error(error_msg)
                session.rollback()
                send_slack_message(error_msg, is_error=True)
                return False

            # Commit or rollback
            if dry_run:
                session.rollback()
                logger.info(f"\n[DRY RUN] Would add {total_annotations} total ND annotations")
            else:
                session.commit()
                logger.info(f"\nCommitted {total_annotations} total ND annotations")

                # Send success notification
                send_slack_message(
                    f"Successfully updated {strain_abbrev} with "
                    f"{total_annotations} ND annotations"
                )

            logger.info(f"Completed: {datetime.now()}")
            print(f"{'[DRY RUN] ' if dry_run else ''}{strain_abbrev}: {total_annotations} ND annotations")

            return True

    except Exception as e:
        logger.exception(f"Error making {strain_abbrev} GO-complete: {e}")
        send_slack_message(
            f"Error making {strain_abbrev} GO-complete: {e}",
            is_error=True
        )
        return False

    finally:
        logger.removeHandler(file_handler)
        file_handler.close()


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Make CGD GO-complete by assigning root GO terms"
    )
    parser.add_argument(
        "strain",
        nargs="?",
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all strains",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually modify database, just show what would be done",
    )

    args = parser.parse_args()

    if args.all:
        strains = STRAIN_ABBREVS
    elif args.strain:
        strains = [args.strain]
    else:
        parser.print_help()
        return 1

    success = True
    for strain in strains:
        if not make_go_complete(strain, args.dry_run):
            success = False

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
