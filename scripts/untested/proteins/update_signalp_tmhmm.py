#!/usr/bin/env python3
"""
Update SignalP and TMHMM protein details.

This script loads SignalP signal peptide and TMHMM transmembrane domain
predictions into the protein_detail table.

Original Perl: updateSignalpTmhmm.pl
Converted to Python: 2024
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_, text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Feature, Organism, ProteinDetail, ProteinInfo

load_dotenv()

logger = logging.getLogger(__name__)

# Constants
SIGNALP_GROUP = 'signal peptide'
SIGNALP_TYPE = 'signal peptide'
TMHMM_GROUP = 'transmembrane domain'
TMHMM_TYPE = 'transmembrane domain'


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def get_organism(session: Session, organism_abbrev: str) -> Organism:
    """Get organism by abbreviation."""
    organism = session.query(Organism).filter(
        Organism.organism_abbrev == organism_abbrev
    ).first()

    if not organism:
        raise ValueError(f"Organism not found: {organism_abbrev}")

    return organism


def get_or_create_protein_info(
    session: Session,
    feature_no: int,
    created_by: str,
) -> int:
    """Get or create protein_info record."""
    protein_info = session.query(ProteinInfo).filter(
        ProteinInfo.feature_no == feature_no
    ).first()

    if protein_info:
        return protein_info.protein_info_no

    # Create new
    protein_info = ProteinInfo(
        feature_no=feature_no,
        created_by=created_by,
    )
    session.add(protein_info)
    session.flush()

    return protein_info.protein_info_no


def delete_existing_signalp_tmhmm(
    session: Session,
    organism: Organism,
    feature_list: list[str] = None,
) -> int:
    """
    Delete existing SignalP/TMHMM data.

    Args:
        session: Database session
        organism: Organism object
        feature_list: Optional list of specific features

    Returns:
        Number of rows deleted
    """
    groups = [SIGNALP_GROUP, TMHMM_GROUP]

    if feature_list:
        count = 0
        for feature_name in feature_list:
            result = session.execute(
                text("""
                    DELETE FROM protein_detail
                    WHERE protein_detail_no IN (
                        SELECT pd.protein_detail_no
                        FROM protein_detail pd
                        JOIN protein_info pi ON pd.protein_info_no = pi.protein_info_no
                        JOIN feature f ON pi.feature_no = f.feature_no
                        JOIN organism o ON f.organism_no = o.organism_no
                        WHERE o.organism_abbrev = :org_abbrev
                          AND f.feature_name = :feat_name
                          AND pd.protein_detail_group IN :groups
                    )
                """),
                {
                    'org_abbrev': organism.organism_abbrev,
                    'feat_name': feature_name,
                    'groups': tuple(groups),
                }
            )
            count += result.rowcount
        return count
    else:
        result = session.execute(
            text("""
                DELETE FROM protein_detail
                WHERE protein_detail_no IN (
                    SELECT pd.protein_detail_no
                    FROM protein_detail pd
                    JOIN protein_info pi ON pd.protein_info_no = pi.protein_info_no
                    JOIN feature f ON pi.feature_no = f.feature_no
                    JOIN organism o ON f.organism_no = o.organism_no
                    WHERE o.organism_abbrev = :org_abbrev
                      AND pd.protein_detail_group IN :groups
                )
            """),
            {
                'org_abbrev': organism.organism_abbrev,
                'groups': tuple(groups),
            }
        )
        return result.rowcount


def insert_protein_detail(
    session: Session,
    protein_info_no: int,
    group: str,
    detail_type: str,
    start_coord: int,
    stop_coord: int,
    created_by: str,
) -> bool:
    """
    Insert protein detail if not exists.

    Returns:
        True if inserted, False if already exists
    """
    existing = session.query(ProteinDetail).filter(
        and_(
            ProteinDetail.protein_info_no == protein_info_no,
            ProteinDetail.protein_detail_group == group,
            ProteinDetail.protein_detail_type == detail_type,
            ProteinDetail.protein_detail_value == 'Y',
            ProteinDetail.start_coord == start_coord,
            ProteinDetail.stop_coord == stop_coord,
        )
    ).first()

    if existing:
        return False

    protein_detail = ProteinDetail(
        protein_info_no=protein_info_no,
        protein_detail_group=group,
        protein_detail_type=detail_type,
        protein_detail_value='Y',
        start_coord=start_coord,
        stop_coord=stop_coord,
        created_by=created_by,
    )
    session.add(protein_detail)

    return True


def load_signalp_tmhmm_data(
    session: Session,
    data_file: Path,
    organism: Organism,
    created_by: str,
    feature_list: set[str] = None,
) -> dict:
    """
    Load SignalP/TMHMM data from InterProScan output.

    Args:
        session: Database session
        data_file: InterProScan TSV file
        organism: Organism object
        created_by: User name
        feature_list: Optional set of features to process

    Returns:
        Statistics dict
    """
    stats = {
        'processed': 0,
        'inserted': 0,
        'skipped': 0,
        'not_found': 0,
    }

    # Cache for feature_no and protein_info_no
    feat_no_cache = {}
    pi_no_cache = {}

    with open(data_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split('\t')
            if len(parts) < 8:
                continue

            orf = parts[0]
            method = parts[3]
            match_start = parts[6]
            match_end = parts[7]

            # Only process SignalP and TMHMM
            if 'signalp' not in method.lower() and 'tmhmm' not in method.lower():
                continue

            # Filter by feature list if provided
            if feature_list and orf not in feature_list:
                continue

            stats['processed'] += 1

            # Determine group and type
            if 'signalp' in method.lower():
                group = SIGNALP_GROUP
                detail_type = SIGNALP_TYPE
            else:
                group = TMHMM_GROUP
                detail_type = TMHMM_TYPE

            # Get feature_no
            if orf in feat_no_cache:
                feat_no = feat_no_cache[orf]
            else:
                feature = session.query(Feature).filter(
                    and_(
                        Feature.feature_name == orf,
                        Feature.organism_no == organism.organism_no,
                    )
                ).first()

                if not feature:
                    feat_no_cache[orf] = None
                    stats['not_found'] += 1
                    continue

                feat_no = feature.feature_no
                feat_no_cache[orf] = feat_no

            if feat_no is None:
                stats['not_found'] += 1
                continue

            # Get protein_info_no
            if feat_no in pi_no_cache:
                pi_no = pi_no_cache[feat_no]
            else:
                pi_no = get_or_create_protein_info(session, feat_no, created_by)
                pi_no_cache[feat_no] = pi_no

            # Insert protein detail
            if insert_protein_detail(
                session, pi_no, group, detail_type,
                int(match_start), int(match_end), created_by
            ):
                stats['inserted'] += 1
            else:
                stats['skipped'] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update SignalP and TMHMM protein details"
    )
    parser.add_argument(
        "--organism",
        required=True,
        help="Organism abbreviation",
    )
    parser.add_argument(
        "--data",
        type=Path,
        required=True,
        help="InterProScan TSV data file",
    )
    parser.add_argument(
        "--created-by",
        required=True,
        help="Database user for audit",
    )
    parser.add_argument(
        "--list",
        type=Path,
        help="Optional file with specific features to process",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without modifying database",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    logger.info(f"Started at {datetime.now()}")

    # Validate inputs
    if not args.data.exists():
        logger.error(f"Data file not found: {args.data}")
        sys.exit(1)

    # Load feature list if provided
    feature_list = None
    if args.list:
        if not args.list.exists():
            logger.error(f"Feature list file not found: {args.list}")
            sys.exit(1)
        feature_list = set()
        with open(args.list) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    feature_list.add(line.split()[0])
        logger.info(f"Loaded {len(feature_list)} features from list")

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    try:
        with SessionLocal() as session:
            # Get organism
            organism = get_organism(session, args.organism)
            logger.info(f"Processing organism: {organism.organism_name}")

            # Delete existing data
            deleted = delete_existing_signalp_tmhmm(
                session, organism,
                list(feature_list) if feature_list else None
            )
            logger.info(f"Deleted {deleted} existing SignalP/TMHMM rows")

            # Load new data
            stats = load_signalp_tmhmm_data(
                session, args.data, organism,
                args.created_by, feature_list
            )

            if not args.dry_run:
                session.commit()
                logger.info("Transaction committed")
            else:
                session.rollback()
                logger.info("Transaction rolled back (dry run)")

            logger.info("=" * 50)
            logger.info("Summary:")
            logger.info(f"  Processed: {stats['processed']}")
            logger.info(f"  Inserted: {stats['inserted']}")
            logger.info(f"  Skipped (existing): {stats['skipped']}")
            logger.info(f"  Not found: {stats['not_found']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
