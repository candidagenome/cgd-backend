#!/usr/bin/env python3
"""
Load domain/motif data into database.

This script loads InterProScan domain and motif annotations into the
protein_detail table.

Original Perl: loadDomainData.pl
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
from cgd.models.models import (
    Dbxref, Feature, Organism,
    ProteinDetail, ProteinInfo
)

load_dotenv()

logger = logging.getLogger(__name__)

# Method to group mapping
GROUP_FOR_METHOD = {
    'CATH': 'DOMAIN',
    'CDD': 'DOMAIN',
    'Hamap': 'DOMAIN',
    'NCBIfam': 'DOMAIN',
    'PANTHER': 'DOMAIN',
    'Pfam': 'DOMAIN',
    'PIRSF': 'DOMAIN',
    'ProSiteProfiles': 'DOMAIN',
    'SFLD': 'DOMAIN',
    'SMART': 'DOMAIN',
    'SUPERFAMILY': 'DOMAIN',
    'PRINTS': 'MOTIF',
    'ProSitePatterns': 'MOTIF',
    'SignalP': 'MOTIF',
    'Coils': 'STRUCTURAL REGION',
    'MobiDBLite': 'STRUCTURAL REGION',
    'TMHMM': 'STRUCTURAL REGION',
}

# Methods that don't need dbxref entries
SKIP_DBXREF_FOR_METHOD = {'SignalP', 'Coils', 'MobiDBLite', 'TMHMM'}

MAX_DESC = 240


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


def get_or_create_dbxref(
    session: Session,
    source: str,
    dbxref_id: str,
    dbxref_type: str,
    description: str,
    created_by: str,
) -> int:
    """Get or create dbxref record."""
    dbxref = session.query(Dbxref).filter(
        and_(
            Dbxref.dbxref_id == dbxref_id,
            Dbxref.dbxref_type == dbxref_type,
            Dbxref.source == source,
        )
    ).first()

    if dbxref:
        # Update description if changed
        if description and description != dbxref.description:
            desc = description[:MAX_DESC - 1] if len(description) > MAX_DESC else description
            dbxref.description = desc
        return dbxref.dbxref_no

    # Create new
    desc = description[:MAX_DESC - 1] if len(description) > MAX_DESC else description
    dbxref = Dbxref(
        source=source,
        dbxref_type=dbxref_type,
        dbxref_id=dbxref_id,
        description=desc,
        created_by=created_by,
    )
    session.add(dbxref)
    session.flush()

    return dbxref.dbxref_no


def insert_protein_detail(
    session: Session,
    protein_info_no: int,
    group: str,
    detail_type: str,
    value: str,
    start_coord: int,
    stop_coord: int,
    created_by: str,
) -> tuple[int, bool]:
    """
    Insert protein detail if not exists.

    Returns:
        Tuple of (protein_detail_no, was_inserted)
    """
    existing = session.query(ProteinDetail).filter(
        and_(
            ProteinDetail.protein_info_no == protein_info_no,
            ProteinDetail.protein_detail_group == group,
            ProteinDetail.protein_detail_type == detail_type,
            ProteinDetail.protein_detail_value == value,
            ProteinDetail.start_coord == start_coord,
            ProteinDetail.stop_coord == stop_coord,
        )
    ).first()

    if existing:
        return existing.protein_detail_no, False

    protein_detail = ProteinDetail(
        protein_info_no=protein_info_no,
        protein_detail_group=group,
        protein_detail_type=detail_type,
        protein_detail_value=value,
        start_coord=start_coord,
        stop_coord=stop_coord,
        created_by=created_by,
    )
    session.add(protein_detail)
    session.flush()

    return protein_detail.protein_detail_no, True


def delete_existing_domain_data(
    session: Session,
    organism: Organism,
    feature_list: list[str] = None,
) -> int:
    """
    Delete existing domain/motif data.

    Args:
        session: Database session
        organism: Organism object
        feature_list: Optional list of specific features

    Returns:
        Number of rows deleted
    """
    groups = ['DOMAIN', 'MOTIF', 'STRUCTURAL REGION']

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


def load_domain_data(
    session: Session,
    data_file: Path,
    organism: Organism,
    created_by: str,
    feature_list: set[str] = None,
) -> dict:
    """
    Load domain data from file.

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
            if len(parts) < 11:
                continue

            (orf, checksum, orf_len, method, member_id, member_desc,
             match_start, match_end, evalue, match_status, run_date) = parts[:11]

            interpro_id = parts[11] if len(parts) > 11 else '-'
            interpro_desc = parts[12] if len(parts) > 12 else '-'

            # Clean values
            member_desc = '' if member_desc == '-' else member_desc
            interpro_id = '' if interpro_id == '-' else interpro_id
            interpro_desc = '' if interpro_desc == '-' else interpro_desc

            # Normalize method
            if method.lower() in ('funfam', 'gene3d'):
                method = 'CATH'
                member_id = member_id.split(':FF')[0] if ':FF' in member_id else member_id

            if method.startswith('SignalP_'):
                method = 'SignalP'
                member_id = 'SignalP'

            # Get group
            if method not in GROUP_FOR_METHOD:
                logger.warning(f"Unknown method: {method}")
                continue

            group = GROUP_FOR_METHOD[method]

            # Filter by feature list if provided
            if feature_list and orf not in feature_list:
                continue

            stats['processed'] += 1

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
            pd_no, inserted = insert_protein_detail(
                session, pi_no, group, method, member_id,
                int(match_start), int(match_end), created_by
            )

            if inserted:
                stats['inserted'] += 1
            else:
                stats['skipped'] += 1

            # Add dbxref if needed
            if method not in SKIP_DBXREF_FOR_METHOD:
                member_dbx = get_or_create_dbxref(
                    session, method, member_id, 'InterPro Member ID',
                    member_desc, created_by
                )

                # Update protein_detail with member_dbxref_id
                pd = session.query(ProteinDetail).get(pd_no)
                if pd:
                    pd.member_dbxref_id = member_dbx

                if interpro_id:
                    ip_dbx = get_or_create_dbxref(
                        session, 'EBI', interpro_id, 'InterPro ID',
                        interpro_desc, created_by
                    )
                    if pd:
                        pd.interpro_dbxref_id = ip_dbx

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load domain/motif data into database"
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
            deleted = delete_existing_domain_data(
                session, organism,
                list(feature_list) if feature_list else None
            )
            logger.info(f"Deleted {deleted} existing domain/motif rows")

            # Load new data
            stats = load_domain_data(
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
