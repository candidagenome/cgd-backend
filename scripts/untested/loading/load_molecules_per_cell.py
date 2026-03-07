#!/usr/bin/env python3
"""
Load molecules/cell data into protein_detail table.

This script loads protein abundance data (molecules per cell)
from localization/abundance studies.

Original Perl: loadMoleculesPerCell.pl
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
from cgd.models.models import Feature, ProteinDetail, ProteinInfo

load_dotenv()

logger = logging.getLogger(__name__)

PROTEIN_DETAIL_TYPE = 'molecules/cell'


def setup_logging(verbose: bool = False, log_file: Path = None) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler()]

    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def get_protein_info_map(session: Session) -> dict:
    """
    Get mapping of feature_name to protein_info_no.

    Returns:
        Dict mapping feature_name to protein_info_no
    """
    result = session.execute(
        text("""
            SELECT pi.protein_info_no, f.feature_name
            FROM protein_info pi
            JOIN feature f ON pi.feature_no = f.feature_no
        """)
    )

    return {row[1]: row[0] for row in result}


def load_molecules_data(
    session: Session,
    data_file: Path,
    created_by: str,
    dry_run: bool = False,
) -> dict:
    """
    Load molecules/cell data from file.

    Args:
        session: Database session
        data_file: Data file path (TSV with yORF and abundance columns)
        created_by: User name for audit
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'processed': 0,
        'inserted': 0,
        'skipped_no_protein': 0,
        'skipped_non_numeric': 0,
        'errors': 0,
    }

    # Get protein_info mapping
    protein_info_map = get_protein_info_map(session)
    logger.info(f"Loaded {len(protein_info_map)} protein_info entries")

    # Find column indices from header
    feat_col_idx = None
    abundance_col_idx = None

    with open(data_file) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            parts = line.split('\t')

            # First line is header
            if line_num == 1:
                for idx, col in enumerate(parts):
                    col_lower = col.lower().strip()
                    if col_lower == 'yorf':
                        feat_col_idx = idx
                    elif col_lower == 'abundance':
                        abundance_col_idx = idx

                if feat_col_idx is None:
                    logger.error("Could not find 'yORF' column in header")
                    return stats
                if abundance_col_idx is None:
                    logger.error("Could not find 'abundance' column in header")
                    return stats

                logger.info(
                    f"Using columns: yORF={feat_col_idx}, abundance={abundance_col_idx}"
                )
                continue

            # Data rows
            if len(parts) <= max(feat_col_idx, abundance_col_idx):
                continue

            feat_name = parts[feat_col_idx].strip()
            abundance = parts[abundance_col_idx].strip()

            # Skip non-numeric abundance values
            if not abundance.isdigit():
                stats['skipped_non_numeric'] += 1
                continue

            stats['processed'] += 1

            # Get protein_info_no
            protein_info_no = protein_info_map.get(feat_name)
            if not protein_info_no:
                logger.debug(f"No protein_info for {feat_name}")
                stats['skipped_no_protein'] += 1
                continue

            # Insert protein_detail
            try:
                protein_detail = ProteinDetail(
                    protein_info_no=protein_info_no,
                    protein_detail_type=PROTEIN_DETAIL_TYPE,
                    protein_detail_value=abundance,
                    created_by=created_by,
                )
                session.add(protein_detail)

                if not dry_run:
                    session.flush()

                logger.debug(
                    f"Inserted molecules/cell for {feat_name}: {abundance}"
                )
                stats['inserted'] += 1

            except Exception as e:
                logger.error(
                    f"Error inserting for protein_info_no={protein_info_no}: {e}"
                )
                stats['errors'] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load molecules/cell data into protein_detail table"
    )
    parser.add_argument(
        "data_file",
        type=Path,
        help="Data file (TSV with yORF and abundance columns)",
    )
    parser.add_argument(
        "--created-by",
        default="SCRIPT",
        help="User name for audit (default: SCRIPT)",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Log file path",
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

    setup_logging(args.verbose, args.log_file)

    logger.info(f"Started at {datetime.now()}")

    # Validate input
    if not args.data_file.exists():
        logger.error(f"Data file not found: {args.data_file}")
        sys.exit(1)

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    try:
        with SessionLocal() as session:
            stats = load_molecules_data(
                session,
                args.data_file,
                args.created_by,
                args.dry_run,
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
            logger.info(f"  Skipped (no protein_info): {stats['skipped_no_protein']}")
            logger.info(f"  Skipped (non-numeric): {stats['skipped_non_numeric']}")
            logger.info(f"  Errors: {stats['errors']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
