#!/usr/bin/env python3
"""
Add aliases from merged/deleted ORFs to their acceptor ORFs.

This script reads a file containing information about which deleted ORFs
were merged into which alive ORFs and loads the deleted ORF and all its
aliases as aliases to the acceptor ORF.

Input file format (tab-delimited):
  acceptor_feature_name, acceptor_locus_name, donor_feature_name, donor_locus_name

Original Perl: addAliasesForMergedOrfs.pl
Converted to Python: 2024
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Alias, FeatAlias, Feature, Locus

load_dotenv()

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def find_feature_by_name(session: Session, name: str) -> Feature | None:
    """
    Find feature by name.

    Args:
        session: Database session
        name: Feature name

    Returns:
        Feature object or None
    """
    return session.query(Feature).filter(
        Feature.feature_name == name
    ).first()


def get_feature_aliases(session: Session, feature_no: int) -> list[Alias]:
    """
    Get all aliases for a feature.

    Args:
        session: Database session
        feature_no: Feature number

    Returns:
        List of Alias objects
    """
    feat_aliases = session.query(FeatAlias).filter(
        FeatAlias.feature_no == feature_no
    ).all()

    aliases = []
    for fa in feat_aliases:
        alias = session.query(Alias).filter(
            Alias.alias_no == fa.alias_no
        ).first()
        if alias:
            aliases.append(alias)

    return aliases


def get_or_create_alias(session: Session, alias_name: str, created_by: str) -> Alias | None:
    """
    Get existing alias or return None if not found.

    Args:
        session: Database session
        alias_name: Alias name
        created_by: User creating the alias

    Returns:
        Alias object or None
    """
    return session.query(Alias).filter(
        Alias.alias_name == alias_name
    ).first()


def add_feat_alias(
    session: Session,
    alias_no: int,
    feature_no: int,
) -> bool:
    """
    Add alias to feature if not already linked.

    Args:
        session: Database session
        alias_no: Alias number
        feature_no: Feature number

    Returns:
        True if added, False if already exists
    """
    existing = session.query(FeatAlias).filter(
        and_(
            FeatAlias.alias_no == alias_no,
            FeatAlias.feature_no == feature_no,
        )
    ).first()

    if existing:
        return False

    feat_alias = FeatAlias(
        alias_no=alias_no,
        feature_no=feature_no,
    )
    session.add(feat_alias)
    return True


def process_merged_orfs(
    session: Session,
    input_file: Path,
    created_by: str,
) -> dict:
    """
    Process merged ORF file and add aliases.

    Args:
        session: Database session
        input_file: Input file with merge information
        created_by: User name for audit

    Returns:
        Statistics dict
    """
    stats = {
        "processed": 0,
        "aliases_added": 0,
        "aliases_skipped": 0,
        "locus_linked": 0,
        "donor_not_found": 0,
        "acceptor_not_found": 0,
    }

    with open(input_file) as f:
        for line_num, line in enumerate(f, 1):
            # Skip header
            if line_num == 1:
                continue

            line = line.strip()
            if not line:
                continue

            parts = line.split('\t')
            if len(parts) < 4:
                logger.warning(f"Line {line_num}: Invalid format, expected 4 columns")
                continue

            acceptor_orf, acceptor_locus, donor_orf, donor_locus = parts[:4]

            # Find donor feature
            donor_feature = find_feature_by_name(session, donor_orf)
            if not donor_feature:
                logger.warning(f"Donor ORF not found: {donor_orf}")
                stats["donor_not_found"] += 1
                continue

            # Find acceptor feature
            acceptor_feature = find_feature_by_name(session, acceptor_orf)
            if not acceptor_feature:
                logger.warning(f"Acceptor ORF not found: {acceptor_orf}")
                stats["acceptor_not_found"] += 1
                continue

            stats["processed"] += 1

            # Get aliases from donor
            donor_aliases = get_feature_aliases(session, donor_feature.feature_no)

            # Also add donor feature name as alias
            alias_names = [a.alias_name for a in donor_aliases]
            if donor_orf not in alias_names:
                donor_alias = get_or_create_alias(session, donor_orf, created_by)
                if donor_alias:
                    donor_aliases.append(donor_alias)

            # Add each alias to acceptor
            for alias in donor_aliases:
                if add_feat_alias(session, alias.alias_no, acceptor_feature.feature_no):
                    logger.info(f"Added alias '{alias.alias_name}' to {acceptor_orf}")
                    stats["aliases_added"] += 1
                else:
                    logger.debug(f"Alias '{alias.alias_name}' already on {acceptor_orf}")
                    stats["aliases_skipped"] += 1

            # Link locus if acceptor doesn't have one
            if not acceptor_feature.locus_no and donor_locus:
                locus = session.query(Locus).filter(
                    Locus.locus_name == donor_locus
                ).first()

                if locus:
                    acceptor_feature.locus_no = locus.locus_no
                    logger.info(f"Linked locus '{donor_locus}' to {acceptor_orf}")
                    stats["locus_linked"] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Add aliases from merged/deleted ORFs to acceptor ORFs"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input file with merge information (tab-delimited)",
    )
    parser.add_argument(
        "--created-by",
        default="SYSTEM",
        help="User name for audit trail (default: SYSTEM)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying database",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    logger.info(f"Started at {datetime.now()}")

    # Validate input file
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    try:
        with SessionLocal() as session:
            stats = process_merged_orfs(
                session,
                args.input_file,
                args.created_by,
            )

            if not args.dry_run:
                session.commit()
                logger.info("Transaction committed")
            else:
                session.rollback()
                logger.info("Transaction rolled back (dry run)")

            logger.info("=" * 50)
            logger.info("Summary:")
            logger.info(f"  Merges processed: {stats['processed']}")
            logger.info(f"  Aliases added: {stats['aliases_added']}")
            logger.info(f"  Aliases skipped (already exist): {stats['aliases_skipped']}")
            logger.info(f"  Locus linked: {stats['locus_linked']}")
            logger.info(f"  Donor not found: {stats['donor_not_found']}")
            logger.info(f"  Acceptor not found: {stats['acceptor_not_found']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
