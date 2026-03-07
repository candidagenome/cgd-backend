#!/usr/bin/env python3
"""
Find and fix ORFs with partial terminal codons.

This script identifies ORFs where the nucleotide sequence length is not
divisible by 3 (indicating partial terminal codons) and can optionally
fix the coordinates.

Original Perl: fixPartialTerminalCodons.pl
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
from cgd.models.models import (
    Feature, FeatLocation, Organism, Seq as SeqModel,
    Subfeature, SubfeatureType
)

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


def get_organism(session: Session, organism_abbrev: str) -> Organism:
    """Get organism by abbreviation."""
    organism = session.query(Organism).filter(
        Organism.organism_abbrev == organism_abbrev
    ).first()

    if not organism:
        raise ValueError(f"Organism not found: {organism_abbrev}")

    return organism


def get_coding_sequence(session: Session, feature: Feature) -> str | None:
    """
    Get the coding sequence for a feature.

    Args:
        session: Database session
        feature: Feature object

    Returns:
        Coding sequence string or None
    """
    seq_record = session.query(SeqModel).filter(
        and_(
            SeqModel.feature_no == feature.feature_no,
            SeqModel.seq_type == 'genomic',
            SeqModel.is_seq_current == 'Y',
        )
    ).first()

    if seq_record and seq_record.residues:
        return seq_record.residues

    return None


def find_partial_terminal_codons(
    session: Session,
    organism: Organism = None,
    feature_type: str = 'ORF',
    feature_pattern: str = None,
) -> list[dict]:
    """
    Find features with partial terminal codons.

    Args:
        session: Database session
        organism: Optional organism filter
        feature_type: Feature type to check
        feature_pattern: Optional pattern for feature names

    Returns:
        List of features with partial codons
    """
    results = []

    # Build query
    query = session.query(Feature).filter(
        Feature.feature_type == feature_type
    )

    if organism:
        query = query.filter(Feature.organism_no == organism.organism_no)

    if feature_pattern:
        query = query.filter(Feature.feature_name.like(feature_pattern))

    features = query.order_by(Feature.feature_name).all()
    logger.info(f"Checking {len(features)} features")

    for feat in features:
        seq = get_coding_sequence(session, feat)
        if not seq:
            continue

        length = len(seq)
        remainder = length % 3

        if remainder != 0:
            # Get location info
            location = session.query(FeatLocation).filter(
                and_(
                    FeatLocation.feature_no == feat.feature_no,
                    FeatLocation.is_loc_current == 'Y',
                )
            ).first()

            results.append({
                'feature_name': feat.feature_name,
                'feature_no': feat.feature_no,
                'length': length,
                'remainder': remainder,
                'correction': -remainder,
                'strand': location.strand if location else None,
                'start_coord': location.start_coord if location else None,
                'stop_coord': location.stop_coord if location else None,
            })

    return results


def fix_coordinates(
    session: Session,
    feature_no: int,
    correction: int,
    strand: str,
) -> bool:
    """
    Fix coordinates for a feature with partial terminal codon.

    Args:
        session: Database session
        feature_no: Feature number
        correction: Number of bases to adjust (negative)
        strand: Strand (W/C)

    Returns:
        True if fixed, False otherwise
    """
    # Get current location
    location = session.query(FeatLocation).filter(
        and_(
            FeatLocation.feature_no == feature_no,
            FeatLocation.is_loc_current == 'Y',
        )
    ).first()

    if not location:
        return False

    old_stop = location.stop_coord

    # Adjust stop coordinate based on strand
    if strand == 'W':
        location.stop_coord = old_stop + correction
    else:
        location.stop_coord = old_stop - correction

    logger.debug(f"Updated stop coord from {old_stop} to {location.stop_coord}")

    # Also update terminal exon if exists
    subfeatures = session.query(Subfeature).filter(
        Subfeature.feature_no == feature_no
    ).all()

    for subfeat in subfeatures:
        # Check if this is an exon
        subfeat_type = session.query(SubfeatureType).filter(
            and_(
                SubfeatureType.subfeature_no == subfeat.subfeature_no,
                SubfeatureType.subfeature_type == 'Exon',
            )
        ).first()

        if subfeat_type:
            # Check if this is the terminal exon (has highest stop coord for W strand)
            if strand == 'W' and subfeat.stop_coord == old_stop:
                subfeat.stop_coord = subfeat.stop_coord + correction
            elif strand == 'C' and subfeat.stop_coord == old_stop:
                subfeat.stop_coord = subfeat.stop_coord - correction

    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Find and fix ORFs with partial terminal codons"
    )
    parser.add_argument(
        "--organism",
        help="Organism abbreviation",
    )
    parser.add_argument(
        "--feature-type",
        default='ORF',
        help="Feature type to check (default: ORF)",
    )
    parser.add_argument(
        "--feature-pattern",
        help="Pattern for feature names (SQL LIKE pattern, e.g., 'orf19%%')",
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Actually fix the coordinates (default: report only)",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output file (default: stdout)",
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

    if args.fix and args.dry_run:
        logger.info("DRY RUN with --fix - will show fixes but not apply them")

    stats = {
        "checked": 0,
        "partial": 0,
        "fixed": 0,
    }

    try:
        with SessionLocal() as session:
            # Get organism if specified
            organism = None
            if args.organism:
                organism = get_organism(session, args.organism)
                logger.info(f"Filtering by organism: {organism.organism_name}")

            # Find partial terminal codons
            results = find_partial_terminal_codons(
                session,
                organism=organism,
                feature_type=args.feature_type,
                feature_pattern=args.feature_pattern,
            )

            stats["partial"] = len(results)

            # Output results
            out_handle = open(args.output, 'w') if args.output else sys.stdout

            try:
                out_handle.write("# Features with partial terminal codons\n")
                out_handle.write("# Feature\tLength\tRemainder\tCorrection\tStrand\n")

                for item in results:
                    out_handle.write(
                        f"{item['feature_name']}\t{item['length']}\t"
                        f"{item['remainder']}\t{item['correction']}\t"
                        f"{item['strand'] or 'N/A'}\n"
                    )

                    if args.fix:
                        if fix_coordinates(
                            session,
                            item['feature_no'],
                            item['correction'],
                            item['strand'],
                        ):
                            stats["fixed"] += 1
                            logger.info(f"Fixed {item['feature_name']}")

            finally:
                if args.output:
                    out_handle.close()

            if args.fix and not args.dry_run:
                session.commit()
                logger.info("Transaction committed")
            else:
                session.rollback()
                if args.fix:
                    logger.info("Transaction rolled back (dry run)")

            logger.info("=" * 50)
            logger.info("Summary:")
            logger.info(f"  Features with partial codons: {stats['partial']}")
            if args.fix:
                logger.info(f"  Coordinates fixed: {stats['fixed']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
