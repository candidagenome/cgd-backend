#!/usr/bin/env python3
"""
Load subfeatures (exons/introns) for features.

This script loads subfeature coordinates from a file, useful for
correcting or adding exon/intron structure to features.

Input file format (tab-delimited):
  feature_name, note, chromosome, start, stop, strand, segments, contig

Segments format: comma-separated ranges like "100-200,300-400"

Original Perl: fixDeletedSubFeatures.pl
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
from cgd.models.models import Feature, Subfeature, SubfeatureType

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


def load_subfeature(
    session: Session,
    feature_no: int,
    start_coord: int,
    stop_coord: int,
    subfeature_type: str,
    created_by: str,
) -> Subfeature:
    """
    Create a subfeature with type.

    Args:
        session: Database session
        feature_no: Parent feature number
        start_coord: Start coordinate (relative to feature)
        stop_coord: Stop coordinate (relative to feature)
        subfeature_type: Type (Exon, Intron, etc.)
        created_by: User creating the record

    Returns:
        Created Subfeature object
    """
    subfeature = Subfeature(
        feature_no=feature_no,
        start_coord=start_coord,
        stop_coord=stop_coord,
        created_by=created_by,
    )
    session.add(subfeature)
    session.flush()  # Get the subfeature_no

    # Add subfeature type
    subfeat_type = SubfeatureType(
        subfeature_no=subfeature.subfeature_no,
        subfeature_type=subfeature_type,
        created_by=created_by,
    )
    session.add(subfeat_type)

    return subfeature


def parse_segments(segments_str: str) -> list[tuple[int, int]]:
    """
    Parse segments string to list of (start, end) tuples.

    Args:
        segments_str: Comma-separated ranges like "100-200,300-400"

    Returns:
        List of (start, end) tuples
    """
    segments = []
    for seg in segments_str.split(','):
        seg = seg.strip()
        if '-' in seg:
            parts = seg.split('-')
            start = int(parts[0])
            end = int(parts[1])
            segments.append((start, end))
    return segments


def insert_subfeatures(
    session: Session,
    feature: Feature,
    start: int,
    stop: int,
    strand: str,
    segments: list[tuple[int, int]],
    created_by: str,
) -> int:
    """
    Insert subfeatures (exons and introns) for a feature.

    Args:
        session: Database session
        feature: Feature object
        start: Feature start coordinate
        stop: Feature stop coordinate
        strand: Strand (W/C)
        segments: List of (start, end) tuples for exons
        created_by: User creating the records

    Returns:
        Number of subfeatures created
    """
    count = 0

    # Single segment = single exon
    if len(segments) == 1:
        seg_start, seg_end = segments[0]

        # Coordinates relative to feature
        rel_start = 1
        rel_end = seg_end - seg_start + 1

        load_subfeature(
            session, feature.feature_no,
            rel_start, rel_end, 'Exon', created_by
        )
        logger.debug(f"  Loaded single exon: {rel_start}-{rel_end}")
        return 1

    # Multiple segments = exons + introns
    for i, (seg_start, seg_end) in enumerate(segments):
        # Calculate relative coordinates based on strand
        if strand == 'W':  # Forward strand
            rel_start = seg_start - start + 1
            rel_end = seg_end - start + 1
        else:  # Reverse strand
            rel_start = stop - seg_end + 1
            rel_end = stop - seg_start + 1

        # Load exon
        load_subfeature(
            session, feature.feature_no,
            rel_start, rel_end, 'Exon', created_by
        )
        logger.debug(f"  Loaded exon: {rel_start}-{rel_end}")
        count += 1

        # Load intron (between this and previous segment)
        if i > 0:
            prev_start, prev_end = segments[i - 1]

            if strand == 'W':
                intron_start = prev_end - start + 2
                intron_end = seg_start - start
            else:
                intron_end = stop - prev_end
                intron_start = stop - seg_start + 2

            # Check for overlapping adjusted exons
            if intron_end < intron_start:
                load_subfeature(
                    session, feature.feature_no,
                    intron_start, intron_end, 'Adjustment', created_by
                )
                logger.debug(f"  Loaded adjustment: {intron_start}-{intron_end}")
            else:
                load_subfeature(
                    session, feature.feature_no,
                    intron_start, intron_end, 'Intron', created_by
                )
                logger.debug(f"  Loaded intron: {intron_start}-{intron_end}")
            count += 1

    return count


def process_subfeatures_file(
    session: Session,
    input_file: Path,
    created_by: str,
) -> dict:
    """
    Process subfeatures file.

    Args:
        session: Database session
        input_file: Input file path
        created_by: User name

    Returns:
        Statistics dict
    """
    stats = {
        "processed": 0,
        "subfeatures_created": 0,
        "not_found": 0,
        "errors": 0,
    }

    with open(input_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            parts = line.split('\t')
            if len(parts) < 7:
                logger.warning(f"Invalid line format: {line}")
                stats["errors"] += 1
                continue

            feature_name = parts[0]
            # note = parts[1]  # unused
            # chromosome = parts[2]  # unused
            start = int(parts[3])
            stop = int(parts[4])
            strand = parts[5]
            segments_str = parts[6]

            logger.info(f"Processing {feature_name}")

            feature = find_feature_by_name(session, feature_name)
            if not feature:
                logger.warning(f"Feature not found: {feature_name}")
                stats["not_found"] += 1
                continue

            # Parse segments
            segments = parse_segments(segments_str)
            if not segments:
                logger.warning(f"No valid segments for {feature_name}")
                stats["errors"] += 1
                continue

            # Insert subfeatures
            try:
                count = insert_subfeatures(
                    session, feature, start, stop, strand, segments, created_by
                )
                stats["subfeatures_created"] += count
                stats["processed"] += 1
            except Exception as e:
                logger.error(f"Error processing {feature_name}: {e}")
                stats["errors"] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load subfeatures (exons/introns) for features"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input file with subfeature coordinates",
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

    # Validate input
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    try:
        with SessionLocal() as session:
            stats = process_subfeatures_file(
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
            logger.info(f"  Features processed: {stats['processed']}")
            logger.info(f"  Subfeatures created: {stats['subfeatures_created']}")
            logger.info(f"  Not found: {stats['not_found']}")
            logger.info(f"  Errors: {stats['errors']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
