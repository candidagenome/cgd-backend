#!/usr/bin/env python3
"""
Load chromosome coordinates for tRNA features.

This script updates tRNA features with chromosome location information
(chromosome, start_coord, stop_coord, strand) from a mapping file.

Input file format (tab-delimited):
- Column 1: Feature name (tRNA name)
- Column 2: Chromosome coordinates (e.g., Ca20chr1:1000-1100W)

Coordinate format: Ca[version]chr[chr_id]:[start]-[stop][strand]
Where strand is W (Watson/forward) or C (Crick/reverse)

Note: This script updates the Feature table fields. The original Perl
script also inserted subfeatures, which requires models not currently
available.

Original Perl: loadChrCoordsForTRNAs.pl
Author: Prachi Shah (Oct 5, 2006)
Converted to Python: 2024
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Feature

load_dotenv()

logger = logging.getLogger(__name__)


def setup_logging(log_file: Path = None, verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler()]

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def parse_coordinates(coord_string: str) -> dict | None:
    """
    Parse chromosome coordinate string.

    Args:
        coord_string: Coordinate string like "Ca20chr1:1000-1100,1200-1300W"

    Returns:
        Dictionary with chr, start, stop, strand, segments or None if parse fails
    """
    # Pattern: Ca[version]chr[chr_id]:[segments][strand]
    # Segments can be comma-separated for multi-exon features
    match = re.match(
        r"Ca\d+chr([\dR]+):([\d+\-,]+)(W|C)",
        coord_string,
        re.IGNORECASE
    )

    if not match:
        return None

    chr_id = match.group(1)
    segments_str = match.group(2)
    strand = match.group(3).upper()

    # Parse segments (could be "1000-1100" or "1000-1100,1200-1300")
    segments = []
    for seg in segments_str.split(","):
        parts = seg.split("-")
        if len(parts) == 2:
            segments.append((int(parts[0]), int(parts[1])))

    if not segments:
        return None

    # Calculate overall start and stop
    all_coords = [c for seg in segments for c in seg]
    start = min(all_coords)
    stop = max(all_coords)

    # For Crick strand, start > stop in the original format
    if strand == "C":
        start, stop = max(all_coords), min(all_coords)

    return {
        "chromosome": chr_id,
        "start": segments[0][0],  # First coordinate of first segment
        "stop": segments[-1][1],   # Last coordinate of last segment
        "strand": strand,
        "segments": segments,
    }


def parse_mapping_file(filepath: Path) -> list[dict]:
    """
    Parse tRNA chromosome mapping file.

    Args:
        filepath: Path to mapping file

    Returns:
        List of dictionaries with name and coordinate info
    """
    entries = []

    with open(filepath) as f:
        for line_num, line in enumerate(f, 1):
            # Skip header
            if line_num == 1:
                continue

            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) < 2:
                logger.warning(f"Line {line_num}: Invalid format, skipping")
                continue

            name = parts[0].strip()
            coords_str = parts[1].strip()

            coords = parse_coordinates(coords_str)
            if not coords:
                logger.warning(
                    f"Line {line_num}: Could not parse coordinates '{coords_str}'"
                )
                continue

            entries.append({
                "name": name,
                "coords_str": coords_str,
                **coords,
            })

    logger.info(f"Parsed {len(entries)} entries from mapping file")
    return entries


def update_feature_coordinates(
    session: Session,
    entries: list[dict],
) -> dict:
    """
    Update feature chromosome coordinates.

    Args:
        session: Database session
        entries: List of entry dictionaries

    Returns:
        Dictionary with statistics
    """
    stats = {
        "entries_processed": 0,
        "features_updated": 0,
        "features_not_found": 0,
        "errors": [],
    }

    for entry in entries:
        name = entry["name"]
        chromosome = entry["chromosome"]
        start = entry["start"]
        stop = entry["stop"]
        strand = entry["strand"]

        feature = session.query(Feature).filter(
            Feature.feature_name == name
        ).first()

        if not feature:
            logger.warning(f"Cannot find feature: {name}")
            stats["features_not_found"] += 1
            continue

        stats["entries_processed"] += 1

        try:
            # Update feature coordinates
            feature.chromosome = chromosome
            feature.start_coord = start
            feature.stop_coord = stop
            feature.strand = strand

            # Set is_on_pmap to Y if we have valid coordinates
            if chromosome and start and stop:
                feature.is_on_pmap = "Y"

            stats["features_updated"] += 1
            logger.info(
                f"Updated {name}: chr{chromosome}:{start}-{stop}{strand}"
            )

        except Exception as e:
            error_msg = f"Error updating {name}: {e}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load chromosome coordinates for tRNA features"
    )
    parser.add_argument(
        "mapping_file",
        type=Path,
        help="Input mapping file (tab-delimited: name, coordinates)",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Path to log file",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse file but don't modify database",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_file, args.verbose)

    # Validate input file
    if not args.mapping_file.exists():
        logger.error(f"Mapping file not found: {args.mapping_file}")
        sys.exit(1)

    logger.info(f"Mapping file: {args.mapping_file}")

    # Parse mapping file
    entries = parse_mapping_file(args.mapping_file)

    if not entries:
        logger.warning("No valid entries found in mapping file")
        return

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")
        logger.info(f"Would update {len(entries)} features")
        for entry in entries[:5]:
            logger.info(
                f"  {entry['name']}: chr{entry['chromosome']}:"
                f"{entry['start']}-{entry['stop']}{entry['strand']}"
            )
        if len(entries) > 5:
            logger.info(f"  ... and {len(entries) - 5} more")
        return

    try:
        with SessionLocal() as session:
            stats = update_feature_coordinates(session, entries)

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Update Summary:")
            logger.info(f"  Entries processed: {stats['entries_processed']}")
            logger.info(f"  Features updated: {stats['features_updated']}")
            if stats["features_not_found"] > 0:
                logger.warning(
                    f"  Features not found: {stats['features_not_found']}"
                )
            if stats["errors"]:
                logger.error(f"  Errors: {len(stats['errors'])}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error updating coordinates: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
