#!/usr/bin/env python3
"""
Load Assembly 21 (A21) chromosome locations for features.

This script loads Assembly 21 coordinates for various feature types
(tRNA, centromere, rDNA genes, etc.) into the FEATURE_A21 table.

Input file format (tab-delimited):
- For tRNAs: A20_location<TAB>A21_location<TAB>feature_name
- For CEN/rDNA: feature_name<TAB>A21_location

Location format: Ca21Chr[chr_id]:[coords][strand]
Where coords can be: start-stop or start-stop,start-stop (for multiple exons)
And strand is W (Watson) or C (Crick)

Original Perl: loadtRNAs_A21.pl, loadCEN_A21.pl, loadRDNgenes_A21.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Feature, FeatureA21

load_dotenv()

logger = logging.getLogger(__name__)

# Feature type mappings
FEATURE_TYPE_MAIN = {
    "trna": "tRNA",
    "cen": "CEN",
    "rdna": "rRNA",
    "rna": "RNA",
    "orf": "ORF",
}


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


def parse_a21_location(loc_string: str) -> dict | None:
    """
    Parse A21 location string.

    Args:
        loc_string: Location string like "Ca21Chr1:1000-2000W" or
                   "Ca21Chr1:1000-1500,1600-2000W"

    Returns:
        Dictionary with chromosome, coords, strand, and segments
    """
    # Pattern: Ca21Chr[chr_id]:[coords][strand]
    match = re.match(
        r"Ca21Chr([^:]+):(.+)(W|C)$",
        loc_string,
        re.IGNORECASE
    )

    if not match:
        return None

    chr_id = match.group(1)
    coords_str = match.group(2)
    strand = match.group(3).upper()

    # Parse segments (comma-separated)
    segments = []
    for seg in coords_str.split(","):
        parts = seg.split("-")
        if len(parts) == 2:
            segments.append((int(parts[0]), int(parts[1])))

    if not segments:
        return None

    # Calculate overall start and stop
    start = segments[0][0]
    stop = segments[-1][1]

    return {
        "chromosome": chr_id,
        "start": start,
        "stop": stop,
        "strand": strand,
        "segments": segments,
    }


def find_feature_by_name(session: Session, name: str) -> Feature | None:
    """
    Find feature by feature_name.

    Args:
        session: Database session
        name: Feature name to search

    Returns:
        Feature object or None
    """
    return session.query(Feature).filter(
        Feature.feature_name == name
    ).first()


def feature_a21_exists(
    session: Session,
    feature_no: int,
    feature_type: str,
) -> bool:
    """
    Check if Feature_A21 entry already exists.

    Args:
        session: Database session
        feature_no: Feature number
        feature_type: Feature A21 type

    Returns:
        True if exists, False otherwise
    """
    existing = session.query(FeatureA21).filter(
        and_(
            FeatureA21.feature_no == feature_no,
            FeatureA21.feature_a21_type == feature_type,
        )
    ).first()
    return existing is not None


def insert_a21_location(
    session: Session,
    feature_no: int,
    chromosome: str,
    start: int,
    stop: int,
    strand: str,
    feature_type: str,
    created_by: str,
) -> bool:
    """
    Insert Feature_A21 location.

    Args:
        session: Database session
        feature_no: Feature number
        chromosome: Chromosome identifier
        start: Start coordinate
        stop: Stop coordinate
        strand: Strand (W or C)
        feature_type: Type of feature location
        created_by: User creating the record

    Returns:
        True if inserted, False if already existed
    """
    # Check if already exists
    if feature_a21_exists(session, feature_no, feature_type):
        logger.debug(
            f"A21 {feature_type} location for feature_no {feature_no} "
            "already exists"
        )
        return False

    new_entry = FeatureA21(
        feature_no=feature_no,
        chromosome=chromosome,
        start_coord=start,
        stop_coord=stop,
        strand=strand,
        feature_a21_type=feature_type,
        created_by=created_by[:12],
    )
    session.add(new_entry)
    return True


def parse_input_file(
    filepath: Path,
    file_format: str,
) -> list[dict]:
    """
    Parse input file based on format.

    Args:
        filepath: Path to input file
        file_format: Format type ('trna', 'cen', 'rdna')

    Returns:
        List of entry dictionaries
    """
    entries = []

    with open(filepath) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")

            if file_format == "trna":
                # Format: A20_location<TAB>A21_location<TAB>feature_name
                # Skip header
                if line_num == 1:
                    continue

                if len(parts) < 3:
                    logger.warning(f"Line {line_num}: Invalid format")
                    continue

                a21_loc = parts[1]
                name = parts[2]
            else:
                # Format: feature_name<TAB>A21_location
                if len(parts) < 2:
                    logger.warning(f"Line {line_num}: Invalid format")
                    continue

                name = parts[0]
                a21_loc = parts[1]

            location = parse_a21_location(a21_loc)
            if not location:
                logger.warning(
                    f"Line {line_num}: Cannot parse location '{a21_loc}'"
                )
                continue

            entries.append({
                "name": name,
                "location": location,
            })

    logger.info(f"Parsed {len(entries)} entries from input file")
    return entries


def load_a21_locations(
    session: Session,
    entries: list[dict],
    feature_format: str,
    created_by: str,
) -> dict:
    """
    Load A21 locations into the database.

    Args:
        session: Database session
        entries: List of entry dictionaries
        feature_format: Feature format type
        created_by: User creating the records

    Returns:
        Dictionary with statistics
    """
    stats = {
        "entries_processed": 0,
        "main_locations_created": 0,
        "subfeature_locations_created": 0,
        "features_not_found": 0,
        "duplicates_skipped": 0,
    }

    for entry in entries:
        name = entry["name"]
        loc = entry["location"]

        feature = find_feature_by_name(session, name)
        if not feature:
            logger.warning(f"Cannot find feature: {name}")
            stats["features_not_found"] += 1
            continue

        stats["entries_processed"] += 1

        # Determine main feature type
        if feature_format == "trna":
            main_type = "tRNA"
        elif feature_format == "cen":
            main_type = "CEN"
        elif feature_format == "rdna":
            # RDN genes get rRNA, others get RNA
            main_type = "rRNA" if "RDN" in name.upper() else "RNA"
        else:
            main_type = "ORF"

        # Insert main feature location
        if insert_a21_location(
            session,
            feature.feature_no,
            loc["chromosome"],
            loc["start"],
            loc["stop"],
            loc["strand"],
            main_type,
            created_by,
        ):
            stats["main_locations_created"] += 1
            logger.info(
                f"Created {main_type} location for {name}: "
                f"chr{loc['chromosome']}:{loc['start']}-{loc['stop']}{loc['strand']}"
            )
        else:
            stats["duplicates_skipped"] += 1

        # Insert subfeature locations (exons and introns) for multi-exon features
        segments = loc["segments"]
        if len(segments) >= 1:
            exon_type = "Non-coding exon"  # For tRNA, CEN, rDNA

            for i, (beg, end) in enumerate(segments):
                # Insert exon
                if insert_a21_location(
                    session,
                    feature.feature_no,
                    loc["chromosome"],
                    beg,
                    end,
                    loc["strand"],
                    exon_type,
                    created_by,
                ):
                    stats["subfeature_locations_created"] += 1

                # Insert intron between exons
                if i < len(segments) - 1:
                    next_beg, next_end = segments[i + 1]
                    if loc["strand"] == "W":
                        intron_start = end + 1
                        intron_stop = next_beg - 1
                    else:
                        intron_start = end - 1
                        intron_stop = next_beg + 1

                    if insert_a21_location(
                        session,
                        feature.feature_no,
                        loc["chromosome"],
                        intron_start,
                        intron_stop,
                        loc["strand"],
                        "Intron",
                        created_by,
                    ):
                        stats["subfeature_locations_created"] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load Assembly 21 chromosome locations for features"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input file with feature names and A21 locations",
    )
    parser.add_argument(
        "--format",
        choices=["trna", "cen", "rdna"],
        required=True,
        help="Input file format (trna, cen, or rdna)",
    )
    parser.add_argument(
        "--created-by",
        default=os.getenv("DB_USER", "SCRIPT"),
        help="Database user name for created_by field",
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
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Format: {args.format}")
    logger.info(f"Created by: {args.created_by}")

    # Parse input file
    entries = parse_input_file(args.input_file, args.format)

    if not entries:
        logger.warning("No valid entries found in input file")
        return

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")
        logger.info(f"Would process {len(entries)} entries")
        for entry in entries[:5]:
            loc = entry["location"]
            logger.info(
                f"  {entry['name']}: chr{loc['chromosome']}:"
                f"{loc['start']}-{loc['stop']}{loc['strand']}"
            )
        if len(entries) > 5:
            logger.info(f"  ... and {len(entries) - 5} more")
        return

    try:
        with SessionLocal() as session:
            stats = load_a21_locations(
                session, entries, args.format, args.created_by
            )

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Entries processed: {stats['entries_processed']}")
            logger.info(
                f"  Main locations created: {stats['main_locations_created']}"
            )
            logger.info(
                f"  Subfeature locations created: "
                f"{stats['subfeature_locations_created']}"
            )
            if stats["features_not_found"] > 0:
                logger.warning(
                    f"  Features not found: {stats['features_not_found']}"
                )
            if stats["duplicates_skipped"] > 0:
                logger.info(
                    f"  Duplicates skipped: {stats['duplicates_skipped']}"
                )
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading A21 locations: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
