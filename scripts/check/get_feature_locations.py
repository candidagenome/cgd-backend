#!/usr/bin/env python3
"""
Get feature locations from database.

This script retrieves genomic coordinates for features from the database
and outputs them in various formats.

Original Perl: getDB_Ca19_location_ForList.pl, getDB_Ca20_location_ForList.pl
Converted to Python: 2024
"""

import argparse
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Feature, FeatLocation, Organism

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


def load_feature_list(input_file: Path) -> list[str]:
    """
    Load feature names from file.

    Args:
        input_file: File with feature names

    Returns:
        List of feature names
    """
    features = []

    with open(input_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            # Extract first column (feature name)
            parts = line.split('\t')
            feature_name = parts[0].strip()

            # Try to extract ORF pattern if present
            match = re.search(r'(orf\d+\.\d+\.?\d*)', feature_name, re.IGNORECASE)
            if match:
                features.append(match.group(1))
            else:
                features.append(feature_name)

    return features


def get_feature_location(
    session: Session,
    feature_name: str,
) -> dict | None:
    """
    Get location for a feature.

    Args:
        session: Database session
        feature_name: Feature name

    Returns:
        Location dict or None
    """
    feature = session.query(Feature).filter(
        Feature.feature_name == feature_name
    ).first()

    if not feature:
        return None

    # Get current location
    location = session.query(FeatLocation).filter(
        and_(
            FeatLocation.feature_no == feature.feature_no,
            FeatLocation.is_loc_current == 'Y',
        )
    ).first()

    result = {
        'feature_name': feature.feature_name,
        'feature_no': feature.feature_no,
        'feature_type': feature.feature_type,
    }

    if location:
        result.update({
            'chromosome': location.chromosome,
            'start_coord': location.start_coord,
            'stop_coord': location.stop_coord,
            'strand': location.strand,
            'contig': location.contig,
        })

        # Format location string
        chr_name = location.chromosome or location.contig or 'unknown'
        result['location_string'] = (
            f"{chr_name}:{location.start_coord}-{location.stop_coord}"
            f"({location.strand})"
        )
    else:
        result.update({
            'chromosome': None,
            'start_coord': None,
            'stop_coord': None,
            'strand': None,
            'contig': None,
            'location_string': 'no_location',
        })

    return result


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Get feature locations from database"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="File with feature names",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "--format",
        choices=['tsv', 'bed', 'gff'],
        default='tsv',
        help="Output format (default: tsv)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    logger.info(f"Started at {datetime.now()}")

    # Validate input
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    # Load feature list
    feature_names = load_feature_list(args.input_file)
    logger.info(f"Loaded {len(feature_names)} feature names")

    stats = {
        'found': 0,
        'with_location': 0,
        'not_found': 0,
    }

    try:
        with SessionLocal() as session:
            locations = []

            for feature_name in feature_names:
                loc = get_feature_location(session, feature_name)

                if loc:
                    stats['found'] += 1
                    if loc['start_coord']:
                        stats['with_location'] += 1
                    locations.append(loc)
                else:
                    stats['not_found'] += 1
                    logger.warning(f"Feature not found: {feature_name}")

            # Output
            out_handle = open(args.output, 'w') if args.output else sys.stdout

            try:
                if args.format == 'tsv':
                    out_handle.write(
                        "feature_name\tchromosome\tstart\tstop\tstrand\tlocation_string\n"
                    )
                    for loc in locations:
                        out_handle.write(
                            f"{loc['feature_name']}\t"
                            f"{loc['chromosome'] or ''}\t"
                            f"{loc['start_coord'] or ''}\t"
                            f"{loc['stop_coord'] or ''}\t"
                            f"{loc['strand'] or ''}\t"
                            f"{loc['location_string']}\n"
                        )

                elif args.format == 'bed':
                    for loc in locations:
                        if loc['start_coord']:
                            # BED is 0-based, half-open
                            start = loc['start_coord'] - 1
                            out_handle.write(
                                f"{loc['chromosome'] or 'unknown'}\t"
                                f"{start}\t{loc['stop_coord']}\t"
                                f"{loc['feature_name']}\t0\t"
                                f"{'+' if loc['strand'] == 'W' else '-'}\n"
                            )

                elif args.format == 'gff':
                    out_handle.write("##gff-version 3\n")
                    for loc in locations:
                        if loc['start_coord']:
                            strand = '+' if loc['strand'] == 'W' else '-'
                            out_handle.write(
                                f"{loc['chromosome'] or 'unknown'}\t"
                                f"CGD\t{loc['feature_type']}\t"
                                f"{loc['start_coord']}\t{loc['stop_coord']}\t"
                                f".\t{strand}\t.\t"
                                f"ID={loc['feature_name']}\n"
                            )

            finally:
                if args.output:
                    out_handle.close()

            logger.info("=" * 50)
            logger.info("Summary:")
            logger.info(f"  Total features: {len(feature_names)}")
            logger.info(f"  Found: {stats['found']}")
            logger.info(f"  With location: {stats['with_location']}")
            logger.info(f"  Not found: {stats['not_found']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
