#!/usr/bin/env python3
"""
Generate Chibana to ORF name mapping from GenBank file.

This utility script parses a GenBank chromosome 7 sequence file and generates
a mapping between Chibana gene names and ORF locus tags.

The script outputs two files:
1. Good mappings: Chibana_name<TAB>ORF_name
2. Questionable mappings: Cases with multiple locus tags or missing data

This is a data preparation script, not a database loading script.

Original Perl: getChibanaToORFMapping.pl
Converted to Python: 2024
"""

import argparse
import logging
import re
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def parse_genbank_file(filepath: Path) -> tuple[dict, dict, dict]:
    """
    Parse GenBank file and extract gene/locus_tag mappings.

    Args:
        filepath: Path to GenBank file

    Returns:
        Tuple of (chibana_to_orfs, bad_mappings, translations)
    """
    try:
        from Bio import SeqIO
    except ImportError:
        logger.error("BioPython is required for this script.")
        logger.error("Install with: pip install biopython")
        sys.exit(1)

    chibana_to_orfs = {}
    bad_mappings = {}
    translations = {}

    for record in SeqIO.parse(filepath, "genbank"):
        for feature in record.features:
            # Only process CDS features
            if feature.type != "CDS":
                continue

            chibana_name = None
            locus_tag = None
            translation = None

            # Extract relevant qualifiers
            if "gene" in feature.qualifiers:
                chibana_name = feature.qualifiers["gene"][0]
                if chibana_name not in chibana_to_orfs:
                    chibana_to_orfs[chibana_name] = ""

            if "locus_tag" in feature.qualifiers:
                locus_tag = feature.qualifiers["locus_tag"][0]

            if "translation" in feature.qualifiers:
                translation = feature.qualifiers["translation"][0]

            if not chibana_name:
                if locus_tag:
                    logger.warning(
                        f"locus_tag '{locus_tag}' encountered before gene name"
                    )
                continue

            # Handle locus_tag
            if locus_tag:
                if chibana_to_orfs[chibana_name]:
                    # Multiple locus tags
                    logger.debug(
                        f"Multiple locus tags for gene {chibana_name}. Marked BAD"
                    )
                    bad_mappings[chibana_name] = True
                    chibana_to_orfs[chibana_name] += "::" + locus_tag
                else:
                    chibana_to_orfs[chibana_name] = locus_tag
            else:
                logger.debug(f"No locus_tag for gene {chibana_name}. Marked BAD")
                bad_mappings[chibana_name] = True

            if translation:
                translations[chibana_name] = translation

        # Check for missing locus_tags
        for name, orf in chibana_to_orfs.items():
            if not orf:
                logger.debug(f"Missing locus_tag for gene {name}. Marked BAD")
                bad_mappings[name] = True

    logger.info(f"Parsed {len(chibana_to_orfs)} gene entries from GenBank file")
    return chibana_to_orfs, bad_mappings, translations


def write_mappings(
    chibana_to_orfs: dict,
    bad_mappings: dict,
    translations: dict,
    output_file: Path,
    bad_file: Path,
) -> dict:
    """
    Write mapping files.

    Args:
        chibana_to_orfs: Gene to ORF mapping
        bad_mappings: Set of problematic genes
        translations: Gene translations
        output_file: Path for good mappings
        bad_file: Path for questionable mappings

    Returns:
        Statistics dictionary
    """
    stats = {
        "good_mappings": 0,
        "bad_mappings": 0,
    }

    # Ensure output directories exist
    output_file.parent.mkdir(parents=True, exist_ok=True)
    bad_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w") as out, open(bad_file, "w") as bad:
        # Write headers
        out.write("ChibanaName\tORFname\n")
        bad.write("ChibanaName\tORFname(s)\tTranslation\n")

        for chibana_name in sorted(chibana_to_orfs.keys()):
            orf_name = chibana_to_orfs[chibana_name]

            # Convert CaO prefix to orf
            orf_name = re.sub(r"CaO", "orf", orf_name)

            if chibana_name in bad_mappings:
                translation = translations.get(chibana_name, "")
                bad.write(f"{chibana_name}\t{orf_name}\t{translation}\n")
                stats["bad_mappings"] += 1
            else:
                out.write(f"{chibana_name}\t{orf_name}\n")
                stats["good_mappings"] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate Chibana to ORF mapping from GenBank file"
    )
    parser.add_argument(
        "genbank_file",
        type=Path,
        help="Input GenBank file (chromosome 7 sequence)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("ChibanaORF_mappings"),
        help="Output file for good mappings (default: ChibanaORF_mappings)",
    )
    parser.add_argument(
        "--bad-output",
        type=Path,
        default=Path("check_QuestionableMappings.txt"),
        help="Output file for questionable mappings",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)

    # Validate input file
    if not args.genbank_file.exists():
        logger.error(f"GenBank file not found: {args.genbank_file}")
        sys.exit(1)

    logger.info(f"GenBank file: {args.genbank_file}")
    logger.info(f"Output file: {args.output}")
    logger.info(f"Bad mappings file: {args.bad_output}")

    # Parse GenBank file
    chibana_to_orfs, bad_mappings, translations = parse_genbank_file(
        args.genbank_file
    )

    # Write output files
    stats = write_mappings(
        chibana_to_orfs,
        bad_mappings,
        translations,
        args.output,
        args.bad_output,
    )

    logger.info("=" * 50)
    logger.info("Summary:")
    logger.info(f"  Good mappings: {stats['good_mappings']}")
    logger.info(f"  Questionable mappings: {stats['bad_mappings']}")
    logger.info("=" * 50)
    logger.info(
        "Please check the questionable mappings file for entries "
        "that may need manual review."
    )


if __name__ == "__main__":
    main()
