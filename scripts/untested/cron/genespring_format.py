#!/usr/bin/env python3
"""
Generate GeneSpring format file from gene_association file.

This script reads the gene_association file and generates a tab-delimited file
where each gene is on a single line with all its GO IDs in a pipe-delimited list.

Based on genespringFormat5.pl by Martha Arnaud (June 2008).

Usage:
    python genespring_format.py
    python genespring_format.py --input /path/to/gene_association.cgd.gz

Environment Variables:
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    HTML_ROOT_DIR: Root directory for download files
    LOG_DIR: Directory for log files
"""

import argparse
import gzip
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Load environment variables
load_dotenv()

# Configuration from environment
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def read_gene_association(
    gaf_file: Path,
) -> dict[str, set[str]]:
    """
    Read gene_association file and collect GO IDs per gene.

    Returns dict mapping gene name to set of GO IDs.
    """
    gene_goids: dict[str, set[str]] = defaultdict(set)

    # Handle gzipped or plain file
    if gaf_file.suffix == ".gz":
        opener = gzip.open
        mode = "rt"
    else:
        opener = open
        mode = "r"

    with opener(gaf_file, mode, encoding="utf-8") as f:
        for line in f:
            # Skip header lines
            if line.startswith("!"):
                continue

            if not line.strip():
                continue

            parts = line.strip().split("\t")
            if len(parts) < 11:
                continue

            # Column 11 (index 10) contains aliases
            # First alias is typically the ORF19 name
            aliases = parts[10]
            goid = parts[4]  # Column 5: GO ID

            # Get the first alias (ORF19 name)
            gene_name = aliases.split("|")[0] if aliases else None

            if gene_name and goid:
                gene_goids[gene_name].add(goid)

    return gene_goids


def write_genespring_file(
    gene_goids: dict[str, set[str]],
    output_file: Path,
) -> int:
    """
    Write GeneSpring format file.

    Returns count of genes written.
    """
    count = 0

    with open(output_file, "w") as f:
        for gene_name in sorted(gene_goids.keys()):
            goids = gene_goids[gene_name]
            if goids:
                goid_str = "|".join(sorted(goids))
                f.write(f"{gene_name}\t{goid_str}\n")
                count += 1

    return count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate GeneSpring format file from gene_association"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Input gene_association file (gzipped)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output file path",
    )

    args = parser.parse_args()

    # Determine input file
    if args.input:
        input_file = args.input
    else:
        input_file = (
            HTML_ROOT_DIR / "download" / "go" /
            f"gene_association.{PROJECT_ACRONYM.lower()}.gz"
        )

    # Determine output file
    if args.output:
        output_file = args.output
    else:
        output_file = (
            HTML_ROOT_DIR / "download" / "misc" /
            f"{PROJECT_ACRONYM}_GO_genespring_format.tab"
        )

    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Reading gene_association file: {input_file}")

    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        return 1

    try:
        # Read gene association file
        gene_goids = read_gene_association(input_file)
        logger.info(f"Found {len(gene_goids)} genes with GO annotations")

        # Write output file
        count = write_genespring_file(gene_goids, output_file)
        logger.info(f"Wrote {count} genes to {output_file}")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
