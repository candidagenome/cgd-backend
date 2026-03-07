#!/usr/bin/env python3
"""
Get C. glabrata orthologs from YGOB data.

This script extracts C. glabrata to S. cerevisiae ortholog mappings from
YGOB (Yeast Gene Order Browser) cluster data.

YGOB groups paralogs from whole genome duplication on the same line but
keeps them in separate columns.

Based on getCglab.pl.

Usage:
    python get_cglab.py
    python get_cglab.py --debug

Environment Variables:
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

# Load environment variables
load_dotenv()

# Configuration from environment
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))

# CGOB configuration
CGOB_DATA_DIR = DATA_DIR / "CGOB"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# YGOB column indices (0-based)
# YGOB groups paralogs from WGD on same line
CGLAB_A_IDX = 7   # C. glabrata A copy
CGLAB_B_IDX = 25  # C. glabrata B copy
SCER_A_IDX = 11   # S. cerevisiae A copy
SCER_B_IDX = 21   # S. cerevisiae B copy


def find_cglab_orthologs(ygob_file: Path) -> dict[str, str]:
    """
    Find C. glabrata orthologs from YGOB cluster file.

    Returns dict mapping S. cerevisiae ID to C. glabrata ID.
    """
    cglab_for_scer: dict[str, str] = {}

    with open(ygob_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            homologs = line.split("\t")

            # Pad list if shorter than expected
            while len(homologs) <= max(CGLAB_A_IDX, CGLAB_B_IDX, SCER_A_IDX, SCER_B_IDX):
                homologs.append("")

            # Extract orthologs for A and B copies
            assign_ortholog(
                homologs[SCER_A_IDX], homologs[CGLAB_A_IDX], cglab_for_scer
            )
            assign_ortholog(
                homologs[SCER_B_IDX], homologs[CGLAB_B_IDX], cglab_for_scer
            )

    return cglab_for_scer


def assign_ortholog(
    scer: str, cglab: str, cglab_for_scer: dict[str, str]
) -> None:
    """
    Assign ortholog mapping if both IDs are valid.

    S. cerevisiae IDs start with 'S' or 'Y'.
    C. glabrata IDs start with 'C'.
    """
    scer = scer.strip()
    cglab = cglab.strip()

    if scer and cglab:
        # Validate ID prefixes
        if (scer.startswith("S") or scer.startswith("Y")) and cglab.startswith("C"):
            cglab_for_scer[scer] = cglab


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Get C. glabrata orthologs from YGOB data"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )
    parser.add_argument(
        "--ygob-clusters",
        type=Path,
        default=None,
        help="Path to YGOB clusters file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output file path",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    # Set up log file
    log_file = LOG_DIR / "getCglab.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    log_text = f"Program {__file__}: Starting {datetime.now()}\n\n"

    # Determine input file
    ygob_clusters = args.ygob_clusters or (CGOB_DATA_DIR / "ygob_clusters.tab")

    if not ygob_clusters.exists():
        error_msg = f"YGOB clusters file does not exist: {ygob_clusters}"
        log_text += f"ERROR: {error_msg}\n"
        logger.error(error_msg)

        with open(log_file, "w") as f:
            f.write(log_text)

        return 1

    # Determine output file
    output_file = args.output or (CGOB_DATA_DIR / "cglab_orthologs.txt")
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Remove existing output
    if output_file.exists():
        output_file.unlink()

    # Find orthologs
    logger.info(f"Reading YGOB clusters from {ygob_clusters}")
    cglab_for_scer = find_cglab_orthologs(ygob_clusters)
    logger.info(f"Found {len(cglab_for_scer)} C. glabrata orthologs")

    # Write output
    with open(output_file, "w") as f:
        for scer, cglab in sorted(cglab_for_scer.items()):
            f.write(f"{cglab}\t{scer}\n")

    log_text += f"Creating file {output_file}\n"
    log_text += f"Found {len(cglab_for_scer)} ortholog pairs\n\n"
    log_text += f"Exiting {__file__}: {datetime.now()}\n\n"

    # Write log file
    with open(log_file, "w") as f:
        f.write(log_text)

    logger.info(f"Output written to {output_file}")
    logger.info(f"Log written to {log_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
