#!/usr/bin/env python3
"""
Fix InterProScan results by replacing EMBOSS_001 with correct protein IDs.

When InterProScan is run with fasta files that don't preserve sequence IDs,
it assigns generic names like EMBOSS_001. This script fixes the results by
extracting the protein ID from the filename.

Usage:
    python fix_interproscan_protein_ids.py --input-dir DIR --output FILE

Example:
    python fix_interproscan_protein_ids.py \
        --input-dir /data/C_tropicalis/interproscan_results \
        --output /data/C_tropicalis/interproscan_results/all_results_fixed.tsv
"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fix_interproscan_results(input_dir: Path, output_file: Path):
    """
    Fix InterProScan TSV results by replacing EMBOSS_001 with protein IDs from filenames.

    Args:
        input_dir: Directory containing individual TSV files named like EER30087.1.tsv
        output_file: Output file path for merged fixed results
    """
    # Find all TSV files
    tsv_files = sorted(input_dir.glob("*.tsv"))

    # Filter out any existing merged files
    tsv_files = [f for f in tsv_files if not f.name.startswith("all_results")]

    logger.info(f"Found {len(tsv_files)} TSV files in {input_dir}")

    fixed_count = 0
    total_lines = 0

    with open(output_file, 'w') as outf:
        for tsv_file in tsv_files:
            # Extract protein ID from filename (e.g., EER30087.1.tsv -> EER30087.1)
            protein_id = tsv_file.stem

            with open(tsv_file, 'r') as inf:
                for line in inf:
                    line = line.strip()
                    if not line:
                        continue

                    total_lines += 1
                    fields = line.split('\t')

                    # Replace first field (EMBOSS_001 or similar) with correct protein ID
                    if fields[0] != protein_id:
                        fields[0] = protein_id
                        fixed_count += 1

                    outf.write('\t'.join(fields) + '\n')

    logger.info(f"Processed {total_lines} lines from {len(tsv_files)} files")
    logger.info(f"Fixed {fixed_count} protein IDs")
    logger.info(f"Output written to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Fix InterProScan results by replacing EMBOSS_001 with correct protein IDs"
    )
    parser.add_argument(
        "--input-dir", required=True, type=Path,
        help="Directory containing individual TSV files (named like EER30087.1.tsv)"
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Output file path for merged fixed results"
    )
    args = parser.parse_args()

    if not args.input_dir.is_dir():
        logger.error(f"Input directory does not exist: {args.input_dir}")
        return 1

    logger.info("=" * 60)
    logger.info("Fixing InterProScan protein IDs")
    logger.info("=" * 60)

    fix_interproscan_results(args.input_dir, args.output)

    logger.info("Done!")
    return 0


if __name__ == "__main__":
    exit(main())
