#!/usr/bin/env python3
"""
Merge individual InterProScan TSV results into a single file,
using the filename as the protein ID (replacing EMBOSS_* IDs).

Usage:
    python merge_iprscan_results.py --input-dir DIR --output FILE
"""
import argparse
import os
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Merge InterProScan TSV results")
    parser.add_argument("--input-dir", required=True, type=Path,
                        help="Directory containing individual TSV files")
    parser.add_argument("--output", required=True, type=Path,
                        help="Output merged TSV file")
    args = parser.parse_args()

    tsv_files = sorted(args.input_dir.glob("*.tsv"))
    print(f"Found {len(tsv_files)} TSV files")

    lines_written = 0
    with open(args.output, 'w') as out:
        for tsv_file in tsv_files:
            # Extract protein ID from filename (e.g., EER30087.1.tsv -> EER30087.1)
            protein_id = tsv_file.stem  # removes .tsv extension

            with open(tsv_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    # Split by tab and replace first field with correct protein ID
                    fields = line.split('\t')
                    if fields:
                        fields[0] = protein_id
                        out.write('\t'.join(fields) + '\n')
                        lines_written += 1

    print(f"Written {lines_written} lines to {args.output}")


if __name__ == "__main__":
    main()
