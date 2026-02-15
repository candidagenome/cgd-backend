#!/usr/bin/env python3
"""
Update sequence files for PatMatch program.

This script prepares sequence files for use with the PatMatch pattern matching
tool. It copies sequence files from download locations, decompresses them,
removes stop codons from protein sequences, and counts the number of sequences.

Based on updatePatmatchDataset.pl by Mike Cherry and Shuai Weng (Oct 1997).
Updated for CGD by Prachi Shah (May 2007).

Usage:
    python update_patmatch_dataset.py
    python update_patmatch_dataset.py --config /path/to/patmatch_config.json

Environment Variables:
    DATA_DIR: Base data directory
    PATMATCH_DIR: Directory for patmatch data files
    LOG_DIR: Directory for log files
"""

import argparse
import gzip
import json
import logging
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration from environment
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
PATMATCH_DIR = Path(os.getenv("PATMATCH_DIR", DATA_DIR / "patmatch"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Default dataset configuration
# Maps dataset names to source files (gzipped) and output files
# This should be configured via JSON file or environment in production
DEFAULT_DATASETS = {
    # Example:
    # "orf_trans": {
    #     "source": "/path/to/download/orf_trans_all.fasta.gz",
    #     "output": "orf_trans_all.fasta"
    # },
}


def decompress_gzip_file(gzipped_file: Path, output_file: Path) -> None:
    """Decompress a gzipped file."""
    logger.info(f"Decompressing {gzipped_file} to {output_file}")
    with gzip.open(gzipped_file, "rb") as f_in:
        with open(output_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


def process_fasta_file(
    input_file: Path,
    output_file: Path,
    dataset_name: str,
) -> int:
    """
    Process a FASTA file for PatMatch:
    - Remove stop codons (*) from sequences
    - Clean up header lines
    - Count sequences

    Args:
        input_file: Path to input FASTA file
        output_file: Path to output FASTA file
        dataset_name: Name of the dataset (for special handling)

    Returns:
        Number of sequences processed
    """
    logger.info(f"Processing {input_file}")

    seq_count = 0
    is_not_feature = "not_feature" in dataset_name.lower()

    with open(input_file) as f_in, open(output_file, "w") as f_out:
        first_header = True

        for line in f_in:
            line = line.rstrip("\n\r")

            if line.startswith(">"):
                seq_count += 1
                header = line

                # Clean up header - various patterns
                # Pattern: >ID Gene Assembly N, Chr:start-endW/C
                match = re.match(
                    r"^>(\S+) (\S+) (\S+) Assembly (\d+), (\S+).+(\d+)-(\d+)([WC])",
                    header,
                )
                if match:
                    header = f">{match.group(1)} {match.group(2)} {match.group(3)} Assembly {match.group(4)}, {match.group(5)}:{match.group(6)}-{match.group(7)}{match.group(8)}"
                else:
                    # Pattern: > Assembly N, Chr ...
                    match = re.match(r"^>\s*Assembly \d+, (\S+) ", header)
                    if match:
                        header = f">{match.group(1)}"
                    else:
                        # Remove leading whitespace after >
                        header = re.sub(r"^>\s+(.+)", r">\1", header)

                # For intergenic files, merge spaces in header to underscores
                if is_not_feature:
                    header = header.replace(" ", "_")

                # Write header with proper newlines
                if not first_header:
                    f_out.write(f"\n{header}\n")
                else:
                    f_out.write(f"{header}\n")
                    first_header = False
            else:
                # Remove terminal stop codon (*)
                line = line.rstrip("*")
                f_out.write(line)

        # Final newline
        f_out.write("\n")

    logger.info(f"Processed {seq_count} sequences from {dataset_name}")
    return seq_count


def update_patmatch_datasets(
    datasets: dict,
    output_dir: Path,
) -> dict[str, int]:
    """
    Update all PatMatch datasets.

    Args:
        datasets: Dictionary of dataset configurations
        output_dir: Output directory for processed files

    Returns:
        Dictionary mapping dataset names to sequence counts
    """
    # Ensure directories exist
    output_dir.mkdir(parents=True, exist_ok=True)
    fasta_dir = output_dir / "fasta"
    fasta_dir.mkdir(parents=True, exist_ok=True)

    seq_counts = {}

    for dataset_name, config in datasets.items():
        source_file = Path(config["source"])
        output_filename = config["output"]

        if not source_file.exists():
            logger.warning(f"Source file not found: {source_file}")
            continue

        logger.info(f"Processing dataset: {dataset_name}")

        # Copy and decompress
        temp_gz = fasta_dir / source_file.name
        shutil.copy2(source_file, temp_gz)

        # Decompress if gzipped
        if temp_gz.suffix == ".gz":
            temp_file = fasta_dir / temp_gz.stem
            decompress_gzip_file(temp_gz, temp_file)
            temp_gz.unlink()
        else:
            temp_file = temp_gz

        # Process FASTA file
        output_file = output_dir / output_filename
        temp_output = output_dir / f"{output_filename}.tmp"

        count = process_fasta_file(temp_file, temp_output, dataset_name)
        seq_counts[dataset_name] = count

        # Move temp to final
        temp_output.rename(output_file)

        # Clean up temp file
        if temp_file.exists():
            temp_file.unlink()

    # Write sequence counts file
    count_file = output_dir / "seq.count"
    with open(count_file, "w") as f:
        for dataset_name, count in seq_counts.items():
            f.write(f"{dataset_name} {count}\n")

    logger.info(f"Wrote sequence counts to {count_file}")

    # Clean up fasta directory
    for f in fasta_dir.iterdir():
        f.unlink()

    return seq_counts


def load_dataset_config(config_file: Path | None) -> dict:
    """Load dataset configuration from JSON file."""
    if config_file and config_file.exists():
        with open(config_file) as f:
            return json.load(f)
    return DEFAULT_DATASETS


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update sequence files for PatMatch program"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="JSON config file with dataset definitions",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PATMATCH_DIR,
        help=f"Output directory for PatMatch files (default: {PATMATCH_DIR})",
    )
    parser.add_argument(
        "--source",
        type=Path,
        help="Single source file to process",
    )
    parser.add_argument(
        "--name",
        help="Dataset name (required with --source)",
    )
    parser.add_argument(
        "--output",
        help="Output filename (required with --source)",
    )

    args = parser.parse_args()

    # Set up file logging
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "update_patmatch_dataset.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Starting PatMatch dataset update at {datetime.now()}")

    try:
        # Single file mode
        if args.source:
            if not args.name or not args.output:
                logger.error("--name and --output are required with --source")
                return 1

            datasets = {
                args.name: {
                    "source": str(args.source),
                    "output": args.output,
                }
            }
        else:
            # Batch mode using config file
            datasets = load_dataset_config(args.config)

            if not datasets:
                logger.warning("No datasets configured. Use --config or --source options.")
                logger.info("Example config file format:")
                logger.info(json.dumps({
                    "orf_trans": {
                        "source": "/path/to/orf_trans_all.fasta.gz",
                        "output": "orf_trans_all.fasta",
                    },
                    "orf_coding": {
                        "source": "/path/to/orf_coding.fasta.gz",
                        "output": "orf_coding.fasta",
                    },
                }, indent=2))
                return 0

        # Process datasets
        seq_counts = update_patmatch_datasets(datasets, args.output_dir)

        # Summary
        logger.info("Summary:")
        for name, count in seq_counts.items():
            logger.info(f"  {name}: {count} sequences")

        logger.info(f"Completed at {datetime.now()}")
        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        return 1

    finally:
        logger.removeHandler(file_handler)
        file_handler.close()


if __name__ == "__main__":
    sys.exit(main())
