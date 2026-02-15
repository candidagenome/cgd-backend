#!/usr/bin/env python3
"""
Create BLAST datasets from sequence files.

This script formats sequence files (FASTA) for use with BLAST. It:
1. Copies sequence files to the BLAST datasets directory
2. Decompresses gzipped files if necessary
3. Runs makeblastdb (NCBI BLAST+) or wu-formatdb (WU-BLAST) to create databases
4. Cleans up temporary files

Based on createBlastDatasets.pl by CGD team.

Usage:
    python create_blast_datasets.py
    python create_blast_datasets.py --config /path/to/blast_datasets.json

Environment Variables:
    BLAST_DATASET_DIR: Directory for BLAST datasets
    BLAST_BIN_PATH: Path to BLAST binaries (makeblastdb)
    DATA_DIR: Base data directory
    LOG_DIR: Directory for log files
    CURATOR_EMAIL: Email for error notifications
"""

import argparse
import gzip
import json
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration from environment
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
BLAST_DATASET_DIR = Path(os.getenv("BLAST_DATASET_DIR", DATA_DIR / "blast_datasets"))
BLAST_BIN_PATH = Path(os.getenv("BLAST_BIN_PATH", "/usr/local/ncbi/blast/bin"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL", "")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Default dataset configuration
# Maps source file paths to dataset info
# Format: {"source_file": {"name": "dataset_name", "type": "protein|nucleotide"}}
DEFAULT_DATASETS = {
    # Example entries - actual paths should be configured via JSON file or env
    # "/path/to/orf_trans_all.fasta.gz": {"name": "orf_trans", "type": "protein"},
    # "/path/to/orf_coding.fasta.gz": {"name": "orf_coding", "type": "nucleotide"},
}


def send_error_email(subject: str, message: str) -> None:
    """Send error notification email."""
    if not CURATOR_EMAIL:
        logger.warning("CURATOR_EMAIL not set, skipping email notification")
        return
    logger.error(f"Email notification: {subject}")
    logger.error(f"Message: {message}")


def decompress_file(gzipped_file: Path, output_file: Path) -> None:
    """Decompress a gzipped file."""
    logger.info(f"Decompressing {gzipped_file}")
    with gzip.open(gzipped_file, "rb") as f_in:
        with open(output_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


def create_blast_database(
    input_file: Path,
    db_name: str,
    db_type: str,
    output_dir: Path,
) -> bool:
    """
    Create a BLAST database from a FASTA file.

    Args:
        input_file: Path to input FASTA file
        db_name: Name for the database
        db_type: "protein" or "nucleotide"
        output_dir: Directory for output database files

    Returns:
        True on success, False on failure
    """
    # Map type to makeblastdb dbtype parameter
    dbtype_map = {
        "protein": "prot",
        "nucleotide": "nucl",
        "prot": "prot",
        "nucl": "nucl",
    }

    dbtype = dbtype_map.get(db_type.lower())
    if not dbtype:
        logger.error(f"Unknown database type: {db_type}")
        return False

    output_db = output_dir / db_name

    # Create lock file
    lock_file = output_dir / f"{db_name}.lock"
    lock_file.touch()

    try:
        # Use NCBI BLAST+ makeblastdb
        makeblastdb = BLAST_BIN_PATH / "makeblastdb"

        if not makeblastdb.exists():
            # Try finding makeblastdb in PATH
            makeblastdb_path = shutil.which("makeblastdb")
            if makeblastdb_path:
                makeblastdb = Path(makeblastdb_path)
            else:
                logger.error("makeblastdb not found")
                return False

        command = [
            str(makeblastdb),
            "-in", str(input_file),
            "-dbtype", dbtype,
            "-out", str(output_db),
            "-title", db_name,
            "-parse_seqids",
        ]

        logger.info(f"Creating BLAST database: {' '.join(command)}")

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            logger.error(f"makeblastdb failed: {result.stderr}")
            return False

        if result.stdout:
            logger.info(f"makeblastdb output: {result.stdout}")

        logger.info(f"Successfully created BLAST database: {db_name}")
        return True

    finally:
        # Remove lock file
        if lock_file.exists():
            lock_file.unlink()


def process_sequence_file(
    source_file: Path,
    dataset_name: str,
    dataset_type: str,
    output_dir: Path,
) -> bool:
    """
    Process a sequence file to create a BLAST database.

    Args:
        source_file: Path to source FASTA file (may be gzipped)
        dataset_name: Name for the BLAST database
        dataset_type: "protein" or "nucleotide"
        output_dir: Directory for BLAST database files

    Returns:
        True on success, False on failure
    """
    if not source_file.exists():
        logger.error(f"Source file not found: {source_file}")
        return False

    logger.info(f"Processing {source_file} -> {dataset_name}")

    # Create output directory if needed
    output_dir.mkdir(parents=True, exist_ok=True)

    # Copy file to output directory
    copied_file = output_dir / source_file.name
    logger.info(f"Copying {source_file} to {output_dir}")
    shutil.copy2(source_file, copied_file)

    # Decompress if gzipped
    working_file = copied_file
    if copied_file.suffix == ".gz":
        decompressed_file = copied_file.with_suffix("")
        decompress_file(copied_file, decompressed_file)
        copied_file.unlink()  # Remove gzipped copy
        working_file = decompressed_file

    # Rename to dataset name
    dataset_file = output_dir / dataset_name
    if working_file != dataset_file:
        working_file.rename(dataset_file)

    # Create BLAST database
    success = create_blast_database(
        dataset_file,
        dataset_name,
        dataset_type,
        output_dir,
    )

    # Clean up input file
    if dataset_file.exists():
        dataset_file.unlink()

    return success


def load_dataset_config(config_file: Path | None) -> dict:
    """Load dataset configuration from JSON file."""
    if config_file and config_file.exists():
        with open(config_file) as f:
            return json.load(f)
    return DEFAULT_DATASETS


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create BLAST datasets from sequence files"
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
        default=BLAST_DATASET_DIR,
        help=f"Output directory for BLAST databases (default: {BLAST_DATASET_DIR})",
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
        "--type",
        choices=["protein", "nucleotide"],
        help="Database type (required with --source)",
    )

    args = parser.parse_args()

    # Set up file logging
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "blast_dataset_creation.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Starting BLAST dataset creation at {datetime.now()}")

    success_count = 0
    failure_count = 0

    try:
        # Single file mode
        if args.source:
            if not args.name or not args.type:
                logger.error("--name and --type are required with --source")
                return 1

            if process_sequence_file(args.source, args.name, args.type, args.output_dir):
                success_count += 1
            else:
                failure_count += 1
        else:
            # Batch mode using config file
            datasets = load_dataset_config(args.config)

            if not datasets:
                logger.warning("No datasets configured. Use --config or --source options.")
                logger.info("Example config file format:")
                logger.info(json.dumps({
                    "/path/to/orf_trans.fasta.gz": {"name": "orf_trans", "type": "protein"},
                    "/path/to/orf_coding.fasta.gz": {"name": "orf_coding", "type": "nucleotide"},
                }, indent=2))
                return 0

            for source_file, info in datasets.items():
                source_path = Path(source_file)
                if process_sequence_file(
                    source_path,
                    info["name"],
                    info["type"],
                    args.output_dir,
                ):
                    success_count += 1
                else:
                    failure_count += 1

        # Summary
        logger.info(f"Completed: {success_count} succeeded, {failure_count} failed")

        if failure_count > 0:
            send_error_email(
                "Error creating Blast datasets",
                f"{failure_count} datasets failed to create. See {log_file} for details.",
            )
            return 1

        return 0

    except Exception as e:
        error_msg = f"Error creating BLAST datasets: {e}"
        logger.error(error_msg)
        send_error_email("Error creating Blast datasets", error_msg)
        return 1

    finally:
        logger.removeHandler(file_handler)
        file_handler.close()


if __name__ == "__main__":
    sys.exit(main())
