#!/usr/bin/env python3
"""
Create BLAST datasets from sequence files.

This script formats sequence files (FASTA) for use with BLAST by:
1. Copying sequence files to the BLAST datasets directory
2. Decompressing gzipped files if necessary
3. Running formatdb/makeblastdb to create BLAST databases
4. Cleaning up intermediate files

Environment Variables:
    BLAST_DATASET_DIR: Directory for BLAST datasets
    BLAST_FORMAT_CMD: Path to blast formatter (default: makeblastdb)
    SEQUENCE_FILES_CONFIG: JSON file mapping source files to dataset names
    LOG_DIR: Directory for log files (default: /tmp)
    CURATOR_EMAIL: Email for error notifications
"""

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

# Configuration
BLAST_DATASET_DIR = Path(os.getenv("BLAST_DATASET_DIR", "/data/blast_datasets"))
BLAST_FORMAT_CMD = os.getenv("BLAST_FORMAT_CMD", "makeblastdb")
SEQUENCE_FILES_CONFIG = os.getenv("SEQUENCE_FILES_CONFIG", "")
LOG_DIR = Path(os.getenv("LOG_DIR", "/tmp"))
LOG_FILE = LOG_DIR / "blast_dataset_creation.log"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# Default sequence file mappings (can be overridden by config file)
# Format: {source_file: (dataset_name, sequence_type)}
# sequence_type: "prot" for protein, "nucl" for nucleotide
DEFAULT_FILES_CONFIG = {
    # Add default mappings here or use config file
}


def send_error_email(message: str) -> None:
    """Send error notification email."""
    curator_email = os.getenv("CURATOR_EMAIL")
    if curator_email:
        logger.info(f"Would send error email to {curator_email}: {message}")


def load_sequence_files_config() -> dict:
    """Load sequence files configuration from JSON file or use defaults."""
    if SEQUENCE_FILES_CONFIG and Path(SEQUENCE_FILES_CONFIG).exists():
        with open(SEQUENCE_FILES_CONFIG) as f:
            return json.load(f)
    return DEFAULT_FILES_CONFIG


def decompress_file(source: Path, dest: Path) -> Path:
    """
    Decompress a gzipped file.

    Args:
        source: Path to gzipped file
        dest: Destination path for decompressed file

    Returns:
        Path to decompressed file
    """
    logger.info(f"Decompressing {source}")

    with gzip.open(source, "rb") as f_in:
        with open(dest, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    return dest


def format_blast_db(
    input_file: Path,
    db_name: str,
    db_type: str,
    output_dir: Path,
) -> bool:
    """
    Format a BLAST database from a FASTA file.

    Args:
        input_file: Input FASTA file
        db_name: Name for the database
        db_type: Database type ("prot" or "nucl")
        output_dir: Output directory

    Returns:
        True on success, False on failure
    """
    logger.info(f"Creating BLAST dataset {db_name} from {input_file}")

    # Create lock file to indicate processing
    lock_file = output_dir / f"{db_name}.lock"
    lock_file.touch()

    try:
        # Use makeblastdb (NCBI BLAST+) or wu-formatdb (WU-BLAST)
        if "makeblastdb" in BLAST_FORMAT_CMD:
            cmd = [
                BLAST_FORMAT_CMD,
                "-in", str(input_file),
                "-dbtype", db_type,
                "-out", str(output_dir / db_name),
                "-title", db_name,
            ]
        else:
            # WU-BLAST format (T = protein, F = nucleotide)
            type_flag = "T" if db_type == "prot" else "F"
            cmd = [
                BLAST_FORMAT_CMD,
                "-i", str(input_file),
                "-t", db_name,
                "-p", type_flag,
            ]

        logger.info(f"Running: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=1800,  # 30 minute timeout
            cwd=output_dir,
        )

        if result.returncode != 0:
            logger.error(f"formatdb failed: {result.stderr}")
            return False

        logger.info(f"Successfully created dataset {db_name}")
        return True

    except subprocess.TimeoutExpired:
        logger.error(f"formatdb timed out for {db_name}")
        return False

    except Exception as e:
        logger.error(f"Error creating BLAST dataset: {e}")
        return False

    finally:
        # Remove lock file
        if lock_file.exists():
            lock_file.unlink()


def create_blast_datasets() -> bool:
    """
    Create BLAST datasets from all configured sequence files.

    Returns:
        True on success, False on any failure
    """
    logger.info("Starting BLAST dataset creation")
    logger.info(f"Output directory: {BLAST_DATASET_DIR}")

    # Ensure output directory exists
    BLAST_DATASET_DIR.mkdir(parents=True, exist_ok=True)

    files_config = load_sequence_files_config()

    if not files_config:
        logger.warning("No sequence files configured")
        return True

    success = True

    for source_file, config in files_config.items():
        source_path = Path(source_file)
        dataset_name = config["name"]
        db_type = config["type"]  # "prot" or "nucl"

        if not source_path.exists():
            logger.error(f"Source file not found: {source_path}")
            success = False
            continue

        try:
            logger.info(f"Processing {source_path}")

            # Copy to dataset directory
            dest_file = BLAST_DATASET_DIR / source_path.name
            shutil.copy(source_path, dest_file)
            logger.info(f"Copied {source_path} to {dest_file}")

            # Decompress if gzipped
            if dest_file.suffix == ".gz":
                decompressed = dest_file.with_suffix("")
                decompress_file(dest_file, decompressed)
                dest_file.unlink()
                dest_file = decompressed

            # Rename to dataset name
            final_file = BLAST_DATASET_DIR / dataset_name
            dest_file.rename(final_file)

            # Format BLAST database
            if not format_blast_db(final_file, dataset_name, db_type, BLAST_DATASET_DIR):
                success = False

            # Clean up input file
            if final_file.exists():
                final_file.unlink()
                logger.info(f"Cleaned up {final_file}")

        except Exception as e:
            logger.exception(f"Error processing {source_file}: {e}")
            success = False

    return success


def main() -> int:
    """Main entry point."""
    logger.info(f"Program {__file__}: Starting {datetime.now()}")

    try:
        success = create_blast_datasets()

        if not success:
            send_error_email("Errors occurred during BLAST dataset creation. See log for details.")

        logger.info(f"Complete: {datetime.now()}")
        return 0 if success else 1

    except Exception as e:
        error_msg = f"Fatal error: {e}"
        logger.exception(error_msg)
        send_error_email(error_msg)
        return 1


if __name__ == "__main__":
    sys.exit(main())
