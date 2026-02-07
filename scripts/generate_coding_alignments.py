#!/usr/bin/env python3
"""
Generate coding sequence alignments from unaligned FASTA files.

This script processes all *_coding.fasta files in the homology/alignments directory
and generates corresponding *_coding_align.fasta files using MUSCLE.

Usage:
    python scripts/generate_coding_alignments.py [--data-dir PATH] [--dry-run]

Requirements:
    - MUSCLE alignment tool must be installed and in PATH
      Install: brew install muscle (macOS) or apt-get install muscle (Ubuntu)
"""
import argparse
import logging
import subprocess
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def find_muscle() -> str:
    """Find the MUSCLE executable."""
    # Try common names
    for cmd in ['muscle', 'muscle5', 'muscle3']:
        try:
            result = subprocess.run(
                [cmd, '-version'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0 or 'MUSCLE' in result.stdout or 'MUSCLE' in result.stderr:
                logger.info(f"Found MUSCLE: {cmd}")
                return cmd
        except (subprocess.SubprocessError, FileNotFoundError):
            continue

    raise RuntimeError(
        "MUSCLE not found. Please install it:\n"
        "  macOS: brew install muscle\n"
        "  Ubuntu: apt-get install muscle\n"
        "  Or download from: https://www.drive5.com/muscle/"
    )


def run_muscle(input_file: Path, output_file: Path, muscle_cmd: str) -> bool:
    """Run MUSCLE alignment on input file."""
    try:
        # MUSCLE v5 syntax: muscle -align input.fa -output output.fa
        # MUSCLE v3 syntax: muscle -in input.fa -out output.fa
        # Try v5 first, fall back to v3
        cmd_v5 = [muscle_cmd, '-align', str(input_file), '-output', str(output_file)]
        cmd_v3 = [muscle_cmd, '-in', str(input_file), '-out', str(output_file)]

        result = subprocess.run(
            cmd_v5,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )

        if result.returncode != 0:
            # Try v3 syntax
            result = subprocess.run(
                cmd_v3,
                capture_output=True,
                text=True,
                timeout=300
            )

        if result.returncode != 0:
            logger.error(f"MUSCLE failed for {input_file}: {result.stderr}")
            return False

        return True

    except subprocess.TimeoutExpired:
        logger.error(f"MUSCLE timed out for {input_file}")
        return False
    except Exception as e:
        logger.error(f"Error running MUSCLE for {input_file}: {e}")
        return False


def process_alignments(data_dir: Path, dry_run: bool = False) -> tuple[int, int, int]:
    """
    Process all coding.fasta files and generate alignments.

    Returns:
        Tuple of (processed, skipped, failed) counts
    """
    alignments_dir = data_dir / "homology" / "alignments"

    if not alignments_dir.exists():
        logger.error(f"Alignments directory not found: {alignments_dir}")
        return 0, 0, 0

    # Find MUSCLE if not dry run
    muscle_cmd = None
    if not dry_run:
        muscle_cmd = find_muscle()

    processed = 0
    skipped = 0
    failed = 0

    # Find all *_coding.fasta files
    coding_files = list(alignments_dir.glob("**/*_coding.fasta"))
    logger.info(f"Found {len(coding_files)} coding.fasta files")

    for coding_file in coding_files:
        # Generate output filename
        align_file = coding_file.with_name(
            coding_file.name.replace("_coding.fasta", "_coding_align.fasta")
        )

        # Skip if alignment already exists
        if align_file.exists():
            logger.debug(f"Skipping {coding_file.name} - alignment exists")
            skipped += 1
            continue

        logger.info(f"Processing: {coding_file.name}")

        if dry_run:
            logger.info(f"  Would create: {align_file.name}")
            processed += 1
            continue

        # Run MUSCLE
        if run_muscle(coding_file, align_file, muscle_cmd):
            logger.info(f"  Created: {align_file.name}")
            processed += 1
        else:
            failed += 1

    return processed, skipped, failed


def main():
    parser = argparse.ArgumentParser(
        description="Generate coding sequence alignments using MUSCLE"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("/data/cgd-data"),
        help="Path to CGD data directory (default: /data/cgd-data)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )

    args = parser.parse_args()

    logger.info(f"Data directory: {args.data_dir}")
    if args.dry_run:
        logger.info("DRY RUN - no changes will be made")

    processed, skipped, failed = process_alignments(args.data_dir, args.dry_run)

    logger.info(f"\nSummary:")
    logger.info(f"  Processed: {processed}")
    logger.info(f"  Skipped (already exist): {skipped}")
    logger.info(f"  Failed: {failed}")

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
