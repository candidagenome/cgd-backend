#!/usr/bin/env python3
"""
Prepare JBrowse protein tracks.

This script prepares protein domain data for display in JBrowse,
creating the necessary reference sequences and feature tracks.

Original Perl: prepareProteinTracks.pl
Converted to Python: 2024
"""

import argparse
import json
import logging
import shutil
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Domain methods to create tracks for
TRACK_METHODS = [
    'Pfam', 'PANTHER', 'SUPERFAMILY', 'CATH', 'SMART',
    'ProSiteProfiles', 'CDD', 'NCBIfam', 'PIRSF', 'Hamap',
    'SFLD', 'PRINTS', 'ProSitePatterns', 'SignalP', 'TMHMM',
    'Coils', 'MobiDBLite',
]


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def run_command(cmd: list[str], description: str) -> bool:
    """Run a command and log results."""
    logger.debug(f"Running: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        logger.error(f"{description} failed: {result.stderr}")
        return False

    return True


def prepare_reference(
    fasta_file: Path,
    output_dir: Path,
    jbrowse_bin: Path,
) -> bool:
    """
    Prepare reference sequences for JBrowse.

    Args:
        fasta_file: Protein FASTA file
        output_dir: JBrowse data directory
        jbrowse_bin: Path to JBrowse bin directory

    Returns:
        True if successful
    """
    prepare_script = jbrowse_bin / 'prepare-refseqs.pl'

    if not prepare_script.exists():
        # Try JBrowse 2 command
        cmd = [
            'jbrowse', 'add-assembly',
            str(fasta_file),
            '--out', str(output_dir),
            '--type', 'protein',
        ]
    else:
        cmd = [
            'perl', str(prepare_script),
            '--fasta', str(fasta_file),
            '--seqtype', 'protein',
            '--out', str(output_dir),
        ]

    return run_command(cmd, "Prepare reference sequences")


def add_track(
    gff_file: Path,
    output_dir: Path,
    track_label: str,
    track_type: str,
    jbrowse_bin: Path,
) -> bool:
    """
    Add a feature track to JBrowse.

    Args:
        gff_file: GFF file with features
        output_dir: JBrowse data directory
        track_label: Track label/name
        track_type: Feature type to filter
        jbrowse_bin: Path to JBrowse bin directory

    Returns:
        True if successful
    """
    flatfile_script = jbrowse_bin / 'flatfile-to-json.pl'

    if not flatfile_script.exists():
        # Try JBrowse 2 command
        cmd = [
            'jbrowse', 'add-track',
            str(gff_file),
            '--out', str(output_dir),
            '--trackId', track_label,
            '--name', track_label,
        ]
    else:
        cmd = [
            'perl', str(flatfile_script),
            '--gff', str(gff_file),
            '--trackLabel', track_label,
            '--out', str(output_dir),
            '--type', track_type,
        ]

    return run_command(cmd, f"Add track {track_label}")


def generate_names(
    output_dir: Path,
    jbrowse_bin: Path,
) -> bool:
    """
    Generate searchable names index.

    Args:
        output_dir: JBrowse data directory
        jbrowse_bin: Path to JBrowse bin directory

    Returns:
        True if successful
    """
    names_script = jbrowse_bin / 'generate-names.pl'

    if not names_script.exists():
        # Try JBrowse 2 command
        cmd = [
            'jbrowse', 'text-index',
            '--out', str(output_dir),
        ]
    else:
        cmd = [
            'perl', str(names_script),
            '--out', str(output_dir),
        ]

    return run_command(cmd, "Generate names index")


def clean_old_data(data_dir: Path) -> None:
    """Remove old JBrowse data directories."""
    for subdir in ['seq', 'names', 'tracks']:
        path = data_dir / subdir
        if path.exists():
            logger.debug(f"Removing {path}")
            shutil.rmtree(path)

    tracklist = data_dir / 'trackList.json'
    if tracklist.exists():
        tracklist.unlink()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Prepare JBrowse protein tracks"
    )
    parser.add_argument(
        "--organism",
        required=True,
        help="Organism abbreviation",
    )
    parser.add_argument(
        "--fasta",
        type=Path,
        required=True,
        help="Protein FASTA file",
    )
    parser.add_argument(
        "--gff",
        type=Path,
        required=True,
        help="Protein domain GFF file",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="JBrowse data directory",
    )
    parser.add_argument(
        "--jbrowse-bin",
        type=Path,
        default=Path("/tools/jbrowse/bin"),
        help="JBrowse bin directory",
    )
    parser.add_argument(
        "--methods",
        nargs="+",
        default=TRACK_METHODS,
        help=f"Domain methods to create tracks for",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove existing data before generating",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    # Validate inputs
    if not args.fasta.exists():
        logger.error(f"FASTA file not found: {args.fasta}")
        sys.exit(1)

    if not args.gff.exists():
        logger.error(f"GFF file not found: {args.gff}")
        sys.exit(1)

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Clean old data if requested
    if args.clean:
        logger.info("Cleaning old data")
        clean_old_data(args.output_dir)

    # Prepare reference sequences
    logger.info("Preparing reference sequences")
    if not prepare_reference(args.fasta, args.output_dir, args.jbrowse_bin):
        logger.error("Failed to prepare reference sequences")
        sys.exit(1)

    # Add protein track (all features)
    logger.info("Adding protein track")
    if not add_track(args.gff, args.output_dir, 'Protein', 'protein', args.jbrowse_bin):
        logger.warning("Failed to add protein track")

    # Add method-specific tracks
    for method in args.methods:
        logger.info(f"Adding {method} track")
        if not add_track(args.gff, args.output_dir, method, method, args.jbrowse_bin):
            logger.warning(f"Failed to add {method} track")

    # Generate names index
    logger.info("Generating names index")
    if not generate_names(args.output_dir, args.jbrowse_bin):
        logger.warning("Failed to generate names index")

    logger.info(f"JBrowse data prepared in {args.output_dir}")


if __name__ == "__main__":
    main()
