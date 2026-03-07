#!/usr/bin/env python3
"""
Submit sequences to InterProScan.

This script submits protein sequences to InterProScan for domain
and motif annotation.

Original Perl: submit-iprscan.pl
Converted to Python: 2024
"""

import argparse
import gzip
import logging
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from Bio import SeqIO
from Bio.SeqRecord import SeqRecord

logger = logging.getLogger(__name__)

# InterProScan applications
APPS_DOMAIN = [
    'Pfam', 'PANTHER', 'SUPERFAMILY', 'Gene3D', 'CDD',
    'SMART', 'ProSiteProfiles', 'ProSitePatterns', 'PRINTS',
    'PIRSF', 'Hamap', 'NCBIfam', 'SFLD',
]
APPS_SIGNAL = ['SignalP', 'TMHMM', 'Phobius', 'Coils', 'MobiDBLite']

CHUNK_SIZE = 10


def setup_logging(verbose: bool = False, log_file: Path = None) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler()]

    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def load_sequences(fasta_file: Path) -> list[SeqRecord]:
    """
    Load sequences from FASTA file.

    Args:
        fasta_file: FASTA file (can be gzipped)

    Returns:
        List of SeqRecord objects
    """
    sequences = []

    open_func = gzip.open if str(fasta_file).endswith('.gz') else open
    mode = 'rt' if str(fasta_file).endswith('.gz') else 'r'

    with open_func(fasta_file, mode) as f:
        for record in SeqIO.parse(f, 'fasta'):
            sequences.append(record)

    return sequences


def run_interproscan(
    input_file: Path,
    output_file: Path,
    iprscan_path: str = 'interproscan.sh',
    applications: list[str] = None,
    formats: list[str] = None,
    goterms: bool = True,
    iprlookup: bool = True,
    pathways: bool = True,
) -> bool:
    """
    Run InterProScan on input file.

    Args:
        input_file: Input FASTA file
        output_file: Output file path (without extension)
        iprscan_path: Path to interproscan.sh
        applications: List of applications to run
        formats: Output formats
        goterms: Include GO terms
        iprlookup: Include InterPro lookup
        pathways: Include pathway annotations

    Returns:
        True if successful
    """
    cmd = [
        iprscan_path,
        '-i', str(input_file),
        '-o', str(output_file),
        '-f', ','.join(formats) if formats else 'tsv',
    ]

    if applications:
        cmd.extend(['-appl', ','.join(applications)])

    if goterms:
        cmd.append('-goterms')

    if iprlookup:
        cmd.append('-iprlookup')

    if pathways:
        cmd.append('-pa')

    logger.debug(f"Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout per chunk
        )

        if result.returncode != 0:
            logger.error(f"InterProScan error: {result.stderr}")
            return False

        return True

    except subprocess.TimeoutExpired:
        logger.error("InterProScan timed out")
        return False
    except Exception as e:
        logger.error(f"Error running InterProScan: {e}")
        return False


def process_in_chunks(
    sequences: list[SeqRecord],
    output_file: Path,
    iprscan_path: str,
    chunk_size: int = CHUNK_SIZE,
    applications: list[str] = None,
) -> dict:
    """
    Process sequences in chunks.

    Args:
        sequences: List of SeqRecord objects
        output_file: Final output file
        iprscan_path: Path to InterProScan
        chunk_size: Number of sequences per chunk
        applications: Applications to run

    Returns:
        Statistics dict
    """
    stats = {
        'total': len(sequences),
        'processed': 0,
        'failed': 0,
    }

    all_results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        for i in range(0, len(sequences), chunk_size):
            chunk = sequences[i:i + chunk_size]
            chunk_num = i // chunk_size + 1
            total_chunks = (len(sequences) + chunk_size - 1) // chunk_size

            logger.info(f"Processing chunk {chunk_num}/{total_chunks} ({len(chunk)} sequences)")

            # Write chunk to temp file
            chunk_input = tmpdir / f"chunk_{chunk_num}.fasta"
            chunk_output = tmpdir / f"chunk_{chunk_num}.tsv"

            with open(chunk_input, 'w') as f:
                SeqIO.write(chunk, f, 'fasta')

            # Run InterProScan
            success = run_interproscan(
                chunk_input,
                chunk_output,
                iprscan_path,
                applications,
                formats=['tsv'],
            )

            if success and chunk_output.exists():
                with open(chunk_output) as f:
                    all_results.extend(f.readlines())
                stats['processed'] += len(chunk)
            else:
                logger.error(f"Chunk {chunk_num} failed")
                stats['failed'] += len(chunk)

    # Write combined results
    with open(output_file, 'w') as f:
        f.writelines(all_results)

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Submit sequences to InterProScan"
    )
    parser.add_argument(
        "proteins",
        type=Path,
        help="Input protein FASTA file",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        required=True,
        help="Output TSV file",
    )
    parser.add_argument(
        "--iprscan-path",
        default="interproscan.sh",
        help="Path to interproscan.sh",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE,
        help=f"Sequences per chunk (default: {CHUNK_SIZE})",
    )
    parser.add_argument(
        "--domain-only",
        action="store_true",
        help="Only run domain applications (no SignalP/TMHMM)",
    )
    parser.add_argument(
        "--signal-only",
        action="store_true",
        help="Only run signal applications (SignalP/TMHMM)",
    )
    parser.add_argument(
        "--applications",
        nargs="+",
        help="Specific applications to run",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Log file path",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    setup_logging(args.verbose, args.log_file)

    logger.info(f"Started at {datetime.now()}")

    # Validate input
    if not args.proteins.exists():
        logger.error(f"Input file not found: {args.proteins}")
        sys.exit(1)

    # Determine applications
    if args.applications:
        applications = args.applications
    elif args.domain_only:
        applications = APPS_DOMAIN
    elif args.signal_only:
        applications = APPS_SIGNAL
    else:
        applications = APPS_DOMAIN + APPS_SIGNAL

    logger.info(f"Applications: {', '.join(applications)}")

    # Load sequences
    logger.info(f"Loading sequences from {args.proteins}")
    sequences = load_sequences(args.proteins)
    logger.info(f"Loaded {len(sequences)} sequences")

    if not sequences:
        logger.warning("No sequences to process")
        sys.exit(0)

    # Calculate statistics
    lengths = [len(s.seq) for s in sequences]
    mean_len = sum(lengths) / len(lengths) if lengths else 0
    median_len = sorted(lengths)[len(lengths) // 2] if lengths else 0

    logger.info(f"Mean length: {mean_len:.1f}, Median length: {median_len}")

    # Process sequences
    args.output.parent.mkdir(parents=True, exist_ok=True)

    stats = process_in_chunks(
        sequences,
        args.output,
        args.iprscan_path,
        args.chunk_size,
        applications,
    )

    logger.info("=" * 50)
    logger.info("Summary:")
    logger.info(f"  Total sequences: {stats['total']}")
    logger.info(f"  Processed: {stats['processed']}")
    logger.info(f"  Failed: {stats['failed']}")
    logger.info(f"  Output: {args.output}")
    logger.info("=" * 50)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
