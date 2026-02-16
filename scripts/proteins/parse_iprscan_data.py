#!/usr/bin/env python3
"""
Parse InterProScan output for display and loading.

This script processes InterProScan TSV output and formats it for:
1. Display by protein domain pages
2. GFF format for genome browsers

Original Perl: parseIprScanData.pl
Converted to Python: 2024
"""

import argparse
import logging
import re
import sys
from pathlib import Path
from urllib.parse import quote

from Bio import SeqIO
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

load_dotenv()

logger = logging.getLogger(__name__)

# Characters allowed in GFF3 format (for URL encoding)
ALLOWED_CHARS = 'a-zA-Z0-9.:=%^*$@!+_?-'


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def load_protein_sequences(seq_file: Path) -> dict:
    """
    Load protein sequences from FASTA file.

    Args:
        seq_file: FASTA file path

    Returns:
        Dict mapping sequence ID to (sequence, length)
    """
    sequences = {}

    for record in SeqIO.parse(str(seq_file), 'fasta'):
        seq = str(record.seq).rstrip('*')
        sequences[record.id] = {
            'seq': seq,
            'length': len(seq),
            'id': record.id,
        }

    return sequences


def parse_iprscan_tsv(
    input_file: Path,
    sequences: dict,
) -> list[dict]:
    """
    Parse InterProScan TSV output.

    Args:
        input_file: InterProScan TSV output file
        sequences: Dict of sequence info

    Returns:
        List of domain annotation dicts
    """
    results = []

    with open(input_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split('\t')
            if len(parts) < 11:
                continue

            (orf, checksum, orf_len, method, member_id, member_desc,
             match_start, match_end, evalue, match_status, run_date) = parts[:11]

            # Optional fields
            interpro_id = parts[11] if len(parts) > 11 else '-'
            interpro_desc = parts[12] if len(parts) > 12 else '-'
            go_ids = parts[13] if len(parts) > 13 else ''
            pathways = parts[14] if len(parts) > 14 else ''

            # Format e-value
            if evalue == '-':
                evalue = '0'
            elif re.match(r'^[\d.\-eE]+$', evalue):
                try:
                    evalue = f"{float(evalue):.3e}"
                except ValueError:
                    evalue = '0'
            else:
                evalue = '0'

            # Clean up values
            member_desc = '' if member_desc == '-' else member_desc
            interpro_id = '' if interpro_id == '-' else interpro_id
            interpro_desc = '' if interpro_desc == '-' else interpro_desc

            # Normalize method names
            if method.lower() in ('funfam', 'gene3d'):
                method = 'CATH'
                member_id = re.sub(r':FF\S+$', '/', member_id)

            if method.startswith('SignalP_'):
                method = 'SignalP'
                member_id = 'SignalP'

            results.append({
                'orf': orf,
                'orf_len': orf_len,
                'method': method,
                'member_id': member_id,
                'member_desc': member_desc,
                'match_start': match_start,
                'match_end': match_end,
                'evalue': evalue,
                'match_status': match_status,
                'interpro_id': interpro_id,
                'interpro_desc': interpro_desc,
            })

    return results


def write_domain_data(
    results: list[dict],
    output_file: Path,
) -> None:
    """
    Write domain data for display.

    Args:
        results: List of domain annotation dicts
        output_file: Output file path
    """
    with open(output_file, 'w') as f:
        for r in results:
            f.write('\t'.join([
                r['orf'],
                r['orf_len'],
                r['method'],
                r['member_id'],
                r['member_desc'],
                r['match_start'],
                r['match_end'],
                r['evalue'],
                r['match_status'],
                r['interpro_id'],
                r['interpro_desc'],
            ]) + '\n')


def write_gff(
    results: list[dict],
    sequences: dict,
    gff_file: Path,
    source: str = 'CGD',
) -> None:
    """
    Write GFF3 format output with embedded FASTA.

    Args:
        results: List of domain annotation dicts
        sequences: Dict of sequence info
        gff_file: Output GFF file
        source: Source field for GFF
    """
    written_orfs = set()

    with open(gff_file, 'w') as f:
        f.write("##gff-version\t3\n")

        for r in results:
            orf = r['orf']

            # Write reference line if not seen
            if orf not in written_orfs and orf in sequences:
                seq_info = sequences[orf]
                f.write(f"reference={orf}\n")
                f.write('\t'.join([
                    orf, source, 'protein', '1', str(seq_info['length']),
                    '.', '.', '.', f"ID={orf};Name={orf}"
                ]) + '\n')
                written_orfs.add(orf)

            # Build description
            desc_parts = []
            if r['member_desc']:
                desc_parts.append(r['member_desc'])
            if r['interpro_id']:
                desc_parts.append(r['interpro_id'])
            if r['interpro_desc'] and r['interpro_desc'] != r['member_desc']:
                desc_parts.append(r['interpro_desc'])
            desc = '; '.join(desc_parts)

            # Write feature line
            f.write('\t'.join([
                orf, source, r['method'],
                r['match_start'], r['match_end'],
                r['evalue'], '.', '.',
                f"ID={r['member_id']}; Note={desc}"
            ]) + '\n')

        # Write any remaining sequences without annotations
        for orf, seq_info in sequences.items():
            if orf not in written_orfs:
                f.write(f"reference={orf}\n")
                f.write('\t'.join([
                    orf, source, 'protein', '1', str(seq_info['length']),
                    '.', '.', '.', f"ID={orf};Name={orf}"
                ]) + '\n')
                written_orfs.add(orf)

        # Append FASTA sequences
        f.write("##FASTA\n")
        for orf in sorted(written_orfs):
            if orf in sequences:
                f.write(f">{orf}\n{sequences[orf]['seq']}\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Parse InterProScan output for display and loading"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="InterProScan TSV output file",
    )
    parser.add_argument(
        "--sequences",
        type=Path,
        required=True,
        help="Protein sequences FASTA file",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        required=True,
        help="Output domain data file",
    )
    parser.add_argument(
        "--gff",
        type=Path,
        help="Output GFF file (optional)",
    )
    parser.add_argument(
        "--source",
        default="CGD",
        help="Source field for GFF (default: CGD)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    # Validate inputs
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    if not args.sequences.exists():
        logger.error(f"Sequence file not found: {args.sequences}")
        sys.exit(1)

    # Load sequences
    logger.info(f"Loading sequences from {args.sequences}")
    sequences = load_protein_sequences(args.sequences)
    logger.info(f"Loaded {len(sequences)} sequences")

    # Parse InterProScan output
    logger.info(f"Parsing {args.input_file}")
    results = parse_iprscan_tsv(args.input_file, sequences)
    logger.info(f"Parsed {len(results)} domain annotations")

    # Write domain data
    logger.info(f"Writing domain data to {args.output}")
    write_domain_data(results, args.output)

    # Write GFF if requested
    if args.gff:
        logger.info(f"Writing GFF to {args.gff}")
        write_gff(results, sequences, args.gff, args.source)

    logger.info("Complete")


if __name__ == "__main__":
    main()
