#!/usr/bin/env python3
"""
Make pairwise sequence alignments.

This script performs pairwise sequence alignments between old and new
versions of sequences, useful for comparing assembly updates.

Original Perl: makeAlignments.pl
Converted to Python: 2024
"""

import argparse
import sys
from pathlib import Path

from Bio import SeqIO, pairwise2
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
from Bio.pairwise2 import format_alignment

# Try to import Bio.Align for newer BioPython
try:
    from Bio import Align
    USE_NEW_ALIGN = True
except ImportError:
    USE_NEW_ALIGN = False


# Alternative Yeast Nuclear genetic code
GENETIC_CODE = 12


def translate_sequence(dna_seq: str, genetic_code: int = GENETIC_CODE) -> str:
    """
    Translate DNA to protein.

    Args:
        dna_seq: DNA sequence
        genetic_code: NCBI genetic code table

    Returns:
        Protein sequence
    """
    seq = Seq(dna_seq)
    return str(seq.translate(table=genetic_code))


def pairwise_align(
    seq1: str,
    seq2: str,
    seq1_id: str = 'seq1',
    seq2_id: str = 'seq2',
    mode: str = 'global',
    match: int = 2,
    mismatch: int = -1,
    gap_open: int = -10,
    gap_extend: int = -1,
) -> dict:
    """
    Perform pairwise alignment.

    Args:
        seq1: First sequence
        seq2: Second sequence
        seq1_id: First sequence ID
        seq2_id: Second sequence ID
        mode: Alignment mode (global, local)
        match: Match score
        mismatch: Mismatch score
        gap_open: Gap opening penalty
        gap_extend: Gap extension penalty

    Returns:
        Alignment result dict
    """
    # Remove terminal stop codons for better alignment
    seq1 = seq1.rstrip('*')
    seq2 = seq2.rstrip('*')

    if USE_NEW_ALIGN:
        # Use newer Bio.Align
        aligner = Align.PairwiseAligner()
        aligner.mode = mode
        aligner.match_score = match
        aligner.mismatch_score = mismatch
        aligner.open_gap_score = gap_open
        aligner.extend_gap_score = gap_extend

        alignments = aligner.align(seq1, seq2)
        if alignments:
            aln = alignments[0]
            aligned_seq1 = aln[0]
            aligned_seq2 = aln[1]
            score = aln.score

            # Calculate identity
            matches = sum(1 for a, b in zip(aligned_seq1, aligned_seq2)
                          if a == b and a != '-')
            aln_length = len(aligned_seq1)
            identity_pct = (matches / aln_length * 100) if aln_length else 0

            return {
                'seq1_id': seq1_id,
                'seq2_id': seq2_id,
                'seq1_length': len(seq1),
                'seq2_length': len(seq2),
                'aligned_seq1': str(aligned_seq1),
                'aligned_seq2': str(aligned_seq2),
                'alignment_length': aln_length,
                'matches': matches,
                'identity_pct': identity_pct,
                'score': score,
            }
    else:
        # Use older pairwise2
        if mode == 'global':
            alignments = pairwise2.align.globalms(
                seq1, seq2, match, mismatch, gap_open, gap_extend
            )
        else:
            alignments = pairwise2.align.localms(
                seq1, seq2, match, mismatch, gap_open, gap_extend
            )

        if alignments:
            aln = alignments[0]
            aligned_seq1 = aln[0]
            aligned_seq2 = aln[1]
            score = aln[2]

            # Calculate identity
            matches = sum(1 for a, b in zip(aligned_seq1, aligned_seq2)
                          if a == b and a != '-')
            aln_length = len(aligned_seq1)
            identity_pct = (matches / aln_length * 100) if aln_length else 0

            return {
                'seq1_id': seq1_id,
                'seq2_id': seq2_id,
                'seq1_length': len(seq1),
                'seq2_length': len(seq2),
                'aligned_seq1': aligned_seq1,
                'aligned_seq2': aligned_seq2,
                'alignment_length': aln_length,
                'matches': matches,
                'identity_pct': identity_pct,
                'score': score,
            }

    return None


def format_clustal(alignment: dict, line_width: int = 60) -> str:
    """
    Format alignment in CLUSTAL-like format.

    Args:
        alignment: Alignment dict
        line_width: Characters per line

    Returns:
        Formatted alignment string
    """
    lines = []
    seq1 = alignment['aligned_seq1']
    seq2 = alignment['aligned_seq2']
    id1 = alignment['seq1_id'][:20].ljust(20)
    id2 = alignment['seq2_id'][:20].ljust(20)

    for i in range(0, len(seq1), line_width):
        chunk1 = seq1[i:i + line_width]
        chunk2 = seq2[i:i + line_width]

        # Match line
        match_line = ''.join(
            '*' if a == b and a != '-' else ' '
            for a, b in zip(chunk1, chunk2)
        )

        lines.append(f"{id1} {chunk1}")
        lines.append(f"{id2} {chunk2}")
        lines.append(f"{''.ljust(20)} {match_line}")
        lines.append("")

    return '\n'.join(lines)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Make pairwise sequence alignments"
    )
    parser.add_argument(
        "sequences",
        type=Path,
        help="Input FASTA file with sequence pairs (old/new versions)",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "--mode",
        choices=['global', 'local'],
        default='global',
        help="Alignment mode (default: global)",
    )
    parser.add_argument(
        "--translate",
        action="store_true",
        help="Translate DNA to protein before aligning",
    )
    parser.add_argument(
        "--genetic-code",
        type=int,
        default=GENETIC_CODE,
        help=f"Genetic code for translation (default: {GENETIC_CODE})",
    )
    parser.add_argument(
        "--pair-suffix",
        nargs=2,
        default=['_old', '_new'],
        help="Suffixes to identify sequence pairs (default: _old _new)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    # Validate input
    if not args.sequences.exists():
        print(f"Error: File not found: {args.sequences}", file=sys.stderr)
        sys.exit(1)

    # Load sequences
    sequences = {}
    for record in SeqIO.parse(str(args.sequences), 'fasta'):
        sequences[record.id] = str(record.seq)

    if args.verbose:
        print(f"Loaded {len(sequences)} sequences", file=sys.stderr)

    # Find pairs
    pairs = []
    suffix1, suffix2 = args.pair_suffix
    used = set()

    for seq_id in sequences:
        if seq_id.endswith(suffix1):
            base_id = seq_id[:-len(suffix1)]
            partner_id = base_id + suffix2
            if partner_id in sequences:
                pairs.append((seq_id, partner_id, base_id))
                used.add(seq_id)
                used.add(partner_id)

    if args.verbose:
        print(f"Found {len(pairs)} sequence pairs", file=sys.stderr)

    # Output
    out_handle = open(args.output, 'w') if args.output else sys.stdout

    try:
        for seq1_id, seq2_id, base_id in pairs:
            seq1 = sequences[seq1_id]
            seq2 = sequences[seq2_id]

            # Translate if requested
            if args.translate:
                try:
                    seq1 = translate_sequence(seq1, args.genetic_code)
                    seq2 = translate_sequence(seq2, args.genetic_code)
                except Exception as e:
                    print(f"Translation error for {base_id}: {e}", file=sys.stderr)
                    continue

            # Align
            alignment = pairwise_align(
                seq1, seq2,
                seq1_id, seq2_id,
                args.mode,
            )

            if alignment:
                out_handle.write(f"\n############### {base_id} ################\n\n")
                out_handle.write(
                    f"OLD: 1-{alignment['seq1_length']}    "
                    f"NEW: 1-{alignment['seq2_length']}\n"
                )
                out_handle.write(f"Percent identity: {alignment['identity_pct']:.1f}%\n\n")
                out_handle.write(format_clustal(alignment))
                out_handle.write("\n")

    finally:
        if args.output:
            out_handle.close()


if __name__ == "__main__":
    main()
