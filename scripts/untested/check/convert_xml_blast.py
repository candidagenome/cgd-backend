#!/usr/bin/env python3
"""
Convert BLAST XML output to text format.

This script reads BLAST results in XML format and converts them to
standard text format for easier reading and parsing.

Original Perl: convertXMLtoTextBlastOutput.pl
Converted to Python: 2024
"""

import argparse
import sys
from pathlib import Path

from Bio import SearchIO


def convert_blast_xml_to_text(input_file: Path, output_file: Path = None) -> None:
    """
    Convert BLAST XML to text format.

    Args:
        input_file: Path to BLAST XML file
        output_file: Path to output text file (stdout if None)
    """
    # Open output file or use stdout
    out_handle = open(output_file, 'w') if output_file else sys.stdout

    try:
        # Parse BLAST XML
        for result in SearchIO.parse(str(input_file), 'blast-xml'):
            # Write query info
            out_handle.write(f"Query= {result.id}\n")
            out_handle.write(f"       {result.description}\n")
            out_handle.write(f"       Length={result.seq_len}\n\n")

            if not result.hits:
                out_handle.write("***** No hits found *****\n\n")
                continue

            # Write hit summary
            out_handle.write("                                                                 Score     E\n")
            out_handle.write("Sequences producing significant alignments:                      (Bits)  Value\n\n")

            for hit in result.hits:
                desc = hit.description[:60] if hit.description else hit.id[:60]
                # Get best HSP score
                if hit.hsps:
                    best_hsp = hit.hsps[0]
                    score = best_hsp.bitscore
                    evalue = best_hsp.evalue
                    out_handle.write(f"{desc:<65} {score:>5.1f}  {evalue:.2e}\n")

            out_handle.write("\n")

            # Write alignments
            for hit in result.hits:
                out_handle.write(f">{hit.id} {hit.description}\n")
                out_handle.write(f"          Length = {hit.seq_len}\n\n")

                for hsp in hit.hsps:
                    out_handle.write(f" Score = {hsp.bitscore:.1f} bits ({hsp.bitscore_raw}), ")
                    out_handle.write(f"Expect = {hsp.evalue:.2e}\n")
                    out_handle.write(f" Identities = {hsp.ident_num}/{hsp.aln_span} ({hsp.ident_pct:.0f}%)")
                    if hasattr(hsp, 'pos_num') and hsp.pos_num:
                        out_handle.write(f", Positives = {hsp.pos_num}/{hsp.aln_span} ({hsp.pos_pct:.0f}%)")
                    if hasattr(hsp, 'gap_num') and hsp.gap_num:
                        out_handle.write(f", Gaps = {hsp.gap_num}/{hsp.aln_span} ({hsp.gap_pct:.0f}%)")
                    out_handle.write("\n")

                    # Get strand info if available
                    if hasattr(hsp, 'query_strand') and hsp.query_strand:
                        q_strand = '+' if hsp.query_strand > 0 else '-'
                        h_strand = '+' if hsp.hit_strand > 0 else '-'
                        out_handle.write(f" Strand = {q_strand}/{h_strand}\n")

                    out_handle.write("\n")

                    # Format alignment
                    query_seq = str(hsp.query.seq) if hasattr(hsp.query, 'seq') else str(hsp.query)
                    hit_seq = str(hsp.hit.seq) if hasattr(hsp.hit, 'seq') else str(hsp.hit)

                    # Simple alignment output
                    line_len = 60
                    q_start = hsp.query_start
                    h_start = hsp.hit_start

                    for i in range(0, len(query_seq), line_len):
                        q_chunk = query_seq[i:i+line_len]
                        h_chunk = hit_seq[i:i+line_len]

                        q_end = q_start + len(q_chunk.replace('-', '')) - 1
                        h_end = h_start + len(h_chunk.replace('-', '')) - 1

                        out_handle.write(f"Query  {q_start:>7}  {q_chunk}  {q_end}\n")
                        out_handle.write(f"               {''.join('|' if q == h else ' ' for q, h in zip(q_chunk, h_chunk))}\n")
                        out_handle.write(f"Sbjct  {h_start:>7}  {h_chunk}  {h_end}\n\n")

                        q_start = q_end + 1
                        h_start = h_end + 1

            out_handle.write("\n")

    finally:
        if output_file:
            out_handle.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Convert BLAST XML output to text format"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input BLAST XML file",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output text file (default: stdout)",
    )

    args = parser.parse_args()

    # Validate input file
    if not args.input_file.exists():
        print(f"Error: File not found: {args.input_file}", file=sys.stderr)
        sys.exit(1)

    try:
        convert_blast_xml_to_text(args.input_file, args.output)
    except Exception as e:
        print(f"Error converting BLAST XML: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
