#!/usr/bin/env python3
"""
Parse BLAST XML output for InParanoid.

This parser parses output files from blastp and outputs one-line descriptions
for each hit with a score above a cut-off score. The output contains:
- Query id, Hit id, Bit score
- Query length, Hit length
- Segment lengths and match lengths
- Segment positions on query and hit

Original Perl: blast_parser.pl (by Isabella Pekkari, 2007-11-19)
Converted to Python: 2024

Usage:
    python blast_parser.py SCORE_CUTOFF blast_results.xml
    python blast_parser.py -a SCORE_CUTOFF blast_results.xml  # alignment mode
"""

import argparse
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO


@dataclass
class HSP:
    """High-scoring segment pair."""
    bit_score: float
    query_from: int
    query_to: int
    hit_from: int
    hit_to: int
    qseq: str = ""
    hseq: str = ""


@dataclass
class Hit:
    """BLAST hit."""
    hit_id: str
    hit_length: int
    total_score: float = 0.0
    total_query_length: int = 0
    total_hit_length: int = 0
    hsps: list = field(default_factory=list)


@dataclass
class Query:
    """BLAST query."""
    query_id: str
    query_length: int
    hits: list = field(default_factory=list)


def check_overlap_linear(
    start1: int, end1: int, start2: int, end2: int
) -> bool:
    """
    Check if two segments overlap (linear mode).

    Allows maximum 5% overlap with regard to shorter segment.
    """
    length1 = end1 - start1 + 1
    length2 = end2 - start2 + 1
    shortest = min(length1, length2)

    # Check if overlap exceeds 5% threshold
    return (start2 - end1 - 1) / shortest < -0.05


def check_overlap_non_linear(
    hsp1: HSP, hsp2: HSP
) -> bool:
    """Check if two HSPs overlap (non-linear mode allowed)."""
    def check_pair(s1, e1, s2, e2):
        length1 = e1 - s1 + 1
        length2 = e2 - s2 + 1
        shortest = min(length1, length2)

        if s1 == s2:
            return True
        elif s1 < s2:
            return (s2 - e1 + 1) / shortest < -0.05
        else:
            return (s1 - e2 + 1) / shortest < -0.05

    return (check_pair(hsp1.query_from, hsp1.query_to,
                      hsp2.query_from, hsp2.query_to) or
            check_pair(hsp1.hit_from, hsp1.hit_to,
                      hsp2.hit_from, hsp2.hit_to))


def has_overlap(hsp1: HSP, hsp2: HSP, linear_mode: bool = True) -> bool:
    """Check if two HSPs overlap."""
    if linear_mode:
        # Determine which HSP comes first on query
        if hsp1.query_from == hsp2.query_from:
            return True

        if hsp1.query_from < hsp2.query_from:
            first, last = hsp1, hsp2
        else:
            first, last = hsp2, hsp1

        return (check_overlap_linear(first.query_from, first.query_to,
                                    last.query_from, last.query_to) or
                check_overlap_linear(first.hit_from, first.hit_to,
                                    last.hit_from, last.hit_to))
    else:
        return check_overlap_non_linear(hsp1, hsp2)


def parse_blast_xml(
    xml_file: Path,
    score_cutoff: float,
    alignment_mode: bool = False,
    linear_segment_mode: bool = True,
    output: TextIO = sys.stdout,
) -> None:
    """
    Parse BLAST XML and output formatted results.

    Args:
        xml_file: Path to BLAST XML file
        score_cutoff: Minimum bit score cutoff
        alignment_mode: If True, output aligned sequences
        linear_segment_mode: If True, HSPs must be ordered linearly
        output: Output file handle
    """
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}", file=sys.stderr)
        return

    # Handle both old and new BLAST XML formats
    iterations = root.findall('.//Iteration')
    if not iterations:
        # Try older format
        iterations = [root]

    for iteration in iterations:
        # Get query info
        query_def = iteration.findtext('Iteration_query-def', '')
        if not query_def:
            query_def = root.findtext('.//BlastOutput_query-def', '')
        query_id = query_def.split()[0] if query_def else ''

        query_len = iteration.findtext('Iteration_query-len', '')
        if not query_len:
            query_len = root.findtext('.//BlastOutput_query-len', '0')
        query_length = int(query_len) if query_len else 0

        query = Query(query_id=query_id, query_length=query_length)

        # Process hits
        for hit_elem in iteration.findall('.//Hit'):
            hit_def = hit_elem.findtext('Hit_def', '')
            hit_id = hit_def.split()[0] if hit_def else ''
            hit_length = int(hit_elem.findtext('Hit_len', '0'))

            hit = Hit(hit_id=hit_id, hit_length=hit_length)

            # Process HSPs
            for hsp_elem in hit_elem.findall('.//Hsp'):
                hsp = HSP(
                    bit_score=float(hsp_elem.findtext('Hsp_bit-score', '0')),
                    query_from=int(hsp_elem.findtext('Hsp_query-from', '0')),
                    query_to=int(hsp_elem.findtext('Hsp_query-to', '0')),
                    hit_from=int(hsp_elem.findtext('Hsp_hit-from', '0')),
                    hit_to=int(hsp_elem.findtext('Hsp_hit-to', '0')),
                    qseq=hsp_elem.findtext('Hsp_qseq', ''),
                    hseq=hsp_elem.findtext('Hsp_hseq', ''),
                )

                # Check for overlap with existing HSPs
                overlap_found = False
                for existing_hsp in hit.hsps:
                    if has_overlap(hsp, existing_hsp, linear_segment_mode):
                        overlap_found = True
                        break

                if not overlap_found:
                    hit.hsps.append(hsp)
                    hit.total_score += hsp.bit_score
                    hit.total_query_length += (hsp.query_to - hsp.query_from + 1)
                    hit.total_hit_length += (hsp.hit_to - hsp.hit_from + 1)

            # Add hit if score meets cutoff
            if hit.total_score >= score_cutoff:
                query.hits.append(hit)

        # Sort hits by score (highest first)
        query.hits.sort(key=lambda h: h.total_score, reverse=True)

        # Output results
        for hit in query.hits:
            # Skip self hits in alignment mode
            if alignment_mode and query.query_id == hit.hit_id:
                continue

            # Sort HSPs by query position
            sorted_hsps = sorted(hit.hsps, key=lambda h: h.query_from)
            sorted_hsps_hit = sorted(hit.hsps, key=lambda h: h.hit_from)

            # Calculate segment lengths
            if len(sorted_hsps) > 1:
                segm_length_query = (sorted_hsps[-1].query_to -
                                    sorted_hsps[0].query_from + 1)
                segm_length_hit = (sorted_hsps_hit[-1].hit_to -
                                  sorted_hsps_hit[0].hit_from + 1)
            else:
                segm_length_query = (sorted_hsps[0].query_to -
                                    sorted_hsps[0].query_from + 1)
                segm_length_hit = (sorted_hsps[0].hit_to -
                                  sorted_hsps[0].hit_from + 1)

            # Build output line
            if alignment_mode:
                output.write(">")

            output.write(f"{query.query_id}\t")
            output.write(f"{hit.hit_id}\t")
            output.write(f"{hit.total_score:.1f}\t")
            output.write(f"{query.query_length}\t")
            output.write(f"{hit.hit_length}\t")
            output.write(f"{segm_length_query}\t")
            output.write(f"{segm_length_hit}\t")
            output.write(f"{hit.total_query_length}\t")
            output.write(f"{hit.total_hit_length}\t")

            # Output segment positions
            hsp_qseq = ""
            hsp_hseq = ""
            for hsp in sorted_hsps:
                output.write(f"q:{hsp.query_from}-{hsp.query_to} ")
                output.write(f"h:{hsp.hit_from}-{hsp.hit_to}\t")
                if alignment_mode:
                    hsp_qseq += hsp.qseq
                    hsp_hseq += hsp.hseq

            output.write("\n")

            if alignment_mode:
                output.write(f"{hsp_qseq}\n")
                output.write(f"{hsp_hseq}\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Parse BLAST XML output for InParanoid"
    )
    parser.add_argument(
        "score_cutoff",
        type=float,
        help="Minimum bit score cutoff",
    )
    parser.add_argument(
        "blast_xml",
        type=Path,
        help="BLAST XML result file (-m7 output)",
    )
    parser.add_argument(
        "-a",
        dest="alignment_mode",
        action="store_true",
        help="Alignment mode - output aligned sequences",
    )
    parser.add_argument(
        "--non-linear",
        dest="non_linear",
        action="store_true",
        help="Allow non-linear HSP arrangements",
    )

    args = parser.parse_args()

    if not args.blast_xml.exists():
        print(f"Error: File not found: {args.blast_xml}", file=sys.stderr)
        sys.exit(1)

    parse_blast_xml(
        args.blast_xml,
        args.score_cutoff,
        args.alignment_mode,
        not args.non_linear,  # linear_segment_mode
    )


if __name__ == "__main__":
    main()
