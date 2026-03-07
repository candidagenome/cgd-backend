#!/usr/bin/env python3
"""
Find closest non-orthologs (CNO) for genes in InParanoid clusters.

This script finds the closest non-ortholog in the same species for all genes
present in an XML cluster file from InParanoid.

Original Perl: cnoFinder.pl
Converted to Python: 2024

Usage:
    python cno_finder.py ORGANISM1 ORGANISM2 clusters.xml blastAA.txt blastBB.txt
"""

import argparse
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path


# Algorithm parameters
SCORE_CUTOFF = 50  # Minimum bit score
SEQ_OVERLAP_CUTOFF = 0.5  # Match area must cover this much of longer sequence
SEGMENT_COVERAGE_CUTOFF = 0.25  # Matching segments must cover this much


@dataclass
class Cluster:
    """InParanoid cluster."""
    cluster_id: str
    orthologs_a: list = field(default_factory=list)
    orthologs_b: list = field(default_factory=list)


def parse_cluster_xml(xml_file: Path, organism1: str, organism2: str) -> tuple:
    """
    Parse InParanoid cluster XML file.

    Args:
        xml_file: Path to XML cluster file
        organism1: Name of first organism
        organism2: Name of second organism

    Returns:
        Tuple of (clusters list, ortholog_table dict)
    """
    clusters = []
    ortholog_table = {}

    tree = ET.parse(xml_file)
    root = tree.getroot()

    for cluster_elem in root.findall('.//CLUSTER'):
        cluster_id = cluster_elem.get('CLUSTERNO', '')
        cluster = Cluster(cluster_id=cluster_id)

        for gene_elem in cluster_elem.findall('GENE'):
            species = gene_elem.get('SPECIES', '')
            protein_id = gene_elem.get('PROTID', '')

            if not protein_id:
                protein_id = gene_elem.get('GENEID', '')

            # Add to ortholog table
            ortholog_table[protein_id] = {}

            # Add to appropriate organism list
            if species == organism1:
                cluster.orthologs_a.append(protein_id)
            else:
                cluster.orthologs_b.append(protein_id)

        clusters.append(cluster)

    return clusters, ortholog_table


def read_blast_file(
    blast_file: Path,
    ortholog_table: dict,
    score_cutoff: float = SCORE_CUTOFF,
    seq_overlap_cutoff: float = SEQ_OVERLAP_CUTOFF,
    segment_coverage_cutoff: float = SEGMENT_COVERAGE_CUTOFF,
) -> None:
    """
    Read BLAST file and populate hit table.

    Args:
        blast_file: Path to BLAST output file (parsed format)
        ortholog_table: Dict to populate with hits
        score_cutoff: Minimum score cutoff
        seq_overlap_cutoff: Sequence overlap cutoff
        segment_coverage_cutoff: Segment coverage cutoff
    """
    with open(blast_file) as f:
        for line in f:
            fields = line.strip().split()
            if len(fields) < 9:
                continue

            query = fields[0]
            match = fields[1]
            score = float(fields[2])

            # Skip if query not in ortholog table
            if query not in ortholog_table:
                continue

            # Get lengths
            query_len = int(fields[3])
            hit_len = int(fields[4])
            match_region_query = int(fields[5])
            match_region_hit = int(fields[6])
            total_match_query = int(fields[7])
            total_match_hit = int(fields[8])

            # Apply coverage filters
            if query_len > hit_len:
                if match_region_query < seq_overlap_cutoff * query_len:
                    continue
                if total_match_query < segment_coverage_cutoff * query_len:
                    continue
            elif query_len < hit_len:
                if match_region_hit < seq_overlap_cutoff * hit_len:
                    continue
                if total_match_hit < segment_coverage_cutoff * hit_len:
                    continue
            else:
                if (match_region_query < seq_overlap_cutoff * query_len or
                    match_region_hit < seq_overlap_cutoff * hit_len):
                    continue
                if total_match_query < segment_coverage_cutoff * query_len:
                    continue
                if total_match_hit < segment_coverage_cutoff * hit_len:
                    continue

            # Skip if score below cutoff
            if score < score_cutoff:
                continue

            # Store hit with integer score
            int_score = int(score + 0.5)
            ortholog_table[query][match] = int_score


def find_cno(ortholog_id: str, orthologs: list, hit_table: dict) -> tuple:
    """
    Find closest non-ortholog for a given ortholog.

    Args:
        ortholog_id: ID of ortholog to find CNO for
        orthologs: List of orthologs in the same cluster
        hit_table: Dict of hits for this ortholog

    Returns:
        Tuple of (cno_id, score) or (None, None)
    """
    if not hit_table:
        return None, None

    # Sort hits by score (highest first)
    sorted_hits = sorted(hit_table.items(), key=lambda x: x[1], reverse=True)

    # Find first hit not in the orthologs list
    for hit_id, score in sorted_hits:
        if hit_id not in orthologs:
            return hit_id, score

    return None, None


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Find closest non-orthologs for InParanoid clusters"
    )
    parser.add_argument(
        "organism1",
        help="Name of first organism (as in XML file)",
    )
    parser.add_argument(
        "organism2",
        help="Name of second organism (as in XML file)",
    )
    parser.add_argument(
        "cluster_file",
        type=Path,
        help="XML cluster file",
    )
    parser.add_argument(
        "blast_aa",
        type=Path,
        help="BLAST file for organism1 vs organism1",
    )
    parser.add_argument(
        "blast_bb",
        type=Path,
        help="BLAST file for organism2 vs organism2",
    )
    parser.add_argument(
        "--score-cutoff",
        type=float,
        default=SCORE_CUTOFF,
        help=f"Minimum bit score (default: {SCORE_CUTOFF})",
    )
    parser.add_argument(
        "--seq-overlap",
        type=float,
        default=SEQ_OVERLAP_CUTOFF,
        help=f"Sequence overlap cutoff (default: {SEQ_OVERLAP_CUTOFF})",
    )
    parser.add_argument(
        "--segment-coverage",
        type=float,
        default=SEGMENT_COVERAGE_CUTOFF,
        help=f"Segment coverage cutoff (default: {SEGMENT_COVERAGE_CUTOFF})",
    )

    args = parser.parse_args()

    # Validate files
    for f in [args.cluster_file, args.blast_aa, args.blast_bb]:
        if not f.exists():
            print(f"Error: File not found: {f}", file=sys.stderr)
            sys.exit(1)

    # Parse cluster file
    clusters, ortholog_table = parse_cluster_xml(
        args.cluster_file, args.organism1, args.organism2
    )

    # Read BLAST files
    read_blast_file(
        args.blast_aa, ortholog_table,
        args.score_cutoff, args.seq_overlap, args.segment_coverage
    )
    read_blast_file(
        args.blast_bb, ortholog_table,
        args.score_cutoff, args.seq_overlap, args.segment_coverage
    )

    # Process each cluster
    for cluster in clusters:
        print(f">{cluster.cluster_id}")

        # Process organism1 orthologs
        for ortholog in cluster.orthologs_a:
            print(f"{args.organism1}\t{ortholog}\t", end="")
            cno_id, score = find_cno(
                ortholog, cluster.orthologs_a, ortholog_table.get(ortholog, {})
            )
            if cno_id:
                print(f"{cno_id}\t{score}")
            else:
                print()

        # Process organism2 orthologs
        for ortholog in cluster.orthologs_b:
            print(f"{args.organism2}\t{ortholog}\t", end="")
            cno_id, score = find_cno(
                ortholog, cluster.orthologs_b, ortholog_table.get(ortholog, {})
            )
            if cno_id:
                print(f"{cno_id}\t{score}")
            else:
                print()


if __name__ == "__main__":
    main()
