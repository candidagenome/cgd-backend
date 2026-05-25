#!/usr/bin/env python3
"""
Generate CRISPOR-compatible input for benchmarking.

This script creates two output formats:
1. Individual FASTA files for each gene (for manual web submission)
2. A combined batch file with N-separated sequences (for batch submission)

CRISPOR web interface: http://crispor.gi.ucsc.edu/
- Select genome: "Candida albicans SC5314"
- PAM: NGG (SpCas9)
- Paste sequence or upload file
"""

import json
import os
from pathlib import Path


def main():
    # Load test genes fixture
    fixture_path = Path(__file__).parent.parent.parent / "tests/api/fixtures/crispr_test_genes.json"
    with open(fixture_path) as f:
        genes = json.load(f)

    # Create output directory
    output_dir = Path(__file__).parent / "input_sequences"
    output_dir.mkdir(exist_ok=True)

    # Generate individual FASTA files
    print("Generating individual FASTA files...")
    for gene in genes:
        gene_name = gene["gene_name"]
        feature_name = gene["feature_name"]
        seq = gene["cds_first_500bp"]

        fasta_path = output_dir / f"{gene_name}.fasta"
        with open(fasta_path, "w") as f:
            f.write(f">{gene_name}_{feature_name}_5prime_500bp\n")
            # Write sequence in 60-char lines
            for i in range(0, len(seq), 60):
                f.write(seq[i:i+60] + "\n")
        print(f"  Created: {fasta_path.name}")

    # Generate combined batch file (N-separated)
    print("\nGenerating combined batch file...")
    batch_path = output_dir / "all_genes_batch.fasta"
    with open(batch_path, "w") as f:
        f.write(">all_20_genes_batch\n")
        seqs = []
        for gene in genes:
            seqs.append(gene["cds_first_500bp"])
        # Join with 10 N's as separator (CRISPOR batch format)
        combined = "NNNNNNNNNN".join(seqs)
        for i in range(0, len(combined), 60):
            f.write(combined[i:i+60] + "\n")
    print(f"  Created: {batch_path.name}")

    # Generate gene list for reference
    gene_list_path = output_dir / "gene_order.txt"
    with open(gene_list_path, "w") as f:
        f.write("# Gene order in batch file (separated by 10 N's)\n")
        f.write("# Use this to map CRISPOR results back to genes\n\n")
        for i, gene in enumerate(genes, 1):
            f.write(f"{i}. {gene['gene_name']} ({gene['feature_name']})\n")
            f.write(f"   CDS length: {gene['cds_length']} bp\n")
            f.write(f"   5' sequence: {len(gene['cds_first_500bp'])} bp\n")
            f.write(f"   CHOPCHOP expected guides: {len(gene['expected_guides_5prime'])}\n\n")
    print(f"  Created: {gene_list_path.name}")

    print("\n" + "=" * 70)
    print("CRISPOR Benchmark Input Generated")
    print("=" * 70)
    print(f"\nOutput directory: {output_dir}")
    print(f"Total genes: {len(genes)}")
    print(f"Total CHOPCHOP expected guides: {sum(len(g['expected_guides_5prime']) for g in genes)}")

    print("\n" + "-" * 70)
    print("INSTRUCTIONS FOR CRISPOR WEB SUBMISSION")
    print("-" * 70)
    print("""
1. Go to: http://crispor.gi.ucsc.edu/

2. For EACH gene (recommended for accuracy):
   - Select genome: "Candida albicans SC5314" (under Fungi)
   - PAM: NGG
   - Upload or paste the sequence from input_sequences/<GENE>.fasta
   - Click "SUBMIT"
   - Download results as Excel or copy the guide table
   - Save to: crispor_results/<GENE>_crispor.tsv

3. For BATCH mode (faster but may be less precise):
   - Use input_sequences/all_genes_batch.fasta
   - Guides will be numbered; use gene_order.txt to map back

4. Key columns to record for each guide:
   - Guide sequence (20bp)
   - Position/strand
   - MIT Specificity Score (0-100)
   - Doench '16 Efficiency Score
   - Off-target counts (0mm, 1mm, 2mm, 3mm)

5. After collecting results, run:
   python scripts/crispor_benchmark/compare_benchmarks.py
""")


if __name__ == "__main__":
    main()
