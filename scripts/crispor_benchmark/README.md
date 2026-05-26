# CRISPR Guide Design Benchmark: CGD vs CHOPCHOP vs CRISPOR

This directory contains scripts and data for benchmarking CGD's CRISPR guide designer against two popular tools: CHOPCHOP and CRISPOR.

## Overview

| Tool | Type | Scoring Method | Reference |
|------|------|----------------|-----------|
| **CGD** | In-house | Doench 2016 + position bonus + CFD off-target | This repository |
| **CHOPCHOP** | Academic | Doench 2016 + MIT specificity | chopchop.cbu.uib.no |
| **CRISPOR** | Academic | Doench 2016 + CFD specificity | crispor.gi.ucsc.edu |

## Test Dataset

- **Organism**: *Candida albicans* SC5314
- **Genes**: 20 well-studied genes (adhesins, proteases, transcription factors, etc.)
- **Target region**: 5' end of CDS (first 500bp) - optimal for knockout experiments
- **PAM**: NGG (SpCas9)

## Directory Structure

```
crispor_benchmark/
├── README.md                    # This file
├── generate_crispor_input.py    # Generate FASTA files for CRISPOR
├── compare_benchmarks.py        # Compare results across tools
├── crispor_results_template.json# Template for storing CRISPOR results
├── crispor_results.json         # Actual CRISPOR results (fill in)
└── input_sequences/             # Generated FASTA files
    ├── ALS1.fasta
    ├── ALS3.fasta
    ├── ...
    ├── all_genes_batch.fasta    # Combined for batch submission
    └── gene_order.txt           # Reference for batch results
```

## Workflow

### Step 1: Generate Input Sequences
```bash
python scripts/crispor_benchmark/generate_crispor_input.py
```

### Step 2: Run CRISPOR Analysis
1. Go to http://crispor.gi.ucsc.edu/
2. Select genome: **Candida albicans SC5314** (under Fungi)
3. PAM: **NGG**
4. For each gene:
   - Upload or paste sequence from `input_sequences/<GENE>.fasta`
   - Click SUBMIT
   - Record top 10 guides with scores

### Step 3: Record CRISPOR Results
Copy `crispor_results_template.json` to `crispor_results.json` and fill in:
- Guide sequences (20bp, no PAM)
- MIT specificity score
- Doench '16 efficiency score
- Off-target counts

### Step 4: Run Comparison
```bash
python scripts/crispor_benchmark/compare_benchmarks.py
```

## Metrics

The comparison analyzes:

1. **Match Rate**: What % of Tool A's guides appear in Tool B's top N
2. **Ranking Correlation**: How similar are the rankings
3. **Consensus Guides**: Guides recommended by 2+ or all 3 tools
4. **Per-Gene Breakdown**: Which genes have good/poor correlation

## Current Results

### CGD vs CHOPCHOP (as of 2024)
- Match rate: **85.0%** (119/140 CHOPCHOP guides in CGD top 50)
- 13 genes with 100% match
- 7 genes with partial matches (60-87.5%)

### CGD vs CRISPOR
- *Pending CRISPOR data collection*

## References

- CHOPCHOP: Labun et al. (2019) Nucleic Acids Research
- CRISPOR: Concordet & Haeussler (2018) Nucleic Acids Research
- Doench 2016: Doench et al. (2016) Nature Biotechnology
