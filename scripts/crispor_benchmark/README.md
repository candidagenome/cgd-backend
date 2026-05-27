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

## Current Results (May 2025)

### Top 10 Guide Comparison (20 Genes)

| Comparison | Overlap | Match Rate | Description |
|------------|---------|------------|-------------|
| **CGD → CHOPCHOP** | 78/140 | **55.7%** | % of CHOPCHOP top 10 found in CGD top 10 |
| **CHOPCHOP → CRISPOR** | 76/140 | 54.3% | % of CHOPCHOP top 10 found in CRISPOR top 10 |
| **CRISPOR → CGD** | 104/200 | 52.0% | % of CRISPOR top 10 found in CGD top 10 |
| **CGD → CRISPOR** | 104/198 | 52.5% | % of CGD top 10 found in CRISPOR top 10 |
| **CRISPOR → CHOPCHOP** | 76/200 | 38.0% | % of CRISPOR top 10 found in CHOPCHOP top 10 |
| **CGD → CHOPCHOP** | 78/198 | 39.4% | % of CGD top 10 found in CHOPCHOP top 10 |

### Key Findings

- **CGD matches CHOPCHOP better than CRISPOR does** (55.7% vs 54.3%)
- All three tools agree on ~50% of top 10 guides, reflecting different ranking priorities
- CHOPCHOP returns fewer guides per gene (140 total vs 200), affecting overlap percentages

### Extended Comparison (Top 20)

| Comparison | Match Rate |
|------------|------------|
| CGD → CHOPCHOP (top 20) | **78.6%** (110/140) |
| CGD → CRISPOR (top 20) | 79.5% (159/200) |

### CGD Efficiency Model

CGD uses the **Doench 2016 Rule Set 2 (Azimuth)** algorithm for efficiency prediction:

- **30-mer context**: 4bp upstream + 20bp guide + 3bp PAM + 3bp downstream
- **Position-specific features**: Nucleotide and dinucleotide weights from published coefficients
- **Coefficient-based implementation**: No external dependencies (scikit-learn model weights embedded)
- **Fallback**: Heuristic scoring when 30-mer context is unavailable

### CGD Ranking Penalty Formula

```
penalty = off_target_penalty
        - efficiency_score * 25
        - position_bonus (exponential decay for 5' targeting)
        + gc_penalty (if outside 40-70%)
        + self_complementarity_penalty
```

Position bonus uses exponential decay: `2000 + 4000 * exp(-position_fraction * 6)`
- At 0% (gene start): ~-6000 bonus
- At 10%: ~-5200 bonus
- At 50%: ~-2000 bonus
- Beyond 50%: no bonus

## References

- CHOPCHOP: Labun et al. (2019) Nucleic Acids Research
- CRISPOR: Concordet & Haeussler (2018) Nucleic Acids Research
- Doench 2016: Doench et al. (2016) Nature Biotechnology
