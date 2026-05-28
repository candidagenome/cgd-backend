# CRISPR Guide Design Benchmark: CGD vs CHOPCHOP vs CRISPOR

This directory contains scripts and data for benchmarking CGD's CRISPR guide designer against two popular tools: CHOPCHOP and CRISPOR.

## Overview

| Tool | Type | Scoring Method | Reference |
|------|------|----------------|-----------|
| **CGD** | In-house | Doench 2016 + position bonus + mismatch-count off-target | This repository |
| **CHOPCHOP** | Academic | Doench 2016 + MIT specificity | chopchop.cbu.uib.no |
| **CRISPOR** | Academic | Doench 2016 + CFD specificity | crispor.gi.ucsc.edu |

## Test Dataset

- **Organism**: *Candida albicans* SC5314
- **Genes**: 20 well-studied genes (adhesins, proteases, transcription factors, etc.)
- **Target region**: 5' end of CDS (first 500bp) - used for benchmark comparison
- **PAM**: NGG (SpCas9)

### Note on CGD's Target Region Settings

CGD offers several target region options with different behaviors:

| Option | Description |
|--------|-------------|
| **5' Region + Upstream (Default)** | 500bp upstream of ATG + first 20% of CDS |
| **5' Region** | First 20% of CDS only |
| **3' Region** | Last 20% of CDS |
| **Full CDS** | Entire coding sequence |

**Why 20% instead of 50%?** CGD uses a conservative 20% threshold for 5' targeting because:
- Frameshifts near the start codon are more effective for knockouts
- Avoids internal start codons that could produce partially functional proteins
- Smaller search region allows more thorough off-target analysis

The benchmark used **first 500bp** to match what CHOPCHOP and CRISPOR provide. For genes with CDS >2500bp, this is less than 20%; for shorter genes, it's more than 20%. The benchmark script uses `target_region="5_prime"` with CGD's API.

---

## Benchmark Results (May 2026)

### Summary

**Overall, CGD guide rankings are broadly consistent with CHOPCHOP and CRISPOR, especially when comparing CGD top 20 against external top 10 results.**

When CGD results are expanded to the top 20, CGD recovers ~79% of CHOPCHOP and CRISPOR guides, suggesting that most differences are due to ranking order rather than missing guide candidates.

### Top 10 Guide Comparison (20 Genes)

| Comparison | Overlap | Match Rate |
|------------|---------|------------|
| CHOPCHOP top 10 found in CGD top 10 | 79/140 | **56.4%** |
| CHOPCHOP top 10 found in CRISPOR top 10 | 76/140 | 54.3% |
| CRISPOR top 10 found in CGD top 10 | 111/200 | 55.5% |
| CGD top 10 found in CRISPOR top 10 | 111/200 | 55.5% |
| CGD top 10 found in CHOPCHOP top 10 | 79/200 | 39.5% |

> **Note on denominators**: CHOPCHOP returned fewer than 10 guides for some genes, so CHOPCHOP-based comparisons use 140 possible guides instead of 200. CGD and CRISPOR each returned 200 guides across the 20 genes.

### Extended Comparison (Top 20)

| Comparison | Match Rate |
|------------|------------|
| CHOPCHOP top 10 found in CGD top 20 | **78.6%** (110/140) |
| CRISPOR top 10 found in CGD top 20 | **79.5%** (159/200) |

This is the strongest evidence that CGD is finding the same candidate guides as other tools, even if the exact ranking differs.

### Key Findings

1. **CGD top guides show similar overlap with CHOPCHOP and CRISPOR**, with ~50–56% agreement in strict top 10 comparisons.

2. **When CGD results are expanded to the top 20**, CGD recovers ~79% of CHOPCHOP/CRISPOR guides, suggesting that many differences are due to ranking order rather than missing guide candidates.

3. **CHOPCHOP returned fewer guides for some genes**, so CHOPCHOP comparisons use 140 possible guides instead of 200.

4. **Match rate measures overlap between tools, not biological correctness.** Each tool uses different efficiency, specificity, filtering, and ranking criteria. Differences are expected.

### Important Caveat

These predictions are intended to prioritize guide selection. **Experimental validation is still recommended** before proceeding with final experiments.

---

## How CGD Scores Guides

### Efficiency Prediction (Doench 2016 Rule Set 2)

CGD uses the **Doench 2016 Rule Set 2 (Azimuth)** algorithm for efficiency prediction:

- **30-mer context**: 4bp upstream + 20bp guide + 3bp PAM + 3bp downstream
- **Position-specific features**: Nucleotide and dinucleotide weights from published coefficients
- **Coefficient-based implementation**: No external dependencies (scikit-learn model weights embedded)
- **Fallback**: Heuristic scoring when 30-mer context is unavailable

### Ranking Formula

Final guide ranking includes more than just efficiency. CGD uses a penalty-based system where **lower penalty = better rank**:

```
penalty = off_target_penalty
        - efficiency_score * 25
        - position_bonus (exponential decay for 5' targeting)
        + gc_penalty (if outside 40-70%)
        + self_complementarity_penalty
```

**Position bonus** uses exponential decay to favor early guides for knockout experiments:
- At 0% (gene start): ~-6000 bonus (highest priority)
- At 10%: ~-5200 bonus
- At 20%: ~-4000 bonus (end of 5' Region)
- At 50%: ~-2000 bonus
- Beyond 50%: no bonus (lowest priority for knockouts)

This position weighting means guides in the **first 20% of CDS** receive the highest ranking boost, aligning with CGD's recommended 5' Region targeting. Guides beyond 50% are not penalized but receive no position bonus, making them rank lower than equally-scored guides near the start.

### Off-Target Penalty Calculation

CGD uses a **mismatch-count-based penalty system** (similar to CHOPCHOP) rather than CFD scoring. Each off-target hit found by BLAST adds to the penalty based on the number of mismatches:

| Mismatches | Penalty per Hit |
|------------|-----------------|
| 0 (exact match) | +1000 |
| 1 | +800 |
| 2 | +600 |
| 3 | +400 |

Additional rules:
- If a guide has **>100 off-target hits**, an extra +20,000 penalty is applied
- Off-targets with ≥4 mismatches are not counted (considered unlikely to cut)

**Example**: A guide with 0 exact matches, 2 hits at 1mm, 5 hits at 2mm, and 10 hits at 3mm:
```
off_target_penalty = (0 × 1000) + (2 × 800) + (5 × 600) + (10 × 400) = 8600
```

### Specificity Score

Specificity scores are only shown when off-target checking is performed:

- **With off-target checking**: Specificity is calculated based on BLAST search results. Guides with high specificity and good efficiency are marked as "Recommended."

- **Without off-target checking**: Specificity shows as "—" (not checked). Guides can still be ranked by efficiency and position, but they are not marked as recommended.

This distinction is important because the UI behavior depends on whether off-target checking was enabled.

### Note on Scoring Methods

CGD's mismatch-count penalty is simpler than CFD (Cutting Frequency Determination), which uses a position-specific matrix derived from experimental data. The mismatch-count approach:
- Treats all mismatches at a given count equally (regardless of position or nucleotide type)
- Is computationally simpler
- Provides similar ranking results for most use cases

---

## Directory Structure

```
crispor_benchmark/
├── README.md                    # This file
├── collect_crispor_auto.py      # Automatic CRISPOR guide collection
├── compare_benchmarks.py        # Compare results across tools
├── generate_crispor_input.py    # Generate FASTA files for CRISPOR
├── crispor_results_template.json# Template for storing CRISPOR results
├── crispor_results.json         # Actual CRISPOR results
└── input_sequences/             # Generated FASTA files
    ├── ALS1.fasta
    ├── ALS3.fasta
    ├── ...
    ├── all_genes_batch.fasta    # Combined for batch submission
    └── gene_order.txt           # Reference for batch results

# Related scripts (in parent scripts/ directory):
# ├── fetch_chopchop_guides.py   # Automatic CHOPCHOP guide collection (Playwright)
```

## Automatic Data Collection

> **Note**: Neither CHOPCHOP nor CRISPOR provides a formal API. The `fetch_chopchop_guides.py` script uses **Playwright browser automation** to submit sequences to the CHOPCHOP website and scrape the results. Similarly, `collect_crispor_auto.py` submits **HTTP form requests** directly to the CRISPOR web interface. These scripts may break if the websites change their UI or form structure.

### Collecting CRISPOR Guides

Use the automated script to collect CRISPOR guides via the web interface:

```bash
# Collect guides for all 20 benchmark genes
python scripts/crispor_benchmark/collect_crispor_auto.py

# Collect guides for specific genes
python scripts/crispor_benchmark/collect_crispor_auto.py --genes ALS1,HWP1,EFG1

# Adjust delay between requests (default: 5 seconds)
python scripts/crispor_benchmark/collect_crispor_auto.py --delay 10
```

Results are saved to `crispor_results.json`.

### Collecting CHOPCHOP Guides

Use the Playwright-based script to collect CHOPCHOP guides:

```bash
# Install Playwright first
pip install playwright
playwright install chromium

# Collect guides for all genes
python scripts/fetch_chopchop_guides.py

# Collect guides for a specific gene
python scripts/fetch_chopchop_guides.py --gene HOG1

# Run in visible mode for debugging
python scripts/fetch_chopchop_guides.py --visible
```

Results are saved to `tests/api/fixtures/crispr_test_genes.json` in the `expected_guides_5prime` field.

---

## Manual Workflow (Alternative)

### Step 1: Generate Input Sequences
```bash
python scripts/crispor_benchmark/generate_crispor_input.py
```

### Step 2: Run CRISPOR Analysis (Manual)
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

---

## References

- CHOPCHOP: Labun et al. (2019) Nucleic Acids Research
- CRISPOR: Concordet & Haeussler (2018) Nucleic Acids Research
- Doench 2016: Doench et al. (2016) Nature Biotechnology
