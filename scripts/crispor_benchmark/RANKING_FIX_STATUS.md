# CRISPR Ranking Fix - Investigation Complete

## Problem Statement

CGD's guide ranking agrees less with CHOPCHOP (~31%) and CRISPOR (~39%) than these tools agree with each other (40-54%). The hypothesis was a **scaling bug** in the efficiency score calculation.

## Investigation Summary (2025-05-26)

### What Was Tested

1. **Efficiency scaling (100x):** Changed `penalty -= efficiency_score` to `penalty -= efficiency_score * 100`
2. **Position bonus increases:** Changed from -3/-2/-1 to -2000/-1500/-1000/-500 for 5' targeting

### Benchmark Results (50 genes, fixture sequences)

| Configuration | Top-5 Overlap | Top-10 Overlap |
|---------------|---------------|----------------|
| Baseline (position: -3/-2/-1) | 22.8% | 38.2% |
| Increased position bonuses (-2000/-1500/-1000/-500) | 25.2% | 40.0% |

### Key Findings

1. **Position bonuses help modestly** (+2.4% top-5, +1.8% top-10)

2. **The real issue is efficiency algorithm differences:**
   - CGD uses a simplified efficiency heuristic (GC content, poly-T, etc.)
   - CHOPCHOP uses the Doench 2016 model with position-specific nucleotide preferences
   - Example: CHOPCHOP ranks a guide with CGD efficiency=49 above one with efficiency=55
   - This fundamental difference cannot be fixed with scaling factors

3. **40% top-10 overlap is actually reasonable:**
   - This is comparable to CHOPCHOP vs CRISPOR agreement (40-54%)
   - Different tools use different efficiency algorithms and arrive at different rankings

4. **Fixture data vs database sequences:**
   - Some CHOPCHOP guides were "NOT FOUND" because fixture sequences differ from current database CDS
   - The 500bp fixture sequences were likely generated from a different source/version

### Current Position Bonus Implementation

```python
# For 5' targeting, position bonuses based on CDS percentage:
if pct <= 0.10:      # First 10%
    penalty -= 2000
elif pct <= 0.15:    # 10-15%
    penalty -= 1500
elif pct <= 0.25:    # 15-25%
    penalty -= 1000
elif pct <= 0.35:    # 25-35%
    penalty -= 500
# 35-50%: no bonus
```

## Recommendations

### For Improved Ranking (Future Work)

1. **Implement a validated efficiency model** (e.g., Doench 2016 Rule Set 2) to better match CHOPCHOP/CRISPOR
   - This would require: dinucleotide context, position-specific nucleotide preferences, PAM-proximal effects

2. **Update fixture data** to match current database CDS sequences for accurate benchmarking

### Current Status

- The position bonus changes provide a small improvement
- The 40% top-10 overlap is acceptable given algorithm differences
- All 82 unit tests pass

## Files Reference

- **Main service:** `cgd/api/services/crispr_service.py`
  - `_calculate_chopchop_penalty()` - lines 286-355
  - `_chopchop_penalty_to_display_score()` - lines 358-376
- **Test fixtures:** `tests/api/fixtures/crispr_test_genes.json`
- **Unit tests:** `tests/api/test_crispr_service.py`

## Git Status

- Branch: `redmine_79_to_85`
- All 82 unit tests pass
