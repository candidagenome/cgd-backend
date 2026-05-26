# CRISPR Ranking Fix - Work in Progress

## Problem Statement

CGD's guide ranking agrees less with CHOPCHOP (~31%) and CRISPOR (~39%) than these tools agree with each other (40-54%). The hypothesis was a **scaling bug** in the efficiency score calculation.

## Fix Implemented (commit fa55600)

**File:** `cgd/api/services/crispr_service.py`

### Changes Made:

1. **Efficiency scaling (line 329):** Changed `penalty -= efficiency_score` to `penalty -= efficiency_score * 100`
   - Rationale: CHOPCHOP uses `penalty -= efficiency * 100` (range 0-10,000), while CGD was using raw efficiency (range 0-100)
   - This caused off-targets (400-1000 penalty each) to dominate rankings far more than in CHOPCHOP

2. **Position bonuses reduced (lines 342-350):** Changed from -8/-5/-2 to -3/-2/-1
   - Rationale: CHOPCHOP uses position as a tie-breaker, not a major ranking factor

3. **Display score mapping updated (lines 355-373):** Adjusted for new penalty range
   - Maps penalty range [-8000, +2000] to score range [100, 0]

## Current Results

After the fix, running benchmark comparisons shows:

| Test | Top 5 Overlap | Top 10 Overlap |
|------|---------------|----------------|
| 3 genes (with off-targets) | - | 17% |
| 10 genes (via API) | 12% | 16% |

**This is WORSE than the baseline (~31%)**, suggesting the fix may have overcorrected or there's another issue.

## What Needs Investigation

1. **Verify baseline:** Re-run benchmark with the OLD code to confirm the 31% baseline
   ```bash
   git stash  # or git checkout HEAD~1
   # restart server and run benchmark
   ```

2. **Check penalty values:** Examine actual penalty calculations to see if efficiency is being weighted correctly
   ```python
   # In the guide response, check:
   # - efficiency_score
   # - chopchop_penalty
   # - offtarget_count
   ```

3. **Compare ranking factors:** For a specific gene, compare CGD vs CHOPCHOP rankings side-by-side with all factors visible

4. **Consider alternative fixes:**
   - Maybe efficiency scaling should be less than 100x
   - Maybe other factors (GC penalty, self-complementarity) need adjustment
   - Maybe the off-target penalties need recalibration

## How to Run Benchmarks

### Quick test (no off-targets, fast):
```bash
ssh cgd-backend-dev
cd work/cgd-backend
source venv/bin/activate

# Restart server to pick up code changes
pkill -f gunicorn
nohup gunicorn -k uvicorn.workers.UvicornWorker -w 2 -b 127.0.0.1:8000 --timeout 300 cgd.main:app > /tmp/cgd-backend.log 2>&1 &

# Run quick comparison via API
python3 -c "
import json, requests
with open('tests/api/fixtures/crispr_test_genes.json') as f:
    data = json.load(f)
for gene_data in data[:5]:
    gene = gene_data['gene_name']
    resp = requests.post('http://localhost:8000/api/crispr/design',
        json={'gene_name': gene, 'organism': 'C_albicans_SC5314_A22',
              'target_region': '5_prime', 'max_guides': 10, 'check_offtargets': False})
    cgd = [g['sequence'] for g in resp.json()['guides'][:10]]
    chop = gene_data['expected_guides_5prime'][:10]
    overlap = len(set(cgd) & set(chop))
    print(f'{gene}: {overlap}/10 overlap')
"
```

### Full benchmark (with off-targets, slow):
```bash
python3 scripts/crispor_benchmark/compare_benchmarks.py
```

### Run unit tests:
```bash
python -m pytest tests/api/test_crispr_service.py -v
```

## Files Reference

- **Main service:** `cgd/api/services/crispr_service.py`
  - `_calculate_chopchop_penalty()` - lines 286-352
  - `_chopchop_penalty_to_display_score()` - lines 355-373
- **Test fixtures:** `tests/api/fixtures/crispr_test_genes.json`
- **Benchmark scripts:** `scripts/crispor_benchmark/`
- **Unit tests:** `tests/api/test_crispr_service.py`

## Git Status

- Branch: `redmine_70_to_75`
- Last commit: `fa55600` - "fix(crispr): scale efficiency by 100x to match CHOPCHOP weighting"
- All 112 unit tests pass

## Next Steps

1. Revert the fix and confirm baseline: `git revert fa55600`
2. Investigate why the fix made rankings worse
3. Try smaller efficiency scaling factors (e.g., 50x, 25x)
4. Consider if CHOPCHOP fixture data itself needs validation
