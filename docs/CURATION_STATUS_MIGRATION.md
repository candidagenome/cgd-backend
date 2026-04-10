# Curation Status Migration Analysis

**Date:** April 2026
**Purpose:** Document the current state of paper curation statuses and propose a migration strategy to simplify the system.

## Background

The current curation system has two statuses that have become confusing:

- **"Not Yet Curated"** - Currently a catch-all for papers that came into the system but were never triaged
- **"High Priority"** - Papers that actually need curation

The goal is to simplify to:
- **"Not yet curated"** - Papers that need curation (what "High Priority" means today)
- **"Done: Curated"** - Completed papers

However, we cannot simply rename statuses because the "Not Yet Curated" bucket contains legacy data that would get mixed with recent papers.

## Current State Analysis

### Overall Counts by Curation Status

| Status | Count |
|--------|-------|
| Not yet curated | 42,623 |
| Basic, lit guide, GO, Pheno curation done | 7,796 |
| Done: Abstract curated, full text not curated | 3,537 |
| not gene specific | 784 |
| **High Priority** | **407** |
| Related species | 376 |
| Pathways | 292 |
| Abstract curated, full text not curated | 212 |
| cell biology | 179 |
| clinical | 154 |
| multiple | 149 |
| other | 123 |
| Dataset to load | 73 |
| Genomic sequence not identified | 31 |
| Gene model | 9 |
| Reserved | 1 |
| **TOTAL** | **56,746** |

### "Not Yet Curated" Breakdown

#### By Publication Year

| Period | Paper Count | Percentage |
|--------|-------------|------------|
| Pre-2020 (old papers) | 42,553 | 99.8% |
| 2020 or later (recent) | 70 | 0.2% |

Detailed year distribution:
- 2025: 15
- 2024: 6
- 2023: 5
- 2022: 16
- 2021: 13
- 2020: 15
- 2019: 126
- 2013-2018: ~3,300
- 2000-2012: ~14,000
- Pre-2000: ~25,000
- Oldest: 1940

#### By Date Added to System

| Year Added | Count | Notes |
|------------|-------|-------|
| 2008 | 35,412 | **Single bulk import (83%)** |
| 2009 | 1,524 | |
| 2010-2019 | ~5,600 | |
| 2020-2026 | ~90 | Recent additions |

#### Gene Associations

| Has Gene Links? | Count | Percentage |
|-----------------|-------|------------|
| With gene links | 191 | 0.4% |
| Without gene links | 42,432 | 99.6% |

#### Partially Curated

Papers marked "Not yet curated" but having literature_topic annotations: **645**

### "High Priority" Breakdown

| Year | Count |
|------|-------|
| 2026 | 121 |
| 2025 | 57 |
| 2024 | 3 |
| 2020-2023 | 9 |
| Pre-2020 | 217 |
| **TOTAL** | **407** |

### Other Reference Counts

- Total references in database: 55,909
- References with PubMed ID: 55,844
- Discarded papers (REF_BAD): 3,532

## Key Findings

1. **The "Not Yet Curated" bucket is 99.8% historical papers** - only 70 papers are from 2020 or later.

2. **83% came from a single 2008 bulk import** - 35,412 papers were loaded at once, likely a comprehensive PubMed import that was never triaged.

3. **99.6% have no gene associations** - these papers are essentially orphaned in the system.

4. **"High Priority" is the actual work queue** - only 407 papers, mostly recent (178 from 2025-2026).

5. **The 2008 import includes papers dating back to 1940** - historical literature that was imported comprehensively.

## Proposed Migration Strategies

### Option 1: Archive + Rename (Recommended)

1. **Create new status**: "Archived: Legacy Import"
2. **Migrate old papers**: Move all pre-2020 "Not yet curated" papers (42,553) to "Archived: Legacy Import"
3. **Review recent papers**: Manually review the 70 recent "Not yet curated" papers (2020+) and either:
   - Move to "High Priority" if they need curation
   - Move to an appropriate "Done:" status if not relevant
4. **Rename statuses**:
   - "High Priority" becomes "Not yet curated" (new meaning)
   - Old "Not yet curated" status is deprecated
5. **Update UI**: Remove "High Priority" from dropdowns, update labels

**Pros:**
- Preserves historical data for reference
- Clear audit trail
- Non-destructive

**Cons:**
- Adds a new status to the system
- Requires UI updates

### Option 2: Simple Cutoff with "Done" Status

1. **Bulk update**: Change all pre-2020 "Not yet curated" to "Done: No curation needed"
2. **Move recent papers**: Move 70 recent "Not yet curated" papers to "High Priority"
3. **Rename**: "High Priority" becomes "Not yet curated"
4. **Deprecate**: Remove old "Not yet curated" status

**Pros:**
- Simpler, uses existing status values
- Fewer changes needed

**Cons:**
- Mixes legacy imports with legitimately curated papers in "Done" status
- Less clear audit trail

### Option 3: Delete Legacy Data

1. **Delete**: Remove the 42,553 pre-2020 "Not yet curated" REF_PROPERTY records
2. **Keep references**: The REFERENCE records stay, just lose their curation_status property
3. **Continue**: "High Priority" becomes "Not yet curated"

**Pros:**
- Cleanest result
- Reduces database clutter

**Cons:**
- Irreversible
- May lose useful historical context

## Technical Implementation Notes

### Database Tables Involved

- `REF_PROPERTY` - Stores curation status (`property_type='curation_status'`)
- `REFPROP_FEAT` - Links ref_property to features (genes)
- `REFERENCE` - Main reference table (has unused `curation_status` column)

### Analysis Script

A script has been created to regenerate this analysis:

```bash
cd cgd-backend
source .venv/bin/activate
python scripts/analyze_curation_status.py
```

### Migration Script Location

Migration scripts should be created in: `cgd-backend/scripts/migrations/`

## Next Steps

1. [ ] Discuss options with PI
2. [ ] Decide on migration strategy
3. [ ] Create migration script with dry-run mode
4. [ ] Test on development database
5. [ ] Schedule migration window
6. [ ] Update frontend UI after migration
7. [ ] Update documentation

## Questions to Discuss with PI

1. Do we need to preserve the historical "Not yet curated" papers, or can they be archived/deleted?
2. Should the 70 recent papers (2020+) be automatically moved to the curation queue, or manually reviewed?
3. Is there any reporting or analytics that depends on the current status values?
4. What should happen to the 645 "partially curated" papers (have topics but still marked NYC)?
5. Timeline preferences for the migration?
