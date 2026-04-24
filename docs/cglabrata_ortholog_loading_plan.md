# C. glabrata Transitive Ortholog Loading Plan

## Summary

Use S. cerevisiae as a bridge to create transitive orthologs between C. glabrata and Candida species.

### Data Flow
```
C. glabrata Gene A <=> S. cerevisiae Gene B (via YGOB)
                     +
S. cerevisiae Gene B <=> C. albicans Gene C (via CGOB)
                     =
C. glabrata Gene A <=> C. albicans Gene C (transitive)
```

## Current Data State

### Source Files (on cgd-backend-dev:/data/)
- `/data/ygob/Pillars.tab` - YGOB homology pillars
- `/data/ygob/cglabrata_scerevisiae_orthologs.tsv` - C. glabrata <-> S. cerevisiae mapping (already generated)
- `/data/cgob/Pillars.tab` - CGOB homology pillars
- `/data/cgob/cglabrata_transitive_orthologs.tsv` - Transitive orthologs (already generated)

### Transitive Ortholog Statistics
- **4,245** C. glabrata genes with transitive orthologs
- **16,605** total ortholog pairs:
  - C. albicans SC5314: 4,225 pairs
  - C. auris B8441: 4,004 pairs
  - C. dubliniensis CD36: 4,213 pairs
  - C. parapsilosis CDC317: 4,163 pairs

### Database Current State
- **37,549** total C. glabrata features in database
- **4,750** C. glabrata genes already have CGOB orthologs
- **4,117** genes overlap between transitive file and existing CGOB data
- **128 NEW genes** from transitive orthologs not currently in CGOB groups

## Key Findings

1. **Most transitive orthologs already exist**: The CGOB data already includes C. glabrata in most ortholog groups. The transitive approach confirms these relationships but adds only ~128 new genes.

2. **Feature naming differences**:
   - Transitive file uses `orf19.XXXX` for C. albicans (Assembly 19)
   - Database uses `C1_XXXXX_A`/`C2_XXXXX_A` (Assembly 22)
   - `orf19.XXXX` names exist as aliases in the database
   - Need to resolve via alias lookup when matching

3. **Case sensitivity**: Some feature names have slight case differences (e.g., `CD36_16530` vs `Cd36_16530`)

## Recommended Approach

### Option A: Add Missing C. glabrata to Existing CGOB Groups (Recommended)
For the 128 NEW C. glabrata genes, find the matching CGOB ortholog groups and add the C. glabrata feature.

**Pros:**
- Maintains consistency with existing CGOB data
- Smaller data change (128 additions vs 4,000+)
- Ortholog groups stay unified

**Cons:**
- Need to map orf19.XXXX to Assembly 22 features via aliases
- Some groups may not exist if the S. cerevisiae gene wasn't in CGOB

### Option B: Create New Homology Groups for All Transitive Orthologs
Create new `ortholog / YGOB_transitive` homology groups for all transitive relationships.

**Pros:**
- Clear provenance (YGOB-derived vs CGOB-direct)
- Complete coverage of transitive relationships

**Cons:**
- Duplicates existing CGOB data
- 4,000+ new homology groups

## Implementation Plan (Option A)

### Phase 1: Data Validation Script
Create a Python script to:
1. Read transitive orthologs file
2. Map orf19.XXXX to Assembly 22 feature_no via alias lookup
3. Find existing CGOB homology groups containing the Candida orthologs
4. Identify C. glabrata features not already in those groups
5. Generate report of proposed additions

### Phase 2: Database Loading Script
Create a Python script to:
1. For each new C. glabrata -> Candida ortholog:
   - Find the CGOB homology group containing the Candida feature
   - If group exists and C. glabrata not in it:
     - Add `FeatHomology` record linking C. glabrata to the group
   - If no existing group:
     - Log for manual review or create new group
2. Commit changes with audit trail

### Phase 3: Verification
1. Run counts to verify additions
2. Sample spot-checks on specific genes
3. Verify locus pages show new orthologs

## Database Schema Reference

### Tables
- `MULTI.homology_group` - Ortholog group definitions
  - `homology_group_type`: 'ortholog'
  - `method`: 'CGOB' (existing) or 'YGOB_transitive' (new)

- `MULTI.feat_homology` - Links features to homology groups
  - `feature_no`: FK to feature
  - `homology_group_no`: FK to homology_group

### Existing Methods
- `best hit for C. glabrata CBS138 / BLAST`: 3,232 entries
- `best hit for C. auris B8441 / BLAST`: 1,971 entries
- `ortholog / CGOB`: 7,676 entries

## Scripts Location
- `/data/ygob/parse_orthologs.awk` - Generates C. glabrata <-> S. cerevisiae mapping
- `/data/cgob/parse_transitive_orthologs.py` - Generates transitive orthologs

## Next Steps
1. Review and approve this plan
2. Create validation script
3. Run validation on dev database
4. Review validation results
5. Create and run loading script on dev
6. Verify on dev
7. Deploy to production (if approved)
