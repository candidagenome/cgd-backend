# BioGRID Interaction Data Loader

This directory contains scripts for loading physical and genetic interaction data from BioGRID into CGD.

## Data Statistics (June 2026)

| Type | Raw Interactions |
|------|------------------|
| Physical | 1,520 |
| Genetic | 530 |
| **Total** | **2,099** |

After filtering (inter-species removed, missing PMIDs, duplicates):
- **1,806 interactions** loaded
- **178 interactions** skipped due to 8 missing PMIDs
- **49 interactions** skipped (inter-species)
- **66 interactions** skipped (duplicates)

---

## Production Deployment Checklist

### Step 1: Set Environment Variables

Add to `.env` file on the server:
```bash
BIOGRID_ACCESS_KEY=896de1f1a0b7fbe68517145c8bbff8e7
```

### Step 2: Add BioGRID Experiment Types to CV Terms

The database trigger validates `experiment_type` against the `cv_term` table (cv_no=7). Run this SQL or Python script to add the 22 BioGRID experiment types:

```sql
-- Add BioGRID experiment types to cv_term table (cv_no=7 = experiment_type)
-- Run as a user in the dbuser table (e.g., SHUAI, CGDADMIN)

INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Affinity Capture-MS', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Affinity Capture-Western', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Biochemical Activity', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Co-crystal Structure', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Co-localization', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Co-purification', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Dosage Growth Defect', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Dosage Rescue', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'FRET', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Far Western', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Negative Genetic', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'PCA', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Phenotypic Enhancement', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Phenotypic Suppression', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Positive Genetic', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Protein-peptide', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Reconstituted Complex', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Synthetic Growth Defect', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Synthetic Haploinsufficiency', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Synthetic Lethality', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Synthetic Rescue', SYSDATE, 'SHUAI');
INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by) VALUES ((SELECT MAX(cv_term_no)+1 FROM MULTI.cv_term), 7, 'Two-hybrid', SYSDATE, 'SHUAI');
COMMIT;
```

Or use this Python script:
```bash
cd ~/work/cgd-backend
source venv/bin/activate
python scripts/loading/biogrid/add_cv_terms.py
```

### Step 2b: Add Code Table Entries

The database triggers also validate `source` and `action` values against the `code` table:

```sql
-- Add BioGRID as a source for INTERACTION table
INSERT INTO MULTI.code (code_no, tab_name, col_name, code_value, description, date_created, created_by)
VALUES ((SELECT MAX(code_no)+1 FROM MULTI.code), 'INTERACTION', 'SOURCE', 'BioGRID', 'BioGRID protein interaction database', SYSDATE, 'SHUAI');

-- Add Bait and Hit as actions for FEAT_INTERACT table
INSERT INTO MULTI.code (code_no, tab_name, col_name, code_value, description, date_created, created_by)
VALUES ((SELECT MAX(code_no)+1 FROM MULTI.code), 'FEAT_INTERACT', 'ACTION', 'Bait', 'Bait protein in interaction', SYSDATE, 'SHUAI');

INSERT INTO MULTI.code (code_no, tab_name, col_name, code_value, description, date_created, created_by)
VALUES ((SELECT MAX(code_no)+1 FROM MULTI.code), 'FEAT_INTERACT', 'ACTION', 'Hit', 'Hit/prey protein in interaction', SYSDATE, 'SHUAI');

COMMIT;
```

### Step 3: (Optional) Add Missing References

8 PMIDs are missing from CGD's reference table, causing 178 interactions to be skipped:

| PMID | Year | First Author | Title | Candida-relevant? |
|------|------|--------------|-------|-------------------|
| 23885115 | 2013 | Burrack LS | Monopolin recruits condensin to organize centromere DNA... | Maybe |
| 23953117 | 2013 | Coyle SM | Exploitation of latent allostery enables evolution of MAP kinase regulation | No (S. cerevisiae) |
| 26370501 | 2015 | Yan L | Structures of yeast dynamin-like GTPase Sey1p... | No (S. cerevisiae) |
| 28183985 | ? | ? | (Could not fetch) | Unknown |
| 29062088 | ? | ? | (Could not fetch) | Unknown |
| 30617184 | 2019 | Singh SP | Mitochondrial single-stranded DNA binding proteins... | Maybe |
| **31185286** | 2019 | Han Q | **PP2A regulatory subunits in C. albicans growth, morphogenesis, virulence** | **Yes** |
| **38839956** | 2024 | Li K | **Profiling phagosome proteins identifies PD-L1 as fungal-binding receptor** | **Yes** |

Add these references to CGD via the standard curation process before running the loader to capture all interactions.

### Step 4: Run the Data Loader

```bash
cd ~/work/cgd-backend
source venv/bin/activate

# Dry run first (validate without writing)
BIOGRID_ACCESS_KEY=896de1f1a0b7fbe68517145c8bbff8e7 \
python scripts/loading/biogrid/load_biogrid_interactions.py --dry-run

# Actual load
BIOGRID_ACCESS_KEY=896de1f1a0b7fbe68517145c8bbff8e7 \
python scripts/loading/biogrid/load_biogrid_interactions.py
```

### Step 5: Verify Data Loaded

```sql
-- Check interaction counts
SELECT COUNT(*) FROM MULTI.interaction WHERE source = 'BioGRID';
SELECT COUNT(*) FROM MULTI.feat_interact;

-- Check by experiment type
SELECT experiment_type, COUNT(*)
FROM MULTI.interaction
WHERE source = 'BioGRID'
GROUP BY experiment_type
ORDER BY COUNT(*) DESC;
```

---

## Database Schema

### Tables Used

| Table | Purpose |
|-------|---------|
| `INTERACTION` | Stores interaction metadata (experiment_type, source, description) |
| `FEAT_INTERACT` | Links features (genes) to interactions with action (Bait/Hit) |
| `REF_LINK` | Links interactions to references (PMIDs) |
| `CV_TERM` | Controlled vocabulary for experiment types (cv_no=7) |

### Database Triggers

The Oracle database has triggers that validate:
- `experiment_type` must exist in `cv_term` table (cv_no=7)
- `created_by` must exist in `dbuser` table

Valid user IDs: ADIL, ARNAUDM, BINKLEY, CGDADMIN, DIANE, JODI, MAREK, MARIA, MULTI, PRACHI, SHERLOCK, SHUAI, STUART, WYMORE

---

## BioGRID API Reference

**Base URL:** `https://webservice.thebiogrid.org/interactions/`

**C. albicans taxon ID:** `237561`

**Example API calls:**
```bash
# Get count
curl "https://webservice.thebiogrid.org/interactions/?taxId=237561&format=count&accesskey=YOUR_KEY"

# Get all interactions (tab2 format)
curl "https://webservice.thebiogrid.org/interactions/?taxId=237561&format=tab2&max=10000&accesskey=YOUR_KEY"
```

**Tab2 format columns:**
| Index | Column |
|-------|--------|
| 0 | BioGRID Interaction ID |
| 5 | Systematic Name A |
| 6 | Systematic Name B |
| 11 | Experimental System |
| 12 | Type (physical/genetic) |
| 14 | PMID |
| 17 | Throughput |

---

## Experiment Types

### Physical Interactions (14 types in C. albicans data)
- Affinity Capture-MS
- Affinity Capture-Western
- Biochemical Activity
- Co-crystal Structure
- Co-localization
- Co-purification
- Far Western
- FRET
- PCA
- Protein-peptide
- Reconstituted Complex
- Two-hybrid

### Genetic Interactions (8 types in C. albicans data)
- Dosage Growth Defect
- Dosage Rescue
- Negative Genetic
- Phenotypic Enhancement
- Phenotypic Suppression
- Positive Genetic
- Synthetic Growth Defect
- Synthetic Haploinsufficiency
- Synthetic Lethality
- Synthetic Rescue

---

## Troubleshooting

### Error: CV term not found
```
ORA-20011: CV term "Synthetic Growth Defect" not found in cv_term table.
```
**Solution:** Run Step 2 to add BioGRID experiment types to cv_term table.

### Error: Userid not found
```
ORA-20024: Userid "SCRIPT" not found in dbuser table.
```
**Solution:** Update the `CREATED_BY` variable in the script to use a valid user ID (e.g., 'SHUAI').

### Error: Reference not found
Some interactions are skipped if the PMID is not in CGD's reference table. Run Step 3 to add missing references.

---

## References

- BioGRID: https://thebiogrid.org/
- BioGRID REST API: https://wiki.thebiogrid.org/doku.php/biogridrest
- Oughtred R, et al. (2021) The BioGRID database. Nucleic Acids Research.
