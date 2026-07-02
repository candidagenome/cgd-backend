# Generating UniProt IDs for C. tropicalis

How C. tropicalis MYA-3404 gets UniProt (Swiss-Prot / TrEMBL) cross-references
in CGD, so it appears alongside the other five species in the
[External_id_mappings](https://www.candidagenome.org/download/External_id_mappings/)
`gp2protein` download files.

Script: [`load_uniprot_dbxrefs.py`](load_uniprot_dbxrefs.py)

## Background — why this was needed

The other five CGD species (C. albicans, C. auris, C. dubliniensis, C. glabrata,
C. parapsilosis) were loaded with UniProt cross-references stored in the
`dbxref` table. C. tropicalis was imported with **only its CGDID and no external
cross-references at all** — its features had zero `dbxref` rows other than the
`CGDID Primary` entry. As a result:

- C. tropicalis had no UniProt IDs anywhere in CGD, and
- it was **missing from the `gp2protein` download files** (only 4–5 species were present).

So the fix is a one-time backfill: fetch the UniProt IDs and load them into the
`dbxref` tables using the **same convention** the other species already use.

## Where the UniProt IDs come from

The C. tropicalis MYA-3404 UniProt proteome (**NCBI taxon `294747`**) records
every protein under its **ORF name = the `CTRG_#####` locus tag**, which is
**identical to the CGD `feature_name`**. That gives a clean 1:1 join with no
BLAST or sequence matching required.

UniProt REST API (streamed as TSV):

```
https://rest.uniprot.org/uniprotkb/stream
  ?query=organism_id:294747
  &format=tsv
  &fields=accession,id,gene_orf,gene_names,reviewed
```

| UniProt field | Used for |
|---|---|
| `accession`  | the UniProt ID stored as `dbxref.dbxref_id` (e.g. `C5M591`) |
| `gene_orf` / `gene_names` | `CTRG_#####` locus tag → matched to CGD `feature_name` |
| `reviewed`   | `reviewed` → Swiss-Prot, otherwise → TrEMBL |

The proteome has **6,226** entries, all carrying a `CTRG_` ORF name.

## The mapping / load procedure

For each UniProt entry:

1. **Resolve the CGD feature** — uppercase the ORF/gene-name tokens and look them
   up in a `feature_name → feature_no` map built for organism `C_tropicalis`.
   (35 UniProt entries have no matching CGD feature — retired/merged locus tags —
   and are skipped; 27 entries map to more than one feature and link to each.)
2. **Create the `dbxref` row** (if it does not already exist), matching the
   existing UniProt convention exactly:
   - `source = 'EBI'`
   - `dbxref_type = 'SwissProt'` (reviewed) or `'TrEMBL'` (unreviewed)
   - `dbxref_id = <UniProt accession>`
   - `dbxref_no` from the Oracle sequence `MULTI.DBXREF_SEQ`
3. **Link it to the feature** — a `dbxref_feat` row (`MULTI.DBXREF_FEAT_SEQ`).
4. **Make it clickable** — a `dbxref_url` row to the standard UniProt URL
   (`MULTI.DBXREF_URL_SEQ`): Swiss-Prot → `url_no 44473`, TrEMBL → `url_no 44475`.

The insert idiom follows the codebase pattern
(`SELECT MULTI.<seq>.NEXTVAL FROM dual`, then an explicit `INSERT`).

The script is **idempotent**: existing `dbxref` / `dbxref_feat` / `dbxref_url`
rows are reused, never duplicated, so it is safe to re-run.

## Running it

The database is only reachable from the deployed servers (see the Oracle-access
note). Always do a `--dry-run` first.

**Dev** (`cgd-backend-dev`, checkout at `~/work/cgd-backend`):

```bash
ssh cgd-backend-dev
cd ~/work/cgd-backend && source venv/bin/activate
python scripts/C_tropicalis/load_uniprot_dbxrefs.py --dry-run   # preview
python scripts/C_tropicalis/load_uniprot_dbxrefs.py             # load
```

**Prod** — the running deployment is `/opt/cgd_api` (user `cgdapi`, its own
`.env` with `ENV_STATE=prod` and its own `.venv`), **not** `~/cgd-backend`:

```bash
ssh cgd-prod
sudo -u cgdapi bash -c "cd /opt/cgd_api && ./.venv/bin/python scripts/C_tropicalis/load_uniprot_dbxrefs.py --dry-run"
sudo -u cgdapi bash -c "cd /opt/cgd_api && ./.venv/bin/python scripts/C_tropicalis/load_uniprot_dbxrefs.py"
```

Options: `--dry-run` (no commit), `--created-by NAME` (audit user, default
`$ADMIN_USER` → `cgdadmin`), `-v` (debug logging).

## Results (loaded 2026-07-02, dev + prod)

| Metric | Count |
|---|---|
| UniProt records fetched | 6,226 |
| Swiss-Prot dbxrefs created | 69 |
| TrEMBL dbxrefs created | 6,122 |
| `dbxref_feat` links created | 6,218 |
| `dbxref_url` links created | 6,191 |
| UniProt entries with no CGD feature (skipped) | 35 |
| CGD C. tropicalis features now with a UniProt ID | **6,218** of 6,403 ORFs (~97%) |

## Verifying

```sql
SELECT COUNT(DISTINCT f.dbxref_id)
FROM   MULTI.dbxref d
JOIN   MULTI.dbxref_feat df ON d.dbxref_no = df.dbxref_no
JOIN   MULTI.feature f      ON df.feature_no = f.feature_no
JOIN   MULTI.organism o     ON f.organism_no = o.organism_no
WHERE  d.dbxref_type IN ('SwissProt','TrEMBL')
  AND  o.organism_abbrev = 'C_tropicalis'
  AND  f.feature_type IN ('ORF','allele','pseudogene');
-- expect 6218
```

Re-running the loader with `--dry-run` should report `dbxrefs reused: 6191` and
`created: 0` — confirming idempotency.

## Notes

- This is the same convention used to feed `gp2protein` generation: those files
  are built from `dbxref` rows with `source='EBI'` and
  `dbxref_type IN ('SwissProt','TrEMBL')` (both emitted with the `UniProtKB:`
  prefix). With this backfill, all six species can be generated uniformly.
- To refresh after a UniProt release, just re-run the script; only new
  accessions/links are added.
