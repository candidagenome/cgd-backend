# Exporting CGD Data for an External / Different Database

How to hand a collaborator a copy of the CGD database so they can run it on
**their own** database engine — e.g. to prototype a natural-language-query
chatbot. Written for the case where the recipient is **not** running Oracle and
may be **off-site / international**.

> TL;DR: Don't ship the raw Oracle dump. **Sanitize → convert to the target
> engine (Postgres or SQLite) → publish the dump + a schema/data-dictionary
> doc** to a spot the collaborator can download from.

---

## 1. What the CGD database actually is

- **Engine:** Oracle (legacy GMOD/SGD-derived genomics schema).
- **Schema owner:** `MULTI` (SQL refers to `MULTI.FEATURE`, `MULTI.GO_ANNOTATION`, …).
- **Size / shape:** ~96 tables, ~12 GB total (mostly sequence / large-object
  data; row counts are modest — e.g. FEATURE ~168k, GO_ANNOTATION ~153k,
  REFERENCE ~56k, PHENOTYPE ~1.4k).
- **Not part of the relational DB (and NOT needed for SQL/NL querying):**
  - an Elasticsearch search index (`cgd_search`) — derived from Oracle, rebuildable;
  - on-disk flat files under `/data` (sequences, BLAST DBs, coverage tracks).

A NL→SQL chatbot only needs the **relational tables + a good schema description**.

### Connection details (from the backend `.env`)
```
ORACLE_USER=<user>
ORACLE_PASSWORD=<password>
ORACLE_DSN=<host>:<port>/?service_name=<service>   # also DATABASE_URL=oracle+oracledb://...
DB_SCHEMA=MULTI
```
Driver: `oracledb` (already in the backend venv / `requirements.txt`).

---

## 2. Pick a target format

| Target | When to use | Tooling |
|---|---|---|
| **SQLite** | Lightweight prototype / demo; want a single portable file | CSV-per-table → load, or `sqlite3 .import` |
| **Postgres** | Realistic relational target, free, easy to run | `ora2pg` (Oracle→Postgres schema + data) |
| **DBMS-agnostic bundle** | Let the recipient choose | DDL + one CSV per table + data dictionary |
| **Full Oracle clone** | Recipient genuinely runs Oracle and wants 1:1 | `expdp` (see §5) |

**Recommendation for a chatbot prototype:** Postgres or SQLite, **trimmed to the
query-relevant tables**. Both are free, easy to stand up, far smaller than the
12 GB Oracle footprint, and don't constrain the chatbot (it just targets the
recipient's SQL dialect).

> ⚠️ **Oracle XE (free) caps user data at 12 GB** and the live schema is already
> ~12.25 GB — so a full Oracle clone does **not** fit in free XE without
> trimming. Another reason to prefer Postgres/SQLite.

---

## 3. Sanitize BEFORE exporting (required)

Strip anything sensitive — especially important for off-site / international
transfer. The genomics data itself is public; the following are not:

- **Drop / exclude:**
  - `COLLEAGUE` and any curator/person tables — contain researcher **names,
    emails, addresses** (personal data).
  - Any auth / password / session / API-key tables (e.g. curator session table).
  - DB credentials, connection strings, internal hostnames.
  - Any embargoed / unpublished curation, if present.
- **Keep (public):** feature/gene, sequence, GO annotation, phenotype,
  reference, taxonomy, ontology, homology, interaction, etc.

Maintain the exclude list in one place and apply it to every export path below.
Set it once:
```bash
EXCLUDE_TABLES="COLLEAGUE COLLEAGUE_* CURATOR_SESSION *_AUTH *PASSWORD* API_KEY*"
```

---

## 4. Generate the schema + data dictionary (do this first — highest value)

For NL→SQL, the model understanding the (cryptically named) legacy schema
matters more than the data volume. Produce a human/LLM-readable description:

- **Table inventory:** name, row count, one-line purpose.
- **Per-table columns:** name, type, nullability, PK/FK, comment.
- **Relationships:** foreign keys (table → table).
- **A few example queries** in the recipient's target dialect.

Pull structure straight from Oracle's catalog:
```sql
-- tables + comments
SELECT t.table_name, c.comments
FROM all_tables t
LEFT JOIN all_tab_comments c
  ON c.owner=t.owner AND c.table_name=t.table_name
WHERE t.owner='MULTI' ORDER BY t.table_name;

-- columns
SELECT table_name, column_name, data_type, data_length, nullable
FROM all_tab_columns WHERE owner='MULTI' ORDER BY table_name, column_id;

-- foreign keys (relationships)
SELECT a.table_name AS child, a.column_name, c_pk.table_name AS parent
FROM all_cons_columns a
JOIN all_constraints c   ON a.owner=c.owner AND a.constraint_name=c.constraint_name
JOIN all_constraints c_pk ON c.r_owner=c_pk.owner AND c.r_constraint_name=c_pk.constraint_name
WHERE c.constraint_type='R' AND c.owner='MULTI';
```
Ship the result as `CGD_SCHEMA_DICTIONARY.md` alongside the data dump.

---

## 5. Export procedures

### Option A — Full Oracle dump (`expdp`)  *(only if recipient runs Oracle)*
```bash
# On a host with Oracle client + a configured DATA_PUMP_DIR directory object
expdp <user>/<password>@<dsn> \
  schemas=MULTI \
  exclude=TABLE:"IN ('COLLEAGUE','CURATOR_SESSION', ...)" \
  compression=ALL \
  directory=DATA_PUMP_DIR dumpfile=cgd_multi_%U.dmp logfile=cgd_multi_exp.log
# Recipient imports with impdp into their own Oracle (XE won't fit untrimmed).
```

### Option B — Convert to Postgres (`ora2pg`)  *(recommended realistic target)*
```bash
# ora2pg reads Oracle, emits Postgres DDL + data.
# Minimal ora2pg.conf:
#   ORACLE_DSN   dbi:Oracle:host=<host>;service_name=<service>;port=<port>
#   ORACLE_USER  <user>
#   ORACLE_PWD   <password>
#   SCHEMA       MULTI
#   TYPE         TABLE,COPY            # structure, then data
#   EXCLUDE      COLLEAGUE CURATOR_SESSION ...   # sanitization
ora2pg -c ora2pg.conf -t TABLE -o schema.sql      # DDL
ora2pg -c ora2pg.conf -t COPY  -o data.sql        # data
# Recipient: createdb cgd && psql cgd -f schema.sql -f data.sql
```

### Option C — SQLite / CSV-per-table  *(most portable; great for a demo)*
Dump each (non-excluded) table to CSV, then load into SQLite:
```bash
# Example pattern (python + oracledb): for each table not in EXCLUDE_TABLES,
#   SELECT * -> write CSV  -> sqlite3 cgd.db ".import --csv <table>.csv <table>"
# Result: a single cgd.db file the recipient drops next to their app.
```
A small driver script can live in `scripts/` (e.g. `scripts/export_for_external_db.py`).

> Oracle-isms (sequences, `NUMBER`/`DATE` types, `CLOB`s) are translated by
> ora2pg automatically; for the CSV/SQLite route they flatten to TEXT/REAL/BLOB.

---

## 6. Where to put it (download / transfer)

The data is public, so hosting is mostly about convenience + access control.
For an **international** recipient (e.g. Korea):

| Option | Good for | Notes |
|---|---|---|
| **Zenodo / Figshare** | Public data | Free, ≤50 GB, permanent DOI + URL; anyone can download. Cleanest for public scientific data. |
| **Stanford Google Drive / Box** | Simple one-off | Share to the recipient's email/link; multi-GB OK. Follow Stanford external-sharing guidance. |
| **AWS S3 presigned URL** | If using existing AWS | Private bucket + time-limited link; fast globally; no account needed for recipient. Requires AWS creds/bucket setup (not configured on the app servers by default). |
| **Globus** | Very large / robust transfers | Academic standard; Korean endpoints (KISTI, universities) exist; resumable. Overkill unless the dump stays large. |

Going Postgres/SQLite + trimmed tables usually shrinks the artifact enough that
Drive or Zenodo is trivial.

---

## 7. Checklist

- [ ] Decide target engine (Postgres / SQLite / Oracle) and table scope (full vs trimmed).
- [ ] Apply the **sanitization exclude list** (§3).
- [ ] Generate `CGD_SCHEMA_DICTIONARY.md` (§4).
- [ ] Export data in the chosen format (§5).
- [ ] Quick load test on a clean target DB; spot-check row counts vs source.
- [ ] Publish dump + dictionary to the chosen location (§6); share access.
- [ ] Confirm no credentials / personal data are present in the shared artifacts.
