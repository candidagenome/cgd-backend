#!/usr/bin/env python3
"""Export a sanitized, query-relevant subset of the CGD Oracle database as a
PostgreSQL-loadable bundle (DDL + data + constraints) plus a schema/data
dictionary, for handing to an external collaborator (e.g. a NL-query chatbot
prototype).

See docs/DB_EXPORT_FOR_EXTERNAL_DB.md for the surrounding procedure.

The Oracle DB (AWS RDS) is only reachable from inside the VPC, so this runs on
cgd-backend-dev. It reads Oracle credentials from the backend .env and needs
only the `oracledb` driver (already in the backend venv).

Output (written to --outdir, gzipped to keep disk/transfer small):
    01_schema.sql.gz        CREATE TABLE (types mapped Oracle -> Postgres)
    02_data.sql.gz          COPY blocks (text format), one per table
    03_constraints.sql.gz   PK / UNIQUE / FK + FK-column indexes (applied last)
    CGD_SCHEMA_DICTIONARY.md tables, columns, relationships, example queries
    load.sh                 one-shot loader (zcat ... | psql)
    MANIFEST.txt            per-table row counts + byte sizes

Usage (on cgd-backend-dev):
    ./venv/bin/python scripts/export_for_external_db.py --outdir ~/cgd_pg_export
    ./venv/bin/python scripts/export_for_external_db.py --list   # dry-run: show table plan
"""
import argparse
import datetime as dt
import gzip
import os
import re
import sys

import oracledb

# --- Tables to EXCLUDE (everything else in the schema is included) ----------
# 1) Sanitization: personal data (researcher names/emails/addresses) + auth.
SANITIZE = {
    "COLLEAGUE", "COLLEAGUE_REMARK", "COLL_URL", "COLL_RELATIONSHIP",
    "COLL_KW", "COLL_FEAT", "COLL_GENERES", "COL_RULE",
    "CURATOR_SESSION", "DBUSER", "GENE_RESERVATION",
}
# 2) Bulk audit / archive logs — not needed for querying, ~3.7 GB.
BULK = {"UPDATE_LOG", "DELETE_LOG", "SEQ_CHANGE_ARCHIVE"}
# 3) PDB structural-alignment feature — self-contained, ~2.4 GB, niche.
PDB = {"PDB_ALIGNMENT", "PDB_ALIGNMENT_SEQUENCE", "PDB_SEQUENCE"}
# 4) Temp / working / rejected-rows scratch tables.
TEMP = {"TMP14015", "TMP23704", "TMP10054", "TMP6095",
        "REF_TEMP", "REF_BAD", "REF_UNLINK"}

EXCLUDE = SANITIZE | BULK | PDB | TEMP

# Columns dropped from every table (curator login handles = mild personal data).
# date_created timestamps are retained.
EXCLUDE_COLUMNS = {"CREATED_BY"}

FETCH_BATCH = 5000


def load_env(env_path):
    env = {}
    with open(env_path) as fh:
        for line in fh:
            m = re.match(r"([A-Z_]+)=(.*)", line.strip())
            if m:
                env[m.group(1)] = m.group(2)
    return env


def pg_type(data_type, prec, scale, length):
    """Map an Oracle column type to a PostgreSQL column type."""
    if data_type == "VARCHAR2":
        return "varchar(%d)" % length if length else "text"
    if data_type == "CLOB":
        return "text"
    if data_type == "DATE":
        return "timestamp"
    if data_type == "NUMBER":
        if scale is not None and scale > 0:
            return "numeric(%d,%d)" % (prec, scale) if prec else "numeric"
        # integer-valued NUMBER (scale 0 or undefined)
        if prec is None:
            return "numeric"          # unconstrained NUMBER -> arbitrary precision
        if prec <= 9:
            return "integer"
        if prec <= 18:
            return "bigint"
        return "numeric(%d)" % prec
    # schema only contains the four types above; fall back to text
    return "text"


def q(ident):
    """Quote a (lowercased) identifier so reserved words like start/end/type
    are safe. Unquoted lowercase queries still match a quoted lowercase name."""
    return '"%s"' % ident.lower()


def esc_copy(val):
    """Escape a value for a PostgreSQL COPY (text format) field."""
    if val is None:
        return r"\N"
    if isinstance(val, dt.datetime):
        s = val.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(val, dt.date):
        s = val.strftime("%Y-%m-%d")
    else:
        s = str(val)
    return (s.replace("\\", "\\\\").replace("\t", "\\t")
             .replace("\n", "\\n").replace("\r", "\\r"))


def get_included_tables(cur):
    cur.execute("SELECT table_name FROM user_tables ORDER BY table_name")
    all_tables = [r[0] for r in cur.fetchall()]
    return [t for t in all_tables if t not in EXCLUDE], all_tables


def get_columns(cur, table):
    cur.execute("""
        SELECT column_name, data_type, data_precision, data_scale,
               char_length, nullable, column_id
        FROM user_tab_columns WHERE table_name = :t ORDER BY column_id
    """, t=table)
    cols = []
    for name, dtype, prec, scale, clen, nullable, _cid in cur.fetchall():
        if name in EXCLUDE_COLUMNS:
            continue
        cols.append({
            "name": name, "dtype": dtype, "prec": prec, "scale": scale,
            "len": clen, "nullable": nullable,
            "pg": pg_type(dtype, prec, scale, clen),
        })
    return cols


def get_constraints(cur, included):
    """Return (pks, uniques, fks). Only FKs where both ends are included."""
    inc = set(included)
    # PK / UNIQUE columns
    cur.execute("""
        SELECT c.table_name, c.constraint_type, c.constraint_name,
               cc.column_name, cc.position
        FROM user_constraints c
        JOIN user_cons_columns cc ON cc.constraint_name = c.constraint_name
        WHERE c.constraint_type IN ('P', 'U')
        ORDER BY c.table_name, c.constraint_name, cc.position
    """)
    pk, uq = {}, {}
    for tbl, ctype, cname, col, _pos in cur.fetchall():
        if tbl not in inc or col in EXCLUDE_COLUMNS:
            continue
        target = pk if ctype == "P" else uq
        target.setdefault((tbl, cname), []).append(col)
    # FKs
    cur.execute("""
        SELECT c.table_name, c.constraint_name, cc.column_name, cc.position,
               pk.table_name AS ref_table
        FROM user_constraints c
        JOIN user_cons_columns cc ON cc.constraint_name = c.constraint_name
        JOIN user_constraints pk ON pk.constraint_name = c.r_constraint_name
        WHERE c.constraint_type = 'R'
        ORDER BY c.table_name, c.constraint_name, cc.position
    """)
    fk = {}
    for tbl, cname, col, _pos, ref in cur.fetchall():
        if tbl not in inc or ref not in inc or col in EXCLUDE_COLUMNS:
            continue
        fk.setdefault((tbl, cname, ref), []).append(col)
    return pk, uq, fk


def gzo(path):
    return gzip.open(path, "wt", encoding="utf-8", newline="\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default=os.path.expanduser("~/cgd_pg_export"))
    ap.add_argument("--env", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
    ap.add_argument("--list", action="store_true",
                    help="dry-run: print the include/exclude plan and exit")
    args = ap.parse_args()

    env = load_env(args.env)
    con = oracledb.connect(user=env["ORACLE_USER"],
                           password=env["ORACLE_PASSWORD"],
                           dsn=env["ORACLE_DSN"])
    con.fetch_lobs = False  # return CLOB as str, not LOB locator
    cur = con.cursor()

    included, all_tables = get_included_tables(cur)
    excluded = [t for t in all_tables if t in EXCLUDE]

    if args.list:
        print("INCLUDED (%d):" % len(included))
        for t in included:
            print("  ", t)
        print("\nEXCLUDED (%d):" % len(excluded))
        for t in excluded:
            tag = ("sanitize" if t in SANITIZE else "bulk" if t in BULK
                   else "pdb" if t in PDB else "temp")
            print("   %-28s [%s]" % (t, tag))
        return

    os.makedirs(args.outdir, exist_ok=True)
    schema_f = gzo(os.path.join(args.outdir, "01_schema.sql.gz"))
    data_f = gzo(os.path.join(args.outdir, "02_data.sql.gz"))
    cons_f = gzo(os.path.join(args.outdir, "03_constraints.sql.gz"))
    manifest = open(os.path.join(args.outdir, "MANIFEST.txt"), "w")

    stamp = env.get("ORACLE_DSN", "").split("/")[-1]
    header = ("-- CGD PostgreSQL export (subset) -- source Oracle service %s\n"
              "-- %d tables. Load with load.sh\n\n" % (stamp, len(included)))
    for f in (schema_f, data_f, cons_f):
        f.write(header)
    schema_f.write("SET client_encoding = 'UTF8';\n\n")

    cols_by_table = {}
    rowcounts = {}
    for tbl in included:
        cols = get_columns(cur, tbl)
        cols_by_table[tbl] = cols
        # DDL
        lines = []
        for c in cols:
            nn = "" if c["nullable"] == "Y" else " NOT NULL"
            lines.append("    %s %s%s" % (q(c["name"]), c["pg"], nn))
        schema_f.write("CREATE TABLE %s (\n%s\n);\n\n"
                       % (q(tbl), ",\n".join(lines)))

        # DATA (COPY text format)
        collist = ", ".join(q(c["name"]) for c in cols)
        data_f.write("COPY %s (%s) FROM stdin;\n" % (q(tbl), collist))
        cur.execute("SELECT %s FROM %s"
                    % (", ".join('"%s"' % c["name"] for c in cols), tbl))
        n = 0
        while True:
            batch = cur.fetchmany(FETCH_BATCH)
            if not batch:
                break
            for row in batch:
                data_f.write("\t".join(esc_copy(v) for v in row))
                data_f.write("\n")
            n += len(batch)
        data_f.write("\\.\n\n")
        rowcounts[tbl] = n
        manifest.write("%-30s %10d rows\n" % (tbl, n))
        print("  exported %-30s %10d rows" % (tbl, n))
        sys.stdout.flush()

    # CONSTRAINTS (applied after data load)
    pk, uq, fk = get_constraints(cur, included)
    cons_f.write("-- Primary keys\n")
    for (tbl, cname), cs in pk.items():
        cons_f.write("ALTER TABLE %s ADD PRIMARY KEY (%s);\n"
                     % (q(tbl), ", ".join(q(c) for c in cs)))
    cons_f.write("\n-- Unique constraints\n")
    for (tbl, cname), cs in uq.items():
        cons_f.write("ALTER TABLE %s ADD UNIQUE (%s);\n"
                     % (q(tbl), ", ".join(q(c) for c in cs)))
    cons_f.write("\n-- Foreign keys\n")
    for (tbl, cname, ref), cs in fk.items():
        # reference the parent PK columns (positional match)
        parent_pk = None
        for (ptbl, _pcn), pcols in pk.items():
            if ptbl == ref:
                parent_pk = pcols
                break
        if not parent_pk or len(parent_pk) != len(cs):
            continue
        cons_f.write("ALTER TABLE %s ADD FOREIGN KEY (%s) REFERENCES %s (%s);\n"
                     % (q(tbl), ", ".join(q(c) for c in cs),
                        q(ref), ", ".join(q(c) for c in parent_pk)))
    cons_f.write("\n-- Indexes on FK columns (help chatbot joins)\n")
    seen_idx = set()
    for (tbl, cname, ref), cs in fk.items():
        key = (tbl, tuple(cs))
        if key in seen_idx:
            continue
        seen_idx.add(key)
        cons_f.write("CREATE INDEX ON %s (%s);\n"
                     % (q(tbl), ", ".join(q(c) for c in cs)))

    for f in (schema_f, data_f, cons_f, manifest):
        f.close()

    write_loader(args.outdir)
    write_dictionary(args.outdir, cur, included, cols_by_table, rowcounts,
                     pk, uq, fk)

    total = sum(rowcounts.values())
    print("\nDone. %d tables, %d rows total -> %s"
          % (len(included), total, args.outdir))
    con.close()


def write_loader(outdir):
    sh = """#!/usr/bin/env bash
# Load the CGD subset into a fresh PostgreSQL database.
# Usage: ./load.sh [dbname]   (default: cgd)
set -euo pipefail
DB="${1:-cgd}"
createdb "$DB" 2>/dev/null || true
echo "Loading schema...";       gunzip -c 01_schema.sql.gz      | psql -q "$DB"
echo "Loading data...";         gunzip -c 02_data.sql.gz        | psql -q "$DB"
echo "Applying constraints..."; gunzip -c 03_constraints.sql.gz | psql -q "$DB"
echo "Done. Try:  psql $DB -c 'SELECT COUNT(*) FROM feature;'"
"""
    p = os.path.join(outdir, "load.sh")
    with open(p, "w") as fh:
        fh.write(sh)
    os.chmod(p, 0o755)


EXAMPLE_QUERIES = r"""
### Example queries

All queries below were run against the exported subset and return rows.

```sql
-- Organisms / strains in the database (Candida species + reference strains)
SELECT organism_no, organism_name, organism_abbrev, taxon_id, taxonomic_rank
FROM organism ORDER BY organism_order;

-- Gene (feature) counts per organism strain
SELECT o.organism_abbrev, COUNT(*) AS features
FROM feature f JOIN organism o USING (organism_no)
GROUP BY o.organism_abbrev ORDER BY features DESC;

-- Current genomic location(s) of a gene by its standard name
SELECT f.gene_name, f.feature_name, l.start_coord, l.stop_coord, l.strand
FROM feature f
JOIN feat_location l USING (feature_no)
WHERE f.gene_name = 'ERG11' AND l.is_loc_current = 'Y';

-- GO annotations for a gene (feature -> go_annotation -> go)
SELECT f.gene_name, f.feature_name, g.goid, g.go_term, g.go_aspect
FROM feature f
JOIN go_annotation ga USING (feature_no)
JOIN go g            USING (go_no)
WHERE f.gene_name = 'ERG11';

-- All aliases / synonyms of a gene (feature -> feat_alias -> alias)
SELECT f.gene_name, a.alias_name, a.alias_type
FROM feature f
JOIN feat_alias fa USING (feature_no)
JOIN alias a       USING (alias_no)
WHERE f.gene_name = 'ERG11';

-- Phenotype annotations (pheno_annotation -> phenotype)
SELECT f.gene_name, p.observable, p.qualifier, p.experiment_type
FROM pheno_annotation pa
JOIN feature f   USING (feature_no)
JOIN phenotype p USING (phenotype_no)
LIMIT 50;

-- Genetic/physical interactions between genes (interaction self-joined via feat_interact)
SELECT a.feature_no AS gene_a, b.feature_no AS gene_b, i.experiment_type
FROM interaction i
JOIN feat_interact a ON a.interaction_no = i.interaction_no
JOIN feat_interact b ON b.interaction_no = i.interaction_no
                    AND b.feature_no <> a.feature_no
LIMIT 50;
```

> Note: identifiers are lowercase; reserved-word columns must be double-quoted,
> e.g. `SELECT "start", "end" FROM ...`. GO ids are stored numeric in `go.goid`
> (prepend `GO:` and zero-pad for display). Coordinates: on the Crick/`C`
> strand `start_coord > stop_coord`.
"""


def write_dictionary(outdir, cur, included, cols_by_table, rowcounts,
                     pk, uq, fk):
    # table comments (usually sparse in this schema)
    cur.execute("SELECT table_name, comments FROM user_tab_comments")
    tcomments = {r[0]: r[1] for r in cur.fetchall() if r[1]}
    cur.execute("SELECT table_name, column_name, comments "
                "FROM user_col_comments WHERE comments IS NOT NULL")
    ccomments = {(r[0], r[1]): r[2] for r in cur.fetchall()}

    pk_cols = {}
    for (tbl, _cn), cs in pk.items():
        pk_cols.setdefault(tbl, set()).update(cs)
    fk_map = {}
    for (tbl, _cn, ref), cs in fk.items():
        for c in cs:
            fk_map[(tbl, c)] = ref

    lines = ["# CGD Schema & Data Dictionary (PostgreSQL subset)", ""]
    lines.append("Subset of the CGD (Candida Genome Database) Oracle schema, "
                 "converted to PostgreSQL for external prototyping. "
                 "Personal/curator and bulk audit tables were excluded.")
    lines.append("")
    lines.append("## Table inventory")
    lines.append("")
    lines.append("| Table | Rows | Purpose |")
    lines.append("|---|---:|---|")
    for tbl in included:
        purpose = tcomments.get(tbl, "").replace("\n", " ").strip()
        lines.append("| `%s` | %d | %s |"
                     % (tbl.lower(), rowcounts.get(tbl, 0), purpose))
    lines.append("")
    lines.append("## Relationships (foreign keys)")
    lines.append("")
    lines.append("| Child table | Column(s) | Parent table |")
    lines.append("|---|---|---|")
    for (tbl, _cn, ref), cs in sorted(fk.items()):
        lines.append("| `%s` | %s | `%s` |"
                     % (tbl.lower(), ", ".join(c.lower() for c in cs),
                        ref.lower()))
    lines.append("")
    lines.append("## Columns by table")
    lines.append("")
    for tbl in included:
        lines.append("### `%s`  (%d rows)" % (tbl.lower(), rowcounts.get(tbl, 0)))
        if tcomments.get(tbl):
            lines.append("")
            lines.append(tcomments[tbl].strip())
        lines.append("")
        lines.append("| Column | Type | Null | Key | Comment |")
        lines.append("|---|---|---|---|---|")
        for c in cols_by_table[tbl]:
            key = []
            if c["name"] in pk_cols.get(tbl, ()):
                key.append("PK")
            if (tbl, c["name"]) in fk_map:
                key.append("FK->%s" % fk_map[(tbl, c["name"])].lower())
            cm = ccomments.get((tbl, c["name"]), "").replace("\n", " ").strip()
            lines.append("| `%s` | %s | %s | %s | %s |"
                         % (c["name"].lower(), c["pg"],
                            "" if c["nullable"] == "Y" else "NOT NULL",
                            ", ".join(key), cm))
        lines.append("")
    lines.append(EXAMPLE_QUERIES)

    with open(os.path.join(outdir, "CGD_SCHEMA_DICTIONARY.md"), "w") as fh:
        fh.write("\n".join(lines))


if __name__ == "__main__":
    main()
