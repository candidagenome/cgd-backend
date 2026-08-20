#!/usr/bin/env python3
"""Build C_tropicalis_full_gene_descriptions.tab for the download site.

FEATURE.HEADLINE is VARCHAR2(240); the ortholog-derived C. tropicalis
descriptions longer than that were loaded truncated ("...237 chars..."), and
the chromosomal_feature.tab dump faithfully reproduces the truncation. The
untruncated text lives in the generator output
(ctrop_ortholog_descriptions_ctrg.tsv). This script emits one row per
feature, restoring the full description ONLY where the DB headline is a
truncated prefix of the generated text (so any later curator edits to
headlines always win), and reports counts.
"""
import csv
import os
import sys
from datetime import date
from dotenv import load_dotenv
load_dotenv(".env")
from sqlalchemy import text
from cgd.db.engine import SessionLocal

TSV = "/tmp/ctrop_ortholog_descriptions_ctrg.tsv"
OUT = "/data/downloads/chromosomal_feature_files/C_tropicalis/C_tropicalis_full_gene_descriptions.tab"

full = {}
with open(TSV) as f:
    for row in csv.DictReader(f, delimiter="\t"):
        full[row["ctrop_gene_id"]] = row["generated_description"]

db = SessionLocal()
S = os.getenv("DB_SCHEMA", "MULTI")
rows = db.execute(text(f"""
    SELECT feature_name, gene_name, dbxref_id, headline
    FROM {S}.feature
    WHERE organism_no=12 AND headline IS NOT NULL
    ORDER BY feature_name
""")).fetchall()
db.close()

n_restored = n_kept = n_mismatch = 0
out_rows = []
for name, gene, cgdid, headline in rows:
    desc = headline
    if headline.endswith("..."):
        cand = full.get(name)
        if cand and cand.startswith(headline[:-3]):
            desc = cand
            n_restored += 1
        else:
            n_mismatch += 1
    else:
        n_kept += 1
    out_rows.append((name, gene or "", cgdid, desc))

with open(OUT, "w") as f:
    f.write("! C. tropicalis MYA-3404 full-length gene descriptions\n")
    f.write(f"! Generated {date.today().isoformat()} from CGD (untruncated companion to\n")
    f.write("! C_tropicalis_version_s01-m02-r01_chromosomal_feature.tab, whose Description\n")
    f.write("! column is capped at 240 characters by the database schema)\n")
    f.write("!\n")
    f.write("! Systematic name\tGene name\tCGDID\tDescription\n")
    for r in out_rows:
        f.write("\t".join(r) + "\n")

print(f"wrote {OUT}: {len(out_rows)} rows "
      f"({n_restored} restored full text, {n_kept} already complete, {n_mismatch} truncated-no-match)")
if n_mismatch:
    sys.exit(f"WARNING: {n_mismatch} truncated headlines had no matching full text")
