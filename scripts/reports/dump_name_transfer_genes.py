#!/usr/bin/env python3
"""
Dump the list of genes whose standard name came from an ortholog name transfer.

The authoritative record is the set of REF_LINK (FEATURE / GENE_NAME) rows
citing the CGD name-transfer methods reference ("Conserved name transfer based
on orthology..." — see create_name_transfer_reference.py; looked up by title
because dev and prod mint different CAL ids). One row per gene:

    organism  feature_name  gene_name  cgdid  name_transferred_on

Written for the public download site (/download/ortholog_name_transfers/);
rerun after future transfer rounds to refresh the file.

Usage:
    python dump_name_transfer_genes.py --output ortholog_name_transfers.tsv
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal  # noqa: E402
from scripts.ortholog_loading.create_name_transfer_reference import REF_TITLE  # noqa: E402

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--output", required=True, help="output TSV path")
    args = ap.parse_args()

    with SessionLocal() as session:
        ref = session.execute(text(
            f"SELECT reference_no, dbxref_id FROM {DB_SCHEMA}.reference"
            f" WHERE title = :t"), {"t": REF_TITLE}).fetchone()
        if not ref:
            sys.exit("name-transfer reference not found in this database")
        ref_no, ref_dbxref = ref

        # the loader names features through the FEATURE triggers, so the
        # actual transfer date is the update_log NULL->name row (ref_link
        # date_created can be a later backfill day)
        rows = session.execute(text(f"""
            SELECT o.organism_name, f.feature_name, f.gene_name,
                   (SELECT MAX(d.dbxref_id)
                    FROM {DB_SCHEMA}.dbxref_feat df
                    JOIN {DB_SCHEMA}.dbxref d ON d.dbxref_no = df.dbxref_no
                    WHERE df.feature_no = f.feature_no
                      AND d.dbxref_type = 'CGDID Primary') AS cgdid,
                   COALESCE((SELECT MIN(ul.date_created)
                             FROM {DB_SCHEMA}.update_log ul
                             WHERE ul.tab_name = 'FEATURE'
                               AND ul.col_name = 'GENE_NAME'
                               AND ul.primary_key = f.feature_no
                               AND ul.old_value IS NULL
                               AND ul.new_value = f.gene_name),
                            rl.date_created) AS transferred_on
            FROM {DB_SCHEMA}.ref_link rl
            JOIN {DB_SCHEMA}.feature f ON f.feature_no = rl.primary_key
            JOIN {DB_SCHEMA}.organism o ON o.organism_no = f.organism_no
            WHERE rl.reference_no = :r
              AND rl.tab_name = 'FEATURE' AND rl.col_name = 'GENE_NAME'
            ORDER BY o.organism_name, f.gene_name, f.feature_name
        """), {"r": ref_no}).fetchall()

    with open(args.output, "w") as fh:
        fh.write(f"! Genes with standard names assigned by ortholog name transfer\n"
                 f"! Candida Genome Database (www.candidagenome.org)\n"
                 f"! Reference: CGD (2026) {REF_TITLE} [{ref_dbxref}]\n"
                 f"! Generated: {date.today().isoformat()}  ({len(rows)} genes)\n"
                 f"organism\tfeature_name\tgene_name\tcgdid\tname_transferred_on\n")
        for org, fname, gname, cgdid, dt in rows:
            fh.write(f"{org}\t{fname}\t{gname}\t{cgdid or ''}\t"
                     f"{dt.date().isoformat() if dt else ''}\n")
    print(f"wrote {len(rows)} genes to {args.output} (reference {ref_dbxref})")


if __name__ == "__main__":
    main()
