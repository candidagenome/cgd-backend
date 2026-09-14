#!/usr/bin/env python3
"""
Apply curator-approved SGD ortholog-link fixes (FIX_LINK decisions).

Input is the classified fix-link plan (fix_link_plan_classified.tsv from the
2026-09 transfers-worklist round). Only two classes touch data:

  REPOINT           the target's own SGD 'Gene ID' link points at a paralog
                    of the family consensus — repoint it to the consensus
                    SGD gene (get-or-create the dbxref row; extra divergent
                    links are removed rather than duplicated)
  EQUIV_PLUS_STRAY  the consensus link already exists alongside stray
                    paralog links — delete the strays

ALIAS_EQUIVALENT rows need no data change (the QC's SGD-alias-aware name
comparison recognizes them); CONSENSUS_NOT_IN_SGD rows are handled by the
curator-approved transfer pairs file; AMBIGUOUS rows are skipped.

Dry-run by default.

Usage:
    python apply_sgd_link_fixes.py --plan fix_link_plan_classified.tsv \
        --sgd-features SGD_features.tab [--apply]
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal  # noqa: E402

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
ADMIN_USER = os.getenv("ADMIN_USER", "cgdadmin").upper()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def sgd_standard_names(path):
    std = {}
    for line in open(path):
        f = line.rstrip("\n").split("\t")
        if len(f) >= 5 and f[0].startswith("S000") and f[1] not in (
                "CDS", "intron", "ARS", "telomere", "long_terminal_repeat"):
            std.setdefault(f[0], f[4] or f[3])
    return std


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--plan", required=True)
    ap.add_argument("--sgd-features", required=True)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    dry = not args.apply

    std_names = sgd_standard_names(args.sgd_features)
    rows = [r for r in csv.DictReader(open(args.plan), delimiter="\t")
            if r["class"] in ("REPOINT", "EQUIV_PLUS_STRAY")]
    logger.info("plan: %d link fixes%s", len(rows), " [DRY-RUN]" if dry else "")

    repointed = deleted = skipped = 0
    with SessionLocal() as session:
        def q(sql, **kw):
            return session.execute(text(sql), kw).fetchall()

        def x(sql, **kw):
            return session.execute(text(sql), kw)

        def sgd_dbxref_no(sgdid):
            hit = q(f"SELECT dbxref_no FROM {DB_SCHEMA}.dbxref WHERE source=:s"
                    f" AND dbxref_type=:t AND dbxref_id=:i",
                    s="SGD", t="Gene ID", i=sgdid)
            if hit:
                return hit[0][0]
            if dry:
                return None  # would be created
            x(f"INSERT INTO {DB_SCHEMA}.dbxref (source, dbxref_type,"
              f" dbxref_id, description, created_by)"
              f" VALUES (:s, :t, :i, :d, :u)",
              s="SGD", t="Gene ID", i=sgdid,
              d=std_names.get(sgdid), u=ADMIN_USER)
            return q(f"SELECT dbxref_no FROM {DB_SCHEMA}.dbxref WHERE"
                     f" source=:s AND dbxref_type=:t AND dbxref_id=:i",
                     s="SGD", t="Gene ID", i=sgdid)[0][0]

        for r in rows:
            fname = r["feature_name"]
            feats = q(f"SELECT feature_no FROM {DB_SCHEMA}.feature"
                      f" WHERE feature_name=:n", n=fname)
            if len(feats) != 1:
                logger.warning("SKIP %s: %d features", fname, len(feats))
                skipped += 1
                continue
            fno = feats[0][0]
            own = {sgdid: dfno for sgdid, dfno in q(
                f"SELECT d.dbxref_id, df.dbxref_feat_no"
                f" FROM {DB_SCHEMA}.dbxref_feat df"
                f" JOIN {DB_SCHEMA}.dbxref d ON d.dbxref_no = df.dbxref_no"
                f" WHERE df.feature_no=:f AND d.source=:s AND d.dbxref_type=:t",
                f=fno, s="SGD", t="Gene ID")}
            documented = {tok.split("=", 1)[0]
                          for tok in (r["old_sgd_links"] or "").split(";")
                          if "=" in tok}
            if set(own) != documented:
                logger.warning("SKIP %s: links changed since planning"
                               " (db %s vs plan %s)", fname,
                               sorted(own), sorted(documented))
                skipped += 1
                continue

            consensus = r["consensus_sgdid"]
            if r["class"] == "REPOINT":
                divergent = [s for s in own if s != consensus]
                first = divergent[0]
                if dry:
                    logger.info("[DRY] %s: repoint %s -> %s (%s)%s", fname,
                                first, consensus, std_names.get(consensus),
                                f" + delete {divergent[1:]}"
                                if len(divergent) > 1 else "")
                else:
                    x(f"UPDATE {DB_SCHEMA}.dbxref_feat SET dbxref_no=:d"
                      f" WHERE dbxref_feat_no=:k",
                      d=sgd_dbxref_no(consensus), k=own[first])
                    for stray in divergent[1:]:
                        x(f"DELETE FROM {DB_SCHEMA}.dbxref_feat"
                          f" WHERE dbxref_feat_no=:k", k=own[stray])
                        deleted += 1
                repointed += 1
            else:  # EQUIV_PLUS_STRAY
                strays = [s for s in own if s != consensus]
                if dry:
                    logger.info("[DRY] %s: delete stray link(s) %s (keep %s)",
                                fname, strays, consensus)
                else:
                    for stray in strays:
                        x(f"DELETE FROM {DB_SCHEMA}.dbxref_feat"
                          f" WHERE dbxref_feat_no=:k", k=own[stray])
                        deleted += 1

        if not dry:
            session.commit()

    logger.info("done: %d repointed, %d stray links deleted, %d skipped%s",
                repointed, deleted, skipped,
                " [DRY-RUN — no changes]" if dry else "")
    if skipped:
        sys.exit(1)


if __name__ == "__main__":
    main()
