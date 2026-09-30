#!/usr/bin/env python3
"""
Backfill REF_LINK rows for ortholog name transfers applied before the loader
learned to cite its methods reference.

apply_name_transfers.py now inserts a REF_LINK (FEATURE / GENE_NAME) citing
the CGD name-transfer reference (create_name_transfer_reference.py) for every
name it sets. Loads that predate that change left their names uncited; this
script adds the missing links after the fact.

The applied set is reconstructed from UPDATE_LOG, which the FEATURE triggers
populated on every loader update: rows with tab_name=FEATURE,
col_name=GENE_NAME, old_value NULL (transfers only ever name unnamed
features) on the given load day(s). This captures the A21 twin propagations
too, and — unlike replaying manifests — cannot mis-attribute a name the
loader didn't set. A feature whose name changed since the load is skipped
loudly (a curator has intervened; their citation governs).

Idempotent; dry-run by default.

Usage:
    python backfill_name_transfer_ref_links.py --day 2026-09-14           # dry-run
    python backfill_name_transfer_ref_links.py --day 2026-09-14 --apply
"""
from __future__ import annotations

import argparse
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
from scripts.ortholog_loading.create_name_transfer_reference import (  # noqa: E402
    REF_TITLE,
    ensure_seq_ahead,
)

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
ADMIN_USER = os.getenv("ADMIN_USER", "cgdadmin").upper()
BATCH = 500

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--day", action="append", required=True,
                    help="load day (YYYY-MM-DD); repeatable")
    ap.add_argument("--apply", action="store_true",
                    help="actually insert (default: dry-run)")
    args = ap.parse_args()
    dry = not args.apply

    with SessionLocal() as session:
        ref_no = session.execute(text(
            f"SELECT reference_no FROM {DB_SCHEMA}.reference WHERE title = :t"),
            {"t": REF_TITLE}).scalar()
        if not ref_no:
            sys.exit("name-transfer reference not found — run "
                     "create_name_transfer_reference.py --apply first")

        candidates: dict[int, str] = {}
        for day in args.day:
            rows = session.execute(text(f"""
                SELECT ul.primary_key, ul.new_value, f.gene_name
                FROM {DB_SCHEMA}.update_log ul
                JOIN {DB_SCHEMA}.feature f ON f.feature_no = ul.primary_key
                WHERE ul.tab_name = 'FEATURE' AND ul.col_name = 'GENE_NAME'
                  AND ul.old_value IS NULL
                  AND ul.date_created >= TO_DATE(:d, 'YYYY-MM-DD')
                  AND ul.date_created < TO_DATE(:d, 'YYYY-MM-DD') + 1
            """), {"d": day}).fetchall()
            logger.info("%s: %d gene-name assignments in update_log", day, len(rows))
            for fno, new_value, current in rows:
                if current != new_value:
                    logger.warning("SKIP feature_no=%d: name changed since load "
                                   "(%s -> %s)", fno, new_value, current)
                    continue
                candidates[fno] = new_value

        existing = {r[0] for r in session.execute(text(f"""
            SELECT primary_key FROM {DB_SCHEMA}.ref_link
            WHERE tab_name = 'FEATURE' AND col_name = 'GENE_NAME'
              AND reference_no = :r
        """), {"r": ref_no}).fetchall()}

        todo = sorted(set(candidates) - existing)
        logger.info("%d features to link (%d already linked)%s",
                    len(todo), len(candidates) - len(todo),
                    " [DRY-RUN]" if dry else "")

        inserted = 0
        if not dry and todo:
            ensure_seq_ahead(session, "REF_LINK_SEQ", "ref_link", "ref_link_no")
            for fno in todo:
                session.execute(text(f"""
                    INSERT INTO {DB_SCHEMA}.ref_link
                        (reference_no, tab_name, primary_key, col_name, created_by)
                    VALUES (:r, 'FEATURE', :f, 'GENE_NAME', :u)
                """), {"r": ref_no, "f": fno, "u": ADMIN_USER})
                inserted += 1
                if inserted % BATCH == 0:
                    session.commit()
                    logger.info("...%d inserted", inserted)
            session.commit()

        logger.info("done: %d ref_links inserted%s", inserted,
                    " [DRY-RUN — no changes]" if dry else "")


if __name__ == "__main__":
    main()
