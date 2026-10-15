#!/usr/bin/env python3
from __future__ import annotations

"""
Remove ND GO placeholders that have been superseded by real annotations.

make_go_complete.py gives every gene without any annotation in an aspect the
root term with evidence ND ("No biological Data available"). Once that aspect
gains a real annotation -- most often an IEA from the quarterly ortholog
transfers (transfer_go_from_sgd.py / transfer_go_from_orthologs.py), but also
manual curation -- the ND row is stale and must go, otherwise the GO tab and
the GAF show "no data" next to the new annotation.

An ND annotation is removed when the same feature has at least one non-ND
annotation in the same aspect (the inverse of make_go_complete's rule). The
go_ref / go_qualifier child rows go with it via ON DELETE CASCADE.

Both transfer scripts call remove_superseded_nd() in their own transaction
after inserting; run this script directly for a one-off cleanup.

--all removes every ND placeholder, superseded or not. Curators decided
(2026-10) to drop the NDs entirely: the 2026-04-04 make_go_complete batch had
only covered C. albicans, C. glabrata and C. parapsilosis, so the placeholders
were inconsistent across species.

Usage:
    python remove_superseded_nd_go.py --dry-run
    python remove_superseded_nd_go.py --organisms C_glabrata_CBS138
    python remove_superseded_nd_go.py
    python remove_superseded_nd_go.py --all --dry-run

Environment Variables:
    DATABASE_URL, DB_SCHEMA
"""

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

from cgd.db.engine import SessionLocal

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _nd_where(organisms: list[str] | None, superseded_only: bool = True) -> tuple[str, dict]:
    """FROM/WHERE clause (alias nd, go gn, feature f, organism o) selecting ND
    annotations -- only superseded ones unless superseded_only is False --
    optionally limited to the given organisms."""
    binds: dict = {}
    org_sql = ""
    if organisms:
        binds = {f"org{i}": o for i, o in enumerate(organisms)}
        org_sql = f"AND o.organism_abbrev IN ({', '.join(f':{k}' for k in binds)})"
    sql = f"""
        FROM {DB_SCHEMA}.go_annotation nd
        JOIN {DB_SCHEMA}.go gn ON gn.go_no = nd.go_no
        JOIN {DB_SCHEMA}.feature f ON f.feature_no = nd.feature_no
        JOIN {DB_SCHEMA}.organism o ON o.organism_no = f.organism_no
        WHERE nd.go_evidence = 'ND' {org_sql}"""
    if superseded_only:
        sql += f"""
          AND EXISTS (
              SELECT 1
              FROM {DB_SCHEMA}.go_annotation ga
              JOIN {DB_SCHEMA}.go g ON g.go_no = ga.go_no
              WHERE ga.feature_no = nd.feature_no
                AND ga.go_evidence <> 'ND'
                AND g.go_aspect = gn.go_aspect)"""
    return sql, binds


def remove_superseded_nd(session, organisms: list[str] | None = None,
                         superseded_only: bool = True) -> int:
    """Delete ND annotations whose aspect now has a real annotation on the same
    feature (or every ND annotation when superseded_only is False). Does not
    commit. Returns the number of ND annotations deleted."""
    where_sql, binds = _nd_where(organisms, superseded_only)
    label = "superseded ND" if superseded_only else "ND"
    rows = session.execute(text(f"""
        SELECT o.organism_abbrev, gn.go_aspect, COUNT(*) {where_sql}
        GROUP BY o.organism_abbrev, gn.go_aspect
        ORDER BY o.organism_abbrev, gn.go_aspect
    """), binds).fetchall()
    for org, aspect, n in rows:
        logger.info("  %s: %-24s %s %6d", label, org, aspect, n)
    total = sum(r[2] for r in rows)
    if total:
        session.execute(text(f"""
            DELETE FROM {DB_SCHEMA}.go_annotation
            WHERE go_annotation_no IN (SELECT nd.go_annotation_no {where_sql})
        """), binds)
    return total


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Remove ND GO placeholders superseded by real annotations")
    parser.add_argument("--dry-run", action="store_true", help="Count only; no DB changes")
    parser.add_argument("--all", action="store_true",
                        help="Remove every ND annotation, not just superseded ones")
    parser.add_argument("--organisms",
                        help="Comma-separated organism_abbrev list; default: all species")
    args = parser.parse_args()
    organisms = [o.strip() for o in args.organisms.split(",") if o.strip()] if args.organisms else None

    try:
        with SessionLocal() as session:
            n = remove_superseded_nd(session, organisms, superseded_only=not args.all)
            what = "ND" if args.all else "superseded ND"
            if args.dry_run:
                session.rollback()
                logger.info("[DRY RUN] Would delete %d %s annotations", n, what)
            else:
                session.commit()
                logger.info("Deleted %d %s annotations", n, what)
    except Exception as e:  # noqa: BLE001
        logger.exception("Failed: %s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
