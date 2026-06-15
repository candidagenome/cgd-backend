#!/usr/bin/env python3
"""Annotate a C. tropicalis review TSV with whether each paper's PMID is
already loaded as a reference in the CGD database.

Reads the input TSV, collects the unique PMIDs, queries the reference table
once per distinct PMID, and writes an output TSV with an appended
``in_database`` column (``yes``/``no``). An empty PMID is reported as ``no``.

Environment:
    DATABASE_URL: Database connection URL (via .env)
    DB_SCHEMA:    Database schema name (default: MULTI)
"""
from __future__ import annotations

import argparse
import csv
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


def pmid_in_database(db, pmid: str) -> bool:
    """Return True if a reference with this PubMed id exists."""
    if not pmid or not pmid.strip():
        return False
    row = db.execute(
        text(f"SELECT reference_no FROM {DB_SCHEMA}.reference WHERE pubmed = :p"),
        {"p": int(pmid)},
    ).first()
    return row is not None


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Annotate review TSV with in-database status per paper"
    )
    ap.add_argument("input", help="input TSV path")
    ap.add_argument(
        "-o", "--output", help="output TSV path (default: overwrite input)"
    )
    args = ap.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output) if args.output else in_path

    with in_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t")
        rows = list(reader)

    if not rows:
        print("Empty file, nothing to do.")
        return 0

    header, data = rows[0], rows[1:]
    try:
        pmid_idx = header.index("pmid")
    except ValueError:
        print("No 'pmid' column found in header.", file=sys.stderr)
        return 1

    db = SessionLocal()
    try:
        # Resolve each distinct PMID once.
        unique_pmids = {r[pmid_idx].strip() for r in data if len(r) > pmid_idx}
        status = {p: pmid_in_database(db, p) for p in unique_pmids}
    finally:
        db.close()

    new_header = header + ["in_database"]
    out_rows = []
    for r in data:
        pmid = r[pmid_idx].strip() if len(r) > pmid_idx else ""
        out_rows.append(r + ["yes" if status.get(pmid) else "no"])

    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(new_header)
        writer.writerows(out_rows)

    present = sum(1 for r in out_rows if r[-1] == "yes")
    distinct_present = sum(1 for p in status.values() if p)
    print(f"Rows: {len(out_rows)}")
    print(f"Distinct PMIDs: {len(unique_pmids)} "
          f"({distinct_present} already in database)")
    print(f"Rows marked in_database=yes: {present}")
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
