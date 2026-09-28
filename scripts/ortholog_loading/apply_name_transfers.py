#!/usr/bin/env python3
"""
Apply QC-passed ortholog gene-name transfers to FEATURE.gene_name.

Input is the transfer manifest emitted by qc_ortholog_name_transfers.py
propose mode (TRANSFER rows only: feature_name + proposed_name). Run the QC
round-trip (check mode) on the manifest immediately before applying; this
loader re-verifies only the local invariants:

  - the target feature exists, uniquely, and is an ORF
  - its gene_name is NULL (a target already carrying exactly the proposed
    name counts as already-applied and is skipped — idempotent reruns)
  - a target named anything else is skipped loudly (stale manifest)

C. albicans conventions (verified against the DB, 2026-09-14):
  - Assembly 22 "_B" allele twins never carry gene_name (0/3,270 named _A
    twins have a named _B) — the loader leaves them untouched.
  - The Assembly 21 primary feature (child side of the 'Assembly 21 Primary
    Allele' relationship) shares the A22 name in ~89% of named loci — the
    loader propagates the new name to an unnamed A21 primary twin and logs
    it; a differently-named twin is a warning, never overwritten.

Every name the loader sets is backed by a REF_LINK (FEATURE / GENE_NAME) row
citing the CGD name-transfer methods reference (see
create_name_transfer_reference.py — run it once per database before applying).
Already-applied rows get the ref_link too if it is missing, so reruns heal
older loads. Names the loader did NOT set (a differently-named target or an
A21 twin that already carried a name) are never linked.

Updates run through the normal FEATURE triggers, so update_log carries the
full audit trail. Dry-run by default.

Usage:
    python apply_name_transfers.py --manifest manifest_transfer.tsv           # dry-run
    python apply_name_transfers.py --manifest manifest_transfer.tsv --apply
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
from scripts.ortholog_loading.create_name_transfer_reference import (  # noqa: E402
    REF_TITLE,
    ensure_seq_ahead,
)

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
ADMIN_USER = os.getenv("ADMIN_USER", "cgdadmin").upper()
BATCH = 500

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def read_manifest(path):
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        cols = {c.lower(): c for c in reader.fieldnames or ()}
        fcol = cols.get("feature_name")
        ncol = cols.get("proposed_name") or cols.get("gene_name")
        if not fcol or not ncol:
            sys.exit("manifest needs feature_name and proposed_name columns")
        rows = [(r[fcol].strip(), r[ncol].strip())
                for r in reader if r.get(fcol, "").strip() and r.get(ncol, "").strip()]
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--apply", action="store_true",
                    help="actually update (default: dry-run)")
    args = ap.parse_args()
    dry = not args.apply

    rows = read_manifest(args.manifest)
    logger.info("manifest: %d transfers%s", len(rows), " [DRY-RUN]" if dry else "")

    applied = already = skipped = a21_named = linked = 0
    with SessionLocal() as session:
        def q(sql, **kw):
            return session.execute(text(sql), kw).fetchall()

        ref_no = session.execute(text(
            f"SELECT reference_no FROM {DB_SCHEMA}.reference WHERE title = :t"),
            {"t": REF_TITLE}).scalar()
        if not ref_no:
            msg = ("name-transfer reference not found — run "
                   "create_name_transfer_reference.py --apply first")
            if dry:
                logger.warning("%s (dry-run continues without ref_links)", msg)
            else:
                sys.exit(msg)
        if not dry and ref_no:
            ensure_seq_ahead(session, "REF_LINK_SEQ", "ref_link", "ref_link_no")

        def ensure_ref_link(feature_no: int) -> None:
            """Cite the name-transfer reference for a name this loader set."""
            nonlocal linked
            if dry or not ref_no:
                return
            exists = q(f"""SELECT 1 FROM {DB_SCHEMA}.ref_link
                       WHERE tab_name = 'FEATURE' AND col_name = 'GENE_NAME'
                         AND primary_key = :f AND reference_no = :r""",
                       f=feature_no, r=ref_no)
            if not exists:
                session.execute(text(
                    f"""INSERT INTO {DB_SCHEMA}.ref_link
                        (reference_no, tab_name, primary_key, col_name, created_by)
                    VALUES (:r, 'FEATURE', :f, 'GENE_NAME', :u)"""),
                    {"r": ref_no, "f": feature_no, "u": ADMIN_USER})
                linked += 1

        for i, (fname, name) in enumerate(rows, 1):
            feats = q(f"SELECT feature_no, gene_name, feature_type, organism_no"
                      f" FROM {DB_SCHEMA}.feature WHERE feature_name=:n", n=fname)
            if len(feats) != 1:
                logger.warning("SKIP %s: %d features by that name", fname, len(feats))
                skipped += 1
                continue
            fno, current, ftype, org_no = feats[0]
            if current:
                if current.upper() == name.upper():
                    ensure_ref_link(fno)
                    already += 1
                else:
                    logger.warning("SKIP %s: already named %s (manifest says %s)",
                                   fname, current, name)
                    skipped += 1
                continue
            if ftype != "ORF":
                logger.warning("SKIP %s: feature_type %s", fname, ftype)
                skipped += 1
                continue

            if not dry:
                session.execute(text(
                    f"UPDATE {DB_SCHEMA}.feature SET gene_name=:g"
                    f" WHERE feature_no=:f"), {"g": name, "f": fno})
                ensure_ref_link(fno)
            applied += 1

            # Propagate to an unnamed Assembly 21 primary twin (albicans A22)
            twins = q(
                f"""SELECT c.feature_no, c.feature_name, c.gene_name
                FROM {DB_SCHEMA}.feat_relationship fr
                JOIN {DB_SCHEMA}.feature c ON c.feature_no = fr.child_feature_no
                WHERE fr.parent_feature_no = :f
                  AND fr.relationship_type = :rt""",
                f=fno, rt="Assembly 21 Primary Allele")
            for tno, tname, tgene in twins:
                if tgene is None:
                    if not dry:
                        session.execute(text(
                            f"UPDATE {DB_SCHEMA}.feature SET gene_name=:g"
                            f" WHERE feature_no=:f"), {"g": name, "f": tno})
                        ensure_ref_link(tno)
                    a21_named += 1
                elif tgene.upper() != name.upper():
                    logger.warning("A21 twin %s of %s already named %s (not touched)",
                                   tname, fname, tgene)

            if not dry and applied % BATCH == 0:
                session.commit()
                logger.info("...%d applied (of %d rows)", applied, i)

        if not dry:
            session.commit()

    logger.info("done: %d applied, %d A21 twins named, %d already applied, "
                "%d skipped, %d ref_links added%s", applied, a21_named, already,
                skipped, linked, " [DRY-RUN — no changes]" if dry else "")
    if skipped:
        sys.exit(1)


if __name__ == "__main__":
    main()
