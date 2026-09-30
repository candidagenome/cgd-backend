#!/usr/bin/env python3
"""
Apply curator-approved deletions of legacy PomBase GO-transfer IEAs, and
rewrite the auto-generated description lines that were built from them.

Input is the decided QC worklist from qc_pombase_ortholog_iea.py (columns
feature_name, pombe_gene, recommendation, DECISION). Rows are acted on when
DECISION is 'OK' (accept a DELETE recommendation) or 'DELETE'; 'KEEP',
'NOT SURE' and blank rows are skipped.

For each approved (gene, pombe gene) pair:
 1. its IEAs are deleted — annotations whose sole with/from is that pombe
    gene are removed outright; a mixed annotation that also cites another
    source (typically an EBI InterPro domain) is kept, but the pombe gene is
    stripped from its with/from list so the bad ortholog is no longer cited;
 2. if the gene's description line is the auto-generated kind (headline
    owned by the CAL0139843 auto-description reference via REF_LINK), it is
    regenerated from the GO annotations that survive the deletion, using the
    same logic as make_automatic_descriptions.py — so a headline that led
    with a bad pombe term (the XOG1 "Ortholog(s) have glucan endo-1,6..."
    case) is rebuilt around the remaining SGD/intra-CGD-supported terms.
    Curator-written headlines (no auto-description ref_link) are never
    touched and are reported instead.

Everything runs in one transaction: dry-run (default) executes the deletions
and regenerations, prints the before/after headlines, then rolls back.

Usage:
    python apply_pombase_iea_deletions.py --decisions pombase_iea_qc_priority.tsv
    python apply_pombase_iea_deletions.py --decisions ... --apply
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
from scripts.untested.cron.make_automatic_descriptions import (  # noqa: E402
    AutomaticDescriptionGenerator,
)

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
AUTO_DESC_REF_NO = 56824  # CAL0139843, "Description lines for gene products..."

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def read_decisions(path, all_recommended=False):
    """Yield (feature_name, pombe_gene) pairs approved for deletion.

    all_recommended: curator gave a blanket OK — every recommendation=DELETE
    row counts as approved, unless its DECISION says KEEP/NOT SURE.
    """
    approved, skipped = [], 0
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            decision = (row.get("DECISION") or "").strip().upper()
            rec = (row.get("recommendation") or "").strip().upper()
            ok = (decision == "DELETE"
                  or (decision == "OK" and rec == "DELETE")
                  or (all_recommended and rec == "DELETE"
                      and decision not in ("KEEP", "NOT SURE")))
            if ok:
                approved.append((row["feature_name"].strip(),
                                 row["pombe_gene"].strip()))
            else:
                skipped += 1
    return approved, skipped


def delete_pair_ieas(session, feature_name, pombe_gene):
    """Delete IEAs on the feature whose sole with/from is the pombe gene.

    Returns (feature_no, n_deleted); mixed-source annotations are warned about
    and kept.
    """
    feats = session.execute(text(
        f"SELECT feature_no FROM {DB_SCHEMA}.feature WHERE feature_name = :f"),
        {"f": feature_name}).fetchall()
    if len(feats) != 1:
        logger.warning("SKIP %s: %d features by that name", feature_name, len(feats))
        return None, 0
    fno = feats[0][0]

    rows = session.execute(text(f"""
        SELECT ga.go_annotation_no,
               (SELECT count(*) FROM {DB_SCHEMA}.goref_dbxref gd2
                JOIN {DB_SCHEMA}.dbxref d2 ON d2.dbxref_no = gd2.dbxref_no
                JOIN {DB_SCHEMA}.go_ref gr2 ON gr2.go_ref_no = gd2.go_ref_no
                WHERE gr2.go_annotation_no = ga.go_annotation_no
                  AND NOT (d2.source = 'POMBASE' AND d2.dbxref_id = :p)) other_wf
        FROM {DB_SCHEMA}.go_annotation ga
        WHERE ga.feature_no = :f AND ga.go_evidence = 'IEA'
          AND ga.annotation_type = 'computational' AND ga.source = 'CGD'
          AND EXISTS (
              SELECT 1 FROM {DB_SCHEMA}.go_ref gr
              JOIN {DB_SCHEMA}.goref_dbxref gd ON gd.go_ref_no = gr.go_ref_no
              JOIN {DB_SCHEMA}.dbxref d ON d.dbxref_no = gd.dbxref_no
              WHERE gr.go_annotation_no = ga.go_annotation_no
                AND d.source = 'POMBASE' AND d.dbxref_id = :p)
    """), {"f": fno, "p": pombe_gene}).fetchall()

    to_delete = [r[0] for r in rows if r[1] == 0]
    mixed = [r[0] for r in rows if r[1] > 0]
    for i in range(0, len(to_delete), 900):
        chunk = to_delete[i:i + 900]
        binds = {f"a{j}": v for j, v in enumerate(chunk)}
        inlist = ", ".join(f":{k}" for k in binds)
        session.execute(text(
            f"DELETE FROM {DB_SCHEMA}.go_annotation"
            f" WHERE go_annotation_no IN ({inlist})"), binds)
    # Mixed annotations survive on their other support (e.g. InterPro
    # domains), but stop citing the disavowed pombe gene.
    stripped = 0
    for ann_no in mixed:
        stripped += session.execute(text(f"""
            DELETE FROM {DB_SCHEMA}.goref_dbxref
            WHERE go_ref_no IN (SELECT go_ref_no FROM {DB_SCHEMA}.go_ref
                                WHERE go_annotation_no = :a)
              AND dbxref_no IN (SELECT dbxref_no FROM {DB_SCHEMA}.dbxref
                                WHERE source = 'POMBASE' AND dbxref_id = :p)
        """), {"a": ann_no, "p": pombe_gene}).rowcount
    if mixed:
        logger.info("%s/%s: %d mixed annotations kept, %d pombe citations stripped",
                    feature_name, pombe_gene, len(mixed), stripped)
    return fno, len(to_delete)


def rewrite_headline(session, generators, fno, feature_name):
    """Regenerate an auto-owned headline from surviving GO. Returns status."""
    owned = session.execute(text(f"""
        SELECT 1 FROM {DB_SCHEMA}.ref_link
        WHERE tab_name = 'FEATURE' AND col_name = 'HEADLINE'
          AND primary_key = :f AND reference_no = :r"""),
        {"f": fno, "r": AUTO_DESC_REF_NO}).fetchone()
    old, org_abbrev = session.execute(text(f"""
        SELECT f.headline, o.organism_abbrev
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.organism o ON o.organism_no = f.organism_no
        WHERE f.feature_no = :f"""), {"f": fno}).fetchone()
    if not owned:
        if old:
            return f"headline left alone (not auto-generated): {old[:60]}..."
        return "no headline"

    if org_abbrev not in generators:
        generators[org_abbrev] = AutomaticDescriptionGenerator(
            session, org_abbrev, load_to_db=False)
    new = generators[org_abbrev].generate_description(fno, feature_name)
    if new == old:
        return "headline unchanged"
    session.execute(text(
        f"UPDATE {DB_SCHEMA}.feature SET headline = :h WHERE feature_no = :f"),
        {"h": new, "f": fno})
    return f"headline rewritten:\n      old: {old}\n      new: {new}"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--decisions", required=True,
                    help="decided QC TSV (DECISION column filled in)")
    ap.add_argument("--all-recommended", action="store_true",
                    help="blanket curator OK: act on every recommendation="
                         "DELETE row not marked KEEP/NOT SURE")
    ap.add_argument("--apply", action="store_true",
                    help="commit (default: run everything, then roll back)")
    args = ap.parse_args()
    dry = not args.apply

    approved, skipped = read_decisions(args.decisions, args.all_recommended)
    logger.info("%d pairs approved for deletion, %d rows skipped%s",
                len(approved), skipped, " [DRY-RUN]" if dry else "")
    if not approved:
        return

    deleted_total = rewritten = 0
    generators = {}
    with SessionLocal() as session:
        affected = {}
        for feature_name, pombe_gene in approved:
            fno, n = delete_pair_ieas(session, feature_name, pombe_gene)
            if fno is None:
                continue
            deleted_total += n
            logger.info("%s / %s: %d IEAs deleted", feature_name, pombe_gene, n)
            affected[fno] = feature_name

        for fno, feature_name in affected.items():
            status = rewrite_headline(session, generators, fno, feature_name)
            if status.startswith("headline rewritten"):
                rewritten += 1
            logger.info("%s: %s", feature_name, status)

        if dry:
            session.rollback()
        else:
            session.commit()

    logger.info("done: %d IEAs deleted, %d headlines rewritten%s",
                deleted_total, rewritten,
                " [DRY-RUN — rolled back]" if dry else "")


if __name__ == "__main__":
    main()
