#!/usr/bin/env python3
"""
Load curated ortholog-family clusters from a plan JSON into HOMOLOGY_GROUP.

Each plan cluster (see build_curated_cluster_plan.py) becomes one
HOMOLOGY_GROUP row with homology_group_type='curated family' and
method='CGD curation', its members linked via FEAT_HOMOLOGY, and an optional
curation NOTE attached via NOTE_LINK (tab_name='HOMOLOGY_GROUP').

The 'curated family' type is deliberately distinct from 'ortholog' so no
existing type='ortholog' consumer (Homologs tab, synteny, batch download)
picks these up implicitly; display precedence is added explicitly.

Idempotent: an existing 'curated family' group with the same
homology_group_id label is compared member-by-member — identical sets are
skipped, differing sets are reported and left untouched (use --replace to
delete and recreate them).

Primary keys come from Oracle sequences (never MAX+1 — see
scripts/resync_all_sequences.py for why).

Usage:
    python load_curated_clusters.py --plan curated_cluster_plan.json           # dry-run
    python load_curated_clusters.py --plan curated_cluster_plan.json --apply
"""
from __future__ import annotations

import argparse
import json
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

GROUP_TYPE = "curated family"
METHOD = "CGD curation"
NOTE_TYPE = "Curation note"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_scalar(session, sql, **kw):
    return session.execute(text(sql), kw).scalar()


def ensure_code(session, tab, col, value, description, dry):
    hit = get_scalar(session,
                     f"SELECT COUNT(*) FROM {DB_SCHEMA}.code WHERE tab_name=:t "
                     f"AND col_name=:c AND code_value=:v", t=tab, c=col, v=value)
    if hit:
        return
    if dry:
        logger.info("[DRY] would create CODE %s.%s=%s", tab, col, value)
        return
    session.execute(text(
        f"INSERT INTO {DB_SCHEMA}.code (tab_name, col_name, code_value, "
        f"description, created_by) VALUES (:t, :c, :v, :d, :u)"),
        {"t": tab, "c": col, "v": value, "d": description, "u": ADMIN_USER})
    logger.info("created CODE %s.%s=%s", tab, col, value)


def resolve_features(session, members):
    """feature_name -> feature_no, failing loudly on missing/ambiguous."""
    resolved = []
    for m in members:
        rows = session.execute(text(
            f"SELECT feature_no FROM {DB_SCHEMA}.feature WHERE feature_name=:n"),
            {"n": m["feature_name"]}).fetchall()
        if len(rows) != 1:
            raise SystemExit(f"member {m['feature_name']}: expected exactly 1 "
                             f"feature, found {len(rows)}")
        resolved.append((m["feature_name"], rows[0][0]))
    return resolved


def existing_group(session, label):
    """Return (group_no, member_feature_nos) for an existing curated group."""
    row = session.execute(text(
        f"SELECT homology_group_no FROM {DB_SCHEMA}.homology_group "
        f"WHERE homology_group_type=:t AND homology_group_id=:l"),
        {"t": GROUP_TYPE, "l": label}).fetchall()
    if not row:
        return None, None
    if len(row) > 1:
        raise SystemExit(f"label {label!r}: {len(row)} existing curated groups")
    gno = row[0][0]
    members = {r[0] for r in session.execute(text(
        f"SELECT feature_no FROM {DB_SCHEMA}.feat_homology "
        f"WHERE homology_group_no=:g"), {"g": gno}).fetchall()}
    return gno, members


def delete_group(session, gno):
    session.execute(text(
        f"DELETE FROM {DB_SCHEMA}.note WHERE note_no IN ("
        f"SELECT note_no FROM {DB_SCHEMA}.note_link WHERE tab_name=:t "
        f"AND primary_key=:g)"), {"t": "HOMOLOGY_GROUP", "g": gno})
    session.execute(text(
        f"DELETE FROM {DB_SCHEMA}.note_link WHERE tab_name=:t AND primary_key=:g"),
        {"t": "HOMOLOGY_GROUP", "g": gno})
    session.execute(text(
        f"DELETE FROM {DB_SCHEMA}.feat_homology WHERE homology_group_no=:g"),
        {"g": gno})
    session.execute(text(
        f"DELETE FROM {DB_SCHEMA}.homology_group WHERE homology_group_no=:g"),
        {"g": gno})


def load_cluster(session, cluster, dry, replace):
    label = cluster["label"]
    if len(label) > 40:
        raise SystemExit(f"label too long for homology_group_id: {label!r}")
    resolved = resolve_features(session, cluster["members"])
    target = {fno for _, fno in resolved}

    gno, current = existing_group(session, label)
    if gno is not None:
        if current == target:
            logger.info("SKIP %-40s already loaded (group %d)", label, gno)
            return "skipped"
        if not replace:
            logger.warning("DIFFERS %-40s group %d has %d members vs plan %d "
                           "— rerun with --replace to recreate",
                           label, gno, len(current), len(target))
            return "differs"
        if dry:
            logger.info("[DRY] would replace group %d (%s)", gno, label)
            return "replaced"
        delete_group(session, gno)
        logger.info("replaced: deleted group %d (%s)", gno, label)

    if dry:
        logger.info("[DRY] %-40s %d members%s", label, len(resolved),
                    " + note" if cluster.get("note") else "")
        return "loaded"

    gno = get_scalar(session,
                     f"SELECT {DB_SCHEMA}.homology_group_seq.NEXTVAL FROM DUAL")
    session.execute(text(
        f"INSERT INTO {DB_SCHEMA}.homology_group (homology_group_no, "
        f"homology_group_type, method, homology_group_id, created_by) "
        f"VALUES (:g, :t, :m, :l, :u)"),
        {"g": gno, "t": GROUP_TYPE, "m": METHOD, "l": label, "u": ADMIN_USER})
    for fname, fno in resolved:
        session.execute(text(
            f"INSERT INTO {DB_SCHEMA}.feat_homology (feature_no, "
            f"homology_group_no, created_by) VALUES (:f, :g, :u)"),
            {"f": fno, "g": gno, "u": ADMIN_USER})
    if cluster.get("note"):
        note_no = get_scalar(session,
                             f"SELECT {DB_SCHEMA}.note_seq.NEXTVAL FROM DUAL")
        session.execute(text(
            f"INSERT INTO {DB_SCHEMA}.note (note_no, note, note_type, "
            f"created_by) VALUES (:n, :txt, :ty, :u)"),
            {"n": note_no, "txt": cluster["note"], "ty": NOTE_TYPE,
             "u": ADMIN_USER})
        session.execute(text(
            f"INSERT INTO {DB_SCHEMA}.note_link (note_no, tab_name, "
            f"primary_key, created_by) VALUES (:n, :t, :g, :u)"),
            {"n": note_no, "t": "HOMOLOGY_GROUP", "g": gno, "u": ADMIN_USER})
    logger.info("LOADED %-40s group %d, %d members%s", label, gno,
                len(resolved), " + note" if cluster.get("note") else "")
    return "loaded"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--plan", required=True)
    ap.add_argument("--apply", action="store_true",
                    help="actually load (default: dry-run)")
    ap.add_argument("--replace", action="store_true",
                    help="recreate existing curated groups whose members differ")
    args = ap.parse_args()
    dry = not args.apply

    plan = json.load(open(args.plan))
    clusters = plan["clusters"]
    logger.info("plan: %d clusters (%s)%s", len(clusters),
                plan.get("created_for", "?"), " [DRY-RUN]" if dry else "")

    counts = {}
    with SessionLocal() as session:
        ensure_code(session, "HOMOLOGY_GROUP", "HOMOLOGY_GROUP_TYPE", GROUP_TYPE,
                    "Curator-approved ortholog family cluster", dry)
        ensure_code(session, "HOMOLOGY_GROUP", "METHOD", METHOD,
                    "Manual curation of intransitive ortholog components", dry)
        for cluster in clusters:
            outcome = load_cluster(session, cluster, dry, args.replace)
            counts[outcome] = counts.get(outcome, 0) + 1
        if not dry:
            session.commit()
            logger.info("committed")

    logger.info("summary: %s", counts)
    if counts.get("differs"):
        sys.exit(1)


if __name__ == "__main__":
    main()
