#!/usr/bin/env python3
from __future__ import annotations

"""
Transfer manual GO annotations to CGD orthologs as IEA (intra-CGD, all 6 species).

For a gene with an experimental ("manual") GO annotation that also has assigned
orthologs in other CGD species, the GO term is transferred to each cross-species
ortholog as an electronic (IEA) annotation, citing the source gene as with/from.

This complements the existing external-species transfers (PomBase/SGD -> CGD,
which also cite reference CAL0121033 "based on orthology"): this script owns only
the *intra-CGD* subset -- IEAs on CAL0121033 whose with/from is a CGD gene -- and
regenerates them each run, leaving the external transfers untouched.

Rules
-----
Source annotations transferred:  go_evidence in (IDA, IPI, IMP, IGI, IEP),
  annotation_type = 'manually curated', not qualified (no NOT/qualifier).
Target:  cross-species orthologs (same homology_group of type 'ortholog',
  different organism).
Transferred annotation:  go_evidence='IEA', annotation_type='computational',
  source='CGD', reference CAL0121033, with/from = source gene ('CGDID Primary').
Redundancy (skip a candidate target term when):
  - the target already has any IEA/computational/CGD annotation for that term
    (would collide on the unique key; the term is already asserted); OR
  - the target has a curated (non-IEA) annotation for that term or a more
    specific (descendant) term.
  Among the transferred terms for a target, only the most specific are kept
  (a term is dropped if another transferred term is its descendant).
  GO root terms are never transferred.

Refresh model:  each run deletes the script-owned intra-CGD transfers (IEAs on
CAL0121033 whose with/from are all CGD genes) and recomputes them.

Usage:
    python transfer_go_from_orthologs.py --dry-run
    python transfer_go_from_orthologs.py

Environment Variables:
    DATABASE_URL, DB_SCHEMA
"""

import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")

# Source annotations that qualify as "manual" (experimental evidence codes)
SOURCE_EVIDENCE = ("IDA", "IPI", "IMP", "IGI", "IEP")
SOURCE_ANNOTATION_TYPE = "manually curated"

# Transferred (IEA) annotation attributes
XFER_EVIDENCE = "IEA"
XFER_ANNOTATION_TYPE = "computational"
XFER_SOURCE = "CGD"
ORTHOLOG_REF_DBXREF = "CAL0121033"  # "CGD Prediction of GO annotations based on orthology"

# GO root go_no values (never transfer): P=GO:0008150, F=GO:0003674, C=GO:0005575
GO_ROOT_GO_NOS = (24318, 32814, 39472)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def resolve_reference_no(session) -> int:
    r = session.execute(
        text(f"SELECT reference_no FROM {DB_SCHEMA}.reference WHERE dbxref_id = :d"),
        {"d": ORTHOLOG_REF_DBXREF},
    ).scalar()
    if not r:
        raise RuntimeError(f"Reference {ORTHOLOG_REF_DBXREF} not found")
    return r


def get_ortholog_groups(session) -> dict[int, list[tuple[int, int]]]:
    """homology_group_no -> [(feature_no, organism_no), ...] for 'ortholog' groups."""
    rows = session.execute(text(f"""
        SELECT fh.homology_group_no, fh.feature_no, f.organism_no
        FROM {DB_SCHEMA}.feat_homology fh
        JOIN {DB_SCHEMA}.homology_group hg
          ON fh.homology_group_no = hg.homology_group_no AND hg.homology_group_type = 'ortholog'
        JOIN {DB_SCHEMA}.feature f ON fh.feature_no = f.feature_no
    """)).fetchall()
    groups: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for hg_no, feat_no, org_no in rows:
        groups[hg_no].append((feat_no, org_no))
    return groups


def get_source_annotations(session):
    """Return (feat->set(go_no), (feat,go_no)->set(evidence)) for qualifying
    experimental, unqualified annotations."""
    ev_list = ", ".join(f"'{e}'" for e in SOURCE_EVIDENCE)
    rows = session.execute(text(f"""
        SELECT ga.feature_no, ga.go_no, ga.go_evidence
        FROM {DB_SCHEMA}.go_annotation ga
        WHERE ga.go_evidence IN ({ev_list})
          AND ga.annotation_type = :atype
          AND NOT EXISTS (
              SELECT 1 FROM {DB_SCHEMA}.go_ref gr
              WHERE gr.go_annotation_no = ga.go_annotation_no AND gr.has_qualifier = 'Y'
          )
    """), {"atype": SOURCE_ANNOTATION_TYPE}).fetchall()
    out: dict[int, set[int]] = defaultdict(set)
    ev: dict[tuple[int, int], set[str]] = defaultdict(set)
    for feat_no, go_no, evid in rows:
        out[feat_no].add(go_no)
        ev[(feat_no, go_no)].add(evid)
    return out, ev


def get_existing_target_annotations(session):
    """Return (already_iea, curated) sets/maps for redundancy checks.

    already_iea: set of (feature_no, go_no) already annotated IEA/computational/CGD.
    curated_go:  feature_no -> set(go_no) with a curated (non-IEA) annotation.
    """
    already_iea: set[tuple[int, int]] = set()
    curated_go: dict[int, set[int]] = defaultdict(set)
    rows = session.execute(text(f"""
        SELECT feature_no, go_no, go_evidence, annotation_type, source
        FROM {DB_SCHEMA}.go_annotation
    """)).fetchall()
    for feat_no, go_no, ev, atype, src in rows:
        if ev == XFER_EVIDENCE and atype == XFER_ANNOTATION_TYPE and src == XFER_SOURCE:
            already_iea.add((feat_no, go_no))
        elif ev != XFER_EVIDENCE:  # curated (any non-IEA evidence)
            curated_go[feat_no].add(go_no)
    return already_iea, curated_go


def get_descendants(session, go_nos: set[int]) -> dict[int, set[int]]:
    """ancestor go_no -> set(descendant go_no) from go_path, limited to given ancestors."""
    desc: dict[int, set[int]] = defaultdict(set)
    if not go_nos:
        return desc
    go_list = list(go_nos)
    # chunk IN-lists to stay within Oracle's 1000-item limit
    for i in range(0, len(go_list), 900):
        chunk = go_list[i:i + 900]
        binds = {f"g{j}": v for j, v in enumerate(chunk)}
        inlist = ", ".join(f":{k}" for k in binds)
        rows = session.execute(text(f"""
            SELECT ancestor_go_no, child_go_no FROM {DB_SCHEMA}.go_path
            WHERE ancestor_go_no IN ({inlist})
        """), binds).fetchall()
        for anc, child in rows:
            desc[anc].add(child)
    return desc


def get_cgdid_dbxref_map(session, feature_nos: set[int]) -> dict[int, int]:
    """source feature_no -> its 'CGDID Primary' dbxref_no (for with/from)."""
    rows = session.execute(text(f"""
        SELECT f.feature_no, d.dbxref_no
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.dbxref d
          ON d.dbxref_id = f.dbxref_id AND d.dbxref_type = 'CGDID Primary' AND d.source = 'CGD'
    """)).fetchall()
    return {feat_no: dbxref_no for feat_no, dbxref_no in rows if feat_no in feature_nos}


def nextval(session, seq: str) -> int:
    return session.execute(text(f"SELECT {DB_SCHEMA}.{seq}.NEXTVAL FROM dual")).scalar()


def delete_owned_transfers(session) -> int:
    """Delete script-owned intra-CGD transfers: IEA/computational/CGD annotations
    on ref CAL0121033 whose with/from dbxrefs are ALL CGD genes (no external)."""
    ref_no = resolve_reference_no(session)
    # annotation_nos that are IEA/comp/CGD, linked to the ortholog ref, and have at
    # least one CGD with/from but NO non-CGD with/from.
    rows = session.execute(text(f"""
        SELECT ga.go_annotation_no
        FROM {DB_SCHEMA}.go_annotation ga
        JOIN {DB_SCHEMA}.go_ref gr ON ga.go_annotation_no = gr.go_annotation_no
        WHERE ga.go_evidence = :ev AND ga.annotation_type = :at AND ga.source = :src
          AND gr.reference_no = :ref
          AND EXISTS (
              SELECT 1 FROM {DB_SCHEMA}.goref_dbxref gd JOIN {DB_SCHEMA}.dbxref d ON gd.dbxref_no = d.dbxref_no
              WHERE gd.go_ref_no = gr.go_ref_no AND d.source = 'CGD')
          AND NOT EXISTS (
              SELECT 1 FROM {DB_SCHEMA}.goref_dbxref gd JOIN {DB_SCHEMA}.dbxref d ON gd.dbxref_no = d.dbxref_no
              WHERE gd.go_ref_no = gr.go_ref_no AND d.source <> 'CGD')
    """), {"ev": XFER_EVIDENCE, "at": XFER_ANNOTATION_TYPE, "src": XFER_SOURCE, "ref": ref_no}).fetchall()
    ann_nos = [r[0] for r in rows]
    if not ann_nos:
        return 0
    # delete go_annotation rows; FK cascade removes go_ref + goref_dbxref
    for i in range(0, len(ann_nos), 900):
        chunk = ann_nos[i:i + 900]
        binds = {f"a{j}": v for j, v in enumerate(chunk)}
        inlist = ", ".join(f":{k}" for k in binds)
        session.execute(text(f"DELETE FROM {DB_SCHEMA}.go_annotation WHERE go_annotation_no IN ({inlist})"), binds)
    return len(ann_nos)


def build_transfers(session):
    """Compute {(target_feature, go_no): set(source_feature)} after redundancy rules."""
    groups = get_ortholog_groups(session)
    logger.info("Ortholog groups: %d", len(groups))

    source_ann, src_ev = get_source_annotations(session)
    logger.info("Genes with qualifying manual GO: %d", len(source_ann))

    # Candidate transfers: target_feature -> go_no -> set(source_feature)
    cand: dict[int, dict[int, set[int]]] = defaultdict(lambda: defaultdict(set))
    roots = set(GO_ROOT_GO_NOS)
    for members in groups.values():
        for src_feat, src_org in members:
            src_gos = source_ann.get(src_feat)
            if not src_gos:
                continue
            for tgt_feat, tgt_org in members:
                if tgt_org == src_org:
                    continue  # cross-species only
                for go_no in src_gos:
                    if go_no in roots:
                        continue
                    cand[tgt_feat][go_no].add(src_feat)

    logger.info("Candidate target genes: %d", len(cand))

    already_iea, curated_go = get_existing_target_annotations(session)

    # Descendants of all candidate go_nos (for redundancy + most-specific rules)
    all_cand_gos: set[int] = set()
    for gomap in cand.values():
        all_cand_gos.update(gomap.keys())
    descendants = get_descendants(session, all_cand_gos)

    transfers: dict[tuple[int, int], set[int]] = {}
    stats = {"skip_existing_iea": 0, "skip_curated": 0, "skip_less_specific": 0, "kept": 0}

    for tgt_feat, gomap in cand.items():
        tgt_curated = curated_go.get(tgt_feat, set())
        go_nos = set(gomap.keys())
        for go_no in go_nos:
            # collision / already asserted electronically
            if (tgt_feat, go_no) in already_iea:
                stats["skip_existing_iea"] += 1
                continue
            # curated annotation to this term or a more specific descendant
            desc = descendants.get(go_no, set())
            if go_no in tgt_curated or (tgt_curated & desc):
                stats["skip_curated"] += 1
                continue
            # keep only most specific among candidates: drop if a descendant is also a candidate
            if desc & (go_nos - {go_no}):
                stats["skip_less_specific"] += 1
                continue
            transfers[(tgt_feat, go_no)] = gomap[go_no]
            stats["kept"] += 1

    logger.info("Transfer decisions: %s", stats)
    return transfers, src_ev


def insert_transfers(session, transfers, ref_no, created_by) -> dict:
    stats = {"annotations": 0, "with_from_links": 0, "missing_dbxref": 0}
    src_feats = {f for sources in transfers.values() for f in sources}
    dbxref_map = get_cgdid_dbxref_map(session, src_feats)

    for (tgt_feat, go_no), sources in transfers.items():
        source_dbxrefs = [dbxref_map[f] for f in sorted(sources) if f in dbxref_map]
        if not source_dbxrefs:
            stats["missing_dbxref"] += 1
            continue
        ann_no = nextval(session, "go_annotation_seq")
        session.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.go_annotation
                (go_annotation_no, go_no, feature_no, go_evidence, annotation_type, source, created_by)
            VALUES (:n, :go, :feat, :ev, :at, :src, :cb)
        """), {"n": ann_no, "go": go_no, "feat": tgt_feat, "ev": XFER_EVIDENCE,
               "at": XFER_ANNOTATION_TYPE, "src": XFER_SOURCE, "cb": created_by})

        ref_row = nextval(session, "go_ref_seq")
        session.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.go_ref
                (go_ref_no, reference_no, go_annotation_no, has_qualifier, has_supporting_evidence, created_by)
            VALUES (:n, :ref, :ann, 'N', 'Y', :cb)
        """), {"n": ref_row, "ref": ref_no, "ann": ann_no, "cb": created_by})

        for dbxref_no in source_dbxrefs:
            gd_no = nextval(session, "goref_dbxref_seq")
            session.execute(text(f"""
                INSERT INTO {DB_SCHEMA}.goref_dbxref (goref_dbxref_no, go_ref_no, dbxref_no, support_type)
                VALUES (:n, :gr, :dx, 'With')
            """), {"n": gd_no, "gr": ref_row, "dx": dbxref_no})
            stats["with_from_links"] += 1
        stats["annotations"] += 1

        if stats["annotations"] % 2000 == 0:
            session.flush()
            logger.info("Inserted %d transferred annotations so far...", stats["annotations"])
    return stats


def _feature_info(session, feature_nos: set[int]) -> dict[int, tuple]:
    """feature_no -> (feature_name, gene_name, cgdid, organism_abbrev)."""
    info: dict[int, tuple] = {}
    fnos = list(feature_nos)
    for i in range(0, len(fnos), 900):
        chunk = fnos[i:i + 900]
        binds = {f"f{j}": v for j, v in enumerate(chunk)}
        inlist = ", ".join(f":{k}" for k in binds)
        for r in session.execute(text(f"""
            SELECT f.feature_no, f.feature_name, f.gene_name, f.dbxref_id, o.organism_abbrev
            FROM {DB_SCHEMA}.feature f JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE f.feature_no IN ({inlist})
        """), binds):
            info[r[0]] = (r[1], r[2] or "", r[3], r[4])
    return info


def _go_info(session, go_nos: set[int]) -> dict[int, tuple]:
    """go_no -> (GOID string, term, aspect)."""
    info: dict[int, tuple] = {}
    gnos = list(go_nos)
    for i in range(0, len(gnos), 900):
        chunk = gnos[i:i + 900]
        binds = {f"g{j}": v for j, v in enumerate(chunk)}
        inlist = ", ".join(f":{k}" for k in binds)
        for r in session.execute(text(f"""
            SELECT go_no, goid, go_term, go_aspect FROM {DB_SCHEMA}.go WHERE go_no IN ({inlist})
        """), binds):
            info[r[0]] = (f"GO:{int(r[1]):07d}", r[2], r[3])
    return info


def write_report(session, transfers, src_ev, path: str) -> int:
    """Write a TSV of every proposed transfer (one row per source->target,GO)."""
    feats = {t for t, _ in transfers} | {f for srcs in transfers.values() for f in srcs}
    gos = {go for _, go in transfers}
    finfo = _feature_info(session, feats)
    ginfo = _go_info(session, gos)

    header = ["Source_Gene", "Source_Name", "Source_CGDID", "Source_Species",
              "Source_Evidence", "GOID", "GO_Term", "Aspect",
              "Target_Gene", "Target_Name", "Target_CGDID", "Target_Species",
              "Transferred_Evidence", "Reference"]
    rows = []
    for (tgt, go_no), sources in transfers.items():
        tg = finfo.get(tgt)
        gg = ginfo.get(go_no)
        if not tg or not gg:
            continue
        goid, term, aspect = gg
        for src in sorted(sources):
            sf = finfo.get(src)
            if not sf:
                continue
            ev = ",".join(sorted(src_ev.get((src, go_no), {"?"})))
            rows.append([sf[0], sf[1], sf[2], sf[3], ev, goid, term, aspect,
                         tg[0], tg[1], tg[2], tg[3], XFER_EVIDENCE,
                         f"CGD_REF:{ORTHOLOG_REF_DBXREF}"])
    # sort by target species, target gene, GOID for reviewability
    rows.sort(key=lambda r: (r[11], r[8], r[5]))
    with open(path, "w") as f:
        f.write("\t".join(header) + "\n")
        for r in rows:
            f.write("\t".join("" if c is None else str(c) for c in r) + "\n")
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Transfer manual GO to CGD orthologs as IEA")
    parser.add_argument("--dry-run", action="store_true", help="Compute + report; no DB changes")
    parser.add_argument("--report", help="Write a TSV of proposed transfers to this path")
    parser.add_argument("--created-by", default=os.getenv("ADMIN_USER", "SWENG").upper()[:12])
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Transfer manual GO -> CGD orthologs (IEA)")
    if args.dry_run:
        logger.info("[DRY RUN] no database changes")
    logger.info("created_by=%s started=%s", args.created_by, datetime.now())
    logger.info("=" * 60)

    try:
        with SessionLocal() as session:
            ref_no = resolve_reference_no(session)
            logger.info("Ortholog reference %s = reference_no %d", ORTHOLOG_REF_DBXREF, ref_no)

            transfers, src_ev = build_transfers(session)
            logger.info("Transfers to apply: %d annotations across %d target genes",
                        len(transfers), len({t for t, _ in transfers}))

            if args.report:
                n = write_report(session, transfers, src_ev, args.report)
                logger.info("Wrote report: %s (%d rows)", args.report, n)

            deleted = delete_owned_transfers(session)
            logger.info("%s %d existing intra-CGD transfer annotations",
                        "Would delete" if args.dry_run else "Deleted", deleted)

            if args.dry_run:
                # still compute insert stats without writing
                src_feats = {f for s in transfers.values() for f in s}
                dbxref_map = get_cgdid_dbxref_map(session, src_feats)
                missing = sum(1 for s in transfers.values() if not any(f in dbxref_map for f in s))
                wf = sum(len([f for f in s if f in dbxref_map]) for s in transfers.values())
                logger.info("Would insert %d annotations, %d with/from links (%d missing source dbxref)",
                            len(transfers) - missing, wf, missing)
                session.rollback()
            else:
                ins = insert_transfers(session, transfers, ref_no, args.created_by)
                session.commit()
                logger.info("Inserted: %s", ins)
    except Exception as e:  # noqa: BLE001
        logger.exception("Failed: %s", e)
        return 1

    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
