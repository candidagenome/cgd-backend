#!/usr/bin/env python3
from __future__ import annotations

"""
Transfer experimental GO annotations from S. cerevisiae (SGD) to CGD orthologs as IEA.

For each S. cerevisiae gene that is a member of a CGD 'ortholog' homology group
(Sc genes are stored as external dbxrefs on the group, e.g. YDR298C), its
experimental GO annotations from the SGD gene-association file are transferred
to the CGD genes in the same group as electronic (IEA) annotations, citing the
Sc gene as with/from.

This restores the SGD leg of the documented orthology-transfer process
(reference CAL0121033, "Prediction of GO annotations based on orthology"),
which was lost in the old-CGD migration: old GAFs carried ~21k SGD-derived
IEAs per species through 2024-01, the current database has none.

Companion scripts, disjoint ownership on the same reference:
  - transfer_go_from_orthologs.py owns IEAs whose with/from are all CGD genes.
  - this script owns IEAs whose with/from are all SGD genes.
  - the frozen PomBase-with/from transfers are untouched by both.
Run this script BEFORE transfer_go_from_orthologs.py so that a term supported
by both a direct Sc experimental annotation and an intra-CGD one is attributed
to the Sc source (the unique key allows only one IEA/computational/CGD
annotation per feature+term; whichever script runs first claims it).

Rules
-----
Source annotations (from the SGD GAF, DB column 'SGD', taxon 559292):
  evidence in (IDA, IPI, IMP, IGI); the GAF 2.2 relation must be either an
  unqualified one (enables / involved_in / part_of / located_in /
  is_active_in) or a transferable qualifier (contributes_to /
  colocalizes_with, carried onto the transferred annotation as a
  go_qualifier row, matching old-CGD behavior); NOT and acts_upstream_of*
  lines are skipped; root terms and GOIDS_TO_AVOID (e.g. protein binding)
  are skipped. When a term is supported by both unqualified and qualified
  source lines, it is transferred unqualified.
Targets:  CGD features in the same 'ortholog' homology group, either direct
  feat_homology members (pseudogenes excluded) or external dbxref members
  whose id exactly matches a current CGD ORF feature_name (this is how
  C. tropicalis genes, which are dbxref-only members, are reached).
Transferred annotation:  go_evidence='IEA', annotation_type='computational',
  source='CGD', reference CAL0121033, with/from = SGD gene (dbxref source
  'SGD', type 'Gene ID', dbxref_id = Sc systematic name).
Redundancy (skip a candidate target term when):
  - the target already has any IEA/computational/CGD annotation for the term
    (unique-key collision; the term is already asserted electronically); OR
  - the target has a curated (non-IEA) annotation for the term or a more
    specific (descendant) term.
  Among the transferred terms for a target, only the most specific are kept.

Refresh model:  each run deletes the script-owned SGD transfers (IEAs on
CAL0121033 whose with/from are all SGD dbxrefs) and recomputes them.

Usage:
    python transfer_go_from_sgd.py --dry-run --report /tmp/sgd_transfer.tsv
    python transfer_go_from_sgd.py --gaf-file /data/GO/sgd.gaf.gz
    python transfer_go_from_sgd.py --organisms C_glabrata_CBS138   # one species only

Environment Variables:
    DATABASE_URL, DB_SCHEMA
"""

import argparse
import gzip
import logging
import os
import re
import sys
import urllib.request
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

SGD_GAF_URL = "http://current.geneontology.org/annotations/sgd.gaf.gz"
SGD_TAXON = "taxon:559292"

# Source annotations that qualify for transfer (experimental evidence codes)
SOURCE_EVIDENCE = {"IDA", "IPI", "IMP", "IGI"}

# GAF 2.2 relations accepted as "unqualified" (anything else is skipped)
ALLOWED_RELATIONS = {"", "enables", "involved_in", "part_of", "located_in", "is_active_in"}

# GAF 2.2 qualifiers that transfer WITH the qualifier preserved (old-CGD behavior)
TRANSFERABLE_QUALIFIERS = {"contributes_to", "colocalizes_with"}

# GO root terms (never transfer) and terms too general to be useful
GO_ROOT_GOIDS = {8150, 3674, 5575}
GOIDS_TO_AVOID = {5515, 5488}  # protein binding, binding

# Sc systematic names: nuclear (YDR298C, YML081C-A) and mitochondrial (Q0080)
SC_NAME_RE = re.compile(r"^(Y[A-P][LR][0-9]{3}[CW](-[A-Z])?|Q0[0-9]{3})$")

# Transferred (IEA) annotation attributes
XFER_EVIDENCE = "IEA"
XFER_ANNOTATION_TYPE = "computational"
XFER_SOURCE = "CGD"
ORTHOLOG_REF_DBXREF = "CAL0121033"  # "CGD Prediction of GO annotations based on orthology"

# with/from dbxref attributes for the Sc source genes
WITH_DBXREF_SOURCE = "SGD"
WITH_DBXREF_TYPE = "Gene ID"

FEATURE_TYPES_TO_AVOID = {"pseudogene"}

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


def download_gaf(dest: Path) -> Path:
    logger.info("Downloading SGD GAF from %s ...", SGD_GAF_URL)
    urllib.request.urlretrieve(SGD_GAF_URL, dest)
    logger.info("Saved %s (%d bytes)", dest, dest.stat().st_size)
    return dest


def read_sgd_gaf(path: Path):
    """Parse the SGD GAF.

    Returns (annotations, evidence, symbols, qualifiers, unqualified_pairs):
      annotations: sc systematic name -> set(goid int)
      evidence:    (sysname, goid) -> set(evidence code)
      symbols:     sysname -> gene symbol (for dbxref description / report)
      qualifiers:  (sysname, goid) -> set(transferable qualifier)
      unqualified_pairs: set of (sysname, goid) seen on an unqualified line
    """
    annotations: dict[str, set[int]] = defaultdict(set)
    evidence: dict[tuple[str, int], set[str]] = defaultdict(set)
    symbols: dict[str, str] = {}
    qualifiers: dict[tuple[str, int], set[str]] = defaultdict(set)
    unqualified_pairs: set[tuple[str, int]] = set()
    stats = defaultdict(int)

    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt") as f:
        for line in f:
            if line.startswith("!"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 13 or parts[0] != "SGD":
                continue
            stats["lines"] += 1
            qualifier, goid_str, ev, taxon = parts[3], parts[4], parts[6], parts[12]
            if ev not in SOURCE_EVIDENCE:
                stats["skip_evidence"] += 1
                continue
            if qualifier not in ALLOWED_RELATIONS and qualifier not in TRANSFERABLE_QUALIFIERS:
                stats["skip_qualifier"] += 1
                continue
            if not taxon.startswith(SGD_TAXON):
                stats["skip_taxon"] += 1
                continue
            try:
                goid = int(goid_str.removeprefix("GO:"))
            except ValueError:
                stats["skip_bad_goid"] += 1
                continue
            if goid in GO_ROOT_GOIDS or goid in GOIDS_TO_AVOID:
                stats["skip_root_or_avoided"] += 1
                continue
            # systematic name: check symbol column then synonyms (usually first)
            symbol = parts[2]
            sysname = None
            for cand in [symbol] + parts[10].split("|"):
                if SC_NAME_RE.match(cand):
                    sysname = cand
                    break
            if not sysname:
                stats["skip_no_sysname"] += 1
                continue
            annotations[sysname].add(goid)
            evidence[(sysname, goid)].add(ev)
            if qualifier in TRANSFERABLE_QUALIFIERS:
                qualifiers[(sysname, goid)].add(qualifier)
                stats["kept_qualified"] += 1
            else:
                unqualified_pairs.add((sysname, goid))
                stats["kept"] += 1
            if symbol and symbol != sysname:
                symbols[sysname] = symbol

    logger.info("SGD GAF: %s", dict(stats))
    logger.info("Sc genes with qualifying experimental GO: %d", len(annotations))
    return annotations, evidence, symbols, qualifiers, unqualified_pairs


def get_sc_members(session) -> dict[str, set[int]]:
    """Sc systematic name -> set(homology_group_no) for 'ortholog' groups."""
    rows = session.execute(text(f"""
        SELECT d.dbxref_id, dh.homology_group_no
        FROM {DB_SCHEMA}.dbxref_homology dh
        JOIN {DB_SCHEMA}.dbxref d ON d.dbxref_no = dh.dbxref_no
        JOIN {DB_SCHEMA}.homology_group hg
          ON hg.homology_group_no = dh.homology_group_no
         AND hg.homology_group_type = 'ortholog'
        WHERE REGEXP_LIKE(d.dbxref_id, '^Y[A-P][LR][0-9]{{3}}[CW](-[A-Z])?$')
           OR REGEXP_LIKE(d.dbxref_id, '^Q0[0-9]{{3}}$')
    """)).fetchall()
    out: dict[str, set[int]] = defaultdict(set)
    for dbxref_id, hg_no in rows:
        out[dbxref_id].add(hg_no)
    logger.info("Sc genes in ortholog groups: %d (groups: %d)",
                len(out), len({g for gs in out.values() for g in gs}))
    return out


def get_group_targets(session, organisms: list[str] | None) -> dict[int, set[int]]:
    """homology_group_no -> set(target feature_no).

    Direct feat_homology members (pseudogenes and old-assembly names excluded),
    plus external dbxref members resolved by exact current-ORF feature_name
    match (covers C. tropicalis, whose genes are dbxref-only group members).
    Restricted to the given organisms when a filter is active.
    """
    targets: dict[int, set[int]] = defaultdict(set)
    org_sql, org_binds = _organism_filter(organisms, "f")

    rows = session.execute(text(f"""
        SELECT fh.homology_group_no, fh.feature_no
        FROM {DB_SCHEMA}.feat_homology fh
        JOIN {DB_SCHEMA}.homology_group hg
          ON hg.homology_group_no = fh.homology_group_no
         AND hg.homology_group_type = 'ortholog'
        JOIN {DB_SCHEMA}.feature f ON f.feature_no = fh.feature_no
        WHERE f.feature_type NOT IN ({", ".join(f"'{t}'" for t in FEATURE_TYPES_TO_AVOID)})
          AND UPPER(f.feature_name) NOT LIKE 'ORF19.%'
          AND UPPER(f.feature_name) NOT LIKE 'ORF21.%'{org_sql}
    """), org_binds).fetchall()
    n_direct = 0
    for hg_no, feat_no in rows:
        targets[hg_no].add(feat_no)
        n_direct += 1

    rows = session.execute(text(f"""
        SELECT dh.homology_group_no, f.feature_no
        FROM {DB_SCHEMA}.dbxref_homology dh
        JOIN {DB_SCHEMA}.dbxref d ON d.dbxref_no = dh.dbxref_no
        JOIN {DB_SCHEMA}.homology_group hg
          ON hg.homology_group_no = dh.homology_group_no
         AND hg.homology_group_type = 'ortholog'
        JOIN {DB_SCHEMA}.feature f ON f.feature_name = d.dbxref_id
        WHERE f.feature_type = 'ORF'
          AND UPPER(f.feature_name) NOT LIKE 'ORF19.%'
          AND UPPER(f.feature_name) NOT LIKE 'ORF21.%'{org_sql}
    """), org_binds).fetchall()
    n_by_name = 0
    for hg_no, feat_no in rows:
        if feat_no not in targets[hg_no]:
            targets[hg_no].add(feat_no)
            n_by_name += 1

    logger.info("Group targets: %d direct members, %d added by dbxref name match",
                n_direct, n_by_name)
    return targets


def get_go_no_map(session, goids: set[int]) -> dict[int, int]:
    """goid (int) -> go_no for goids present in the GO table."""
    out: dict[int, int] = {}
    goid_list = list(goids)
    for i in range(0, len(goid_list), 900):
        chunk = goid_list[i:i + 900]
        binds = {f"g{j}": v for j, v in enumerate(chunk)}
        inlist = ", ".join(f":{k}" for k in binds)
        rows = session.execute(
            text(f"SELECT goid, go_no FROM {DB_SCHEMA}.go WHERE goid IN ({inlist})"), binds
        ).fetchall()
        for goid, go_no in rows:
            out[int(goid)] = go_no
    return out


def get_existing_target_annotations(session):
    """(already_iea, curated_go) for redundancy checks (see companion script)."""
    already_iea: set[tuple[int, int]] = set()
    curated_go: dict[int, set[int]] = defaultdict(set)
    rows = session.execute(text(f"""
        SELECT feature_no, go_no, go_evidence, annotation_type, source
        FROM {DB_SCHEMA}.go_annotation
    """)).fetchall()
    for feat_no, go_no, ev, atype, src in rows:
        if ev == XFER_EVIDENCE and atype == XFER_ANNOTATION_TYPE and src == XFER_SOURCE:
            already_iea.add((feat_no, go_no))
        elif ev != XFER_EVIDENCE:
            curated_go[feat_no].add(go_no)
    return already_iea, curated_go


def get_descendants(session, go_nos: set[int]) -> dict[int, set[int]]:
    """ancestor go_no -> set(descendant go_no), limited to given ancestors.

    Restricted to SAME-ASPECT descendants: go_path includes cross-aspect
    part_of links (e.g. MF 'ATP synthase activity' under BP 'ATP synthesis'),
    which must not make a BP transfer redundant with an MF one -- old CGD
    transferred both aspects independently.
    """
    desc: dict[int, set[int]] = defaultdict(set)
    go_list = list(go_nos)
    for i in range(0, len(go_list), 900):
        chunk = go_list[i:i + 900]
        binds = {f"g{j}": v for j, v in enumerate(chunk)}
        inlist = ", ".join(f":{k}" for k in binds)
        rows = session.execute(text(f"""
            SELECT gp.ancestor_go_no, gp.child_go_no
            FROM {DB_SCHEMA}.go_path gp
            JOIN {DB_SCHEMA}.go ga ON ga.go_no = gp.ancestor_go_no
            JOIN {DB_SCHEMA}.go gc ON gc.go_no = gp.child_go_no
            WHERE gp.ancestor_go_no IN ({inlist})
              AND ga.go_aspect = gc.go_aspect
        """), binds).fetchall()
        for anc, child in rows:
            desc[anc].add(child)
    return desc


def nextval(session, seq: str) -> int:
    return session.execute(text(f"SELECT {DB_SCHEMA}.{seq}.NEXTVAL FROM dual")).scalar()


def _organism_filter(organisms: list[str] | None, feature_alias: str) -> tuple[str, dict]:
    """SQL fragment + binds restricting a feature alias to the given organisms."""
    if not organisms:
        return "", {}
    binds = {f"org{i}": o for i, o in enumerate(organisms)}
    inlist = ", ".join(f":{k}" for k in binds)
    sql = f"""
          AND {feature_alias}.organism_no IN (
              SELECT organism_no FROM {DB_SCHEMA}.organism WHERE organism_abbrev IN ({inlist}))"""
    return sql, binds


def delete_owned_transfers(session, organisms: list[str] | None) -> int:
    """Delete script-owned SGD transfers: IEA/computational/CGD annotations on
    ref CAL0121033 whose with/from dbxrefs are ALL SGD (no non-SGD), limited
    to the given organisms when a filter is active."""
    ref_no = resolve_reference_no(session)
    org_sql, org_binds = _organism_filter(organisms, "f")
    rows = session.execute(text(f"""
        SELECT ga.go_annotation_no
        FROM {DB_SCHEMA}.go_annotation ga
        JOIN {DB_SCHEMA}.go_ref gr ON ga.go_annotation_no = gr.go_annotation_no
        JOIN {DB_SCHEMA}.feature f ON f.feature_no = ga.feature_no
        WHERE ga.go_evidence = :ev AND ga.annotation_type = :at AND ga.source = :src
          AND gr.reference_no = :ref
          AND EXISTS (
              SELECT 1 FROM {DB_SCHEMA}.goref_dbxref gd
              JOIN {DB_SCHEMA}.dbxref d ON gd.dbxref_no = d.dbxref_no
              WHERE gd.go_ref_no = gr.go_ref_no AND d.source = :ws)
          AND NOT EXISTS (
              SELECT 1 FROM {DB_SCHEMA}.goref_dbxref gd
              JOIN {DB_SCHEMA}.dbxref d ON gd.dbxref_no = d.dbxref_no
              WHERE gd.go_ref_no = gr.go_ref_no AND d.source <> :ws){org_sql}
    """), {"ev": XFER_EVIDENCE, "at": XFER_ANNOTATION_TYPE, "src": XFER_SOURCE,
           "ref": ref_no, "ws": WITH_DBXREF_SOURCE, **org_binds}).fetchall()
    ann_nos = [r[0] for r in rows]
    if not ann_nos:
        return 0
    for i in range(0, len(ann_nos), 900):
        chunk = ann_nos[i:i + 900]
        binds = {f"a{j}": v for j, v in enumerate(chunk)}
        inlist = ", ".join(f":{k}" for k in binds)
        session.execute(
            text(f"DELETE FROM {DB_SCHEMA}.go_annotation WHERE go_annotation_no IN ({inlist})"),
            binds,
        )
    return len(ann_nos)


def build_transfers(session, sc_annotations, qualifiers, unqualified_pairs,
                    organisms: list[str] | None):
    """Compute transfers after redundancy rules.

    Returns (transfers, transfer_quals):
      transfers:      {(target_feature, go_no): set(sc sysname)}
      transfer_quals: {(target_feature, go_no): set(qualifier)} -- empty set
                      when any contributing source line was unqualified.
    """
    sc_members = get_sc_members(session)
    group_targets = get_group_targets(session, organisms)

    all_goids = {g for gos in sc_annotations.values() for g in gos}
    go_no_map = get_go_no_map(session, all_goids)
    logger.info("GOIDs in GAF: %d, resolved in GO table: %d (missing/obsolete: %d)",
                len(all_goids), len(go_no_map), len(all_goids) - len(go_no_map))

    cand: dict[int, dict[int, set[str]]] = defaultdict(lambda: defaultdict(set))
    cand_quals: dict[tuple[int, int], set[str]] = defaultdict(set)
    cand_unqual: set[tuple[int, int]] = set()
    for sysname, groups in sc_members.items():
        src_gos = sc_annotations.get(sysname)
        if not src_gos:
            continue
        resolved = [(g, go_no_map[g]) for g in src_gos if g in go_no_map]
        for hg_no in groups:
            for tgt_feat in group_targets.get(hg_no, ()):
                for goid, go_no in resolved:
                    cand[tgt_feat][go_no].add(sysname)
                    if (sysname, goid) in unqualified_pairs:
                        cand_unqual.add((tgt_feat, go_no))
                    else:
                        cand_quals[(tgt_feat, go_no)].update(qualifiers[(sysname, goid)])
    logger.info("Candidate target genes: %d", len(cand))

    already_iea, curated_go = get_existing_target_annotations(session)

    all_cand_gos: set[int] = set()
    for gomap in cand.values():
        all_cand_gos.update(gomap.keys())
    descendants = get_descendants(session, all_cand_gos)

    transfers: dict[tuple[int, int], set[str]] = {}
    stats = {"skip_existing_iea": 0, "skip_curated": 0, "skip_less_specific": 0, "kept": 0}
    for tgt_feat, gomap in cand.items():
        tgt_curated = curated_go.get(tgt_feat, set())
        go_nos = set(gomap.keys())
        for go_no in go_nos:
            if (tgt_feat, go_no) in already_iea:
                stats["skip_existing_iea"] += 1
                continue
            desc = descendants.get(go_no, set())
            if go_no in tgt_curated or (tgt_curated & desc):
                stats["skip_curated"] += 1
                continue
            if desc & (go_nos - {go_no}):
                stats["skip_less_specific"] += 1
                continue
            transfers[(tgt_feat, go_no)] = gomap[go_no]
            stats["kept"] += 1

    transfer_quals = {
        key: (set() if key in cand_unqual else cand_quals.get(key, set()))
        for key in transfers
    }
    n_qualified = sum(1 for q in transfer_quals.values() if q)
    stats["kept_with_qualifier"] = n_qualified
    logger.info("Transfer decisions: %s", stats)
    return transfers, transfer_quals


def get_or_create_sgd_dbxrefs(session, sysnames: set[str], symbols: dict[str, str],
                              created_by: str, dry_run: bool) -> dict[str, int]:
    """sysname -> dbxref_no for source='SGD' with/from rows, creating as needed.

    Existing SGD 'Gene ID' rows may be keyed by gene symbol (legacy best-hit
    rows), so both the systematic name and the symbol are checked before
    creating a new row keyed by systematic name.
    """
    rows = session.execute(text(f"""
        SELECT dbxref_id, dbxref_no FROM {DB_SCHEMA}.dbxref
        WHERE source = :s AND dbxref_type = :t
    """), {"s": WITH_DBXREF_SOURCE, "t": WITH_DBXREF_TYPE}).fetchall()
    existing = {dbxref_id: dbxref_no for dbxref_id, dbxref_no in rows}

    out: dict[str, int] = {}
    created = 0
    for sysname in sorted(sysnames):
        dbxref_no = existing.get(sysname) or existing.get(symbols.get(sysname, ""))
        if dbxref_no:
            out[sysname] = dbxref_no
            continue
        created += 1
        if dry_run:
            out[sysname] = -created  # placeholder; rolled back anyway
            continue
        dbxref_no = nextval(session, "dbxref_seq")
        session.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.dbxref
                (dbxref_no, source, dbxref_type, dbxref_id, description, date_created, created_by)
            VALUES (:n, :s, :t, :id, :d, SYSDATE, :cb)
        """), {"n": dbxref_no, "s": WITH_DBXREF_SOURCE, "t": WITH_DBXREF_TYPE,
               "id": sysname, "d": symbols.get(sysname), "cb": created_by})
        out[sysname] = dbxref_no
    logger.info("with/from dbxrefs: %d reused, %d %s", len(out) - created, created,
                "would be created" if dry_run else "created")
    return out


def insert_transfers(session, transfers, transfer_quals, dbxref_map, ref_no, created_by) -> dict:
    stats = {"annotations": 0, "with_from_links": 0, "qualifiers": 0}
    for (tgt_feat, go_no), sources in transfers.items():
        quals = transfer_quals.get((tgt_feat, go_no), set())
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
            VALUES (:n, :ref, :ann, :hq, 'Y', :cb)
        """), {"n": ref_row, "ref": ref_no, "ann": ann_no,
               "hq": "Y" if quals else "N", "cb": created_by})

        for qual in sorted(quals):
            gq_no = nextval(session, "go_qualifier_seq")
            session.execute(text(f"""
                INSERT INTO {DB_SCHEMA}.go_qualifier (go_qualifier_no, go_ref_no, qualifier)
                VALUES (:n, :gr, :q)
            """), {"n": gq_no, "gr": ref_row, "q": qual})
            stats["qualifiers"] += 1

        for sysname in sorted(sources):
            gd_no = nextval(session, "goref_dbxref_seq")
            session.execute(text(f"""
                INSERT INTO {DB_SCHEMA}.goref_dbxref (goref_dbxref_no, go_ref_no, dbxref_no, support_type)
                VALUES (:n, :gr, :dx, 'With')
            """), {"n": gd_no, "gr": ref_row, "dx": dbxref_map[sysname]})
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
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
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


def write_report(session, transfers, transfer_quals, evidence, symbols, path: str) -> int:
    """Write a TSV of every proposed transfer (one row per source->target,GO)."""
    feats = {t for t, _ in transfers}
    gos = {go for _, go in transfers}
    finfo = _feature_info(session, feats)
    ginfo = _go_info(session, gos)

    header = ["Source_Gene", "Source_Name", "Source_Evidence", "GOID", "GO_Term", "Aspect",
              "Qualifier", "Target_Gene", "Target_Name", "Target_CGDID", "Target_Species",
              "Transferred_Evidence", "Reference"]
    rows = []
    for (tgt, go_no), sources in transfers.items():
        tg = finfo.get(tgt)
        gg = ginfo.get(go_no)
        if not tg or not gg:
            continue
        goid, term, aspect = gg
        goid_int = int(goid.removeprefix("GO:"))
        qual = ",".join(sorted(transfer_quals.get((tgt, go_no), set())))
        for sysname in sorted(sources):
            ev = ",".join(sorted(evidence.get((sysname, goid_int), {"?"})))
            rows.append([sysname, symbols.get(sysname, ""), ev, goid, term, aspect, qual,
                         tg[0], tg[1], tg[2], tg[3], XFER_EVIDENCE,
                         f"CGD_REF:{ORTHOLOG_REF_DBXREF}"])
    rows.sort(key=lambda r: (r[10], r[7], r[3]))
    with open(path, "w") as f:
        f.write("\t".join(header) + "\n")
        for r in rows:
            f.write("\t".join("" if c is None else str(c) for c in r) + "\n")
    return len(rows)


def log_species_summary(session, transfers) -> None:
    finfo = _feature_info(session, {t for t, _ in transfers})
    per_species_ann: dict[str, int] = defaultdict(int)
    per_species_genes: dict[str, set[int]] = defaultdict(set)
    for (tgt, _go), _src in transfers.items():
        org = finfo.get(tgt, ("", "", "", "?"))[3]
        per_species_ann[org] += 1
        per_species_genes[org].add(tgt)
    for org in sorted(per_species_ann):
        logger.info("  %s: %d annotations on %d genes",
                    org, per_species_ann[org], len(per_species_genes[org]))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Transfer experimental SGD GO annotations to CGD orthologs as IEA")
    parser.add_argument("--dry-run", action="store_true", help="Compute + report; no DB changes")
    parser.add_argument("--report", help="Write a TSV of proposed transfers to this path")
    parser.add_argument("--gaf-file", help="Local SGD GAF (default: download from GO Consortium)")
    parser.add_argument("--organisms",
                        help="Comma-separated organism_abbrev list to restrict targets "
                             "(delete+recompute are both scoped); default: all species")
    parser.add_argument("--created-by", default=os.getenv("ADMIN_USER", "SWENG").upper()[:12])
    args = parser.parse_args()
    organisms = [o.strip() for o in args.organisms.split(",") if o.strip()] if args.organisms else None

    logger.info("=" * 60)
    logger.info("Transfer SGD experimental GO -> CGD orthologs (IEA)")
    if args.dry_run:
        logger.info("[DRY RUN] no database changes")
    if organisms:
        logger.info("Organism filter: %s", ", ".join(organisms))
    logger.info("created_by=%s started=%s", args.created_by, datetime.now())
    logger.info("=" * 60)

    gaf_path = Path(args.gaf_file) if args.gaf_file else download_gaf(Path("/tmp/sgd.gaf.gz"))
    sc_annotations, evidence, symbols, qualifiers, unqualified_pairs = read_sgd_gaf(gaf_path)

    try:
        with SessionLocal() as session:
            ref_no = resolve_reference_no(session)
            logger.info("Ortholog reference %s = reference_no %d", ORTHOLOG_REF_DBXREF, ref_no)

            # Delete the script-owned SGD transfers FIRST (in this transaction)
            # so the recompute below does not see them as pre-existing IEAs --
            # makes the replace idempotent. In --dry-run this is rolled back.
            deleted = delete_owned_transfers(session, organisms)
            logger.info("%s %d existing SGD transfer annotations",
                        "Would delete" if args.dry_run else "Deleted", deleted)

            transfers, transfer_quals = build_transfers(
                session, sc_annotations, qualifiers, unqualified_pairs, organisms)
            logger.info("Transfers to apply: %d annotations across %d target genes",
                        len(transfers), len({t for t, _ in transfers}))
            log_species_summary(session, transfers)

            if args.report:
                n = write_report(session, transfers, transfer_quals, evidence, symbols,
                                 args.report)
                logger.info("Wrote report: %s (%d rows)", args.report, n)

            sysnames = {s for srcs in transfers.values() for s in srcs}
            dbxref_map = get_or_create_sgd_dbxrefs(
                session, sysnames, symbols, args.created_by, args.dry_run)

            if args.dry_run:
                wf = sum(len(s) for s in transfers.values())
                logger.info("Would insert %d annotations, %d with/from links",
                            len(transfers), wf)
                session.rollback()
            else:
                ins = insert_transfers(session, transfers, transfer_quals, dbxref_map,
                                       ref_no, args.created_by)
                session.commit()
                logger.info("Inserted: %s", ins)
    except Exception as e:  # noqa: BLE001
        logger.exception("Failed: %s", e)
        return 1

    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
