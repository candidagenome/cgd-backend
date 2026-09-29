#!/usr/bin/env python3
"""
QC the frozen PomBase GO-transfer IEAs against PomBase's curated ortholog table.

Background (XOG1 ticket, 2026-09-28): CGD's legacy PomBase GO transfers
(IEA/computational/CGD on ref CAL0121033, with/from a POMBASE 'Gene ID'
dbxref) came from a synteny-blind ortholog mapping. For the XOG1 family the
pombe partner chosen was exg3 (SPBC1105.05), a glucan endo-1,6-beta-
glucosidase that PomBase lists only as a co-ortholog of Sc EXG1 alongside
exg1 — the transferred terms then contradicted the family's characterized
exo-1,3 activity. These transfers are frozen (no script refreshes them), so
each bad pairing found here can simply be deleted.

For every (CGD gene, pombe with/from gene) pair backing at least one IEA,
the pair is classified against PomBase's pombe<->S. cerevisiae ortholog
table, using the gene's own S. cerevisiae ortholog(s) (Y-number dbxrefs in
its CGD 'ortholog' homology groups) as the bridge:

  NOT_ENDORSED           pombe gene's PomBase Sc-ortholog set does not
                         contain any of the gene's own Sc orthologs (or the
                         pombe gene is absent from the table) — the pairing
                         has no PomBase support; strongest deletion candidates.
  CO_ORTHOLOG_AMBIGUOUS  pairing is PomBase-endorsed, but the Sc ortholog
                         maps to MULTIPLE pombe co-orthologs — the legacy
                         pipeline picked one of several twins (the XOG1/exg3
                         case). Review where pombe_only_mf_terms is non-empty:
                         those molecular functions are asserted by nothing
                         but the pombe pick.
  CONSISTENT_UNIQUE      endorsed and one-to-one — no action needed.
  NO_SC_ORTHOLOG         gene has no Sc ortholog to check against — cannot
                         be validated by this method.

pombe_only_mf_terms lists MF terms on the gene asserted ONLY by the pombe
with/from (no other annotation of any source/evidence asserts the same term)
— the "misleading activity" signal that fed the XOG1 description bug.

Each row carries a RECOMMENDATION (DELETE / REVIEW) with CONFIDENCE and a
per-row REASON, plus empty DECISION/NOTES columns for the curator. Deleting
a pair removes only that pair's IEAs; terms corroborated by SGD/intra-CGD
transfers or curation survive on their own annotations, so for endorsed-but-
ambiguous pairs a delete loses nothing except the uncorroborated claims.

Inputs:
    https://www.pombase.org/data/orthologs/pombe-cerevisiae-orthologs.tsv
    https://www.pombase.org/data/names_and_identifiers/gene_IDs_names.tsv
        (optional, --pombase-names: adds the pombe common name, e.g. exg3)

Usage:
    python qc_pombase_ortholog_iea.py \
        --pombase-orthologs pombe-cerevisiae-orthologs.tsv \
        --pombase-names gene_IDs_names.tsv \
        --output pombase_iea_qc.tsv
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal  # noqa: E402

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
SC_NAME = re.compile(r"^Y[A-P][LR]\d{3}[CW](-[A-Z])?$")

STATUS_ORDER = {"NOT_ENDORSED": 0, "CO_ORTHOLOG_AMBIGUOUS": 1,
                "NO_SC_ORTHOLOG": 2, "CONSISTENT_UNIQUE": 3}


def load_pombase_table(path):
    """Return (pombe -> set(Sc), Sc -> set(pombe)) from the PomBase TSV."""
    p2s, s2p = defaultdict(set), defaultdict(set)
    with open(path) as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            pombe, sc = parts[0].strip(), parts[1].strip()
            if not pombe or not SC_NAME.match(sc):
                continue
            p2s[pombe].add(sc)
            s2p[sc].add(pombe)
    return p2s, s2p


def load_pombe_names(path):
    """systematic id -> primary gene name, from PomBase gene_IDs_names.tsv."""
    names = {}
    with open(path) as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2 and parts[1].strip():
                names[parts[0].strip()] = parts[1].strip()
    return names


def recommend(status, pombe_label, sc_label, pombase_sc, twins, pombe_only_mf):
    """Return (recommendation, confidence, reason) for one (gene, pombe) pair."""
    if status == "NOT_ENDORSED":
        where = (f"PomBase maps {pombe_label} to {'|'.join(sorted(pombase_sc))}"
                 if pombase_sc else f"PomBase has no S. cerevisiae ortholog for {pombe_label}")
        if pombe_only_mf:
            return ("DELETE", "HIGH",
                    f"{where}, not to this gene's ortholog {sc_label}; and its "
                    f"transferred activity ({'; '.join(pombe_only_mf)}) is supported "
                    f"by no other annotation on this gene")
        return ("DELETE", "HIGH",
                f"{where}, not to this gene's ortholog {sc_label}; pairing has no "
                f"PomBase support (terms it transferred are corroborated elsewhere "
                f"or are process/component only)")
    if status == "CO_ORTHOLOG_AMBIGUOUS":
        twin_txt = ", ".join(f"{sc} ↔ {'|'.join(sorted(ps))}" for sc, ps in twins)
        if pombe_only_mf:
            return ("DELETE", "MEDIUM",
                    f"{pombe_label} is only one of several pombe co-orthologs "
                    f"({twin_txt}) — the synteny-blind legacy pick may be the wrong "
                    f"twin (the XOG1/exg3 pattern), and its activity claim "
                    f"({'; '.join(pombe_only_mf)}) is supported by no other "
                    f"annotation on this gene; deleting removes only these "
                    f"uncorroborated terms")
        return ("REVIEW", "LOW",
                f"co-ortholog pick ({twin_txt}) but every transferred term is "
                f"corroborated by another source — harmless unless the pairing "
                f"itself is wrong")
    if status == "NO_SC_ORTHOLOG":
        return ("REVIEW", "LOW",
                "gene has no S. cerevisiae ortholog in CGD, so the pombe pairing "
                "cannot be checked against PomBase's table")
    return ("KEEP", "HIGH",
            "PomBase endorses this pairing one-to-one")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--pombase-orthologs", required=True,
                    help="PomBase pombe-cerevisiae-orthologs.tsv")
    ap.add_argument("--pombase-names",
                    help="PomBase gene_IDs_names.tsv (adds pombe common names)")
    ap.add_argument("--output", required=True, help="output TSV")
    args = ap.parse_args()

    p2s, s2p = load_pombase_table(args.pombase_orthologs)
    pombe_names = load_pombe_names(args.pombase_names) if args.pombase_names else {}
    print(f"PomBase table: {len(p2s)} pombe genes, {len(s2p)} Sc genes, "
          f"{len(pombe_names)} names")

    with SessionLocal() as session:
        def q(sql, **kw):
            return session.execute(text(sql), kw).fetchall()

        # (feature, pombe with/from) pairs and their transferred terms
        rows = q(f"""
            SELECT f.feature_no, f.feature_name, f.gene_name, o.organism_name,
                   d.dbxref_id, g.go_no, g.go_term, g.go_aspect
            FROM {DB_SCHEMA}.go_annotation ga
            JOIN {DB_SCHEMA}.go g ON g.go_no = ga.go_no
            JOIN {DB_SCHEMA}.feature f ON f.feature_no = ga.feature_no
            JOIN {DB_SCHEMA}.organism o ON o.organism_no = f.organism_no
            JOIN {DB_SCHEMA}.go_ref gr ON gr.go_annotation_no = ga.go_annotation_no
            JOIN {DB_SCHEMA}.goref_dbxref gd ON gd.go_ref_no = gr.go_ref_no
            JOIN {DB_SCHEMA}.dbxref d ON d.dbxref_no = gd.dbxref_no
            WHERE ga.go_evidence = 'IEA' AND ga.annotation_type = 'computational'
              AND ga.source = 'CGD' AND d.source = 'POMBASE'
        """)

        pairs = defaultdict(lambda: {"terms": []})
        feat_info = {}
        for fno, fname, gname, org, pombe, go_no, term, aspect in rows:
            pairs[(fno, pombe)]["terms"].append((go_no, term, aspect))
            feat_info[fno] = (fname, gname, org)
        fnos = sorted({fno for fno, _ in pairs})
        print(f"{len(pairs)} (gene, pombe) pairs on {len(fnos)} genes")

        # per-feature: terms corroborated independently of the pombe pairings —
        # any annotation with no POMBASE with/from at all, or a mixed one that
        # also cites a non-POMBASE source (e.g. an EBI InterPro domain).
        other_terms = defaultdict(set)
        for i in range(0, len(fnos), 900):
            chunk = fnos[i:i + 900]
            binds = {f"f{j}": v for j, v in enumerate(chunk)}
            inlist = ", ".join(f":{k}" for k in binds)
            for fno, go_no in q(f"""
                SELECT ga.feature_no, ga.go_no
                FROM {DB_SCHEMA}.go_annotation ga
                WHERE ga.feature_no IN ({inlist})
                  AND (NOT EXISTS (
                           SELECT 1 FROM {DB_SCHEMA}.go_ref gr
                           JOIN {DB_SCHEMA}.goref_dbxref gd ON gd.go_ref_no = gr.go_ref_no
                           JOIN {DB_SCHEMA}.dbxref d ON d.dbxref_no = gd.dbxref_no
                           WHERE gr.go_annotation_no = ga.go_annotation_no
                             AND d.source = 'POMBASE')
                       OR EXISTS (
                           SELECT 1 FROM {DB_SCHEMA}.go_ref gr
                           JOIN {DB_SCHEMA}.goref_dbxref gd ON gd.go_ref_no = gr.go_ref_no
                           JOIN {DB_SCHEMA}.dbxref d ON d.dbxref_no = gd.dbxref_no
                           WHERE gr.go_annotation_no = ga.go_annotation_no
                             AND d.source <> 'POMBASE'))
            """, **binds):
                other_terms[fno].add(go_no)

        # per-feature Sc orthologs: Y-number dbxrefs in its ortholog groups
        sc_of = defaultdict(set)
        for i in range(0, len(fnos), 900):
            chunk = fnos[i:i + 900]
            binds = {f"f{j}": v for j, v in enumerate(chunk)}
            inlist = ", ".join(f":{k}" for k in binds)
            for fno, dbid in q(f"""
                SELECT fh.feature_no, d.dbxref_id
                FROM {DB_SCHEMA}.feat_homology fh
                JOIN {DB_SCHEMA}.homology_group hg
                  ON hg.homology_group_no = fh.homology_group_no
                     AND hg.homology_group_type = 'ortholog'
                JOIN {DB_SCHEMA}.dbxref_homology dh
                  ON dh.homology_group_no = hg.homology_group_no
                JOIN {DB_SCHEMA}.dbxref d ON d.dbxref_no = dh.dbxref_no
                WHERE fh.feature_no IN ({inlist})
            """, **binds):
                if SC_NAME.match(dbid or ""):
                    sc_of[fno].add(dbid)

    out_rows = []
    counts = defaultdict(int)
    for (fno, pombe), data in pairs.items():
        fname, gname, org = feat_info[fno]
        sc_set = sc_of.get(fno, set())
        pombase_sc = p2s.get(pombe, set())
        endorsed = sc_set & pombase_sc
        if not sc_set:
            status = "NO_SC_ORTHOLOG"
        elif not endorsed:
            status = "NOT_ENDORSED"
        elif any(len(s2p.get(sc, set())) > 1 for sc in endorsed):
            status = "CO_ORTHOLOG_AMBIGUOUS"
        else:
            status = "CONSISTENT_UNIQUE"
        pombe_only_mf = sorted(t for g_no, t, a in data["terms"]
                               if a == "F" and g_no not in other_terms[fno])
        pombe_label = (f"{pombe_names[pombe]} ({pombe})"
                       if pombe in pombe_names else pombe)
        twins = [(sc, s2p[sc]) for sc in sorted(endorsed)
                 if len(s2p.get(sc, set())) > 1]
        twins = [(sc, {pombe_names.get(p, p) for p in ps}) for sc, ps in twins]
        rec, conf, reason = recommend(status, pombe_label,
                                      "|".join(sorted(sc_set)) or "(none)",
                                      pombase_sc, twins, pombe_only_mf)
        counts[status] += 1
        out_rows.append({
            "status": status,
            "recommendation": rec,
            "confidence": conf,
            "organism": org,
            "feature_name": fname,
            "gene_name": gname or "",
            "pombe_gene": pombe,
            "pombe_gene_name": pombe_names.get(pombe, ""),
            "cgd_sc_orthologs": "|".join(sorted(sc_set)),
            "pombase_sc_orthologs": "|".join(sorted(pombase_sc)),
            "n_terms": len(data["terms"]),
            "pombe_only_mf_terms": "|".join(pombe_only_mf),
            "reason": reason,
            "terms": "|".join(sorted(t for _, t, _ in data["terms"])),
            "DECISION": "",
            "NOTES": "",
        })

    out_rows.sort(key=lambda r: (STATUS_ORDER[r["status"]],
                                 -len(r["pombe_only_mf_terms"]),
                                 r["organism"], r["feature_name"]))
    with open(args.output, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(out_rows[0].keys()), delimiter="\t")
        w.writeheader()
        w.writerows(out_rows)

    print(f"\nwrote {len(out_rows)} rows to {args.output}")
    for st in STATUS_ORDER:
        n_mf = sum(1 for r in out_rows if r["status"] == st and r["pombe_only_mf_terms"])
        print(f"  {st}: {counts[st]} pairs ({n_mf} with pombe-only MF terms)")


if __name__ == "__main__":
    main()
