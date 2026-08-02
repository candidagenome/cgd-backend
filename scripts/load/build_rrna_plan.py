#!/usr/bin/env python3
"""Build an rRNA load plan (JSON) from a barrnap GFF for one species.

Subtask-2 of the "missing non-coding features" project. C. tropicalis (org 12)
and C. dubliniensis CD36 (org 9) had rRNA=0 (genuinely absent, not mislabeled),
so their ribosomal RNAs are predicted de novo with barrnap and inserted by
load_rrnas.py. This script turns the barrnap GFF into a load plan.

Upstream step (barrnap 0.9), run on contig sequences dumped from CGD so the
coordinates map 1:1 onto CGD contigs:
    barrnap --kingdom euk --threads 4 <species>_contigs.fna > <species>.rrna.gff

Applies CGD naming + the coordinate policy:
  - 18S/5.8S/5S: barrnap coords (dubliniensis 18S/5.8S snapped to the curator
                 ITS1/ITS2 anchors already in CGD)
  - 25S: start = 5.8S_end + ITS2  (dubliniensis: exact ITS2 anchor;
                                   tropicalis: per-unit measured ITS1 as proxy),
         end = barrnap 28S end, capped at the longest sister-species 25S (3408 bp)
         to trim barrnap's LSU 3' envelope overshoot.

Naming is PROVISIONAL (curator review item): dubliniensis reuses the reserved
Cd36_3435x systematic slots; tropicalis (no locus-tag source) uses RDN<n>-<copy>.

Usage:
    python build_rrna_plan.py --species cdub|ctrop --gff FILE \\
        --organism "<organism_name>" --out plan.json
"""
import argparse
import json
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[0]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))
from cgd.db.engine import SessionLocal
DB = os.getenv("DB_SCHEMA", "MULTI")

CLASS_HEADLINE = {
    "18S": "18S ribosomal RNA; component of the small (40S) ribosomal subunit; predicted by barrnap",
    "5_8S": "5.8S ribosomal RNA; component of the large (60S) ribosomal subunit; predicted by barrnap",
    "25S": "25S ribosomal RNA; component of the large (60S) ribosomal subunit; predicted by barrnap",
    "5S": "5S ribosomal RNA; component of the large (60S) ribosomal subunit; predicted by barrnap",
}
NAME_MAP = {"18S_rRNA": "18S", "5_8S_rRNA": "5_8S", "28S_rRNA": "25S", "5S_rRNA": "5S"}
# barrnap's LSU (28S) 3' envelope can overshoot the true 25S. Cap the 25S at the
# longest 25S observed in the curated sister species (C. glabrata RDN25 = 3408 bp).
# This never truncates a canonical 25S (tropicalis units are ~3354 bp) and only
# trims the dubliniensis overshoot.
CANONICAL_25S_MAX = 3408


def parse_barrnap(gff):
    feats = []
    for line in open(gff):
        if line.startswith("#"):
            continue
        f = line.rstrip("\n").split("\t")
        if len(f) < 9 or f[2] != "rRNA":
            continue
        attrs = dict(kv.split("=", 1) for kv in f[8].split(";") if "=" in kv)
        feats.append({"contig": f[0], "begin": int(f[3]), "end": int(f[4]),
                      "strand": f[6], "cls": NAME_MAP[attrs["Name"]]})
    return feats


def db_lookups(organism_name):
    with SessionLocal() as s:
        org = s.execute(text(f"SELECT organism_no FROM {DB}.organism WHERE organism_name=:n"),
                        {"n": organism_name}).scalar()
        src = s.execute(text(f"""SELECT DISTINCT s.source FROM {DB}.seq s JOIN {DB}.feature f
            ON f.feature_no=s.feature_no WHERE f.organism_no=:o
            AND f.feature_type IN ('contig','chromosome') AND s.seq_type='genomic'
            AND s.is_seq_current='Y'"""), {"o": org}).fetchall()
        src = src[0][0] if src else "CGD"
        its = {}
        for r in s.execute(text(f"""SELECT f.gene_name, l.start_coord, l.stop_coord
            FROM {DB}.feature f JOIN {DB}.feat_location l ON l.feature_no=f.feature_no
            AND l.is_loc_current='Y' WHERE f.organism_no=:o AND f.gene_name IN ('ITS1','ITS2')"""),
            {"o": org}):
            its[r[0]] = (min(r[1], r[2]), max(r[1], r[2]))
    return org, src, its


def group_units(feats):
    """Order by position; each 18S opens a unit that gathers the following 5.8S/25S."""
    core = sorted([f for f in feats if f["cls"] in ("18S", "5_8S", "25S")],
                  key=lambda f: (f["contig"], f["begin"]))
    units, cur = [], None
    for f in core:
        if f["cls"] == "18S":
            cur = {"18S": f}
            units.append(cur)
        elif cur is not None:
            cur[f["cls"]] = f
    return units


def build(species, gff, organism_name, out):
    feats = parse_barrnap(gff)
    org, src, its = db_lookups(organism_name)
    units = group_units(feats)
    fives = sorted([f for f in feats if f["cls"] == "5S"],
                   key=lambda f: (f["contig"], f["begin"]))
    plan = []

    def rec(name, gene, cls, contig, start, stop, strand):
        plan.append({"organism_name": organism_name, "seq_source": src,
                     "feature_name": name, "gene_name": gene, "rrna_class": cls,
                     "contig": contig, "start": start, "stop": stop,
                     "strand": strand, "headline": CLASS_HEADLINE[cls]})

    if species == "cdub":
        assert len(units) == 1, f"expected 1 dubliniensis unit, got {len(units)}"
        u = units[0]
        its1_lo, its1_hi = its["ITS1"]
        its2_lo, its2_hi = its["ITS2"]
        contig = u["18S"]["contig"]
        # 18S: snap 3' end to just before ITS1
        rec("Cd36_34353", "RDN18", "18S", contig, u["18S"]["begin"], its1_lo - 1, "W")
        # 5.8S: snap to the ITS1..ITS2 gap (canonical 158bp)
        rec("Cd36_34355", "RDN58", "5_8S", contig, its1_hi + 1, its2_lo - 1, "W")
        # 25S: start just after ITS2, end = barrnap 28S end (capped)
        start25 = its2_hi + 1
        rec("Cd36_34357", "RDN25", "25S", contig, start25,
            min(u["25S"]["end"], start25 + CANONICAL_25S_MAX - 1), "W")
        # 5S copies (minus strand) in genomic order: RDN5, RDN5-2
        names5 = [("Cd36_34352", "RDN5"), ("Cd36_34358", "RDN5-2")]
        for (nm, gn), f in zip(names5, fives):
            rec(nm, gn, "5S", f["contig"], f["begin"], f["end"], "C")

    elif species == "ctrop":
        for i, u in enumerate(units, start=1):
            contig = u["18S"]["contig"]
            its1_len = u["5_8S"]["begin"] - u["18S"]["end"] - 1  # measured ITS1
            rec(f"RDN18-{i}", f"RDN18-{i}", "18S", contig, u["18S"]["begin"], u["18S"]["end"], "W")
            rec(f"RDN58-{i}", f"RDN58-{i}", "5_8S", contig, u["5_8S"]["begin"], u["5_8S"]["end"], "W")
            # 25S start = 5.8S end + ITS2 (proxy = measured ITS1), end = barrnap 28S end (capped)
            start25 = u["5_8S"]["end"] + its1_len
            rec(f"RDN25-{i}", f"RDN25-{i}", "25S", contig, start25,
                min(u["25S"]["end"], start25 + CANONICAL_25S_MAX - 1), "W")
        for j, f in enumerate(fives, start=1):
            rec(f"RDN5-{j}", f"RDN5-{j}", "5S", f["contig"], f["begin"], f["end"], "C")
    else:
        raise SystemExit("species must be cdub|ctrop")

    Path(out).write_text(json.dumps(plan, indent=2))
    print(f"{species}: {len(units)} unit(s), wrote {len(plan)} rRNA specs -> {out}")
    for p in plan:
        span = abs(p["stop"] - p["start"]) + 1
        print(f"  {p['feature_name']:12} {p['gene_name']:10} {p['rrna_class']:5} "
              f"{p['contig']:26} {p['start']:>9}..{p['stop']:<9} {p['strand']} len={span}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--species", required=True, choices=["cdub", "ctrop"])
    ap.add_argument("--gff", required=True)
    ap.add_argument("--organism", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    build(a.species, a.gff, a.organism, a.out)
