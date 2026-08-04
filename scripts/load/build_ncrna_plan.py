#!/usr/bin/env python3
"""
Build the ncRNA insert plan for C. tropicalis (subtask-3).

Two provenance classes:
  * ITS1/ITS2 -- derived LIVE as the spacer gaps between the already-loaded rRNA
    subunits (18S end+1 .. 5.8S start-1 = ITS1; 5.8S end+1 .. 25S start-1 = ITS2),
    one pair per rDNA unit. Exact, no prediction. Requires load_rrnas.py (subtask-2)
    to have run first.
  * RPR1 (RNase P RNA) + SCR1 (SRP RNA) -- located by blastn homology to the
    C. albicans orthologs (RPR1 79% id / e-72 over 334 bp; SCR1 86% id / e-81 over
    262 bp). Coordinates are the reviewed blast-aligned spans, baked in as a
    constant with curator-reviewed CTRG_XXXX.5 systematic names.

Writes a plan JSON consumed by load_ncrnas.py.

Usage:
    python build_ncrna_plan.py --out ctrop_ncrna_plan.json
"""
import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path("scripts/load/load_ncrnas.py").resolve().parents[2]))
from sqlalchemy import text  # noqa: E402
from cgd.db.engine import SessionLocal  # noqa: E402

S = os.getenv("DB_SCHEMA", "MULTI")
ORGANISM_NAME = "Candida tropicalis MYA-3404"
SEQ_SOURCE = "C. tropicalis MYA-3404"

# rDNA units -> ITS gap definitions (from the loaded rRNA gene_names)
ITS_UNITS = [("1", "RDN18-1", "RDN58-1", "RDN25-1"),
             ("2", "RDN18-2", "RDN58-2", "RDN25-2"),
             ("3", "RDN18-3", "RDN58-3", "RDN25-3")]

# reviewed homology-based conserved ncRNAs (blast-aligned span; CGD coords)
CONSERVED = [
    {"feature_name": "CTRG_01806.5", "gene_name": "RPR1", "ncrna_class": "RNase_P_RNA",
     "contig": "CP047869.1", "start": 1241642, "stop": 1241322, "strand": "C",
     "headline": "Predicted RNase P RNA (RPR1); identified by sequence homology to "
                 "the C. albicans ortholog"},
    {"feature_name": "CTRG_00391.5", "gene_name": "SCR1", "ncrna_class": "SRP_RNA",
     "contig": "CP047871.1", "start": 835094, "stop": 835352, "strand": "W",
     "headline": "Predicted signal recognition particle (SRP) RNA (SCR1); identified "
                 "by sequence homology to the C. albicans ortholog"},
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, type=Path)
    args = ap.parse_args()

    plan = []
    with SessionLocal() as s:
        org = s.execute(text(f"SELECT organism_no FROM {S}.organism WHERE organism_name=:n"),
                        {"n": ORGANISM_NAME}).scalar()
        # root_seq_no is a SEQ.seq_no of the contig -> seq.feature_no -> feature.feature_name
        rdn = {}
        rdn_contigs = set()
        for r in s.execute(text(f"""
            SELECT f.gene_name, LEAST(l.start_coord,l.stop_coord),
                   GREATEST(l.start_coord,l.stop_coord), cf.feature_name
            FROM {S}.feature f
            JOIN {S}.feat_location l ON l.feature_no=f.feature_no AND l.is_loc_current='Y'
            JOIN {S}.seq cs ON cs.seq_no=l.root_seq_no
            JOIN {S}.feature cf ON cf.feature_no=cs.feature_no
            WHERE f.organism_no=:o AND f.feature_type='rRNA'"""), {"o": org}).fetchall():
            rdn[r[0]] = {"lo": r[1], "hi": r[2], "contig": r[3]}
            rdn_contigs.add(r[3])
        rdn_contig = next(iter(rdn_contigs)) if len(rdn_contigs) == 1 else None
        if rdn_contig is None:
            print(f"ERROR: rDNA spans multiple contigs {rdn_contigs}", file=sys.stderr)
            sys.exit(1)

        for u, r18, r58, r25 in ITS_UNITS:
            if not all(k in rdn for k in (r18, r58, r25)):
                print(f"ERROR: rDNA unit {u} not fully loaded ({r18}/{r58}/{r25})", file=sys.stderr)
                sys.exit(1)
            its1 = (rdn[r18]["hi"] + 1, rdn[r58]["lo"] - 1)   # 18S end+1 .. 5.8S start-1
            its2 = (rdn[r58]["hi"] + 1, rdn[r25]["lo"] - 1)   # 5.8S end+1 .. 25S start-1
            plan.append({"organism_name": ORGANISM_NAME, "seq_source": SEQ_SOURCE,
                         "feature_name": f"ITS1-{u}", "gene_name": f"ITS1-{u}",
                         "ncrna_class": "ITS1", "contig": rdn_contig,
                         "start": its1[0], "stop": its1[1], "strand": "W",
                         "headline": "Internal transcribed spacer 1; non-coding region "
                                     "within the rDNA repeat, between RDN18 and RDN58"})
            plan.append({"organism_name": ORGANISM_NAME, "seq_source": SEQ_SOURCE,
                         "feature_name": f"ITS2-{u}", "gene_name": f"ITS2-{u}",
                         "ncrna_class": "ITS2", "contig": rdn_contig,
                         "start": its2[0], "stop": its2[1], "strand": "W",
                         "headline": "Internal transcribed spacer 2; non-coding region "
                                     "within the rDNA repeat, between RDN58 and RDN25"})

        for c in CONSERVED:
            e = {"organism_name": ORGANISM_NAME, "seq_source": SEQ_SOURCE}
            e.update(c)
            plan.append(e)

        # guard: none of the target names may already exist
        for e in plan:
            r = s.execute(text(f"SELECT feature_no FROM {S}.feature WHERE feature_name=:n"),
                          {"n": e["feature_name"]}).first()
            if r:
                print(f"ERROR: {e['feature_name']} already exists ({r})", file=sys.stderr)
                sys.exit(1)

    args.out.write_text(json.dumps(plan, indent=2))
    print(f"wrote {len(plan)} ncRNA specs -> {args.out}")
    for e in plan:
        lo, hi = min(e["start"], e["stop"]), max(e["start"], e["stop"])
        print(f"  {e['feature_name']:14s} {e['gene_name']:8s} {e['ncrna_class']:11s} "
              f"{e['contig']}:{lo}-{hi} {e['strand']} len={hi-lo+1}")


if __name__ == "__main__":
    main()
