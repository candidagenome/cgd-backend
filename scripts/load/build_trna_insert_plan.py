#!/usr/bin/env python3
"""
Build the insert plan for the C. tropicalis tRNAs that load_trnas.py left
UNMATCHED (genuinely new tRNAs, absent from CGD).

Re-derives the unmatched set from tRNAscan + the DB exactly the way load_trnas.py
does (so coordinates/anticodons are authoritative), then attaches the
curator-reviewed systematic feature_name to each and computes a gene_name that
CONTINUES the existing t<AA>(anticodon)<n> series for that organism. Writes a
plan JSON consumed by load_trna_inserts.py.

  * feature_name : reviewed CTRG_XXXX.5 name (EFG1 already owns CTRG_00421.5, so
    the tRNA in that gap gets .6). Keyed by (contig, lo, hi) so a change in the
    tRNAscan input can never silently misname a feature -- an unexpected set of
    unmatched predictions aborts the build.
  * gene_name    : t<letter>(<anticodon>)<n>, n continuing the existing max for
    that family, assigned in genomic order.

Usage:
    python build_trna_insert_plan.py --out ctrop_novel_trna_plan.json
"""
import argparse
import importlib.util
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

LOADER = Path("scripts/C_tropicalis/load_trnas.py").resolve()
sys.path.insert(0, str(LOADER.parents[2]))
spec = importlib.util.spec_from_file_location("load_trnas", LOADER)
lt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lt)

from sqlalchemy import text  # noqa: E402

GFF = Path("/data/HTS/trna_tropicalis/GCF_000006335.3_ASM633v3_genomic.gff.gz")
OUT = Path("/data/HTS/trna_tropicalis/ctrop.trna.out")

ORGANISM_NAME = "Candida tropicalis MYA-3404"
SEQ_SOURCE = "C. tropicalis MYA-3404"

# curator-reviewed systematic names, keyed by (contig, lo, hi) of the prediction.
# CTRG_00421.5 is already the curated gene EFG1, so the tRNA sharing that
# 00421->00422 gap takes .6.  The three contig-5'-end tRNAs take the number just
# below their downstream gene (numeric adjacency in the ASM633v3 order).
NAME_MAP = {
    ("CP047869.1", 2832849, 2832937): "CTRG_01076.5",   # tRNA-Val, after CTRG_01076
    ("CP047870.1", 1280, 1395):       "CTRG_03730.5",   # tRNA-Leu, before CTRG_03731
    ("CP047871.1", 898895, 898965):   "CTRG_00421.6",   # tRNA-Gly, .5 = EFG1
    ("CP047873.1", 2794, 2909):       "CTRG_06155.5",   # tRNA-Leu, before CTRG_06156
    ("CP047875.1", 1478, 1593):       "CTRG_05771.5",   # tRNA-Leu, before CTRG_05772
}


def existing_family_max(session, org, letter, codon):
    """Highest <n> already used in t<letter>(<codon>)<n> for this organism."""
    rows = session.execute(text(f"""
        SELECT gene_name FROM {lt.DB_SCHEMA}.feature
        WHERE organism_no=:o AND feature_type='tRNA' AND gene_name LIKE :p
    """), {"o": org, "p": f"t{letter}({codon})%"}).fetchall()
    mx = 0
    pat = re.compile(rf"^t{re.escape(letter)}\({re.escape(codon)}\)(\d+)$")
    for (g,) in rows:
        m = pat.match(g or "")
        if m:
            mx = max(mx, int(m.group(1)))
    return mx


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, type=Path)
    args = ap.parse_args()

    trna_tags = lt.parse_asm633_trna_tags(GFF)
    preds = [p for p in lt.parse_trnascan(OUT) if not p["is_pseudo"]]

    with lt.SessionLocal() as session:
        org = lt.get_scalar(session,
            f"SELECT organism_no FROM {lt.DB_SCHEMA}.organism WHERE organism_name=:n",
            n=ORGANISM_NAME)
        contigs_by_name, seqno_to_name = lt.get_contig_map(session, org)
        current = lt.get_current_features(session, org)

        preds_by_contig = defaultdict(list)
        for p in preds:
            preds_by_contig[p["seqid"]].append(p)

        matched_ids = set()
        for tag in trna_tags:
            feat = current.get(tag)
            if not feat:
                continue
            cn = seqno_to_name.get(feat["root_seq_no"])
            best, match = 0.0, None
            for p in preds_by_contig.get(cn, []):
                ov = lt.reciprocal_overlap(feat["lo"], feat["hi"],
                                           min(p["begin"], p["end"]),
                                           max(p["begin"], p["end"]))
                if ov > best:
                    best, match = ov, p
            if best >= lt.OVERLAP_MIN and match is not None:
                matched_ids.add(id(match))

        unmatched = [p for p in preds if id(p) not in matched_ids]

        # guard: the unmatched set must be EXACTLY the reviewed NAME_MAP keys
        keys = {(p["seqid"], min(p["begin"], p["end"]), max(p["begin"], p["end"]))
                for p in unmatched}
        if keys != set(NAME_MAP):
            print("ERROR: unmatched predictions differ from the reviewed NAME_MAP.",
                  file=sys.stderr)
            print("  unmatched:", sorted(keys), file=sys.stderr)
            print("  NAME_MAP :", sorted(NAME_MAP), file=sys.stderr)
            sys.exit(1)

        # assign gene_name copy numbers in genomic order, continuing each family
        counters = {}
        plan = []
        for p in sorted(unmatched, key=lambda x: (x["seqid"],
                                                  min(x["begin"], x["end"]))):
            lo, hi = min(p["begin"], p["end"]), max(p["begin"], p["end"])
            fname = NAME_MAP[(p["seqid"], lo, hi)]
            letter = lt.AA_TO_LETTER.get(p["aa"], "X")
            codon = p["codon"]
            fam = (letter, codon)
            if fam not in counters:
                counters[fam] = existing_family_max(session, org, letter, codon)
            counters[fam] += 1
            gene_name = f"t{letter}({codon}){counters[fam]}"
            headline = (f"tRNA-{p['aa']}, predicted by tRNAscan-SE; "
                        f"{codon} anticodon")
            if fname in current:
                print(f"ERROR: feature_name {fname} already exists", file=sys.stderr)
                sys.exit(1)
            plan.append({
                "organism_name": ORGANISM_NAME,
                "seq_source": SEQ_SOURCE,
                "feature_name": fname,
                "gene_name": gene_name,
                "headline": headline,
                "contig": p["seqid"],
                "begin": p["begin"],
                "end": p["end"],
                "intron_begin": p["intron_begin"],
                "intron_end": p["intron_end"],
            })

    args.out.write_text(json.dumps(plan, indent=2))
    print(f"wrote {len(plan)} tRNA specs -> {args.out}")
    for s in plan:
        strand = "+" if s["begin"] < s["end"] else "-"
        print(f"  {s['feature_name']:14s} {s['gene_name']:10s} "
              f"{s['contig']}:{min(s['begin'],s['end'])}-{max(s['begin'],s['end'])} "
              f"{strand}  intron={bool(s['intron_begin'])}  | {s['headline']}")


if __name__ == "__main__":
    main()
