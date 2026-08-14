#!/usr/bin/env python3
"""Albicans curator check file for the repeat/TE dev load.

Same per-family layout as the other species' *_repeat_load_check.tsv
(make_repeat_check_files.py), built from load_plan_C_albicans_SC5314.json
after the dev DB load. Albicans-specific: features are haplotype-paired
_A/_B alleles sharing an alias, so n_copies counts LOCI (serials) and the
evidence column notes the feature count. Run on cgd-frontend-dev.
"""
import json
import re

D = "/data/HTS/repeat_te"
plan = json.load(open(f"{D}/load_plan_C_albicans_SC5314.json"))

fams = {}
for r in plan["rows"]:
    base = re.sub(r"-\d+$", "", r["alias"])
    f = fams.setdefault(base, dict(ftype=r["ftype"], loci=set(), nfeat=0, bp=0,
                                   maxlen=0, first=r["feature_name"],
                                   family=r["family"], head=r["headline"],
                                   overlaps=0))
    f["loci"].add(r["alias"])
    f["nfeat"] += 1
    ln = int(r["end"]) - int(r["start"]) + 1
    f["bp"] += ln
    f["maxlen"] = max(f["maxlen"], ln)
    if "overlaps" in r["headline"]:
        f["overlaps"] += 1

out = f"{D}/C_albicans_SC5314_repeat_load_check.tsv"
with open(out, "w") as fh:
    fh.write("family_display\tstatus\tfeature_type\tn_copies\ttotal_bp\tmax_len"
             "\tfirst_feature\texample_url\tevidence\tREVIEW(Accept/Revert)\n")
    for base, f in sorted(fams.items(), key=lambda x: (x[1]["ftype"], -len(x[1]["loci"]))):
        ev = f["head"].split("; ")[1] if "; " in f["head"] else f["head"]
        ev += f"; {f['nfeat']} features across {len(f['loci'])} loci (A/B allele pairs)"
        if f["overlaps"]:
            ev += f"; {f['overlaps']} features overlap annotated genes (paralog check)"
        url = f"https://frontend.dev.candidagenome.org/locus/{f['first']}"
        fh.write(f"{base}\tLOADED\t{f['ftype']}\t{len(f['loci'])}\t{f['bp']}\t{f['maxlen']}"
                 f"\t{f['first']}\t{url}\t{ev}\t\n")
    for s in plan["skipped_families"]:
        fh.write(f"{s['family']}\tNOT_LOADED\trepeat_region\t{s['n_copies']}\t{s['total_bp']}"
                 f"\t-\t-\t-\t{s['reason']}\t\n")

n_loaded = len(fams)
n_feat = sum(f["nfeat"] for f in fams.values())
print(f"wrote {out}: {n_loaded} loaded families ({n_feat} features) + "
      f"{len(plan['skipped_families'])} skipped")
