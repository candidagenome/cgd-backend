#!/usr/bin/env python3
"""Per-species curator check files for the repeat/TE dev load.
One row per FAMILY (review unit) + skipped families; REVIEW column blank."""
import json
import re

D = "/data/HTS/repeat_te"
SPECIES = ["C_tropicalis", "C_auris_B8441", "C_parapsilosis_CDC317",
           "C_glabrata_CBS138", "C_dubliniensis_CD36"]

for sp in SPECIES:
    plan = json.load(open(f"{D}/load_plan_{sp}.json"))
    fams = {}
    for r in plan["rows"]:
        base = re.sub(r"-\d+$", "", r["alias"])
        f = fams.setdefault(base, dict(ftype=r["ftype"], n=0, bp=0, maxlen=0,
                                       first=r["feature_name"], family=r["family"],
                                       head=r["headline"]))
        f["n"] += 1
        ln = int(r["end"]) - int(r["start"]) + 1
        f["bp"] += ln
        f["maxlen"] = max(f["maxlen"], ln)
    out = f"{D}/{sp}_repeat_load_check.tsv"
    with open(out, "w") as fh:
        fh.write("family_display\tstatus\tfeature_type\tn_copies\ttotal_bp\tmax_len"
                 "\tfirst_feature\texample_url\tevidence\tREVIEW(Accept/Revert)\n")
        for base, f in sorted(fams.items(), key=lambda x: (x[1]["ftype"], -x[1]["n"])):
            ev = f["head"].split("; ")[1] if "; " in f["head"] else f["head"]
            url = f"https://frontend.dev.candidagenome.org/locus/{f['first']}"
            fh.write(f"{base}\tLOADED\t{f['ftype']}\t{f['n']}\t{f['bp']}\t{f['maxlen']}"
                     f"\t{f['first']}\t{url}\t{ev}\t\n")
        for s in plan["skipped_families"]:
            fh.write(f"{s['family']}\tNOT_LOADED\trepeat_region\t{s['n_copies']}\t{s['total_bp']}"
                     f"\t-\t-\t-\t{s['reason']}\t\n")
    print(f"{sp}: {len(fams)} loaded families + {len(plan['skipped_families'])} skipped -> {out}")
