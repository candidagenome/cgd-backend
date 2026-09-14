#!/usr/bin/env python3
"""Parse tropicalis de novo RepeatMasker .out into the candidate-feature TSV
per REPEAT_TE_ANNOTATION_STRATEGY.md sec.5 crosswalk + sec.7 load/mask split."""
import glob
import re
import sys

SP = sys.argv[1]      # e.g. C_auris_B8441
TAG = sys.argv[2]     # e.g. CAURIS

OUT = glob.glob(f"/data/HTS/repeat_te/rm_denovo_{SP}/*.out")[0]
FAM = f"/data/HTS/repeat_te/rmodel_{SP}/{TAG}-families.fa"
TES = f"/data/HTS/repeat_te/{TAG}_tesorter.cls.tsv"
TSV = f"/data/HTS/repeat_te/{SP}_repeat_candidates.tsv"
FSUM = f"/data/HTS/repeat_te/{SP}_repeat_family_summary.tsv"

# family metadata: LTR-part vs INT-part from RepeatModeler headers
ftype = {}
for line in open(FAM):
    if not line.startswith(">"):
        continue
    name = line[1:].split("#")[0]
    cls = line.split("#")[1].split()[0]
    part = ""
    m = re.search(r"Type=(\w+)", line)
    if m:
        part = m.group(1)
    ftype[name] = (cls, part)

tes = {}
for i, line in enumerate(open(TES)):
    if line.startswith("#"):
        continue
    p = line.rstrip("\n").split("\t")
    tes[p[0].split("#")[0]] = f"{p[1]}/{p[2]}/{p[3]}"

rows = []
mask_only = {"Simple_repeat": 0, "Low_complexity": 0}
for line in open(OUT):
    p = line.split()
    if len(p) < 11 or not p[0].isdigit():
        continue
    contig, start, end, strand, fam, cls = p[4], int(p[5]), int(p[6]), p[8], p[9], p[10]
    div = float(p[1])
    length = end - start + 1
    base_cls = cls.split("/")[0]
    if base_cls in ("Simple_repeat", "Low_complexity"):
        mask_only[base_cls] += 1
        continue
    fcls, part = ftype.get(fam, (cls, ""))
    if base_cls == "LTR":
        if length >= 1000 or part == "INT":
            feat, load = "retrotransposon", "LOAD"
        elif length >= 100:
            feat, load = "long_terminal_repeat", "LOAD"
        else:
            continue
    elif base_cls == "Satellite":
        if length < 200:
            continue
        feat, load = "repeat_region", "CURATOR_GATED"
    else:  # Unknown dispersed families
        if length < 200 or div > 25:
            continue
        feat, load = "repeat_region", "CURATOR_GATED"
    rows.append((contig, start, end, "+" if strand == "+" else "-", length,
                 fam, cls, part, tes.get(fam, ""), round(div, 1), feat, load))

rows.sort(key=lambda r: (r[0], r[1]))
hdr = ("contig\tstart\tend\tstrand\tlength\tfamily\trm_class\tltr_part\ttesorter_class"
       "\tpct_divergence\tproposed_feature_type\tload_class\n")
with open(TSV, "w") as f:
    f.write(hdr)
    for r in rows:
        f.write("\t".join(map(str, r)) + "\n")

# per-family summary
fams = {}
for r in rows:
    k = r[5]
    fams.setdefault(k, []).append(r)
with open(FSUM, "w") as f:
    f.write("family\trm_class\tltr_part\ttesorter\tn_copies\ttotal_bp\tmax_len"
            "\tproposed_feature_type\tload_class\n")
    for k, v in sorted(fams.items(), key=lambda x: -len(x[1])):
        cls, part = ftype.get(k, ("", ""))
        f.write(f"{k}\t{cls}\t{part}\t{tes.get(k, '')}\t{len(v)}\t{sum(r[4] for r in v)}"
                f"\t{max(r[4] for r in v)}\t{v[0][10]}\t{v[0][11]}\n")

print(f"candidates: {len(rows)} rows -> {TSV}")
by = {}
for r in rows:
    by[(r[10], r[11])] = by.get((r[10], r[11]), 0) + 1
for k, v in sorted(by.items()):
    print("  ", k, v)
print(f"mask-only excluded: {mask_only}")
print(f"family summary -> {FSUM} ({len(fams)} families with candidate copies)")
