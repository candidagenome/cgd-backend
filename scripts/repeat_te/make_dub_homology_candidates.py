#!/usr/bin/env python3
"""Dubliniensis homology-fill: parse the albicans-seed RepeatMasker survey hits
into a candidate-feature TSV. Each candidate is named by its albicans homolog
(the seed element), so curators see e.g. 'matches albicans Tca2'."""
import glob

OUT = glob.glob("/data/HTS/repeat_te/rm_survey_C_dubliniensis_CD36/*.out")[0]
SEEDS = "/data/HTS/repeat_te/albicans_te_seeds.fasta"
TSV = "/data/HTS/repeat_te/C_dubliniensis_CD36_repeat_candidates.tsv"
FSUM = "/data/HTS/repeat_te/C_dubliniensis_CD36_repeat_family_summary.tsv"

# seed name -> class from library headers (name#Class/Subclass)
seedcls = {}
for line in open(SEEDS):
    if line.startswith(">"):
        name, cls = line[1:].strip().split("#")
        seedcls[name] = cls

rows = []
mask_only = {"Simple_repeat": 0, "Low_complexity": 0}
for line in open(OUT):
    p = line.split()
    if len(p) < 11 or not p[0].isdigit():
        continue
    contig, start, end, strand, seed, cls = p[4], int(p[5]), int(p[6]), p[8], p[9], p[10]
    div = float(p[1])
    length = end - start + 1
    base = cls.split("/")[0]
    if base in ("Simple_repeat", "Low_complexity"):
        mask_only[base] += 1
        continue
    if div > 30:
        continue
    if base == "LTR":
        # seeds: albicans retrotransposon features (Tca*, full elements) vs solo-LTR features
        is_retro_seed = cls in ("LTR/Gypsy", "LTR/Copia")
        if is_retro_seed and length >= 1000:
            feat, load = "retrotransposon", "LOAD"
        elif length >= 100:
            feat, load = "long_terminal_repeat", "LOAD"
        else:
            continue
    elif base == "LINE":
        if length < 500:
            continue
        feat, load = "retrotransposon", "LOAD"  # non-LTR/LINE, note in headline at load time
    else:  # Unknown = albicans repeat_region seeds (MRS/RPS etc.)
        if length < 200:
            continue
        feat, load = "repeat_region", "CURATOR_GATED"
    rows.append((contig, start, end, "+" if strand == "+" else "-", length,
                 seed, cls, round(div, 1), feat, load))

rows.sort(key=lambda r: (r[0], r[1]))
with open(TSV, "w") as f:
    f.write("contig\tstart\tend\tstrand\tlength\talbicans_homolog\tclass"
            "\tpct_divergence\tproposed_feature_type\tload_class\n")
    for r in rows:
        f.write("\t".join(map(str, r)) + "\n")

fams = {}
for r in rows:
    fams.setdefault(r[5], []).append(r)
with open(FSUM, "w") as f:
    f.write("albicans_homolog\tclass\tn_copies\ttotal_bp\tmax_len\tmin_pct_div"
            "\tproposed_feature_type\tload_class\n")
    for k, v in sorted(fams.items(), key=lambda x: -len(x[1])):
        f.write(f"{k}\t{v[0][6]}\t{len(v)}\t{sum(r[4] for r in v)}\t{max(r[4] for r in v)}"
                f"\t{min(r[7] for r in v)}\t{v[0][8]}\t{v[0][9]}\n")

print(f"candidates: {len(rows)} -> {TSV}")
by = {}
for r in rows:
    by[(r[8], r[9])] = by.get((r[8], r[9]), 0) + 1
for k, v in sorted(by.items()):
    print("  ", k, v)
print(f"mask-only excluded: {mask_only}")
print(f"family summary -> {FSUM} ({len(fams)} albicans seed elements with hits)")
