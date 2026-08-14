#!/usr/bin/env python3
"""Albicans repeat gap diff: de novo RepeatMasker hits vs existing curated
repeat/TE annotation (repeat_region, long_terminal_repeat, retrotransposon).

Candidates use the same crosswalk thresholds as make_repeat_candidates_sp.py.
Each candidate is classified by overlap with curated repeats:
  KNOWN   >=50% covered   | PARTIAL 1-50% | NOVEL 0% (= the annotation gaps)
Reverse diff: curated features with no de novo hit coverage.
"""
import glob
import gzip
import re

SP = "C_albicans_SC5314"
TAG = "CALB"
BASE = "/data/HTS/repeat_te"
GFF = "/data/genomes/C_albicans_SC5314_A22_features.sorted.gff.gz"
OUT = glob.glob(f"{BASE}/rm_denovo_{SP}/*.out")[0]
FAM = f"{BASE}/rmodel_{SP}/{TAG}-families.fa"
TES = f"{BASE}/{TAG}_tesorter.cls.tsv"

REPEAT_TYPES = {"repeat_region", "long_terminal_repeat", "retrotransposon"}

# --- family metadata ---
ftype = {}
for line in open(FAM):
    if not line.startswith(">"):
        continue
    name = line[1:].split("#")[0]
    cls = line.split("#")[1].split()[0]
    m = re.search(r"Type=(\w+)", line)
    ftype[name] = (cls, m.group(1) if m else "")

tes = {}
for line in open(TES):
    if line.startswith("#"):
        continue
    p = line.rstrip("\n").split("\t")
    tes[p[0].split("#")[0]] = f"{p[1]}/{p[2]}/{p[3]}"

# --- curated repeat features + genes from GFF ---
curated = {}   # contig -> [(start, end, type, name)]
genes = {}     # contig -> [(start, end, name)]
with gzip.open(GFF, "rt") as fh:
    for line in fh:
        if line.startswith("#"):
            continue
        p = line.rstrip("\n").split("\t")
        if len(p) < 9:
            continue
        contig, typ, s, e, attr = p[0], p[2], int(p[3]), int(p[4]), p[8]
        m = re.search(r"(?:Name|ID)=([^;]+)", attr)
        name = m.group(1) if m else "?"
        if typ in REPEAT_TYPES:
            curated.setdefault(contig, []).append((s, e, typ, name))
        elif typ in ("gene", "pseudogene"):
            genes.setdefault(contig, []).append((s, e, name))
for v in curated.values():
    v.sort()
for v in genes.values():
    v.sort()
n_curated = sum(len(v) for v in curated.values())

def overlaps(ivs, s, e):
    return [(a, b, *rest) for (a, b, *rest) in ivs if a <= e and b >= s]

# --- de novo candidates (same thresholds as the 5-species crosswalk) ---
cands = []
for line in open(OUT):
    p = line.split()
    if len(p) < 11 or not p[0].isdigit():
        continue
    contig, start, end, strand, fam, cls = p[4], int(p[5]), int(p[6]), p[8], p[9], p[10]
    div = float(p[1])
    length = end - start + 1
    base_cls = cls.split("/")[0]
    if base_cls in ("Simple_repeat", "Low_complexity"):
        continue
    fcls, part = ftype.get(fam, (cls, ""))
    if base_cls == "LTR":
        if length >= 1000 or part == "INT":
            feat = "retrotransposon"
        elif length >= 100:
            feat = "long_terminal_repeat"
        else:
            continue
    elif base_cls == "Satellite":
        if length < 200:
            continue
        feat = "repeat_region"
    else:
        if length < 200 or div > 25:
            continue
        feat = "repeat_region"
    cands.append([contig, start, end, "+" if strand == "+" else "-", length,
                  fam, cls, part, tes.get(fam, ""), round(div, 1), feat])

# --- classify candidates against curated ---
def covfrac(hits, s, e):
    if not hits:
        return 0.0
    return sum(min(e, b) - max(s, a) + 1 for (a, b, *_) in hits) / (e - s + 1)

rows = []
tally = {}
for c in sorted(cands, key=lambda r: (r[0], r[1])):
    contig, s, e = c[0], c[1], c[2]
    hits = overlaps(curated.get(contig, []), s, e)
    cov = covfrac(hits, s, e)
    gh = overlaps(genes.get(contig, []), s, e)
    gcov = covfrac(gh, s, e)
    if cov >= 0.5:
        status = "KNOWN"
    elif hits:
        status = "PARTIAL"
    elif gcov >= 0.5:
        status = "NOVEL_GENEFAM"   # mostly inside genes: multicopy gene family, not a repeat target
    else:
        status = "NOVEL"
    tally[status] = tally.get(status, 0) + 1
    known = ";".join(f"{n}({t})" for (_, _, t, n) in hits[:4])
    gnames = ";".join(n for (_, _, n) in gh[:4])
    rows.append(c + [status, round(cov * 100), round(gcov * 100), known, gnames])

DIFF = f"{BASE}/{SP}_repeat_gap_diff.tsv"
with open(DIFF, "w") as f:
    f.write("contig\tstart\tend\tstrand\tlength\tfamily\trm_class\tltr_part"
            "\ttesorter_class\tpct_divergence\tproposed_feature_type"
            "\tstatus\tpct_covered_by_curated\tpct_covered_by_genes"
            "\tcurated_overlaps\tgene_overlaps\n")
    for r in rows:
        f.write("\t".join(map(str, r)) + "\n")

# --- reverse diff: curated features not recovered de novo ---
denovo = {}
for c in cands:
    denovo.setdefault(c[0], []).append((c[1], c[2]))
for v in denovo.values():
    v.sort()
missed = []
for contig, feats in curated.items():
    for (s, e, typ, name) in feats:
        hits = overlaps(denovo.get(contig, []), s, e)
        cov = 0
        if hits:
            cov = sum(min(e, b) - max(s, a) + 1 for (a, b) in hits) / (e - s + 1)
        if cov < 0.2:
            missed.append((contig, s, e, typ, name, round(cov * 100)))
missed.sort()
REV = f"{BASE}/{SP}_curated_not_recovered.tsv"
with open(REV, "w") as f:
    f.write("contig\tstart\tend\tfeature_type\tname\tpct_covered_by_denovo\n")
    for r in missed:
        f.write("\t".join(map(str, r)) + "\n")

# --- novel-family summary (families driving NOVEL calls, gene-fam separate) ---
fams = {}
for r in rows:
    if r[11] in ("NOVEL", "NOVEL_GENEFAM"):
        fams.setdefault(r[5], []).append(r)
FSUM = f"{BASE}/{SP}_novel_family_summary.tsv"
with open(FSUM, "w") as f:
    f.write("family\trm_class\tltr_part\ttesorter\tn_novel\tn_novel_genefam"
            "\ttotal_bp\tmax_len\tproposed_feature_type\texample_locus"
            "\texample_genes\n")
    for k, v in sorted(fams.items(),
                       key=lambda x: -sum(1 for r in x[1] if r[11] == "NOVEL")):
        cls, part = ftype.get(k, ("", ""))
        nn = sum(1 for r in v if r[11] == "NOVEL")
        ng = len(v) - nn
        ex = next((r for r in v if r[11] == "NOVEL"), v[0])
        exg = next((r[15] for r in v if r[15]), "")
        f.write(f"{k}\t{cls}\t{part}\t{tes.get(k, '')}\t{nn}\t{ng}"
                f"\t{sum(r[4] for r in v)}\t{max(r[4] for r in v)}\t{v[0][10]}"
                f"\t{ex[0]}:{ex[1]}-{ex[2]}\t{exg}\n")

print(f"curated repeat features: {n_curated}")
print(f"de novo candidates (post-crosswalk): {len(rows)}")
for k in ("KNOWN", "PARTIAL", "NOVEL", "NOVEL_GENEFAM"):
    print(f"  {k}: {tally.get(k, 0)}")
print(f"curated NOT recovered de novo (<20% cov): {len(missed)}")
print(f"novel families: {len(fams)}")
print(f"-> {DIFF}\n-> {REV}\n-> {FSUM}")
