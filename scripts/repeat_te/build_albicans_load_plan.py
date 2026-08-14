#!/usr/bin/env python3
"""Build the albicans repeat/TE load plan from the gap-diff NOVEL rows.

Scope mirrors build_repeat_load_plans.py: LOAD-class rows (retrotransposon /
long_terminal_repeat) always; repeat_region families gated by the D6 floor
(>=5 novel copies or >=5kb novel bp).

Albicans-specific: the A22 assembly carries both haplotypes, and curated
features exist as _A/_B allele pairs. Copies of the same family on sister
chromosomes (Ca22chrXA / Ca22chrXB) at nearby coordinates are paired into
one serial with _A/_B feature names (CALB_RPT_0001_A / CALB_RPT_0001_B,
sharing an alias, like curated alleles). Unpaired copies keep their own
serial with their haplotype suffix. Copies overlapping annotated genes get
the overlap flagged in the headline (likeliest Revert candidates).

Run on cgd-frontend-dev; emits /data/HTS/repeat_te/load_plan_C_albicans_SC5314.json
"""
import csv
import json
import re

D = "/data/HTS/repeat_te"
GAP = f"{D}/C_albicans_SC5314_repeat_gap_diff.tsv"
SUMMARY = f"{D}/C_albicans_SC5314_novel_family_summary.tsv"
OUT = f"{D}/load_plan_C_albicans_SC5314.json"

PREFIX = "CALB_RPT"
TAG = "Ca"
PAIR_MAX_DELTA = 200_000  # bp between sister-haplotype starts

TYPE_TEXT = {
    "retrotransposon": "Retrotransposon",
    "long_terminal_repeat": "Long terminal repeat (LTR)",
    "repeat_region": "Repeat region",
}
TYPE_ORDER = {"long_terminal_repeat": 0, "retrotransposon": 1, "repeat_region": 2}


def wicker(rm_class, tes):
    c = (tes or rm_class or "").lower()
    if "gypsy" in c:
        return "RLG"
    if "copia" in c:
        return "RLC"
    if "line" in c or "zorro" in c:
        return "RIL"
    if (rm_class or "").startswith("LTR"):
        return "RLX"
    if (rm_class or "").startswith("DNA"):
        return "DTX"
    return "RPT"


gap = list(csv.DictReader(open(GAP), delimiter="\t"))
summary = list(csv.DictReader(open(SUMMARY), delimiter="\t"))

floor_ok = {r["family"] for r in summary
            if int(r["n_novel"]) >= 5 or int(r["total_bp"]) >= 5000}

novel = [r for r in gap if r["status"] == "NOVEL"]
in_scope, skipped = [], {}
for r in novel:
    if r["proposed_feature_type"] == "repeat_region" and r["family"] not in floor_ok:
        skipped[r["family"]] = skipped.get(r["family"], 0) + 1
        continue
    in_scope.append(r)

# --- haplotype pairing within (family, chromosome) ---
HAP_RE = re.compile(r"^(Ca22chr[0-9R]+)([AB])(_C_albicans_SC5314)$")

def hap_key(contig):
    m = HAP_RE.match(contig)
    return (m.group(1), m.group(2)) if m else (contig, None)

by_fam_chr = {}
for r in in_scope:
    chrom, hap = hap_key(r["contig"])
    r["_hap"] = hap
    by_fam_chr.setdefault((r["family"], chrom), []).append(r)

pairs, singletons = [], []
for (fam, chrom), copies in by_fam_chr.items():
    a = sorted([c for c in copies if c["_hap"] == "A"], key=lambda c: int(c["start"]))
    b = sorted([c for c in copies if c["_hap"] == "B"], key=lambda c: int(c["start"]))
    used_b = set()
    for ca in a:
        best, best_d = None, PAIR_MAX_DELTA + 1
        for i, cb in enumerate(b):
            if i in used_b:
                continue
            d = abs(int(ca["start"]) - int(cb["start"]))
            if d < best_d:
                best, best_d = i, d
        if best is not None and best_d <= PAIR_MAX_DELTA:
            used_b.add(best)
            pairs.append((ca, b[best]))
        else:
            singletons.append(ca)
    singletons.extend(cb for i, cb in enumerate(b) if i not in used_b)

# --- serials: stable order (type, family, chromosome, position) ---
def unit_sort_key(unit):
    r = unit[0]
    return (TYPE_ORDER[r["proposed_feature_type"]], r["family"],
            hap_key(r["contig"])[0], int(r["start"]))

units = [tuple(p) for p in pairs] + [(s,) for s in singletons]
units.sort(key=unit_sort_key)

# family -> Wicker alias base, numbered per class in first-seen order
fam_meta = {r["family"]: r for r in summary}
alias_base, class_counter, fam_copy_counter = {}, {}, {}
rows = []
serial = 0
for unit in units:
    serial += 1
    fam = unit[0]["family"]
    if fam not in alias_base:
        m = fam_meta.get(fam, {})
        w = wicker(m.get("rm_class", ""), m.get("tesorter", ""))
        class_counter[w] = class_counter.get(w, 0) + 1
        alias_base[fam] = f"{TAG}{w}{class_counter[w]}"
    fam_copy_counter[fam] = fam_copy_counter.get(fam, 0) + 1
    alias = f"{alias_base[fam]}-{fam_copy_counter[fam]}"
    for r in unit:
        ftype = r["proposed_feature_type"]
        head = [TYPE_TEXT[ftype]]
        prov = f"RepeatModeler family {r['family']} ({r['rm_class'] or 'Unknown'}"
        if r.get("tesorter_class"):
            prov += f"; TEsorter {r['tesorter_class']}"
        prov += f"), {r['pct_divergence']}% divergence from consensus"
        head.append(prov)
        if len(unit) == 2:
            head.append("haplotype-paired allele")
        if r.get("gene_overlaps"):
            head.append(f"overlaps {r['gene_overlaps']} (check paralog artifact)")
        head.append("provisional, curator review pending")
        rows.append({
            "feature_name": f"{PREFIX}_{serial:04d}_{r['_hap']}",
            "alias": alias,
            "contig": r["contig"],
            "start": int(r["start"]),
            "end": int(r["end"]),
            "strand": r["strand"],
            "ftype": ftype,
            "family": r["family"],
            "headline": "; ".join(head),
        })

skipped_families = [
    {"family": f, "rm_class": fam_meta.get(f, {}).get("rm_class", ""),
     "n_copies": fam_meta.get(f, {}).get("n_novel", "?"),
     "total_bp": fam_meta.get(f, {}).get("total_bp", "?"),
     "reason": "below D6 floor (<5 copies and <5kb)"}
    for f in sorted(skipped)
]

json.dump({"rows": rows, "skipped_families": skipped_families},
          open(OUT, "w"), indent=1)
print(f"wrote {OUT}: {len(rows)} features ({len(pairs)} A/B pairs + "
      f"{len(singletons)} singletons = {serial} serials); "
      f"{len(skipped_families)} families skipped")
