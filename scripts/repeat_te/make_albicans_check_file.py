#!/usr/bin/env python3
"""Albicans curator check file for the repeat/TE gap analysis.

Same column layout as the other species' *_repeat_load_check.tsv, but
albicans candidates are NOT loaded yet (gap-diff finished 2026-08-13), so
status is CANDIDATE / CANDIDATE_BELOW_FLOOR (D6 floor: >=5 copies or
>=5 kb per family) and example_url is a JBrowse region link rather than
a locus page. One row per novel family from the gap diff.
"""
import csv

D = "/data/HTS/repeat_te"
SUMMARY = f"{D}/C_albicans_SC5314_novel_family_summary.tsv"
OUT = f"{D}/C_albicans_SC5314_repeat_load_check.tsv"

rows = list(csv.DictReader(open(SUMMARY), delimiter="\t"))

def jbrowse_url(locus):
    # example_locus like Ca22chr1A_C_albicans_SC5314:6623-10271
    if not locus or ":" not in locus:
        return "-"
    contig, span = locus.rsplit(":", 1)
    span = span.replace("-", "..")
    return (f"https://frontend.dev.candidagenome.org/jbrowse2/"
            f"?assembly=C_albicans_SC5314&loc={contig}:{span}")

out_rows = []
for r in rows:
    n = int(r["n_novel"])
    bp = int(r["total_bp"])
    below_floor = n < 5 and bp < 5000
    status = "CANDIDATE_BELOW_FLOOR" if below_floor else "CANDIDATE"
    cls = r["rm_class"] or "Unknown"
    ev_bits = [f"RepeatModeler family {r['family']} ({cls})"]
    if r.get("tesorter"):
        ev_bits.append(f"TEsorter: {r['tesorter']}")
    if r.get("ltr_part"):
        ev_bits.append(f"LTR part: {r['ltr_part']}")
    ngene = int(r.get("n_novel_genefam") or 0)
    if ngene:
        genes = r.get("example_genes") or ""
        ev_bits.append(f"{ngene} copies overlap gene features"
                       + (f" (e.g. {genes})" if genes else ""))
    out_rows.append({
        "family_display": r["family"],
        "status": status,
        "feature_type": r["proposed_feature_type"],
        "n_copies": n,
        "total_bp": bp,
        "max_len": r["max_len"],
        "first_feature": "-",
        "example_url": jbrowse_url(r.get("example_locus", "")),
        "evidence": "; ".join(ev_bits),
        "REVIEW(Accept/Reject)": "",
    })

# Mirror the other check files' ordering: feature_type, then copy count desc
out_rows.sort(key=lambda x: (x["feature_type"], -x["n_copies"]))

cols = ["family_display", "status", "feature_type", "n_copies", "total_bp",
        "max_len", "first_feature", "example_url", "evidence",
        "REVIEW(Accept/Reject)"]
with open(OUT, "w", newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=cols, delimiter="\t")
    w.writeheader()
    w.writerows(out_rows)

n_cand = sum(1 for r in out_rows if r["status"] == "CANDIDATE")
n_floor = len(out_rows) - n_cand
print(f"wrote {OUT}: {len(out_rows)} families "
      f"({n_cand} CANDIDATE, {n_floor} CANDIDATE_BELOW_FLOOR)")
