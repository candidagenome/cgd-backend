#!/usr/bin/env python3
"""Build per-species repeat/TE load plans from the candidate TSVs.

Scope (per CURATOR_REVIEW_REPEAT_TE.md):
  - all LOAD-class rows (retrotransposon / long_terminal_repeat)
  - CURATOR_GATED repeat_region rows whose FAMILY passes the D6 floor
    (>=5 copies or >=5kb total)
Dubliniensis: homology route preferred for LOAD-class (albicans-anchored names),
de novo LOAD rows added when not overlapping a homology row; gated repeat_region
families come from the DE NOVO route (consensus extents), labeled with an
albicans homolog name when >=50% of copies overlap homology hits of one element.

Emits /data/HTS/repeat_te/load_plan_<species>.json:
  rows: [{name_serial_assigned_later, contig, start, end, strand, ftype, alias,
          family, headline}], plus families_loaded / families_skipped manifests.
"""
import json
import os

D = "/data/HTS/repeat_te"
SPECIES = {
    "C_tropicalis":        dict(prefix="CTRG_RPT",  tag="Ct"),
    "C_auris_B8441":       dict(prefix="B9J08_RPT", tag="Cau"),
    "C_parapsilosis_CDC317": dict(prefix="CPAR2_RPT", tag="Cp"),
    "C_glabrata_CBS138":   dict(prefix="CAGL_RPT",  tag="Cg"),
    "C_dubliniensis_CD36": dict(prefix="Cd36_RPT",  tag="Cd"),
}
TYPE_TEXT = {
    "retrotransposon": "Retrotransposon",
    "long_terminal_repeat": "Long terminal repeat (LTR)",
    "repeat_region": "Repeat region",
}


def read_tsv(path):
    rows = []
    hdr = None
    for line in open(path):
        if line.startswith("#") or not line.strip():
            continue
        p = line.rstrip("\n").split("\t")
        if hdr is None:
            hdr = p
            continue
        rows.append(dict(zip(hdr, p)))
    return rows


def wicker(rm_class, tes):
    c = (tes or rm_class or "").lower()
    if "gypsy" in c:
        return "RLG"
    if "copia" in c:
        return "RLC"
    if "line" in c or "zorro" in c:
        return "RIL"
    if rm_class.startswith("LTR"):
        return "RLX"
    if rm_class.startswith("DNA"):
        return "DTX"
    return "RPT"


def famfloor(fs_rows):
    ok = set()
    for r in fs_rows:
        if int(r["n_copies"]) >= 5 or int(r["total_bp"]) >= 5000:
            ok.add(r["family"])
    return ok


def build_headline(ftype, fam, rm_class, tes, div, alb=None):
    bits = [TYPE_TEXT[ftype]]
    prov = f"RepeatModeler family {fam} ({rm_class}"
    if tes:
        prov += f"; TEsorter {tes}"
    prov += f"), {div}% divergence from consensus"
    if alb:
        prov = f"homolog of C. albicans {alb}; " + prov
    bits.append(prov)
    bits.append("provisional annotation from de novo repeat pipeline, curator review pending")
    return "; ".join(bits)


def overlaps(a, blist):
    for b in blist:
        if a["contig"] == b["contig"] and min(int(a["end"]), int(b["end"])) - max(int(a["start"]), int(b["start"])) > 0:
            return True
    return False


summary = {}
for sp, cfg in SPECIES.items():
    cand = read_tsv(f"{D}/{sp}_repeat_candidates.tsv")
    fams = read_tsv(f"{D}/{sp}_repeat_family_summary.tsv")
    floor_ok = famfloor([f for f in fams if f["load_class"] == "CURATOR_GATED"])
    rows = []
    fam_alias = {}
    fam_counter = {}

    def fam_label(fam, rm_class, tes):
        if fam not in fam_alias:
            w = wicker(rm_class, tes)
            fam_counter[w] = fam_counter.get(w, 0) + 1
            fam_alias[fam] = f"{cfg['tag']}{w}{fam_counter[w]}"
        return fam_alias[fam]

    # normalize candidate rows
    for r in cand:
        r["contig"], r["start"], r["end"] = r["contig"], int(r["start"]), int(r["end"])

    picked = []
    if sp == "C_dubliniensis_CD36":
        hom = read_tsv(f"{D}/C_dubliniensis_CD36_homology_repeat_candidates.tsv")
        for r in hom:
            r["contig"], r["start"], r["end"] = r["contig"], int(r["start"]), int(r["end"])
        hom_load = [r for r in hom if r["load_class"] == "LOAD"]
        for r in hom_load:
            alb = r.get("albicans_gene_name") or r.get("albicans_homolog")
            lbl = (alb + "-like") if alb else "CdRPT"
            picked.append(dict(r, alias_base=lbl, ftype=r["proposed_feature_type"],
                               headline=build_headline(r["proposed_feature_type"],
                                                       r["albicans_homolog"], r["class"], "",
                                                       r["pct_divergence"], alb=alb)))
        for r in cand:
            if r["load_class"] == "LOAD" and not overlaps(r, hom_load):
                lbl = fam_label(r["family"], r["rm_class"], r.get("tesorter", ""))
                picked.append(dict(r, alias_base=lbl, ftype=r["proposed_feature_type"],
                                   headline=build_headline(r["proposed_feature_type"], r["family"],
                                                           r["rm_class"], r.get("tesorter", ""),
                                                           r["pct_divergence"])))
        gated = [r for r in cand if r["load_class"] == "CURATOR_GATED" and r["family"] in floor_ok]
        hom_gated = [r for r in hom if r["load_class"] == "CURATOR_GATED"]
        for r in gated:
            albname = ""
            for h in hom_gated:
                if r["contig"] == h["contig"] and min(r["end"], h["end"]) - max(r["start"], h["start"]) > 0:
                    albname = h.get("albicans_gene_name") or h.get("albicans_homolog")
                    break
            lbl = (albname + "-like") if albname else fam_label(r["family"], r["rm_class"], r.get("tesorter", ""))
            picked.append(dict(r, alias_base=lbl, ftype="repeat_region",
                               headline=build_headline("repeat_region", r["family"], r["rm_class"],
                                                       r.get("tesorter", ""), r["pct_divergence"],
                                                       alb=albname or None)))
    else:
        for r in cand:
            if r["load_class"] == "LOAD" or (r["load_class"] == "CURATOR_GATED" and r["family"] in floor_ok):
                lbl = fam_label(r["family"], r["rm_class"], r.get("tesorter", ""))
                picked.append(dict(r, alias_base=lbl, ftype=r["proposed_feature_type"],
                                   headline=build_headline(r["proposed_feature_type"], r["family"],
                                                           r["rm_class"], r.get("tesorter", ""),
                                                           r["pct_divergence"])))

    # dedup identical intervals, assign serials in genome order + per-family copy numbers
    seen = set()
    uniq = []
    for r in picked:
        key = (r["contig"], r["start"], r["end"])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)
    uniq.sort(key=lambda r: (r["contig"], r["start"]))
    copyn = {}
    out = []
    for i, r in enumerate(uniq, 1):
        copyn[r["alias_base"]] = copyn.get(r["alias_base"], 0) + 1
        out.append(dict(
            feature_name=f"{cfg['prefix']}_{i:04d}",
            alias=f"{r['alias_base']}-{copyn[r['alias_base']]}",
            contig=r["contig"], start=r["start"], end=r["end"],
            strand=r.get("strand", "+"), ftype=r["ftype"],
            family=r.get("family", r.get("albicans_homolog", "")),
            headline=r["headline"],
        ))
    skipped = [dict(family=f["family"], rm_class=f["rm_class"], n_copies=f["n_copies"],
                    total_bp=f["total_bp"], reason="below D6 floor (<5 copies and <5kb)")
               for f in fams if f["load_class"] == "CURATOR_GATED" and f["family"] not in floor_ok]
    json.dump(dict(rows=out, skipped_families=skipped),
              open(f"{D}/load_plan_{sp}.json", "w"), indent=1)
    by = {}
    for r in out:
        by[r["ftype"]] = by.get(r["ftype"], 0) + 1
    summary[sp] = (len(out), by, len(skipped))
    print(f"{sp}: {len(out)} features {by}; {len(skipped)} families skipped (below floor)")
