#!/usr/bin/env python3
"""
Build the curated ortholog-family cluster load plan from curator decisions.

Inputs:
  1. The families worklist (ortholog_family_worklist_suggested.tsv) — 104
     intransitive components with a curator-reviewed `members` column
     ("species:feature_name=gene_name; ...").
  2. The curator's round-1 decisions (xlsx export of the same worklist with
     the DECISION column filled).
  3. The second-pass outcomes (2026-09-14, all rows OK'd) — embedded below as
     directives, since they are final and few: rows fixed at the data source
     are skipped, the ATO rows merge, FLO9/IFF11 and MFA1/MFG1 split.

Output: a load plan JSON for load_curated_clusters.py plus a human-readable
summary TSV. Each plan cluster becomes one HOMOLOGY_GROUP row of type
'curated family' (method 'CGD curation') with FEAT_HOMOLOGY members.

Usage:
    python build_curated_cluster_plan.py \
        --worklist ortholog_family_worklist_suggested.tsv \
        --round1 ortholog_family_worklist.suggested.jls.xlsx \
        --out-json curated_cluster_plan.json \
        --out-tsv curated_cluster_plan_summary.tsv
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter

import openpyxl

# ---------------------------------------------------------------------------
# Second-pass directives (curator sign-off 2026-09-14, all rows OK)
# ---------------------------------------------------------------------------

# Components resolved at the data source or ruled out — no curated cluster.
SKIP_RANKS = {
    21: "NOT A FAMILY per curator (GOR1;IFM3 domain relatedness only; "
        "allowlisted in the audit)",
    25: "resolved at source: Cd36_24130 removed from CGOB group 285079, "
        "TOM7 and PPR7 components are now clean 1:1 orthologies",
    27: "resolved at source: CPAR2_400040 removed from CGOB group 284738, "
        "SME1 and MDN1 components are now clean 1:1 orthologies",
}

# Rows whose round-1 decision (SPLIT / not sure) was superseded by the
# second-pass review outcome FAMILY.
FAMILY_OVERRIDE_RANKS = {
    7: "second pass: Not3/Not5 paralog family (clean 2-per-species after "
       "the erroneous KAR2 name was removed from CPAR2_213770)",
    45: "second pass: both are PI-PLC enzymes — paralog family",
    54: "second pass: albicans-specific duplication (CIS308+CSP2) over "
        "single-copy orthologs — co-ortholog family",
}

# Rows merged into a single curated cluster (curator: "merge into one family
# and add NOTES that explain the assignments").
MERGE_GROUPS = [
    {
        "ranks": [20, 58],
        "label": "ATO/FRP family",
        "note": (
            "ATO/FRP transporter family (curator-merged from two "
            "intransitive components, 2026-09). Per-species ATO numbering "
            "reflects discovery order within each species, not cross-species "
            "orthology: e.g. C. auris ATO1 (B9J08_001775) is the syntenic "
            "counterpart of C. albicans ATO7 (C2_02470C_A), and C. auris "
            "ATO2 (B9J08_002831) clusters with C. albicans FRP3 "
            "(C2_06680W_A). ATO/FRP gene numbers should therefore not be "
            "read as implying numbered ortholog correspondence between "
            "species."
        ),
    },
]

# Rows split into explicit curated clusters. Members listed by feature_name;
# any component member not listed is excluded (reported in the summary).
SPLIT_GROUPS = {
    53: {
        "reason": "second pass: conserved albicans+dubliniensis tandem pair; "
                  "curator chose the two tandem columns (FLO9 wall-anchored "
                  "vs IFF11 secreted)",
        "clusters": [
            {"label": "FLO9 orthologs",
             "members": ["C3_00580W_A", "Cd36_80540", "CTRG_02188"]},
            {"label": "IFF11 orthologs",
             "members": ["C3_00600W_A", "Cd36_80550"]},
        ],
    },
    47: {
        "reason": "round 1 SPLIT: MFA1 (43 aa mating pheromone) is not part "
                  "of the MFG1 ortholog set (653-759 aa regulators); MFA1 "
                  "remains a singleton (no cluster needed)",
        "clusters": [
            {"label": "MFG1 orthologs",
             "members": ["C2_08730W_A", "CAGL0K04873g", "CPAR2_806370",
                         "Cd36_22900", "B9J08_005191", "CTRG_01944"]},
        ],
    },
}

# Rows whose decision was RENAME: the component is still a genuine family
# cluster; the naming harmonization itself is separate curator follow-up.
RENAME_FOLLOWUP = {
    10: "RENAME: CGD name stands, alternative becomes an alias "
        "(PSA2/SRB1/VIG9)",
    18: "RENAME: clean up BMH1(A)/BMH1(B) naming variants",
    29: "RENAME: harmonize CAM1/CAM1-1 (CAM1-2 stub retired 2026-09-14)",
    48: "RENAME: all pyruvate kinases (CDC19/PYK1)",
}

# Rows with a blank DECISION cell, loaded on the suggested call but flagged
# for explicit curator confirmation.
ASSUMED_RANKS = {
    4: "DECISION cell blank in round-1 file; loaded as the suggested FAMILY "
       "(ADH/SCR2/SOU1/SOU2 sorbose-reductase-type paralog family) — "
       "confirm with curator",
}

# Label overrides where the worklist's gene names are stale or unwieldy
LABEL_OVERRIDES = {
    7: "NOT3/NOT5 family",   # KAR2 name removed from CPAR2_213770 at source
    18: "BMH1 family",       # BMH1(A)/BMH1(B) naming variants
    52: "ATO3/FRP6 family",  # distinct component from the merged ATO/FRP family
}

MEMBER_RE = re.compile(r"^(?P<species>[^:]+):(?P<feature>[^=;]+)(=(?P<gene>.+))?$")


def parse_members(cell: str) -> list[dict]:
    members = []
    for part in (cell or "").split(";"):
        part = part.strip()
        if not part:
            continue
        m = MEMBER_RE.match(part)
        if not m:
            raise ValueError(f"unparseable member entry: {part!r}")
        members.append({
            "species": m.group("species").strip(),
            "feature_name": m.group("feature").strip(),
            "gene_name": (m.group("gene") or "").strip() or None,
        })
    return members


def name_stem(name: str) -> str:
    """SAP1 -> SAP, CAM1-1 -> CAM, BMH1(A) -> BMH; leading alpha run."""
    m = re.match(r"^([A-Za-z]+)", name)
    return (m.group(1) if m else name).upper()


def make_label(members: list[dict], used: Counter) -> str:
    """Stem-based label; on collision fall back to the full gene names."""
    names = sorted({m["gene_name"] for m in members if m["gene_name"]})
    stems = sorted({name_stem(n) for n in names})
    candidates = ["/".join(stems) + " family" if stems else "unnamed family",
                  ("/".join(names) + " family")[:40]]
    for cand in candidates:
        cand = cand[:40]
        if not used[cand]:
            used[cand] += 1
            return cand
    base = candidates[-1]
    used[base] += 1
    suffix = f" ({used[base]})"
    label = base[:40 - len(suffix)] + suffix
    used[label] += 1
    return label


def load_decisions(xlsx_path: str) -> dict[int, str]:
    wb = openpyxl.load_workbook(xlsx_path)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    hdr = [str(h) if h is not None else "" for h in rows[0]]
    ri, di = hdr.index("rank"), hdr.index("DECISION")
    out = {}
    for row in rows[1:]:
        if row[ri] is None:
            continue
        out[int(row[ri])] = (str(row[di]).strip() if row[di] else "")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--worklist", required=True)
    ap.add_argument("--round1", required=True)
    ap.add_argument("--out-json", default="curated_cluster_plan.json")
    ap.add_argument("--out-tsv", default="curated_cluster_plan_summary.tsv")
    args = ap.parse_args()

    with open(args.worklist, newline="") as fh:
        worklist = {int(r["rank"]): r for r in csv.DictReader(fh, delimiter="\t")}
    decisions = load_decisions(args.round1)

    missing = set(worklist) - set(decisions)
    if missing:
        print(f"WARNING: {len(missing)} worklist ranks absent from round-1 "
              f"decisions: {sorted(missing)}", file=sys.stderr)

    merged_ranks = {r for grp in MERGE_GROUPS for r in grp["ranks"]}
    used_labels: Counter = Counter()
    clusters, skipped, excluded_members = [], [], []

    def add_cluster(label, members, ranks, flags, note=None):
        clusters.append({
            "label": label,
            "ranks": ranks,
            "members": members,
            "note": note,
            "flags": flags,
        })

    # Merged clusters first so their labels win any collision
    for grp in MERGE_GROUPS:
        members, seen = [], set()
        for rank in grp["ranks"]:
            for m in parse_members(worklist[rank]["members"]):
                if m["feature_name"] not in seen:
                    seen.add(m["feature_name"])
                    members.append(m)
        used_labels[grp["label"]] += 1
        add_cluster(grp["label"], members, grp["ranks"],
                    ["curator-directed merge"], note=grp["note"])

    for rank in sorted(worklist):
        if rank in merged_ranks:
            continue
        row = worklist[rank]
        decision = decisions.get(rank, "")
        members = parse_members(row["members"])

        if rank in SKIP_RANKS:
            skipped.append((rank, row["gene_names"], SKIP_RANKS[rank]))
            continue

        if rank in SPLIT_GROUPS:
            spec = SPLIT_GROUPS[rank]
            listed = {fn for c in spec["clusters"] for fn in c["members"]}
            by_name = {m["feature_name"]: m for m in members}
            unknown = listed - set(by_name)
            if unknown:
                raise SystemExit(f"rank {rank}: split members not in "
                                 f"component: {sorted(unknown)}")
            for c in spec["clusters"]:
                used_labels[c["label"]] += 1
                add_cluster(c["label"], [by_name[fn] for fn in c["members"]],
                            [rank], [f"curator-directed split: {spec['reason']}"])
            for fn in sorted(set(by_name) - listed):
                excluded_members.append((rank, fn, by_name[fn]["gene_name"],
                                         "left out of split clusters"))
            continue

        flags = []
        if rank in FAMILY_OVERRIDE_RANKS:
            flags.append(FAMILY_OVERRIDE_RANKS[rank])
        elif rank in RENAME_FOLLOWUP:
            flags.append(RENAME_FOLLOWUP[rank])
        elif rank in ASSUMED_RANKS:
            flags.append(ASSUMED_RANKS[rank])
        elif decision.upper() != "OK":
            raise SystemExit(f"rank {rank} ({row['gene_names']}): unhandled "
                             f"decision {decision!r} — add a directive")
        if rank in LABEL_OVERRIDES:
            label = LABEL_OVERRIDES[rank]
            used_labels[label] += 1
        else:
            label = make_label(members, used_labels)
        add_cluster(label, members, [rank], flags)

    with open(args.out_json, "w") as fh:
        json.dump({"created_for": "curated ortholog family clusters "
                                  "(intransitive-component curation 2026-09)",
                   "clusters": clusters}, fh, indent=1)

    with open(args.out_tsv, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["label", "ranks", "n_members", "members", "flags", "note"])
        for c in clusters:
            w.writerow([
                c["label"], ",".join(map(str, c["ranks"])), len(c["members"]),
                "; ".join(f"{m['feature_name']}"
                          + (f"={m['gene_name']}" if m["gene_name"] else "")
                          for m in c["members"]),
                " | ".join(c["flags"]), c["note"] or "",
            ])
        w.writerow([])
        for rank, genes, why in skipped:
            w.writerow([f"SKIPPED rank {rank}", genes, "", "", why, ""])
        for rank, fn, gn, why in excluded_members:
            w.writerow([f"EXCLUDED member (rank {rank})",
                        f"{fn}={gn or ''}", "", "", why, ""])

    n_members = sum(len(c["members"]) for c in clusters)
    print(f"plan: {len(clusters)} clusters / {n_members} member rows; "
          f"{len(skipped)} components skipped; "
          f"{len(excluded_members)} members excluded by splits")
    for c in clusters:
        if c["flags"]:
            print(f"  FLAGGED {c['label']} (ranks {c['ranks']}): {c['flags'][0]}")
    print(f"wrote {args.out_json} + {args.out_tsv}")


if __name__ == "__main__":
    main()
