#!/usr/bin/env python3
"""Name-transfer candidates recovered by mutual BLAST best hits.

Read-only. The ortholog name-transfer QC (qc_ortholog_name_transfers.py)
draws its ortholog neighborhoods from 'ortholog' homology groups only (CGOB
pillars and BLAST RBH pairs). A gene that CGOB left out of the pillar — for
example because a local rearrangement broke gene-order support, as for
C. parapsilosis TPS2 (CPAR2_108720) — can then fall below the two-ortholog
support threshold even though BLAST clearly pairs it with the named
orthologs.

This report adds each unnamed ORF's MUTUAL best hits — gene pairs that sit
in both directional 'best hit for <species>' groups (A is B's best hit in
A's species and B is A's best hit in B's species) — to that gene's own
ortholog neighborhood, re-applies the unchanged QC guidelines, and lists
the genes that pass only with this extra evidence. Mutual best hits never
override a conflict: a partner carrying a different name blocks exactly as
an ortholog would.

C. albicans genes appear in best-hit groups as Assembly 22 loci on the hit
side and as Assembly 21 primaries (orf19.*) on the query side; A21 primaries
are mapped to their A22 locus before the two directions are paired.

Output is a curator review list (TSV) with an empty DECISION column. Rows a
curator approves can be applied with
scripts/ortholog_loading/apply_name_transfers.py (feature_name and
proposed_name columns).

Usage:
    python scripts/reports/mutual_best_hit_name_candidates.py \
        [--out mbh_candidates.tsv] [--sgd-features SGD_features.tab]
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from sqlalchemy import text  # noqa: E402

from cgd.db.engine import SessionLocal  # noqa: E402
from scripts.reports.qc_ortholog_name_transfers import (  # noqa: E402
    Snapshot,
    flag_duplicate_targets,
    load_sgd_name_sets,
    norm,
    summarize,
)

COLUMNS = [
    "feature_name", "organism", "proposed_name", "warn_codes",
    "baseline_fail_codes", "baseline_support", "support_count",
    "support_detail", "mutual_best_hits", "cgob_status", "DECISION", "NOTES",
]


class MbhSnapshot(Snapshot):
    """QC snapshot plus directional BLAST best-hit pairs."""

    def __init__(self, db):
        super().__init__(db)
        self.cgob_groups = defaultdict(set)   # feature_no -> {CGOB group_no}
        self.cgob_size = {}                   # CGOB group_no -> member count
        self.mutual = defaultdict(set)        # feature_no -> {feature_no}
        self._load_best_hits(db)

    def _load_best_hits(self, db):
        a21_to_a22 = dict(db.execute(text(
            "SELECT child_feature_no, parent_feature_no FROM feat_relationship"
            " WHERE relationship_type = 'Assembly 21 Primary Allele'")).fetchall())

        def locus(fno):
            return self.canonical(a21_to_a22.get(fno, fno))

        for gno, fno in db.execute(text(
                "SELECT fh.homology_group_no, fh.feature_no"
                " FROM feat_homology fh"
                " JOIN homology_group hg"
                "   ON hg.homology_group_no = fh.homology_group_no"
                " WHERE hg.homology_group_type = 'ortholog'"
                "   AND hg.method = 'CGOB'")):
            self.cgob_groups[locus(fno)].add(gno)
            self.cgob_size[gno] = self.cgob_size.get(gno, 0) + 1

        # 'best hit for C. albicans SC5314' -> organism 'Candida albicans SC5314'
        org_by_name = {name: no for no, name in self.org_name.items()}
        members = defaultdict(list)
        group_org = {}
        for gno, gtype, fno in db.execute(text(
                "SELECT fh.homology_group_no, hg.homology_group_type,"
                "       fh.feature_no"
                " FROM feat_homology fh"
                " JOIN homology_group hg"
                "   ON hg.homology_group_no = fh.homology_group_no"
                " WHERE hg.homology_group_type LIKE 'best hit for %'")):
            members[gno].append(locus(fno))
            species = gtype[len("best hit for "):].replace("C. ", "Candida ", 1)
            group_org[gno] = org_by_name.get(species)

        directed = set()    # (query, hit)
        unparsed = 0
        for gno, feats in members.items():
            hit_org = group_org[gno]
            if len(set(feats)) != 2 or hit_org is None:
                unparsed += 1
                continue
            hits = [f for f in feats if self.features[f]["organism_no"] == hit_org]
            queries = [f for f in feats if f not in hits]
            if len(hits) != 1 or len(queries) != 1:
                unparsed += 1
                continue
            directed.add((queries[0], hits[0]))
        for query, hit in directed:
            if (hit, query) in directed:
                self.mutual[query].add(hit)
        n_pairs = sum(len(v) for v in self.mutual.values()) // 2
        print(f"best hits: {len(directed)} directed pairs, {n_pairs} mutual"
              f" ({unparsed} groups skipped as unpairable)", file=sys.stderr)

    def cgob_status(self, fno, partners):
        groups = self.cgob_groups.get(fno, set())
        if not groups:
            return "not in CGOB"
        if any(groups & self.cgob_groups.get(p, set()) for p in partners):
            return "CGOB pillar shared with a best-hit partner"
        if all(self.cgob_size[g] == 1 for g in groups):
            return "CGOB singleton"
        return "CGOB pillar without its best-hit partners"


def eligible(snap, fno):
    """Same target filter as qc_ortholog_name_transfers propose mode."""
    feat = snap.features[fno]
    return not (feat["gene_name"] or feat["feature_type"] != "ORF"
                or feat["feature_name"].endswith("_B")
                or fno in snap.a21_twins
                or snap.curated_of.get(snap.canonical(fno)))


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=argparse.FileType("w"), default=sys.stdout)
    parser.add_argument("--sgd-features", help="SGD_features.tab for"
                        " alias-aware S. cerevisiae name comparison")
    args = parser.parse_args()

    with SessionLocal() as db:
        snap = MbhSnapshot(db)
    if args.sgd_features:
        snap.sgd_name_sets = load_sgd_name_sets(args.sgd_features)

    baseline = {}       # feature_no -> assess() result without best hits
    for fno in snap.neighbors:
        if eligible(snap, fno):
            baseline[fno] = snap.assess(fno)
    flag_duplicate_targets(snap, list(baseline.values()))

    recovered, added_of = [], {}
    for fno, partners in snap.mutual.items():
        if not eligible(snap, fno):
            continue
        base = baseline.get(fno)
        if base and not base["fails"] and base["name"]:
            continue                      # already a regular TRANSFER
        added = {p for p in partners
                 if p not in snap.neighbors.get(fno, ())
                 and snap.neighbor_visible(fno, p)}
        if not added:
            continue
        original = snap.neighbors.get(fno)
        snap.neighbors[fno] = (original or set()) | added
        res = snap.assess(fno)
        if original is None:
            del snap.neighbors[fno]
        else:
            snap.neighbors[fno] = original
        if res["name"] and not res["fails"]:
            recovered.append(res)
            added_of[fno] = added

    # A recovered name must not collide with another proposal (recovered or
    # regular) for the same species — paralogs are a curator call.
    regular = [r for r in baseline.values() if not r["fails"] and r["name"]]
    flag_duplicate_targets(snap, recovered + regular)
    recovered = [r for r in recovered if not r["fails"]]

    writer = csv.writer(args.out, delimiter="\t", lineterminator="\n")
    writer.writerow(COLUMNS)
    recovered.sort(key=lambda r: (snap.org_name[r["feature"]["organism_no"]],
                                  r["feature"]["feature_name"]))
    for res in recovered:
        feat = res["feature"]
        fno = feat["feature_no"]
        base = baseline.get(fno)
        partners = sorted(
            f"{snap.org_name[snap.features[p]['organism_no']]}:"
            f"{snap.features[p]['feature_name']}"
            + (f"({snap.features[p]['gene_name']})"
               if snap.features[p]["gene_name"] else "")
            for p in added_of[fno])
        # The QC compares upper-cased names; emit the supporting orthologs'
        # own spelling (e.g. MFalpha) so the loader writes it verbatim.
        spellings = sorted(
            snap.features[nb]["gene_name"]
            for nb in snap.neighbors.get(fno, set()) | added_of[fno]
            if norm(snap.features[nb]["gene_name"]) == res["name"])
        name = spellings[0] if spellings else res["name"]
        if base is None:
            base_fails = "NO_ORTHOLOG_GROUPS"
        elif base["name"] is None and not base["fails"]:
            base_fails = "NO_NAMED_ORTHOLOGS"
        else:
            base_fails = ",".join(base["fails"])
        writer.writerow([
            feat["feature_name"], snap.org_name[feat["organism_no"]],
            name, ",".join(res["warns"]), base_fails,
            base["support_count"] if base and base["name"] else 0,
            res["support_count"], res["support_detail"], ";".join(partners),
            snap.cgob_status(fno, snap.mutual[fno]), "", "",
        ])
    summarize(recovered, f"recovered by mutual best hits: {len(recovered)}")


if __name__ == "__main__":
    main()
