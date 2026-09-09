#!/usr/bin/env python3
"""QC for ortholog-based gene-name transfers.

Read-only. Implements the curator guidelines for transferring standard names
to uncharacterized Candida spp. genes:

  (1) the target gene has no standard name of its own; and
  (2) at least two orthologous genes share the same name among the six CGD
      species plus S. cerevisiae, and NO ortholog carries a different name.

The script derives each gene's ortholog neighborhood as the union of all
'ortholog' homology groups containing it (groups are mostly pairwise, so a
single group is not the full ortholog set). S. cerevisiae orthologs are not
FEATURE rows; they come from DBXREF (source='SGD', dbxref_type='Gene ID'),
whose description holds the S. cerevisiae standard name.

Modes
-----
propose   Derive the guideline-compliant candidate transfer list from the DB.
          Every unnamed ORF with named-ortholog evidence is emitted with
          status TRANSFER or BLOCKED plus reason codes, so curators see both
          what transfers and what was withheld and why.
check     QC a transfer manifest (TSV with feature_name and gene_name
          columns) row by row. Re-derives the evidence for each row from the
          DB and reports PASS/FAIL/WARN with reason codes. Run it on a
          proposed list before loading, and again after loading (it also
          reports whether the name is applied in the DB).
audit     DB-wide consistency sweep, independent of any manifest: ortholog
          neighborhoods whose members carry different standard names,
          duplicated standard names within a species (allele twins and
          Assembly 21 twins excluded), and CO_ORTHOLOG_COMPONENT findings —
          transitive ortholog components that contain two or more genes from
          one species (expanded families like CDR/PDH, SAP, TLO, where 1:1
          orthology does not exist and pairwise groups disagree).
worklist  The combined curator worklist: one row per conflicted transitive
          component, merging the co-ortholog census with the name-transfer
          QC — each row lists the family members, every standard name in
          play, the S. cerevisiae evidence, and which blocked name transfers
          curating the component would unblock, plus empty DECISION/NOTES
          columns. See docs in the emitted header and the accompanying
          curator instructions file.

          --allowlist FILE suppresses conflicts curators have reviewed and
          accepted (e.g. an established Candida name that diverges from the
          S. cerevisiae standard name, like VID21 vs EAF1). Each line lists
          one accepted conflict as its full set of names — copy the
          conflict_key column of the audit output verbatim ("EAF1;VID21");
          commas/whitespace also work as separators, '#' starts a comment,
          matching is case-insensitive and order-insensitive. A finding is
          only suppressed on an exact name-set match, so if a new name
          joins an accepted conflict it resurfaces. For
          DUPLICATE_NAME_IN_SPECIES findings, a line with the single
          duplicated name suppresses it. The allowlist affects audit output
          ONLY — transfers involving conflicted names stay blocked, because
          an accepted divergence still violates guideline (2).

FAIL codes (violate the guidelines; block the transfer):
  TARGET_ALREADY_NAMED      target has a standard name (rule 1)
  NAME_CONFLICT             orthologs carry more than one distinct name
  INSUFFICIENT_SUPPORT      fewer than two orthologs carry the name
  SCER_ONLY_SUPPORT         the only support is the S. cerevisiae ortholog
  NAME_IN_USE_IN_SPECIES    name already on a different gene of the species
  MULTIPLE_UNNAMED_TARGETS  several unnamed genes of one species would all
                            receive the same name (paralogs; curator call)
  NON_ORF_TARGET            target is not an ORF
  UNKNOWN_FEATURE           manifest feature not found (check mode)
  NO_ORTHOLOG_EVIDENCE      manifest name matches no ortholog name (check)

WARN codes (transfer allowed; curator attention suggested):
  SUPPORT_INCLUDES_RESERVED   a supporting name is reserved, not standardized
  SCER_VIA_NEIGHBOR_ONLY      S. cerevisiae evidence comes from an ortholog's
                              SGD link, not the target's own
  SGD_MULTIPLE_NAMES          neighborhood maps to >1 S. cerevisiae name
  ALIAS_COLLISION             name is an alias of another gene in the species
  RESERVED_NAME_ELSEWHERE     name is reserved by another gene in the species
  B_ALLELE_TARGET             target is a C. albicans _B allele

Usage:
    python scripts/reports/qc_ortholog_name_transfers.py propose \
        [--out proposals.tsv]
    python scripts/reports/qc_ortholog_name_transfers.py check \
        --manifest transfers.tsv [--out qc_report.tsv]
    python scripts/reports/qc_ortholog_name_transfers.py audit \
        [--out audit.tsv] [--allowlist accepted_conflicts.txt]
    python scripts/reports/qc_ortholog_name_transfers.py worklist \
        [--out ortholog_conflict_worklist.tsv]
"""

import argparse
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from sqlalchemy import text  # noqa: E402

from cgd.db.engine import SessionLocal  # noqa: E402

SCER = "S. cerevisiae"


def norm(name):
    """Normalize a gene name for comparison."""
    return name.strip().upper() if name else None


class Snapshot:
    """In-memory, read-only snapshot of everything the QC needs."""

    def __init__(self, db):
        self.org_name = {}          # organism_no -> organism_name
        self.features = {}          # feature_no -> dict
        self.by_name = {}           # UPPER(feature_name) -> feature_no
        self.neighbors = defaultdict(set)        # feature_no -> {feature_no}
        self.methods = defaultdict(set)          # feature_no -> {method}
        self.groups_of = defaultdict(set)        # feature_no -> {group_no}
        self.sgd_names = defaultdict(set)        # feature_no -> {(sgdid, NAME)}
        self.reserved_feats = set()              # feature_no with live reservation
        self.names_in_org = defaultdict(set)     # (org_no, NAME) -> {feature_no}
        self.aliases_in_org = defaultdict(set)   # (org_no, NAME) -> {feature_no}
        self.a21_twins = set()                   # albicans Assembly 21 feature_nos
        self._load(db)

    def _load(self, db):
        for org_no, name in db.execute(text(
                "SELECT organism_no, organism_name FROM organism")):
            self.org_name[org_no] = name

        for fno, fname, gname, org_no, ftype in db.execute(text(
                "SELECT feature_no, feature_name, gene_name, organism_no,"
                " feature_type FROM feature")):
            self.features[fno] = {
                "feature_no": fno, "feature_name": fname,
                "gene_name": gname, "organism_no": org_no,
                "feature_type": ftype,
            }
            self.by_name[fname.upper()] = fno
            if gname:
                self.names_in_org[(org_no, norm(gname))].add(fno)

        # Assembly 21 twins (albicans): the A21 primary feature is the child
        # side of the 'Assembly 21 Primary Allele' relationship, and A21
        # allele-level records (orf19.9xxx, feature_type 'allele') hang off
        # the primary via 'allele' relationships. Both duplicate the A22
        # locus name legitimately, so both are excluded from name-collision
        # checks and from transfer targeting.
        for (fno,) in db.execute(text(
                "SELECT child_feature_no FROM feat_relationship"
                " WHERE relationship_type IN"
                " ('Assembly 21 Primary Allele', 'allele')")):
            self.a21_twins.add(fno)

        members = defaultdict(list)   # group_no -> [feature_no]
        group_method = {}
        for gno, fno, method in db.execute(text(
                "SELECT fh.homology_group_no, fh.feature_no, hg.method"
                " FROM feat_homology fh"
                " JOIN homology_group hg"
                "   ON hg.homology_group_no = fh.homology_group_no"
                " WHERE hg.homology_group_type = 'ortholog'")):
            members[gno].append(fno)
            group_method[gno] = method
        for gno, feats in members.items():
            canon = [self.canonical(f) for f in feats]
            for f in canon:
                self.groups_of[f].add(gno)
                self.methods[f].add(group_method[gno])
                self.neighbors[f].update(c for c in canon if c != f)

        for fno, sgdid, desc in db.execute(text(
                "SELECT df.feature_no, d.dbxref_id, d.description"
                " FROM dbxref_feat df"
                " JOIN dbxref d ON d.dbxref_no = df.dbxref_no"
                " WHERE d.source = 'SGD' AND d.dbxref_type = 'Gene ID'")):
            if desc:
                self.sgd_names[self.canonical(fno)].add((sgdid, norm(desc)))

        for (fno,) in db.execute(text(
                "SELECT feature_no FROM gene_reservation"
                " WHERE date_standardized IS NULL")):
            self.reserved_feats.add(fno)

        for fno, org_no, alias in db.execute(text(
                "SELECT fa.feature_no, f.organism_no, a.alias_name"
                " FROM feat_alias fa"
                " JOIN alias a ON a.alias_no = fa.alias_no"
                " JOIN feature f ON f.feature_no = fa.feature_no")):
            self.aliases_in_org[(org_no, norm(alias))].add(fno)

    def components(self):
        """
        Transitive components of the ortholog graph (union-find over group
        co-membership, allele-canonicalized). Returns a list of components,
        each a sorted list of feature_nos. If orthology were transitive and
        1:1, no component would hold two genes of one species; components
        that do are co-ortholog families where the pairwise groups disagree
        (e.g. CGOB picks CgPDH1 for CaCDR1's glabrata slot while BLAST RBH
        links CgCDR1 via tropicalis).
        """
        parent = {}

        def find(x):
            while parent.setdefault(x, x) != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        for fno, group_nos in self.groups_of.items():
            for other in self.neighbors.get(fno, ()):
                ra, rb = find(fno), find(other)
                if ra != rb:
                    parent[ra] = rb
        comps = defaultdict(list)
        for fno in self.groups_of:
            comps[find(fno)].append(fno)
        return [sorted(members) for members in comps.values()]

    def duplicated_species(self, members):
        """Species (organism_no) appearing more than once in a member list."""
        per_sp = defaultdict(list)
        for fno in members:
            per_sp[self.features[fno]["organism_no"]].append(fno)
        return {sp: feats for sp, feats in per_sp.items() if len(feats) > 1}

    def canonical(self, fno):
        """Map a C. albicans _B allele to its _A twin (one locus, one vote)."""
        feat = self.features.get(fno)
        if feat and feat["feature_name"].endswith("_B"):
            a_twin = self.by_name.get(feat["feature_name"][:-2].upper() + "_A")
            if a_twin:
                return a_twin
        return fno

    def evidence(self, fno):
        """Collect naming evidence across the ortholog neighborhood of fno.

        Returns (cgd_named, scer_named, all_names) where
          cgd_named: NAME -> [(organism_no, feature_name, reserved?)]
          scer_named: NAME -> [(sgdid, direct?)]  direct = target's own link
          all_names: every distinct NAME seen among orthologs
        """
        cgd_named = defaultdict(list)
        scer_named = defaultdict(list)
        for nb in self.neighbors.get(fno, ()):
            feat = self.features[nb]
            name = norm(feat["gene_name"])
            if name:
                cgd_named[name].append((
                    feat["organism_no"], feat["feature_name"],
                    nb in self.reserved_feats,
                ))
        for sgdid, name in self.sgd_names.get(fno, ()):
            scer_named[name].append((sgdid, True))
        for nb in self.neighbors.get(fno, ()):
            for sgdid, name in self.sgd_names.get(nb, ()):
                if not any(s == sgdid for s, _ in scer_named.get(name, ())):
                    scer_named[name].append((sgdid, False))
        all_names = set(cgd_named) | set(scer_named)
        return cgd_named, scer_named, all_names

    def assess(self, fno, proposed=None):
        """Apply the guidelines to feature fno.

        If proposed is given (check mode), judge that name; otherwise derive
        the single candidate name from the evidence (propose mode).
        Returns dict with name, status, fails, warns, and evidence columns.
        """
        feat = self.features[fno]
        cgd_named, scer_named, all_names = self.evidence(fno)
        fails, warns = [], []

        name = norm(proposed) if proposed else None
        if name is None and len(all_names) == 1:
            name = next(iter(all_names))

        if feat["feature_type"] != "ORF":
            fails.append("NON_ORF_TARGET")
        if feat["feature_name"].endswith("_B"):
            warns.append("B_ALLELE_TARGET")

        current = norm(feat["gene_name"])
        if current and current != name:
            fails.append("TARGET_ALREADY_NAMED")

        if len(all_names) > 1:
            fails.append("NAME_CONFLICT")
        if name and name not in all_names:
            fails.append("NO_ORTHOLOG_EVIDENCE")

        support_orgs = set()
        support_detail = []
        if name:
            for org_no, fname, reserved in cgd_named.get(name, ()):
                support_orgs.add(self.org_name[org_no])
                support_detail.append(
                    f"{self.org_name[org_no]}:{fname}"
                    + ("(reserved)" if reserved else ""))
                if reserved:
                    warns.append("SUPPORT_INCLUDES_RESERVED")
            scer_hits = scer_named.get(name, ())
            if scer_hits:
                support_orgs.add(SCER)
                for sgdid, direct in scer_hits:
                    support_detail.append(
                        f"{SCER}:{sgdid}" + ("" if direct else "(via ortholog)"))
                if not any(direct for _, direct in scer_hits):
                    warns.append("SCER_VIA_NEIGHBOR_ONLY")
            if len(support_orgs) < 2:
                fails.append("SCER_ONLY_SUPPORT"
                             if support_orgs == {SCER}
                             else "INSUFFICIENT_SUPPORT")

            org_no = feat["organism_no"]
            holders = {
                self.canonical(h)
                for h in self.names_in_org.get((org_no, name), set())
            } - {fno} - self.a21_twins
            if holders:
                fails.append("NAME_IN_USE_IN_SPECIES")

            alias_holders = self.aliases_in_org.get((org_no, name), set()) - {fno}
            if alias_holders:
                warns.append("ALIAS_COLLISION")
            reserved_holders = {
                h for h in self.names_in_org.get((org_no, name), set())
                if h in self.reserved_feats and h != fno
            }
            if reserved_holders:
                warns.append("RESERVED_NAME_ELSEWHERE")

        if len(self.methods.get(fno, ())) > 1 and len(all_names) > 1:
            warns.append("CROSS_METHOD_DISAGREEMENT")
        if len(scer_named) > 1:
            warns.append("SGD_MULTIPLE_NAMES")

        return {
            "feature": feat,
            "name": name,
            "current_name": feat["gene_name"] or "",
            "fails": sorted(set(fails)),
            "warns": sorted(set(warns)),
            "support_count": len(support_orgs),
            "support_detail": ";".join(sorted(support_detail)),
            "conflict_names": ";".join(sorted(all_names - ({name} if name else set()))),
            "groups": ";".join(str(g) for g in sorted(self.groups_of.get(fno, ()))),
        }


COLUMNS = [
    "feature_name", "organism", "proposed_name", "status", "fail_codes",
    "warn_codes", "current_name", "support_count", "support_detail",
    "conflict_names", "homology_groups",
]


def result_row(snap, res, status):
    feat = res["feature"]
    return [
        feat["feature_name"], snap.org_name[feat["organism_no"]],
        res["name"] or "", status, ",".join(res["fails"]),
        ",".join(res["warns"]), res["current_name"],
        res["support_count"], res["support_detail"],
        res["conflict_names"], res["groups"],
    ]


def flag_duplicate_targets(snap, results):
    """FAIL every proposal where one species+name maps to >1 target gene."""
    by_key = defaultdict(list)
    for res in results:
        if res["name"] and not res["fails"]:
            key = (res["feature"]["organism_no"], res["name"])
            by_key[key].append(res)
    for group in by_key.values():
        if len(group) > 1:
            for res in group:
                res["fails"].append("MULTIPLE_UNNAMED_TARGETS")


def cmd_propose(snap, out):
    results = []
    for fno in sorted(snap.neighbors):
        feat = snap.features[fno]
        if feat["gene_name"] or feat["feature_type"] != "ORF":
            continue
        if feat["feature_name"].endswith("_B") or fno in snap.a21_twins:
            continue
        res = snap.assess(fno)
        if res["name"] is None and not res["conflict_names"]:
            continue                      # no named orthologs at all
        results.append(res)
    flag_duplicate_targets(snap, results)

    writer = csv.writer(out, delimiter="\t", lineterminator="\n")
    writer.writerow(COLUMNS)
    n_ok = 0
    for res in results:
        status = "TRANSFER" if not res["fails"] else "BLOCKED"
        n_ok += status == "TRANSFER"
        writer.writerow(result_row(snap, res, status))
    summarize(results, f"propose: {n_ok} TRANSFER / "
                       f"{len(results) - n_ok} BLOCKED")


def cmd_check(snap, manifest_path, out):
    results, rows = [], []
    with open(manifest_path, newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        cols = {c.lower(): c for c in reader.fieldnames or ()}
        fcol = cols.get("feature_name") or cols.get("feature")
        ncol = (cols.get("proposed_name") or cols.get("gene_name")
                or cols.get("name"))
        if not fcol or not ncol:
            sys.exit("manifest needs feature_name and gene_name/proposed_name"
                     " columns")
        rows = [(r[fcol].strip(), r[ncol].strip()) for r in reader
                if r.get(fcol, "").strip()]

    writer = csv.writer(out, delimiter="\t", lineterminator="\n")
    writer.writerow(COLUMNS + ["applied_in_db"])
    for fname, name in rows:
        fno = snap.by_name.get(fname.upper())
        if fno is None:
            writer.writerow([fname, "", name, "FAIL", "UNKNOWN_FEATURE",
                             "", "", 0, "", "", "", ""])
            continue
        res = snap.assess(fno, proposed=name)
        # Rule 1 nuance for post-load runs: a target whose current DB name IS
        # the manifest name is "applied", not a violation.
        applied = norm(res["current_name"]) == norm(name)
        if applied and "TARGET_ALREADY_NAMED" in res["fails"]:
            res["fails"].remove("TARGET_ALREADY_NAMED")
        results.append(res)
        status = "PASS" if not res["fails"] else "FAIL"
        writer.writerow(result_row(snap, res, status)
                        + ["yes" if applied else "no"])
    summarize(results, f"check: {sum(1 for r in results if not r['fails'])}"
                       f" PASS / {sum(1 for r in results if r['fails'])} FAIL"
                       f" of {len(rows)} manifest rows")


def load_allowlist(path):
    """Read accepted-conflict name sets: one conflict per line, names
    separated by semicolons/commas/whitespace (the audit conflict_key column
    can be pasted verbatim); '#' starts a comment; case-insensitive."""
    allow = set()
    with open(path) as fh:
        for line in fh:
            line = line.split("#", 1)[0].strip()
            names = frozenset(
                norm(tok) for tok in re.split(r"[;,\s]+", line) if tok)
            if names:
                allow.add(names)
    return allow


def cmd_audit(snap, out, allowlist=frozenset()):
    writer = csv.writer(out, delimiter="\t", lineterminator="\n")
    writer.writerow(["check", "organism", "conflict_key", "detail"])
    n = 0
    suppressed = 0

    seen = set()
    for fno in snap.neighbors:
        feat = snap.features[fno]
        if not feat["gene_name"]:
            continue
        _, scer_named, all_names = snap.evidence(fno)
        all_names.add(norm(feat["gene_name"]))
        if len(all_names) > 1:
            key = frozenset(
                [fno] + [n_ for n_ in snap.neighbors[fno]
                         if snap.features[n_]["gene_name"]])
            if key in seen:
                continue
            seen.add(key)
            if frozenset(all_names) in allowlist:
                suppressed += 1
                continue
            members = sorted(
                f"{snap.org_name[snap.features[m]['organism_no']]}:"
                f"{snap.features[m]['feature_name']}="
                f"{snap.features[m]['gene_name']}"
                for m in key)
            members += sorted(
                f"{SCER}:{sgdid}={name}"
                for name, hits in scer_named.items()
                for sgdid, _ in hits)
            writer.writerow(["ORTHOLOG_NAME_CONFLICT",
                             snap.org_name[feat["organism_no"]],
                             ";".join(sorted(all_names)),
                             ";".join(members)])
            n += 1

    for (org_no, name), holders in sorted(snap.names_in_org.items(),
                                          key=lambda kv: (kv[0][0], kv[0][1])):
        canon = {snap.canonical(h) for h in holders} - snap.a21_twins
        if len(canon) > 1:
            if frozenset({name}) in allowlist:
                suppressed += 1
                continue
            writer.writerow([
                "DUPLICATE_NAME_IN_SPECIES", snap.org_name[org_no], name,
                name + ": " + ";".join(sorted(
                    snap.features[h]["feature_name"] for h in canon)),
            ])
            n += 1
    # Co-ortholog components: a species duplicated within one transitive
    # component means the family expanded and 1:1 orthology does not exist
    for members in snap.components():
        dup = snap.duplicated_species(members)
        if not dup:
            continue
        names = sorted({
            norm(snap.features[m]["gene_name"]) for m in members
            if snap.features[m]["gene_name"]
        })
        if names and frozenset(names) in allowlist:
            suppressed += 1
            continue
        dup_orgs = "; ".join(sorted(snap.org_name[sp] for sp in dup))
        detail = "; ".join(
            f"{snap.org_name[snap.features[m]['organism_no']]}:"
            f"{snap.features[m]['feature_name']}"
            + (f"={snap.features[m]['gene_name']}"
               if snap.features[m]["gene_name"] else "")
            for m in members)
        writer.writerow(["CO_ORTHOLOG_COMPONENT", dup_orgs,
                         ";".join(names) or "-", detail])
        n += 1

    print(f"audit: {n} findings"
          + (f" ({suppressed} suppressed by allowlist)" if suppressed else ""),
          file=sys.stderr)


def _name_stem(name):
    """Gene-family stem: strip trailing digits (SAP1 -> SAP, TLO16 -> TLO)."""
    return name.rstrip("0123456789")


def _component_data(snap, members):
    """Everything the worklist views need for one transitive component."""
    dup = snap.duplicated_species(members)
    names = sorted({
        norm(snap.features[m]["gene_name"]) for m in members
        if snap.features[m]["gene_name"]
    })
    blocked = []
    for fno in members:
        feat = snap.features[fno]
        if feat["gene_name"] or feat["feature_type"] != "ORF":
            continue
        if feat["feature_name"].endswith("_B") or fno in snap.a21_twins:
            continue
        res = snap.assess(fno)
        if "NAME_CONFLICT" in res["fails"]:
            blocked.append(feat["feature_name"])
    scer = sorted({name for m in members
                   for _, name in snap.sgd_names.get(m, ())})
    detail = "; ".join(
        f"{snap.org_name[snap.features[m]['organism_no']].replace('Candida ', 'C. ')}:"
        f"{snap.features[m]['feature_name']}"
        + (f"={snap.features[m]['gene_name']}"
           if snap.features[m]["gene_name"] else "")
        for m in members)
    groups = sorted({g for m in members for g in snap.groups_of[m]})
    methods = sorted({m for f in members for m in snap.methods[f]})
    return {
        "members": members, "dup": dup, "names": names, "scer": scer,
        "blocked": sorted(blocked), "detail": detail,
        "groups": groups, "methods": methods,
    }


def cmd_worklist(snap, out, view="families"):
    """
    Curator worklists derived from the conflicted transitive components.

    view=families  Co-ortholog families only: a species duplicated AND >=2
                   named CGD genes (the reviewer-visible intransitivity).
                   likely_family=YES marks rows where every name shares one
                   family stem (SAP1/SAP2/SAP8...) — bulk-acceptable as
                   FAMILY. same_stem_genes_not_in_component lists named genes
                   that look like family members but are absent (they carry
                   no ortholog-group membership), addressing "members seem
                   to be missing".
    view=transfers Name-conflict rows that actually gate transfers under the
                   guidelines: >=1 named CGD gene AND >=2 named entities
                   among the 7 species (S. cerevisiae counts once), so a
                   naming decision could genuinely unblock the transfer.
                   Components with zero named Candida genes are excluded —
                   they can never reach the two-identical-names bar
                   regardless of conflicts (their "conflicts" are between
                   S. cerevisiae paralog links only).
    view=all       The original full census (every conflicted component).
    """
    # Named genes elsewhere in the DB, for the missing-members column
    named_by_stem = defaultdict(list)
    for feat in snap.features.values():
        if feat["gene_name"] and feat["feature_no"] not in snap.a21_twins \
                and not feat["feature_name"].endswith("_B"):
            named_by_stem[_name_stem(norm(feat["gene_name"]))].append(feat)

    comps = [_component_data(snap, members) for members in snap.components()]
    comps = [c for c in comps if c["dup"] or c["blocked"]]

    writer = csv.writer(out, delimiter="\t", lineterminator="\n")

    if view == "families":
        rows = [c for c in comps if c["dup"] and len(c["names"]) >= 2]
        writer.writerow([
            "rank", "gene_names", "likely_family", "n_named", "n_genes",
            "duplicated_species", "scer_names",
            "same_stem_genes_not_in_component", "members",
            "homology_groups", "methods", "DECISION", "NOTES",
        ])
        rows.sort(key=lambda c: (
            len({_name_stem(n) for n in c["names"]}) == 1,  # unclear first
            -len(c["names"]), -len(c["members"]),
        ))
        for rank, c in enumerate(rows, 1):
            stems = {_name_stem(n) for n in c["names"]}
            likely = "YES" if len(stems) == 1 else ""
            in_comp = set(c["members"])
            missing = sorted({
                f"{feat['gene_name']}/{feat['feature_name']}"
                f" ({snap.org_name[feat['organism_no']].replace('Candida ', 'C. ')})"
                for stem in stems
                for feat in named_by_stem.get(stem, ())
                if feat["feature_no"] not in in_comp
            })
            writer.writerow([
                rank, ",".join(c["names"]), likely, len(c["names"]),
                len(c["members"]),
                "; ".join(sorted(snap.org_name[sp] for sp in c["dup"])),
                ",".join(c["scer"]), "; ".join(missing), c["detail"],
                ",".join(str(g) for g in c["groups"]),
                ",".join(c["methods"]), "", "",
            ])
        bulk = sum(1 for c in rows if len({_name_stem(n) for n in c["names"]}) == 1)
        print(f"worklist(families): {len(rows)} rows "
              f"({bulk} likely_family=YES, bulk-acceptable)", file=sys.stderr)

    elif view == "transfers":
        rows = [
            c for c in comps
            if c["blocked"] and c["names"]
            and len(c["names"]) + (1 if c["scer"] else 0) >= 2
        ]
        writer.writerow([
            "rank", "cgd_names", "scer_names", "conflict_type",
            "blocked_transfers", "blocked_targets", "members",
            "DECISION", "NOTES",
        ])

        def conflict_type(c):
            if len(c["names"]) >= 2:
                return "CGD_vs_CGD" if not c["scer"] else "MIXED"
            return "CGD_vs_SCER"

        rows.sort(key=lambda c: (-len(c["blocked"]), len(c["names"])))
        for rank, c in enumerate(rows, 1):
            writer.writerow([
                rank, ",".join(c["names"]), ",".join(c["scer"]),
                conflict_type(c), len(c["blocked"]),
                ",".join(c["blocked"]), c["detail"], "", "",
            ])
        print(f"worklist(transfers): {len(rows)} rows gating "
              f"{sum(len(c['blocked']) for c in rows)} transfers "
              f"(components with no named Candida gene excluded)",
              file=sys.stderr)

    else:  # all — the original census
        writer.writerow([
            "rank", "gene_names", "n_named", "n_genes", "duplicated_species",
            "scer_names", "blocked_transfers", "blocked_targets",
            "members", "homology_groups", "methods", "DECISION", "NOTES",
        ])
        comps.sort(key=lambda c: (-len(c["names"]), -len(c["blocked"]),
                                  -len(c["members"])))
        for rank, c in enumerate(comps, 1):
            writer.writerow([
                rank, ",".join(c["names"]) or "-", len(c["names"]),
                len(c["members"]),
                "; ".join(sorted(snap.org_name[sp] for sp in c["dup"])),
                ",".join(c["scer"]), len(c["blocked"]),
                ",".join(c["blocked"]), c["detail"],
                ",".join(str(g) for g in c["groups"]),
                ",".join(c["methods"]), "", "",
            ])
        print(f"worklist(all): {len(comps)} conflicted components",
              file=sys.stderr)


def summarize(results, headline):
    from collections import Counter
    counts = Counter(code for res in results
                     for code in res["fails"] + res["warns"])
    print(headline, file=sys.stderr)
    for code, cnt in counts.most_common():
        print(f"  {code}: {cnt}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="mode", required=True)
    p = sub.add_parser("propose")
    p.add_argument("--out", type=argparse.FileType("w"), default=sys.stdout)
    p = sub.add_parser("check")
    p.add_argument("--manifest", required=True)
    p.add_argument("--out", type=argparse.FileType("w"), default=sys.stdout)
    p = sub.add_parser("audit")
    p.add_argument("--out", type=argparse.FileType("w"), default=sys.stdout)
    p.add_argument("--allowlist",
                   help="file of accepted conflicts (one name set per line,"
                        " e.g. 'EAF1;VID21'); suppressed from the report")
    p = sub.add_parser("worklist")
    p.add_argument("--out", type=argparse.FileType("w"), default=sys.stdout)
    p.add_argument("--view", choices=["families", "transfers", "all"],
                   default="families",
                   help="families: co-ortholog families needing FAMILY/SPLIT"
                        " calls; transfers: name conflicts that actually gate"
                        " transfers; all: full census")
    args = parser.parse_args()

    with SessionLocal() as db:
        snap = Snapshot(db)
    if args.mode == "propose":
        cmd_propose(snap, args.out)
    elif args.mode == "check":
        cmd_check(snap, args.manifest, args.out)
    elif args.mode == "worklist":
        cmd_worklist(snap, args.out, args.view)
    else:
        allow = load_allowlist(args.allowlist) if args.allowlist else frozenset()
        cmd_audit(snap, args.out, allow)


if __name__ == "__main__":
    main()
