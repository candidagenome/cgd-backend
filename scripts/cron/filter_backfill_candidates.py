#!/usr/bin/env python3
"""
Filter backfill review candidates down to the papers worth curating.

Codifies the curator's 2026-09 training decisions on the gene-mention
shortlist. Reads an annotated review TSV (from annotate_backfill_genes.py),
fetches each paper's Medline record for title/abstract/journal/publication
type, and writes the same rows plus auto_decision (Y/N) and auto_reason
columns. Keep = passes every rule below.

Rules (a paper is dropped for the first that fires):
  preprint        journal is a preprint server (bioRxiv/medRxiv/etc.)
  review          PubMed publication type includes Review
  nano            "nano" appears in the journal name
  food            food-science journal (Foods, food/wine/dairy/nutr titles)
  worthless_gene  the ONLY gene tokens are ITS1/ITS2/THP1/HSF (uninformative)
  krusei/kudriavzevii  C. krusei / P. kudriavzevii is not a CGD species yet
  no_genus+species  no CGD-lineage genus paired with a CGD target species
                    (the major split: needs BOTH, e.g. "Candida albicans",
                    not bare "Candida" nor a non-CGD species)

Usage (validate against curator labels, then apply):
    python filter_backfill_candidates.py --in review_genes.tsv \
        --out review_filtered.tsv [--labeled curator_labeled.tsv]
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import time

from Bio import Entrez, Medline

TARGET_SPECIES = ["albicans", "glabrata", "glabratus", "auris", "dubliniensis",
                  "parapsilosis", "tropicalis"]
# C. krusei / Pichia kudriavzevii is not currently a CGD species; per
# curator (2026-09) these should not sit in the queue ahead of the species
# being added — drop with a clear reason, not a vague "no genus+species"
EXCLUDED_SPECIES = ["krusei", "kudriavzevii"]
GENERA = ["candida", "candidozyma", "nakaseomyces", "torulopsis", "pichia"]
PREPRINT_JOURNALS = {"biorxiv", "medrxiv", "arxiv", "research square",
                     "preprints", "ssrn", "chemrxiv"}
WORTHLESS_GENES = {"ITS1", "ITS2", "THP1", "HSF"}
FOOD_JOURNAL_RE = re.compile(r"\bfood|\bfoods\b|wine|dairy|meat|brew|nutr",
                             re.I)
NANO_RE = re.compile(r"nano", re.I)

# Genus (word or single-letter abbrev) immediately followed by a target
# species epithet — the strong "genus AND species" signal
GENUS_SPECIES_RE = re.compile(
    r"\b(?:candida|candidozyma|nakaseomyces|torulopsis|pichia|[CNTP])\.?\s+"
    r"(?:" + "|".join(TARGET_SPECIES) + r")\b", re.I)
EXCLUDED_GENUS_SPECIES_RE = re.compile(
    r"\b(?:candida|pichia|[CP])\.?\s+"
    r"(?:" + "|".join(EXCLUDED_SPECIES) + r")\b", re.I)


def classify(row: dict, rec: dict) -> tuple[str, str]:
    journal = (row.get("journal") or "").lower()
    title = rec.get("TI", "") or row.get("title", "")
    abstract = rec.get("AB", "")
    text = f"{title} {abstract}"
    pub_types = rec.get("PT", []) or []
    genes = [g.strip() for g in (row.get("genes_mentioned") or "").split(",")
             if g.strip() and not g.startswith("(+")]

    if any(p in journal for p in PREPRINT_JOURNALS):
        return "N", "preprint"
    if any("review" in pt.lower() for pt in pub_types):
        return "N", "review"
    if NANO_RE.search(journal):
        return "N", "nano (journal)"
    if FOOD_JOURNAL_RE.search(journal):
        return "N", "food (journal)"
    if genes and all(g in WORTHLESS_GENES for g in genes):
        return "N", "worthless_gene (ITS/THP1/HSF only)"

    if (EXCLUDED_GENUS_SPECIES_RE.search(text)
            and not GENUS_SPECIES_RE.search(text)):
        return "N", "krusei/kudriavzevii (species not in CGD)"

    # major split: a CGD-lineage genus (or genus-initial) immediately
    # followed by a CGD target species — bare genus, a non-CGD species, or
    # an unrelated "X. tropicalis" (e.g. Xenopus) all correctly fail
    if not GENUS_SPECIES_RE.search(text):
        return "N", "no genus+species"

    return "Y", ""


def fetch_records(pmids: list[int]) -> dict[int, dict]:
    records: dict[int, dict] = {}
    for i in range(0, len(pmids), 200):
        batch = pmids[i:i + 200]
        try:
            handle = Entrez.efetch(db="pubmed", id=",".join(map(str, batch)),
                                   rettype="medline", retmode="text")
            for rec in Medline.parse(handle):
                pmid = rec.get("PMID")
                if pmid:
                    records[int(pmid)] = rec
            handle.close()
        except Exception as e:
            print(f"fetch error at {i}: {e}", file=sys.stderr)
        time.sleep(0.4)
    return records


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--in", dest="infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--labeled",
                    help="curator-labeled TSV (decision col) to score against")
    ap.add_argument("--email", default="cgd-curator@lists.stanford.edu")
    args = ap.parse_args()
    Entrez.email = args.email

    with open(args.infile, newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        rows = list(reader)
        fields = reader.fieldnames or []
    pmid_col = "pmid"
    records = fetch_records([int(r[pmid_col]) for r in rows if r[pmid_col]])

    out_fields = ["auto_decision", "auto_reason"] + fields
    keep = 0
    with open(args.out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=out_fields, delimiter="\t",
                           lineterminator="\n")
        w.writeheader()
        decisions = {}
        for r in rows:
            rec = records.get(int(r[pmid_col]), {})
            d, reason = classify(r, rec)
            decisions[r[pmid_col]] = (d, reason)
            keep += d == "Y"
            w.writerow({"auto_decision": d, "auto_reason": reason, **r})
    print(f"kept {keep} / {len(rows)} -> {args.out}", file=sys.stderr)

    if args.labeled:
        lab = {r["pmid"]: (r["decision"] or "").strip().upper()[:1]
               for r in csv.DictReader(open(args.labeled), delimiter="\t")
               if r.get("pmid")}
        tp = tn = fp = fn = 0
        disagreements = []
        for pmid, (auto, reason) in decisions.items():
            truth = lab.get(pmid)
            if truth not in ("Y", "N"):
                continue
            if auto == "Y" and truth == "Y":
                tp += 1
            elif auto == "N" and truth == "N":
                tn += 1
            elif auto == "Y" and truth == "N":
                fp += 1
                disagreements.append((pmid, "auto=Y curator=N", reason))
            else:
                fn += 1
                disagreements.append((pmid, "auto=N curator=Y", reason))
        total = tp + tn + fp + fn
        print(f"\nvalidation vs curator ({total} labeled):", file=sys.stderr)
        print(f"  agree: {tp + tn}/{total} ({100 * (tp + tn) // max(total, 1)}%)",
              file=sys.stderr)
        print(f"  auto-keep curator-drop (false keep): {fp}", file=sys.stderr)
        print(f"  auto-drop curator-keep (MISSED keep): {fn}", file=sys.stderr)
        for pmid, kind, reason in disagreements:
            print(f"    {pmid} {kind} [{reason}]", file=sys.stderr)
    return 0


if __name__ == "__main__":
    main()
