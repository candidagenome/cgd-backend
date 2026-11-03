#!/usr/bin/env python3
"""Load PathoYeastract regulation flat files into the locus Regulation tab.

PathoYeastract publishes one regulation flat file per species, linked from
each species' Download page (e.g. /pathoyeastract/calbicans/download.php):

    https://yeastract-plus.org/Yeastract-Regulations-<Species_strain>-<YYYY-MM-DD>.tsv.gz

One row per evidence observation: TFgene TForf TGgene TGorf Pubmedid Strain
EvidenceCode EvidenceExperiment AssociationType EnvironmentalGroup
EnvironmentalSubgroup EnvironmentalCondition Log2FC.

This script converts the five Candida files into the per-gene JSON that
cgd/api/services/regulation_service.py serves (the same contract
harvest_pathoyeastract.py writes), for every gene with data:

  * <ORF>.json "regulators": documented regulators of the gene, each with its
    evidence rows;
  * <ORF>.json "tf" (TF genes only): the TF's documented targets, flagged by
    evidence type (DNA binding / expression) and direction.

The flat files hold documented associations only. Predicted (consensus)
regulators, binding sites and promoter sequences come from
harvest_pathoyeastract.py; those already in the output directory are carried
over into the new files. Citations for papers not in CGD are fetched from
PubMed (cached). Known PubMed ID errors in the files are corrected
(PMID_CORRECTIONS); evidence rows in other runs of consecutive IDs (an ID
incremented down a spreadsheet column, real paper unknown) are skipped and
logged.

The new tree is built next to the output directory and swapped in at the end,
so the API never reads a half-written set; the previous tree is kept as
<out>.prev.

Usage
-----
  load_pathoyeastract_flatfiles.py --out /data/regulation/pathoyeastract
  load_pathoyeastract_flatfiles.py --out ... --input-dir /tmp/yeastract   # files already downloaded
  load_pathoyeastract_flatfiles.py --out ... --dry-run                    # parse and report only
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import logging
import math
import re
import shutil
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harvest_pathoyeastract import (  # noqa: E402
    BASE_URL,
    SPECIES,
    USER_AGENT,
    Harvester,
    PathoYeastractClient,
)

logger = logging.getLogger("load_pathoyeastract_flatfiles")

FILE_BASE_URL = "https://yeastract-plus.org"
DOWNLOAD_PAGE = f"{BASE_URL}/calbicans/download.php"
CITATION_DOI = "10.1093/nar/gkac1041"
EUTILS_SUMMARY = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

# Flat-file species name -> PathoYeastract species slug
FLAT_FILE_SPECIES = {
    "Candida_albicans_SC5314": "calbicans",
    "Candida_auris_B8441": "cauris",
    "Candida_glabrata_CBS138": "cglabrata",
    "Candida_parapsilosis_CDC317": "cparapsilosis",
    "Candida_tropicalis_MYA_3404": "ctropicalis",
}

# PubMed IDs that PathoYeastract incremented down a column (one paper's ID
# became a run of consecutive, unrelated IDs). Checked against PubMed
# 2026-11-03. slug -> ((first, last), correct PMID)
PMID_CORRECTIONS = {
    # Lotz et al. 2004, RBR1 / RIM101 (Eukaryot Cell); 15189999.. are unrelated
    "calbicans": [((15189999, 15190033), 15189998)],
    # Yap5 competes with Hap4 (Front Cell Infect Microbiol 2021)
    "cglabrata": [((34900751, 34900754), 34900750)],
}
SUSPICIOUS_RUN = 4
RUN_GAP = 2

FLAGS = ("documented", "binding", "expression", "activated", "repressed", "potential")


def fetch(url: str, timeout: int = 300) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as resp:
                return resp.read()
        except OSError as exc:
            if attempt == 2:
                raise
            logger.warning("retrying %s after error: %s", url, exc)
            time.sleep(10 * (attempt + 1))
    raise AssertionError("unreachable")


def latest_files() -> dict[str, str]:
    """Flat-file species name -> file name of the current release."""
    page = fetch(DOWNLOAD_PAGE).decode("utf-8", errors="replace")
    files = {}
    for name, species in re.findall(r'(Yeastract-Regulations-(\w+?)-\d{4}-\d{2}-\d{2}\.tsv\.gz)', page):
        if species in FLAT_FILE_SPECIES:
            files[species] = max(files.get(species, ""), name)
    missing = sorted(set(FLAT_FILE_SPECIES) - set(files))
    if missing:
        raise SystemExit(f"Download page lists no flat file for: {', '.join(missing)}")
    return files


def get_flat_files(input_dir: Path | None, download_dir: Path) -> dict[str, Path]:
    """Flat-file species name -> local .tsv.gz path."""
    if input_dir:
        out = {}
        for species in FLAT_FILE_SPECIES:
            hits = sorted(input_dir.glob(f"*{species}*.tsv.gz"))
            if not hits:
                raise SystemExit(f"No {species} flat file in {input_dir}")
            out[species] = hits[-1]
        return out
    download_dir.mkdir(parents=True, exist_ok=True)
    out = {}
    for species, name in latest_files().items():
        path = download_dir / name
        if not path.exists():
            logger.info("downloading %s", name)
            tmp = path.with_suffix(".part")
            tmp.write_bytes(fetch(f"{FILE_BASE_URL}/{name}"))
            tmp.rename(path)
        out[species] = path
    return out


def release_date(path: Path) -> str | None:
    match = re.search(r"(\d{4}-\d{2}-\d{2})\.tsv\.gz$", path.name)
    return match.group(1) if match else None


def correct_pmid(slug: str, pmid: int | None) -> int | None:
    for (first, last), correct in PMID_CORRECTIONS.get(slug, ()):
        if pmid is not None and first <= pmid <= last:
            return correct
    return pmid


def numbering_runs(slug: str, rows: list[dict]) -> set[int]:
    """PMIDs in runs of (near-)consecutive IDs: an ID incremented down a column.

    Independent papers essentially never take SUSPICIOUS_RUN or more PubMed
    IDs within RUN_GAP of each other, so such runs are numbering errors whose
    real paper is unknown (PathoYeastract's own pages show the unrelated
    papers). Runs with a confirmed real paper are fixed in PMID_CORRECTIONS
    before this check.
    """
    counts = defaultdict(int)
    tfs = defaultdict(set)
    for row in rows:
        if row["pubmed"]:
            counts[row["pubmed"]] += 1
            tfs[row["pubmed"]].add(row["tf_gene"] or row["tf_orf"])
    pmids = sorted(counts)
    bad = set()
    run = pmids[:1]
    for pmid in pmids[1:] + [None]:
        if pmid is not None and pmid - run[-1] <= RUN_GAP:
            run.append(pmid)
            continue
        if len(run) >= SUSPICIOUS_RUN:
            bad.update(run)
            logger.warning("%s: PMIDs %d-%d (%d IDs, %d rows, TFs %s) look like a numbering error; "
                           "their evidence rows are skipped", slug, run[0], run[-1], len(run),
                           sum(counts[p] for p in run), ",".join(sorted(set().union(*(tfs[p] for p in run)))))
        run = [pmid] if pmid is not None else []
    return bad


def read_flat_file(path: Path, slug: str) -> tuple[list[dict], int]:
    """Evidence rows from one flat file, with PMIDs corrected."""
    rows, corrected = [], 0
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
        for raw in csv.DictReader(fh, delimiter="\t"):
            pmid = int(raw["Pubmedid"]) if raw["Pubmedid"].strip().isdigit() else None
            fixed = correct_pmid(slug, pmid)
            corrected += fixed != pmid
            try:
                log2fc = float(raw["Log2FC"])
            except (TypeError, ValueError):
                log2fc = None
            if log2fc is not None and not math.isfinite(log2fc):
                log2fc = None  # 'inf' / 'nan' in the files; not valid JSON

            def clean(key):
                value = (raw.get(key) or "").strip()
                return value or None

            rows.append({
                "tf_gene": clean("TFgene"),
                "tf_orf": raw["TForf"].strip(),
                "target_orf": raw["TGorf"].strip(),
                "pubmed": fixed,
                "evidence_code": clean("EvidenceCode"),
                "experiment": clean("EvidenceExperiment"),
                "association_type": clean("AssociationType"),
                "strain": clean("Strain"),
                "environmental_group": clean("EnvironmentalGroup"),
                "environmental_condition": clean("EnvironmentalCondition"),
                "log2fc": log2fc,
            })
    return rows, corrected


def pubmed_citations(pmids: set[int], cache_path: Path) -> dict[int, str]:
    """PubMed ID -> 'Author X et al., Journal, Date;Vol(Issue):Pages' (cached)."""
    cache = {}
    if cache_path.exists():
        cache = {int(k): v for k, v in json.loads(cache_path.read_text()).items()}
    todo = sorted(p for p in pmids if p not in cache)
    for start in range(0, len(todo), 200):
        chunk = todo[start:start + 200]
        query = urllib.parse.urlencode({"db": "pubmed", "id": ",".join(map(str, chunk)), "retmode": "json"})
        result = json.loads(fetch(f"{EUTILS_SUMMARY}?{query}"))["result"]
        for uid in result.get("uids", []):
            doc = result[uid]
            if doc.get("error"):
                continue
            authors = [a["name"] for a in doc.get("authors") or [] if a.get("authtype") == "Author"]
            who = (authors[0] + (" et al." if len(authors) > 1 else "")) if authors else ""
            where = doc.get("fulljournalname") or doc.get("source") or ""
            when = doc.get("pubdate") or ""
            vol = doc.get("volume") or ""
            issue = f"({doc['issue']})" if doc.get("issue") else ""
            pages = f":{doc['pages']}" if doc.get("pages") else ""
            detail = f"{when};{vol}{issue}{pages}" if vol else when
            cache[int(uid)] = ", ".join(p for p in (who, where, detail) if p)
        time.sleep(0.4)  # NCBI: at most 3 requests/second without an API key
    if todo:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps({str(k): v for k, v in sorted(cache.items())}, indent=1))
        logger.info("PubMed citations: %d fetched, %d cached in total", len(todo), len(cache))
    return cache


def tf_proteins(slug: str, tf_orfs: dict[str, str], old_dir: Path, client) -> dict[str, str]:
    """TF ORF -> PathoYeastract protein name (used in its evidence-page URLs).

    Known names come from the harvester's _tf_orfs.json; the rest are read
    from each TF's PathoYeastract locus page (cached).
    """
    harvester = Harvester(client, slug, old_dir.parent) if client else None
    if harvester:
        tf_orf_map = harvester.tf_orfs
    else:
        tf_orf_map = load_old(old_dir / "_tf_orfs.json")
    known = {orf: protein for protein, orf in tf_orf_map.items() if orf}
    out = {}
    for orf, gene in sorted(tf_orfs.items()):
        protein = known.get(orf)
        if not protein and harvester:
            try:
                protein = harvester.tf_protein(orf)
            except OSError as exc:
                logger.warning("%s: protein name lookup failed for %s: %s", slug, orf, exc)
        out[orf] = protein or gene or orf
    if harvester:
        harvester.tf_orf_path.write_text(json.dumps(harvester.tf_orfs, indent=1, sort_keys=True))
    return out


def load_old(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def build_species(slug: str, rows: list[dict], proteins: dict[str, str], citations: dict[int, str],
                  old_dir: Path, new_dir: Path, source: dict) -> dict:
    """Write one species' per-gene JSON files; return counts."""
    abbrev = SPECIES[slug]
    targets = {row["target_orf"] for row in rows}

    def gene_key(orf: str) -> str:
        # C. albicans B-haplotype rows go to the A record when it has data too
        if orf.endswith("_B") and f"{orf[:-2]}_A" in targets:
            return f"{orf[:-2]}_A"
        return orf

    by_target: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    tf_targets: dict[str, dict[str, dict]] = defaultdict(dict)
    for row in rows:
        target = gene_key(row["target_orf"])
        by_target[target][row["tf_orf"]].append({
            "pubmed": row["pubmed"],
            "citation": citations.get(row["pubmed"]),
            **{k: row[k] for k in ("evidence_code", "experiment", "association_type", "strain",
                                   "environmental_group", "environmental_condition", "log2fc")},
        })
        flags = tf_targets[row["tf_orf"]].setdefault(row["target_orf"], dict.fromkeys(FLAGS, False))
        flags["documented"] = True
        flags["binding"] |= row["evidence_code"] == "Direct"
        flags["expression"] |= row["evidence_code"] == "Indirect"
        flags["activated"] |= "Positive" in (row["association_type"] or "")
        flags["repressed"] |= "Negative" in (row["association_type"] or "")

    out_dir = new_dir / abbrev
    out_dir.mkdir(parents=True, exist_ok=True)
    old_files = {p.stem: p for p in (old_dir.glob("*.json") if old_dir.is_dir() else []) if not p.stem.startswith("_")}
    counts = {"genes": 0, "tf_files": 0, "predictions_kept": 0}

    for orf in sorted(set(by_target) | set(tf_targets) | set(old_files)):
        old = load_old(old_files[orf]) if orf in old_files else {}
        old_regs = {r.get("tf_orf") or r["tf_protein"]: r for r in old.get("regulators") or []}
        regulators = []
        for tf_orf, evidence in sorted(by_target.get(orf, {}).items()):
            prev = old_regs.pop(tf_orf, {})
            regulators.append({
                "tf_protein": proteins.get(tf_orf, tf_orf),
                "tf_orf": tf_orf,
                "evidence": evidence,
                "potential_status": prev.get("potential_status", "not_scanned"),
                "consensus": prev.get("consensus"),
                "sites": prev.get("sites"),
            })
        # Predicted-only regulators from an earlier harvest
        for prev in old_regs.values():
            if prev.get("potential_status") == "found":
                regulators.append({**prev, "evidence": []})
        record = {"orf": orf, "species_slug": slug, "organism_abbrev": abbrev, "regulators": regulators,
                  "promoter": old.get("promoter"), "source": source}
        if old.get("promoter") or any(r.get("potential_status") == "found" for r in regulators):
            counts["predictions_kept"] += 1

        if orf in tf_targets or old.get("tf"):
            old_tf = old.get("tf") or {}
            merged = {t: dict(f) for t, f in tf_targets.get(orf, {}).items()}
            for prev in old_tf.get("targets") or []:
                if prev.get("potential"):
                    merged.setdefault(prev["orf"], dict.fromkeys(FLAGS, False))["potential"] = True
            record["tf"] = {
                "protein": proteins.get(orf, old_tf.get("protein") or orf),
                "consensus": old_tf.get("consensus"),
                "targets": [{"orf": t, **merged[t]} for t in sorted(merged)],
            }
            counts["tf_files"] += 1
        if not regulators and not record.get("tf"):
            continue
        (out_dir / f"{orf}.json").write_text(json.dumps(record, separators=(",", ":")))
        counts["genes"] += 1

    if (old_dir / "_tf_orfs.json").exists():
        shutil.copy2(old_dir / "_tf_orfs.json", out_dir / "_tf_orfs.json")
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", required=True, type=Path, help="Regulation tab data directory "
                        "(e.g. /data/regulation/pathoyeastract)")
    parser.add_argument("--input-dir", type=Path, help="use flat files already downloaded here")
    parser.add_argument("--download-dir", type=Path,
                        help="where downloaded flat files and the citation cache are kept "
                             "(default: <out>/../flatfiles)")
    parser.add_argument("--no-protein-lookup", action="store_true",
                        help="do not query PathoYeastract for unknown TF protein names")
    parser.add_argument("--keep-numbering-runs", action="store_true",
                        help="keep evidence rows whose PMIDs look like a numbering error")
    parser.add_argument("--dry-run", action="store_true", help="parse and report; write nothing")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    out = args.out.resolve()
    download_dir = (args.download_dir or out.parent / "flatfiles").resolve()
    files = get_flat_files(args.input_dir, download_dir)

    parsed = {}
    for species, path in files.items():
        slug = FLAT_FILE_SPECIES[species]
        rows, corrected = read_flat_file(path, slug)
        bad = set() if args.keep_numbering_runs else numbering_runs(slug, rows)
        kept = [r for r in rows if r["pubmed"] not in bad]
        parsed[slug] = (path, kept)
        logger.info("%s: %s — %d rows, %d TFs, %d targets; %d PMIDs corrected, %d rows skipped",
                    slug, path.name, len(rows), len({r['tf_orf'] for r in rows}),
                    len({r['target_orf'] for r in rows}), corrected, len(rows) - len(kept))
    if args.dry_run:
        return 0

    citations = pubmed_citations({r["pubmed"] for _, rows in parsed.values() for r in rows if r["pubmed"]},
                                 download_dir / "_pubmed_citations.json")
    client = None if args.no_protein_lookup else PathoYeastractClient(out / "_cache", delay=1.0)

    new = out.with_name(out.name + ".new")
    if new.exists():
        shutil.rmtree(new)
    new.mkdir(parents=True)
    summary = {"loaded": time.strftime("%Y-%m-%d %H:%M:%S"), "species": {}}
    for slug, (path, rows) in parsed.items():
        old_dir = out / SPECIES[slug]
        tfs = {}
        for row in rows:
            tfs.setdefault(row["tf_orf"], row["tf_gene"])
        proteins = tf_proteins(slug, tfs, old_dir, client)
        source = {"name": "PathoYeastract", "url": f"{BASE_URL}/{slug}/", "citation_doi": CITATION_DOI,
                  "retrieved": release_date(path), "flat_file": path.name}
        counts = build_species(slug, rows, proteins, citations, old_dir, new, source)
        summary["species"][SPECIES[slug]] = {"flat_file": path.name, "rows": len(rows), **counts}
        logger.info("%s: wrote %d gene files (%d TF target lists, %d with harvested predictions)",
                    slug, counts["genes"], counts["tf_files"], counts["predictions_kept"])

    # Keep the harvester's HTML cache with the live tree
    if (out / "_cache").is_dir():
        shutil.move(str(out / "_cache"), str(new / "_cache"))
    (new / "_release.json").write_text(json.dumps(summary, indent=1))

    prev = out.with_name(out.name + ".prev")
    if out.exists():
        if prev.exists():
            shutil.rmtree(prev)
        out.rename(prev)
    new.rename(out)
    logger.info("swapped in %s (previous tree kept at %s)", out, prev)
    return 0


if __name__ == "__main__":
    sys.exit(main())
