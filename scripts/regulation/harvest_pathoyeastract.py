#!/usr/bin/env python3
"""Harvest transcriptional regulation data from PathoYeastract for CGD genes.

PathoYeastract (https://yeastract-plus.org/pathoyeastract/) curates
TF -> target-gene regulatory associations for C. albicans, C. glabrata,
C. parapsilosis, C. tropicalis and C. auris (not C. dubliniensis). Its flat
file downloads and web services are discontinued, so this script reads the
public HTML query pages and writes one JSON file per gene, which the locus
page Regulation tab (cgd/api/services/regulation_service.py) serves.

This is the stop-gap for the Regulation-tab mock-up: it is polite (one
request per --delay seconds, raw HTML cached under <out>/_cache so re-runs
never re-fetch) but it is not meant to mirror the whole database. A
full-genome load should come from a bulk export requested from the
PathoYeastract team; the per-gene JSON written here is the contract that
such an export would be converted to.

For every gene (target-centric, "who regulates this gene?"):
  * documented regulators, each with its evidence rows (PubMed ID, direct
    DNA-binding vs indirect expression evidence, experiment, activator vs
    repressor, strain, environmental condition, log2 fold change);
  * potential regulators: TFs whose consensus binding site occurs in the
    1 kb upstream region, with the matched consensus and its positions;
  * the promoter sequence with every predicted site's coordinates.

With --tfs (TF-centric, "what does this TF regulate?") the TF's ORF file also
gets a genome-wide target list flagged by evidence type and direction.

Documented associations come from "Search for TFs" / "Search for Genes"
(findregulators.php / findregulated.php), whose results agree with the
per-association evidence pages. The "Search for Associations" table
(regassociations.php) is used only for its potential (consensus) column: its
documented column also lists TF-target pairs that have no evidence on
PathoYeastract's own evidence page (2026-10: 90 vs 38 documented regulators of
C. albicans SAP3; 6,015 vs 3,573 documented MRR1 targets).

Usage
-----
  harvest_pathoyeastract.py --species calbicans --genes C3_05230W_A C3_05920W_A \
      --tfs Mrr1p Efg1p --out /data/regulation/pathoyeastract

Output: <out>/<CGD organism_abbrev>/<ORF>.json
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import html
import json
import logging
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

logger = logging.getLogger("harvest_pathoyeastract")

BASE_URL = "https://yeastract-plus.org/pathoyeastract"
USER_AGENT = "CGD-regulation-harvester/1.0 (Candida Genome Database; candidagenome.org)"

# PathoYeastract species slug -> CGD ORGANISM.organism_abbrev
SPECIES = {
    "calbicans": "C_albicans_SC5314",
    "cglabrata": "C_glabrata_CBS138",
    "cparapsilosis": "C_parapsilosis_CDC317",
    "ctropicalis": "C_tropicalis",
    "cauris": "C_auris_B8441",
}

# Systematic-name patterns, used to pick the ORF out of a TF's FASTA header
ORF_PATTERNS = {
    "calbicans": re.compile(r"^C[1-7R]_\d{5}[WC]_[AB]$"),
    "cglabrata": re.compile(r"^CAGL0[A-M]\d{5}g$", re.I),
    "cparapsilosis": re.compile(r"^CPAR2_\d{6}$"),
    "ctropicalis": re.compile(r"^CTRG_\d{5}(\.\d+)?$"),
    "cauris": re.compile(r"^B9J08_\d{5,6}$"),
}

# Common query filters: DNA-binding OR expression evidence, activator,
# repressor or unspecified, no environmental-condition restriction.
BASE_FILTER = {
    "evidence": "plus",
    "t_pos": "true",
    "t_neg": "true",
    "use_na": "true",
    "biggroup": "0",
    "subgroup": "0",
    "association": "oneToOne",
    "submit": "Search",
}

# Extra fields each search form posts (cross-species options left off)
FIND_REGULATORS_FIELDS = {"doc-species": "0", "synteny": "0", "pot-species": "0"}
FIND_REGULATED_FIELDS = {"species": "0", "synteny": "0"}

PROMOTER_START = -1000


class PathoYeastractClient:
    """Throttled, disk-cached HTTP client for the PathoYeastract web pages."""

    def __init__(self, cache_dir: Path, delay: float, refresh: bool = False):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.delay = delay
        self.refresh = refresh
        self._last_request = 0.0
        self.fetched = 0
        self.cached = 0

    def _cache_path(self, url: str, data: dict | None) -> Path:
        key = url + "?" + urllib.parse.urlencode(sorted((data or {}).items()))
        return self.cache_dir / (hashlib.sha1(key.encode()).hexdigest() + ".html")

    def get(self, species: str, page: str, data: dict | None = None,
            params: dict | None = None) -> str:
        url = f"{BASE_URL}/{species}/{page}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        path = self._cache_path(url, data)
        if path.exists() and not self.refresh:
            self.cached += 1
            return path.read_text(encoding="utf-8")

        wait = self.delay - (time.monotonic() - self._last_request)
        if wait > 0:
            time.sleep(wait)
        body = urllib.parse.urlencode(data).encode() if data is not None else None
        request = urllib.request.Request(url, data=body, headers={"User-Agent": USER_AGENT})
        for attempt in range(3):
            try:
                with urllib.request.urlopen(request, timeout=300) as resp:
                    text = resp.read().decode("utf-8", errors="replace")
                break
            except (urllib.error.URLError, TimeoutError) as exc:
                if attempt == 2:
                    raise
                logger.warning("retrying %s after error: %s", url, exc)
                time.sleep(10 * (attempt + 1))
        self._last_request = time.monotonic()
        self.fetched += 1
        path.write_text(text, encoding="utf-8")
        return text


# ---------------------------------------------------------------------------
# HTML parsing helpers
# ---------------------------------------------------------------------------

def _text(fragment: str) -> str:
    fragment = re.sub(r'<span style="display:none">.*?</span>', "", fragment, flags=re.S)
    fragment = re.sub(r"<[^>]+>", " ", fragment)
    return re.sub(r"\s+", " ", html.unescape(fragment)).strip()


def _normalize_orf(name: str) -> str:
    # Target lists occasionally carry a stray '>' (e.g. '>C3_03320W_B')
    return html.unescape(name).lstrip(">").strip()


def _content(page: str) -> str:
    start = page.find('class="content"')
    return page[start:] if start >= 0 else page


def _error_message(page: str) -> str | None:
    match = re.search(r'class="error">([^<]+)', _content(page))
    return match.group(1).strip() if match else None


def _top_level_cells(row_html: str) -> list[str]:
    """The three <td class="align"> cells of a regassociations result row.

    Rows embed javascript menu tables (<td class="jsmenu">), so split on the
    'align' cells only.
    """
    starts = [m.start() for m in re.finditer(r'<td class="align"', row_html)]
    return [row_html[s:e] for s, e in zip(starts, starts[1:] + [len(row_html)])]


def parse_associations(page: str) -> list[dict]:
    """Parse a regassociations.php result into one entry per TF row.

    Each entry: {tf, documented_targets, potential: {target: consensus},
    potential_status} where potential_status is 'found', 'not_found' (the TF
    has a known consensus but it does not occur) or 'no_consensus'.
    """
    content = _content(page)
    anchors = list(re.finditer(
        r'<a name="([^"]+)" href="view\.php\?existing=protein&amp;proteinname=[^"]*">', content))
    rows = []
    for idx, anchor in enumerate(anchors):
        # Rows nest javascript menu tables, so the last row runs to the end of
        # the content rather than to the next </table>
        end = anchors[idx + 1].start() if idx + 1 < len(anchors) else len(content)
        segment = content[anchor.start():end]
        cells = _top_level_cells(content[content.rfind("<tr", 0, anchor.start()):end])
        tf = html.unescape(anchor.group(1))
        documented = [
            _normalize_orf(m) for m in re.findall(
                r'existing=regulation&amp;proteinname=[^&]+&amp;orfname=(>?[^"> ]+)', segment)
        ]
        potential = {
            _normalize_orf(orf): html.unescape(consensus)
            for orf, consensus in re.findall(
                r'viewconsensusinpromoter\.php\?orfname=([^"&]+)&amp;consensus=([^"&]*)', segment)
        }
        pot_cell = cells[2] if len(cells) >= 3 else ""
        if potential:
            status = "found"
        elif "Not found" in pot_cell:
            status = "not_found"
        else:
            status = "no_consensus"
        rows.append({
            "tf": tf,
            "documented_targets": documented,
            "potential": potential,
            "potential_status": status,
        })
    return rows


def parse_documented_pairs(page: str) -> set[tuple[str, str]]:
    """(TF, target ORF) pairs linked to an evidence page in a search result."""
    return {
        (html.unescape(tf), _normalize_orf(orf))
        for tf, orf in re.findall(
            r'existing=regulation&amp;proteinname=([^&"]+)&amp;orfname=(>?[^"> ]+)', _content(page))
    }


def parse_evidence(page: str) -> list[dict]:
    """Parse a view.php?existing=regulation page into evidence rows."""
    content = _content(page)
    start = content.find("Log2FC")
    if start < 0:
        return []
    table = content[start:content.find("</table>", start)]
    evidence = []
    current_ref = {"pubmed": None, "citation": None}
    for row in re.split(r"<tr[^>]*>", table)[1:]:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, flags=re.S)
        if len(cells) < 7:
            continue
        for cell in cells:
            pmid = re.search(r"ncbi\.nlm\.nih\.gov/pubmed/(\d+)", cell)
            if pmid:
                citation = _text(cell).replace("PubMed", "", 1).strip()
                current_ref = {"pubmed": int(pmid.group(1)), "citation": citation}
        code, experiment, association, strain, group, condition, log2fc = (_text(c) for c in cells[-7:])
        try:
            log2fc_value = float(log2fc)
        except ValueError:
            log2fc_value = None
        evidence.append({
            **current_ref,
            "evidence_code": code or None,
            "experiment": experiment or None,
            "association_type": association or None,
            "strain": strain or None,
            "environmental_group": group or None,
            "environmental_condition": condition or None,
            "log2fc": log2fc_value,
        })
    return evidence


def parse_promoter(page: str) -> dict | None:
    """Parse viewconsensusinpromoter.php into sequence + highlighted sites.

    PathoYeastract prints the upstream region (-1000..-1) and its complement
    in the same left-to-right orientation, highlighting each consensus match,
    so a highlighted column index i is promoter coordinate -1000 + i on both
    strands. Positions listed per consensus are kept as reported.
    """
    content = _content(page)
    pres = re.findall(r"<pre>(.*?)</pre>", content, flags=re.S)
    if len(pres) < 2:
        return None
    strands = {}
    sequence = None
    for strand, pre in zip(("+", "-"), pres[:2]):
        lines = re.split(r"<br\s*/?>", pre)
        body = "".join(lines[1:])
        seq_chars, sites, pos = [], [], 0
        for token in re.split(r"(<span[^>]*>.*?</span>)", body, flags=re.S):
            span = re.match(r"<span[^>]*>(.*?)</span>", token, flags=re.S)
            chunk = re.sub(r"<[^>]+>|\s", "", span.group(1) if span else token)
            if span and chunk:
                sites.append([PROMOTER_START + pos, PROMOTER_START + pos + len(chunk) - 1])
            seq_chars.append(chunk)
            pos += len(chunk)
        if strand == "+":
            sequence = "".join(seq_chars).upper()
        strands[strand] = sites

    # Consensus legend: "<small>MMCCASC: -861 -898 </small>" per strand header
    headers = re.findall(r'<th class="align">Promoter Sequence(.*?)</th>', content, flags=re.S)
    legend = {}
    for strand, header in zip(("+", "-"), headers[:2]):
        for motif, positions in re.findall(r"([A-Z0-9{}|/,\[\]]+):\s*((?:-?\d+\s*)+)", _text(header)):
            legend.setdefault(motif, {})[strand] = [int(p) for p in positions.split()]
    return {"sequence": sequence, "sites": strands, "consensus_positions": legend}


def parse_tf_orf(page: str, species: str) -> str | None:
    """Pick the ORF systematic name out of a protein page's FASTA header.

    The sequence is printed as <pre>>MRR1 C3_05920W_A\n... (named genes) or
    <pre>>B9J08_004820 PIS49793\n... (unnamed ones).
    """
    pattern = ORF_PATTERNS[species]
    for header in re.findall(r"<pre>\s*(?:>|&gt;)([^\n<]+)", _content(page)):
        for token in html.unescape(header).split():
            if pattern.match(token):
                return token
    return None


# ---------------------------------------------------------------------------
# Harvest
# ---------------------------------------------------------------------------

class Harvester:
    def __init__(self, client: PathoYeastractClient, species: str, out_dir: Path):
        self.client = client
        self.species = species
        self.out_dir = out_dir / SPECIES[species]
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.tf_orf_path = self.out_dir / "_tf_orfs.json"
        self.tf_orfs = json.loads(self.tf_orf_path.read_text()) if self.tf_orf_path.exists() else {}

    def tf_orf(self, protein: str) -> str | None:
        if protein not in self.tf_orfs:
            page = self.client.get(self.species, "view.php",
                                   params={"existing": "protein", "proteinname": protein})
            self.tf_orfs[protein] = parse_tf_orf(page, self.species)
            self.tf_orf_path.write_text(json.dumps(self.tf_orfs, indent=1, sort_keys=True))
        return self.tf_orfs[protein]

    def tf_protein(self, name: str) -> str | None:
        """Accept a TF as its protein name (Mrr1p) or its ORF (B9J08_004061)."""
        if not ORF_PATTERNS[self.species].match(name):
            return name
        page = self.client.get(self.species, "view.php", params={"existing": "locus", "orfname": name})
        match = re.search(r"existing=protein&amp;proteinname=([^\"&']+)", page)
        if not match:
            logger.error("%s: no protein page linked from locus %s", self.species, name)
            return None
        protein = html.unescape(match.group(1))
        self.tf_orfs.setdefault(protein, name)
        return protein

    def _post(self, page: str, drop: tuple[str, ...] = (), **query) -> str:
        # The server tests checkbox presence, so an unticked filter is omitted
        data = {k: v for k, v in {**BASE_FILTER, **query}.items() if k not in drop}
        return self.client.get(self.species, page, data=data)

    def _associations(self, **query) -> list[dict]:
        page = self._post("regassociations.php", **query)
        error = _error_message(page)
        if error and "No regulatory associations" in error:
            return []
        return parse_associations(page)

    def _documented_regulators(self, orf: str) -> set[str]:
        page = self._post("findregulators.php", genes=orf, **FIND_REGULATORS_FIELDS)
        return {tf for tf, _ in parse_documented_pairs(page)}

    def _documented_targets(self, protein: str, drop: tuple[str, ...] = (), **query) -> set[str]:
        page = self._post("findregulated.php", drop, tfs=protein, **FIND_REGULATED_FIELDS, **query)
        return {target for _, target in parse_documented_pairs(page)}

    def _gene_file(self, orf: str) -> Path:
        return self.out_dir / f"{orf}.json"

    def _load(self, orf: str) -> dict:
        path = self._gene_file(orf)
        if path.exists():
            return json.loads(path.read_text())
        return {"orf": orf, "species_slug": self.species, "organism_abbrev": SPECIES[self.species]}

    def _save(self, record: dict) -> None:
        record["source"] = {
            "name": "PathoYeastract",
            "url": f"{BASE_URL}/{self.species}/",
            "citation_doi": "10.1093/nar/gkac1041",
            "retrieved": datetime.date.today().isoformat(),
        }
        self._gene_file(record["orf"]).write_text(json.dumps(record, indent=1))

    def harvest_gene(self, orf: str) -> dict:
        """Regulators of one target gene, with evidence and promoter sites."""
        rows = {row["tf"]: row for row in self._associations(regulators="", regulated=orf, type="dbtfs")}
        documented_tfs = self._documented_regulators(orf)
        unsupported = sorted(tf for tf, row in rows.items() if row["documented_targets"] and tf not in documented_tfs)
        if unsupported:
            logger.info("%s %s: skipping %d documented-only-in-association-table regulators: %s",
                        self.species, orf, len(unsupported), " ".join(unsupported))

        regulators = []
        promoter = None
        tfs = [tf for tf, row in rows.items() if tf in documented_tfs or row["potential_status"] == "found"]
        tfs += sorted(documented_tfs - set(rows))
        for tf in tfs:
            row = rows.get(tf, {"potential_status": "unknown", "potential": {}})
            entry = {
                "tf_protein": tf,
                "tf_orf": self.tf_orf(tf),
                "evidence": [],
                "potential_status": row["potential_status"],
                "consensus": None,
                "sites": None,
            }
            if tf in documented_tfs:
                page = self.client.get(self.species, "view.php", params={
                    "existing": "regulation", "proteinname": tf, "orfname": orf})
                entry["evidence"] = parse_evidence(page)
            if row["potential_status"] == "found":
                consensus = next(iter(row["potential"].values()))
                entry["consensus"] = consensus.split("|")
                page = self.client.get(self.species, "viewconsensusinpromoter.php", params={
                    "orfname": orf, "consensus": consensus, "subst": "0"})
                parsed = parse_promoter(page)
                if parsed:
                    promoter = promoter or {"start": PROMOTER_START, "end": -1,
                                            "sequence": parsed["sequence"]}
                    entry["sites"] = {
                        "plus": parsed["sites"]["+"],
                        "minus": parsed["sites"]["-"],
                        "consensus_positions": parsed["consensus_positions"],
                    }
            regulators.append(entry)

        record = self._load(orf)
        record["regulators"] = regulators
        record["promoter"] = promoter
        self._save(record)
        n_doc = sum(1 for r in regulators if r["evidence"])
        n_pot = sum(1 for r in regulators if r["potential_status"] == "found")
        logger.info("%s %s: %d documented, %d potential regulators", self.species, orf, n_doc, n_pot)
        return record

    def harvest_tf(self, name: str) -> dict | None:
        """Genome-wide targets of one TF, flagged by evidence type and sign."""
        protein = self.tf_protein(name)
        orf = self.tf_orf(protein) if protein else None
        if not orf:
            logger.error("%s: could not resolve TF %s to an ORF", self.species, name)
            return None

        documented = self._documented_targets(protein)
        binding = self._documented_targets(protein, evidence="dir")
        expression = self._documented_targets(protein, evidence="indir")
        activated = self._documented_targets(protein, drop=("t_neg", "use_na"))
        repressed = self._documented_targets(protein, drop=("t_pos", "use_na"))

        rows = self._associations(regulators=protein, regulated="", type="dbgenes")
        potential = set(rows[0]["potential"]) if rows else set()
        consensus = None
        if rows and rows[0]["potential"]:
            consensus = next(iter(rows[0]["potential"].values())).split("|")

        target_list = [
            {
                "orf": target,
                "documented": target in documented,
                "binding": target in binding,
                "expression": target in expression,
                "activated": target in activated,
                "repressed": target in repressed,
                "potential": target in potential,
            }
            for target in sorted(documented | potential)
        ]
        record = self._load(orf)
        record["tf"] = {"protein": protein, "consensus": consensus, "targets": target_list}
        self._save(record)
        logger.info("%s %s (%s): %d documented (%d DNA binding), %d potential targets",
                    self.species, protein, orf, len(documented), len(binding), len(potential))
        return record


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--species", required=True, choices=sorted(SPECIES))
    parser.add_argument("--genes", nargs="*", default=[], help="target ORF systematic names")
    parser.add_argument("--tfs", nargs="*", default=[], help="TFs, as protein names (Mrr1p) or ORF systematic names")
    parser.add_argument("--out", required=True, type=Path, help="output root (e.g. /data/regulation/pathoyeastract)")
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between requests (default 1.0)")
    parser.add_argument("--refresh", action="store_true", help="ignore the HTML cache and re-fetch")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = PathoYeastractClient(args.out / "_cache", args.delay, args.refresh)
    harvester = Harvester(client, args.species, args.out)
    for orf in args.genes:
        harvester.harvest_gene(orf)
    for protein in args.tfs:
        harvester.harvest_tf(protein)
    logger.info("done: %d pages fetched, %d served from cache", client.fetched, client.cached)
    return 0


if __name__ == "__main__":
    sys.exit(main())
