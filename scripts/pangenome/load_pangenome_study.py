#!/usr/bin/env python3
"""
Load a published pangenome study into the files the pangenome viewer reads.

Each study is parsed from its paper's supplementary tables into
<PANGENOME_DATA_DIR>/<organism_abbrev>/<study_id>/:

    study.json   metadata (citation, PubMed, counts) and the isolates
    genes.json   one record per pangenome gene: category (core / accessory),
                 whether it is in the reference annotation, the CGD reference
                 gene it maps to, presence counts, notes, coordinates,
                 premature stop codons per isolate, and the study's own
                 extra columns
    source/      the supplementary files as downloaded (provenance)

The directory is built as <study_id>.new and swapped in, keeping the previous
load as <study_id>.prev. Studies are registered in STUDIES with a parser;
adding a species or paper means adding a parser, not changing the viewer.

Usage:
    python scripts/pangenome/load_pangenome_study.py --study ryan2025
    python scripts/pangenome/load_pangenome_study.py --study ryan2025 --source-dir <dir with the .xlsx files>
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import os
import re
import shutil
import sys
import urllib.request
import zipfile
from datetime import date
from pathlib import Path

import openpyxl
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EUROPEPMC_SUPP = "https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/supplementaryFiles"


def data_root() -> Path:
    if os.getenv("PANGENOME_DATA_DIR"):
        return Path(os.environ["PANGENOME_DATA_DIR"])
    return Path(os.getenv("CGD_DATA_DIR", "/data")) / "pangenome"


def sheet_rows(path: Path, sheet: str, min_row: int) -> list[tuple]:
    ws = openpyxl.load_workbook(path, read_only=True, data_only=True)[sheet]
    return [r for r in ws.iter_rows(min_row=min_row, values_only=True) if r and r[0] not in (None, "")]


def clean(value):
    """Spreadsheet cell -> JSON value ('NA', '~' and blanks become None)."""
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        value = value.strip()
        return None if value in ("", "NA", "~", "N/A") else value
    return value


# --- Ryan et al. 2025, C. parapsilosis -------------------------------------------------------------

RYAN_FILES = {
    "isolates": "mbio.01320-25-s0002.xlsx",   # Table S1 (A isolates, B assemblies, C accessory, D stops)
    "divergent": "mbio.01320-25-s0006.xlsx",  # Table S5 (all pangenome genes vs PEU651 / EGM1079)
}
CPAR2_ID = re.compile(r"^(CPAR2_\d{6})")


def parse_ryan2025(src: Path) -> tuple[dict, list[dict]]:
    s1, s5 = src / RYAN_FILES["isolates"], src / RYAN_FILES["divergent"]

    assemblies = {str(r[0]): r for r in sheet_rows(s1, "1B", 3)}
    isolates = []
    for r in sheet_rows(s1, "1A", 3):
        name = str(r[0]).strip()
        asm = assemblies.get(name)
        isolates.append({
            "name": name,
            "origin": clean(r[1]),
            "short_read_source": clean(r[2]),
            "short_read_accession": clean(r[3]),
            "long_read_source": clean(r[4]),
            "long_read_accession": clean(r[5]),
            "assembly_level": clean(asm[10]) if asm else None,
            "platform": clean(asm[11]) if asm else None,
            "contigs": clean(asm[1]) if asm else None,
            "assembly_length": clean(asm[3]) if asm else None,
            "n50": clean(asm[5]) if asm else None,
        })

    accessory = {}
    for r in sheet_rows(s1, "1C", 3):
        accessory[str(r[0]).strip()] = {
            "presence": clean(r[1]),
            "type": clean(r[2]),
            "length_aa": clean(r[3]),
            "closest_reference_gene": clean(r[4]),
            "ortholog": clean(r[5]),
            "blomberg_k": clean(r[6]),
            "phylo_p_value": clean(r[7]),
            "interpro": clean(r[8]),
            "note": clean(r[9]),
        }

    # Table S1D: premature stop codons; per isolate 0 = none, 1 = heterozygous, 2 = homozygous
    d_rows = openpyxl.load_workbook(s1, read_only=True, data_only=True)["1D"].iter_rows(min_row=2, values_only=True)
    header = next(d_rows)
    strain_cols = [(i, str(h)) for i, h in enumerate(header) if i >= 7 and h not in (None, "")]
    stops: dict[str, list] = {}
    for r in d_rows:
        if not r or not r[0]:
            continue
        stops.setdefault(str(r[0]).strip(), []).append({
            "chromosome": clean(r[1]),
            "position": clean(r[2]),
            "aa_position": clean(r[3]),
            "isolates": clean(r[4]),
            "homozygous_isolates": clean(r[5]),
            "heterozygous": [s for i, s in strain_cols if r[i] == 1],
            "homozygous": [s for i, s in strain_cols if r[i] == 2],
        })

    def divergent(cells) -> dict | None:
        presence, hit, identity, status = clean(cells[1]), clean(cells[2]), clean(cells[3]), clean(cells[5])
        if presence is None and hit is None:
            return None
        return {"presence": presence, "coverage": clean(cells[0]), "hit": hit, "identity": clean(identity),
                "hit_status": status}

    genes, seen = [], set()
    for r in sheet_rows(s5, "Comparison to pangenome", 5):
        gid = str(r[0]).strip()
        seen.add(gid)
        genes.append({"id": gid, "length_nt": clean(r[1]), "chromosome": clean(r[2]), "start": clean(r[3]),
                      "end": clean(r[4]),
                      "divergent_strains": {k: v for k, v in (("PEU651", divergent(r[5:11])),
                                                              ("EGM1079", divergent(r[11:17]))) if v}})
    # Accessory genes listed only in Table S1C (fusion variants, some paralogs)
    for gid in accessory:
        if gid not in seen:
            genes.append({"id": gid, "length_nt": None, "chromosome": None, "start": None, "end": None,
                          "divergent_strains": {}})

    for g in genes:
        acc = accessory.get(g["id"])
        base = CPAR2_ID.match(g["id"])
        reference_gene = base.group(1) if base else None
        if acc:
            g["category"] = "accessory"
            g["in_reference"] = bool(acc["type"] and "CDC317 Reference Gene" in acc["type"])
            g["presence"] = acc["presence"]
            g.update({k: acc[k] for k in ("type", "length_aa", "ortholog", "blomberg_k", "phylo_p_value",
                                          "interpro", "note")})
            reference_gene = reference_gene or acc["closest_reference_gene"]
            if not g["in_reference"] and acc["closest_reference_gene"]:
                reference_gene = acc["closest_reference_gene"]
        else:
            g["category"] = "core"
            g["in_reference"] = g["id"].startswith("CPAR2_")
            g["presence"] = None
            g["type"] = "CDC317 Reference Gene" if g["in_reference"] else "Not in CDC317"
        g["reference_gene"] = reference_gene
        g["stop_codons"] = stops.get(g["id"], [])

    study = {
        "isolate_count": 376,  # Table S1C presence is given out of 376 genomes
        "isolates": isolates,
        "stop_codon_isolates": [s for _, s in strain_cols],
        "divergent_strains": ["PEU651", "EGM1079"],
        "core_definition": "present in at least 99.5% of isolates",
        "notes": [
            "The paper describes 372 isolates; accessory gene presence is given as the number of the 376 "
            "genomes analysed that carry the gene (Table S1C). The per-isolate presence/absence matrix was "
            "not published.",
            "Premature stop codons per isolate are from Table S1D (heterozygous or homozygous).",
            "PEU651 and EGM1079 are the two highly divergent isolates compared gene by gene in Table S5.",
        ],
    }
    return study, genes


STUDIES = {
    "ryan2025": {
        "organism_abbrev": "C_parapsilosis_CDC317",
        "species": "Candida parapsilosis",
        "reference_strain": "CDC317",
        "pubmed": 40852993,
        "pmcid": "PMC12505961",
        "doi": "10.1128/mbio.01320-25",
        "citation": "Ryan AP et al. (2025) Small pangenome of Candida parapsilosis reflects overall low "
                    "intraspecific diversity. mBio 16(10):e0132025",
        "title": "Candida parapsilosis pangenome (Ryan et al. 2025)",
        "license": "CC BY 4.0",
        "data_source": "Supplementary Tables S1 and S5 of the paper; reads at NCBI BioProject PRJNA1173375",
        "files": list(RYAN_FILES.values()),
        "parser": parse_ryan2025,
    },
}


def fetch_supplements(pmcid: str, files: list[str], dest: Path) -> None:
    logger.info("downloading supplementary files for %s from Europe PMC", pmcid)
    with urllib.request.urlopen(EUROPEPMC_SUPP.format(pmcid=pmcid), timeout=120) as resp:
        archive = zipfile.ZipFile(io.BytesIO(resp.read()))
    names = {Path(n).name: n for n in archive.namelist()}
    for f in files:
        if f not in names:
            sys.exit(f"{f} not in the {pmcid} supplementary archive")
        (dest / f).write_bytes(archive.read(names[f]))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--study", required=True, choices=sorted(STUDIES))
    ap.add_argument("--source-dir", type=Path, help="directory holding the supplementary files "
                                                    "(default: download from Europe PMC)")
    ap.add_argument("--out-dir", type=Path, help="pangenome data root (default: PANGENOME_DATA_DIR or "
                                                 "<CGD_DATA_DIR>/pangenome)")
    args = ap.parse_args()
    spec = STUDIES[args.study]

    root = (args.out_dir or data_root()) / spec["organism_abbrev"]
    root.mkdir(parents=True, exist_ok=True)
    final, new, prev = root / args.study, root / f"{args.study}.new", root / f"{args.study}.prev"
    shutil.rmtree(new, ignore_errors=True)
    (new / "source").mkdir(parents=True)
    if args.source_dir:
        for f in spec["files"]:
            shutil.copy2(args.source_dir / f, new / "source" / f)
    else:
        fetch_supplements(spec["pmcid"], spec["files"], new / "source")

    study, genes = spec["parser"](new / "source")
    meta = {k: v for k, v in spec.items() if k not in ("parser", "files")}
    study = {"study_id": args.study, **meta, "loaded": date.today().isoformat(), **study}
    counts = {
        "genes": len(genes),
        "core": sum(1 for g in genes if g["category"] == "core"),
        "accessory": sum(1 for g in genes if g["category"] == "accessory"),
        "not_in_reference": sum(1 for g in genes if not g["in_reference"]),
        "with_stop_codons": sum(1 for g in genes if g["stop_codons"]),
        "isolates_listed": len(study["isolates"]),
    }
    study["counts"] = counts
    (new / "study.json").write_text(json.dumps(study, indent=1))
    (new / "genes.json").write_text(json.dumps(genes))

    shutil.rmtree(prev, ignore_errors=True)
    if final.exists():
        final.rename(prev)
    new.rename(final)
    logger.info("%s loaded into %s: %s", args.study, final, counts)


if __name__ == "__main__":
    main()
