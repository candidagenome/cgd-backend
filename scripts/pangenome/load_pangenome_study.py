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
    python scripts/pangenome/load_pangenome_study.py --study marcet2022
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
# CDC317 systematic names, including split genes such as CPAR2_100850.1 (their own CGD loci).
# Study IDs may add a suffix: _A/_B/_C fusion forms, _nontrunc full-length forms of genes
# CDC317 carries truncated (e.g. DAL4).
CPAR2_ID = re.compile(r"^(CPAR2_\d{6}(?:\.\d+)?)")
CPAR2_EXACT = re.compile(r"^CPAR2_\d{6}(?:\.\d+)?$")


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
            g["in_reference"] = bool(CPAR2_EXACT.match(g["id"]))
            g["presence"] = None
            if g["in_reference"]:
                g["type"] = "CDC317 Reference Gene"
            elif g["id"].endswith("_nontrunc"):
                g["type"] = "Full-length form"
                g["note"] = f"Full-length gene; CDC317 carries a truncated copy ({reference_gene})"
            else:
                g["type"] = "Not in CDC317"
        g["reference_gene"] = reference_gene
        # Where to draw a gene the study gives no coordinates for: the CDC317 gene it derives from
        # (CPAR2_100850 for CPAR2_100850.1, CPAR2_103390 for CPAR2_103390_B)
        anchor = re.match(r"^(CPAR2_\d{6})", reference_gene or "")
        g["anchor_gene"] = anchor.group(1) if anchor and not g.get("chromosome") else None
        g["stop_codons"] = stops.get(g["id"], [])

    study = {
        "isolate_count": 376,  # Table S1C presence is given out of 376 genomes
        "isolates": isolates,
        "stop_codon_isolates": [s for _, s in strain_cols],
        "divergent_strains": ["PEU651", "EGM1079"],
        "stop_codons_reported": True,
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


# --- Marcet-Houben et al. 2022, C. glabrata -------------------------------------------------------

MARCET_FILES = {
    "tables": "12915_2022_1412_MOESM2_ESM.xlsx",  # Additional file 2: Tables S1-S8
    "features": "C_glabrata_CBS138_current_chromosomal_feature.tab",  # CGD, for CBS138 gene coordinates
}
MARCET_URLS = {
    MARCET_FILES["tables"]: "https://static-content.springer.com/esm/art%3A10.1186%2Fs12915-022-01412-1/"
                            "MediaObjects/12915_2022_1412_MOESM2_ESM.xlsx",
    MARCET_FILES["features"]: "https://www.candidagenome.org/download/chromosomal_feature_files/C_glabrata_CBS138/"
                              "C_glabrata_CBS138_current_chromosomal_feature.tab",
}
# Table S4 "Filtering" of accessory orthogroups after the authors' manual curation
MARCET_KEEP = {"Genuine": "Accessory (confirmed)", "Unclear": "Accessory (unclear)"}
# Families whose variable presence the authors put down to an artefact: their CBS138 genes are
# listed as core, flagged with the reason; artefact families without a CBS138 gene are not loaded
MARCET_ARTEFACT = {"Sequencing error": "sequencing error", "Miss-prediction": "gene mis-prediction"}
MAX_FAMILY_LISTED = 5


def cgd_features(path: Path) -> dict[str, dict]:
    """CGD chromosomal feature file -> {systematic name or alias: coordinates of the current feature}."""
    features, aliases = {}, {}
    for line in path.read_text().splitlines():
        if line.startswith("!") or not line.strip():
            continue
        f = line.split("\t")
        if len(f) < 7 or not f[5] or not f[6]:
            continue
        start, end = sorted((int(f[5]), int(f[6])))
        features[f[0]] = {"feature_name": f[0], "chromosome": f[4], "start": start, "end": end}
        for alias in f[2].split("|"):
            if alias:
                aliases.setdefault(alias, f[0])
    for alias, name in aliases.items():
        features.setdefault(alias, features[name])
    return features


def resolve_retired_names(names: list[str], organism_abbrev: str) -> dict[str, str]:
    """
    CGD's current systematic name for study gene IDs missing from the feature file, resolved as the
    locus page does: an ID that is a live feature keeps its name; an ID whose own feature is deleted,
    or that is only an alias, goes to the live feature carrying it as an alias (CAGL0B00116g ->
    CAGL0B00154g). IDs with no live match keep their own name.
    """
    if not names:
        return {}
    sys.path.insert(0, str(PROJECT_ROOT))
    from sqlalchemy import func

    from cgd.db.engine import SessionLocal
    from cgd.models.models import Alias, FeatAlias, FeatProperty, Feature, Organism

    upper = [n.upper() for n in names]
    with SessionLocal() as session:
        deleted = {no for (no,) in session.query(FeatProperty.feature_no)
                   .filter(func.lower(FeatProperty.property_value).like("%deleted%"))}
        live = {name.upper() for name, no in session.query(Feature.feature_name, Feature.feature_no)
                .join(Organism, Organism.organism_no == Feature.organism_no)
                .filter(Organism.organism_abbrev == organism_abbrev, func.upper(Feature.feature_name).in_(upper))
                if no not in deleted}
        rows = (session.query(Alias.alias_name, Feature.feature_name, Feature.feature_no)
                .join(FeatAlias, FeatAlias.alias_no == Alias.alias_no)
                .join(Feature, Feature.feature_no == FeatAlias.feature_no)
                .join(Organism, Organism.organism_no == Feature.organism_no)
                .filter(Organism.organism_abbrev == organism_abbrev, func.upper(Alias.alias_name).in_(upper))
                .all())
    renamed = {alias.upper(): feature for alias, feature, no in rows if alias.upper() not in live and no not in deleted}
    return {n: renamed.get(n.upper(), n) for n in names}


def parse_marcet2022(src: Path) -> tuple[dict, list[dict]]:
    tables = src / MARCET_FILES["tables"]
    features = cgd_features(src / MARCET_FILES["features"])

    assemblies = {str(r[0]).strip(): r for r in sheet_rows(tables, "Supplementary_table_1", 2)}
    gene_counts = {str(r[0]).split(" (")[0].strip(): clean(r[1])
                   for r in sheet_rows(tables, "Supplementary_table_3", 2)}

    ws = openpyxl.load_workbook(tables, read_only=True, data_only=True)["Supplementary_table_4"]
    rows = ws.iter_rows(values_only=True)
    header = next(rows)
    strain_cols = [(i, str(h).strip()) for i, h in enumerate(header) if i >= 4 and h not in (None, "")]
    total = len(strain_cols)

    def members(cell) -> list[str]:
        return [] if clean(cell) in (None, "-") else [m.strip() for m in str(cell).split(",") if m.strip()]

    rows = [r for r in rows if r and r[0]]
    kept = [r for r in rows if clean(r[1]) == "Core" or clean(r[2]) in MARCET_KEEP
            or (clean(r[2]) in MARCET_ARTEFACT and members(r[3]))]
    missing = sorted({m for r in kept for m in members(r[3]) if m not in features})
    current = resolve_retired_names(missing, "C_glabrata_CBS138")
    logger.info("%d CBS138 IDs not in the feature file; %d renamed in CGD", len(missing),
                sum(1 for n in missing if current[n] != n))

    genes, skipped, artefact_genes = [], {}, 0
    for r in rows:
        family, tag, filtering = str(r[0]).strip(), clean(r[1]), clean(r[2])
        if r not in kept:
            skipped[filtering] = skipped.get(filtering, 0) + 1
            continue
        isolate_genes = {strain: members(r[i]) for i, strain in strain_cols if members(r[i])}
        if tag == "Core":
            category, kind = "core", "Core gene family"
        elif filtering in MARCET_ARTEFACT:
            category, kind = "core", f"Core (gap: {MARCET_ARTEFACT[filtering]})"
            artefact_genes += len(members(r[3]))
        else:
            category, kind = "accessory", MARCET_KEEP[filtering]
        common = {
            "category": category,
            "presence": len(isolate_genes),
            "type": kind,
            "isolate_genes": isolate_genes,
            "stop_codons": [],
            "divergent_strains": {},
        }
        reference = members(r[3])
        if not reference:
            genes.append({"id": family, "in_reference": False, "reference_gene": None,
                          "chromosome": None, "start": None, "end": None,
                          "note": f"Gene family {family}; no CBS138 gene in the reference annotation", **common})
            continue
        for cagl in reference:
            feature = features.get(cagl) or features.get(current.get(cagl, ""))
            others = [m for m in reference if m != cagl]
            note = f"Gene family {family}"
            if others:
                listed = ", ".join(others[:MAX_FAMILY_LISTED])
                more = len(others) - MAX_FAMILY_LISTED
                note += f" with {len(others)} other CBS138 gene{'s' if len(others) > 1 else ''}: {listed}"
                note += f" and {more} more" if more > 0 else ""
            genes.append({
                "id": cagl,
                "in_reference": True,
                # Current CGD name when the study's ID is now an alias
                "reference_gene": feature["feature_name"] if feature else current.get(cagl, cagl),
                "chromosome": feature["chromosome"] if feature else None,
                "start": feature["start"] if feature else None,
                "end": feature["end"] if feature else None,
                "note": note,
                **common,
            })

    isolates = []
    for _, strain in strain_cols:
        asm = assemblies.get(strain)
        isolates.append({
            "name": strain,
            "assembly_level": "Chromosome-level",
            "platform": "long + short reads",
            "contigs": clean(asm[2]) if asm else None,
            "n50": clean(asm[3]) if asm else None,
            "gene_count": gene_counts.get(strain),
        })
    logger.info("artefact families: %d CBS138 genes listed as core; skipped without a CBS138 gene: %s",
                artefact_genes, skipped)
    study = {
        "isolate_count": total,
        "isolates": isolates,
        "divergent_strains": [],
        "stop_codons_reported": False,
        "core_definition": f"present in all {total} strains",
        "notes": [
            f"Gene families (OrthoFinder orthogroups) are from Table S4 across {total} chromosome-level genomes; "
            "each CBS138 gene in a family is listed with that family's presence, and families with no CBS138 "
            "gene are listed by family ID. The paper counts families: 5,072 core and 323 accessory.",
            "The authors curated the accessory families by hand. Confirmed and unclear families are shown as "
            "accessory. Where they attributed a family's gaps to sequencing errors or gene mis-prediction, its "
            f"{artefact_genes} CBS138 genes are listed as core, flagged with the reason and the strains annotated; "
            f"the {sum(skipped.values())} such families with no CBS138 gene are not shown.",
            "Strain gene IDs per family are in each gene's details. The study did not report premature stop codons.",
            "CBS138 gene positions are from the current CGD annotation; genes since merged or split in CGD "
            "are listed but not drawn.",
        ],
    }
    return study, genes


STUDIES = {
    "marcet2022": {
        "organism_abbrev": "C_glabrata_CBS138",
        "species": "Candida glabrata",
        "reference_strain": "CBS138",
        "pubmed": 36209154,
        "pmcid": "PMC9548116",
        "doi": "10.1186/s12915-022-01412-1",
        "citation": "Marcet-Houben M et al. (2022) Chromosome-level assemblies from diverse clades reveal limited "
                    "structural and gene content variation in the genome of Candida glabrata. BMC Biol 20:226",
        "title": "Candida glabrata pangenome (Marcet-Houben et al. 2022)",
        "license": "CC BY 4.0",
        "data_source": "CBS138 gene positions from CGD; Additional file 2 (Tables S1, S3 and S4) of the paper",
        "files": list(MARCET_FILES.values()),
        "urls": MARCET_URLS,
        "parser": parse_marcet2022,
    },
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
    elif spec.get("urls"):
        for f in spec["files"]:
            logger.info("downloading %s", spec["urls"][f])
            with urllib.request.urlopen(spec["urls"][f], timeout=300) as resp:
                (new / "source" / f).write_bytes(resp.read())
    else:
        fetch_supplements(spec["pmcid"], spec["files"], new / "source")

    study, genes = spec["parser"](new / "source")
    meta = {k: v for k, v in spec.items() if k not in ("parser", "files", "urls")}
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
