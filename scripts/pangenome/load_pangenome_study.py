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
    python scripts/pangenome/load_pangenome_study.py --study bergin2024
    python scripts/pangenome/load_pangenome_study.py --study wang2025
    python scripts/pangenome/load_pangenome_study.py --study xu2025
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


CHROMOSOME_SAMPLE = 7  # reference genes per chromosome, spread along it, used to find its CGD sequence
COORDINATE_TOLERANCE = 1000  # study and CGD gene starts may differ this much (UTRs included or not)


def chromosome_lengths(genes: list[dict], organism_abbrev: str) -> dict[str, int]:
    """
    CGD sequence length of each chromosome the study's genes are placed on, keyed by the study's own
    chromosome name (cpar_Chr_1, ChrA_C_glabrata_CBS138, ...): found through the root sequence of reference
    genes spread along it, so the name need not be CGD's. Selects seq_length only, never the residues.
    A length is used only when those genes all lie on the same CGD sequence near their study coordinates
    and it covers the study's furthest gene: a study on its own assembly (Ryan et al. join CDC317 contigs
    differently on chromosomes 3 and 4) keeps the gene-end estimate.
    """
    placed: dict[str, list[tuple[int, int, str]]] = {}
    for g in genes:
        if g.get("chromosome") and g.get("reference_gene") and g.get("start"):
            placed.setdefault(g["chromosome"], []).append(
                (int(g["start"]), int(g["end"] or g["start"]), g["reference_gene"].upper()))
    if not placed:
        return {}
    samples: dict[str, list[tuple[int, int, str]]] = {}
    for chrom, items in placed.items():
        items.sort()
        step = max(1, (len(items) - 1) // (CHROMOSOME_SAMPLE - 1))
        samples[chrom] = items[::step][:CHROMOSOME_SAMPLE - 1] + [items[-1]]
    sys.path.insert(0, str(PROJECT_ROOT))
    from sqlalchemy import func

    from cgd.db.engine import SessionLocal
    from cgd.models.models import FeatLocation, Feature, Organism, Seq

    names = list({ref for items in samples.values() for _, _, ref in items})
    with SessionLocal() as session:
        rows = (session.query(Feature.feature_name, FeatLocation.root_seq_no, Seq.seq_length,
                              FeatLocation.start_coord, FeatLocation.stop_coord)
                .join(Organism, Organism.organism_no == Feature.organism_no)
                .join(FeatLocation, FeatLocation.feature_no == Feature.feature_no)
                .join(Seq, Seq.seq_no == FeatLocation.root_seq_no)
                .filter(Organism.organism_abbrev == organism_abbrev, FeatLocation.is_loc_current == "Y",
                        Seq.is_seq_current == "Y", func.upper(Feature.feature_name).in_(names))
                .all())
    cgd = {name.upper(): (root, int(length), min(int(a), int(b))) for name, root, length, a, b in rows}
    lengths, rejected = {}, []
    for chrom, items in samples.items():
        found = [(cgd[ref], start) for start, _, ref in items if ref in cgd]
        furthest = max(end for _, end, _ in placed[chrom])
        if (found and len({root for (root, _, _), _ in found}) == 1
                and all(abs(cgd_start - start) <= COORDINATE_TOLERANCE for (_, _, cgd_start), start in found)
                and found[0][0][1] >= furthest):
            lengths[chrom] = found[0][0][1]
        else:
            rejected.append(chrom)
    if rejected:
        logger.info("study coordinates differ from CGD on %s; their lengths are not used", sorted(rejected))
    return lengths


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


# --- Wang et al. 2025, C. glabrata (109 clinical isolates) ------------------------------------------

WANG_FILES = {
    "pangenome": "TEMI_A_2595789_SM5779.xlsx",  # Dataset S7: families x isolates, CBS138 homolog, PG type
    "features": MARCET_FILES["features"],
}
WANG_URLS = {
    WANG_FILES["pangenome"]: "https://pmc-oa-opendata.s3.amazonaws.com/PMC12704144.1/TEMI_A_2595789_SM5779.xlsx",
    WANG_FILES["features"]: MARCET_URLS[MARCET_FILES["features"]],
}
# The paper's four classes; CGD counts core and soft-core together as core, as the paper does
PANGENOME_CLASSES = {"Core": ("core", "Core"), "Softcore": ("core", "Soft-core"),
                     "Shell": ("accessory", "Shell"), "Cloud": ("accessory", "Cloud")}


def parse_wang2025(src: Path) -> tuple[dict, list[dict]]:
    features = cgd_features(src / WANG_FILES["features"])
    ws = openpyxl.load_workbook(src / WANG_FILES["pangenome"], read_only=True, data_only=True).worksheets[0]
    rows = [r for r in ws.iter_rows(values_only=True) if r and r[0]]
    header = [str(h).strip() if h is not None else "" for h in rows[0]]
    homolog_col, class_col = header.index("CBS138_homolog"), header.index("PG_type")
    strain_cols = [(i, h) for i, h in enumerate(header) if 0 < i < homolog_col and h]
    total = len(strain_cols)

    families = []
    for r in rows[1:]:
        # an isolate with several genes in the family lists them joined by "|"
        isolate_genes = {strain: [g.strip() for g in str(r[i]).split("|") if g.strip()]
                         for i, strain in strain_cols if clean(r[i]) not in (None, "-")}
        homolog = clean(r[homolog_col])
        families.append((str(r[0]).strip(), homolog if homolog != "-" else None, clean(r[class_col]), isolate_genes))
    # A CBS138 gene in more than one family (a split gene): the most widespread family carries its name,
    # the others are listed by family ID as variants of it
    named = {}
    for family, homolog, _, isolate_genes in sorted(families, key=lambda f: -len(f[3])):
        if homolog and homolog not in named:
            named[homolog] = family
    missing = sorted(h for h in named if h not in features)
    current = resolve_retired_names(missing, "C_glabrata_CBS138")

    genes = []
    for family, homolog, pg_type, isolate_genes in families:
        category, label = PANGENOME_CLASSES[pg_type]
        feature = (features.get(homolog) or features.get(current.get(homolog, ""))) if homolog else None
        reference = (feature["feature_name"] if feature else current.get(homolog, homolog)) if homolog else None
        own = homolog is not None and named[homolog] == family
        note = f"Gene family {family}"
        if homolog and not own:
            note += f"; also matches {homolog}, which is named for family {named[homolog]}"
        elif not homolog:
            note += "; no CBS138 homolog"
        genes.append({
            "id": homolog if own else family,
            "category": category,
            "in_reference": homolog is not None,
            "reference_gene": reference,
            "presence": len(isolate_genes),
            "type": label,
            # a variant is drawn at the CBS138 gene it matches
            "chromosome": feature["chromosome"] if feature and own else None,
            "start": feature["start"] if feature and own else None,
            "end": feature["end"] if feature and own else None,
            "note": note,
            "isolate_genes": isolate_genes,
            "stop_codons": [],
            "divergent_strains": {},
        })
    study = {
        "isolate_count": total,
        "isolates": [{"name": strain} for _, strain in strain_cols],
        "divergent_strains": [],
        "stop_codons_reported": False,
        "core_definition": f"present in at least 103 of {total} isolates, the paper's core and soft-core",
        "notes": [
            f"Gene families are from Dataset S7 across {total} clinical isolates from China, with each isolate's "
            "gene in the family and the family's CBS138 homolog. Families with a CBS138 homolog are listed by "
            "that gene; the rest by family ID.",
            "The paper classes families as core (all isolates), soft-core, shell or cloud; CGD counts core and "
            "soft-core as core, as the paper does, and shows the paper's class as the type.",
            "Where a CBS138 gene matches more than one family, the most widespread family is listed by the gene "
            "and the others by family ID.",
            "Isolate gene IDs per family are in each gene's details. The study did not report premature stop codons.",
        ],
    }
    return study, genes


# --- Xu et al. 2025, C. auris (1,307 genomes, six clades) -----------------------------------------

XU_FILES = {
    "tables": "EXP2-5-20240482-s001.xlsx",  # Tables S1 (isolates), S3 (assemblies), S4 (gene distribution)
    "features": "C_auris_B8441_current_chromosomal_feature.tab",  # CGD, for B8441 gene coordinates
}
XU_URLS = {
    XU_FILES["tables"]: "https://pmc-oa-opendata.s3.amazonaws.com/PMC12752650.1/EXP2-5-20240482-s001.xlsx",
    XU_FILES["features"]: "https://www.candidagenome.org/download/chromosomal_feature_files/C_auris_B8441/"
                          "C_auris_B8441_current_chromosomal_feature.tab",
}


def parse_xu2025(src: Path) -> tuple[dict, list[dict]]:
    tables = src / XU_FILES["tables"]
    features = cgd_features(src / XU_FILES["features"])
    # each table has a title row, then its header
    s1 = sheet_rows(tables, "Table S1", 3)
    s3 = {str(r[0]).strip(): r for r in sheet_rows(tables, "Table S3", 3)}
    s4 = sheet_rows(tables, "Table S4", 3)

    def isolate(run: str, row) -> dict:
        asm = s3.get(run)
        return {
            # some strain names are numbers in the sheet (9109611)
            "name": str(clean(row[1])) if row is not None and clean(row[1]) is not None else run,
            "origin": ", ".join(str(v) for v in (clean(row[2]), clean(row[12])) if v and v != "unknown")
            if row is not None else None,
            "short_read_accession": run if re.match(r"^[SED]RR\d+$", run) else None,
            "platform": clean(row[10]) if row is not None else None,
            "contigs": clean(asm[2]) if asm else None,
            "assembly_length": clean(asm[1]) if asm else None,
            "n50": clean(asm[3]) if asm else None,
        }
    listed = {str(r[0]).strip() for r in s1}
    isolates = [isolate(str(r[0]).strip(), r) for r in s1]
    # Table S3 has assemblies Table S1 does not list (BJCA001); presence counts include them
    isolates += [isolate(run, None) for run in s3 if run not in listed]
    total = max(int(r[2]) for r in s4)

    b8441 = [str(r[0]).strip() for r in s4 if str(r[0]).startswith("B9J08_")]
    current = resolve_retired_names([g for g in b8441 if g not in features], "C_auris_B8441")
    genes = []
    for r in s4:
        gid, pg_type, presence = str(r[0]).strip(), clean(r[1]), int(r[2])
        category, label = PANGENOME_CLASSES[pg_type]
        in_reference = gid.startswith("B9J08_")
        feature = (features.get(gid) or features.get(current.get(gid, ""))) if in_reference else None
        genes.append({
            "id": gid,
            "category": category,
            "in_reference": in_reference,
            "reference_gene": (feature["feature_name"] if feature else current.get(gid, gid)) if in_reference else None,
            "presence": presence,
            "type": label,
            "chromosome": feature["chromosome"] if feature else None,
            "start": feature["start"] if feature else None,
            "end": feature["end"] if feature else None,
            "note": None if in_reference else "Gene not in the B8441 reference annotation",
            "stop_codons": [],
            "divergent_strains": {},
        })
    study = {
        "isolate_count": total,
        "isolates": isolates,
        "divergent_strains": [],
        "stop_codons_reported": False,
        "core_definition": f"present in more than 95% of the {total:,} genomes, the paper's core and soft-core",
        "notes": [
            f"Gene presence across {total:,} genomes of all six clades is from Table S4: the number of genomes "
            "carrying each gene. The per-isolate presence/absence matrix was not published.",
            f"The paper describes {len(listed):,} isolates (Table S1); presence counts are out of {total:,}, which "
            "includes the assemblies listed only in Table S3 (BJCA001).",
            "The paper classes genes as core (all genomes), soft-core (over 95%), shell (5-95%) or cloud (under 5%); "
            "CGD counts core and soft-core as core and shows the paper's class as the type.",
            "Genes named CA_ are the study's genes with no B8441 counterpart; they are listed but not drawn. "
            "The study did not report premature stop codons.",
        ],
    }
    return study, genes


# --- Bergin et al. 2024, C. parapsilosis gene copy number ---------------------------------------

BERGIN_FILES = {
    "copy_number": "aac.01619-23-s0006.xlsx",  # Table S2: estimated copy number, every CDC317 gene x 42 strains
    "features": "C_parapsilosis_CDC317_current_chromosomal_feature.tab",  # CGD, for CDC317 gene coordinates
}
BERGIN_URLS = {
    BERGIN_FILES["copy_number"]: "https://pmc-oa-opendata.s3.amazonaws.com/PMC11620501.1/aac.01619-23-s0006.xlsx",
    BERGIN_FILES["features"]: "https://www.candidagenome.org/download/chromosomal_feature_files/"
                              "C_parapsilosis_CDC317/C_parapsilosis_CDC317_current_chromosomal_feature.tab",
}
# A gene counts as absent from a strain below this estimated copy number (diploid ~2); the paper sets none
BERGIN_ABSENT_BELOW = 0.2


def parse_bergin2024(src: Path) -> tuple[dict, list[dict]]:
    features = cgd_features(src / BERGIN_FILES["features"])
    ws = openpyxl.load_workbook(src / BERGIN_FILES["copy_number"], read_only=True, data_only=True)["ORF"]
    rows = [r for r in ws.iter_rows(values_only=True) if r and r[0]]
    gene_ids = [str(g).strip() for g in rows[0][1:]]
    strains = [str(r[0]).strip() for r in rows[1:]]
    total = len(strains)

    # CGD names the CDC317 chromosomes by contig; label each by the chromosome its CPAR2 genes are numbered
    # for (CPAR2_1xxxxx on chromosome 1), as Ryan et al. do (cpar_Chr_1), so both studies' maps read alike
    digits: dict[str, dict[str, int]] = {}
    for name, f in features.items():
        m = re.match(r"^CPAR2_(\d)\d{5}", name)
        if m:
            digits.setdefault(f["chromosome"], {}).setdefault(m.group(1), 0)
            digits[f["chromosome"]][m.group(1)] += 1
    chrom_label = {contig: f"cpar_Chr_{max(counts, key=counts.get)}" for contig, counts in digits.items()}

    missing = [g for g in gene_ids if g not in features]
    current = resolve_retired_names(missing, "C_parapsilosis_CDC317")
    genes = []
    for col, gid in enumerate(gene_ids, start=1):
        copies = {strain: round(float(r[col]), 2) for strain, r in zip(strains, rows[1:])}
        absent = [s for s, cn in copies.items() if cn < BERGIN_ABSENT_BELOW]
        feature = features.get(gid) or features.get(current.get(gid, ""))
        genes.append({
            "id": gid,
            "category": "accessory" if absent else "core",
            "in_reference": True,
            "reference_gene": feature["feature_name"] if feature else current.get(gid, gid),
            "presence": total - len(absent),
            "type": f"Absent from {len(absent)} strain{'s' if len(absent) > 1 else ''}" if absent else "CDC317 gene",
            "chromosome": chrom_label.get(feature["chromosome"], feature["chromosome"]) if feature else None,
            "start": feature["start"] if feature else None,
            "end": feature["end"] if feature else None,
            "note": f"Absent from {', '.join(absent)}" if absent else None,
            "copy_number": copies,
            "copy_number_min": min(copies.values()),
            "copy_number_max": max(copies.values()),
            "stop_codons": [],
            "divergent_strains": {},
        })
    study = {
        "isolate_count": total,
        "isolates": [{"name": s} for s in strains],
        "divergent_strains": [],
        "stop_codons_reported": False,
        "copy_number_reported": True,
        "core_definition": f"at least {BERGIN_ABSENT_BELOW} estimated copies in all {total} strains",
        "notes": [
            f"Estimated copy number of every CDC317 gene in {total} strains, from genome sequencing read coverage "
            "(Table S2; a diploid gene with no copy-number change is about 2). The study covers CDC317 genes only, "
            "so it finds no genes outside the reference; see Ryan et al. 2025 for those.",
            f"CGD counts a gene absent from a strain when its estimated copy number is below {BERGIN_ABSENT_BELOW}; "
            "the paper sets no threshold. Accessory genes here are those absent from at least one strain.",
            "High copy numbers can reflect whole-chromosome aneuploidy as well as gene amplification (the paper "
            "reports amplification of CDR1B, CPAR2_304370, in fluconazole-resistant isolates).",
            "Per-strain copy numbers are in each gene's details and the copy number download.",
        ],
    }
    return study, genes


STUDIES = {
    "xu2025": {
        "organism_abbrev": "C_auris_B8441",
        "species": "Candida auris",
        "reference_strain": "B8441",
        "pubmed": 41476647,
        "pmcid": "PMC12752650",
        "doi": "10.1002/EXP.20240482",
        "citation": "Xu C et al. (2025) Global stress responses identify the functionally divergent regulators "
                    "required for Candida auris commensalism and pathogenicity. Exploration 5(6):20240482",
        "title": "Candida auris pangenome (Xu et al. 2025)",
        "license": "CC BY 4.0",
        "data_source": "B8441 gene positions from CGD; Supplementary Tables S1, S3 and S4 of the paper",
        "files": list(XU_FILES.values()),
        "urls": XU_URLS,
        "parser": parse_xu2025,
    },
    "wang2025": {
        "organism_abbrev": "C_glabrata_CBS138",
        "species": "Candida glabrata",
        "reference_strain": "CBS138",
        "pubmed": 41307179,
        "pmcid": "PMC12704144",
        "doi": "10.1080/22221751.2025.2595789",
        "citation": "Wang Q et al. (2025) Genome analysis and machine learning-based feature selection strategy "
                    "reveal potential drug-resistance determinants in Nakaseomyces glabratus. Emerg Microbes Infect "
                    "14(1):2595789",
        "title": "Candida glabrata pangenome (Wang et al. 2025)",
        "license": "CC BY 4.0",
        "data_source": "CBS138 gene positions from CGD; Supplementary Dataset S7 of the paper",
        "files": list(WANG_FILES.values()),
        "urls": WANG_URLS,
        "parser": parse_wang2025,
    },
    "bergin2024": {
        "section": "strain_variation",  # copy number of reference genes, not gene families
        "organism_abbrev": "C_parapsilosis_CDC317",
        "species": "Candida parapsilosis",
        "reference_strain": "CDC317",
        "pubmed": 38712935,
        "pmcid": "PMC11620501",
        "doi": "10.1128/aac.01619-23",
        "citation": "Bergin S et al. (2024) Analysis of clinical Candida parapsilosis isolates reveals copy number "
                    "variation in key fluconazole resistance genes. Antimicrob Agents Chemother 68(6):e0161923",
        "title": "Candida parapsilosis gene copy number (Bergin et al. 2024)",
        "license": "CC BY 4.0",
        "data_source": "CDC317 gene positions from CGD; Supplementary Table S2 of the paper",
        "files": list(BERGIN_FILES.values()),
        "urls": BERGIN_URLS,
        "parser": parse_bergin2024,
    },
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
    try:
        study["chromosome_lengths"] = chromosome_lengths(genes, spec["organism_abbrev"])
        logger.info("chromosome lengths from CGD: %s", study["chromosome_lengths"])
    except Exception as e:  # the map falls back to the furthest gene end
        logger.warning("could not read chromosome lengths from CGD: %s", e)
    (new / "study.json").write_text(json.dumps(study, indent=1))
    (new / "genes.json").write_text(json.dumps(genes))

    shutil.rmtree(prev, ignore_errors=True)
    if final.exists():
        final.rename(prev)
    new.rename(final)
    logger.info("%s loaded into %s: %s", args.study, final, counts)


if __name__ == "__main__":
    main()
