"""
Pangenome viewer: published pangenome studies of CGD species.

Each study is a directory written by scripts/pangenome/load_pangenome_study.py
under <PANGENOME_DATA_DIR>/<organism_abbrev>/<study_id>/ (study.json and
genes.json). Studies are read from disk and cached until their files change;
study genes are linked to current CGD gene names through each study's
reference_gene (the CGD systematic name it maps to).
"""
from __future__ import annotations

import csv
import io
import json
import logging
import time
from pathlib import Path
from typing import Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session, joinedload

from cgd.api.services.locus_service import _filter_features_by_preference
from cgd.core.settings import settings
from cgd.models.models import Feature, Organism, Reference
from cgd.schemas.pangenome_schema import (
    MapChromosome,
    MapGene,
    MapStop,
    PangenomeGene,
    PangenomeGenePage,
    PangenomeLocusResponse,
    PangenomeLocusStudy,
    PangenomeMap,
    PangenomeStudy,
    PangenomeStudyList,
    PangenomeStudySummary,
)

logger = logging.getLogger(__name__)

GENE_NAME_TTL = 600  # seconds; gene names change with curation
SORT_KEYS = {
    "id": lambda g: g["id"],
    "presence": lambda g: (g.get("presence") is None, g.get("presence") or 0),
    "stops": lambda g: -len({s for stop in g.get("stop_codons") or []
                             for s in stop.get("heterozygous", []) + stop.get("homozygous", [])}),
    # genes without coordinates (most not in the reference) follow, by ID
    "position": lambda g: (g.get("chromosome") or "~", g.get("start") or 0, g["id"]),
}

_studies: dict[str, dict] = {}  # study_id -> {"mtime", "study", "genes", "by_reference"}
_gene_names: dict[str, tuple[float, dict]] = {}  # organism_abbrev -> (loaded at, feature_name -> gene_name)


class PangenomeNotFound(Exception):
    pass


def _data_dir() -> Path:
    if settings.pangenome_data_dir:
        return Path(settings.pangenome_data_dir)
    return Path(settings.cgd_data_dir) / "pangenome"


def _study_dirs() -> dict[str, Path]:
    """study_id -> directory, for every loaded study (skipping .new / .prev copies)."""
    root = _data_dir()
    if not root.is_dir():
        return {}
    return {d.name: d for d in sorted(root.glob("*/*")) if (d / "study.json").is_file() and "." not in d.name}


def _load(study_id: str) -> dict:
    directory = _study_dirs().get(study_id)
    if directory is None:
        raise PangenomeNotFound(f"No pangenome study '{study_id}'")
    mtime = (directory / "genes.json").stat().st_mtime
    cached = _studies.get(study_id)
    if cached and cached["mtime"] == mtime:
        return cached
    study = json.loads((directory / "study.json").read_text())
    genes = json.loads((directory / "genes.json").read_text())
    by_reference: dict[str, list[dict]] = {}
    for gene in genes:
        if gene.get("reference_gene"):
            by_reference.setdefault(gene["reference_gene"].upper(), []).append(gene)
    cached = {"mtime": mtime, "study": study, "genes": genes, "by_reference": by_reference}
    _studies[study_id] = cached
    return cached


def _gene_name_map(db: Session, organism_abbrev: str) -> dict[str, str]:
    """Current CGD gene names by feature_name for one organism (cached for GENE_NAME_TTL)."""
    cached = _gene_names.get(organism_abbrev)
    if cached and time.time() - cached[0] < GENE_NAME_TTL:
        return cached[1]
    rows = (
        db.query(Feature.feature_name, Feature.gene_name)
        .join(Organism, Organism.organism_no == Feature.organism_no)
        .filter(Organism.organism_abbrev == organism_abbrev, Feature.gene_name.isnot(None))
        .all()
    )
    names = {feature_name.upper(): gene_name for feature_name, gene_name in rows}
    _gene_names[organism_abbrev] = (time.time(), names)
    return names


def _summary(db: Session, study: dict) -> PangenomeStudySummary:
    dbxref_id = None
    if study.get("pubmed"):
        dbxref_id = db.query(Reference.dbxref_id).filter(Reference.pubmed == study["pubmed"]).scalar()
    fields = PangenomeStudySummary.model_fields
    return PangenomeStudySummary(**{k: v for k, v in study.items() if k in fields},
                                 reference_dbxref_id=dbxref_id)


def _gene(record: dict, names: dict[str, str], with_stops: bool = True) -> PangenomeGene:
    stops = record.get("stop_codons") or []
    isolates_with_stop = {s for stop in stops for s in stop.get("heterozygous", []) + stop.get("homozygous", [])}
    reference = record.get("reference_gene")
    fields = PangenomeGene.model_fields
    data = {k: v for k, v in record.items() if k in fields and k not in ("stop_codons", "divergent_strains")}
    return PangenomeGene(
        **data,
        gene_name=names.get(reference.upper()) if reference else None,
        stop_codon_count=len(stops),
        stop_codon_isolates=len(isolates_with_stop),
        stop_codons=stops if with_stops else [],
        divergent_strains=record.get("divergent_strains") or {},
    )


def list_studies(db: Session) -> PangenomeStudyList:
    return PangenomeStudyList(studies=[_summary(db, _load(study_id)["study"]) for study_id in _study_dirs()])


def get_study(db: Session, study_id: str) -> PangenomeStudy:
    study = _load(study_id)["study"]
    summary = _summary(db, study)
    return PangenomeStudy(**summary.model_dump(), notes=study.get("notes", []),
                          divergent_strains=study.get("divergent_strains", []),
                          isolates=study.get("isolates", []))


def _matches(gene: dict, names: dict[str, str], category: Optional[str], reference: Optional[str],
             stops: Optional[bool], query: Optional[str]) -> bool:
    if category and gene["category"] != category:
        return False
    if reference == "in" and not gene["in_reference"]:
        return False
    if reference == "novel" and gene["in_reference"]:
        return False
    if stops is not None and bool(gene.get("stop_codons")) != stops:
        return False
    if query:
        q = query.strip().upper()
        ref = (gene.get("reference_gene") or "").upper()
        haystack = [gene["id"].upper(), ref, (names.get(ref) or "").upper(), (gene.get("ortholog") or "").upper(),
                    (gene.get("type") or "").upper(), (gene.get("note") or "").upper(),
                    (gene.get("interpro") or "").upper()]
        if not any(q in h for h in haystack):
            return False
    return True


def list_genes(db: Session, study_id: str, category: Optional[str] = None, reference: Optional[str] = None,
               stops: Optional[bool] = None, query: Optional[str] = None, sort: str = "position",
               page: int = 1, page_size: int = 50) -> PangenomeGenePage:
    data = _load(study_id)
    names = _gene_name_map(db, data["study"]["organism_abbrev"])
    genes = [g for g in data["genes"] if _matches(g, names, category, reference, stops, query)]
    genes.sort(key=SORT_KEYS.get(sort, SORT_KEYS["position"]))
    start = (page - 1) * page_size
    return PangenomeGenePage(study_id=study_id, total=len(genes), page=page, page_size=page_size,
                             genes=[_gene(g, names, with_stops=False) for g in genes[start:start + page_size]])


def get_map(db: Session, study_id: str) -> PangenomeMap:
    """
    Every gene placed on the study's chromosomes, for the graphical browser.

    Genes without their own coordinates (paralogs, fusions and other variants
    not in the reference) are placed at the reference gene they derive from,
    or at the study's anchor_gene for them; the rest are returned as unplaced. Chromosome lengths are the
    furthest gene end, as the study gives no assembly lengths.
    """
    data = _load(study_id)
    names = _gene_name_map(db, data["study"]["organism_abbrev"])
    coords = {g["id"].upper(): g for g in data["genes"] if g.get("chromosome") and g.get("start")}
    lengths: dict[str, int] = {}
    genes, unplaced = [], []
    for g in data["genes"]:
        ref = g.get("reference_gene")
        anchor = (coords.get(g["id"].upper()) or (coords.get(ref.upper()) if ref else None)
                  or (coords.get(g["anchor_gene"].upper()) if g.get("anchor_gene") else None))
        stops = [MapStop(position=s.get("position"), isolates=len(s.get("heterozygous", []) + s.get("homozygous", [])),
                         homozygous=len(s.get("homozygous", [])))
                 for s in g.get("stop_codons") or [] if s.get("position")]
        record = MapGene(id=g["id"], reference_gene=ref, gene_name=names.get(ref.upper()) if ref else None,
                         category=g["category"], in_reference=g["in_reference"], presence=g.get("presence"),
                         type=g.get("type"), ortholog=g.get("ortholog"), stops=stops)
        if anchor is None:
            unplaced.append(record)
            continue
        start, end = sorted((int(anchor["start"]), int(anchor["end"] or anchor["start"])))
        record.chromosome, record.start, record.end = anchor["chromosome"], start, end
        record.placed_by = "own" if anchor is g else "reference_gene"
        record.drawn_at = None if anchor is g else anchor["id"]
        lengths[anchor["chromosome"]] = max(lengths.get(anchor["chromosome"], 0), end)
        genes.append(record)

    def chrom_key(name: str):
        number = name.rsplit("_", 1)[-1]
        return (0, int(number), name) if number.isdigit() else (1, 0, name)
    chromosomes = [MapChromosome(name=c, length=lengths[c]) for c in sorted(lengths, key=chrom_key)]
    genes.sort(key=lambda r: (r.chromosome, r.start, r.placed_by != "own", r.id))
    return PangenomeMap(study_id=study_id, isolate_count=data["study"]["isolate_count"],
                        chromosomes=chromosomes, genes=genes, unplaced=unplaced)


def get_gene(db: Session, study_id: str, gene_id: str) -> PangenomeGene:
    """One study gene with its premature stop codons per isolate."""
    data = _load(study_id)
    for gene in data["genes"]:
        if gene["id"].upper() == gene_id.strip().upper():
            return _gene(gene, _gene_name_map(db, data["study"]["organism_abbrev"]))
    raise PangenomeNotFound(f"No gene '{gene_id}' in {study_id}")


def get_locus_pangenome(db: Session, name: str) -> PangenomeLocusResponse:
    """Pangenome records for a locus: the study's copy of the gene plus accessory genes derived from it."""
    studies_by_organism: dict[str, list[str]] = {}
    for study_id, directory in _study_dirs().items():
        studies_by_organism.setdefault(directory.parent.name, []).append(study_id)
    if not studies_by_organism:
        return PangenomeLocusResponse()

    n = name.strip()
    features = (
        db.query(Feature)
        .options(joinedload(Feature.organism))
        .join(Organism, Organism.organism_no == Feature.organism_no)
        .filter(Organism.organism_abbrev.in_(list(studies_by_organism)))
        .filter(or_(func.upper(Feature.gene_name) == func.upper(n),
                    func.upper(Feature.feature_name) == func.upper(n),
                    func.upper(Feature.dbxref_id) == func.upper(n)))
        .filter(func.lower(Feature.feature_type) != "allele")
        .all()
    )
    features = _filter_features_by_preference(db, features)
    for feature in features:
        abbrev = feature.organism.organism_abbrev
        names = _gene_name_map(db, abbrev)
        found = []
        for study_id in studies_by_organism.get(abbrev, []):
            data = _load(study_id)
            records = data["by_reference"].get(feature.feature_name.upper(), [])
            if records:
                records = sorted(records, key=lambda g: (g["id"] != feature.feature_name, g["id"]))
                found.append(PangenomeLocusStudy(study=_summary(db, data["study"]),
                                                 isolate_count=data["study"]["isolate_count"],
                                                 genes=[_gene(g, names) for g in records]))
        if found:
            return PangenomeLocusResponse(feature_name=feature.feature_name, gene_name=feature.gene_name,
                                          organism=feature.organism.organism_name, studies=found)
    return PangenomeLocusResponse()


DOWNLOAD_TABLES = ("genes", "accessory", "stop_codons", "isolates")


def download_table(db: Session, study_id: str, table: str) -> tuple[str, str]:
    """(filename, TSV text) for one of the study's tables."""
    if table not in DOWNLOAD_TABLES:
        raise PangenomeNotFound(f"No table '{table}'")
    data = _load(study_id)
    study = data["study"]
    names = _gene_name_map(db, study["organism_abbrev"])
    out = io.StringIO()
    writer = csv.writer(out, delimiter="\t", lineterminator="\n")
    out.write(f"# {study['title']}\n# Source: {study['citation']} (PMID {study.get('pubmed')}); "
              f"{study.get('license') or ''}\n")

    if table == "isolates":
        cols = ["name", "origin", "short_read_accession", "long_read_accession", "assembly_level", "platform",
                "contigs", "assembly_length", "n50"]
        writer.writerow(cols)
        for isolate in study.get("isolates", []):
            writer.writerow([isolate.get(c) if isolate.get(c) is not None else "" for c in cols])
    elif table == "stop_codons":
        writer.writerow(["gene", "cgd_gene", "gene_name", "chromosome", "position", "aa_position", "isolates",
                         "homozygous_isolates", "heterozygous_in", "homozygous_in"])
        for g in data["genes"]:
            ref = g.get("reference_gene") or ""
            for stop in g.get("stop_codons") or []:
                writer.writerow([g["id"], ref, names.get(ref.upper(), ""), stop.get("chromosome") or "",
                                 stop.get("position") or "", stop.get("aa_position") or "", stop.get("isolates") or 0,
                                 stop.get("homozygous_isolates") or 0, ",".join(stop.get("heterozygous", [])),
                                 ",".join(stop.get("homozygous", []))])
    else:
        cols = ["id", "category", "in_reference", "type", "presence", "reference_gene", "gene_name", "chromosome",
                "start", "end", "length_nt", "length_aa", "ortholog", "interpro", "note", "stop_codon_count"]
        writer.writerow(cols[:4] + [f"presence_of_{study['isolate_count']}"] + cols[5:])
        for g in data["genes"]:
            if table == "accessory" and g["category"] != "accessory":
                continue
            row = _gene(g, names, with_stops=False).model_dump()
            writer.writerow(["" if row.get(c) is None else row.get(c) for c in cols])
    return f"{study_id}_{table}.tsv", out.getvalue()
