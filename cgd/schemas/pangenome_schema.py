"""Schemas for the pangenome viewer (see cgd/api/services/pangenome_service.py)."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class PangenomeIsolate(BaseModel):
    name: str
    origin: Optional[str] = None
    short_read_source: Optional[str] = None
    short_read_accession: Optional[str] = None
    long_read_source: Optional[str] = None
    long_read_accession: Optional[str] = None
    assembly_level: Optional[str] = None
    platform: Optional[str] = None
    contigs: Optional[int] = None
    assembly_length: Optional[int] = None
    n50: Optional[int] = None


class PangenomeCounts(BaseModel):
    genes: int
    core: int
    accessory: int
    not_in_reference: int
    with_stop_codons: int
    isolates_listed: int


class PangenomeStudySummary(BaseModel):
    study_id: str
    organism_abbrev: str
    species: str
    reference_strain: str
    title: str
    citation: str
    pubmed: Optional[int] = None
    doi: Optional[str] = None
    reference_dbxref_id: Optional[str] = None  # CGD reference page for the paper
    isolate_count: int
    core_definition: Optional[str] = None
    license: Optional[str] = None
    data_source: Optional[str] = None
    loaded: Optional[str] = None
    counts: PangenomeCounts


class PangenomeStudy(PangenomeStudySummary):
    notes: list[str] = []
    divergent_strains: list[str] = []
    isolates: list[PangenomeIsolate] = []


class PangenomeStudyList(BaseModel):
    studies: list[PangenomeStudySummary]


class StopCodon(BaseModel):
    chromosome: Optional[str] = None
    position: Optional[int] = None
    aa_position: Optional[int] = None
    isolates: Optional[int] = None
    homozygous_isolates: Optional[int] = None
    heterozygous: list[str] = []
    homozygous: list[str] = []


class DivergentStrainHit(BaseModel):
    presence: Optional[str] = None
    coverage: Optional[float] = None
    hit: Optional[str] = None
    identity: Optional[float] = None
    hit_status: Optional[str] = None


class PangenomeGene(BaseModel):
    id: str  # gene ID as given in the study
    category: str  # "core" or "accessory"
    in_reference: bool
    type: Optional[str] = None
    presence: Optional[int] = None  # isolates carrying an accessory gene
    reference_gene: Optional[str] = None  # CGD systematic name the study gene maps to
    gene_name: Optional[str] = None  # current CGD gene name of reference_gene
    length_nt: Optional[int] = None
    length_aa: Optional[int] = None
    chromosome: Optional[str] = None
    start: Optional[int] = None
    end: Optional[int] = None
    ortholog: Optional[str] = None
    interpro: Optional[str] = None
    note: Optional[str] = None
    blomberg_k: Optional[float] = None
    phylo_p_value: Optional[float] = None
    stop_codon_count: int = 0
    stop_codon_isolates: int = 0  # isolates with a premature stop at any site
    stop_codons: list[StopCodon] = []
    divergent_strains: dict[str, DivergentStrainHit] = {}


class PangenomeGenePage(BaseModel):
    study_id: str
    total: int
    page: int
    page_size: int
    genes: list[PangenomeGene]


class PangenomeLocusStudy(BaseModel):
    """One study's records for a locus: its own gene plus accessory paralogs / fusions derived from it."""
    study: PangenomeStudySummary
    isolate_count: int
    genes: list[PangenomeGene]


class PangenomeLocusResponse(BaseModel):
    feature_name: Optional[str] = None
    gene_name: Optional[str] = None
    organism: Optional[str] = None
    studies: list[PangenomeLocusStudy] = []


class MapStop(BaseModel):
    position: int
    isolates: int
    homozygous: int = 0


class MapGene(BaseModel):
    """A gene drawn on the graphical browser."""
    id: str
    reference_gene: Optional[str] = None
    gene_name: Optional[str] = None
    category: str
    in_reference: bool
    presence: Optional[int] = None
    type: Optional[str] = None
    ortholog: Optional[str] = None
    chromosome: Optional[str] = None
    start: Optional[int] = None
    end: Optional[int] = None
    placed_by: Optional[str] = None  # "own" coordinates, or those of its "reference_gene"
    stops: list[MapStop] = []


class MapChromosome(BaseModel):
    name: str
    length: int


class PangenomeMap(BaseModel):
    study_id: str
    isolate_count: int
    chromosomes: list[MapChromosome]
    genes: list[MapGene]
    unplaced: list[MapGene] = []
