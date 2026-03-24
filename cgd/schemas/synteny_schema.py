"""Pydantic schemas for synteny viewer endpoint."""
from __future__ import annotations

import typing
from pydantic import BaseModel


class SyntenyGene(BaseModel):
    """A gene in a synteny region."""
    feature_name: str
    gene_name: typing.Optional[str] = None
    start: int
    stop: int
    strand: str  # 'W' or 'C'
    is_query: bool = False
    ortholog_id: typing.Optional[str] = None  # CGOB cluster ID if part of an ortholog group


class SyntenyRegion(BaseModel):
    """Genes in a syntenic region for one species."""
    organism_name: str
    chromosome: str
    genes: list[SyntenyGene] = []


class OrthologConnection(BaseModel):
    """Links genes across species that belong to the same ortholog group."""
    ortholog_id: str  # e.g., "CGOB_123"
    genes: list[str] = []  # List of feature_names in this ortholog group


class QueryGene(BaseModel):
    """Information about the query gene."""
    feature_name: str
    gene_name: typing.Optional[str] = None
    organism: str
    chromosome: str
    start: int
    stop: int
    strand: str


class SyntenyResponse(BaseModel):
    """
    Response for /api/locus/{name}/synteny endpoint.

    Contains the query gene info, synteny regions for all species,
    and ortholog connections between genes.
    """
    query_gene: QueryGene
    synteny_regions: dict[str, SyntenyRegion]  # organism_name -> SyntenyRegion
    ortholog_connections: list[OrthologConnection] = []


# ============================================================================
# Genome Synteny Browser Schemas
# ============================================================================

class ChromosomeInfo(BaseModel):
    """Information about a chromosome for the genome synteny browser."""
    organism_name: str
    chromosome: str  # feature_name of the chromosome
    length: int  # sequence length in bp
    gene_count: int  # number of ORFs on this chromosome


class ChromosomeListResponse(BaseModel):
    """
    Response for /api/synteny/chromosomes endpoint.

    Lists all chromosomes for each CGD species.
    """
    chromosomes: dict[str, list[ChromosomeInfo]]  # organism_name -> list of chromosomes


class GenomeGene(BaseModel):
    """A gene in a chromosome region for the genome synteny browser."""
    feature_name: str
    gene_name: typing.Optional[str] = None
    start: int
    stop: int
    strand: str  # 'W' or 'C'
    ortholog_id: typing.Optional[str] = None  # CGOB cluster ID
    headline: typing.Optional[str] = None  # gene description


class ChromosomeGenesResponse(BaseModel):
    """
    Response for /api/synteny/chromosome/{name} endpoint.

    Contains genes in a chromosome region with ortholog information.
    """
    organism_name: str
    chromosome: str
    chromosome_length: int
    genes: list[GenomeGene] = []
    window_start: typing.Optional[int] = None
    window_end: typing.Optional[int] = None
