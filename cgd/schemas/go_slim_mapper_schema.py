"""
GO Slim Mapper schemas for request/response models.
"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class GoSlimTerm(BaseModel):
    """A GO term in a GO Slim set."""
    go_no: int
    goid: str  # GO:XXXXXXX format
    go_term: str
    go_aspect: str  # P, F, or C


class GoSlimSet(BaseModel):
    """A GO Slim set grouping."""
    go_set_name: str
    aspects: list[str] = []  # List of aspects available (P, F, C)


class GoSlimSetDetail(BaseModel):
    """Detailed GO Slim set with terms."""
    go_set_name: str
    go_aspect: str
    terms: list[GoSlimTerm] = []


class GoSlimMapperRequest(BaseModel):
    """Request model for GO Slim Mapper analysis."""
    genes: list[str] = Field(..., min_length=1, description="List of gene names/IDs")
    organism_no: int = Field(..., description="Organism number")
    go_set_name: str = Field(..., description="Name of the GO Slim set to use")
    go_aspect: str = Field(..., description="GO aspect (P, F, or C)")
    selected_terms: Optional[list[str]] = Field(
        default=None,
        description="Specific GO term IDs to include (default: all terms in set)"
    )
    annotation_types: Optional[list[str]] = Field(
        default=None,
        description="Annotation types to include: manually_curated, high_throughput, computational"
    )


class MappedGene(BaseModel):
    """A gene mapped to a GO Slim term."""
    feature_no: int
    systematic_name: str  # feature_name
    gene_name: Optional[str] = None


class MappedSlimTerm(BaseModel):
    """A GO Slim term with mapped genes."""
    go_no: int
    goid: str  # GO:XXXXXXX format
    go_term: str
    go_aspect: str  # P, F, or C

    # Frequency
    gene_count: int  # Number of genes mapped to this term
    total_genes: int  # Total genes with GO annotations
    frequency_percent: float  # (gene_count / total_genes) * 100

    # Genes
    genes: list[MappedGene] = []


class GoSlimMapperResult(BaseModel):
    """Result of GO Slim Mapper analysis."""
    # Query summary
    query_genes_submitted: int  # Total genes submitted
    query_genes_found: int  # Genes found in database
    query_genes_with_go: int  # Genes with GO annotations
    query_genes_not_found: list[str] = []  # Genes not found

    # Configuration used
    organism_no: int
    organism_name: str
    go_set_name: str
    go_aspect: str
    annotation_types_used: list[str] = []

    # Results
    mapped_terms: list[MappedSlimTerm] = []  # Terms with at least one gene

    # Special categories
    other_genes: list[MappedGene] = []  # Genes with GO but not mapped to any slim term
    not_annotated_genes: list[MappedGene] = []  # Genes without GO annotations


class GoSlimMapperResponse(BaseModel):
    """Response for GO Slim Mapper analysis."""
    success: bool
    result: Optional[GoSlimMapperResult] = None
    error: Optional[str] = None
    warnings: list[str] = []


class OrganismOption(BaseModel):
    """Organism option for selection."""
    organism_no: int
    organism_name: str
    display_name: str


class AnnotationTypeOption(BaseModel):
    """Annotation type option."""
    value: str
    label: str


class GoSlimMapperConfigResponse(BaseModel):
    """Configuration options for GO Slim Mapper."""
    organisms: list[OrganismOption] = []
    go_slim_sets: list[GoSlimSet] = []
    annotation_types: list[AnnotationTypeOption] = []
