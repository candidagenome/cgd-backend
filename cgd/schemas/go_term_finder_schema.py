"""
GO Term Finder schemas for request/response models.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class GoOntology(str, Enum):
    """GO ontology aspect filter."""
    PROCESS = "P"
    FUNCTION = "F"
    COMPONENT = "C"
    ALL = "all"


class MultipleCorrectionMethod(str, Enum):
    """Multiple testing correction methods."""
    BONFERRONI = "bonferroni"
    BENJAMINI_HOCHBERG = "bh"
    NONE = "none"


class GoTermFinderRequest(BaseModel):
    """Request model for GO Term Finder analysis."""
    genes: list[str] = Field(..., min_length=1, description="List of gene names/IDs")
    organism_no: int = Field(..., description="Organism number")
    ontology: GoOntology = Field(default=GoOntology.ALL, description="GO ontology aspect filter")
    background_genes: Optional[list[str]] = Field(
        default=None,
        description="Custom background gene set (default: all genes with GO annotations)"
    )
    evidence_codes: Optional[list[str]] = Field(
        default=None,
        description="Evidence codes to include (default: all)"
    )
    annotation_types: Optional[list[str]] = Field(
        default=None,
        description="Annotation types to include: manually_curated, high_throughput, computational"
    )
    p_value_cutoff: float = Field(
        default=0.01,
        ge=0.0,
        le=1.0,
        description="P-value cutoff for significance"
    )
    correction_method: MultipleCorrectionMethod = Field(
        default=MultipleCorrectionMethod.BENJAMINI_HOCHBERG,
        description="Multiple testing correction method"
    )
    min_genes_in_term: int = Field(
        default=1,
        ge=1,
        description="Minimum number of query genes annotated to a term"
    )


class GeneHit(BaseModel):
    """A gene annotated to an enriched GO term."""
    feature_no: int
    systematic_name: str  # feature_name
    gene_name: Optional[str] = None
    evidence_codes: list[str] = []


class EnrichedGoTerm(BaseModel):
    """An enriched GO term from the analysis."""
    go_no: int
    goid: str  # GO:XXXXXXX format
    go_term: str
    go_aspect: str  # P, F, or C
    aspect_name: str  # Biological Process, Molecular Function, Cellular Component

    # Counts
    query_count: int  # k - genes in query annotated to this term
    query_total: int  # n - total genes in query with GO annotations
    background_count: int  # K - genes in background annotated to this term
    background_total: int  # N - total genes in background

    # Frequencies (as percentages)
    query_frequency: float  # (k/n) * 100
    background_frequency: float  # (K/N) * 100
    fold_enrichment: float  # (k/n) / (K/N)

    # Statistics
    p_value: float
    fdr: Optional[float] = None  # FDR-corrected p-value (if correction applied)

    # Genes
    genes: list[GeneHit] = []


class GoTermFinderResult(BaseModel):
    """Result of GO Term Finder analysis."""
    # Query summary
    query_genes_submitted: int  # Total genes submitted
    query_genes_found: int  # Genes found in database
    query_genes_with_go: int  # Genes with GO annotations
    query_genes_not_found: list[str] = []  # Genes not found

    # Background summary
    background_size: int
    background_type: str  # "default" or "custom"

    # Filters applied
    ontology_filter: str
    evidence_codes_used: list[str] = []
    annotation_types_used: list[str] = []
    p_value_cutoff: float
    correction_method: str

    # Results by aspect
    process_terms: list[EnrichedGoTerm] = []
    function_terms: list[EnrichedGoTerm] = []
    component_terms: list[EnrichedGoTerm] = []

    # Total enriched terms
    total_enriched_terms: int = 0


class GoTermFinderResponse(BaseModel):
    """Response for GO Term Finder analysis."""
    success: bool
    result: Optional[GoTermFinderResult] = None
    error: Optional[str] = None
    warnings: list[str] = []


class OrganismOption(BaseModel):
    """Organism option for selection."""
    organism_no: int
    organism_name: str
    display_name: str


class EvidenceCodeOption(BaseModel):
    """Evidence code option."""
    code: str
    description: str


class AnnotationTypeOption(BaseModel):
    """Annotation type option."""
    value: str
    label: str


class GoTermFinderConfigResponse(BaseModel):
    """Configuration options for GO Term Finder."""
    organisms: list[OrganismOption] = []
    evidence_codes: list[EvidenceCodeOption] = []
    annotation_types: list[AnnotationTypeOption] = []
    default_p_value_cutoff: float = 0.01
    correction_methods: list[dict] = [
        {"value": "bh", "label": "Benjamini-Hochberg (FDR)"},
        {"value": "bonferroni", "label": "Bonferroni"},
        {"value": "none", "label": "None"},
    ]


class ValidateGenesRequest(BaseModel):
    """Request for gene validation."""
    genes: list[str] = Field(..., min_length=1)
    organism_no: int


class ValidatedGene(BaseModel):
    """A validated gene."""
    input_name: str  # Name as submitted
    feature_no: int
    systematic_name: str
    gene_name: Optional[str] = None
    has_go_annotations: bool = False


class ValidateGenesResponse(BaseModel):
    """Response for gene validation."""
    found: list[ValidatedGene] = []
    not_found: list[str] = []
    total_submitted: int
    total_found: int
    total_with_go: int


class GoEnrichmentGraphNode(BaseModel):
    """Node in the GO enrichment graph."""
    goid: str
    go_term: str
    go_aspect: str
    p_value: float
    fdr: Optional[float] = None
    query_count: int
    is_enriched: bool  # Whether this term is in the enriched results


class GoEnrichmentGraphEdge(BaseModel):
    """Edge in the GO enrichment graph."""
    source: str  # parent GOID
    target: str  # child GOID
    relationship_type: str  # is_a or part_of


class GoEnrichmentGraphResponse(BaseModel):
    """GO enrichment hierarchy graph for visualization."""
    nodes: list[GoEnrichmentGraphNode] = []
    edges: list[GoEnrichmentGraphEdge] = []
