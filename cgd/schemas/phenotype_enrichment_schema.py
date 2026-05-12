"""
Phenotype Enrichment schemas for request/response models.

Follows the same pattern as GO Term Finder for enrichment analysis
of phenotype observable terms.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class MultipleCorrectionMethod(str, Enum):
    """Multiple testing correction methods."""
    BONFERRONI = "bonferroni"
    BENJAMINI_HOCHBERG = "bh"
    NONE = "none"


class PhenotypeEnrichmentRequest(BaseModel):
    """Request model for phenotype enrichment analysis."""
    genes: list[str] = Field(..., min_length=1, description="List of gene names/IDs")
    organism_no: int = Field(..., description="Organism number")
    background_genes: Optional[list[str]] = Field(
        default=None,
        description="Custom background gene set (default: all genes with phenotype annotations)"
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
        description="Minimum number of query genes annotated to a phenotype"
    )


class GeneHit(BaseModel):
    """A gene annotated to an enriched phenotype."""
    feature_no: int
    systematic_name: str  # feature_name
    gene_name: Optional[str] = None


class EnrichedPhenotype(BaseModel):
    """An enriched phenotype from the analysis."""
    phenotype_no: int
    observable: str  # The phenotype observable term
    mutant_type: Optional[str] = None
    qualifier: Optional[str] = None

    # Counts
    query_count: int  # k - genes in query annotated to this phenotype
    query_total: int  # n - total genes in query with phenotype annotations
    background_count: int  # K - genes in background annotated to this phenotype
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


class PhenotypeEnrichmentResult(BaseModel):
    """Result of phenotype enrichment analysis."""
    # Query summary
    query_genes_submitted: int  # Total genes submitted
    query_genes_found: int  # Genes found in database
    query_genes_with_phenotype: int  # Genes with phenotype annotations
    query_genes_not_found: list[str] = []  # Genes not found

    # Background summary
    background_size: int
    background_type: str  # "default" or "custom"

    # Filters applied
    p_value_cutoff: float
    correction_method: str

    # Results
    enriched_phenotypes: list[EnrichedPhenotype] = []

    # Total enriched terms
    total_enriched_phenotypes: int = 0


class PhenotypeEnrichmentResponse(BaseModel):
    """Response for phenotype enrichment analysis."""
    success: bool
    result: Optional[PhenotypeEnrichmentResult] = None
    error: Optional[str] = None
    warnings: list[str] = []


class OrganismOption(BaseModel):
    """Organism option for selection."""
    organism_no: int
    organism_name: str
    display_name: str


class PhenotypeEnrichmentConfigResponse(BaseModel):
    """Configuration options for Phenotype Enrichment."""
    organisms: list[OrganismOption] = []
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
    has_phenotype_annotations: bool = False


class ValidateGenesResponse(BaseModel):
    """Response for gene validation."""
    found: list[ValidatedGene] = []
    not_found: list[str] = []
    total_submitted: int
    total_found: int
    total_with_phenotype: int
