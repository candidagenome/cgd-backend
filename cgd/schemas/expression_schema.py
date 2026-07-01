"""
Expression Data Schemas.
"""
from __future__ import annotations

from typing import Optional, List, Dict
from pydantic import BaseModel, Field


class ExpressionCondition(BaseModel):
    """Expression data for a single condition."""
    condition_id: str = Field(description="Internal condition identifier")
    label: str = Field(description="Human-readable condition name")
    value: float = Field(description="Raw expression value (mean coverage)")
    fold_change: float = Field(description="Fold change vs control")
    bucket: str = Field(description="Category: control, basic_biology, kill_candida, stress")
    group: Optional[str] = Field(None, description="Group name for grouped studies (e.g. strain); None if ungrouped")
    control_label: Optional[str] = Field(None, description="Label of the control this condition is compared against (per-group); None if ungrouped")


class ExpressionStudy(BaseModel):
    """Expression data for a single study."""
    study_id: str = Field(description="Study identifier (e.g., Bruno_2010)")
    category: str = Field(description="Study category (e.g., Stress Response)")
    pmid: Optional[str] = Field(None, description="PubMed ID")
    control_id: str = Field(description="Control condition identifier")
    control_label: str = Field(description="Human-readable control description (e.g., 'Planktonic')")
    control_value: float = Field(description="Control expression value")
    conditions: List[ExpressionCondition] = Field(
        default_factory=list,
        description="Expression data for each condition"
    )


class GeneExpressionResponse(BaseModel):
    """Response for gene expression data."""
    success: bool = Field(description="Whether the request succeeded")
    error: Optional[str] = Field(None, description="Error message if failed")

    # Gene info
    gene_name: Optional[str] = Field(None, description="Gene name")
    feature_name: Optional[str] = Field(None, description="Systematic feature name")
    description: Optional[str] = Field(None, description="Gene description")
    chromosome: Optional[str] = Field(None, description="Chromosome")
    start: Optional[int] = Field(None, description="Start coordinate")
    end: Optional[int] = Field(None, description="End coordinate")

    # Expression data by study
    studies: List[ExpressionStudy] = Field(
        default_factory=list,
        description="Expression data organized by study"
    )

    # Summary statistics
    total_conditions: int = Field(0, description="Total number of conditions analyzed")
    max_upregulation: Optional[float] = Field(None, description="Maximum fold change (upregulation)")
    max_downregulation: Optional[float] = Field(None, description="Minimum fold change (downregulation)")

    # Warnings
    warnings: List[str] = Field(default_factory=list, description="Any warnings")


class ExpressionConfigResponse(BaseModel):
    """Response with available expression datasets."""
    organisms: List[dict] = Field(description="Available organisms")
    studies: List[dict] = Field(description="Available studies with metadata")
    buckets: List[dict] = Field(description="Condition bucket categories")


class ExpressionDetailsForOrganism(BaseModel):
    """Expression data for a single organism (follows locus endpoint pattern)."""
    gene_name: Optional[str] = Field(None, description="Gene name")
    feature_name: Optional[str] = Field(None, description="Systematic feature name")
    description: Optional[str] = Field(None, description="Gene description")
    chromosome: Optional[str] = Field(None, description="Chromosome")
    start: Optional[int] = Field(None, description="Start coordinate")
    end: Optional[int] = Field(None, description="End coordinate")

    # Expression data by study
    studies: List[ExpressionStudy] = Field(
        default_factory=list,
        description="Expression data organized by study"
    )

    # Summary statistics
    total_conditions: int = Field(0, description="Total number of conditions analyzed")
    max_upregulation: Optional[float] = Field(None, description="Maximum fold change (upregulation)")
    max_downregulation: Optional[float] = Field(None, description="Minimum fold change (downregulation)")

    # Warnings
    warnings: List[str] = Field(default_factory=list, description="Any warnings")


class ExpressionDetailsResponse(BaseModel):
    """Response for expression_details endpoint (follows locus endpoint pattern)."""
    results: dict[str, ExpressionDetailsForOrganism] = Field(
        default_factory=dict,
        description="Expression data keyed by organism name"
    )


# ============================================================================
# Similar Expression Genes
# ============================================================================

class SimilarGene(BaseModel):
    """A gene with similar expression profile to the query gene."""
    gene_name: Optional[str] = Field(None, description="Standard gene name")
    feature_name: str = Field(description="Systematic name (e.g., orf19.1234)")
    description: Optional[str] = Field(None, description="Gene headline/description")
    correlation: float = Field(description="Similarity score (-1 to 1)")
    p_value: Optional[float] = Field(None, description="Statistical significance")
    shared_conditions: int = Field(description="Number of conditions used in comparison")


class SimilarGenesResponse(BaseModel):
    """Response for similar expression genes endpoint."""
    success: bool = Field(description="Whether the request succeeded")
    error: Optional[str] = Field(None, description="Error message if failed")

    # Query gene info
    query_gene: Optional[str] = Field(None, description="Query gene name")
    query_feature_name: Optional[str] = Field(None, description="Query systematic name")
    organism: Optional[str] = Field(None, description="Organism searched")
    organism_no: Optional[int] = Field(None, description="Organism number for downstream analysis")
    metric: Optional[str] = Field(None, description="Similarity metric used")

    # Results
    similar_genes: List[SimilarGene] = Field(
        default_factory=list,
        description="List of genes with similar expression profiles"
    )

    # Statistics
    total_genes_compared: int = Field(0, description="Total number of genes compared")
    conditions_used: int = Field(0, description="Number of conditions in expression profiles")
    computation_time_ms: float = Field(0.0, description="Time taken to compute results in ms")


# ============================================================================
# Batch Expression Data (for multi-gene heatmap)
# ============================================================================

class BatchGeneExpression(BaseModel):
    """Expression data for a single gene in batch response."""
    gene_name: str = Field(description="Gene name or feature name queried")
    data: Optional[ExpressionDetailsForOrganism] = Field(
        None, description="Expression data for this gene"
    )
    error: Optional[str] = Field(None, description="Error message if data unavailable")


class BatchExpressionRequest(BaseModel):
    """Request for batch expression data."""
    gene_names: List[str] = Field(
        description="List of gene names to fetch (max 50)",
        min_length=1,
        max_length=50
    )
    organism: str = Field(
        default="Candida albicans SC5314",
        description="Organism display name"
    )


class BatchExpressionResponse(BaseModel):
    """Response for batch expression data."""
    success: bool = Field(description="Whether the request succeeded")
    results: List[BatchGeneExpression] = Field(
        default_factory=list,
        description="Expression data for each gene"
    )
    genes_found: int = Field(0, description="Number of genes with data")
    genes_missing: int = Field(0, description="Number of genes without data")
    computation_time_ms: float = Field(0.0, description="Time taken in ms")


# ============================================================================
# Expression Matrix Download
# ============================================================================

class ExpressionMatrixRequest(BaseModel):
    """Request for expression matrix download."""
    gene_names: List[str] = Field(
        description="List of gene names to include in matrix",
        min_length=1,
        max_length=200
    )
    organism: str = Field(
        default="Candida albicans SC5314",
        description="Organism display name"
    )
    include_metadata: bool = Field(
        default=True,
        description="Include gene metadata columns (description, correlation)"
    )
    correlations: Optional[Dict[str, float]] = Field(
        default=None,
        description="Optional correlation values for each gene (from similar genes)"
    )
