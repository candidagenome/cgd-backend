"""
Expression Data Schemas.
"""
from __future__ import annotations

from typing import Optional, List
from pydantic import BaseModel, Field


class ExpressionCondition(BaseModel):
    """Expression data for a single condition."""
    condition_id: str = Field(description="Internal condition identifier")
    label: str = Field(description="Human-readable condition name")
    value: float = Field(description="Raw expression value (mean coverage)")
    fold_change: float = Field(description="Fold change vs control")
    bucket: str = Field(description="Category: control, basic_biology, kill_candida, stress")


class ExpressionStudy(BaseModel):
    """Expression data for a single study."""
    study_id: str = Field(description="Study identifier (e.g., Bruno_2010)")
    category: str = Field(description="Study category (e.g., Stress Response)")
    pmid: Optional[str] = Field(None, description="PubMed ID")
    control_id: str = Field(description="Control condition identifier")
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
