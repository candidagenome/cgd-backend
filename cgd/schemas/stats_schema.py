"""
Site statistics schemas.
"""
from typing import Optional
from pydantic import BaseModel, Field


class StatsSummaryResponse(BaseModel):
    """Database-wide totals used by the Explore/landing page."""

    genes: int = Field(0, description="Total haploid protein-coding ORFs across all strains")
    references: int = Field(0, description="Total references (literature and curated sources)")
    phenotypes: int = Field(0, description="Distinct phenotype terms")
    phenotype_annotations: int = Field(0, description="Feature-phenotype annotations")
    go_annotations: int = Field(0, description="GO annotations")
    ortholog_clusters: int = Field(0, description="Homology (ortholog) groups")
    interactions: int = Field(0, description="Physical and genetic interactions")
    colleagues: int = Field(0, description="Registered colleagues")
    organisms: int = Field(0, description="Reference strains")
    success: bool = Field(True)
    error: Optional[str] = Field(None)


class GeneOfTheDayResponse(BaseModel):
    """A deterministic per-day featured gene for the Explore/landing page."""

    display_name: Optional[str] = Field(None, description="Gene name, e.g. ACT1")
    systematic_name: Optional[str] = Field(None, description="Feature/systematic name")
    headline: Optional[str] = Field(None, description="Short description of the gene")
    organism: Optional[str] = Field(None, description="Organism the gene belongs to")
    link: Optional[str] = Field(None, description="Relative locus-page URL")
    dbxref_id: Optional[str] = Field(None, description="Primary CGDID for the feature")
    success: bool = Field(True)
    error: Optional[str] = Field(None)
