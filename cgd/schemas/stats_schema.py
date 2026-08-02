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


class RecentPhenotypeAnnotation(BaseModel):
    """One recently created feature-phenotype association."""

    annotation_no: int
    feature_name: str
    gene_name: Optional[str] = None
    observable: str
    qualifier: Optional[str] = None
    experiment_type: str
    date_created: str


class RecentActivityResponse(BaseModel):
    """Database records created during a shared recent-activity window."""

    days: int = Field(..., description="Lookback window in days")
    references: int = Field(0, description="References created during the window")
    phenotype_annotations: int = Field(
        0, description="Phenotype annotations created during the window"
    )
    ortholog_clusters: int = Field(0, description="Ortholog groups created during the window")
    recent_phenotype_annotations: list[RecentPhenotypeAnnotation] = Field(
        default_factory=list,
        description="Phenotype annotations created during the window, newest first",
    )
    success: bool = Field(True)
    error: Optional[str] = Field(None)


class OrganismCategoryCounts(BaseModel):
    """Per-organism category totals for the Explore page filter."""

    organism_abbrev: str
    organism_name: str
    genes: int = 0
    references: int = 0
    phenotypes: int = 0
    phenotype_annotations: int = 0
    go_annotations: int = 0
    ortholog_clusters: int = 0
    interactions: int = 0


class CountsByOrganismResponse(BaseModel):
    """Category totals broken down by reference strain."""

    by_organism: dict = Field(
        default_factory=dict,
        description="Map of organism_abbrev -> OrganismCategoryCounts",
    )
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
