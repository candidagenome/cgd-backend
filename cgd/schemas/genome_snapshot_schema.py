"""
Genome Snapshot Schemas.
"""
from typing import Optional, List, Dict
from pydantic import BaseModel, Field


class GoAnnotationCounts(BaseModel):
    """GO annotation counts by aspect."""
    molecular_function: int = Field(0, description="Molecular Function annotation count")
    cellular_component: int = Field(0, description="Cellular Component annotation count")
    biological_process: int = Field(0, description="Biological Process annotation count")
    total: int = Field(0, description="Total GO annotations")


class GoSlimCategory(BaseModel):
    """A single GO Slim category with gene count and percentage."""
    go_term: str = Field(..., description="GO Slim term name")
    goid: str = Field(..., description="GO ID (e.g., GO:0008150)")
    count: int = Field(0, description="Number of genes annotated to this term")
    percentage: float = Field(0.0, description="Percentage of total genes annotated")


class GoSlimDistribution(BaseModel):
    """GO Slim distribution for a single aspect."""
    aspect: str = Field(..., description="GO aspect (F, C, or P)")
    aspect_name: str = Field(..., description="Full aspect name")
    categories: List[GoSlimCategory] = Field(
        default_factory=list,
        description="List of GO Slim categories with counts"
    )
    total_genes: int = Field(0, description="Total genes with GO annotations in this aspect")


class GoSlimDistributionResponse(BaseModel):
    """Response for GO Slim distribution data."""
    success: bool = Field(..., description="Request success")
    organism_abbrev: str = Field(..., description="Organism abbreviation")
    organism_name: str = Field(..., description="Full organism name")
    molecular_function: Optional[GoSlimDistribution] = Field(
        None, description="Molecular Function distribution"
    )
    cellular_component: Optional[GoSlimDistribution] = Field(
        None, description="Cellular Component distribution"
    )
    biological_process: Optional[GoSlimDistribution] = Field(
        None, description="Biological Process distribution"
    )
    error: Optional[str] = Field(None, description="Error message if failed")


class GenomeSnapshotResponse(BaseModel):
    """Response for genome snapshot statistics."""
    success: bool = Field(..., description="Request success")
    organism_abbrev: str = Field(..., description="Organism abbreviation")
    organism_name: str = Field(..., description="Full organism name")
    strain: str = Field(..., description="Strain name")
    last_updated: Optional[str] = Field(None, description="Last update date")

    # ORF counts
    total_orfs: int = Field(0, description="Total ORF count")
    haploid_orfs: int = Field(0, description="Haploid ORF count (for diploids)")
    verified_orfs: int = Field(0, description="Verified ORF count")
    uncharacterized_orfs: int = Field(0, description="Uncharacterized ORF count")
    dubious_orfs: int = Field(0, description="Dubious ORF count")

    # Other feature counts
    trna_count: int = Field(0, description="tRNA gene count")

    # Genome info
    chromosomes: List[str] = Field(default_factory=list, description="List of chromosome names")
    genome_length: str = Field("", description="Total genome length formatted")
    genome_length_bp: int = Field(0, description="Total genome length in base pairs")

    # GO annotations
    go_annotations: GoAnnotationCounts = Field(
        default_factory=GoAnnotationCounts,
        description="GO annotation counts by aspect"
    )

    error: Optional[str] = Field(None, description="Error message if failed")


class GenomeSnapshotListResponse(BaseModel):
    """Response listing available organisms for genome snapshot."""
    success: bool = Field(..., description="Request success")
    organisms: List[Dict[str, str]] = Field(
        default_factory=list,
        description="List of available organisms with abbrev and name"
    )
    error: Optional[str] = Field(None, description="Error message if failed")
