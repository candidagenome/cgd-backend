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
