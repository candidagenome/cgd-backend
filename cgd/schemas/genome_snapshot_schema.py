"""
Genome Snapshot Schemas.
"""
from typing import Optional, List, Dict
from pydantic import BaseModel, Field


class GoAnnotationCounts(BaseModel):
    """Counts of distinct gene products with GO annotations."""
    molecular_function: int = Field(0, description="Gene products annotated to Molecular Function")
    cellular_component: int = Field(0, description="Gene products annotated to Cellular Component")
    biological_process: int = Field(0, description="Gene products annotated to Biological Process")
    total: int = Field(0, description="Sum of the three aspect coverage counts")
    unique_gene_products: int = Field(0, description="Distinct gene products annotated in any GO aspect")


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
    ltr_count: int = Field(0, description="Long terminal repeat count")
    snorna_count: int = Field(0, description="snoRNA count")
    repeat_region_count: int = Field(0, description="Repeat region count")
    retrotransposon_count: int = Field(0, description="Retrotransposon count")
    centromere_count: int = Field(0, description="Centromere count")
    pseudogene_count: int = Field(0, description="Pseudogene count")
    blocked_reading_frame_count: int = Field(0, description="Blocked reading frame count")
    snrna_count: int = Field(0, description="snRNA count")
    rrna_count: int = Field(0, description="rRNA count")
    ncrna_count: int = Field(0, description="ncRNA count")
    total_features: int = Field(0, description="Total feature count")

    # Genome info
    chromosome_length: int = Field(0, description="Total chromosome length in bp")
    haploid_chromosome_length: int = Field(0, description="Haploid chromosome length in bp")
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


class ChromosomeFeatureCounts(BaseModel):
    """Feature counts for a single chromosome."""
    chromosome: str = Field(..., description="Chromosome name")
    chromosome_display: str = Field(..., description="Display name for chromosome")
    length_bp: int = Field(0, description="Chromosome length in base pairs")
    total_orfs: int = Field(0, description="Total ORFs on this chromosome")
    verified_orfs: int = Field(0, description="Verified ORFs")
    uncharacterized_orfs: int = Field(0, description="Uncharacterized ORFs")
    dubious_orfs: int = Field(0, description="Dubious ORFs")
    trna: int = Field(0, description="tRNA count")
    snorna: int = Field(0, description="snoRNA count")
    rrna: int = Field(0, description="rRNA count")
    ncrna: int = Field(0, description="ncRNA count")
    pseudogene: int = Field(0, description="Pseudogene count")
    snrna: int = Field(0, description="snRNA count")
    ltr: int = Field(0, description="Long terminal repeat count")
    retrotransposon: int = Field(0, description="Retrotransposon count")
    centromere: int = Field(0, description="Centromere count")
    repeat_region: int = Field(0, description="Repeat region count")
    blocked_reading_frame: int = Field(0, description="Blocked reading frame count")
    total_features: int = Field(0, description="Total current feature placements on this chromosome")


class ChromosomeInventoryResponse(BaseModel):
    """Response for chromosome feature inventory."""
    success: bool = Field(..., description="Request success")
    organism_abbrev: str = Field(..., description="Organism abbreviation")
    organism_name: str = Field(..., description="Full organism name")
    chromosomes: List[ChromosomeFeatureCounts] = Field(
        default_factory=list,
        description="Feature counts per chromosome"
    )
    nuclear_totals: Optional[ChromosomeFeatureCounts] = Field(
        None, description="Totals for nuclear genome"
    )
    mitochondrial: Optional[ChromosomeFeatureCounts] = Field(
        None, description="Mitochondrial genome counts"
    )
    grand_totals: Optional[ChromosomeFeatureCounts] = Field(
        None, description="Grand totals across all chromosomes"
    )
    feature_types: List[str] = Field(
        default_factory=list,
        description="List of feature types present in this organism"
    )
    error: Optional[str] = Field(None, description="Error message if failed")
