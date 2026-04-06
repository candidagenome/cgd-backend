"""
CRISPR Guide RNA Designer Schemas.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field


class PAMType(str, Enum):
    """Supported PAM sequences for CRISPR systems."""
    NGG = "NGG"          # SpCas9 (most common)
    NAG = "NAG"          # SpCas9 (lower efficiency)
    NNGRRT = "NNGRRT"    # SaCas9
    NNNNGATT = "NNNNGATT"  # NmeCas9
    TTTV = "TTTV"        # Cas12a/Cpf1


class TargetRegion(str, Enum):
    """Target region preferences for guide design."""
    FIVE_PRIME = "5_prime"      # First 20% of CDS (for knockout)
    THREE_PRIME = "3_prime"     # Last 20% of CDS
    FULL_CDS = "full_cds"       # Entire coding sequence
    CUSTOM = "custom"           # User-provided sequence


class CrisprDesignRequest(BaseModel):
    """Request for CRISPR guide RNA design."""
    # Input - one of these required
    gene_name: Optional[str] = Field(
        None,
        description="Gene name to design guides for (e.g., HOG1, EFG1)"
    )
    sequence: Optional[str] = Field(
        None,
        description="Raw DNA sequence to design guides for (if no gene name provided)"
    )

    # Organism selection
    organism: str = Field(
        "C_albicans_SC5314_A22",
        description="Target organism/genome assembly"
    )

    # PAM and guide settings
    pam: PAMType = Field(
        PAMType.NGG,
        description="PAM sequence for the CRISPR system"
    )
    guide_length: int = Field(
        20,
        ge=17,
        le=25,
        description="Length of the guide RNA (typically 20 for SpCas9)"
    )

    # Target region (for gene input)
    target_region: TargetRegion = Field(
        TargetRegion.FIVE_PRIME,
        description="Preferred target region within the gene"
    )

    # Off-target settings
    check_offtargets: bool = Field(
        True,
        description="Whether to perform off-target analysis"
    )
    offtarget_genomes: List[str] = Field(
        default_factory=list,
        description="Additional genomes to check for off-targets (empty = same as organism)"
    )
    max_offtarget_mismatches: int = Field(
        3,
        ge=0,
        le=5,
        description="Maximum mismatches to consider as potential off-target"
    )

    # Homology arm settings
    include_homology_arms: bool = Field(
        False,
        description="Whether to design homology arms for HDR"
    )
    homology_arm_length: int = Field(
        50,
        ge=30,
        le=1000,
        description="Length of homology arms in bp"
    )

    # Output limits
    max_guides: int = Field(
        50,
        ge=1,
        le=100,
        description="Maximum number of guides to return"
    )


class RestrictionSite(BaseModel):
    """Restriction enzyme site found in a guide."""
    enzyme: str = Field(description="Enzyme name (e.g., BbsI, BsaI)")
    position: int = Field(description="Position within guide sequence")
    sequence: str = Field(description="Recognition sequence")


class OffTargetHit(BaseModel):
    """A potential off-target site."""
    chromosome: str = Field(description="Chromosome/contig name")
    position: int = Field(description="Position on chromosome")
    strand: str = Field(description="+ or -")
    sequence: str = Field(description="Off-target sequence")
    mismatches: int = Field(description="Number of mismatches")
    mismatch_positions: List[int] = Field(description="Positions of mismatches (0-indexed)")
    gene_name: Optional[str] = Field(None, description="Gene name if in a gene")
    gene_region: Optional[str] = Field(None, description="exon, intron, intergenic, promoter")
    cfd_score: float = Field(description="CFD off-target score (0-1, lower = worse off-target)")


class HomologyArms(BaseModel):
    """Homology arms for HDR."""
    upstream: str = Field(description="5' homology arm sequence")
    downstream: str = Field(description="3' homology arm sequence")
    upstream_start: int = Field(description="Genomic start of upstream arm")
    downstream_end: int = Field(description="Genomic end of downstream arm")


class CloningPrimers(BaseModel):
    """Primers for cloning guide into expression vector."""
    forward: str = Field(description="Forward oligo with overhangs")
    reverse: str = Field(description="Reverse oligo with overhangs")
    vector_system: str = Field(description="Compatible vector system (e.g., pX330, pV1093)")


class GuideResult(BaseModel):
    """A single guide RNA result."""
    rank: int = Field(description="Ranking (1 = best)")
    sequence: str = Field(description="Guide RNA sequence (20bp typically)")
    pam: str = Field(description="PAM sequence")
    full_target: str = Field(description="Full target site (guide + PAM)")

    # Position information
    position: int = Field(description="Position in target sequence (1-based)")
    strand: str = Field(description="+ (sense) or - (antisense)")
    genomic_start: Optional[int] = Field(None, description="Genomic coordinate start")
    genomic_end: Optional[int] = Field(None, description="Genomic coordinate end")
    chromosome: Optional[str] = Field(None, description="Chromosome name")

    # Scoring
    gc_content: float = Field(description="GC content (0-100%)")
    efficiency_score: float = Field(description="Predicted efficiency (0-100)")
    specificity_score: float = Field(description="Off-target specificity (0-100, higher = better)")
    combined_score: float = Field(description="Combined ranking score (0-100)")

    # Off-target summary
    offtarget_count: int = Field(0, description="Number of potential off-targets")
    offtarget_0mm: int = Field(0, description="Off-targets with 0 mismatches (exact)")
    offtarget_1mm: int = Field(0, description="Off-targets with 1 mismatch")
    offtarget_2mm: int = Field(0, description="Off-targets with 2 mismatches")
    offtarget_3mm: int = Field(0, description="Off-targets with 3 mismatches")
    offtargets: List[OffTargetHit] = Field(
        default_factory=list,
        description="Top off-target hits (limited for performance)"
    )

    # Sequence features
    has_poly_t: bool = Field(False, description="Contains TTTT (Pol III terminator)")
    restriction_sites: List[RestrictionSite] = Field(
        default_factory=list,
        description="Restriction sites within guide"
    )

    # Cloning primers
    primers: Optional[CloningPrimers] = Field(None, description="Cloning primers")

    # Homology arms (if requested)
    homology_arms: Optional[HomologyArms] = Field(None, description="HDR homology arms")


class GeneInfo(BaseModel):
    """Information about the target gene."""
    gene_name: Optional[str] = Field(None, description="Standard gene name")
    feature_name: str = Field(description="Systematic feature name")
    dbxref_id: str = Field(description="Database cross-reference ID")
    organism: str = Field(description="Organism name")
    description: Optional[str] = Field(None, description="Gene description/headline")
    chromosome: Optional[str] = Field(None, description="Chromosome location")
    start: Optional[int] = Field(None, description="Start coordinate")
    end: Optional[int] = Field(None, description="End coordinate")
    strand: Optional[str] = Field(None, description="Strand (W/C or +/-)")
    sequence_length: int = Field(description="Length of coding sequence")
    cgd_url: Optional[str] = Field(None, description="Link to CGD locus page")
    jbrowse_url: Optional[str] = Field(None, description="Link to JBrowse view")


class CrisprDesignResponse(BaseModel):
    """Response for CRISPR guide design."""
    success: bool = Field(description="Whether the request succeeded")
    error: Optional[str] = Field(None, description="Error message if failed")

    # Gene information (if gene name was provided)
    gene_info: Optional[GeneInfo] = Field(None, description="Target gene information")

    # Target sequence info
    target_sequence: Optional[str] = Field(None, description="Target sequence used for design")
    target_length: int = Field(0, description="Length of target sequence")

    # Design parameters used
    organism: str = Field(description="Organism used")
    pam: str = Field(description="PAM sequence used")
    guide_length: int = Field(description="Guide length used")

    # Results
    total_guides_found: int = Field(0, description="Total number of potential guides found")
    guides: List[GuideResult] = Field(default_factory=list, description="Ranked guide results")

    # Warnings
    warnings: List[str] = Field(default_factory=list, description="Any warnings or notes")


class CrisprConfigResponse(BaseModel):
    """Response with CRISPR tool configuration."""
    pam_options: List[dict] = Field(description="Available PAM sequences")
    organisms: List[dict] = Field(description="Available organisms")
    default_guide_length: int = Field(20, description="Default guide length")
    max_guides: int = Field(50, description="Maximum guides per request")
    cloning_systems: List[dict] = Field(description="Available cloning vector systems")


class CrisprDownloadRequest(BaseModel):
    """Request for downloading CRISPR results."""
    guides: List[GuideResult] = Field(description="Guide results to download")
    gene_info: Optional[GeneInfo] = Field(None, description="Gene information")
    format: str = Field("tsv", description="Download format (tsv, csv, fasta)")
    include_offtargets: bool = Field(False, description="Include off-target details")
    include_primers: bool = Field(True, description="Include cloning primers")
