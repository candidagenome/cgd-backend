"""
Pattern Match Search Schemas.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field


class PatternType(str, Enum):
    """Pattern type (DNA or protein)."""
    DNA = "dna"
    PROTEIN = "protein"


class StrandOption(str, Enum):
    """Strand search options."""
    BOTH = "both"
    WATSON = "watson"  # + strand
    CRICK = "crick"    # - strand


class SequenceDataset(str, Enum):
    """Available sequence datasets to search."""
    # DNA datasets - C. albicans Assembly 22
    CA22_CHROMOSOMES = "ca22_chromosomes"
    CA22_ORF_GENOMIC = "ca22_orf_genomic"
    CA22_ORF_CODING = "ca22_orf_coding"
    CA22_ORF_GENOMIC_1KB = "ca22_orf_genomic_1kb"
    CA22_INTERGENIC = "ca22_intergenic"
    CA22_NONCODING = "ca22_noncoding"
    CA22_ORF_PROTEIN = "ca22_orf_protein"
    # DNA datasets - C. albicans Assembly 21
    CA21_CHROMOSOMES = "ca21_chromosomes"
    CA21_ORF_GENOMIC = "ca21_orf_genomic"
    CA21_ORF_CODING = "ca21_orf_coding"
    CA21_ORF_GENOMIC_1KB = "ca21_orf_genomic_1kb"
    CA21_INTERGENIC = "ca21_intergenic"
    CA21_NONCODING = "ca21_noncoding"
    CA21_ORF_PROTEIN = "ca21_orf_protein"
    # C. glabrata
    CG_CHROMOSOMES = "cg_chromosomes"
    CG_ORF_GENOMIC = "cg_orf_genomic"
    CG_ORF_CODING = "cg_orf_coding"
    CG_ORF_PROTEIN = "cg_orf_protein"
    # All organisms combined
    ALL_CHROMOSOMES = "all_chromosomes"
    ALL_ORF_GENOMIC = "all_orf_genomic"
    ALL_ORF_CODING = "all_orf_coding"
    ALL_ORF_PROTEIN = "all_orf_protein"


class PatmatchSearchRequest(BaseModel):
    """Request for pattern match search."""
    pattern: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Pattern to search for (DNA or protein sequence, supports IUPAC codes)"
    )
    pattern_type: PatternType = Field(
        PatternType.DNA,
        description="Type of pattern (DNA or protein)"
    )
    dataset: SequenceDataset = Field(
        SequenceDataset.CA22_CHROMOSOMES,
        description="Sequence dataset to search"
    )
    strand: StrandOption = Field(
        StrandOption.BOTH,
        description="Which strand(s) to search (DNA only)"
    )
    max_mismatches: int = Field(
        0,
        ge=0,
        le=3,
        description="Maximum number of mismatches allowed"
    )
    max_insertions: int = Field(
        0,
        ge=0,
        le=3,
        description="Maximum number of insertions allowed"
    )
    max_deletions: int = Field(
        0,
        ge=0,
        le=3,
        description="Maximum number of deletions allowed"
    )
    max_results: int = Field(
        100,
        ge=1,
        le=1000,
        description="Maximum number of results to return"
    )


class PatmatchHit(BaseModel):
    """Single pattern match hit."""
    sequence_name: str
    sequence_description: Optional[str] = None
    match_start: int
    match_end: int
    strand: str
    matched_sequence: str
    context_before: str = ""
    context_after: str = ""
    mismatches: int = 0
    insertions: int = 0
    deletions: int = 0
    # Links
    locus_link: Optional[str] = None
    jbrowse_link: Optional[str] = None


class PatmatchSearchResult(BaseModel):
    """Complete pattern match search result."""
    pattern: str
    pattern_type: str
    dataset: str
    strand: str
    total_hits: int
    hits: List[PatmatchHit]
    search_params: dict
    # Statistics
    sequences_searched: int
    total_residues_searched: int


class PatmatchSearchResponse(BaseModel):
    """API response for pattern match search."""
    success: bool
    result: Optional[PatmatchSearchResult] = None
    error: Optional[str] = None


class DatasetInfo(BaseModel):
    """Information about a searchable dataset."""
    name: str
    display_name: str
    description: str
    pattern_type: PatternType
    sequence_count: Optional[int] = None


class PatmatchConfigResponse(BaseModel):
    """Response with pattern match configuration options."""
    datasets: List[DatasetInfo]
    max_pattern_length: int = 100
    max_mismatches: int = 3
    max_insertions: int = 3
    max_deletions: int = 3
