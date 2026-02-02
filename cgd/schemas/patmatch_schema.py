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
    # DNA datasets
    CHROMOSOMES = "chromosomes"
    ORF_GENOMIC = "orf_genomic"
    ORF_CODING = "orf_coding"
    ORF_GENOMIC_1KB = "orf_genomic_1kb"
    INTERGENIC = "intergenic"
    NONCODING = "noncoding"
    # Protein datasets
    ORF_PROTEIN = "orf_protein"


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
        SequenceDataset.CHROMOSOMES,
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
