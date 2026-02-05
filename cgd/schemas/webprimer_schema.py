"""
WebPrimer Schema - Primer design request/response models.
"""
from __future__ import annotations

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field


class PrimerPurpose(str, Enum):
    PCR = "PCR"
    SEQUENCING = "SEQUENCING"


class SequencingStrand(str, Enum):
    CODING = "CODING"
    NON_CODING = "NON-CODING"


class SequencingStrandCount(str, Enum):
    ONE = "ONE"
    BOTH = "BOTH"


# --- Request Models ---

class WebPrimerConfigResponse(BaseModel):
    """Configuration for primer design form."""
    default_params: dict
    purpose_options: List[str] = ["PCR", "SEQUENCING"]


class WebPrimerSequenceRequest(BaseModel):
    """Request to get sequence for a locus."""
    locus: str = Field(..., description="Gene/ORF name")


class WebPrimerSequenceResponse(BaseModel):
    """Response with sequence for a locus."""
    success: bool
    sequence: Optional[str] = None
    locus: Optional[str] = None
    error: Optional[str] = None


class WebPrimerRequest(BaseModel):
    """Request for primer design."""
    # Sequence input
    sequence: str = Field(..., min_length=20, description="DNA sequence")

    # Purpose
    purpose: PrimerPurpose = PrimerPurpose.PCR

    # Location parameters (for locus-based input)
    bp_from_start: int = Field(default=35, description="Distance from START codon (upstream)")
    bp_from_stop: int = Field(default=35, description="Distance from STOP codon (downstream)")
    specific_ends: bool = Field(default=False, description="Use exact endpoints")
    parsed_length: int = Field(default=35, ge=10, le=100, description="Length of DNA to search for primers")

    # Sequencing parameters
    seq_strand_count: SequencingStrandCount = SequencingStrandCount.ONE
    seq_strand: SequencingStrand = SequencingStrand.CODING
    seq_spacing: int = Field(default=250, ge=50, le=1000, description="Distance between sequencing primers")

    # Melting temperature
    opt_tm: float = Field(default=56, ge=40, le=80, description="Optimum Tm")
    min_tm: float = Field(default=52, ge=40, le=80, description="Minimum Tm")
    max_tm: float = Field(default=60, ge=40, le=80, description="Maximum Tm")

    # Primer length
    opt_length: int = Field(default=20, ge=15, le=35, description="Optimum primer length")
    min_length: int = Field(default=18, ge=10, le=35, description="Minimum primer length")
    max_length: int = Field(default=21, ge=15, le=35, description="Maximum primer length")

    # GC content
    opt_gc: float = Field(default=45, ge=20, le=80, description="Optimum GC %")
    min_gc: float = Field(default=30, ge=20, le=80, description="Minimum GC %")
    max_gc: float = Field(default=60, ge=20, le=80, description="Maximum GC %")

    # Annealing parameters
    max_self_anneal: int = Field(default=24, ge=0, le=50, description="Max self annealing score")
    max_self_end_anneal: int = Field(default=12, ge=0, le=50, description="Max self end annealing score")
    max_pair_anneal: int = Field(default=24, ge=0, le=50, description="Max pair annealing score (PCR)")
    max_pair_end_anneal: int = Field(default=12, ge=0, le=50, description="Max pair end annealing score (PCR)")

    # Concentration parameters (advanced)
    dna_conc: float = Field(default=50, description="DNA concentration (nM)")
    salt_conc: float = Field(default=50, description="Salt concentration (mM)")

    # Limits
    max_results: int = Field(default=250, ge=1, le=500, description="Maximum primer pairs to return")


# --- Response Models ---

class PrimerResult(BaseModel):
    """Single primer result."""
    sequence: str
    length: int
    tm: float
    gc_percent: float
    self_anneal: int
    self_end_anneal: int
    position: int
    strand: str = "CODING"


class PrimerPairResult(BaseModel):
    """PCR primer pair result."""
    forward: PrimerResult
    reverse: PrimerResult
    pair_anneal: int
    pair_end_anneal: int
    product_length: int
    rank: int
    score: float


class SequencingPrimerResult(BaseModel):
    """Sequencing primer result."""
    primer: PrimerResult
    rank: int


class WebPrimerResponse(BaseModel):
    """Response for primer design."""
    success: bool
    purpose: str
    sequence_length: int
    amplified_sequence: Optional[str] = None

    # PCR results
    best_pair: Optional[PrimerPairResult] = None
    all_pairs: List[PrimerPairResult] = []
    total_pairs: int = 0

    # Sequencing results
    coding_primers: List[SequencingPrimerResult] = []
    noncoding_primers: List[SequencingPrimerResult] = []

    # Stats
    forward_gc_valid: int = 0
    forward_tm_valid: int = 0
    forward_self_valid: int = 0
    reverse_gc_valid: int = 0
    reverse_tm_valid: int = 0
    reverse_self_valid: int = 0

    error: Optional[str] = None
    warnings: List[str] = []
