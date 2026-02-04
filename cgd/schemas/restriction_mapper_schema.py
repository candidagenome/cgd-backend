"""
Restriction Mapper Schemas.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field


class EnzymeFilterType(str, Enum):
    """Filter type for restriction enzymes."""
    ALL = "all"
    THREE_PRIME_OVERHANG = "3_overhang"
    FIVE_PRIME_OVERHANG = "5_overhang"
    BLUNT = "blunt"
    CUT_ONCE = "cut_once"
    CUT_TWICE = "cut_twice"
    SIX_BASE = "six_base"
    NO_CUT = "no_cut"


class EnzymeType(str, Enum):
    """Type of restriction enzyme based on overhang."""
    THREE_PRIME = "3_prime"
    FIVE_PRIME = "5_prime"
    BLUNT = "blunt"


class RestrictionMapperRequest(BaseModel):
    """Request for restriction mapping."""
    locus: Optional[str] = Field(
        None,
        description="Gene name, ORF name, or CGDID to map"
    )
    sequence: Optional[str] = Field(
        None,
        max_length=100000,
        description="Raw DNA sequence to map (alternative to locus)"
    )
    sequence_name: Optional[str] = Field(
        None,
        description="Name for the sequence (used when providing raw sequence)"
    )
    enzyme_filter: EnzymeFilterType = Field(
        EnzymeFilterType.ALL,
        description="Filter for which enzymes to include"
    )


class EnzymeCutSite(BaseModel):
    """Cut site information for a single enzyme."""
    enzyme_name: str = Field(..., description="Name of the restriction enzyme")
    recognition_seq: str = Field(..., description="Recognition sequence pattern")
    enzyme_type: EnzymeType = Field(..., description="Type of enzyme (3', 5', or blunt)")
    cut_positions_watson: List[int] = Field(
        default_factory=list,
        description="Cut positions on Watson (+ strand), 1-based"
    )
    cut_positions_crick: List[int] = Field(
        default_factory=list,
        description="Cut positions on Crick (- strand), 1-based"
    )
    total_cuts: int = Field(..., description="Total number of cuts")
    fragment_sizes: List[int] = Field(
        default_factory=list,
        description="Sizes of DNA fragments after digestion"
    )


class RestrictionMapResult(BaseModel):
    """Complete restriction mapping result."""
    seq_name: str = Field(..., description="Name of the mapped sequence")
    seq_length: int = Field(..., description="Length of the sequence in bp")
    coordinates: Optional[str] = Field(
        None,
        description="Genomic coordinates if from a locus"
    )
    cutting_enzymes: List[EnzymeCutSite] = Field(
        default_factory=list,
        description="Enzymes that cut the sequence"
    )
    non_cutting_enzymes: List[str] = Field(
        default_factory=list,
        description="Enzymes that do not cut the sequence"
    )
    total_enzymes_searched: int = Field(
        ...,
        description="Total number of enzymes searched"
    )


class RestrictionMapperResponse(BaseModel):
    """API response for restriction mapping."""
    success: bool
    result: Optional[RestrictionMapResult] = None
    error: Optional[str] = None


class EnzymeFilterInfo(BaseModel):
    """Information about an enzyme filter option."""
    value: EnzymeFilterType
    display_name: str
    description: str


class RestrictionMapperConfigResponse(BaseModel):
    """Response with restriction mapper configuration options."""
    enzyme_filters: List[EnzymeFilterInfo]
    total_enzymes: int = Field(..., description="Total number of enzymes in database")
