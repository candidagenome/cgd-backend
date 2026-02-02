"""Sequence retrieval schema definitions."""
from __future__ import annotations

from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class SeqType(str, Enum):
    """Sequence type options."""
    GENOMIC = "genomic"
    PROTEIN = "protein"
    CODING = "coding"  # Exons only (CDS)


class SeqFormat(str, Enum):
    """Output format options."""
    FASTA = "fasta"
    RAW = "raw"  # Plain sequence without header
    JSON = "json"


class SequenceRequest(BaseModel):
    """Request parameters for sequence retrieval."""
    # Query by identifier (gene name, ORF, etc.)
    query: Optional[str] = Field(
        None,
        description="Gene name, ORF name, feature name, or CGDID"
    )

    # Query by coordinates
    chr: Optional[str] = Field(None, description="Chromosome name")
    start: Optional[int] = Field(None, alias="beg", description="Start coordinate")
    end: Optional[int] = Field(None, description="End coordinate")

    # Sequence options
    seq_type: SeqType = Field(
        SeqType.GENOMIC,
        description="Type of sequence to retrieve"
    )
    format: SeqFormat = Field(
        SeqFormat.FASTA,
        description="Output format"
    )

    # Flanking regions
    flank_left: int = Field(0, alias="flankl", ge=0, le=10000, description="Left flanking bp")
    flank_right: int = Field(0, alias="flankr", ge=0, le=10000, description="Right flanking bp")

    # Reverse complement
    reverse_complement: bool = Field(False, description="Return reverse complement")

    class Config:
        populate_by_name = True


class SequenceInfo(BaseModel):
    """Metadata about the retrieved sequence."""
    feature_name: Optional[str] = None
    gene_name: Optional[str] = None
    dbxref_id: Optional[str] = None
    organism: Optional[str] = None
    chromosome: Optional[str] = None
    start: Optional[int] = None
    end: Optional[int] = None
    strand: Optional[str] = None
    seq_type: str
    length: int


class SequenceResponse(BaseModel):
    """Response containing sequence data."""
    sequence: str
    info: SequenceInfo
    fasta_header: Optional[str] = None


class CoordinateSequenceResponse(BaseModel):
    """Response for coordinate-based sequence retrieval."""
    chromosome: str
    start: int
    end: int
    strand: str
    sequence: str
    length: int
    fasta_header: Optional[str] = None
