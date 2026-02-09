"""
Sequence Alignment Router - Compare and align two sequences.

Provides a tool for curators to compare sequences during curation work.

Requires curator authentication.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from cgd.auth.deps import CurrentUser
from cgd.api.services.curation.seq_alignment_service import SeqAlignmentService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/curation/seq-alignment",
    tags=["curation-seq-alignment"],
)


# ---------------------------
# Request/Response Schemas
# ---------------------------


class AlignmentRequest(BaseModel):
    """Request to align two sequences."""

    seq1: str = Field(..., description="First sequence")
    seq2: str = Field(..., description="Second sequence")
    seq1_name: str = Field(default="Current", description="Name for first sequence")
    seq2_name: str = Field(default="New", description="Name for second sequence")


class AlignmentBlock(BaseModel):
    """A block of aligned sequences for display."""

    seq1: str
    seq2: str
    symbols: str
    seq1_start: int
    seq1_end: int
    seq2_start: int
    seq2_end: int


class AlignmentResponse(BaseModel):
    """Response with alignment results."""

    seq1_name: str
    seq2_name: str
    seq1_length: int
    seq2_length: int
    aligned_length: int
    matches: int
    mismatches: int
    gaps: int
    identity_percent: float
    aligned_seq1: str
    aligned_seq2: str
    symbols: str
    blocks: list[AlignmentBlock]


class CompareRequest(BaseModel):
    """Request to compare two sequences."""

    seq1: str = Field(..., description="First sequence")
    seq2: str = Field(..., description="Second sequence")


class SequenceDifference(BaseModel):
    """A single position difference between sequences."""

    position: int
    seq1_char: str
    seq2_char: str


class CompareResponse(BaseModel):
    """Response with comparison results."""

    seq1_length: int
    seq2_length: int
    length_difference: int
    identical: bool
    difference_count: int
    differences: list[SequenceDifference]


# ---------------------------
# Endpoints
# ---------------------------


@router.post("/align", response_model=AlignmentResponse)
def align_sequences(
    request: AlignmentRequest,
    current_user: CurrentUser = None,
):
    """
    Align two sequences and show the alignment.

    Uses global alignment to show matches, mismatches, and gaps
    between the sequences.
    """
    if not request.seq1.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="First sequence is required",
        )
    if not request.seq2.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Second sequence is required",
        )

    # Limit sequence length
    max_length = 50000
    if len(request.seq1) > max_length or len(request.seq2) > max_length:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Sequences must be less than {max_length} characters",
        )

    service = SeqAlignmentService()
    result = service.align_sequences(
        request.seq1,
        request.seq2,
        request.seq1_name,
        request.seq2_name,
    )

    return AlignmentResponse(**result)


@router.post("/compare", response_model=CompareResponse)
def compare_sequences(
    request: CompareRequest,
    current_user: CurrentUser = None,
):
    """
    Quick comparison of two sequences.

    Returns length differences and position-by-position differences
    without full alignment.
    """
    if not request.seq1.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="First sequence is required",
        )
    if not request.seq2.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Second sequence is required",
        )

    service = SeqAlignmentService()
    result = service.compare_sequences(request.seq1, request.seq2)

    return CompareResponse(**result)
