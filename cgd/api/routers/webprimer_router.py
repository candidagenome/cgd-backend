"""
WebPrimer API Router.

Provides endpoints for primer design.
"""
import logging
import traceback
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.schemas.webprimer_schema import (
    WebPrimerConfigResponse,
    WebPrimerSequenceRequest,
    WebPrimerSequenceResponse,
    WebPrimerRequest,
    WebPrimerResponse,
)
from cgd.api.services.webprimer_service import (
    get_webprimer_config,
    get_sequence_for_locus,
    design_primers,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/webprimer", tags=["webprimer"])


@router.get("/config", response_model=WebPrimerConfigResponse)
def get_config():
    """
    Get default configuration for primer design.

    Returns default parameter values for the primer design form.
    """
    return get_webprimer_config()


@router.post("/sequence", response_model=WebPrimerSequenceResponse)
def get_sequence(
    request: WebPrimerSequenceRequest,
    db: Session = Depends(get_db),
):
    """
    Get genomic sequence for a locus.

    Given a gene/ORF name, returns the genomic DNA sequence.
    """
    try:
        return get_sequence_for_locus(db, request.locus)
    except Exception as e:
        logger.error(f"Sequence fetch error: {e}")
        logger.error(traceback.format_exc())
        return WebPrimerSequenceResponse(
            success=False,
            error=str(e)
        )


@router.get("/sequence/{locus}", response_model=WebPrimerSequenceResponse)
def get_sequence_by_name(
    locus: str,
    db: Session = Depends(get_db),
):
    """
    Get genomic sequence for a locus by name.

    Given a gene/ORF name, returns the genomic DNA sequence.
    """
    try:
        return get_sequence_for_locus(db, locus)
    except Exception as e:
        logger.error(f"Sequence fetch error: {e}")
        logger.error(traceback.format_exc())
        return WebPrimerSequenceResponse(
            success=False,
            error=str(e)
        )


@router.post("/design", response_model=WebPrimerResponse)
def design(request: WebPrimerRequest):
    """
    Design primers for PCR or sequencing.

    Takes a DNA sequence and primer parameters, returns optimal primers.

    For PCR:
    - Returns best primer pair and list of all valid pairs
    - Pairs are ranked by score (lower is better)
    - Score considers Tm, GC%, self-annealing, pair-annealing

    For Sequencing:
    - Returns primers spaced along the sequence
    - Can design for coding strand, non-coding strand, or both
    """
    try:
        # Validate parameter relationships
        if request.min_tm > request.opt_tm or request.opt_tm > request.max_tm:
            raise HTTPException(
                status_code=400,
                detail="Tm values must satisfy: min <= opt <= max"
            )

        if request.min_gc > request.opt_gc or request.opt_gc > request.max_gc:
            raise HTTPException(
                status_code=400,
                detail="GC% values must satisfy: min <= opt <= max"
            )

        if request.min_length > request.opt_length or request.opt_length > request.max_length:
            raise HTTPException(
                status_code=400,
                detail="Length values must satisfy: min <= opt <= max"
            )

        if request.parsed_length < request.min_length:
            raise HTTPException(
                status_code=400,
                detail="Search length must be >= minimum primer length"
            )

        return design_primers(request)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Primer design error: {e}")
        logger.error(traceback.format_exc())
        return WebPrimerResponse(
            success=False,
            purpose=request.purpose.value,
            sequence_length=len(request.sequence) if request.sequence else 0,
            error=str(e)
        )
