"""
GO Router - API endpoints for GO term pages.
"""
from __future__ import annotations

import logging
import traceback

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.api.services import go_service
from cgd.schemas.go_schema import GoTermResponse, GoEvidenceResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/go", tags=["go"])


@router.get("/evidence", response_model=GoEvidenceResponse)
def get_go_evidence_codes(db: Session = Depends(get_db)):
    """
    Get all GO evidence codes with their definitions and examples.

    Returns a list of evidence codes used in GO annotations,
    including the code abbreviation, full definition, and example usages.
    """
    try:
        return go_service.get_go_evidence_codes(db)
    except Exception as e:
        logger.error(f"Error in get_go_evidence_codes: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{goid}", response_model=GoTermResponse)
def get_go_term(goid: str, db: Session = Depends(get_db)):
    """
    Get GO term information and annotated genes by GOID.

    Args:
        goid: GO identifier (e.g., "GO:0005634" or "5634")

    Returns:
        GO term info with definition, synonyms, and all annotated genes
        grouped by annotation type (manually curated, high-throughput, computational).
    """
    try:
        return go_service.get_go_term_info(db, goid)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_go_term for {goid}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
