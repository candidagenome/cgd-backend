"""
Genome Version History API Router.
"""
import logging
import traceback
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.schemas.genome_version_schema import (
    GenomeVersionConfigResponse,
    GenomeVersionHistoryResponse,
)
from cgd.api.services.genome_version_service import (
    get_genome_version_config,
    get_genome_version_history,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/genome-version", tags=["genome-version"])


@router.get("/config", response_model=GenomeVersionConfigResponse)
def get_config(
    db: Session = Depends(get_db),
):
    """
    Get genome version page configuration.

    Returns available strains/assemblies and version format explanation.
    """
    try:
        return get_genome_version_config(db)
    except Exception as e:
        logger.error(f"Error getting genome version config: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history/{seq_source}", response_model=GenomeVersionHistoryResponse)
def get_history(
    seq_source: str,
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(20, ge=1, le=100, description="Results per page"),
    db: Session = Depends(get_db),
):
    """
    Get genome version history for a specific strain/assembly.

    Args:
        seq_source: Organism abbreviation (e.g., C_albicans_SC5314)
        page: Page number (1-indexed)
        page_size: Results per page (max 100)

    Returns:
        Paginated list of genome versions with dates and descriptions.
    """
    try:
        return get_genome_version_history(db, seq_source, page, page_size)
    except Exception as e:
        logger.error(f"Error getting genome version history for {seq_source}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history", response_model=GenomeVersionHistoryResponse)
def get_history_query(
    seq_source: str = Query(..., description="Organism abbreviation"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(20, ge=1, le=100, description="Results per page"),
    db: Session = Depends(get_db),
):
    """
    Get genome version history (using query parameter).

    Alternative endpoint using query parameter instead of path parameter.
    """
    try:
        return get_genome_version_history(db, seq_source, page, page_size)
    except Exception as e:
        logger.error(f"Error getting genome version history for {seq_source}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
