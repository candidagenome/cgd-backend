"""
Site statistics API Router.
"""
import logging
import traceback

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.schemas.stats_schema import StatsSummaryResponse, CountsByOrganismResponse
from cgd.api.services.stats_service import get_stats_summary, get_counts_by_organism

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stats", tags=["stats"])


@router.get("/summary", response_model=StatsSummaryResponse)
def stats_summary(refresh: bool = False, db: Session = Depends(get_db)):
    """
    Get database-wide totals (genes, references, phenotypes, GO annotations,
    ortholog clusters, interactions, colleagues) for the Explore/landing page.

    Results are cached in-process for up to an hour; pass ?refresh=true to
    force a recompute.
    """
    try:
        result = get_stats_summary(db, refresh=refresh)
        if not result.success:
            raise HTTPException(status_code=500, detail=result.error)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting stats summary: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/counts-by-organism", response_model=CountsByOrganismResponse)
def counts_by_organism(refresh: bool = False, db: Session = Depends(get_db)):
    """
    Get per-organism category totals (genes, references, phenotypes, GO
    annotations, ortholog clusters, interactions) for the Explore-page
    organism filter. Cached in-process for up to an hour.
    """
    try:
        result = get_counts_by_organism(db, refresh=refresh)
        if not result.success:
            raise HTTPException(status_code=500, detail=result.error)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting counts by organism: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
