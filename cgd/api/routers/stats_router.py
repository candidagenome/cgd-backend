"""
Site statistics API Router.
"""
import logging
import traceback

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.schemas.stats_schema import (
    CountsByOrganismResponse,
    RecentActivityResponse,
    RecentOrthologClustersResponse,
    StatsSummaryResponse,
)
from cgd.api.services.stats_service import (
    get_counts_by_organism,
    get_recent_activity,
    get_recent_ortholog_clusters,
    get_stats_summary,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stats", tags=["stats"])


@router.get("/recent-ortholog-clusters", response_model=RecentOrthologClustersResponse)
def recent_ortholog_clusters(
    days: int = Query(90, ge=1, le=365),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    query: str = Query("", max_length=100),
    db: Session = Depends(get_db),
):
    """List recently created ortholog/homology clusters."""
    return get_recent_ortholog_clusters(db, days=days, page=page, limit=limit, query=query)


@router.get("/recent-activity", response_model=RecentActivityResponse)
def recent_activity(
    days: int = Query(90, ge=1, le=365), db: Session = Depends(get_db)
):
    """Get recent reference, phenotype-annotation, and ortholog-cluster counts."""
    result = get_recent_activity(db, days=days)
    if not result.success:
        raise HTTPException(status_code=500, detail=result.error)
    return result


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
