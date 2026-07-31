"""
Gene-of-the-day API Router.
"""
import logging
import traceback

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.schemas.stats_schema import GeneOfTheDayResponse
from cgd.api.services.gene_of_the_day_service import get_gene_of_the_day

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["gene-of-the-day"])


@router.get("/gene-of-the-day", response_model=GeneOfTheDayResponse)
def gene_of_the_day(db: Session = Depends(get_db)):
    """
    Get the deterministic gene of the day (a named, characterized
    C. albicans SC5314 ORF that rotates once per calendar day).
    """
    try:
        result = get_gene_of_the_day(db)
        if not result.success:
            raise HTTPException(status_code=404, detail=result.error)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting gene of the day: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
