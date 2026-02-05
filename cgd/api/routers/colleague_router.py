"""
Colleague Search API Router.
"""
import logging
import traceback
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.schemas.colleague_schema import (
    ColleagueSearchResponse,
    ColleagueDetailResponse,
)
from cgd.api.services.colleague_service import (
    search_colleagues,
    get_colleague_detail,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/colleague", tags=["colleague"])


@router.get("/search", response_model=ColleagueSearchResponse)
def search(
    last_name: str = Query(..., min_length=1, description="Last name to search"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Results per page"),
    db: Session = Depends(get_db),
):
    """
    Search colleagues by last name.

    Searches both last_name and other_last_name fields (case-insensitive).
    Supports wildcard (*) at any position.
    If no results found, automatically appends wildcard and retries.

    Args:
        last_name: Last name to search (e.g., "Smith", "Bot*")
        page: Page number (1-indexed)
        page_size: Results per page (max 100)

    Returns:
        List of matching colleagues with contact info.
    """
    try:
        return search_colleagues(db, last_name, page, page_size)
    except Exception as e:
        logger.error(f"Colleague search error: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{colleague_no}", response_model=ColleagueDetailResponse)
def get_detail(
    colleague_no: int,
    db: Session = Depends(get_db),
):
    """
    Get detailed information for a colleague.

    Returns full colleague profile including:
    - Contact information (email, phone, address)
    - Organization and position
    - Lab relationships (PI, lab members, associates)
    - Associated genes
    - Research interests and keywords

    Args:
        colleague_no: Colleague ID

    Returns:
        Full colleague details.
    """
    try:
        return get_colleague_detail(db, colleague_no)
    except Exception as e:
        logger.error(f"Colleague detail error for {colleague_no}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
