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
    ColleagueFormConfigResponse,
    ColleagueSubmissionRequest,
    ColleagueSubmissionResponse,
)
from cgd.api.services.colleague_service import (
    search_colleagues,
    get_colleague_detail,
    get_colleague_form_config,
    submit_colleague,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/colleague", tags=["colleague"])


@router.get("/form-config", response_model=ColleagueFormConfigResponse)
def get_form_config(
    db: Session = Depends(get_db),
):
    """
    Get configuration for colleague registration/update form.

    Returns lists of countries, US states, Canadian provinces,
    professions, and job positions for form dropdowns.
    """
    try:
        config = get_colleague_form_config(db)
        return ColleagueFormConfigResponse(**config)
    except Exception as e:
        logger.error(f"Form config error: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/submit", response_model=ColleagueSubmissionResponse)
def submit(
    request: ColleagueSubmissionRequest,
    db: Session = Depends(get_db),
):
    """
    Submit colleague registration or update.

    For new registrations, omit colleague_no.
    For updates, include the colleague_no.

    The submission will be queued for curator review.
    """
    try:
        result = submit_colleague(
            db,
            request.colleague_no,
            request.data.model_dump(),
        )
        return ColleagueSubmissionResponse(**result)
    except Exception as e:
        logger.error(f"Colleague submission error: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


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
