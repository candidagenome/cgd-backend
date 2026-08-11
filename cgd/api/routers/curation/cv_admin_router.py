"""
CV Admin Router - Endpoints for maintaining CGD-managed controlled vocabularies.

Backs the "Manage CV Terms" curator tool (phenotype strains, literature
topics, and other small CVs). Requires curator authentication.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.cv_admin_service import CvAdminService, CvAdminError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/curation/cv-admin", tags=["curation-cv-admin"])


class CvOut(BaseModel):
    cv_no: int
    cv_name: str
    label: str
    description: str
    featured: bool
    term_count: int


class CvListResponse(BaseModel):
    cvs: list[CvOut]


class CvTermOut(BaseModel):
    cv_term_no: int
    term_name: str
    parent_cv_term_no: Optional[int] = None
    parent_term_name: Optional[str] = None
    date_created: Optional[str] = None
    created_by: Optional[str] = None


class CvTermsResponse(BaseModel):
    cv_no: int
    cv_name: str
    terms: list[CvTermOut]


class AddTermRequest(BaseModel):
    term_name: str = Field(..., min_length=1, max_length=1024)
    parent_cv_term_no: Optional[int] = Field(
        None,
        description="Parent term in the same CV; omit for a top-level term",
    )


class AddTermResponse(BaseModel):
    cv_term_no: int
    term_name: str
    cv_name: str
    parent_cv_term_no: Optional[int] = None
    parent_term_name: Optional[str] = None


@router.get("/cvs", response_model=CvListResponse)
def list_editable_cvs(current_user: CurrentUser, db: Session = Depends(get_db)):
    """List the CVs curators may edit through this tool."""
    return CvListResponse(cvs=CvAdminService(db).list_cvs())


@router.get("/cv/{cv_name}/terms", response_model=CvTermsResponse)
def get_cv_terms(cv_name: str, current_user: CurrentUser, db: Session = Depends(get_db)):
    """All terms of a CV with parent assignments."""
    try:
        return CvTermsResponse(**CvAdminService(db).get_cv_terms(cv_name))
    except CvAdminError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.post("/cv/{cv_name}/terms", response_model=AddTermResponse)
def add_cv_term(
    cv_name: str,
    request: AddTermRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Add a new term to a CV, optionally under a parent term."""
    try:
        result = CvAdminService(db).add_term(
            cv_name=cv_name,
            term_name=request.term_name,
            curator_userid=current_user.userid,
            parent_cv_term_no=request.parent_cv_term_no,
        )
        return AddTermResponse(**result)
    except CvAdminError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception:
        db.rollback()
        logger.exception("Failed to add CV term")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to add CV term",
        )
