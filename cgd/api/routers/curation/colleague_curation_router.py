"""
Colleague Curation Router - Endpoints for colleague CRUD operations.

Requires curator authentication.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.colleague_curation_service import (
    ColleagueCurationService,
    ColleagueCurationError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/curation/colleague", tags=["curation-colleague"])


# ---------------------------
# Request/Response Schemas
# ---------------------------


class ColleagueListItem(BaseModel):
    """Colleague summary for list view."""

    colleague_no: int
    first_name: str
    last_name: str
    email: Optional[str]
    institution: Optional[str]
    is_pi: str
    is_contact: str
    date_created: Optional[str]
    date_modified: Optional[str]


class ColleagueListResponse(BaseModel):
    """Response for colleague list."""

    colleagues: list[ColleagueListItem]
    total: int
    page: int
    page_size: int


class UrlOut(BaseModel):
    """URL in colleague details."""

    coll_url_no: int
    url_no: int
    url_type: str
    link: str


class KeywordOut(BaseModel):
    """Keyword in colleague details."""

    coll_kw_no: int
    keyword_no: int
    keyword: str


class FeatureOut(BaseModel):
    """Feature in colleague details."""

    coll_feat_no: int
    feature_no: int
    feature_name: str
    gene_name: Optional[str]


class RelationshipOut(BaseModel):
    """Relationship in colleague details."""

    coll_relationship_no: int
    associate_no: int
    associate_name: str
    relationship_type: str


class RemarkOut(BaseModel):
    """Remark in colleague details."""

    colleague_remark_no: int
    remark_type: str
    remark_text: str
    date_created: Optional[str]


class ColleagueDetailResponse(BaseModel):
    """Full colleague details for curation."""

    colleague_no: int
    first_name: str
    last_name: str
    suffix: Optional[str]
    other_last_name: Optional[str]
    email: Optional[str]
    profession: Optional[str]
    job_title: Optional[str]
    institution: Optional[str]
    address1: Optional[str]
    address2: Optional[str]
    address3: Optional[str]
    address4: Optional[str]
    address5: Optional[str]
    city: Optional[str]
    state: Optional[str]
    region: Optional[str]
    country: Optional[str]
    postal_code: Optional[str]
    work_phone: Optional[str]
    other_phone: Optional[str]
    fax: Optional[str]
    is_pi: str
    is_contact: str
    source: str
    date_created: Optional[str]
    date_modified: Optional[str]
    created_by: str
    urls: list[UrlOut]
    keywords: list[KeywordOut]
    features: list[FeatureOut]
    relationships: list[RelationshipOut]
    remarks: list[RemarkOut]


class CreateColleagueRequest(BaseModel):
    """Request to create a new colleague."""

    first_name: str = Field(..., description="First name")
    last_name: str = Field(..., description="Last name")
    suffix: Optional[str] = Field(None, description="Suffix (Jr., Sr., etc.)")
    other_last_name: Optional[str] = Field(None, description="Other last name")
    email: Optional[str] = Field(None, description="Email address")
    profession: Optional[str] = Field(None, description="Profession")
    job_title: Optional[str] = Field(None, description="Job title")
    institution: Optional[str] = Field(None, description="Institution")
    address1: Optional[str] = Field(None, description="Address line 1")
    address2: Optional[str] = Field(None, description="Address line 2")
    address3: Optional[str] = Field(None, description="Address line 3")
    address4: Optional[str] = Field(None, description="Address line 4")
    address5: Optional[str] = Field(None, description="Address line 5")
    city: Optional[str] = Field(None, description="City")
    state: Optional[str] = Field(None, description="State/province")
    region: Optional[str] = Field(None, description="Region")
    country: Optional[str] = Field(None, description="Country")
    postal_code: Optional[str] = Field(None, description="Postal code")
    work_phone: Optional[str] = Field(None, description="Work phone")
    other_phone: Optional[str] = Field(None, description="Other phone")
    fax: Optional[str] = Field(None, description="Fax")
    is_pi: str = Field(default="N", description="Is PI (Y/N)")
    is_contact: str = Field(default="N", description="Is contact (Y/N)")


class CreateColleagueResponse(BaseModel):
    """Response for colleague creation."""

    colleague_no: int
    message: str


class UpdateColleagueRequest(BaseModel):
    """Request to update a colleague."""

    first_name: Optional[str] = None
    last_name: Optional[str] = None
    suffix: Optional[str] = None
    other_last_name: Optional[str] = None
    email: Optional[str] = None
    profession: Optional[str] = None
    job_title: Optional[str] = None
    institution: Optional[str] = None
    address1: Optional[str] = None
    address2: Optional[str] = None
    address3: Optional[str] = None
    address4: Optional[str] = None
    address5: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    region: Optional[str] = None
    country: Optional[str] = None
    postal_code: Optional[str] = None
    work_phone: Optional[str] = None
    other_phone: Optional[str] = None
    fax: Optional[str] = None
    is_pi: Optional[str] = None
    is_contact: Optional[str] = None


class SuccessResponse(BaseModel):
    """Generic success response."""

    success: bool
    message: str


class AddUrlRequest(BaseModel):
    """Request to add URL to colleague."""

    url_type: str = Field(..., description="URL type (e.g., Lab, Personal)")
    link: str = Field(..., description="URL")


class AddKeywordRequest(BaseModel):
    """Request to add keyword to colleague."""

    keyword: str = Field(..., description="Keyword")


class AddFeatureRequest(BaseModel):
    """Request to add feature to colleague."""

    feature_name: str = Field(..., description="Feature or gene name")


class AddRelationshipRequest(BaseModel):
    """Request to add relationship between colleagues."""

    associate_no: int = Field(..., description="Associate colleague number")
    relationship_type: str = Field(
        ..., description="Relationship type (e.g., PI, lab member)"
    )


class AddRemarkRequest(BaseModel):
    """Request to add remark to colleague."""

    remark_type: str = Field(..., description="Remark type")
    remark_text: str = Field(..., description="Remark text")


class AddItemResponse(BaseModel):
    """Response for adding an item."""

    id: int
    message: str


# ---------------------------
# Endpoints
# ---------------------------


@router.get("/list", response_model=ColleagueListResponse)
def list_colleagues(
    current_user: CurrentUser,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Results per page"),
    db: Session = Depends(get_db),
):
    """
    Get paginated list of all colleagues.

    For curator management of colleague records.
    """
    service = ColleagueCurationService(db)

    colleagues, total = service.get_all_colleagues(page, page_size)

    return ColleagueListResponse(
        colleagues=[ColleagueListItem(**c) for c in colleagues],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{colleague_no}", response_model=ColleagueDetailResponse)
def get_colleague_details(
    colleague_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Get full colleague details for curation.

    Includes URLs, keywords, features, relationships, and remarks.
    """
    service = ColleagueCurationService(db)

    try:
        details = service.get_colleague_details(colleague_no)
        return ColleagueDetailResponse(**details)
    except ColleagueCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )


@router.post("/", response_model=CreateColleagueResponse)
def create_colleague(
    request: CreateColleagueRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Create a new colleague.
    """
    service = ColleagueCurationService(db)

    try:
        colleague_no = service.create_colleague(
            curator_userid=current_user.userid,
            **request.model_dump(),
        )

        return CreateColleagueResponse(
            colleague_no=colleague_no,
            message="Colleague created successfully",
        )
    except ColleagueCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.put("/{colleague_no}", response_model=SuccessResponse)
def update_colleague(
    colleague_no: int,
    request: UpdateColleagueRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Update an existing colleague.
    """
    service = ColleagueCurationService(db)

    try:
        # Filter out None values
        update_data = {k: v for k, v in request.model_dump().items() if v is not None}

        service.update_colleague(
            colleague_no=colleague_no,
            curator_userid=current_user.userid,
            **update_data,
        )

        return SuccessResponse(
            success=True,
            message=f"Colleague {colleague_no} updated successfully",
        )
    except ColleagueCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/{colleague_no}", response_model=SuccessResponse)
def delete_colleague(
    colleague_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Delete a colleague.
    """
    service = ColleagueCurationService(db)

    try:
        service.delete_colleague(colleague_no, current_user.userid)

        return SuccessResponse(
            success=True,
            message=f"Colleague {colleague_no} deleted",
        )
    except ColleagueCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{colleague_no}/url", response_model=AddItemResponse)
def add_colleague_url(
    colleague_no: int,
    request: AddUrlRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Add URL to colleague."""
    service = ColleagueCurationService(db)

    try:
        coll_url_no = service.add_colleague_url(
            colleague_no,
            request.url_type,
            request.link,
            current_user.userid,
        )

        return AddItemResponse(
            id=coll_url_no,
            message="URL added to colleague",
        )
    except ColleagueCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/url/{coll_url_no}", response_model=SuccessResponse)
def remove_colleague_url(
    coll_url_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Remove URL from colleague."""
    service = ColleagueCurationService(db)

    try:
        service.remove_colleague_url(coll_url_no)

        return SuccessResponse(
            success=True,
            message="URL removed from colleague",
        )
    except ColleagueCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{colleague_no}/keyword", response_model=AddItemResponse)
def add_colleague_keyword(
    colleague_no: int,
    request: AddKeywordRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Add keyword to colleague."""
    service = ColleagueCurationService(db)

    try:
        coll_kw_no = service.add_colleague_keyword(
            colleague_no,
            request.keyword,
            current_user.userid,
        )

        return AddItemResponse(
            id=coll_kw_no,
            message="Keyword added to colleague",
        )
    except ColleagueCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/keyword/{coll_kw_no}", response_model=SuccessResponse)
def remove_colleague_keyword(
    coll_kw_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Remove keyword from colleague."""
    service = ColleagueCurationService(db)

    try:
        service.remove_colleague_keyword(coll_kw_no)

        return SuccessResponse(
            success=True,
            message="Keyword removed from colleague",
        )
    except ColleagueCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{colleague_no}/feature", response_model=AddItemResponse)
def add_colleague_feature(
    colleague_no: int,
    request: AddFeatureRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Add feature association to colleague."""
    service = ColleagueCurationService(db)

    try:
        coll_feat_no = service.add_colleague_feature(
            colleague_no,
            request.feature_name,
            current_user.userid,
        )

        return AddItemResponse(
            id=coll_feat_no,
            message="Feature added to colleague",
        )
    except ColleagueCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/feature/{coll_feat_no}", response_model=SuccessResponse)
def remove_colleague_feature(
    coll_feat_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Remove feature from colleague."""
    service = ColleagueCurationService(db)

    try:
        service.remove_colleague_feature(coll_feat_no)

        return SuccessResponse(
            success=True,
            message="Feature removed from colleague",
        )
    except ColleagueCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{colleague_no}/relationship", response_model=AddItemResponse)
def add_colleague_relationship(
    colleague_no: int,
    request: AddRelationshipRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Add relationship between colleagues."""
    service = ColleagueCurationService(db)

    try:
        rel_no = service.add_colleague_relationship(
            colleague_no,
            request.associate_no,
            request.relationship_type,
        )

        return AddItemResponse(
            id=rel_no,
            message="Relationship added",
        )
    except ColleagueCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/relationship/{coll_relationship_no}", response_model=SuccessResponse)
def remove_colleague_relationship(
    coll_relationship_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Remove relationship between colleagues."""
    service = ColleagueCurationService(db)

    try:
        service.remove_colleague_relationship(coll_relationship_no)

        return SuccessResponse(
            success=True,
            message="Relationship removed",
        )
    except ColleagueCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{colleague_no}/remark", response_model=AddItemResponse)
def add_colleague_remark(
    colleague_no: int,
    request: AddRemarkRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Add remark to colleague."""
    service = ColleagueCurationService(db)

    try:
        remark_no = service.add_colleague_remark(
            colleague_no,
            request.remark_type,
            request.remark_text,
            current_user.userid,
        )

        return AddItemResponse(
            id=remark_no,
            message="Remark added to colleague",
        )
    except ColleagueCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
