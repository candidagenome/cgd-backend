"""
Locus Curation Router - Endpoints for locus/feature info updates.

Requires curator authentication.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.locus_curation_service import (
    LocusCurationService,
    LocusCurationError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/curation/locus", tags=["curation-locus"])


# ---------------------------
# Request/Response Schemas
# ---------------------------


class FeatureSearchItem(BaseModel):
    """Feature summary for search results."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str]
    feature_type: str
    headline: Optional[str]


class FeatureSearchResponse(BaseModel):
    """Response for feature search."""

    features: List[FeatureSearchItem]
    total: int
    page: int
    page_size: int


class AliasRefOut(BaseModel):
    """Reference for alias."""

    reference_no: int
    pubmed: Optional[int]


class AliasOut(BaseModel):
    """Alias in feature details."""

    feat_alias_no: int
    alias_no: int
    alias_name: str
    alias_type: str
    references: List[AliasRefOut]


class NoteOut(BaseModel):
    """Note in feature details."""

    note_link_no: int
    note_no: int
    note_type: str
    note_text: str
    date_created: Optional[str]


class UrlOut(BaseModel):
    """URL in feature details."""

    feat_url_no: int
    url_no: int
    url_type: str
    link: str


class FeatureDetailResponse(BaseModel):
    """Full feature details for curation."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str]
    name_description: Optional[str]
    feature_type: str
    headline: Optional[str]
    source: str
    date_created: Optional[str]
    created_by: str
    aliases: List[AliasOut]
    notes: List[NoteOut]
    urls: List[UrlOut]


class UpdateFeatureRequest(BaseModel):
    """Request to update feature."""

    gene_name: Optional[str] = Field(None, description="Standard gene name")
    name_description: Optional[str] = Field(None, description="Name description")
    headline: Optional[str] = Field(None, description="Headline/short description")
    feature_type: Optional[str] = Field(None, description="Feature type")


class SuccessResponse(BaseModel):
    """Generic success response."""

    success: bool
    message: str


class AddAliasRequest(BaseModel):
    """Request to add alias to feature."""

    alias_name: str = Field(..., description="Alias name")
    alias_type: str = Field(default="Uniform", description="Alias type")
    reference_no: Optional[int] = Field(None, description="Reference number")


class AddNoteRequest(BaseModel):
    """Request to add note to feature."""

    note_type: str = Field(..., description="Note type")
    note_text: str = Field(..., description="Note text")


class AddUrlRequest(BaseModel):
    """Request to add URL to feature."""

    url_type: str = Field(..., description="URL type")
    link: str = Field(..., description="URL")


class AddItemResponse(BaseModel):
    """Response for adding an item."""

    id: int
    message: str


# ---------------------------
# Endpoints
# ---------------------------


@router.get("/search", response_model=FeatureSearchResponse)
def search_features(
    current_user: CurrentUser,
    query: str = Query(..., min_length=1, description="Search query"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Results per page"),
    db: Session = Depends(get_db),
):
    """
    Search features by name.

    Searches both feature_name and gene_name.
    """
    service = LocusCurationService(db)

    features, total = service.search_features(query, page, page_size)

    return FeatureSearchResponse(
        features=[FeatureSearchItem(**f) for f in features],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{identifier}", response_model=FeatureDetailResponse)
def get_feature_details(
    identifier: str,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Get full feature details for curation.

    identifier can be feature_no (int) or feature_name/gene_name (str).
    """
    service = LocusCurationService(db)

    try:
        # Try as integer first
        feature_no = int(identifier)
        details = service.get_feature_details(feature_no)
    except ValueError:
        # Treat as name
        feature = service.get_feature_by_name(identifier)
        if not feature:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Feature '{identifier}' not found",
            )
        details = service.get_feature_details(feature.feature_no)
    except LocusCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )

    return FeatureDetailResponse(**details)


@router.put("/{feature_no}", response_model=SuccessResponse)
def update_feature(
    feature_no: int,
    request: UpdateFeatureRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Update feature fields.
    """
    service = LocusCurationService(db)

    try:
        service.update_feature(
            feature_no=feature_no,
            curator_userid=current_user.userid,
            **request.model_dump(exclude_unset=True),
        )

        return SuccessResponse(
            success=True,
            message=f"Feature {feature_no} updated successfully",
        )
    except LocusCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{feature_no}/alias", response_model=AddItemResponse)
def add_alias(
    feature_no: int,
    request: AddAliasRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Add alias to feature."""
    service = LocusCurationService(db)

    try:
        feat_alias_no = service.add_alias(
            feature_no,
            request.alias_name,
            request.alias_type,
            current_user.userid,
            request.reference_no,
        )

        return AddItemResponse(
            id=feat_alias_no,
            message="Alias added to feature",
        )
    except LocusCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/alias/{feat_alias_no}", response_model=SuccessResponse)
def remove_alias(
    feat_alias_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Remove alias from feature."""
    service = LocusCurationService(db)

    try:
        service.remove_alias(feat_alias_no, current_user.userid)

        return SuccessResponse(
            success=True,
            message="Alias removed",
        )
    except LocusCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{feature_no}/note", response_model=AddItemResponse)
def add_note(
    feature_no: int,
    request: AddNoteRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Add note to feature."""
    service = LocusCurationService(db)

    try:
        note_link_no = service.add_note(
            feature_no,
            request.note_type,
            request.note_text,
            current_user.userid,
        )

        return AddItemResponse(
            id=note_link_no,
            message="Note added to feature",
        )
    except LocusCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/note/{note_link_no}", response_model=SuccessResponse)
def remove_note(
    note_link_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Remove note from feature."""
    service = LocusCurationService(db)

    try:
        service.remove_note(note_link_no, current_user.userid)

        return SuccessResponse(
            success=True,
            message="Note removed",
        )
    except LocusCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{feature_no}/url", response_model=AddItemResponse)
def add_url(
    feature_no: int,
    request: AddUrlRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Add URL to feature."""
    service = LocusCurationService(db)

    try:
        feat_url_no = service.add_url(
            feature_no,
            request.url_type,
            request.link,
            current_user.userid,
        )

        return AddItemResponse(
            id=feat_url_no,
            message="URL added to feature",
        )
    except LocusCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/url/{feat_url_no}", response_model=SuccessResponse)
def remove_url(
    feat_url_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Remove URL from feature."""
    service = LocusCurationService(db)

    try:
        service.remove_url(feat_url_no, current_user.userid)

        return SuccessResponse(
            success=True,
            message="URL removed",
        )
    except LocusCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
