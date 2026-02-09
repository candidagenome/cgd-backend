"""
Reference Curation Router - Endpoints for reference management.

Requires curator authentication.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.reference_curation_service import (
    ReferenceCurationService,
    ReferenceCurationError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/curation/reference", tags=["curation-reference"])


# ---------------------------
# Request/Response Schemas
# ---------------------------


class CreateFromPubmedRequest(BaseModel):
    """Request to create reference from PubMed ID."""

    pubmed: int = Field(..., description="PubMed ID")
    status: str = Field(
        default="Published",
        description="Reference status (Published, Epub ahead of print, etc.)",
    )
    override_bad: bool = Field(
        default=False,
        description="Override if PubMed ID is in bad reference list",
    )


class CreateReferenceResponse(BaseModel):
    """Response for reference creation."""

    reference_no: int
    pubmed: int
    message: str


class UpdateReferenceRequest(BaseModel):
    """Request to update reference metadata."""

    title: Optional[str] = None
    status: Optional[str] = None
    year: Optional[int] = None
    volume: Optional[str] = None
    pages: Optional[str] = None


class UpdateReferenceResponse(BaseModel):
    """Response for reference update."""

    success: bool
    message: str


class DeleteReferenceResponse(BaseModel):
    """Response for reference deletion."""

    success: bool
    message: str


class SetCurationStatusRequest(BaseModel):
    """Request to set curation status."""

    curation_status: str = Field(
        ...,
        description="Curation status (Not Yet Curated, High Priority, etc.)",
    )


class SetCurationStatusResponse(BaseModel):
    """Response for setting curation status."""

    ref_property_no: int
    message: str


class LinkToLitGuideRequest(BaseModel):
    """Request to link reference to literature guide."""

    feature_names: list[str] = Field(
        ...,
        description="Feature names to link",
        min_length=1,
    )
    topic: str = Field(..., description="Literature topic")


class LinkToLitGuideResponse(BaseModel):
    """Response for literature guide linking."""

    linked_count: int
    refprop_feat_nos: list[int]
    message: str


class TopicOut(BaseModel):
    """Topic with linked features."""

    topic: str
    features: list[dict]


class ReferenceCurationDetailsResponse(BaseModel):
    """Full curation details for a reference."""

    reference_no: int
    pubmed: Optional[int]
    title: Optional[str]
    citation: str
    year: int
    status: str
    source: str
    curation_status: Optional[str]
    topics: list[TopicOut]
    abstract: Optional[str]
    authors: list[dict]


# ---------------------------
# Endpoints
# ---------------------------


@router.post("/pubmed", response_model=CreateReferenceResponse)
def create_reference_from_pubmed(
    request: CreateFromPubmedRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Create a new reference from a PubMed ID.

    Fetches metadata from NCBI and creates the reference record.
    """
    service = ReferenceCurationService(db)

    try:
        reference_no = service.create_reference_from_pubmed(
            pubmed=request.pubmed,
            reference_status=request.status,
            curator_userid=current_user.userid,
            override_bad=request.override_bad,
        )

        return CreateReferenceResponse(
            reference_no=reference_no,
            pubmed=request.pubmed,
            message=f"Reference created successfully from PubMed {request.pubmed}",
        )

    except ReferenceCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get("/{reference_no}", response_model=ReferenceCurationDetailsResponse)
def get_reference_curation_details(
    reference_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Get full curation details for a reference.

    Includes curation status, topics, linked features, abstract, and authors.
    """
    service = ReferenceCurationService(db)

    try:
        details = service.get_reference_curation_details(reference_no)
        return ReferenceCurationDetailsResponse(**details)

    except ReferenceCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )


@router.put("/{reference_no}", response_model=UpdateReferenceResponse)
def update_reference(
    reference_no: int,
    request: UpdateReferenceRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Update reference metadata.
    """
    service = ReferenceCurationService(db)

    try:
        service.update_reference(
            reference_no=reference_no,
            curator_userid=current_user.userid,
            title=request.title,
            status=request.status,
            year=request.year,
            volume=request.volume,
            pages=request.pages,
        )

        return UpdateReferenceResponse(
            success=True,
            message=f"Reference {reference_no} updated",
        )

    except ReferenceCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/{reference_no}", response_model=DeleteReferenceResponse)
def delete_reference(
    reference_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Delete a reference.

    Warning: This will fail if the reference has linked annotations.
    """
    service = ReferenceCurationService(db)

    try:
        service.delete_reference(reference_no, current_user.userid)

        return DeleteReferenceResponse(
            success=True,
            message=f"Reference {reference_no} deleted",
        )

    except ReferenceCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{reference_no}/status", response_model=SetCurationStatusResponse)
def set_curation_status(
    reference_no: int,
    request: SetCurationStatusRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Set or update the curation status for a reference.
    """
    service = ReferenceCurationService(db)

    try:
        ref_property_no = service.set_curation_status(
            reference_no=reference_no,
            curation_status=request.curation_status,
            curator_userid=current_user.userid,
        )

        return SetCurationStatusResponse(
            ref_property_no=ref_property_no,
            message=f"Curation status set to '{request.curation_status}'",
        )

    except ReferenceCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{reference_no}/litguide", response_model=LinkToLitGuideResponse)
def link_to_literature_guide(
    reference_no: int,
    request: LinkToLitGuideRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Link a reference to features via literature guide.

    Creates associations between the reference, topic, and features.
    """
    service = ReferenceCurationService(db)

    try:
        refprop_feat_nos = service.link_to_literature_guide(
            reference_no=reference_no,
            feature_names=request.feature_names,
            topic=request.topic,
            curator_userid=current_user.userid,
        )

        return LinkToLitGuideResponse(
            linked_count=len(refprop_feat_nos),
            refprop_feat_nos=refprop_feat_nos,
            message=f"Linked {len(refprop_feat_nos)} features to reference {reference_no}",
        )

    except ReferenceCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get("/statuses/reference")
def get_reference_statuses(current_user: CurrentUser):
    """Get list of valid reference status values."""
    return {"statuses": ReferenceCurationService.VALID_STATUSES}


@router.get("/statuses/curation")
def get_curation_statuses(current_user: CurrentUser):
    """Get list of valid curation status values."""
    return {"statuses": ReferenceCurationService.CURATION_STATUSES}
