"""
Literature Guide Curation Router - Endpoints for feature-centric literature curation.

Requires curator authentication.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.litguide_curation_service import (
    LitGuideCurationService,
    LitGuideCurationError,
    LITERATURE_TOPICS,
    CURATION_STATUSES,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/curation/litguide", tags=["curation-litguide"])


# ---------------------------
# Request/Response Schemas
# ---------------------------


class TopicOut(BaseModel):
    """Topic association in literature guide."""

    topic: str
    ref_property_no: int
    refprop_feat_no: int


class ReferenceOut(BaseModel):
    """Reference in literature guide."""

    reference_no: int
    pubmed: Optional[int]
    citation: Optional[str]
    title: Optional[str]
    year: Optional[int]


class CuratedReferenceOut(ReferenceOut):
    """Curated reference with topics."""

    topics: list[TopicOut]


class FeatureLiteratureResponse(BaseModel):
    """Response for feature literature."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str]
    curated: list[CuratedReferenceOut]
    uncurated: list[ReferenceOut]


class AddTopicRequest(BaseModel):
    """Request to add topic association."""

    reference_no: int = Field(..., description="Reference number")
    topic: str = Field(..., description="Literature topic")


class AddTopicResponse(BaseModel):
    """Response for adding topic association."""

    refprop_feat_no: int
    message: str


class SuccessResponse(BaseModel):
    """Generic success response."""

    success: bool
    message: str


class SetStatusRequest(BaseModel):
    """Request to set curation status."""

    curation_status: str = Field(..., description="Curation status")


class ReferenceSearchItem(BaseModel):
    """Reference item in search results."""

    reference_no: int
    pubmed: Optional[int]
    citation: Optional[str]
    title: Optional[str]
    year: Optional[int]
    curation_status: Optional[str]


class ReferenceSearchResponse(BaseModel):
    """Response for reference search."""

    references: list[ReferenceSearchItem]
    total: int
    page: int
    page_size: int


class TopicsResponse(BaseModel):
    """Response for available topics."""

    topics: list[str]


class StatusesResponse(BaseModel):
    """Response for available curation statuses."""

    statuses: list[str]


# ---------------------------
# Endpoints
# ---------------------------


@router.get("/topics", response_model=TopicsResponse)
def get_literature_topics(current_user: CurrentUser):
    """Get available literature topics."""
    return TopicsResponse(topics=LITERATURE_TOPICS)


@router.get("/statuses", response_model=StatusesResponse)
def get_curation_statuses(current_user: CurrentUser):
    """Get available curation statuses."""
    return StatusesResponse(statuses=CURATION_STATUSES)


@router.get("/feature/{identifier}", response_model=FeatureLiteratureResponse)
def get_feature_literature(
    identifier: str,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Get all literature for a feature.

    identifier can be feature_no (int) or feature_name/gene_name (str).
    Returns curated (with topics) and uncurated references.
    """
    service = LitGuideCurationService(db)

    try:
        # Try as integer first
        feature_no = int(identifier)
        feature = service.get_feature_by_no(feature_no)
    except ValueError:
        # Treat as name
        feature = service.get_feature_by_name(identifier)

    if not feature:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature '{identifier}' not found",
        )

    try:
        return service.get_feature_literature(feature.feature_no)
    except LitGuideCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/feature/{feature_no}/topic", response_model=AddTopicResponse)
def add_topic_association(
    feature_no: int,
    request: AddTopicRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Add a topic association between a feature and reference."""
    service = LitGuideCurationService(db)

    try:
        refprop_feat_no = service.add_topic_association(
            feature_no,
            request.reference_no,
            request.topic,
            current_user.userid,
        )

        return AddTopicResponse(
            refprop_feat_no=refprop_feat_no,
            message=f"Topic '{request.topic}' added to feature-reference association",
        )
    except LitGuideCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/topic/{refprop_feat_no}", response_model=SuccessResponse)
def remove_topic_association(
    refprop_feat_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Remove a topic association."""
    service = LitGuideCurationService(db)

    try:
        service.remove_topic_association(refprop_feat_no, current_user.userid)

        return SuccessResponse(
            success=True,
            message="Topic association removed",
        )
    except LitGuideCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.put("/reference/{reference_no}/status", response_model=SuccessResponse)
def set_reference_status(
    reference_no: int,
    request: SetStatusRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Set or update curation status for a reference."""
    service = LitGuideCurationService(db)

    try:
        service.set_reference_curation_status(
            reference_no,
            request.curation_status,
            current_user.userid,
        )

        return SuccessResponse(
            success=True,
            message=f"Curation status set to '{request.curation_status}'",
        )
    except LitGuideCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get("/reference/search", response_model=ReferenceSearchResponse)
def search_references(
    current_user: CurrentUser,
    query: str = Query(..., min_length=1, description="Search query (pubmed, title, or citation)"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Results per page"),
    db: Session = Depends(get_db),
):
    """Search references by pubmed, title, or citation."""
    service = LitGuideCurationService(db)

    references, total = service.search_references(query, page, page_size)

    return ReferenceSearchResponse(
        references=[ReferenceSearchItem(**r) for r in references],
        total=total,
        page=page,
        page_size=page_size,
    )


# ---------------------------
# Reference-centric Endpoints
# ---------------------------


class FeatureTopicOut(BaseModel):
    """Feature with topics in reference literature."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str]
    feature_type: Optional[str]
    topics: list[TopicOut]


class ReferenceLiteratureResponse(BaseModel):
    """Response for reference literature."""

    reference_no: int
    pubmed: Optional[int]
    citation: Optional[str]
    title: Optional[str]
    year: Optional[int]
    abstract: Optional[str]
    curation_status: Optional[str]
    features: list[FeatureTopicOut]


class AddFeatureRequest(BaseModel):
    """Request to add feature-topic association to reference."""

    feature_identifier: str = Field(..., description="Feature name, gene name, or feature_no")
    topic: str = Field(..., description="Literature topic")


class AddFeatureResponse(BaseModel):
    """Response for adding feature to reference."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str]
    refprop_feat_no: int
    message: str


@router.get("/reference/{reference_no}", response_model=ReferenceLiteratureResponse)
def get_reference_literature(
    reference_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Get reference details with all associated features and topics.

    Used for reference-centric literature guide curation (from todo list "Lit Guide" link).
    """
    service = LitGuideCurationService(db)

    try:
        return service.get_reference_literature(reference_no)
    except LitGuideCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )


@router.post("/reference/{reference_no}/feature", response_model=AddFeatureResponse)
def add_feature_to_reference(
    reference_no: int,
    request: AddFeatureRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Add a feature-topic association to a reference."""
    service = LitGuideCurationService(db)

    try:
        result = service.add_feature_to_reference(
            reference_no,
            request.feature_identifier,
            request.topic,
            current_user.userid,
        )

        return AddFeatureResponse(
            feature_no=result["feature_no"],
            feature_name=result["feature_name"],
            gene_name=result["gene_name"],
            refprop_feat_no=result["refprop_feat_no"],
            message=f"Feature '{result['feature_name']}' added with topic '{request.topic}'",
        )
    except LitGuideCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
