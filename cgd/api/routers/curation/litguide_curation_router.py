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
from cgd.models.models import Reference

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


class RefUrlOut(BaseModel):
    """URL associated with a reference."""

    url: str
    url_type: str


class ReferenceOut(BaseModel):
    """Reference in literature guide."""

    reference_no: int
    pubmed: Optional[int]
    citation: Optional[str]
    title: Optional[str]
    year: Optional[int]
    dbxref_id: Optional[str] = None
    urls: list[RefUrlOut] = []


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
    organism: Optional[str] = Query(None, description="Filter by organism abbreviation"),
    db: Session = Depends(get_db),
):
    """
    Get all literature for a feature.

    identifier can be feature_no (int) or feature_name/gene_name (str).
    Returns curated (with topics) and uncurated references.

    If organism is provided, only returns the feature from that organism.
    """
    service = LitGuideCurationService(db)

    try:
        # Try as integer first
        feature_no = int(identifier)
        feature = service.get_feature_by_no(feature_no)
    except ValueError:
        # Treat as name, with optional organism filter
        feature = service.get_feature_by_name(identifier, organism)

    if not feature:
        if organism:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Feature '{identifier}' not found in organism '{organism}'",
            )
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
    organism_abbrev: Optional[str] = None
    organism_name: Optional[str] = None
    topics: list[TopicOut]


class OrganismOut(BaseModel):
    """Organism info."""

    organism_abbrev: str
    organism_name: str
    common_name: Optional[str] = None


class OrganismFeaturesOut(BaseModel):
    """Features grouped by organism."""

    organism_abbrev: str
    organism_name: str
    features: list[FeatureTopicOut]


class ReferenceLiteratureResponse(BaseModel):
    """Response for reference literature."""

    reference_no: int
    pubmed: Optional[int]
    citation: Optional[str]
    title: Optional[str]
    year: Optional[int]
    dbxref_id: Optional[str] = None
    abstract: Optional[str] = None
    urls: list[RefUrlOut] = []
    curation_status: Optional[str]
    current_organism: Optional[OrganismOut] = None
    features: list[FeatureTopicOut]
    other_organisms: dict[str, OrganismFeaturesOut] = {}


class OrganismsResponse(BaseModel):
    """Response for available organisms."""

    organisms: list[OrganismOut]


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


class UnlinkFeatureRequest(BaseModel):
    """Request to unlink feature from reference."""

    feature_identifier: str = Field(..., description="Feature name, gene name, or feature_no")


class UnlinkFeatureResponse(BaseModel):
    """Response for unlinking feature from reference."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str]
    removed_topics: int
    message: str


class NoteOut(BaseModel):
    """Note in reference notes response."""

    feature_name: Optional[str]
    topic: str
    note: str
    note_type: str


class ReferenceNotesResponse(BaseModel):
    """Response for reference notes."""

    reference_no: int
    notes: list[NoteOut]


class NongeneTopicOut(BaseModel):
    """Non-gene topic in response."""

    topic: str
    ref_property_no: int


class NongeneTopicsResponse(BaseModel):
    """Response for non-gene topics."""

    reference_no: int
    public_topics: list[NongeneTopicOut]
    internal_topics: list[NongeneTopicOut]


class AddNongeneTopicRequest(BaseModel):
    """Request to add non-gene topic."""

    topic: str = Field(..., description="Literature topic")


class AddNongeneTopicResponse(BaseModel):
    """Response for adding non-gene topic."""

    ref_property_no: int
    message: str


@router.get("/organisms", response_model=OrganismsResponse)
def get_available_organisms(
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Get list of organisms that have features in the database."""
    service = LitGuideCurationService(db)
    organisms = service.get_available_organisms()

    return OrganismsResponse(
        organisms=[OrganismOut(**org) for org in organisms]
    )


@router.get("/reference/{reference_no}", response_model=ReferenceLiteratureResponse)
def get_reference_literature(
    reference_no: int,
    current_user: CurrentUser,
    organism: Optional[str] = Query(
        None,
        description="Organism abbreviation to filter features. "
        "If provided, features are grouped into current vs other organisms.",
    ),
    db: Session = Depends(get_db),
):
    """
    Get reference details with all associated features and topics.

    Used for reference-centric literature guide curation.

    If 'organism' parameter is provided:
    - 'features' contains only features from that organism (editable)
    - 'other_organisms' contains features from other organisms (read-only)
    - 'current_organism' contains info about the selected organism

    If 'organism' is not provided:
    - 'features' contains all features
    - 'other_organisms' is empty
    """
    service = LitGuideCurationService(db)

    try:
        result = service.get_reference_literature(reference_no, organism)

        # Convert other_organisms dict values to proper models
        other_organisms = {}
        for abbrev, org_data in result.get("other_organisms", {}).items():
            other_organisms[abbrev] = OrganismFeaturesOut(
                organism_abbrev=org_data["organism_abbrev"],
                organism_name=org_data["organism_name"],
                features=[FeatureTopicOut(**f) for f in org_data["features"]],
            )

        return ReferenceLiteratureResponse(
            reference_no=result["reference_no"],
            pubmed=result["pubmed"],
            citation=result["citation"],
            title=result["title"],
            year=result["year"],
            dbxref_id=result.get("dbxref_id"),
            abstract=result.get("abstract"),
            urls=[RefUrlOut(**u) for u in result.get("urls", [])],
            curation_status=result["curation_status"],
            current_organism=OrganismOut(**result["current_organism"]) if result.get("current_organism") else None,
            features=[FeatureTopicOut(**f) for f in result["features"]],
            other_organisms=other_organisms,
        )
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


@router.delete("/reference/{reference_no}/feature", response_model=UnlinkFeatureResponse)
def unlink_feature_from_reference(
    reference_no: int,
    request: UnlinkFeatureRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Unlink a feature from a reference.

    Removes the link between the feature and reference, as well as
    any topic associations for this feature-reference pair.
    """
    service = LitGuideCurationService(db)

    try:
        result = service.unlink_feature_from_reference(
            reference_no,
            request.feature_identifier,
            current_user.userid,
        )

        return UnlinkFeatureResponse(
            feature_no=result["feature_no"],
            feature_name=result["feature_name"],
            gene_name=result["gene_name"],
            removed_topics=result["removed_topics"],
            message=f"Feature '{result['feature_name']}' unlinked from reference {reference_no}",
        )
    except LitGuideCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get("/reference/{reference_no}/notes", response_model=ReferenceNotesResponse)
def get_reference_notes(
    reference_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Get all curation notes associated with a reference.

    Returns notes linked to features (via topics) and non-gene topic notes.
    """
    service = LitGuideCurationService(db)

    # Verify reference exists
    reference = (
        db.query(Reference)
        .filter(Reference.reference_no == reference_no)
        .first()
    )
    if not reference:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Reference {reference_no} not found",
        )

    notes = service.get_reference_notes(reference_no)

    return ReferenceNotesResponse(
        reference_no=reference_no,
        notes=[NoteOut(**n) for n in notes],
    )


@router.get("/reference/{reference_no}/nongene-topics", response_model=NongeneTopicsResponse)
def get_nongene_topics(
    reference_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Get topics linked to reference but NOT associated with any feature.

    Returns public topics (literature_topic) and internal topics (curation_status).
    """
    service = LitGuideCurationService(db)

    # Verify reference exists
    reference = (
        db.query(Reference)
        .filter(Reference.reference_no == reference_no)
        .first()
    )
    if not reference:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Reference {reference_no} not found",
        )

    result = service.get_nongene_topics(reference_no)

    return NongeneTopicsResponse(
        reference_no=reference_no,
        public_topics=[NongeneTopicOut(**t) for t in result["public_topics"]],
        internal_topics=[NongeneTopicOut(**t) for t in result["internal_topics"]],
    )


@router.post("/reference/{reference_no}/nongene-topic", response_model=AddNongeneTopicResponse)
def add_nongene_topic(
    reference_no: int,
    request: AddNongeneTopicRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Add a non-gene topic to a reference (topic not associated with any feature)."""
    service = LitGuideCurationService(db)

    try:
        ref_property_no = service.add_nongene_topic(
            reference_no,
            request.topic,
            current_user.userid,
        )

        return AddNongeneTopicResponse(
            ref_property_no=ref_property_no,
            message=f"Non-gene topic '{request.topic}' added to reference",
        )
    except LitGuideCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/reference/{reference_no}/nongene-topic/{ref_property_no}", response_model=SuccessResponse)
def remove_nongene_topic(
    reference_no: int,
    ref_property_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Remove a non-gene topic from a reference."""
    service = LitGuideCurationService(db)

    try:
        service.remove_nongene_topic(ref_property_no, current_user.userid)

        return SuccessResponse(
            success=True,
            message="Non-gene topic removed",
        )
    except LitGuideCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
