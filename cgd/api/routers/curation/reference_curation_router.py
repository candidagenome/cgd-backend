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


class CreateManualReferenceRequest(BaseModel):
    """Request to create reference manually (without PubMed)."""

    title: str = Field(..., description="Reference title")
    year: int = Field(..., description="Publication year")
    status: str = Field(
        default="Published",
        description="Reference status (Published, Epub ahead of print, etc.)",
    )
    authors: Optional[list[str]] = Field(
        None,
        description="List of author names (e.g., ['Smith J', 'Doe JA'])",
    )
    journal_abbrev: Optional[str] = Field(
        None,
        description="Journal abbreviation",
    )
    volume: Optional[str] = Field(None, description="Volume number")
    pages: Optional[str] = Field(None, description="Page range")
    abstract: Optional[str] = Field(None, description="Abstract text")
    publication_types: Optional[list[str]] = Field(
        None,
        description="Publication types (e.g., ['Journal Article'])",
    )


class CreateReferenceResponse(BaseModel):
    """Response for reference creation."""

    reference_no: int
    pubmed: Optional[int] = None
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


class ReferenceSearchRequest(BaseModel):
    """Request to search for references."""

    pubmed: Optional[int] = Field(None, description="PubMed ID")
    reference_no: Optional[int] = Field(None, description="Reference number")
    dbxref_id: Optional[str] = Field(None, description="CGDID (dbxref_id)")
    volume: Optional[str] = Field(None, description="Journal volume")
    page: Optional[str] = Field(None, description="Page number/range")
    author: Optional[str] = Field(None, description="Author name (partial match)")
    keyword: Optional[str] = Field(None, description="Keyword in title/abstract")
    min_year: Optional[int] = Field(None, description="Minimum publication year")
    max_year: Optional[int] = Field(None, description="Maximum publication year")
    limit: int = Field(default=100, description="Maximum results")


class ReferenceSearchResult(BaseModel):
    """Single reference in search results."""

    reference_no: int
    pubmed: Optional[int]
    dbxref_id: Optional[str]
    citation: Optional[str]
    title: Optional[str]
    year: Optional[int]
    volume: Optional[str]
    page: Optional[str]
    status: str
    source: str


class ReferenceSearchResponse(BaseModel):
    """Response for reference search."""

    results: list[ReferenceSearchResult]
    count: int


class ReferenceUsageResponse(BaseModel):
    """Response for checking reference usage."""

    reference_no: int
    in_use: bool
    go_ref_count: int
    ref_link_count: int
    refprop_feat_count: int


class DeleteWithCleanupRequest(BaseModel):
    """Request to delete reference with cleanup."""

    delete_log_comment: Optional[str] = Field(
        None, description="Comment for delete log"
    )
    make_secondary_for: Optional[int] = Field(
        None, description="Make CGDID secondary for this reference_no"
    )


class DeleteWithCleanupResponse(BaseModel):
    """Response for delete with cleanup."""

    success: bool
    messages: list[str]
    dbxref_id: Optional[str]


class YearRangeResponse(BaseModel):
    """Response for year range query."""

    min_year: int
    max_year: int


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


# Non-parameterized routes MUST come before /{reference_no} routes
# to prevent "statuses" from matching as a reference_no


@router.get("/year-range", response_model=YearRangeResponse)
def get_year_range(
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Get min and max publication years in database."""
    service = ReferenceCurationService(db)
    min_year, max_year = service.get_year_range()
    return YearRangeResponse(min_year=min_year, max_year=max_year)


@router.get("/statuses/reference")
def get_reference_statuses(current_user: CurrentUser):
    """Get list of valid reference status values."""
    return {"statuses": ReferenceCurationService.VALID_STATUSES}


@router.get("/statuses/curation")
def get_curation_statuses(current_user: CurrentUser):
    """Get list of valid curation status values."""
    return {"statuses": ReferenceCurationService.CURATION_STATUSES}


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


@router.post("/manual", response_model=CreateReferenceResponse)
def create_manual_reference(
    request: CreateManualReferenceRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Create a new reference manually (without PubMed ID).

    Use this for references that are not in PubMed, such as
    unpublished work, theses, or non-indexed publications.
    """
    service = ReferenceCurationService(db)

    try:
        reference_no = service.create_manual_reference(
            title=request.title,
            year=request.year,
            reference_status=request.status,
            curator_userid=current_user.userid,
            authors=request.authors,
            journal_abbrev=request.journal_abbrev,
            volume=request.volume,
            pages=request.pages,
            abstract=request.abstract,
            publication_types=request.publication_types,
        )

        return CreateReferenceResponse(
            reference_no=reference_no,
            pubmed=None,
            message=f"Reference {reference_no} created successfully",
        )

    except ReferenceCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/search", response_model=ReferenceSearchResponse)
def search_references(
    request: ReferenceSearchRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Search for references by various criteria.

    At least one search criterion must be provided.
    """
    service = ReferenceCurationService(db)

    # Validate that at least one search criterion is provided
    if not any([
        request.pubmed,
        request.reference_no,
        request.dbxref_id,
        request.volume and request.page,
        request.author,
        request.keyword,
    ]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one search criterion must be provided",
        )

    results = service.search_references(
        pubmed=request.pubmed,
        reference_no=request.reference_no,
        dbxref_id=request.dbxref_id,
        volume=request.volume,
        page=request.page,
        author=request.author,
        keyword=request.keyword,
        min_year=request.min_year,
        max_year=request.max_year,
        limit=request.limit,
    )

    return ReferenceSearchResponse(
        results=[ReferenceSearchResult(**r) for r in results],
        count=len(results),
    )


@router.get("/{reference_no}/usage", response_model=ReferenceUsageResponse)
def get_reference_usage(
    reference_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Check if a reference has linked data.

    Returns counts for each type of linked data.
    """
    service = ReferenceCurationService(db)

    ref = service.get_reference_by_no(reference_no)
    if not ref:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Reference {reference_no} not found",
        )

    usage = service.is_reference_in_use(reference_no)

    return ReferenceUsageResponse(
        reference_no=reference_no,
        **usage,
    )


@router.delete("/{reference_no}/full", response_model=DeleteWithCleanupResponse)
def delete_reference_with_cleanup(
    reference_no: int,
    request: DeleteWithCleanupRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Delete a reference with full cleanup.

    - Adds pubmed to REF_BAD
    - Cleans up REF_UNLINK
    - Handles CGDID (make secondary or mark deleted)
    - Logs deletion

    Note: Will fail if reference has linked annotations.
    """
    service = ReferenceCurationService(db)

    try:
        result = service.delete_reference_with_cleanup(
            reference_no=reference_no,
            curator_userid=current_user.userid,
            delete_log_comment=request.delete_log_comment,
            make_secondary_for=request.make_secondary_for,
        )

        return DeleteWithCleanupResponse(**result)

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
