"""
Literature Review Curation Router - API endpoints for paper triage.

Provides endpoints for curators to review and triage papers from the
PubMed literature review queue (REF_TEMP table).
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth import get_current_user
from cgd.db.deps import get_db
from cgd.api.services.curation.litreview_curation_service import (
    LitReviewCurationService,
    LitReviewError,
)

router = APIRouter(
    prefix="/api/curation/litreview",
    tags=["Literature Review Curation"],
)


# Request/Response models
class TriageAddRequest(BaseModel):
    """Request to add a paper with 'Not yet curated' status."""

    pubmed: int = Field(..., description="PubMed ID")


class TriageHighPriorityRequest(BaseModel):
    """Request to add a paper with 'High Priority' status."""

    pubmed: int = Field(..., description="PubMed ID")
    feature_names: Optional[list[str]] = Field(
        None, description="Gene/feature names to link (pipe-separated in legacy)"
    )
    organism_abbrev: Optional[str] = Field(
        None, description="Organism abbreviation for validating features"
    )


class TriageDiscardRequest(BaseModel):
    """Request to discard a paper."""

    pubmed: int = Field(..., description="PubMed ID")


class TriageBatchAction(BaseModel):
    """Single action in a batch triage request."""

    pubmed: int = Field(..., description="PubMed ID")
    action: str = Field(
        ..., description="Action: 'add', 'high_priority', or 'discard'"
    )
    feature_names: Optional[list[str]] = Field(
        None, description="For high_priority: gene names to link"
    )
    organism_abbrev: Optional[str] = Field(
        None, description="For high_priority: organism abbreviation"
    )


class TriageBatchRequest(BaseModel):
    """Batch triage request."""

    actions: list[TriageBatchAction] = Field(..., description="List of triage actions")


class TriageResponse(BaseModel):
    """Response from a triage action."""

    success: bool
    reference_no: Optional[int] = None
    linked_features: Optional[list[str]] = None
    messages: list[str]


class BatchTriageResponse(BaseModel):
    """Response from batch triage."""

    results: list[dict]
    total_processed: int
    successful: int


class PaperListResponse(BaseModel):
    """Response containing list of papers to review."""

    papers: list[dict]
    total: int
    limit: int
    offset: int


class OrganismListResponse(BaseModel):
    """Response containing list of organisms."""

    organisms: list[dict]


# Endpoints
@router.get("/papers", response_model=PaperListResponse)
async def get_pending_papers(
    limit: int = 200,
    offset: int = 0,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """
    Get papers pending review from REF_TEMP.

    Returns papers with citation and abstract for curator review.
    """
    service = LitReviewCurationService(db)
    return service.get_pending_papers(limit=limit, offset=offset)


@router.get("/papers/{pubmed}")
async def get_paper(
    pubmed: int,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Get a single paper from review queue by PubMed ID."""
    service = LitReviewCurationService(db)
    paper = service.get_paper_by_pubmed(pubmed)
    if not paper:
        raise HTTPException(status_code=404, detail=f"Paper {pubmed} not found in queue")
    return paper


@router.get("/organisms", response_model=OrganismListResponse)
async def get_organisms(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Get list of organisms for feature linking dropdown."""
    service = LitReviewCurationService(db)
    return {"organisms": service.get_organisms()}


@router.post("/triage/add", response_model=TriageResponse)
async def triage_add(
    request: TriageAddRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """
    Add a paper to database with 'Not yet curated' status.

    Paper is removed from review queue after processing.
    """
    service = LitReviewCurationService(db)
    try:
        result = service.triage_add(
            pubmed=request.pubmed,
            curator_userid=current_user["username"],
        )
        return TriageResponse(**result)
    except LitReviewError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/triage/high-priority", response_model=TriageResponse)
async def triage_high_priority(
    request: TriageHighPriorityRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """
    Add a paper with 'High Priority' status and optionally link to genes.

    Paper is removed from review queue after processing.
    """
    service = LitReviewCurationService(db)
    try:
        result = service.triage_high_priority(
            pubmed=request.pubmed,
            curator_userid=current_user["username"],
            feature_names=request.feature_names,
            organism_abbrev=request.organism_abbrev,
        )
        return TriageResponse(**result)
    except LitReviewError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/triage/discard", response_model=TriageResponse)
async def triage_discard(
    request: TriageDiscardRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """
    Discard a paper (add to REF_BAD).

    Paper is removed from review queue after processing.
    """
    service = LitReviewCurationService(db)
    try:
        result = service.triage_discard(
            pubmed=request.pubmed,
            curator_userid=current_user["username"],
        )
        return TriageResponse(**result)
    except LitReviewError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/triage/batch", response_model=BatchTriageResponse)
async def triage_batch(
    request: TriageBatchRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """
    Process multiple triage actions in one request.

    Useful for submitting all decisions from a page of papers at once.
    """
    service = LitReviewCurationService(db)

    # Convert to list of dicts for service
    actions = [
        {
            "pubmed": action.pubmed,
            "action": action.action,
            "feature_names": action.feature_names,
            "organism_abbrev": action.organism_abbrev,
        }
        for action in request.actions
    ]

    result = service.triage_batch(
        actions=actions,
        curator_userid=current_user["username"],
    )

    return BatchTriageResponse(**result)
