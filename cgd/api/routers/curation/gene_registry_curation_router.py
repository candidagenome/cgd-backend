"""
Gene Registry Curation Router - Endpoints for processing gene registry submissions.

Requires curator authentication.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.gene_registry_curation_service import (
    GeneRegistryCurationService,
    GeneRegistryCurationError,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/curation/gene-registry",
    tags=["curation-gene-registry"],
)


# ---------------------------
# Request/Response Schemas
# ---------------------------


class SubmissionSummary(BaseModel):
    """Summary of a pending submission."""

    id: str
    filename: str
    gene_name: Optional[str] = None
    orf_name: Optional[str] = None
    organism: Optional[str] = None
    submitted_at: Optional[str] = None
    colleague_no: Optional[int] = None
    submitter_name: Optional[str] = None


class PendingSubmissionsResponse(BaseModel):
    """Response for list of pending submissions."""

    submissions: list[SubmissionSummary]


class OrfInfo(BaseModel):
    """ORF information from database."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str] = None
    feature_type: Optional[str] = None
    headline: Optional[str] = None
    name_description: Optional[str] = None


class ColleagueInfo(BaseModel):
    """Colleague information from database."""

    colleague_no: int
    name: str
    email: Optional[str] = None
    institution: Optional[str] = None


class SubmissionDetails(BaseModel):
    """Full details of a submission."""

    id: str
    filename: str
    submitted_at: Optional[str] = None
    gene_name: Optional[str] = None
    orf_name: Optional[str] = None
    organism: Optional[str] = None
    colleague_no: Optional[int] = None
    data: dict = {}
    orf_info: Optional[OrfInfo] = None
    colleague_info: Optional[ColleagueInfo] = None


class SubmissionDetailsResponse(BaseModel):
    """Response for submission details."""

    found: bool
    submission: Optional[SubmissionDetails] = None


class ProcessSubmissionRequest(BaseModel):
    """Request to process a gene registry submission."""

    submission_id: str
    gene_name: str = Field(..., description="Gene name to register")
    orf_name: Optional[str] = Field(
        None, description="ORF name (defaults to gene name uppercased)"
    )
    organism_abbrev: str = Field(..., description="Organism abbreviation")
    description: Optional[str] = Field(None, description="Gene description")
    headline: Optional[str] = Field(None, description="Gene headline")
    aliases: Optional[list[str]] = Field(None, description="Gene aliases")
    reference_no: Optional[int] = Field(
        None, description="Reference number to link"
    )


class ProcessSubmissionResponse(BaseModel):
    """Response for processing a submission."""

    success: bool
    feature_no: Optional[int] = None
    feature_name: Optional[str] = None
    gene_name: Optional[str] = None
    gene_reservation_no: Optional[int] = None
    message: str


class DelaySubmissionRequest(BaseModel):
    """Request to delay a submission."""

    submission_id: str
    comment: Optional[str] = None


class ActionResponse(BaseModel):
    """Response for delay/delete actions."""

    success: bool
    message: str


# ---------------------------
# Endpoints
# ---------------------------


@router.get("/pending", response_model=PendingSubmissionsResponse)
def list_pending_submissions(
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    List all pending gene registry submissions.

    Returns submissions that have not yet been processed.
    """
    service = GeneRegistryCurationService(db)

    submissions = service.list_pending_submissions()

    return PendingSubmissionsResponse(
        submissions=[SubmissionSummary(**s) for s in submissions]
    )


@router.get("/{submission_id}", response_model=SubmissionDetailsResponse)
def get_submission_details(
    submission_id: str,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Get full details of a submission.

    Includes database lookups for ORF and colleague info.
    """
    service = GeneRegistryCurationService(db)

    details = service.get_submission_details(submission_id)

    if details:
        return SubmissionDetailsResponse(
            found=True,
            submission=SubmissionDetails(**details),
        )

    return SubmissionDetailsResponse(found=False)


@router.post("/process", response_model=ProcessSubmissionResponse)
def process_submission(
    request: ProcessSubmissionRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Process (commit) a gene registry submission.

    Creates/updates Feature, GeneReservation, and links to colleague.
    """
    service = GeneRegistryCurationService(db)

    try:
        result = service.process_submission(
            submission_id=request.submission_id,
            curator_userid=current_user.userid,
            gene_name=request.gene_name,
            orf_name=request.orf_name,
            organism_abbrev=request.organism_abbrev,
            description=request.description,
            headline=request.headline,
            aliases=request.aliases,
            reference_no=request.reference_no,
        )

        return ProcessSubmissionResponse(
            success=True,
            feature_no=result["feature_no"],
            feature_name=result["feature_name"],
            gene_name=result["gene_name"],
            gene_reservation_no=result["gene_reservation_no"],
            message=(
                f"Gene registry processed successfully. "
                f"Feature: {result['feature_name']}, "
                f"Gene name: {result['gene_name']}"
            ),
        )

    except GeneRegistryCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/delay", response_model=ActionResponse)
def delay_submission(
    request: DelaySubmissionRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Mark a submission as delayed.

    The submission remains in the pending queue but is marked for later processing.
    """
    service = GeneRegistryCurationService(db)

    success = service.delay_submission(
        submission_id=request.submission_id,
        comment=request.comment,
        curator_userid=current_user.userid,
    )

    if success:
        return ActionResponse(
            success=True,
            message=f"Submission '{request.submission_id}' has been delayed.",
        )

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Submission '{request.submission_id}' not found",
    )


@router.delete("/{submission_id}", response_model=ActionResponse)
def delete_submission(
    submission_id: str,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Delete a submission.

    The submission is archived with a 'deleted' marker.
    """
    service = GeneRegistryCurationService(db)

    success = service.delete_submission(submission_id)

    if success:
        return ActionResponse(
            success=True,
            message=f"Submission '{submission_id}' has been deleted.",
        )

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Submission '{submission_id}' not found",
    )
