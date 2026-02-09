"""
GO Curation Router - Endpoints for GO annotation CRUD operations.

Requires curator authentication.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.go_curation_service import (
    GoCurationService,
    GoCurationError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/curation/go", tags=["curation-go"])


# ---------------------------
# Request/Response Schemas
# ---------------------------


class GoReferenceOut(BaseModel):
    """GO reference in annotation response."""

    go_ref_no: int
    reference_no: int
    pubmed: Optional[int]
    citation: Optional[str]
    has_qualifier: str
    has_supporting_evidence: str
    qualifiers: list[str]


class GoAnnotationOut(BaseModel):
    """GO annotation response."""

    go_annotation_no: int
    go_no: int
    goid: Optional[int]
    go_term: Optional[str]
    go_aspect: Optional[str]
    go_evidence: str
    annotation_type: str
    source: str
    date_last_reviewed: Optional[str]
    date_created: Optional[str]
    created_by: str
    references: list[GoReferenceOut]


class FeatureAnnotationsResponse(BaseModel):
    """Response for all GO annotations of a feature."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str]
    annotations: list[GoAnnotationOut]


class CreateAnnotationRequest(BaseModel):
    """Request to create a new GO annotation."""

    goid: int = Field(..., description="GO ID (without GO: prefix)")
    evidence: str = Field(..., description="Evidence code (e.g., IDA, IMP, IC)")
    reference_no: int = Field(..., description="Reference number")
    annotation_type: str = Field(
        default="manually curated",
        description="Annotation type",
    )
    source: str = Field(default="CGD", description="Source")
    qualifiers: Optional[list[str]] = Field(
        default=None,
        description="GO qualifiers (e.g., NOT, contributes_to)",
    )
    ic_from_goid: Optional[int] = Field(
        default=None,
        description="Required GO ID for IC evidence 'from' field",
    )


class CreateAnnotationResponse(BaseModel):
    """Response for annotation creation."""

    go_annotation_no: int
    message: str


class UpdateReviewRequest(BaseModel):
    """Request to update date_last_reviewed."""

    pass  # No fields needed, just confirmation


class UpdateReviewResponse(BaseModel):
    """Response for review update."""

    success: bool
    message: str


class DeleteAnnotationResponse(BaseModel):
    """Response for annotation deletion."""

    success: bool
    message: str


# ---------------------------
# Endpoints
# ---------------------------


@router.get("/{feature_name}", response_model=FeatureAnnotationsResponse)
def get_go_annotations(
    feature_name: str,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Get all GO annotations for a feature.

    Returns annotations grouped with their references, qualifiers, and evidence.
    """
    service = GoCurationService(db)

    feature = service.get_feature_by_name(feature_name)
    if not feature:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature '{feature_name}' not found",
        )

    annotations = service.get_annotations_for_feature(feature.feature_no)

    return FeatureAnnotationsResponse(
        feature_no=feature.feature_no,
        feature_name=feature.feature_name,
        gene_name=feature.gene_name,
        annotations=[GoAnnotationOut(**ann) for ann in annotations],
    )


@router.post("/{feature_name}", response_model=CreateAnnotationResponse)
def create_go_annotation(
    feature_name: str,
    request: CreateAnnotationRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Create a new GO annotation for a feature.

    Validates GO ID, evidence code, reference, and qualifiers according to
    GO annotation rules.
    """
    service = GoCurationService(db)

    feature = service.get_feature_by_name(feature_name)
    if not feature:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature '{feature_name}' not found",
        )

    try:
        annotation_no = service.create_annotation(
            feature_no=feature.feature_no,
            goid=request.goid,
            evidence=request.evidence,
            reference_no=request.reference_no,
            curator_userid=current_user.userid,
            annotation_type=request.annotation_type,
            source=request.source,
            qualifiers=request.qualifiers,
            ic_from_goid=request.ic_from_goid,
        )

        return CreateAnnotationResponse(
            go_annotation_no=annotation_no,
            message=f"GO annotation created successfully",
        )

    except GoCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{annotation_no}/review", response_model=UpdateReviewResponse)
def update_go_annotation_review(
    annotation_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Update date_last_reviewed for an annotation.

    Called when curator confirms they have reviewed the annotation.
    """
    service = GoCurationService(db)

    try:
        service.update_date_last_reviewed(annotation_no, current_user.userid)

        return UpdateReviewResponse(
            success=True,
            message=f"Annotation {annotation_no} marked as reviewed",
        )

    except GoCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/{annotation_no}", response_model=DeleteAnnotationResponse)
def delete_go_annotation(
    annotation_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Delete a GO annotation.

    Removes the annotation and all associated references/qualifiers.
    """
    service = GoCurationService(db)

    try:
        service.delete_annotation(annotation_no, current_user.userid)

        return DeleteAnnotationResponse(
            success=True,
            message=f"Annotation {annotation_no} deleted",
        )

    except GoCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/ref/{go_ref_no}", response_model=DeleteAnnotationResponse)
def delete_go_reference(
    go_ref_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Remove a reference from a GO annotation.

    Cannot remove the only reference; delete the annotation instead.
    """
    service = GoCurationService(db)

    try:
        service.delete_reference_from_annotation(go_ref_no, current_user.userid)

        return DeleteAnnotationResponse(
            success=True,
            message=f"Reference removed from annotation",
        )

    except GoCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get("/evidence-codes", tags=["curation-go"])
def get_evidence_codes(current_user: CurrentUser):
    """Get list of valid GO evidence codes."""
    return {"evidence_codes": GoCurationService.EVIDENCE_CODES}


@router.get("/qualifiers/{aspect}", tags=["curation-go"])
def get_qualifiers_for_aspect(
    aspect: str,
    current_user: CurrentUser,
):
    """
    Get valid GO qualifiers for a given aspect.

    Args:
        aspect: GO aspect (F/P/C or function/process/component)
    """
    aspect_key = aspect[0].upper() if len(aspect) > 1 else aspect.upper()
    qualifiers = GoCurationService.QUALIFIERS.get(aspect_key, [])

    if not qualifiers:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid aspect: {aspect}. Use F/P/C or function/process/component",
        )

    return {"aspect": aspect_key, "qualifiers": qualifiers}
