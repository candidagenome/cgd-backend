"""
Paragraph Curation Router - Endpoints for paragraph management.

Requires curator authentication.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.paragraph_curation_service import (
    ParagraphCurationService,
    ParagraphCurationError,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/curation/paragraph",
    tags=["curation-paragraph"],
)


# ---------------------------
# Request/Response Schemas
# ---------------------------


class LinkedFeature(BaseModel):
    """Feature linked to a paragraph."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str] = None
    paragraph_order: int


class LinkedReference(BaseModel):
    """Reference linked to a paragraph."""

    reference_no: int
    citation: str
    dbxref_id: Optional[str] = None


class ParagraphSummary(BaseModel):
    """Summary of a paragraph."""

    paragraph_no: int
    paragraph_text: str
    date_edited: Optional[str] = None
    paragraph_order: int
    linked_features: list[LinkedFeature]


class FeatureParagraphsResponse(BaseModel):
    """Response for paragraphs of a feature."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str] = None
    organism_abbrev: Optional[str] = None
    organism_name: Optional[str] = None
    paragraphs: list[ParagraphSummary]


class ParagraphDetailsResponse(BaseModel):
    """Full details for a paragraph."""

    paragraph_no: int
    paragraph_text: str
    date_edited: Optional[str] = None
    linked_features: list[LinkedFeature]
    linked_references: list[LinkedReference]


class CreateParagraphRequest(BaseModel):
    """Request to create a new paragraph."""

    paragraph_text: str = Field(..., description="Paragraph text with optional markup")
    feature_names: list[str] = Field(
        ..., description="Feature names to link", min_length=1
    )
    organism_abbrev: str = Field(..., description="Organism abbreviation")


class CreateParagraphResponse(BaseModel):
    """Response for paragraph creation."""

    paragraph_no: int
    linked_features: list[LinkedFeature]
    message: str


class UpdateParagraphRequest(BaseModel):
    """Request to update a paragraph."""

    paragraph_text: str = Field(..., description="New paragraph text")
    update_date: bool = Field(
        default=False, description="Whether to update date_edited"
    )


class UpdateParagraphResponse(BaseModel):
    """Response for paragraph update."""

    success: bool
    message: str


class ParagraphOrderItem(BaseModel):
    """Single paragraph order item."""

    paragraph_no: int
    order: int


class ReorderParagraphsRequest(BaseModel):
    """Request to reorder paragraphs."""

    paragraph_orders: list[ParagraphOrderItem] = Field(
        ..., description="List of paragraph_no and order pairs"
    )


class ReorderParagraphsResponse(BaseModel):
    """Response for reorder operation."""

    success: bool
    message: str


class LinkFeatureRequest(BaseModel):
    """Request to link a feature to a paragraph."""

    feature_name: str = Field(..., description="Feature name to link")
    organism_abbrev: str = Field(..., description="Organism abbreviation")


class LinkFeatureResponse(BaseModel):
    """Response for linking a feature."""

    feature_no: int
    feature_name: str
    paragraph_order: int
    message: str


class UnlinkFeatureResponse(BaseModel):
    """Response for unlinking a feature."""

    success: bool
    message: str


class OrganismItem(BaseModel):
    """Organism for dropdown."""

    organism_abbrev: str
    organism_name: str


class OrganismsResponse(BaseModel):
    """Response for organisms list."""

    organisms: list[OrganismItem]


# ---------------------------
# Endpoints
# ---------------------------


@router.get("/organisms", response_model=OrganismsResponse)
def get_organisms(
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Get list of organisms for dropdown."""
    try:
        service = ParagraphCurationService(db)
        organisms = service.get_organisms()
        return OrganismsResponse(organisms=[OrganismItem(**o) for o in organisms])
    except Exception as e:
        logger.exception(f"Error fetching organisms: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        )


@router.get("/feature/{feature_name}", response_model=FeatureParagraphsResponse)
def get_paragraphs_for_feature(
    feature_name: str,
    organism: Optional[str] = None,
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """
    Get all paragraphs for a feature.

    Returns paragraphs ordered by paragraph_order.
    """
    service = ParagraphCurationService(db)

    try:
        result = service.get_paragraphs_for_feature(feature_name, organism)
        return FeatureParagraphsResponse(**result)

    except ParagraphCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )


@router.get("/{paragraph_no}", response_model=ParagraphDetailsResponse)
def get_paragraph_details(
    paragraph_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Get full details for a paragraph.

    Includes linked features and references.
    """
    service = ParagraphCurationService(db)

    try:
        result = service.get_paragraph_details(paragraph_no)
        return ParagraphDetailsResponse(**result)

    except ParagraphCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )


@router.post("", response_model=CreateParagraphResponse)
@router.post("/", response_model=CreateParagraphResponse, include_in_schema=False)
def create_paragraph(
    request: CreateParagraphRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Create a new paragraph and link to features.

    Paragraph text can contain markup:
    - <reference:S000123456> for reference links
    - <feature:S000012345>ACT1</feature> for feature links
    - <go:1234>term</go> for GO term links
    """
    service = ParagraphCurationService(db)

    try:
        result = service.create_paragraph(
            paragraph_text=request.paragraph_text,
            feature_names=request.feature_names,
            organism_abbrev=request.organism_abbrev,
            curator_userid=current_user.userid,
        )

        return CreateParagraphResponse(
            paragraph_no=result["paragraph_no"],
            linked_features=[LinkedFeature(**f) for f in result["linked_features"]],
            message=f"Paragraph {result['paragraph_no']} created successfully",
        )

    except ParagraphCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.put("/{paragraph_no}", response_model=UpdateParagraphResponse)
def update_paragraph(
    paragraph_no: int,
    request: UpdateParagraphRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Update paragraph text.

    Reference links are automatically updated based on markup in text.
    """
    service = ParagraphCurationService(db)

    try:
        service.update_paragraph(
            paragraph_no=paragraph_no,
            paragraph_text=request.paragraph_text,
            update_date=request.update_date,
            curator_userid=current_user.userid,
        )

        return UpdateParagraphResponse(
            success=True,
            message=f"Paragraph {paragraph_no} updated",
        )

    except ParagraphCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post(
    "/feature/{feature_no}/reorder", response_model=ReorderParagraphsResponse
)
def reorder_paragraphs(
    feature_no: int,
    request: ReorderParagraphsRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Reorder paragraphs for a feature.

    Orders must be sequential starting at 1.
    """
    service = ParagraphCurationService(db)

    try:
        service.reorder_paragraphs(
            feature_no=feature_no,
            paragraph_orders=[po.model_dump() for po in request.paragraph_orders],
            curator_userid=current_user.userid,
        )

        return ReorderParagraphsResponse(
            success=True,
            message=f"Paragraphs reordered for feature {feature_no}",
        )

    except ParagraphCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{paragraph_no}/link", response_model=LinkFeatureResponse)
def link_feature(
    paragraph_no: int,
    request: LinkFeatureRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Link a paragraph to a feature.

    The paragraph will be added at the end of the feature's paragraph list.
    """
    service = ParagraphCurationService(db)

    try:
        result = service.link_feature(
            paragraph_no=paragraph_no,
            feature_name=request.feature_name,
            organism_abbrev=request.organism_abbrev,
            curator_userid=current_user.userid,
        )

        return LinkFeatureResponse(
            feature_no=result["feature_no"],
            feature_name=result["feature_name"],
            paragraph_order=result["paragraph_order"],
            message=f"Linked paragraph {paragraph_no} to {result['feature_name']}",
        )

    except ParagraphCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete(
    "/{paragraph_no}/feature/{feature_no}", response_model=UnlinkFeatureResponse
)
def unlink_feature(
    paragraph_no: int,
    feature_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Unlink a paragraph from a feature.
    """
    service = ParagraphCurationService(db)

    try:
        service.unlink_feature(
            paragraph_no=paragraph_no,
            feature_no=feature_no,
            curator_userid=current_user.userid,
        )

        return UnlinkFeatureResponse(
            success=True,
            message=f"Unlinked paragraph {paragraph_no} from feature {feature_no}",
        )

    except ParagraphCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
