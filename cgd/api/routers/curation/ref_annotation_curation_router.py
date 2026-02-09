"""
Reference Annotation Curation Router - Endpoints for managing annotations
linked to a reference.

Mirrors functionality from legacy UpdateReferenceAnnotation.pm:
- Literature Guide management
- GO annotation management
- REF_LINK management

Requires curator authentication.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.ref_annotation_curation_service import (
    RefAnnotationCurationService,
    RefAnnotationCurationError,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/curation/reference-annotation",
    tags=["curation-reference-annotation"],
)


# ---------------------------
# Request/Response Schemas
# ---------------------------


class ReferenceInfo(BaseModel):
    """Basic reference information."""

    reference_no: int
    pubmed: Optional[int]
    dbxref_id: Optional[str]
    citation: Optional[str]
    title: Optional[str]


class LitGuideEntry(BaseModel):
    """Literature guide entry (topic/feature link)."""

    type: str  # 'feature' or 'non_gene'
    ref_property_no: int
    refprop_feat_no: Optional[int]
    property_type: str
    property_value: str
    feature_no: Optional[int]
    feature_name: Optional[str]
    gene_name: Optional[str]
    date_created: Optional[str]
    created_by: Optional[str]
    date_last_reviewed: Optional[str] = None


class GoAnnotationEntry(BaseModel):
    """GO annotation entry."""

    go_ref_no: int
    go_annotation_no: int
    feature_no: int
    feature_name: str
    gene_name: Optional[str]
    go_no: int
    goid: int
    go_term: str
    go_aspect: str
    go_evidence: str
    qualifier: Optional[str]
    support: Optional[str]
    has_qualifier: str
    has_supporting_evidence: str
    date_created: Optional[str]
    created_by: Optional[str]


class RefLinkEntry(BaseModel):
    """REF_LINK entry."""

    ref_link_no: int
    tab_name: str
    primary_key: int
    col_name: str
    date_created: Optional[str]
    created_by: Optional[str]
    # Additional fields depending on table type
    feature_no: Optional[int] = None
    feature_name: Optional[str] = None
    gene_name: Optional[str] = None
    feature_type: Optional[str] = None
    headline: Optional[str] = None
    pheno_annotation_no: Optional[int] = None
    observable: Optional[str] = None
    qualifier: Optional[str] = None
    feat_alias_no: Optional[int] = None
    alias_name: Optional[str] = None


class RefLinksResponse(BaseModel):
    """REF_LINK entries grouped by table type."""

    feature: list[RefLinkEntry]
    pheno_annotation: list[RefLinkEntry]
    feat_alias: list[RefLinkEntry]
    other: list[RefLinkEntry]


class ReferenceAnnotationsResponse(BaseModel):
    """Full response with all annotations for a reference."""

    reference: ReferenceInfo
    lit_guide: list[LitGuideEntry]
    go_annotations: list[GoAnnotationEntry]
    ref_links: RefLinksResponse


class DeleteLitGuideRequest(BaseModel):
    """Request to delete a literature guide entry."""

    refprop_feat_no: Optional[int] = Field(
        None, description="RefpropFeat ID (for feature links)"
    )
    ref_property_no: int = Field(..., description="RefProperty ID")


class TransferLitGuideRequest(BaseModel):
    """Request to transfer a literature guide entry."""

    refprop_feat_no: Optional[int] = Field(
        None, description="RefpropFeat ID (for feature links)"
    )
    ref_property_no: int = Field(..., description="RefProperty ID")
    new_reference_no: int = Field(..., description="Target reference number")


class DeleteGoRefRequest(BaseModel):
    """Request to delete a GO annotation entry."""

    go_ref_no: int = Field(..., description="GoRef ID")


class TransferGoRefRequest(BaseModel):
    """Request to transfer a GO annotation entry."""

    go_ref_no: int = Field(..., description="GoRef ID")
    new_reference_no: int = Field(..., description="Target reference number")


class DeleteRefLinkRequest(BaseModel):
    """Request to delete a REF_LINK entry."""

    ref_link_no: int = Field(..., description="RefLink ID")


class TransferRefLinkRequest(BaseModel):
    """Request to transfer a REF_LINK entry."""

    ref_link_no: int = Field(..., description="RefLink ID")
    new_reference_no: int = Field(..., description="Target reference number")


class BulkActionRequest(BaseModel):
    """Request for bulk delete/transfer."""

    entry_type: str = Field(
        ...,
        description="Entry type: 'lit_guide', 'go_annotation', or 'ref_link'",
    )
    new_reference_no: Optional[int] = Field(
        None, description="Target reference (for transfer only)"
    )


class ActionResponse(BaseModel):
    """Generic response for actions."""

    success: bool
    message: Optional[str] = None
    messages: Optional[list[str]] = None
    warning: Optional[str] = None
    count: Optional[int] = None


# ---------------------------
# Endpoints
# ---------------------------


@router.get("/{reference_no}", response_model=ReferenceAnnotationsResponse)
def get_reference_annotations(
    reference_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Get all annotations associated with a reference.

    Returns literature guide entries, GO annotations, and REF_LINK entries.
    """
    service = RefAnnotationCurationService(db)

    try:
        annotations = service.get_reference_annotations(reference_no)
        return annotations
    except RefAnnotationCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )


@router.post("/lit-guide/delete", response_model=ActionResponse)
def delete_lit_guide_entry(
    request: DeleteLitGuideRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Delete a literature guide entry.

    Deletes the refprop_feat entry (if provided) and optionally
    the ref_property if no more features are linked.
    """
    service = RefAnnotationCurationService(db)

    try:
        result = service.delete_lit_guide_entry(
            refprop_feat_no=request.refprop_feat_no,
            ref_property_no=request.ref_property_no,
            curator_userid=current_user.userid,
        )
        return ActionResponse(**result)
    except RefAnnotationCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/lit-guide/transfer", response_model=ActionResponse)
def transfer_lit_guide_entry(
    request: TransferLitGuideRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Transfer a literature guide entry to another reference.

    Creates new entries for the target reference and deletes the old ones.
    """
    service = RefAnnotationCurationService(db)

    try:
        result = service.transfer_lit_guide_entry(
            refprop_feat_no=request.refprop_feat_no,
            ref_property_no=request.ref_property_no,
            new_reference_no=request.new_reference_no,
            curator_userid=current_user.userid,
        )
        return ActionResponse(**result)
    except RefAnnotationCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/go-ref/delete", response_model=ActionResponse)
def delete_go_ref_entry(
    request: DeleteGoRefRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Delete a GO annotation entry.

    Deletes the go_ref and related entries. If no other references
    link to the go_annotation, deletes that too.
    """
    service = RefAnnotationCurationService(db)

    try:
        result = service.delete_go_ref_entry(
            go_ref_no=request.go_ref_no,
            curator_userid=current_user.userid,
        )
        return ActionResponse(**result)
    except RefAnnotationCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/go-ref/transfer", response_model=ActionResponse)
def transfer_go_ref_entry(
    request: TransferGoRefRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Transfer a GO annotation to another reference.

    Creates a new go_ref for the target reference (including
    qualifiers and supporting evidence), then deletes the old one.
    """
    service = RefAnnotationCurationService(db)

    try:
        result = service.transfer_go_ref_entry(
            go_ref_no=request.go_ref_no,
            new_reference_no=request.new_reference_no,
            curator_userid=current_user.userid,
        )
        return ActionResponse(**result)
    except RefAnnotationCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/ref-link/delete", response_model=ActionResponse)
def delete_ref_link_entry(
    request: DeleteRefLinkRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Delete a REF_LINK entry.

    Removes the reference association. Does NOT delete the underlying data.
    Returns a warning if the data becomes orphaned.
    """
    service = RefAnnotationCurationService(db)

    try:
        result = service.delete_ref_link_entry(
            ref_link_no=request.ref_link_no,
            curator_userid=current_user.userid,
        )
        return ActionResponse(**result)
    except RefAnnotationCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/ref-link/transfer", response_model=ActionResponse)
def transfer_ref_link_entry(
    request: TransferRefLinkRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Transfer a REF_LINK entry to another reference.

    Updates the reference_no on the ref_link entry.
    """
    service = RefAnnotationCurationService(db)

    try:
        result = service.transfer_ref_link_entry(
            ref_link_no=request.ref_link_no,
            new_reference_no=request.new_reference_no,
            curator_userid=current_user.userid,
        )
        return ActionResponse(**result)
    except RefAnnotationCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{reference_no}/bulk-delete", response_model=ActionResponse)
def bulk_delete_annotations(
    reference_no: int,
    request: BulkActionRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Bulk delete all annotations of a given type for a reference.

    entry_type must be 'lit_guide', 'go_annotation', or 'ref_link'.
    """
    service = RefAnnotationCurationService(db)

    try:
        result = service.bulk_delete(
            reference_no=reference_no,
            entry_type=request.entry_type,
            curator_userid=current_user.userid,
        )
        return ActionResponse(**result)
    except RefAnnotationCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{reference_no}/bulk-transfer", response_model=ActionResponse)
def bulk_transfer_annotations(
    reference_no: int,
    request: BulkActionRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Bulk transfer all annotations of a given type to another reference.

    entry_type must be 'lit_guide', 'go_annotation', or 'ref_link'.
    new_reference_no is required.
    """
    if not request.new_reference_no:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="new_reference_no is required for transfer",
        )

    service = RefAnnotationCurationService(db)

    try:
        result = service.bulk_transfer(
            reference_no=reference_no,
            entry_type=request.entry_type,
            new_reference_no=request.new_reference_no,
            curator_userid=current_user.userid,
        )
        return ActionResponse(**result)
    except RefAnnotationCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
