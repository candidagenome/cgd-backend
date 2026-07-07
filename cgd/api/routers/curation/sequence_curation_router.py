"""
Sequence Curation Router - Endpoints for chromosome/contig sequence updates.

Mirrors functionality from legacy UpdateRootSequence.pm for curators to
insert, delete, or substitute nucleotides in root sequences.

Requires curator authentication.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.sequence_curation_service import (
    SequenceCurationService,
    SequenceCurationError,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/curation/sequence",
    tags=["curation-sequence"],
)


# ---------------------------
# Request/Response Schemas
# ---------------------------


class RootSequenceInfo(BaseModel):
    """Info about a root sequence (chromosome/contig)."""

    feature_no: int
    feature_name: str
    feature_type: str
    seq_no: int
    seq_length: int


class AssemblyGroup(BaseModel):
    """Group of sequences by assembly."""

    assembly: str
    sequences: list[RootSequenceInfo]


class RootSequencesResponse(BaseModel):
    """Response for list of root sequences."""

    assemblies: list[AssemblyGroup]


class SequenceSegmentResponse(BaseModel):
    """Response for a sequence segment."""

    feature_name: str
    feature_no: Optional[int] = None
    seq_no: Optional[int] = None
    seq_length: int
    start: int
    end: int
    sequence: str
    error: Optional[str] = None


class SequenceChange(BaseModel):
    """A single sequence change operation."""

    type: str = Field(
        ..., description="Type of change: insertion, deletion, or substitution"
    )
    position: Optional[int] = Field(
        None, description="Position for insertion (after this coord)"
    )
    start: Optional[int] = Field(
        None, description="Start coordinate for deletion/substitution"
    )
    end: Optional[int] = Field(
        None, description="End coordinate for deletion/substitution"
    )
    sequence: Optional[str] = Field(
        None, description="Sequence to insert or substitute"
    )


class PreviewChangesRequest(BaseModel):
    """Request to preview sequence changes."""

    feature_name: str
    changes: list[SequenceChange]


class ChangeDetail(BaseModel):
    """Details of a single change."""

    type: str
    position: Optional[int] = None
    start: Optional[int] = None
    end: Optional[int] = None
    sequence: Optional[str] = None
    deleted_sequence: Optional[str] = None
    old_sequence: Optional[str] = None
    new_sequence: Optional[str] = None
    length: Optional[int] = None
    length_change: Optional[int] = None
    old_context: str
    new_context: str


class AffectedFeature(BaseModel):
    """A feature affected by sequence changes."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str]
    feature_type: str
    start_coord: int
    stop_coord: int
    strand: str
    is_overlapping: bool
    is_downstream: bool
    new_start: int
    new_stop: int


class PreviewChangesResponse(BaseModel):
    """Response for change preview."""

    feature_name: str
    feature_no: int
    seq_no: int
    old_length: int
    new_length: int
    net_change: int
    changes: list[ChangeDetail]
    affected_features: list[AffectedFeature]
    error: Optional[str] = None


class ApplyChangesRequest(BaseModel):
    """Request to commit sequence changes (v1: equal-length substitutions)."""

    feature_name: str
    changes: list[SequenceChange]
    note: str = Field(..., description="Required note describing the change")
    reference_nos: list[int] = Field(
        default_factory=list, description="Optional CGD reference_no(s) to link to the note"
    )
    dry_run: bool = Field(
        False, description="If true, perform all writes then roll back (validation only)"
    )


class RegeneratedFeature(BaseModel):
    """A feature whose sequence was regenerated after the edit."""

    feature_no: int
    feature_name: str
    feature_type: str
    new_genomic_seq_no: int
    genomic_length: int
    new_protein_seq_no: Optional[int] = None
    protein_length: Optional[int] = None


class ApplyChangesResponse(BaseModel):
    """Response for an applied (or dry-run) sequence change."""

    feature_name: str
    feature_no: int
    old_root_seq_no: int
    new_root_seq_no: int
    seq_length: int
    substitutions: list[dict]
    feat_locations_repointed: int
    features_regenerated: list[RegeneratedFeature]
    note_no: int
    reference_nos: list[int]
    dry_run: bool


class NearbyFeature(BaseModel):
    """A feature near a coordinate."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str]
    feature_type: str
    start_coord: int
    stop_coord: int
    strand: str


class NearbyFeaturesResponse(BaseModel):
    """Response for nearby features."""

    features: list[NearbyFeature]
    position: int
    range_size: int


# ---------------------------
# Endpoints
# ---------------------------


@router.get("/root-sequences", response_model=RootSequencesResponse)
def get_root_sequences(
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """
    Get all root sequences (chromosomes/contigs) grouped by assembly.

    Returns the list of chromosomes and contigs available for sequence editing.
    """
    service = SequenceCurationService(db)
    assemblies = service.get_root_sequences()

    return RootSequencesResponse(assemblies=assemblies)


@router.get("/segment", response_model=SequenceSegmentResponse)
def get_sequence_segment(
    feature_name: str,
    start: int,
    length: int = 100,
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """
    Get a segment of sequence starting at a coordinate.

    Args:
        feature_name: Chromosome/contig name
        start: Starting coordinate (1-based)
        length: Number of nucleotides to return (default 100)
    """
    service = SequenceCurationService(db)
    result = service.get_sequence_segment(feature_name, start, length)

    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature {feature_name} not found",
        )

    return SequenceSegmentResponse(**result)


@router.post("/preview", response_model=PreviewChangesResponse)
def preview_changes(
    request: PreviewChangesRequest,
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """
    Preview sequence changes without committing.

    Shows the effect of insertions, deletions, and substitutions on the
    sequence and lists affected features that would need coordinate updates.
    """
    # Validate changes
    for change in request.changes:
        if change.type not in ["insertion", "deletion", "substitution"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid change type: {change.type}",
            )
        if change.type == "insertion":
            if change.position is None or not change.sequence:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Insertion requires position and sequence",
                )
        elif change.type == "deletion":
            if change.start is None or change.end is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Deletion requires start and end coordinates",
                )
        elif change.type == "substitution":
            if change.start is None or change.end is None or not change.sequence:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Substitution requires start, end, and sequence",
                )

    service = SequenceCurationService(db)
    changes_dicts = [c.model_dump() for c in request.changes]
    result = service.preview_changes(request.feature_name, changes_dicts)

    if "error" in result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result["error"],
        )

    return PreviewChangesResponse(**result)


@router.post("/apply", response_model=ApplyChangesResponse)
def apply_changes(
    request: ApplyChangesRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Commit sequence changes to a root (chromosome/contig) sequence.

    v1 supports equal-length **substitutions** only. This inserts a new current
    SEQ version with the corrected residues, deprecates the old one, re-points
    every current feat_location to the new root sequence, records
    seq_change_archive rows plus a note (and any referenced publications), and
    regenerates genomic/protein sequences for features whose residues changed.

    Requires curator authentication. Use `dry_run=true` to validate against the
    live database and roll back without persisting.
    """
    # Enforce v1 scope (substitution-only) at the edge for a clear error.
    for change in request.changes:
        if change.type != "substitution":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "This tool currently supports substitution changes only. "
                    "Insertions/deletions are not yet enabled."
                ),
            )
        if change.start is None or change.end is None or not change.sequence:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Substitution requires start, end, and sequence",
            )

    service = SequenceCurationService(db)
    changes_dicts = [c.model_dump() for c in request.changes]
    try:
        result = service.apply_changes(
            feature_name=request.feature_name,
            changes=changes_dicts,
            note_text=request.note,
            curator_userid=current_user.userid,
            reference_nos=request.reference_nos,
            dry_run=request.dry_run,
        )
    except SequenceCurationError as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception:
        db.rollback()
        logger.exception("apply_changes failed for %s", request.feature_name)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Sequence update failed; the transaction was rolled back.",
        )

    logger.info(
        "Sequence %s applied to %s by %s (dry_run=%s)",
        "change" if not request.dry_run else "change (dry-run)",
        request.feature_name, current_user.userid, request.dry_run,
    )
    return ApplyChangesResponse(**result)


@router.get("/nearby-features", response_model=NearbyFeaturesResponse)
def get_nearby_features(
    feature_name: str,
    position: int,
    range_size: int = 5000,
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """
    Get features near a given coordinate.

    Useful for seeing what features are in the vicinity of a planned
    sequence change.

    Args:
        feature_name: Chromosome/contig name
        position: Coordinate to search around
        range_size: Size of range to search (default 5000bp)
    """
    service = SequenceCurationService(db)
    features = service.get_nearby_features(feature_name, position, range_size)

    return NearbyFeaturesResponse(
        features=features,
        position=position,
        range_size=range_size,
    )
