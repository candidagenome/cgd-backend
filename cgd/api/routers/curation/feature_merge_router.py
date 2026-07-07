"""
Feature Merge Router - Merge two ORF features into one.

Used after a sequence-error correction makes two adjacent ORF fragments a single
gene: extends the survivor, transfers+dedups the retired feature's annotations,
preserves its identifiers, and soft-retires it (deprecates its current location
and sequences while keeping its history).

Requires curator authentication.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.feature_merge_service import (
    FeatureMergeService,
    FeatureMergeError,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/curation/feature-merge",
    tags=["curation-feature-merge"],
)


# ---------------------------
# Schemas
# ---------------------------
class MergeRequest(BaseModel):
    """Request to merge (or preview merging) two ORF features."""

    survivor_name: str = Field(..., description="Feature that survives and is extended")
    retire_name: str = Field(..., description="Feature that is retired into the survivor")
    new_stop_coord: int = Field(
        ..., description="New far-boundary coordinate for the survivor (its stop_coord)"
    )
    note: str = Field(..., description="Required note describing the merge")
    reference_nos: list[int] = Field(
        default_factory=list, description="Optional CGD reference_no(s) to link to the note"
    )
    dry_run: bool = Field(
        False, description="If true, perform all writes then roll back (preview)"
    )


@router.get("/feature-summary")
def get_feature_summary(
    feature_name: str,
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """Summary of a feature (location, CDS children, annotation counts) for the
    merge form."""
    service = FeatureMergeService(db)
    try:
        return service.get_feature_summary(feature_name)
    except FeatureMergeError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.post("/merge")
def merge_features(
    request: MergeRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Merge ``retire_name`` into ``survivor_name``.

    Extends the survivor (and its co-terminal CDS child) to ``new_stop_coord``,
    regenerates its sequences, transfers+dedups the retired feature's
    annotations, preserves its systematic name as a synonym, and soft-retires the
    redundant feature. Requires curator authentication. Use ``dry_run=true`` to
    validate against the live database and roll back without persisting.
    """
    service = FeatureMergeService(db)
    try:
        result = service.merge_features(
            survivor_name=request.survivor_name,
            retire_name=request.retire_name,
            new_stop_coord=request.new_stop_coord,
            note_text=request.note,
            curator_userid=current_user.userid,
            reference_nos=request.reference_nos,
            dry_run=request.dry_run,
        )
    except FeatureMergeError as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception:
        db.rollback()
        logger.exception(
            "merge_features failed: %s <- %s",
            request.survivor_name, request.retire_name,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Feature merge failed; the transaction was rolled back.",
        )

    logger.info(
        "Feature merge %s: %s <- %s by %s (dry_run=%s)",
        "preview" if request.dry_run else "committed",
        request.survivor_name, request.retire_name,
        current_user.userid, request.dry_run,
    )
    return result
