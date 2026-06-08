"""
Interaction Curation Router - CRUD for curator-entered interactions.

Curator-entered interactions are tagged source='CGD'; imported BioGRID
interactions are shown read-only and cannot be edited or deleted here.
Requires curator authentication.
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.interaction_curation_service import (
    InteractionCurationService,
    InteractionCurationError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/curation/interactions", tags=["curation-interactions"])


# ---------------------------
# Response/Request Schemas
# ---------------------------


class InteractionPartnerOut(BaseModel):
    feature_name: str
    gene_name: Optional[str] = None
    action: str  # Bait / Hit


class InteractionReferenceUrlOut(BaseModel):
    url_type: Optional[str] = None
    url: str


class InteractionReferenceOut(BaseModel):
    reference_no: Optional[int] = None
    dbxref_id: Optional[str] = None
    pubmed: Optional[int] = None
    citation: Optional[str] = None
    urls: list[InteractionReferenceUrlOut] = []


class CuratedInteractionOut(BaseModel):
    interaction_no: int
    experiment_type: str
    source: str  # BioGRID or CGD
    description: Optional[str] = None
    partners: list[InteractionPartnerOut] = []
    references: list[InteractionReferenceOut] = []
    editable: bool = False  # True only for CGD-curated rows


class FeatureInteractionsOut(BaseModel):
    feature_name: str
    gene_name: Optional[str] = None
    organism: Optional[str] = None
    physical: list[CuratedInteractionOut] = []
    genetic: list[CuratedInteractionOut] = []


class ExperimentTypesOut(BaseModel):
    physical: list[str] = []
    genetic: list[str] = []


class CreateInteractionRequest(BaseModel):
    organism: Optional[str] = Field(
        None, description="Organism abbrev (e.g. C_albicans_SC5314) to disambiguate genes"
    )
    interactor: str = Field(..., description="Interacting (partner) gene; recorded as Hit")
    experiment_type: str = Field(..., description="Interaction experiment type")
    pubmed: int = Field(..., description="PubMed ID; must already exist in CGD")
    description: Optional[str] = Field(None, max_length=240)


class CreateInteractionResponse(BaseModel):
    interaction_no: int
    interaction_type: str  # physical / genetic
    message: str


class DeleteInteractionResponse(BaseModel):
    success: bool
    message: str


# ---------------------------
# Endpoints
# NOTE: /experiment-types is declared before /{feature_name} so it isn't
# captured by the path parameter.
# ---------------------------


@router.get("/experiment-types", response_model=ExperimentTypesOut)
def get_experiment_types(current_user: CurrentUser, db: Session = Depends(get_db)):
    """Return the physical and genetic experiment-type vocabularies."""
    return InteractionCurationService(db).get_experiment_types()


@router.get("/{feature_name}", response_model=FeatureInteractionsOut)
def get_interactions(
    feature_name: str,
    current_user: CurrentUser,
    organism: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Return a gene's interactions, split into physical and genetic."""
    service = InteractionCurationService(db)
    try:
        return service.get_interactions(feature_name, organism)
    except InteractionCurationError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.post("/{feature_name}", response_model=CreateInteractionResponse)
def create_interaction(
    feature_name: str,
    request: CreateInteractionRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Create a CGD-curated interaction. The queried gene is recorded as Bait."""
    service = InteractionCurationService(db)
    try:
        interaction_no = service.create_interaction(
            feature_name=feature_name,
            organism_abbrev=request.organism,
            partner_name=request.interactor,
            experiment_type=request.experiment_type,
            pubmed=request.pubmed,
            description=request.description,
            curator_userid=current_user.userid,
        )
    except InteractionCurationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    return CreateInteractionResponse(
        interaction_no=interaction_no,
        interaction_type=service.classify(request.experiment_type),
        message="Interaction created.",
    )


@router.delete("/{interaction_no}", response_model=DeleteInteractionResponse)
def delete_interaction(
    interaction_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Delete a CGD-curated interaction (BioGRID interactions cannot be deleted)."""
    service = InteractionCurationService(db)
    try:
        service.delete_interaction(interaction_no, current_user.userid)
    except InteractionCurationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    return DeleteInteractionResponse(success=True, message="Interaction deleted.")
