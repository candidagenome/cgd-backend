from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.api.services import reference_service
from cgd.schemas.reference_schema import (
    ReferenceResponse,
    ReferenceLocusResponse,
    ReferenceGOResponse,
    ReferencePhenotypeResponse,
    ReferenceInteractionResponse,
)

router = APIRouter(prefix="/api/reference", tags=["reference"])


@router.get("/{pubmed_id}", response_model=ReferenceResponse)
def get_reference(pubmed_id: int, db: Session = Depends(get_db)):
    """
    Get basic reference info by PubMed ID.

    Returns citation, title, year, authors, abstract, journal info, and URLs.
    """
    return reference_service.get_reference(db, pubmed_id)


@router.get("/{pubmed_id}/locus_details", response_model=ReferenceLocusResponse)
def get_reference_locus_details(pubmed_id: int, db: Session = Depends(get_db)):
    """
    Get loci (genes/features) addressed in this paper.

    Returns list of features linked to this reference via ref_property.
    """
    return reference_service.get_reference_locus_details(db, pubmed_id)


@router.get("/{pubmed_id}/go_details", response_model=ReferenceGOResponse)
def get_reference_go_details(pubmed_id: int, db: Session = Depends(get_db)):
    """
    Get GO annotations citing this reference.

    Returns list of GO annotations linked to this reference.
    """
    return reference_service.get_reference_go_details(db, pubmed_id)


@router.get("/{pubmed_id}/phenotype_details", response_model=ReferencePhenotypeResponse)
def get_reference_phenotype_details(pubmed_id: int, db: Session = Depends(get_db)):
    """
    Get phenotype annotations citing this reference.

    Returns list of phenotype annotations linked to this reference.
    """
    return reference_service.get_reference_phenotype_details(db, pubmed_id)


@router.get("/{pubmed_id}/interaction_details", response_model=ReferenceInteractionResponse)
def get_reference_interaction_details(pubmed_id: int, db: Session = Depends(get_db)):
    """
    Get interactions citing this reference.

    Returns list of interactions linked to this reference.
    """
    return reference_service.get_reference_interaction_details(db, pubmed_id)
