import logging
import traceback

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.api.services import locus_service
from cgd.schemas.locus_schema import (
    LocusByOrganismResponse,
    SequenceDetailsResponse,
    LocusReferencesResponse,
    LocusSummaryNotesResponse,
    LocusHistoryResponse,
)
from cgd.schemas.phenotype_schema import PhenotypeDetailsResponse
from cgd.schemas.go_schema import GODetailsResponse
from cgd.schemas.interaction_schema import InteractionDetailsResponse
from cgd.schemas.protein_schema import ProteinDetailsResponse
from cgd.schemas.homology_schema import HomologyDetailsResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/locus", tags=["locus"])


@router.get("/{name}", response_model=LocusByOrganismResponse)
def locus(name: str, db: Session = Depends(get_db)):
    """
    Get basic locus info by name, grouped by organism.

    Returns feature info including aliases and external links.
    """
    return locus_service.get_locus_by_organism(db, name)


@router.get("/{name}/go_details", response_model=GODetailsResponse)
def go_details(name: str, db: Session = Depends(get_db)):
    """
    Get GO annotations for this locus, grouped by organism.
    """
    try:
        return locus_service.get_locus_go_details(db, name)
    except Exception as e:
        logger.error(f"Error in go_details for {name}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{name}/phenotype_details", response_model=PhenotypeDetailsResponse)
def phenotype_details(name: str, db: Session = Depends(get_db)):
    """
    Get phenotype annotations for this locus, grouped by organism.
    """
    try:
        return locus_service.get_locus_phenotype_details(db, name)
    except Exception as e:
        logger.error(f"Error in phenotype_details for {name}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{name}/interaction_details", response_model=InteractionDetailsResponse)
def interaction_details(name: str, db: Session = Depends(get_db)):
    """
    Get interactions for this locus, grouped by organism.
    """
    return locus_service.get_locus_interaction_details(db, name)


@router.get("/{name}/protein_details", response_model=ProteinDetailsResponse)
def protein_details(name: str, db: Session = Depends(get_db)):
    """
    Get protein information for this locus, grouped by organism.

    Returns data matching the Perl protein page format:
    - Stanford Name (gene_name)
    - Systematic Name (feature_name)
    - Alias Names
    - Description (headline)
    - Experimental Observations
    - Structural Information
    - Conserved Domains
    - Sequence Detail
    - Homologs
    - External Sequence Database
    - References Cited on This Page
    """
    try:
        return locus_service.get_locus_protein_details(db, name)
    except Exception as e:
        logger.error(f"Error in protein_details for {name}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{name}/homology_details", response_model=HomologyDetailsResponse)
def homology_details(name: str, db: Session = Depends(get_db)):
    """
    Get homology/ortholog information for this locus, grouped by organism.
    """
    return locus_service.get_locus_homology_details(db, name)


@router.get("/{name}/sequence_details", response_model=SequenceDetailsResponse)
def sequence_details(name: str, db: Session = Depends(get_db)):
    """
    Get sequence and location information for this locus, grouped by organism.

    Returns chromosomal coordinates and DNA/protein sequences.
    """
    return locus_service.get_locus_sequence_details(db, name)


@router.get("/{name}/references", response_model=LocusReferencesResponse)
def references(name: str, db: Session = Depends(get_db)):
    """
    Get references citing this locus, grouped by organism.
    """
    return locus_service.get_locus_references(db, name)


@router.get("/{name}/summary_notes", response_model=LocusSummaryNotesResponse)
def summary_notes(name: str, db: Session = Depends(get_db)):
    """
    Get summary paragraphs for this locus, grouped by organism.
    """
    return locus_service.get_locus_summary_notes(db, name)


@router.get("/{name}/history", response_model=LocusHistoryResponse)
def history(name: str, db: Session = Depends(get_db)):
    """
    Get change history for this locus, grouped by organism.
    """
    return locus_service.get_locus_history(db, name)


