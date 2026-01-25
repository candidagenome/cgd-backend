from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.api.services import chromosome_service
from cgd.schemas.chromosome_schema import (
    ChromosomeResponse,
    ChromosomeHistoryResponse,
    ChromosomeReferencesResponse,
    ChromosomeSummaryNotesResponse,
)

router = APIRouter(prefix="/api/chromosome", tags=["chromosome"])


@router.get("/{name}", response_model=ChromosomeResponse)
def get_chromosome(name: str, db: Session = Depends(get_db)):
    """
    Get basic chromosome/contig info by name.

    Returns feature info, organism, coordinates, aliases, and history summary counts.
    """
    return chromosome_service.get_chromosome(db, name)


@router.get("/{name}/history", response_model=ChromosomeHistoryResponse)
def get_chromosome_history(name: str, db: Session = Depends(get_db)):
    """
    Get chromosome change history.

    Returns sequence changes, annotation changes, and curatorial notes.
    """
    return chromosome_service.get_chromosome_history(db, name)


@router.get("/{name}/references", response_model=ChromosomeReferencesResponse)
def get_chromosome_references(name: str, db: Session = Depends(get_db)):
    """
    Get references for a chromosome.

    Returns list of references citing this chromosome.
    """
    return chromosome_service.get_chromosome_references(db, name)


@router.get("/{name}/summary_notes", response_model=ChromosomeSummaryNotesResponse)
def get_chromosome_summary_notes(name: str, db: Session = Depends(get_db)):
    """
    Get summary notes/paragraphs for a chromosome.

    Returns paragraphs that summarize information about this chromosome.
    """
    return chromosome_service.get_chromosome_summary_notes(db, name)
