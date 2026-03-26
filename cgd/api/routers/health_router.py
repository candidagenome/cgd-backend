from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.models.models import Organism

router = APIRouter(tags=["health"])


@router.get("/health")
def health():
    return {"ok": True}


@router.get("/api/health")
def api_health():
    return {"ok": True}


@router.get("/api/organisms")
def get_organisms(db: Session = Depends(get_db)):
    """
    Get list of available organisms.

    Returns all organisms (strain level) with their abbreviations for dropdown filters.
    """
    organisms = (
        db.query(Organism)
        .filter(Organism.taxonomic_rank == "Strain")
        .order_by(Organism.organism_order)
        .all()
    )

    return [
        {
            "organism_no": org.organism_no,
            "organism_name": org.organism_name,
            "organism_abbrev": org.organism_abbrev,
            "common_name": org.common_name,
        }
        for org in organisms
    ]
