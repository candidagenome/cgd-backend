from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.api.services import locus_service
from cgd.schemas.locus_schema import LocusByOrganismResponse
from cgd.schemas.phenotype_schema import PhenotypeDetailsResponse
from cgd.schemas.go_schema import GODetailsResponse
from cgd.schemas.interaction_schema import InteractionDetailsResponse
from cgd.schemas.protein_schema import ProteinDetailsResponse
from cgd.schemas.homology_schema import HomologyDetailsResponse

router = APIRouter(prefix="/api/locus", tags=["locus"])


@router.get("/{name}", response_model=LocusByOrganismResponse)
def locus(name: str, db: Session = Depends(get_db)):
    return locus_service.get_locus_by_organism(db, name)


@router.get("/{name}/phenotype_details", response_model=PhenotypeDetailsResponse)
def phenotype_details(name: str, db: Session = Depends(get_db)):
    return locus_service.get_locus_phenotype_details(db, name)


@router.get("/{name}/go_details", response_model=GODetailsResponse)
def go_details(name: str, db: Session = Depends(get_db)):
    return locus_service.get_locus_go_details(db, name)


@router.get("/{name}/interaction_details", response_model=InteractionDetailsResponse)
def interaction_details(name: str, db: Session = Depends(get_db)):
    return locus_service.get_locus_interaction_details(db, name)


@router.get("/{name}/protein_details", response_model=ProteinDetailsResponse)
def protein_details(name: str, db: Session = Depends(get_db)):
    return locus_service.get_locus_protein_details(db, name)


@router.get("/{name}/homology_details", response_model=HomologyDetailsResponse)
def homology_details(name: str, db: Session = Depends(get_db)):
    return locus_service.get_locus_homology_details(db, name)


