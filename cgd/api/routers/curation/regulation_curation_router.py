"""
Regulation Curation Router - curator entry of regulator -> target associations.

DNA binding and expression changes in single-gene mutants. Rows are stored in
MULTI.REGULATION and shown on the locus Regulation tab alongside
PathoYeastract's associations and RNA accumulation phenotypes. Requires
curator authentication.
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.regulation_curation_service import (
    RegulationCurationError,
    RegulationCurationService,
    strip_private,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/curation/regulation", tags=["curation-regulation"])


class BatchRequest(BaseModel):
    organism: str = Field(..., description="Organism abbrev, e.g. C_glabrata_CBS138")
    pubmed: int = Field(..., description="PubMed ID; the reference must already be in CGD")
    tf: str = Field(..., description="Regulator gene")
    targets: str = Field(..., description="One target per line: GENE [effect | up/down] [log2FC]")
    evidence_code: str = Field(..., description="Direct (DNA binding) or Indirect (expression in a mutant)")
    experiment: str
    mutant_type: Optional[str] = Field(None, description="Indirect: how the regulator was mutated")
    expression_change: Optional[str] = Field(None, description="Indirect: default change in the mutant, "
                                                               "up, down, or infer (from log2FC sign)")
    association_type: str = Field("N/A", description="Direct: default effect, Positive, Negative, N/A, or "
                                                     "infer (from each line's log2FC sign)")
    strain_background: Optional[str] = None
    condition_group: Optional[str] = None
    environmental_condition: Optional[str] = None
    regulation_note: Optional[str] = None


class UpdateRequest(BaseModel):
    evidence_code: str
    experiment: str
    mutant_type: Optional[str] = None
    association_type: str
    strain_background: Optional[str] = None
    condition_group: Optional[str] = None
    environmental_condition: Optional[str] = None
    log2fc: Optional[float] = None
    regulation_note: Optional[str] = None


class DeleteRequest(BaseModel):
    regulation_nos: list[int]


class PeaksRequest(BaseModel):
    organism: str
    text: str = Field(..., description="BED / narrowPeak, a chrom,start,end table, or chrom:start-end lines")
    window: Optional[int] = Field(None, description="Promoter length upstream of the gene start (bp)")


class AllowRequest(BaseModel):
    organism: str
    gene: str
    note: Optional[str] = None


class RemoveAllowRequest(BaseModel):
    organism: str
    gene: str


def _bad_request(e: Exception):
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.get("/vocabulary")
def get_vocabulary(current_user: CurrentUser):
    """Evidence types, experiments, condition groups and effects for the entry form."""
    return RegulationCurationService.get_vocabulary()


@router.get("/review")
def review(organism: str, current_user: CurrentUser, db: Session = Depends(get_db)):
    """Admitted regulators, phenotype regulators left off the tab, and reporters needing review."""
    try:
        return RegulationCurationService(db).review(organism)
    except RegulationCurationError as e:
        _bad_request(e)


@router.post("/regulators/allow")
def allow_regulator(request: AllowRequest, current_user: CurrentUser, db: Session = Depends(get_db)):
    """Admit a gene as a regulator."""
    try:
        return RegulationCurationService(db).allow_regulator(request.organism, request.gene, request.note,
                                                             current_user.userid)
    except RegulationCurationError as e:
        _bad_request(e)


@router.post("/regulators/remove")
def remove_regulator(request: RemoveAllowRequest, current_user: CurrentUser, db: Session = Depends(get_db)):
    """Withdraw a curator admission (the gene may still be a regulator by GO or PathoYeastract)."""
    try:
        return RegulationCurationService(db).remove_regulator(request.organism, request.gene, current_user.userid)
    except RegulationCurationError as e:
        _bad_request(e)


@router.post("/peaks")
def assign_peaks(request: PeaksRequest, current_user: CurrentUser, db: Session = Depends(get_db)):
    """Turn a genome-wide binding peak file into a target list (genes whose promoters have peaks)."""
    try:
        return RegulationCurationService(db).assign_peaks(request.organism, request.text, request.window)
    except RegulationCurationError as e:
        _bad_request(e)


@router.get("/reference/{pubmed}")
def get_reference(pubmed: int, current_user: CurrentUser, db: Session = Depends(get_db)):
    """A paper and the associations already curated from it."""
    try:
        return RegulationCurationService(db).get_reference(pubmed)
    except RegulationCurationError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.get("/gene/{name}")
def get_gene(name: str, organism: str, current_user: CurrentUser, db: Session = Depends(get_db)):
    """Associations curated for a gene, as a TF and as a target."""
    try:
        return RegulationCurationService(db).get_gene(name, organism)
    except RegulationCurationError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.post("/preview")
def preview(request: BatchRequest, current_user: CurrentUser, db: Session = Depends(get_db)):
    """Resolve and check a batch without saving it."""
    try:
        return strip_private(RegulationCurationService(db).preview(request.model_dump()))
    except RegulationCurationError as e:
        _bad_request(e)


@router.post("")
def create(request: BatchRequest, current_user: CurrentUser, db: Session = Depends(get_db)):
    """Save a batch: the targets that resolve to new associations."""
    try:
        return RegulationCurationService(db).create(request.model_dump(), current_user.userid)
    except RegulationCurationError as e:
        _bad_request(e)


@router.put("/{regulation_no}")
def update(regulation_no: int, request: UpdateRequest, current_user: CurrentUser,
           db: Session = Depends(get_db)):
    """Edit one association's evidence fields."""
    try:
        return RegulationCurationService(db).update(regulation_no, request.model_dump(), current_user.userid)
    except RegulationCurationError as e:
        _bad_request(e)


@router.post("/delete")
def delete(request: DeleteRequest, current_user: CurrentUser, db: Session = Depends(get_db)):
    """Delete curated associations."""
    try:
        deleted = RegulationCurationService(db).delete(request.regulation_nos, current_user.userid)
    except RegulationCurationError as e:
        _bad_request(e)
    return {"deleted": deleted}
