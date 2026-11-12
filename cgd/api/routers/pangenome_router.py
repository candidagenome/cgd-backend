"""Pangenome viewer API: published pangenome studies of CGD species."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy.orm import Session

from cgd.api.services import pangenome_service
from cgd.api.services.pangenome_service import PangenomeNotFound
from cgd.db.deps import get_db
from cgd.schemas.pangenome_schema import (
    PangenomeGene,
    PangenomeGenePage,
    PangenomeLocusResponse,
    PangenomeStudy,
    PangenomeStudyList,
)

router = APIRouter(prefix="/api/pangenome", tags=["pangenome"])


@router.get("/studies", response_model=PangenomeStudyList)
def list_studies(db: Session = Depends(get_db)):
    """All loaded pangenome studies with their summary counts."""
    return pangenome_service.list_studies(db)


@router.get("/studies/{study_id}", response_model=PangenomeStudy)
def get_study(study_id: str, db: Session = Depends(get_db)):
    """One study: citation, counts, notes and isolates."""
    try:
        return pangenome_service.get_study(db, study_id)
    except PangenomeNotFound as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/studies/{study_id}/genes", response_model=PangenomeGenePage)
def list_genes(
    study_id: str,
    category: Optional[str] = Query(None, pattern="^(core|accessory)$"),
    reference: Optional[str] = Query(None, pattern="^(in|novel)$",
                                     description="in = in the reference annotation; novel = not in it"),
    stops: Optional[bool] = Query(None, description="only genes with (true) or without (false) premature stops"),
    q: Optional[str] = Query(None, description="gene ID, CGD name, ortholog, type, note or InterPro text"),
    sort: str = Query("position", pattern="^(position|id|presence|stops)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """A study's pangenome genes, filtered and paged."""
    try:
        return pangenome_service.list_genes(db, study_id, category, reference, stops, q, sort, page, page_size)
    except PangenomeNotFound as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/studies/{study_id}/genes/{gene_id}", response_model=PangenomeGene)
def get_gene(study_id: str, gene_id: str, db: Session = Depends(get_db)):
    """One study gene, including its premature stop codons per isolate."""
    try:
        return pangenome_service.get_gene(db, study_id, gene_id)
    except PangenomeNotFound as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/studies/{study_id}/download/{table}")
def download(study_id: str, table: str, db: Session = Depends(get_db)):
    """A study table as TSV: genes, accessory, stop_codons or isolates."""
    try:
        filename, body = pangenome_service.download_table(db, study_id, table)
    except PangenomeNotFound as e:
        raise HTTPException(status_code=404, detail=str(e))
    return Response(body, media_type="text/tab-separated-values",
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@router.get("/locus/{name}", response_model=PangenomeLocusResponse)
def locus_pangenome(name: str, db: Session = Depends(get_db)):
    """Pangenome records for a locus (empty when no loaded study covers it)."""
    return pangenome_service.get_locus_pangenome(db, name)
