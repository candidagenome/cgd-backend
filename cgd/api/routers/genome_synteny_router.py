"""Router for genome-wide synteny browser endpoints."""
from __future__ import annotations

import logging
import traceback
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.api.services import genome_synteny_service, synteny_service
from cgd.schemas.synteny_schema import (
    ChromosomeListResponse,
    ChromosomeGenesResponse,
    SyntenyResolveResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/synteny", tags=["synteny"])


@router.get("/chromosomes", response_model=ChromosomeListResponse)
def get_chromosomes(db: Session = Depends(get_db)):
    """
    Get list of all chromosomes for each CGD species.

    Returns chromosome name, length, and gene count for each species.
    Used by the genome synteny browser to populate the chromosome dropdown.
    """
    try:
        return genome_synteny_service.get_all_chromosomes(db)
    except Exception as e:
        logger.error(f"Error fetching chromosomes: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/resolve", response_model=SyntenyResolveResponse)
def resolve_synteny_target(
    gene: Optional[str] = None,
    sgdid: Optional[str] = None,
    source: str = "SGD",
    db: Session = Depends(get_db),
):
    """
    Resolve an external (e.g. SGD) gene identifier to the Candida ortholog(s)
    whose synteny neighborhood to open.

    Backs the SGD -> CGD synteny cross-link, e.g.
    ``/api/synteny/resolve?gene=HOG1&source=SGD`` or ``?sgdid=S000004103``.
    Accepts a gene name, SGDID, or ORF/systematic name (the last is resolved via
    the SGD API). Returns status ``one`` (load ``target``), ``many`` (show
    ``candidates``), or ``none`` (show ``message``).
    """
    identifier = (sgdid or gene or "").strip()
    if not identifier:
        raise HTTPException(
            status_code=400, detail="A 'gene' or 'sgdid' parameter is required"
        )
    try:
        return synteny_service.resolve_synteny_target(db, identifier, source)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error resolving synteny target for {identifier}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/chromosome/{name}", response_model=ChromosomeGenesResponse)
def get_chromosome_genes(
    name: str,
    window_start: Optional[int] = None,
    window_end: Optional[int] = None,
    db: Session = Depends(get_db),
):
    """
    Get genes on a chromosome with ortholog information.

    Args:
        name: Chromosome feature name (e.g., 'Ca22chr1A_C_albicans_SC5314')
        window_start: Optional start coordinate to filter genes
        window_end: Optional end coordinate to filter genes

    Returns:
        ChromosomeGenesResponse with genes and their CGOB ortholog cluster IDs.
        Used by the genome synteny browser to display genes on a chromosome track.
    """
    try:
        return genome_synteny_service.get_chromosome_genes(
            db, name, window_start, window_end
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error fetching chromosome genes for {name}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
