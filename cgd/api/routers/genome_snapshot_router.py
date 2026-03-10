"""
Genome Snapshot API Router.
"""
import logging
import traceback
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.schemas.genome_snapshot_schema import (
    GenomeSnapshotResponse,
    GenomeSnapshotListResponse,
    GoSlimDistributionResponse,
)
from cgd.api.services.genome_snapshot_service import (
    get_available_organisms,
    get_genome_snapshot,
    get_go_slim_distribution,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/genome-snapshot", tags=["genome-snapshot"])


@router.get("/organisms", response_model=GenomeSnapshotListResponse)
def list_organisms(db: Session = Depends(get_db)):
    """
    Get list of organisms available for genome snapshot.

    Returns list of organism abbreviations and names.
    """
    try:
        return get_available_organisms(db)
    except Exception as e:
        logger.error(f"Error listing organisms: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{organism_abbrev}", response_model=GenomeSnapshotResponse)
def get_snapshot(
    organism_abbrev: str,
    db: Session = Depends(get_db),
):
    """
    Get genome snapshot statistics for a specific organism.

    Args:
        organism_abbrev: Organism abbreviation (e.g., C_albicans_SC5314)

    Returns:
        Genome statistics including ORF counts, GO annotations, etc.
    """
    try:
        result = get_genome_snapshot(db, organism_abbrev)
        if not result.success:
            raise HTTPException(status_code=404, detail=result.error)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting genome snapshot for {organism_abbrev}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{organism_abbrev}/go-slim", response_model=GoSlimDistributionResponse)
def get_go_slim(
    organism_abbrev: str,
    db: Session = Depends(get_db),
):
    """
    Get GO Slim distribution data for genome snapshot visualization.

    Args:
        organism_abbrev: Organism abbreviation (e.g., C_albicans_SC5314)

    Returns:
        GO Slim distribution for Molecular Function, Cellular Component,
        and Biological Process aspects.
    """
    try:
        result = get_go_slim_distribution(db, organism_abbrev)
        if not result.success:
            raise HTTPException(status_code=404, detail=result.error)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting GO Slim distribution for {organism_abbrev}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
