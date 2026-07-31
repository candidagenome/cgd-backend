"""
Site statistics service.

Provides database-wide totals for the Explore/landing page. Counts are
cached in-process for a short window so the landing page does not issue a
fresh set of COUNT(*) queries on every visit.
"""
import logging
import time
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from cgd.models.models import (
    Colleague,
    GoAnnotation,
    HomologyGroup,
    Interaction,
    PhenoAnnotation,
    Phenotype,
    Reference,
    Organism,
)
from cgd.api.services.genome_snapshot_service import _get_orf_counts
from cgd.schemas.stats_schema import StatsSummaryResponse

logger = logging.getLogger(__name__)

# Simple in-process TTL cache (totals move slowly; refresh at most hourly).
_CACHE_TTL_SECONDS = 3600
_cache: dict = {"data": None, "expires": 0.0}


def _count_total_genes(db: Session) -> int:
    """
    Sum haploid protein-coding ORFs across all strain-level organisms.

    Reuses the genome-snapshot ORF counter (current location/seq/version,
    excludes deleted) and halves the diploid C. albicans count, matching the
    per-species gene counts shown elsewhere in CGD.
    """
    organisms = (
        db.query(Organism)
        .filter(Organism.taxonomic_rank == "Strain")
        .all()
    )
    total = 0
    for org in organisms:
        try:
            orf_counts = _get_orf_counts(db, org.organism_no)
        except Exception as exc:  # noqa: BLE001 - one bad strain shouldn't break the total
            logger.warning("ORF count failed for %s: %s", org.organism_abbrev, exc)
            continue
        divisor = 2 if "albicans" in (org.organism_abbrev or "").lower() else 1
        total += orf_counts.get("total", 0) // divisor
    return total


def get_stats_summary(db: Session, refresh: bool = False) -> StatsSummaryResponse:
    """
    Return database-wide totals for the landing page.

    Args:
        db: Database session.
        refresh: When True, bypass the cache and recompute.
    """
    now = time.time()
    if not refresh and _cache["data"] is not None and now < _cache["expires"]:
        return _cache["data"]

    try:
        summary = StatsSummaryResponse(
            genes=_count_total_genes(db),
            references=db.query(func.count(Reference.reference_no)).scalar() or 0,
            phenotypes=db.query(func.count(Phenotype.phenotype_no)).scalar() or 0,
            phenotype_annotations=(
                db.query(func.count(PhenoAnnotation.pheno_annotation_no)).scalar() or 0
            ),
            go_annotations=db.query(func.count(GoAnnotation.go_annotation_no)).scalar() or 0,
            ortholog_clusters=(
                db.query(func.count(HomologyGroup.homology_group_no)).scalar() or 0
            ),
            interactions=db.query(func.count(Interaction.interaction_no)).scalar() or 0,
            colleagues=db.query(func.count(Colleague.colleague_no)).scalar() or 0,
            organisms=(
                db.query(func.count(Organism.organism_no))
                .filter(Organism.taxonomic_rank == "Strain")
                .scalar()
                or 0
            ),
            success=True,
        )
        _cache["data"] = summary
        _cache["expires"] = now + _CACHE_TTL_SECONDS
        return summary
    except Exception as exc:  # noqa: BLE001
        logger.error("Error computing stats summary: %s", exc)
        return StatsSummaryResponse(success=False, error=str(exc))
