"""
Site statistics service.

Provides database-wide totals for the Explore/landing page. Counts are
cached in-process for a short window so the landing page does not issue a
fresh set of COUNT(*) queries on every visit.
"""
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from cgd.models.models import (
    Colleague,
    Feature,
    FeatHomology,
    FeatInteract,
    GoAnnotation,
    HomologyGroup,
    Interaction,
    PhenoAnnotation,
    Phenotype,
    Reference,
    RefProperty,
    RefpropFeat,
    Organism,
)
from cgd.api.services.genome_snapshot_service import _get_orf_counts
from cgd.schemas.stats_schema import (
    CountsByOrganismResponse,
    OrganismCategoryCounts,
    RecentActivityResponse,
    StatsSummaryResponse,
)

logger = logging.getLogger(__name__)

# Simple in-process TTL cache (totals move slowly; refresh at most hourly).
_CACHE_TTL_SECONDS = 3600
_cache: dict = {"data": None, "expires": 0.0}
_by_org_cache: dict = {"data": None, "expires": 0.0}


def _haploid_gene_count(db: Session, org) -> int:
    """Haploid protein-coding ORF count for one organism (halves diploid albicans)."""
    try:
        orf_counts = _get_orf_counts(db, org.organism_no)
    except Exception as exc:  # noqa: BLE001
        logger.warning("ORF count failed for %s: %s", org.organism_abbrev, exc)
        return 0
    divisor = 2 if "albicans" in (org.organism_abbrev or "").lower() else 1
    return orf_counts.get("total", 0) // divisor


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
    return sum(_haploid_gene_count(db, org) for org in organisms)


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


def get_recent_activity(db: Session, days: int = 90) -> RecentActivityResponse:
    """Return creation counts for the Explore page's recent-activity panel."""
    cutoff = datetime.now() - timedelta(days=days)
    try:
        return RecentActivityResponse(
            days=days,
            references=(
                db.query(func.count(Reference.reference_no))
                .filter(Reference.date_created >= cutoff)
                .scalar()
                or 0
            ),
            phenotype_annotations=(
                db.query(func.count(PhenoAnnotation.pheno_annotation_no))
                .filter(PhenoAnnotation.date_created >= cutoff)
                .scalar()
                or 0
            ),
            ortholog_clusters=(
                db.query(func.count(HomologyGroup.homology_group_no))
                .filter(HomologyGroup.date_created >= cutoff)
                .scalar()
                or 0
            ),
            success=True,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Error computing recent activity: %s", exc)
        return RecentActivityResponse(days=days, success=False, error=str(exc))


def _counts_for_organism(db: Session, org) -> OrganismCategoryCounts:
    """Compute per-category totals scoped to a single organism via Feature.organism_no."""
    org_no = org.organism_no

    references = (
        db.query(func.count(func.distinct(RefProperty.reference_no)))
        .select_from(RefProperty)
        .join(RefpropFeat, RefpropFeat.ref_property_no == RefProperty.ref_property_no)
        .join(Feature, Feature.feature_no == RefpropFeat.feature_no)
        .filter(Feature.organism_no == org_no)
        .scalar()
    ) or 0

    go_annotations = (
        db.query(func.count(GoAnnotation.go_annotation_no))
        .join(Feature, Feature.feature_no == GoAnnotation.feature_no)
        .filter(Feature.organism_no == org_no)
        .scalar()
    ) or 0

    phenotype_annotations = (
        db.query(func.count(PhenoAnnotation.pheno_annotation_no))
        .join(Feature, Feature.feature_no == PhenoAnnotation.feature_no)
        .filter(Feature.organism_no == org_no)
        .scalar()
    ) or 0

    phenotypes = (
        db.query(func.count(func.distinct(PhenoAnnotation.phenotype_no)))
        .join(Feature, Feature.feature_no == PhenoAnnotation.feature_no)
        .filter(Feature.organism_no == org_no)
        .scalar()
    ) or 0

    ortholog_clusters = (
        db.query(func.count(func.distinct(FeatHomology.homology_group_no)))
        .join(Feature, Feature.feature_no == FeatHomology.feature_no)
        .filter(Feature.organism_no == org_no)
        .scalar()
    ) or 0

    interactions = (
        db.query(func.count(func.distinct(FeatInteract.interaction_no)))
        .join(Feature, Feature.feature_no == FeatInteract.feature_no)
        .filter(Feature.organism_no == org_no)
        .scalar()
    ) or 0

    return OrganismCategoryCounts(
        organism_abbrev=org.organism_abbrev,
        organism_name=org.organism_name,
        genes=_haploid_gene_count(db, org),
        references=references,
        phenotypes=phenotypes,
        phenotype_annotations=phenotype_annotations,
        go_annotations=go_annotations,
        ortholog_clusters=ortholog_clusters,
        interactions=interactions,
    )


def get_counts_by_organism(db: Session, refresh: bool = False) -> CountsByOrganismResponse:
    """
    Return per-organism category totals for the Explore-page organism filter.

    Cached in-process for up to an hour.
    """
    now = time.time()
    if not refresh and _by_org_cache["data"] is not None and now < _by_org_cache["expires"]:
        return _by_org_cache["data"]

    try:
        organisms = (
            db.query(Organism)
            .filter(Organism.taxonomic_rank == "Strain")
            .order_by(Organism.organism_order)
            .all()
        )
        by_organism = {
            org.organism_abbrev: _counts_for_organism(db, org).model_dump()
            for org in organisms
        }
        result = CountsByOrganismResponse(by_organism=by_organism, success=True)
        _by_org_cache["data"] = result
        _by_org_cache["expires"] = now + _CACHE_TTL_SECONDS
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("Error computing counts by organism: %s", exc)
        return CountsByOrganismResponse(success=False, error=str(exc))
