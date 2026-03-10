"""
Genome Snapshot Service.

Provides real-time genome statistics for the Genome Snapshot page.
"""
from __future__ import annotations

import logging
from typing import List, Dict, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import func, distinct

from cgd.models.models import (
    Organism,
    Feature,
    FeatProperty,
    FeatLocation,
    GoAnnotation,
    Go,
    Seq,
    GenomeVersion,
)
from cgd.schemas.genome_snapshot_schema import (
    GenomeSnapshotResponse,
    GenomeSnapshotListResponse,
    GoAnnotationCounts,
)

logger = logging.getLogger(__name__)


def get_current_feature_nos(
    db: Session,
    organism_no: int,
    feature_type: str = None,
) -> set:
    """
    Get feature numbers for current features (excluding deleted).

    This is the shared logic used by both genome_snapshot and feature_search
    to ensure consistent counts.

    Args:
        db: Database session
        organism_no: Organism number
        feature_type: Optional feature type filter (e.g., "ORF", "tRNA")

    Returns:
        Set of feature_no values
    """
    # Subquery: Get deleted feature_nos
    deleted_subquery = (
        db.query(FeatProperty.feature_no)
        .filter(FeatProperty.property_value.like("Deleted%"))
        .subquery()
    )

    # Base query with current location/seq/version filters
    query = (
        db.query(Feature.feature_no)
        .join(FeatLocation, Feature.feature_no == FeatLocation.feature_no)
        .join(Seq, FeatLocation.root_seq_no == Seq.seq_no)
        .join(GenomeVersion, Seq.genome_version_no == GenomeVersion.genome_version_no)
        .filter(
            Feature.organism_no == organism_no,
            FeatLocation.is_loc_current == "Y",
            Seq.is_seq_current == "Y",
            GenomeVersion.is_ver_current == "Y",
            ~Feature.feature_no.in_(db.query(deleted_subquery.c.feature_no)),
        )
    )

    if feature_type:
        query = query.filter(Feature.feature_type == feature_type)

    return set(f[0] for f in query.distinct().all())


def get_features_with_qualifier(
    db: Session,
    feature_nos: set,
    qualifier: str,
) -> set:
    """
    Get feature numbers that have a specific qualifier.

    Args:
        db: Database session
        feature_nos: Set of feature numbers to filter
        qualifier: Qualifier value (e.g., "Verified", "Uncharacterized")

    Returns:
        Set of feature_no values that have the qualifier
    """
    if not feature_nos:
        return set()

    matching = (
        db.query(FeatProperty.feature_no)
        .filter(
            FeatProperty.feature_no.in_(feature_nos),
            FeatProperty.property_type == "feature_qualifier",
            FeatProperty.property_value == qualifier,
        )
        .distinct()
        .all()
    )
    return set(f[0] for f in matching)


def get_available_organisms(db: Session) -> GenomeSnapshotListResponse:
    """
    Get list of organisms available for genome snapshot.

    Returns organisms with taxonomic_rank = 'Strain'.
    """
    try:
        organisms = (
            db.query(Organism)
            .filter(Organism.taxonomic_rank == "Strain")
            .order_by(Organism.organism_order)
            .all()
        )

        organism_list = []
        for org in organisms:
            organism_list.append({
                "organism_abbrev": org.organism_abbrev,
                "organism_name": org.organism_name,
            })

        return GenomeSnapshotListResponse(
            success=True,
            organisms=organism_list,
        )
    except Exception as e:
        logger.error(f"Error getting available organisms: {e}")
        return GenomeSnapshotListResponse(
            success=False,
            organisms=[],
            error=str(e),
        )


def get_genome_snapshot(db: Session, organism_abbrev: str) -> GenomeSnapshotResponse:
    """
    Get genome snapshot statistics for a specific organism.

    Args:
        db: Database session
        organism_abbrev: Organism abbreviation (e.g., C_albicans_SC5314)

    Returns:
        GenomeSnapshotResponse with all statistics
    """
    try:
        # Get organism
        organism = (
            db.query(Organism)
            .filter(Organism.organism_abbrev == organism_abbrev)
            .first()
        )

        if not organism:
            return GenomeSnapshotResponse(
                success=False,
                organism_abbrev=organism_abbrev,
                organism_name="",
                strain="",
                error=f"Organism '{organism_abbrev}' not found",
            )

        # Parse organism name and strain
        # Format is usually "Candida albicans SC5314" -> name="Candida albicans", strain="SC5314"
        name_parts = organism.organism_name.rsplit(" ", 1)
        organism_name = name_parts[0] if len(name_parts) > 1 else organism.organism_name
        strain = name_parts[1] if len(name_parts) > 1 else ""

        # Get ORF counts
        orf_counts = _get_orf_counts(db, organism.organism_no)

        # Get tRNA count
        trna_count = _get_trna_count(db, organism.organism_no)

        # Get chromosomes and genome length
        chromosomes, genome_length_bp = _get_chromosomes_and_length(db, organism.organism_no)

        # Get GO annotation counts
        go_counts = _get_go_annotation_counts(db, organism.organism_no)

        # Format genome length
        genome_length = f"{genome_length_bp:,} bp" if genome_length_bp > 0 else ""

        # Determine if diploid (C. albicans is diploid)
        is_diploid = "albicans" in organism_abbrev.lower()
        haploid_orfs = orf_counts["total"] // 2 if is_diploid else orf_counts["total"]

        return GenomeSnapshotResponse(
            success=True,
            organism_abbrev=organism_abbrev,
            organism_name=organism_name,
            strain=strain,
            last_updated=datetime.now().strftime("%B %d, %Y"),
            total_orfs=orf_counts["total"],
            haploid_orfs=haploid_orfs,
            verified_orfs=orf_counts["verified"],
            uncharacterized_orfs=orf_counts["uncharacterized"],
            dubious_orfs=orf_counts["dubious"],
            trna_count=trna_count,
            chromosomes=chromosomes,
            genome_length=genome_length,
            genome_length_bp=genome_length_bp,
            go_annotations=go_counts,
        )

    except Exception as e:
        logger.error(f"Error getting genome snapshot for {organism_abbrev}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return GenomeSnapshotResponse(
            success=False,
            organism_abbrev=organism_abbrev,
            organism_name="",
            strain="",
            error=str(e),
        )


def _get_orf_counts(db: Session, organism_no: int) -> Dict[str, int]:
    """
    Get ORF counts by qualifier, matching the original Perl logic.

    The original Perl query filters by:
    1. is_loc_current = 'Y' - Only count features with current location
    2. is_seq_current = 'Y' - Only count features on current sequence
    3. is_ver_current = 'Y' - Only count features on current genome version
    4. Excludes features with 'Deleted%' qualifier
    5. Uses exact qualifier matching for 'Dubious', 'Verified', 'Uncharacterized'
    """
    # Subquery: Get deleted feature_nos
    deleted_subquery = (
        db.query(FeatProperty.feature_no)
        .filter(FeatProperty.property_value.like("Deleted%"))
        .subquery()
    )

    # Main query: Get ORFs that are on current location/seq/version and not deleted
    # This matches the Perl logic:
    # - Join feature -> feat_location (is_loc_current='Y')
    # - Join feat_location -> seq (via root_seq_no, is_seq_current='Y')
    # - Join seq -> genome_version (is_ver_current='Y')
    current_orfs = (
        db.query(Feature.feature_no)
        .join(FeatLocation, Feature.feature_no == FeatLocation.feature_no)
        .join(Seq, FeatLocation.root_seq_no == Seq.seq_no)
        .join(GenomeVersion, Seq.genome_version_no == GenomeVersion.genome_version_no)
        .filter(
            Feature.organism_no == organism_no,
            Feature.feature_type == "ORF",
            FeatLocation.is_loc_current == "Y",
            Seq.is_seq_current == "Y",
            GenomeVersion.is_ver_current == "Y",
            ~Feature.feature_no.in_(db.query(deleted_subquery.c.feature_no)),
        )
        .distinct()
        .all()
    )
    current_orf_nos = set(f[0] for f in current_orfs)
    total = len(current_orf_nos)

    if not current_orf_nos:
        return {
            "total": 0,
            "verified": 0,
            "uncharacterized": 0,
            "dubious": 0,
        }

    # Get qualifier counts using EXACT matching (not substring)
    # Only count 'Dubious', 'Verified', 'Uncharacterized' - the exact values
    qualifier_counts = (
        db.query(
            FeatProperty.property_value,
            func.count(distinct(FeatProperty.feature_no))
        )
        .filter(
            FeatProperty.feature_no.in_(current_orf_nos),
            FeatProperty.property_type == "feature_qualifier",
            FeatProperty.property_value.in_(["Dubious", "Verified", "Uncharacterized"]),
        )
        .group_by(FeatProperty.property_value)
        .all()
    )

    counts = {
        "total": total,
        "verified": 0,
        "uncharacterized": 0,
        "dubious": 0,
    }

    for qualifier, count in qualifier_counts:
        if qualifier == "Verified":
            counts["verified"] = count
        elif qualifier == "Uncharacterized":
            counts["uncharacterized"] = count
        elif qualifier == "Dubious":
            counts["dubious"] = count

    return counts


def _get_trna_count(db: Session, organism_no: int) -> int:
    """
    Get tRNA gene count, matching the original Perl logic.

    Filters by:
    1. is_loc_current = 'Y' - Only count features with current location
    2. is_seq_current = 'Y' - Only count features on current sequence
    3. is_ver_current = 'Y' - Only count features on current genome version
    4. Excludes features with 'Deleted%' qualifier
    """
    # Subquery: Get deleted feature_nos
    deleted_subquery = (
        db.query(FeatProperty.feature_no)
        .filter(FeatProperty.property_value.like("Deleted%"))
        .subquery()
    )

    # Main query: Get tRNAs that are on current location/seq/version and not deleted
    current_trnas = (
        db.query(Feature.feature_no)
        .join(FeatLocation, Feature.feature_no == FeatLocation.feature_no)
        .join(Seq, FeatLocation.root_seq_no == Seq.seq_no)
        .join(GenomeVersion, Seq.genome_version_no == GenomeVersion.genome_version_no)
        .filter(
            Feature.organism_no == organism_no,
            Feature.feature_type == "tRNA",
            FeatLocation.is_loc_current == "Y",
            Seq.is_seq_current == "Y",
            GenomeVersion.is_ver_current == "Y",
            ~Feature.feature_no.in_(db.query(deleted_subquery.c.feature_no)),
        )
        .distinct()
        .all()
    )

    return len(current_trnas)


def _get_chromosomes_and_length(db: Session, organism_no: int) -> tuple:
    """
    Get chromosome list and total genome length.

    Matches the Perl logic by filtering for:
    - is_seq_current = 'Y'
    - is_ver_current = 'Y'
    """
    # Get chromosomes with current sequences on current genome version
    chromosomes = (
        db.query(Feature.feature_name)
        .join(Seq, Feature.feature_no == Seq.feature_no)
        .join(GenomeVersion, Seq.genome_version_no == GenomeVersion.genome_version_no)
        .filter(
            Feature.organism_no == organism_no,
            Feature.feature_type == "chromosome",
            Seq.is_seq_current == "Y",
            GenomeVersion.is_ver_current == "Y",
        )
        .order_by(Feature.feature_name)
        .distinct()
        .all()
    )

    chromosome_names = [c[0] for c in chromosomes]

    # Get genome length from Seq table with current version
    genome_length = (
        db.query(func.sum(Seq.seq_length))
        .join(Feature, Seq.feature_no == Feature.feature_no)
        .join(GenomeVersion, Seq.genome_version_no == GenomeVersion.genome_version_no)
        .filter(
            Feature.organism_no == organism_no,
            Feature.feature_type == "chromosome",
            Seq.is_seq_current == "Y",
            Seq.seq_type == "Genomic",
            GenomeVersion.is_ver_current == "Y",
        )
        .scalar() or 0
    )

    return chromosome_names, int(genome_length)


def _get_go_annotation_counts(db: Session, organism_no: int) -> GoAnnotationCounts:
    """Get GO annotation counts by aspect."""
    # Get feature numbers for this organism
    feature_nos_subquery = (
        db.query(Feature.feature_no)
        .filter(Feature.organism_no == organism_no)
        .subquery()
    )

    # Count distinct genes with GO annotations by aspect
    aspect_counts = (
        db.query(
            Go.go_aspect,
            func.count(distinct(GoAnnotation.feature_no))
        )
        .join(Go, GoAnnotation.go_no == Go.go_no)
        .filter(GoAnnotation.feature_no.in_(
            db.query(feature_nos_subquery.c.feature_no)
        ))
        .group_by(Go.go_aspect)
        .all()
    )

    counts = GoAnnotationCounts()
    total = 0

    for aspect, count in aspect_counts:
        if aspect == "F":
            counts.molecular_function = count
        elif aspect == "C":
            counts.cellular_component = count
        elif aspect == "P":
            counts.biological_process = count
        total += count

    counts.total = total

    return counts
