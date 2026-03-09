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
    GoAnnotation,
    Go,
    Seq,
)
from cgd.schemas.genome_snapshot_schema import (
    GenomeSnapshotResponse,
    GenomeSnapshotListResponse,
    GoAnnotationCounts,
)

logger = logging.getLogger(__name__)


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
    """Get ORF counts by qualifier."""
    # Total ORFs
    total = (
        db.query(func.count(Feature.feature_no))
        .filter(
            Feature.organism_no == organism_no,
            Feature.feature_type == "ORF",
        )
        .scalar() or 0
    )

    # Get qualifier counts
    qualifier_counts = (
        db.query(
            FeatProperty.property_value,
            func.count(distinct(Feature.feature_no))
        )
        .join(Feature, FeatProperty.feature_no == Feature.feature_no)
        .filter(
            Feature.organism_no == organism_no,
            Feature.feature_type == "ORF",
            FeatProperty.property_type == "feature_qualifier",
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
        if qualifier:
            qual_lower = qualifier.lower()
            if "verified" in qual_lower:
                counts["verified"] += count
            elif "uncharacterized" in qual_lower:
                counts["uncharacterized"] += count
            elif "dubious" in qual_lower:
                counts["dubious"] += count

    return counts


def _get_trna_count(db: Session, organism_no: int) -> int:
    """Get tRNA gene count."""
    count = (
        db.query(func.count(Feature.feature_no))
        .filter(
            Feature.organism_no == organism_no,
            Feature.feature_type == "tRNA",
        )
        .scalar() or 0
    )
    return count


def _get_chromosomes_and_length(db: Session, organism_no: int) -> tuple:
    """Get chromosome list and total genome length."""
    # Get chromosomes
    chromosomes = (
        db.query(Feature.feature_name)
        .filter(
            Feature.organism_no == organism_no,
            Feature.feature_type == "chromosome",
        )
        .order_by(Feature.feature_name)
        .all()
    )

    chromosome_names = [c[0] for c in chromosomes]

    # Get genome length from Seq table
    genome_length = (
        db.query(func.sum(Seq.seq_length))
        .join(Feature, Seq.feature_no == Feature.feature_no)
        .filter(
            Feature.organism_no == organism_no,
            Feature.feature_type == "chromosome",
            Seq.is_seq_current == "Y",
            Seq.seq_type == "Genomic",
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
