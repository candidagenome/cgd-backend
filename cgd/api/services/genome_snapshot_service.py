"""
Genome Snapshot Service.

Provides real-time genome statistics for the Genome Snapshot page.
"""
from __future__ import annotations

import logging
from typing import List, Dict, Optional, Set
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import func, distinct

# Oracle IN clause limit
ORACLE_IN_LIMIT = 999

from cgd.models.models import (
    Organism,
    Feature,
    FeatProperty,
    FeatLocation,
    GoAnnotation,
    Go,
    GoPath,
    GoSet,
    Seq,
    GenomeVersion,
)
from cgd.schemas.genome_snapshot_schema import (
    GenomeSnapshotResponse,
    GenomeSnapshotListResponse,
    GoAnnotationCounts,
    GoSlimCategory,
    GoSlimDistribution,
    GoSlimDistributionResponse,
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

    # Use chunked query to avoid Oracle's 1000 item IN clause limit
    feature_nos_list = list(feature_nos)
    all_matching = set()

    for i in range(0, len(feature_nos_list), ORACLE_IN_LIMIT):
        chunk = feature_nos_list[i:i + ORACLE_IN_LIMIT]
        matching = (
            db.query(FeatProperty.feature_no)
            .filter(
                FeatProperty.feature_no.in_(chunk),
                FeatProperty.property_type == "feature_qualifier",
                FeatProperty.property_value == qualifier,
            )
            .distinct()
            .all()
        )
        all_matching.update(f[0] for f in matching)

    return all_matching


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

        # Get all feature type counts
        feature_counts = _get_all_feature_counts(db, organism.organism_no)

        # Get chromosomes and genome length
        chromosomes, genome_length_bp = _get_chromosomes_and_length(db, organism.organism_no)

        # Get GO annotation counts
        go_counts = _get_go_annotation_counts(db, organism.organism_no)

        # Format genome length
        genome_length = f"{genome_length_bp:,} bp" if genome_length_bp > 0 else ""

        # Determine if diploid (C. albicans is diploid)
        is_diploid = "albicans" in organism_abbrev.lower()
        divisor = 2 if is_diploid else 1
        haploid_orfs = orf_counts["total"] // divisor

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
            trna_count=feature_counts["trna_count"],
            ltr_count=feature_counts["ltr_count"],
            snorna_count=feature_counts["snorna_count"],
            repeat_region_count=feature_counts["repeat_region_count"],
            retrotransposon_count=feature_counts["retrotransposon_count"],
            centromere_count=feature_counts["centromere_count"],
            pseudogene_count=feature_counts["pseudogene_count"],
            blocked_reading_frame_count=feature_counts["blocked_reading_frame_count"],
            snrna_count=feature_counts["snrna_count"],
            rrna_count=feature_counts["rrna_count"],
            ncrna_count=feature_counts["ncrna_count"],
            total_features=feature_counts["total_features"],
            chromosomes=chromosomes,
            genome_length=genome_length,
            genome_length_bp=genome_length_bp,
            chromosome_length=genome_length_bp,
            haploid_chromosome_length=genome_length_bp // divisor,
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
    # Use chunked query to avoid Oracle's 1000 item IN clause limit
    current_orf_nos_list = list(current_orf_nos)
    qualifier_results = {}

    for i in range(0, len(current_orf_nos_list), ORACLE_IN_LIMIT):
        chunk = current_orf_nos_list[i:i + ORACLE_IN_LIMIT]
        chunk_counts = (
            db.query(
                FeatProperty.property_value,
                func.count(distinct(FeatProperty.feature_no))
            )
            .filter(
                FeatProperty.feature_no.in_(chunk),
                FeatProperty.property_type == "feature_qualifier",
                FeatProperty.property_value.in_(["Dubious", "Verified", "Uncharacterized"]),
            )
            .group_by(FeatProperty.property_value)
            .all()
        )
        for qualifier, count in chunk_counts:
            qualifier_results[qualifier] = qualifier_results.get(qualifier, 0) + count

    qualifier_counts = list(qualifier_results.items())

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


def _get_feature_type_count(db: Session, organism_no: int, feature_type: str) -> int:
    """
    Get count for a specific feature type, matching the original Perl logic.

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

    # Main query: Get features that are on current location/seq/version and not deleted
    current_features = (
        db.query(Feature.feature_no)
        .join(FeatLocation, Feature.feature_no == FeatLocation.feature_no)
        .join(Seq, FeatLocation.root_seq_no == Seq.seq_no)
        .join(GenomeVersion, Seq.genome_version_no == GenomeVersion.genome_version_no)
        .filter(
            Feature.organism_no == organism_no,
            Feature.feature_type == feature_type,
            FeatLocation.is_loc_current == "Y",
            Seq.is_seq_current == "Y",
            GenomeVersion.is_ver_current == "Y",
            ~Feature.feature_no.in_(db.query(deleted_subquery.c.feature_no)),
        )
        .distinct()
        .all()
    )

    return len(current_features)


def _get_trna_count(db: Session, organism_no: int) -> int:
    """Get tRNA gene count."""
    return _get_feature_type_count(db, organism_no, "tRNA")


def _get_all_feature_counts(db: Session, organism_no: int) -> Dict[str, int]:
    """
    Get counts for all feature types.

    Returns a dictionary with counts for each feature type.
    """
    feature_types = [
        ("tRNA", "trna_count"),
        ("long_terminal_repeat", "ltr_count"),
        ("snoRNA", "snorna_count"),
        ("repeat_region", "repeat_region_count"),
        ("retrotransposon", "retrotransposon_count"),
        ("centromere", "centromere_count"),
        ("pseudogene", "pseudogene_count"),
        ("blocked_reading_frame", "blocked_reading_frame_count"),
        ("snRNA", "snrna_count"),
        ("rRNA", "rrna_count"),
        ("ncRNA", "ncrna_count"),
    ]

    counts = {}
    total = 0

    for feature_type, key in feature_types:
        count = _get_feature_type_count(db, organism_no, feature_type)
        counts[key] = count
        total += count

    # Add ORF counts to total
    orf_counts = _get_orf_counts(db, organism_no)
    total += orf_counts["total"]

    counts["total_features"] = total

    return counts


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


# GO Slim set name used for genome snapshot (note: uses hyphen like original Perl)
GENOME_SNAPSHOT_GO_SLIM_SET = "Candida GO-Slim"

# Aspect names mapping
ASPECT_NAMES = {
    "F": "Molecular Function",
    "C": "Cellular Component",
    "P": "Biological Process",
}


def _format_goid(goid: int) -> str:
    """Format GOID as GO:XXXXXXX (7-digit padded)."""
    return f"GO:{goid:07d}"


def get_go_slim_distribution(
    db: Session,
    organism_abbrev: str,
) -> GoSlimDistributionResponse:
    """
    Get GO Slim distribution for genome snapshot visualization.

    This returns the count of genes mapped to each GO Slim term for all three
    aspects (Molecular Function, Cellular Component, Biological Process).

    Args:
        db: Database session
        organism_abbrev: Organism abbreviation (e.g., C_albicans_SC5314)

    Returns:
        GoSlimDistributionResponse with distribution data for each aspect
    """
    try:
        # Get organism
        organism = (
            db.query(Organism)
            .filter(Organism.organism_abbrev == organism_abbrev)
            .first()
        )

        if not organism:
            return GoSlimDistributionResponse(
                success=False,
                organism_abbrev=organism_abbrev,
                organism_name="",
                error=f"Organism '{organism_abbrev}' not found",
            )

        organism_no = organism.organism_no

        # Get all feature_nos for this organism
        feature_nos = (
            db.query(Feature.feature_no)
            .filter(Feature.organism_no == organism_no)
            .all()
        )
        feature_no_set = set(f[0] for f in feature_nos)

        if not feature_no_set:
            return GoSlimDistributionResponse(
                success=False,
                organism_abbrev=organism_abbrev,
                organism_name=organism.organism_name,
                error="No features found for this organism",
            )

        # Get GO Slim terms from the Candida GO Slim set
        slim_terms = (
            db.query(Go.go_no, Go.goid, Go.go_term, Go.go_aspect)
            .join(GoSet, GoSet.go_no == Go.go_no)
            .filter(GoSet.go_set_name == GENOME_SNAPSHOT_GO_SLIM_SET)
            .all()
        )

        if not slim_terms:
            return GoSlimDistributionResponse(
                success=False,
                organism_abbrev=organism_abbrev,
                organism_name=organism.organism_name,
                error=f"No GO Slim terms found for set '{GENOME_SNAPSHOT_GO_SLIM_SET}'",
            )

        slim_go_nos = {t[0] for t in slim_terms}
        slim_term_map = {t[0]: (t[1], t[2], t[3]) for t in slim_terms}

        # Get all GO annotations for this organism's features
        # We need to chunk to avoid Oracle's 1000 item IN clause limit
        feature_no_list = list(feature_no_set)
        feature_go_annotations = []

        for i in range(0, len(feature_no_list), ORACLE_IN_LIMIT):
            chunk = feature_no_list[i:i + ORACLE_IN_LIMIT]
            annotations = (
                db.query(GoAnnotation.feature_no, GoAnnotation.go_no)
                .filter(GoAnnotation.feature_no.in_(chunk))
                .all()
            )
            feature_go_annotations.extend(annotations)

        # Get all unique go_nos from annotations
        annotation_go_nos = set(a[1] for a in feature_go_annotations)

        # Get ancestors for all annotation go_nos (to map to slim terms)
        go_to_ancestors = {}
        if annotation_go_nos:
            annotation_go_no_list = list(annotation_go_nos)
            for i in range(0, len(annotation_go_no_list), ORACLE_IN_LIMIT):
                chunk = annotation_go_no_list[i:i + ORACLE_IN_LIMIT]
                paths = (
                    db.query(GoPath.child_go_no, GoPath.ancestor_go_no)
                    .filter(GoPath.child_go_no.in_(chunk))
                    .all()
                )
                for child, ancestor in paths:
                    if child not in go_to_ancestors:
                        go_to_ancestors[child] = set()
                    go_to_ancestors[child].add(ancestor)

        # Map features to slim terms
        # For each feature, find which slim terms it maps to
        slim_term_counts = {aspect: {} for aspect in ["F", "C", "P"]}
        aspect_gene_sets = {aspect: set() for aspect in ["F", "C", "P"]}

        for feature_no, go_no in feature_go_annotations:
            # Get aspect for this go_no
            go_info = slim_term_map.get(go_no)
            if go_info:
                # Direct hit - annotation is a slim term
                _, _, aspect = go_info
                if aspect in slim_term_counts:
                    if go_no not in slim_term_counts[aspect]:
                        slim_term_counts[aspect][go_no] = set()
                    slim_term_counts[aspect][go_no].add(feature_no)
                    aspect_gene_sets[aspect].add(feature_no)

            # Check ancestors
            ancestors = go_to_ancestors.get(go_no, set())
            for ancestor_go_no in ancestors:
                if ancestor_go_no in slim_go_nos:
                    go_info = slim_term_map.get(ancestor_go_no)
                    if go_info:
                        _, _, aspect = go_info
                        if aspect in slim_term_counts:
                            if ancestor_go_no not in slim_term_counts[aspect]:
                                slim_term_counts[aspect][ancestor_go_no] = set()
                            slim_term_counts[aspect][ancestor_go_no].add(feature_no)
                            aspect_gene_sets[aspect].add(feature_no)

        # Root terms to exclude (like original Perl code)
        ROOT_TERMS = {
            "cellular_component",
            "molecular_function",
            "biological_process",
        }

        # Get total number of features with GO annotations for percentage calc
        all_annotated_features = set()
        for feature_no, _ in feature_go_annotations:
            all_annotated_features.add(feature_no)
        total_annotated_features = len(all_annotated_features)

        # Build response
        distributions = {}
        for aspect, aspect_name in ASPECT_NAMES.items():
            categories = []
            for go_no, feature_nos_set in slim_term_counts[aspect].items():
                goid, go_term, _ = slim_term_map[go_no]

                # Skip root terms (like original Perl code)
                term_lower = go_term.lower().replace(" ", "_")
                if term_lower in ROOT_TERMS:
                    continue

                count = len(feature_nos_set)
                # Calculate percentage of total annotated genes
                percentage = 0.0
                if total_annotated_features > 0:
                    percentage = round((count / total_annotated_features) * 100, 1)

                categories.append(GoSlimCategory(
                    go_term=go_term,
                    goid=_format_goid(goid),
                    count=count,
                    percentage=percentage,
                ))

            # Sort by percentage descending (like original Perl code)
            categories.sort(key=lambda x: -x.percentage)

            distributions[aspect] = GoSlimDistribution(
                aspect=aspect,
                aspect_name=aspect_name,
                categories=categories,
                total_genes=len(aspect_gene_sets[aspect]),
            )

        return GoSlimDistributionResponse(
            success=True,
            organism_abbrev=organism_abbrev,
            organism_name=organism.organism_name,
            molecular_function=distributions.get("F"),
            cellular_component=distributions.get("C"),
            biological_process=distributions.get("P"),
        )

    except Exception as e:
        logger.error(f"Error getting GO Slim distribution for {organism_abbrev}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return GoSlimDistributionResponse(
            success=False,
            organism_abbrev=organism_abbrev,
            organism_name="",
            error=str(e),
        )
