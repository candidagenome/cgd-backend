"""
GO Annotation Summary Service - Generate GO annotation reports for gene lists.

Takes a list of genes and reports the frequency of GO term annotations,
comparing cluster frequency (genes in list) vs genome frequency (all genes).
"""
from __future__ import annotations

from collections import defaultdict
from typing import Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from cgd.models.models import (
    Alias,
    Feature,
    FeatAlias,
    Go,
    GoAnnotation,
    Organism,
)
from cgd.schemas.go_annotation_summary_schema import (
    AnnotatedGene,
    GoAnnotationSummaryRequest,
    GoAnnotationSummaryResponse,
    GoAnnotationSummaryResult,
    GoTermAnnotation,
)


def _format_goid(goid: int) -> str:
    """Format GOID as GO:XXXXXXX (7-digit padded)."""
    return f"GO:{goid:07d}"


def _chunk_list(lst: list, chunk_size: int = 900) -> list[list]:
    """Split a list into chunks of specified size (default 900 for Oracle's 1000 limit)."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def _validate_genes(
    db: Session,
    genes: list[str],
    organism_no: Optional[int] = None,
) -> tuple[dict[int, AnnotatedGene], list[str]]:
    """
    Validate a list of gene names/IDs against the database.

    Returns:
        tuple of (feature_no -> AnnotatedGene dict, not_found list)
    """
    genes_upper = [g.strip().upper() for g in genes if g.strip()]
    gene_input_map = {g.strip().upper(): g.strip() for g in genes if g.strip()}

    if not genes_upper:
        return {}, list(gene_input_map.values())

    # Query features by feature_name or gene_name (chunked to avoid Oracle 1000 limit)
    features_by_name = []
    for chunk in _chunk_list(genes_upper):
        query = db.query(Feature).filter(
            or_(
                func.upper(Feature.feature_name).in_(chunk),
                func.upper(Feature.gene_name).in_(chunk),
            )
        )
        if organism_no:
            query = query.filter(Feature.organism_no == organism_no)
        features_by_name.extend(query.all())

    # Build result
    found_map: dict[str, Feature] = {}  # input_upper -> Feature
    for feature in features_by_name:
        fname_upper = feature.feature_name.upper() if feature.feature_name else None
        gname_upper = feature.gene_name.upper() if feature.gene_name else None

        if fname_upper in genes_upper:
            found_map[fname_upper] = feature
        if gname_upper and gname_upper in genes_upper:
            found_map[gname_upper] = feature

    # Query aliases for remaining genes (chunked)
    remaining_genes = [g for g in genes_upper if g not in found_map]
    if remaining_genes:
        for chunk in _chunk_list(remaining_genes):
            query = (
                db.query(Feature, Alias)
                .join(FeatAlias, FeatAlias.feature_no == Feature.feature_no)
                .join(Alias, Alias.alias_no == FeatAlias.alias_no)
                .filter(func.upper(Alias.alias_name).in_(chunk))
            )
            if organism_no:
                query = query.filter(Feature.organism_no == organism_no)
            alias_results = query.all()
            for feature, alias in alias_results:
                alias_upper = alias.alias_name.upper() if alias.alias_name else None
                if alias_upper and alias_upper in remaining_genes:
                    found_map[alias_upper] = feature

    # Get organism names for found features
    organism_nos = list(set(f.organism_no for f in found_map.values()))
    organism_names = {}
    if organism_nos:
        for chunk in _chunk_list(organism_nos):
            orgs = db.query(Organism).filter(Organism.organism_no.in_(chunk)).all()
            for org in orgs:
                organism_names[org.organism_no] = org.organism_name

    # Build response - deduplicate by feature_no
    result: dict[int, AnnotatedGene] = {}
    not_found_inputs = []

    for gene_upper, original_input in gene_input_map.items():
        if gene_upper in found_map:
            feature = found_map[gene_upper]
            if feature.feature_no not in result:
                result[feature.feature_no] = AnnotatedGene(
                    feature_no=feature.feature_no,
                    systematic_name=feature.feature_name,
                    gene_name=feature.gene_name,
                    organism=organism_names.get(feature.organism_no),
                )
        else:
            not_found_inputs.append(original_input)

    return result, not_found_inputs


def run_go_annotation_summary(
    db: Session,
    request: GoAnnotationSummaryRequest,
) -> GoAnnotationSummaryResponse:
    """
    Generate GO Annotation Summary for a list of genes.

    Args:
        db: Database session
        request: Request with gene list

    Returns:
        GoAnnotationSummaryResponse with annotation frequencies
    """
    warnings = []

    # Step 1: Validate genes
    gene_map, not_found_genes = _validate_genes(
        db, request.genes, request.organism_no
    )

    if not gene_map:
        return GoAnnotationSummaryResponse(
            success=False,
            error="No valid genes found in the database",
            warnings=warnings,
        )

    # Step 2: Get organism info if specified
    organism_name = None
    if request.organism_no:
        organism = db.query(Organism).filter(
            Organism.organism_no == request.organism_no
        ).first()
        organism_name = organism.organism_name if organism else None

    # Step 3: Get total annotated genes in genome
    genome_total_query = (
        db.query(func.count(func.distinct(GoAnnotation.feature_no)))
    )
    if request.organism_no:
        genome_total_query = genome_total_query.join(
            Feature, Feature.feature_no == GoAnnotation.feature_no
        ).filter(Feature.organism_no == request.organism_no)
    genome_total = genome_total_query.scalar() or 0

    # Step 4: Get GO annotations for genes in list
    feature_nos = list(gene_map.keys())
    cluster_total = len(feature_nos)

    # Query direct annotations for genes (chunked)
    annotations_by_aspect: dict[str, dict[int, list[int]]] = {
        'P': defaultdict(list),  # go_no -> [feature_nos]
        'F': defaultdict(list),
        'C': defaultdict(list),
    }

    for chunk in _chunk_list(feature_nos):
        results = (
            db.query(GoAnnotation.feature_no, GoAnnotation.go_no, Go.go_aspect)
            .join(Go, Go.go_no == GoAnnotation.go_no)
            .filter(GoAnnotation.feature_no.in_(chunk))
            .all()
        )
        for feature_no, go_no, go_aspect in results:
            aspect = go_aspect[0].upper() if go_aspect else 'P'
            if aspect in annotations_by_aspect:
                annotations_by_aspect[aspect][go_no].append(feature_no)

    # Step 5: Get genome counts for each GO term
    all_go_nos = set()
    for aspect_data in annotations_by_aspect.values():
        all_go_nos.update(aspect_data.keys())

    genome_counts: dict[int, int] = {}
    if all_go_nos:
        for chunk in _chunk_list(list(all_go_nos)):
            count_query = (
                db.query(
                    GoAnnotation.go_no,
                    func.count(func.distinct(GoAnnotation.feature_no))
                )
                .filter(GoAnnotation.go_no.in_(chunk))
            )
            if request.organism_no:
                count_query = count_query.join(
                    Feature, Feature.feature_no == GoAnnotation.feature_no
                ).filter(Feature.organism_no == request.organism_no)
            count_query = count_query.group_by(GoAnnotation.go_no)
            for go_no, count in count_query.all():
                genome_counts[go_no] = count

    # Step 6: Get GO term details
    go_details: dict[int, Go] = {}
    if all_go_nos:
        for chunk in _chunk_list(list(all_go_nos)):
            go_records = db.query(Go).filter(Go.go_no.in_(chunk)).all()
            for go in go_records:
                go_details[go.go_no] = go

    # Step 7: Build result terms for each ontology
    def build_term_list(aspect: str) -> list[GoTermAnnotation]:
        terms = []
        for go_no, feature_no_list in annotations_by_aspect[aspect].items():
            go = go_details.get(go_no)
            if not go:
                continue

            cluster_count = len(set(feature_no_list))
            genome_count = genome_counts.get(go_no, 0)

            cluster_freq = (cluster_count / cluster_total * 100) if cluster_total > 0 else 0
            genome_freq = (genome_count / genome_total * 100) if genome_total > 0 else 0

            # Build gene list
            genes = [
                gene_map[fno] for fno in sorted(set(feature_no_list))
                if fno in gene_map
            ]

            terms.append(GoTermAnnotation(
                go_no=go_no,
                goid=_format_goid(go.goid),
                go_term=go.go_term,
                go_aspect=aspect,
                cluster_count=cluster_count,
                cluster_total=cluster_total,
                cluster_frequency=round(cluster_freq, 2),
                genome_count=genome_count,
                genome_total=genome_total,
                genome_frequency=round(genome_freq, 2),
                genes=genes,
            ))

        # Sort by cluster frequency descending
        terms.sort(key=lambda t: (-t.cluster_frequency, -t.cluster_count))
        return terms

    result = GoAnnotationSummaryResult(
        query_genes_submitted=len(request.genes),
        query_genes_found=len(gene_map),
        query_genes_not_found=not_found_genes,
        genome_annotated_genes=genome_total,
        organism_no=request.organism_no,
        organism_name=organism_name,
        process_terms=build_term_list('P'),
        function_terms=build_term_list('F'),
        component_terms=build_term_list('C'),
    )

    return GoAnnotationSummaryResponse(
        success=True,
        result=result,
        warnings=warnings,
    )
