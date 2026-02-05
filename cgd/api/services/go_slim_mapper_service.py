"""
GO Slim Mapper Service - Map genes to predefined GO Slim categories.

Maps a list of genes to broader GO Slim terms via direct or indirect
(ancestor) annotations, without statistical enrichment analysis.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Optional

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

from cgd.models.models import (
    Alias,
    Feature,
    FeatAlias,
    Go,
    GoAnnotation,
    GoPath,
    GoSet,
    Organism,
)
from cgd.schemas.go_slim_mapper_schema import (
    AnnotationTypeOption,
    GoSlimMapperConfigResponse,
    GoSlimMapperRequest,
    GoSlimMapperResponse,
    GoSlimMapperResult,
    GoSlimSet,
    GoSlimSetDetail,
    GoSlimTerm,
    MappedGene,
    MappedSlimTerm,
    OrganismOption,
)


# Map GO aspect codes to full names
ASPECT_NAMES = {
    "C": "Cellular Component",
    "F": "Molecular Function",
    "P": "Biological Process",
}

# Map database annotation_type values to API format
ANNOTATION_TYPE_MAP = {
    "manually curated": "manually_curated",
    "high-throughput": "high_throughput",
    "computational": "computational",
}


def _format_goid(goid: int) -> str:
    """Format GOID as GO:XXXXXXX (7-digit padded)."""
    return f"GO:{goid:07d}"


def _chunk_list(lst: list, chunk_size: int = 900) -> list[list]:
    """Split a list into chunks of specified size (default 900 for Oracle's 1000 limit)."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def get_go_slim_mapper_config(db: Session) -> GoSlimMapperConfigResponse:
    """
    Get configuration options for GO Slim Mapper.

    Returns organisms, available GO Slim sets, and annotation types.
    """
    # Get organisms with GO annotations
    organisms_with_go = (
        db.query(Organism)
        .join(Feature, Feature.organism_no == Organism.organism_no)
        .join(GoAnnotation, GoAnnotation.feature_no == Feature.feature_no)
        .distinct()
        .order_by(Organism.organism_order)
        .all()
    )

    organism_options = [
        OrganismOption(
            organism_no=org.organism_no,
            organism_name=org.organism_name,
            display_name=org.organism_name,
        )
        for org in organisms_with_go
    ]

    # Get GO Slim sets
    go_slim_sets = get_go_slim_sets(db)

    # Annotation types
    annotation_types = [
        AnnotationTypeOption(value="manually_curated", label="Manually Curated"),
        AnnotationTypeOption(value="high_throughput", label="High-Throughput"),
        AnnotationTypeOption(value="computational", label="Computational"),
    ]

    return GoSlimMapperConfigResponse(
        organisms=organism_options,
        go_slim_sets=go_slim_sets,
        annotation_types=annotation_types,
    )


def get_go_slim_sets(db: Session) -> list[GoSlimSet]:
    """
    Get all available GO Slim sets with their aspects.

    Returns a list of GoSlimSet objects with set names and available aspects.
    """
    # Query distinct set names and their aspects
    results = (
        db.query(GoSet.go_set_name, Go.go_aspect)
        .join(Go, GoSet.go_no == Go.go_no)
        .distinct()
        .order_by(GoSet.go_set_name, Go.go_aspect)
        .all()
    )

    # Group by set name
    sets_dict: dict[str, list[str]] = defaultdict(list)
    for set_name, aspect in results:
        # Convert aspect to single letter code
        aspect_code = aspect[0].upper() if aspect else None
        if aspect_code and aspect_code not in sets_dict[set_name]:
            sets_dict[set_name].append(aspect_code)

    # Build response
    return [
        GoSlimSet(go_set_name=name, aspects=sorted(aspects))
        for name, aspects in sorted(sets_dict.items())
    ]


def get_slim_terms_for_set(
    db: Session,
    set_name: str,
    aspect: str,
) -> GoSlimSetDetail:
    """
    Get GO Slim terms for a specific set and aspect.

    Args:
        db: Database session
        set_name: Name of the GO Slim set
        aspect: GO aspect code (P, F, or C)

    Returns:
        GoSlimSetDetail with the list of terms
    """
    # Use single-letter aspect code directly (database stores P, F, C)
    aspect_code = aspect.upper()

    # Query terms in the set
    results = (
        db.query(Go.go_no, Go.goid, Go.go_term, Go.go_aspect)
        .join(GoSet, GoSet.go_no == Go.go_no)
        .filter(GoSet.go_set_name == set_name)
        .filter(Go.go_aspect == aspect_code)
        .order_by(Go.go_term)
        .all()
    )

    terms = [
        GoSlimTerm(
            go_no=go_no,
            goid=_format_goid(goid),
            go_term=go_term,
            go_aspect=go_aspect if go_aspect else aspect_code,
        )
        for go_no, goid, go_term, go_aspect in results
    ]

    return GoSlimSetDetail(
        go_set_name=set_name,
        go_aspect=aspect_code,
        terms=terms,
    )


def _validate_genes(
    db: Session,
    genes: list[str],
    organism_no: int,
) -> tuple[dict[int, MappedGene], list[str]]:
    """
    Validate a list of gene names/IDs against the database.

    Returns:
        tuple of (feature_no -> MappedGene dict, not_found list)
    """
    genes_upper = [g.strip().upper() for g in genes if g.strip()]
    gene_input_map = {g.strip().upper(): g.strip() for g in genes if g.strip()}

    if not genes_upper:
        return {}, list(gene_input_map.values())

    # Query features by feature_name or gene_name
    features_by_name = (
        db.query(Feature)
        .filter(Feature.organism_no == organism_no)
        .filter(
            or_(
                func.upper(Feature.feature_name).in_(genes_upper),
                func.upper(Feature.gene_name).in_(genes_upper),
            )
        )
        .all()
    )

    # Build result
    found_map: dict[str, Feature] = {}  # input_upper -> Feature
    for feature in features_by_name:
        fname_upper = feature.feature_name.upper() if feature.feature_name else None
        gname_upper = feature.gene_name.upper() if feature.gene_name else None

        if fname_upper in genes_upper:
            found_map[fname_upper] = feature
        if gname_upper and gname_upper in genes_upper:
            found_map[gname_upper] = feature

    # Query aliases for remaining genes
    remaining_genes = [g for g in genes_upper if g not in found_map]
    if remaining_genes:
        # Batch query aliases
        for chunk in _chunk_list(remaining_genes):
            alias_results = (
                db.query(Feature, Alias)
                .join(FeatAlias, FeatAlias.feature_no == Feature.feature_no)
                .join(Alias, Alias.alias_no == FeatAlias.alias_no)
                .filter(Feature.organism_no == organism_no)
                .filter(func.upper(Alias.alias_name).in_(chunk))
                .all()
            )
            for feature, alias in alias_results:
                alias_upper = alias.alias_name.upper() if alias.alias_name else None
                if alias_upper and alias_upper in remaining_genes:
                    found_map[alias_upper] = feature

    # Build response - deduplicate by feature_no
    result: dict[int, MappedGene] = {}
    not_found_inputs = []

    for gene_upper, original_input in gene_input_map.items():
        if gene_upper in found_map:
            feature = found_map[gene_upper]
            if feature.feature_no not in result:
                result[feature.feature_no] = MappedGene(
                    feature_no=feature.feature_no,
                    systematic_name=feature.feature_name,
                    gene_name=feature.gene_name,
                )
        else:
            not_found_inputs.append(original_input)

    return result, not_found_inputs


def _build_annotation_filters(annotation_types: Optional[list[str]]):
    """Build SQLAlchemy filter conditions for annotations."""
    filters = []

    if annotation_types:
        # Map API annotation types back to database values
        db_types = []
        type_reverse_map = {v: k for k, v in ANNOTATION_TYPE_MAP.items()}
        for api_type in annotation_types:
            if api_type in type_reverse_map:
                db_types.append(type_reverse_map[api_type])
            else:
                db_types.append(api_type)
        filters.append(GoAnnotation.annotation_type.in_(db_types))

    return filters


def _get_slim_term_go_nos(
    db: Session,
    set_name: str,
    aspect: str,
    selected_terms: Optional[list[str]] = None,
) -> set[int]:
    """
    Get go_no values for slim terms in a set.

    Args:
        db: Database session
        set_name: Name of the GO Slim set
        aspect: GO aspect code (P, F, or C)
        selected_terms: Optional list of specific GO IDs to include

    Returns:
        Set of go_no values
    """
    # Use single-letter aspect code directly (database stores P, F, C)
    aspect_code = aspect.upper()

    query = (
        db.query(Go.go_no, Go.goid)
        .join(GoSet, GoSet.go_no == Go.go_no)
        .filter(GoSet.go_set_name == set_name)
        .filter(Go.go_aspect == aspect_code)
    )

    results = query.all()

    if selected_terms:
        # Filter to selected terms only
        # Convert GO:XXXXXXX format to integer for comparison
        selected_goids = set()
        for term_id in selected_terms:
            if term_id.startswith("GO:"):
                try:
                    selected_goids.add(int(term_id[3:]))
                except ValueError:
                    pass
            else:
                try:
                    selected_goids.add(int(term_id))
                except ValueError:
                    pass

        return {go_no for go_no, goid in results if goid in selected_goids}

    return {go_no for go_no, goid in results}


def run_go_slim_mapper(
    db: Session,
    request: GoSlimMapperRequest,
) -> GoSlimMapperResponse:
    """
    Run GO Slim Mapper analysis.

    Maps genes to GO Slim terms via direct annotations or ancestors.

    Args:
        db: Database session
        request: Analysis request parameters

    Returns:
        GoSlimMapperResponse with mapped terms or error
    """
    warnings = []

    # Step 1: Validate genes
    gene_map, not_found_genes = _validate_genes(
        db, request.genes, request.organism_no
    )

    if not gene_map:
        return GoSlimMapperResponse(
            success=False,
            error="No valid genes found in the database",
            warnings=warnings,
        )

    # Step 2: Get organism name
    organism = db.query(Organism).filter(Organism.organism_no == request.organism_no).first()
    organism_name = organism.organism_name if organism else f"Organism {request.organism_no}"

    # Step 3: Get slim term go_nos
    slim_term_go_nos = _get_slim_term_go_nos(
        db,
        request.go_set_name,
        request.go_aspect,
        request.selected_terms,
    )

    if not slim_term_go_nos:
        return GoSlimMapperResponse(
            success=False,
            error=f"No GO Slim terms found for set '{request.go_set_name}' aspect '{request.go_aspect}'",
            warnings=warnings,
        )

    # Step 4: Get GO annotations for genes
    feature_nos = list(gene_map.keys())
    ann_filters = _build_annotation_filters(request.annotation_types)

    # Use single-letter aspect code directly (database stores P, F, C)
    aspect_code = request.go_aspect.upper()

    # Query direct annotations (batched)
    feature_to_go_nos: dict[int, set[int]] = defaultdict(set)
    for chunk in _chunk_list(feature_nos):
        query = (
            db.query(GoAnnotation.feature_no, GoAnnotation.go_no)
            .join(Go, Go.go_no == GoAnnotation.go_no)
            .filter(GoAnnotation.feature_no.in_(chunk))
            .filter(Go.go_aspect == aspect_code)
        )

        for f in ann_filters:
            query = query.filter(f)

        for feature_no, go_no in query.all():
            feature_to_go_nos[feature_no].add(go_no)

    # Step 5: Map genes to slim terms
    # For each gene, check if any of its annotations (or their ancestors) are slim terms
    all_annotation_go_nos = set()
    for go_nos in feature_to_go_nos.values():
        all_annotation_go_nos.update(go_nos)

    # Query ancestors for all annotations (batched)
    go_no_to_ancestors: dict[int, set[int]] = defaultdict(set)
    if all_annotation_go_nos:
        for chunk in _chunk_list(list(all_annotation_go_nos)):
            ancestor_results = (
                db.query(GoPath.child_go_no, GoPath.ancestor_go_no)
                .filter(GoPath.child_go_no.in_(chunk))
                .all()
            )
            for child_go_no, ancestor_go_no in ancestor_results:
                go_no_to_ancestors[child_go_no].add(ancestor_go_no)

    # Map genes to slim terms
    slim_term_to_genes: dict[int, set[int]] = defaultdict(set)  # slim_go_no -> set of feature_nos
    genes_with_go = set()
    genes_mapped_to_slim = set()

    for feature_no, go_nos in feature_to_go_nos.items():
        genes_with_go.add(feature_no)
        mapped = False

        for go_no in go_nos:
            # Check if direct annotation is a slim term
            if go_no in slim_term_go_nos:
                slim_term_to_genes[go_no].add(feature_no)
                mapped = True

            # Check if any ancestor is a slim term
            for ancestor_go_no in go_no_to_ancestors.get(go_no, set()):
                if ancestor_go_no in slim_term_go_nos:
                    slim_term_to_genes[ancestor_go_no].add(feature_no)
                    mapped = True

        if mapped:
            genes_mapped_to_slim.add(feature_no)

    # Step 6: Build result
    total_genes_with_go = len(genes_with_go)

    # Get slim term details
    slim_go_nos_list = list(slim_term_to_genes.keys())
    slim_term_details = {}
    if slim_go_nos_list:
        for chunk in _chunk_list(slim_go_nos_list):
            go_records = (
                db.query(Go)
                .filter(Go.go_no.in_(chunk))
                .all()
            )
            for go in go_records:
                slim_term_details[go.go_no] = go

    # Build mapped terms
    mapped_terms = []
    for slim_go_no in sorted(slim_term_to_genes.keys()):
        go = slim_term_details.get(slim_go_no)
        if not go:
            continue

        gene_feature_nos = slim_term_to_genes[slim_go_no]
        gene_count = len(gene_feature_nos)
        frequency = (gene_count / total_genes_with_go * 100) if total_genes_with_go > 0 else 0.0

        genes = [gene_map[fno] for fno in sorted(gene_feature_nos) if fno in gene_map]

        term_aspect = go.go_aspect if go.go_aspect else request.go_aspect.upper()

        mapped_terms.append(MappedSlimTerm(
            go_no=slim_go_no,
            goid=_format_goid(go.goid),
            go_term=go.go_term,
            go_aspect=term_aspect,
            gene_count=gene_count,
            total_genes=total_genes_with_go,
            frequency_percent=round(frequency, 2),
            genes=genes,
        ))

    # Sort by gene count descending
    mapped_terms.sort(key=lambda x: -x.gene_count)

    # Build "other" genes (have GO annotations but not mapped to any slim term)
    other_feature_nos = genes_with_go - genes_mapped_to_slim
    other_genes = [gene_map[fno] for fno in sorted(other_feature_nos) if fno in gene_map]

    # Build "not annotated" genes (no GO annotations)
    not_annotated_feature_nos = set(gene_map.keys()) - genes_with_go
    not_annotated_genes = [
        gene_map[fno] for fno in sorted(not_annotated_feature_nos) if fno in gene_map
    ]

    result = GoSlimMapperResult(
        query_genes_submitted=len(request.genes),
        query_genes_found=len(gene_map),
        query_genes_with_go=total_genes_with_go,
        query_genes_not_found=not_found_genes,
        organism_no=request.organism_no,
        organism_name=organism_name,
        go_set_name=request.go_set_name,
        go_aspect=request.go_aspect,
        annotation_types_used=request.annotation_types or [],
        mapped_terms=mapped_terms,
        other_genes=other_genes,
        not_annotated_genes=not_annotated_genes,
    )

    return GoSlimMapperResponse(
        success=True,
        result=result,
        warnings=warnings,
    )
