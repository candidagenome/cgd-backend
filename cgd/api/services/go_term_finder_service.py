"""
GO Term Finder Service - Gene Ontology enrichment analysis.

Performs hypergeometric test for GO term enrichment with optional
multiple testing correction (Bonferroni or Benjamini-Hochberg FDR).
"""
from __future__ import annotations

from collections import defaultdict
from typing import Optional

from scipy.stats import hypergeom
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session, joinedload

from cgd.models.models import (
    Alias,
    Code,
    Feature,
    FeatAlias,
    Go,
    GoAnnotation,
    GoPath,
    Organism,
)
from cgd.schemas.go_term_finder_schema import (
    AnnotationTypeOption,
    EnrichedGoTerm,
    EvidenceCodeOption,
    GeneHit,
    GoEnrichmentGraphEdge,
    GoEnrichmentGraphNode,
    GoEnrichmentGraphResponse,
    GoOntology,
    GoTermFinderConfigResponse,
    GoTermFinderRequest,
    GoTermFinderResponse,
    GoTermFinderResult,
    MultipleCorrectionMethod,
    OrganismOption,
    ValidatedGene,
    ValidateGenesRequest,
    ValidateGenesResponse,
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


def _normalize_annotation_type(db_type: str) -> str:
    """Normalize database annotation type to API format."""
    if not db_type:
        return "manually_curated"
    return ANNOTATION_TYPE_MAP.get(db_type, db_type.replace(" ", "_").replace("-", "_"))


def get_go_term_finder_config(db: Session) -> GoTermFinderConfigResponse:
    """
    Get configuration options for GO Term Finder.

    Returns organisms, evidence codes, annotation types, and default settings.
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

    # Get evidence codes from Code table
    evidence_code_records = (
        db.query(Code)
        .filter(Code.tab_name == "GO_ANNOTATION")
        .filter(Code.col_name == "GO_EVIDENCE")
        .order_by(Code.code_value)
        .all()
    )

    evidence_codes = [
        EvidenceCodeOption(
            code=code.code_value,
            description=code.description or code.code_value,
        )
        for code in evidence_code_records
    ]

    # Annotation types
    annotation_types = [
        AnnotationTypeOption(value="manually_curated", label="Manually Curated"),
        AnnotationTypeOption(value="high_throughput", label="High-Throughput"),
        AnnotationTypeOption(value="computational", label="Computational"),
    ]

    return GoTermFinderConfigResponse(
        organisms=organism_options,
        evidence_codes=evidence_codes,
        annotation_types=annotation_types,
    )


def validate_genes(
    db: Session,
    request: ValidateGenesRequest,
) -> ValidateGenesResponse:
    """
    Validate a list of gene names/IDs against the database.

    Performs case-insensitive matching on:
    - feature_name (systematic name)
    - gene_name (standard name)
    - aliases
    """
    genes_upper = [g.strip().upper() for g in request.genes if g.strip()]
    gene_input_map = {g.strip().upper(): g.strip() for g in request.genes if g.strip()}

    if not genes_upper:
        return ValidateGenesResponse(
            found=[],
            not_found=list(gene_input_map.values()),
            total_submitted=len(request.genes),
            total_found=0,
            total_with_go=0,
        )

    # Query features by feature_name or gene_name (chunked to avoid Oracle 1000 limit)
    features_by_name = []
    for chunk in _chunk_list(genes_upper):
        chunk_results = (
            db.query(Feature)
            .filter(Feature.organism_no == request.organism_no)
            .filter(
                or_(
                    func.upper(Feature.feature_name).in_(chunk),
                    func.upper(Feature.gene_name).in_(chunk),
                )
            )
            .all()
        )
        features_by_name.extend(chunk_results)

    # Build result
    found_map: dict[str, Feature] = {}  # input_upper -> Feature
    for feature in features_by_name:
        fname_upper = feature.feature_name.upper() if feature.feature_name else None
        gname_upper = feature.gene_name.upper() if feature.gene_name else None

        if fname_upper in genes_upper:
            found_map[fname_upper] = feature
        if gname_upper and gname_upper in genes_upper:
            found_map[gname_upper] = feature

    # Query aliases for remaining genes (chunked to avoid Oracle 1000 limit)
    remaining_genes = [g for g in genes_upper if g not in found_map]
    if remaining_genes:
        alias_results = []
        for chunk in _chunk_list(remaining_genes):
            chunk_results = (
                db.query(Feature, Alias)
                .join(FeatAlias, FeatAlias.feature_no == Feature.feature_no)
                .join(Alias, Alias.alias_no == FeatAlias.alias_no)
                .filter(Feature.organism_no == request.organism_no)
                .filter(func.upper(Alias.alias_name).in_(chunk))
                .all()
            )
            alias_results.extend(chunk_results)
        for feature, alias in alias_results:
            alias_upper = alias.alias_name.upper() if alias.alias_name else None
            if alias_upper and alias_upper in remaining_genes:
                found_map[alias_upper] = feature

    # Get GO annotation status for found features (chunked to avoid Oracle 1000 limit)
    feature_nos = list(set(f.feature_no for f in found_map.values()))
    features_with_go = set()
    if feature_nos:
        for chunk in _chunk_list(feature_nos):
            go_check = (
                db.query(GoAnnotation.feature_no)
                .filter(GoAnnotation.feature_no.in_(chunk))
                .distinct()
                .all()
            )
            features_with_go.update(row.feature_no for row in go_check)

    # Build response
    found_genes = []
    not_found_inputs = []

    for gene_upper, original_input in gene_input_map.items():
        if gene_upper in found_map:
            feature = found_map[gene_upper]
            found_genes.append(ValidatedGene(
                input_name=original_input,
                feature_no=feature.feature_no,
                systematic_name=feature.feature_name,
                gene_name=feature.gene_name,
                has_go_annotations=feature.feature_no in features_with_go,
            ))
        else:
            not_found_inputs.append(original_input)

    # Deduplicate found genes by feature_no
    seen_feature_nos = set()
    unique_found = []
    for gene in found_genes:
        if gene.feature_no not in seen_feature_nos:
            seen_feature_nos.add(gene.feature_no)
            unique_found.append(gene)

    total_with_go = sum(1 for g in unique_found if g.has_go_annotations)

    return ValidateGenesResponse(
        found=unique_found,
        not_found=not_found_inputs,
        total_submitted=len(request.genes),
        total_found=len(unique_found),
        total_with_go=total_with_go,
    )


def _get_feature_nos_for_genes(
    db: Session,
    genes: list[str],
    organism_no: int,
) -> tuple[list[int], list[str]]:
    """
    Get feature_nos for a list of gene names.

    Returns (feature_nos, not_found_genes).
    """
    request = ValidateGenesRequest(genes=genes, organism_no=organism_no)
    validation = validate_genes(db, request)
    feature_nos = [g.feature_no for g in validation.found]
    not_found = validation.not_found
    return feature_nos, not_found


def _build_annotation_filters(
    evidence_codes: Optional[list[str]],
    annotation_types: Optional[list[str]],
):
    """Build SQLAlchemy filter conditions for annotations."""
    filters = []

    if evidence_codes:
        filters.append(GoAnnotation.go_evidence.in_(evidence_codes))

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


def _chunk_list(lst: list, chunk_size: int = 900) -> list[list]:
    """Split a list into chunks of specified size (default 900 for Oracle's 1000 limit)."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def _get_go_annotations_with_ancestors(
    db: Session,
    feature_nos: list[int],
    ontology: GoOntology,
    evidence_codes: Optional[list[str]] = None,
    annotation_types: Optional[list[str]] = None,
) -> dict[int, set[int]]:
    """
    Get GO annotations for features, including inherited ancestor terms.

    A gene annotated to a child term is implicitly annotated to all ancestors.

    Returns: dict mapping feature_no -> set of go_no values
    """
    if not feature_nos:
        return {}

    # Build annotation filters
    ann_filters = _build_annotation_filters(evidence_codes, annotation_types)

    # Aspect map for ontology filter (database stores single letters)
    aspect_map = {
        GoOntology.PROCESS: "P",
        GoOntology.FUNCTION: "F",
        GoOntology.COMPONENT: "C",
    }

    # Query direct annotations in batches (Oracle IN clause limit is 1000)
    direct_annotations = []
    for chunk in _chunk_list(feature_nos):
        query = (
            db.query(GoAnnotation.feature_no, GoAnnotation.go_no)
            .join(Go, Go.go_no == GoAnnotation.go_no)
            .filter(GoAnnotation.feature_no.in_(chunk))
        )

        # Apply ontology filter
        if ontology != GoOntology.ALL:
            query = query.filter(Go.go_aspect == aspect_map.get(ontology, ontology.value))

        # Apply annotation filters
        for f in ann_filters:
            query = query.filter(f)

        direct_annotations.extend(query.all())

    # Build feature -> go_no set mapping
    feature_to_go_nos: dict[int, set[int]] = defaultdict(set)
    all_go_nos = set()

    for feature_no, go_no in direct_annotations:
        feature_to_go_nos[feature_no].add(go_no)
        all_go_nos.add(go_no)

    if not all_go_nos:
        return feature_to_go_nos

    # Query ancestors for all direct annotations (also in batches)
    ancestor_paths = []
    for chunk in _chunk_list(list(all_go_nos)):
        ancestor_query = (
            db.query(GoPath.child_go_no, GoPath.ancestor_go_no)
            .filter(GoPath.child_go_no.in_(chunk))
        )

        # Filter ancestors by ontology if needed
        if ontology != GoOntology.ALL:
            ancestor_query = (
                ancestor_query
                .join(Go, Go.go_no == GoPath.ancestor_go_no)
                .filter(Go.go_aspect == aspect_map.get(ontology, ontology.value))
            )

        ancestor_paths.extend(ancestor_query.all())

    # Build child_go_no -> set of ancestor_go_no mapping
    child_to_ancestors: dict[int, set[int]] = defaultdict(set)
    for child_go_no, ancestor_go_no in ancestor_paths:
        child_to_ancestors[child_go_no].add(ancestor_go_no)

    # Add ancestors to each feature's annotation set
    for feature_no, go_nos in feature_to_go_nos.items():
        inherited_go_nos = set()
        for go_no in go_nos:
            inherited_go_nos.update(child_to_ancestors.get(go_no, set()))
        feature_to_go_nos[feature_no].update(inherited_go_nos)

    return feature_to_go_nos


def _calculate_enrichment(
    query_annotations: dict[int, set[int]],
    background_annotations: dict[int, set[int]],
    p_value_cutoff: float,
    min_genes_in_term: int,
) -> list[tuple[int, int, int, int, int, float]]:
    """
    Calculate enrichment using hypergeometric test.

    P(X >= k) = 1 - P(X <= k-1) = hypergeom.sf(k-1, N, K, n)

    Where:
    - N = background set size
    - K = genes in background annotated to term
    - n = query set size
    - k = genes in query annotated to term

    Returns list of (go_no, k, n, K, N, p_value) tuples for significant terms.
    """
    # Calculate N and n
    N = len(background_annotations)  # Total background genes
    n = len(query_annotations)  # Total query genes

    if N == 0 or n == 0:
        return []

    # Count genes per GO term in query and background
    query_term_counts: dict[int, set[int]] = defaultdict(set)  # go_no -> set of feature_nos
    background_term_counts: dict[int, set[int]] = defaultdict(set)

    for feature_no, go_nos in query_annotations.items():
        for go_no in go_nos:
            query_term_counts[go_no].add(feature_no)

    for feature_no, go_nos in background_annotations.items():
        for go_no in go_nos:
            background_term_counts[go_no].add(feature_no)

    # Calculate p-values for each term
    results = []
    for go_no, query_features in query_term_counts.items():
        k = len(query_features)  # Genes in query with this term
        K = len(background_term_counts.get(go_no, set()))  # Genes in background with this term

        if k < min_genes_in_term:
            continue

        if K == 0:
            continue

        # Hypergeometric test: P(X >= k)
        # hypergeom.sf(k-1, N, K, n) gives P(X >= k)
        p_value = hypergeom.sf(k - 1, N, K, n)

        if p_value <= p_value_cutoff:
            results.append((go_no, k, n, K, N, p_value))

    return results


def _apply_multiple_testing_correction(
    results: list[tuple[int, int, int, int, int, float]],
    method: MultipleCorrectionMethod,
    p_value_cutoff: float,
) -> list[tuple[int, int, int, int, int, float, Optional[float]]]:
    """
    Apply multiple testing correction.

    Returns list of (go_no, k, n, K, N, p_value, fdr) tuples.
    """
    if not results:
        return []

    if method == MultipleCorrectionMethod.NONE:
        return [(go_no, k, n, K, N, p_val, None) for go_no, k, n, K, N, p_val in results]

    n_tests = len(results)

    if method == MultipleCorrectionMethod.BONFERRONI:
        # Bonferroni: multiply p-values by number of tests
        corrected = []
        for go_no, k, n, K, N, p_val in results:
            corrected_p = min(p_val * n_tests, 1.0)
            if corrected_p <= p_value_cutoff:
                corrected.append((go_no, k, n, K, N, p_val, corrected_p))
        return corrected

    elif method == MultipleCorrectionMethod.BENJAMINI_HOCHBERG:
        # Benjamini-Hochberg FDR
        # 1. Sort by p-value
        sorted_results = sorted(results, key=lambda x: x[5])

        # 2. Calculate FDR for each rank
        fdr_values = []
        for i, (go_no, k, n, K, N, p_val) in enumerate(sorted_results):
            rank = i + 1
            fdr = (p_val * n_tests) / rank
            fdr_values.append((go_no, k, n, K, N, p_val, fdr))

        # 3. Enforce monotonicity (FDR can only decrease as rank increases)
        for i in range(len(fdr_values) - 2, -1, -1):
            go_no, k, n, K, N, p_val, fdr = fdr_values[i]
            next_fdr = fdr_values[i + 1][6]
            if fdr > next_fdr:
                fdr_values[i] = (go_no, k, n, K, N, p_val, next_fdr)

        # 4. Cap FDR at 1.0 and filter by cutoff
        corrected = []
        for go_no, k, n, K, N, p_val, fdr in fdr_values:
            fdr = min(fdr, 1.0)
            if fdr <= p_value_cutoff:
                corrected.append((go_no, k, n, K, N, p_val, fdr))

        return corrected

    return [(go_no, k, n, K, N, p_val, None) for go_no, k, n, K, N, p_val in results]


def run_go_term_finder(
    db: Session,
    request: GoTermFinderRequest,
) -> GoTermFinderResponse:
    """
    Run GO Term Finder enrichment analysis.

    Args:
        db: Database session
        request: Analysis request parameters

    Returns:
        GoTermFinderResponse with enriched terms or error
    """
    warnings = []

    # Step 1: Validate and get query genes
    query_feature_nos, not_found_genes = _get_feature_nos_for_genes(
        db, request.genes, request.organism_no
    )

    if not query_feature_nos:
        return GoTermFinderResponse(
            success=False,
            error="No valid genes found in the database",
            warnings=warnings,
        )

    # Step 2: Build background set
    background_type = "default"
    if request.background_genes:
        # Custom background
        background_type = "custom"
        background_feature_nos, bg_not_found = _get_feature_nos_for_genes(
            db, request.background_genes, request.organism_no
        )
        if bg_not_found:
            warnings.append(f"{len(bg_not_found)} background genes not found")
    else:
        # Default: all genes with GO annotations for this organism
        bg_query = (
            db.query(GoAnnotation.feature_no)
            .join(Feature, Feature.feature_no == GoAnnotation.feature_no)
            .filter(Feature.organism_no == request.organism_no)
        )

        # Apply annotation filters to background
        ann_filters = _build_annotation_filters(
            request.evidence_codes, request.annotation_types
        )
        for f in ann_filters:
            bg_query = bg_query.filter(f)

        background_feature_nos = list(set(row.feature_no for row in bg_query.distinct().all()))

    if not background_feature_nos:
        return GoTermFinderResponse(
            success=False,
            error="Background set is empty with the specified filters",
            warnings=warnings,
        )

    # Ensure query genes are subset of background
    query_feature_nos = [f for f in query_feature_nos if f in set(background_feature_nos)]

    if not query_feature_nos:
        return GoTermFinderResponse(
            success=False,
            error="No query genes found in background set",
            warnings=warnings,
        )

    # Step 3: Get GO annotations with ancestors
    query_annotations = _get_go_annotations_with_ancestors(
        db,
        query_feature_nos,
        request.ontology,
        request.evidence_codes,
        request.annotation_types,
    )

    background_annotations = _get_go_annotations_with_ancestors(
        db,
        background_feature_nos,
        request.ontology,
        request.evidence_codes,
        request.annotation_types,
    )

    # Filter to genes with GO annotations
    query_genes_with_go = [f for f in query_feature_nos if f in query_annotations]

    if not query_genes_with_go:
        return GoTermFinderResponse(
            success=False,
            error="No query genes have GO annotations with the specified filters",
            warnings=warnings,
        )

    # Step 4: Calculate enrichment
    enrichment_results = _calculate_enrichment(
        {f: query_annotations[f] for f in query_genes_with_go},
        background_annotations,
        request.p_value_cutoff,
        request.min_genes_in_term,
    )

    # Step 5: Apply multiple testing correction
    corrected_results = _apply_multiple_testing_correction(
        enrichment_results,
        request.correction_method,
        request.p_value_cutoff,
    )

    if not corrected_results:
        # Build result with no enriched terms
        result = GoTermFinderResult(
            query_genes_submitted=len(request.genes),
            query_genes_found=len(query_feature_nos) + len(not_found_genes) - len(not_found_genes),
            query_genes_with_go=len(query_genes_with_go),
            query_genes_not_found=not_found_genes,
            background_size=len(background_annotations),
            background_type=background_type,
            ontology_filter=request.ontology.value,
            evidence_codes_used=request.evidence_codes or [],
            annotation_types_used=request.annotation_types or [],
            p_value_cutoff=request.p_value_cutoff,
            correction_method=request.correction_method.value,
            process_terms=[],
            function_terms=[],
            component_terms=[],
            total_enriched_terms=0,
        )
        return GoTermFinderResponse(
            success=True,
            result=result,
            warnings=warnings + ["No significantly enriched GO terms found"],
        )

    # Step 6: Build enriched term objects
    go_nos = [r[0] for r in corrected_results]
    go_records = []
    for chunk in _chunk_list(go_nos):
        go_records.extend(db.query(Go).filter(Go.go_no.in_(chunk)).all())
    go_no_to_go = {go.go_no: go for go in go_records}

    # Get feature info for genes in query (chunked to avoid Oracle 1000 limit)
    feature_records = []
    for chunk in _chunk_list(query_genes_with_go):
        feature_records.extend(db.query(Feature).filter(Feature.feature_no.in_(chunk)).all())
    feature_no_to_feature = {f.feature_no: f for f in feature_records}

    # Build gene-to-evidence mapping for enriched terms (chunked queries)
    ann_filters = _build_annotation_filters(request.evidence_codes, request.annotation_types)
    gene_evidence_results = []
    for feature_chunk in _chunk_list(query_genes_with_go):
        for go_chunk in _chunk_list(go_nos):
            query = (
                db.query(
                    GoAnnotation.feature_no,
                    GoAnnotation.go_no,
                    GoAnnotation.go_evidence,
                )
                .filter(GoAnnotation.feature_no.in_(feature_chunk))
                .filter(GoAnnotation.go_no.in_(go_chunk))
            )
            for f in ann_filters:
                query = query.filter(f)
            gene_evidence_results.extend(query.all())

    # Build mapping: go_no -> feature_no -> evidence_codes
    go_to_gene_evidence: dict[int, dict[int, set[str]]] = defaultdict(lambda: defaultdict(set))
    for feature_no, go_no, evidence in gene_evidence_results:
        go_to_gene_evidence[go_no][feature_no].add(evidence)

    # Build EnrichedGoTerm objects
    process_terms = []
    function_terms = []
    component_terms = []

    for go_no, k, n, K, N, p_val, fdr in corrected_results:
        go = go_no_to_go.get(go_no)
        if not go:
            continue

        aspect_code = go.go_aspect[0].upper() if go.go_aspect else "P"
        aspect_name = ASPECT_NAMES.get(aspect_code, go.go_aspect)

        # Build gene hits
        gene_hits = []
        for feature_no, go_nos in query_annotations.items():
            if go_no in go_nos:
                feature = feature_no_to_feature.get(feature_no)
                if feature:
                    evidence_codes = list(go_to_gene_evidence.get(go_no, {}).get(feature_no, []))
                    gene_hits.append(GeneHit(
                        feature_no=feature_no,
                        systematic_name=feature.feature_name,
                        gene_name=feature.gene_name,
                        evidence_codes=evidence_codes,
                    ))

        # Calculate frequencies
        query_frequency = (k / n) * 100 if n > 0 else 0.0
        background_frequency = (K / N) * 100 if N > 0 else 0.0
        fold_enrichment = (k / n) / (K / N) if K > 0 and N > 0 and n > 0 else 0.0

        enriched_term = EnrichedGoTerm(
            go_no=go_no,
            goid=_format_goid(go.goid),
            go_term=go.go_term,
            go_aspect=aspect_code,
            aspect_name=aspect_name,
            query_count=k,
            query_total=n,
            background_count=K,
            background_total=N,
            query_frequency=round(query_frequency, 2),
            background_frequency=round(background_frequency, 4),
            fold_enrichment=round(fold_enrichment, 2),
            p_value=p_val,
            fdr=fdr,
            genes=gene_hits,
        )

        if aspect_code == "P":
            process_terms.append(enriched_term)
        elif aspect_code == "F":
            function_terms.append(enriched_term)
        elif aspect_code == "C":
            component_terms.append(enriched_term)

    # Sort by p-value
    process_terms.sort(key=lambda x: x.p_value)
    function_terms.sort(key=lambda x: x.p_value)
    component_terms.sort(key=lambda x: x.p_value)

    result = GoTermFinderResult(
        query_genes_submitted=len(request.genes),
        query_genes_found=len(query_feature_nos),
        query_genes_with_go=len(query_genes_with_go),
        query_genes_not_found=not_found_genes,
        background_size=len(background_annotations),
        background_type=background_type,
        ontology_filter=request.ontology.value,
        evidence_codes_used=request.evidence_codes or [],
        annotation_types_used=request.annotation_types or [],
        p_value_cutoff=request.p_value_cutoff,
        correction_method=request.correction_method.value,
        process_terms=process_terms,
        function_terms=function_terms,
        component_terms=component_terms,
        total_enriched_terms=len(process_terms) + len(function_terms) + len(component_terms),
    )

    return GoTermFinderResponse(
        success=True,
        result=result,
        warnings=warnings,
    )


def build_enrichment_graph(
    db: Session,
    enriched_terms: list[EnrichedGoTerm],
    max_terms: int = 5,
) -> GoEnrichmentGraphResponse:
    """
    Build GO hierarchy graph for enriched terms visualization.

    Builds a tree structure from the root term down to the enriched terms,
    including intermediate ancestor terms that connect them.

    Args:
        db: Database session
        enriched_terms: List of enriched GO terms
        max_terms: Maximum number of enriched terms to include (default 10)

    Returns:
        GoEnrichmentGraphResponse with nodes and edges
    """
    if not enriched_terms:
        return GoEnrichmentGraphResponse(nodes=[], edges=[])

    # Filter to terms with >1 gene, sort by p-value, and take top max_terms
    terms_with_genes = [t for t in enriched_terms if t.query_count > 1]
    terms_sorted = sorted(terms_with_genes, key=lambda t: (t.p_value, -t.query_count))
    terms_to_use = terms_sorted[:max_terms]
    if not terms_to_use:
        return GoEnrichmentGraphResponse(nodes=[], edges=[])

    enriched_go_nos = {t.go_no for t in terms_to_use}
    enriched_by_go_no = {t.go_no: t for t in terms_to_use}

    # Get the aspect for these terms (should all be the same since we filter by ontology)
    aspect = terms_to_use[0].go_aspect if terms_to_use else "P"

    # Find the root term for this aspect
    root_terms = {
        "P": "biological_process",
        "F": "molecular_function",
        "C": "cellular_component",
    }
    root_term_name = root_terms.get(aspect, "biological_process")

    # Get the root GO term
    root_go = (
        db.query(Go)
        .filter(Go.go_term == root_term_name)
        .first()
    )

    # Find all paths from enriched terms to root, keeping intermediate nodes
    # Get all ancestors of enriched terms with generation info
    all_ancestor_paths = []
    for chunk in _chunk_list(list(enriched_go_nos)):
        paths = (
            db.query(GoPath)
            .filter(GoPath.child_go_no.in_(chunk))
            .all()
        )
        all_ancestor_paths.extend(paths)

    # Build set of all ancestor go_nos we need
    ancestor_go_nos = set()
    for path in all_ancestor_paths:
        ancestor_go_nos.add(path.ancestor_go_no)

    # Add root if found
    if root_go:
        ancestor_go_nos.add(root_go.go_no)

    # Query all ancestor GO terms
    all_go_nos = enriched_go_nos | ancestor_go_nos
    go_records = []
    for chunk in _chunk_list(list(all_go_nos)):
        records = db.query(Go).filter(Go.go_no.in_(chunk)).all()
        go_records.extend(records)

    go_by_no = {go.go_no: go for go in go_records}

    # Find direct (generation=1) relationships between all nodes
    direct_paths = []
    for chunk in _chunk_list(list(all_go_nos)):
        paths = (
            db.query(GoPath)
            .filter(GoPath.child_go_no.in_(chunk))
            .filter(GoPath.ancestor_go_no.in_(all_go_nos))
            .filter(GoPath.generation == 1)
            .all()
        )
        direct_paths.extend(paths)

    # Build a set of nodes that are on paths between enriched terms
    # Start with enriched terms and work up to find connecting ancestors
    nodes_to_include = set(enriched_go_nos)

    # Add root
    if root_go:
        nodes_to_include.add(root_go.go_no)

    # For each enriched term, find ancestors that connect to other enriched terms or root
    # Build parent-child map
    child_to_parents: dict[int, set[int]] = defaultdict(set)
    for path in direct_paths:
        child_to_parents[path.child_go_no].add(path.ancestor_go_no)

    # BFS from each enriched term to find common ancestors
    def find_path_to_root(start_go_no: int) -> set[int]:
        """Find all ancestors on path to root."""
        visited = set()
        queue = [start_go_no]
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            for parent in child_to_parents.get(current, []):
                if parent in all_go_nos:
                    queue.append(parent)
        return visited

    # Find paths from all enriched terms to root
    for go_no in enriched_go_nos:
        path_nodes = find_path_to_root(go_no)
        nodes_to_include.update(path_nodes)

    # Limit nodes if too many (keep enriched terms, root, and closest ancestors)
    if len(nodes_to_include) > max_terms * 5:
        # Keep enriched terms and root, then add closest ancestors
        essential = enriched_go_nos.copy()
        if root_go:
            essential.add(root_go.go_no)
        nodes_to_include = essential

    # Build nodes
    nodes = []
    for go_no in nodes_to_include:
        go = go_by_no.get(go_no)
        if not go:
            continue

        enriched_term = enriched_by_go_no.get(go_no)
        is_enriched = enriched_term is not None

        # Get genes for enriched terms
        genes = []
        if enriched_term and enriched_term.genes:
            genes = [
                {"gene_name": g.gene_name, "systematic_name": g.systematic_name}
                for g in enriched_term.genes
            ]

        nodes.append(GoEnrichmentGraphNode(
            goid=_format_goid(go.goid),
            go_term=go.go_term,
            go_aspect=aspect,
            p_value=enriched_term.p_value if enriched_term else 1.0,
            fdr=enriched_term.fdr if enriched_term else None,
            query_count=enriched_term.query_count if enriched_term else 0,
            is_enriched=is_enriched,
            genes=genes,
        ))

    # Build edges (only between nodes we're including)
    edges = []
    node_go_nos = nodes_to_include
    for path in direct_paths:
        if path.ancestor_go_no in node_go_nos and path.child_go_no in node_go_nos:
            ancestor_go = go_by_no.get(path.ancestor_go_no)
            child_go = go_by_no.get(path.child_go_no)
            if ancestor_go and child_go:
                rel_type = "is_a"
                if path.relationship_type:
                    rel_type = path.relationship_type.replace(" ", "_")
                edges.append(GoEnrichmentGraphEdge(
                    source=_format_goid(ancestor_go.goid),
                    target=_format_goid(child_go.goid),
                    relationship_type=rel_type,
                ))

    return GoEnrichmentGraphResponse(nodes=nodes, edges=edges)
