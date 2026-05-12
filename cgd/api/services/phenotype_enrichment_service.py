"""
Phenotype Enrichment Service - Phenotype observable term enrichment analysis.

Performs hypergeometric test for phenotype term enrichment with optional
multiple testing correction (Bonferroni or Benjamini-Hochberg FDR).

Follows the same pattern as GO Term Finder.
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Optional

logger = logging.getLogger(__name__)

from scipy.stats import hypergeom
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from cgd.models.models import (
    Alias,
    Feature,
    FeatAlias,
    FeatLocation,
    FeatProperty,
    GenomeVersion,
    Organism,
    PhenoAnnotation,
    Phenotype,
    Seq,
)
from cgd.schemas.phenotype_enrichment_schema import (
    EnrichedPhenotype,
    GeneHit,
    MultipleCorrectionMethod,
    OrganismOption,
    PhenotypeEnrichmentConfigResponse,
    PhenotypeEnrichmentRequest,
    PhenotypeEnrichmentResponse,
    PhenotypeEnrichmentResult,
    ValidatedGene,
    ValidateGenesRequest,
    ValidateGenesResponse,
)


def get_phenotype_enrichment_config(db: Session) -> PhenotypeEnrichmentConfigResponse:
    """
    Get configuration options for Phenotype Enrichment.

    Returns organisms with phenotype annotations and default settings.
    """
    # Get organisms with phenotype annotations
    organisms_with_phenotype = (
        db.query(Organism)
        .join(Feature, Feature.organism_no == Organism.organism_no)
        .join(PhenoAnnotation, PhenoAnnotation.feature_no == Feature.feature_no)
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
        for org in organisms_with_phenotype
    ]

    return PhenotypeEnrichmentConfigResponse(
        organisms=organism_options,
    )


def _chunk_list(lst: list, chunk_size: int = 900) -> list[list]:
    """Split a list into chunks of specified size (default 900 for Oracle's 1000 limit)."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


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
            total_with_phenotype=0,
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

    # Get phenotype annotation status for found features (chunked to avoid Oracle 1000 limit)
    feature_nos = list(set(f.feature_no for f in found_map.values()))
    features_with_phenotype = set()
    if feature_nos:
        for chunk in _chunk_list(feature_nos):
            pheno_check = (
                db.query(PhenoAnnotation.feature_no)
                .filter(PhenoAnnotation.feature_no.in_(chunk))
                .distinct()
                .all()
            )
            features_with_phenotype.update(row.feature_no for row in pheno_check)

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
                has_phenotype_annotations=feature.feature_no in features_with_phenotype,
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

    total_with_phenotype = sum(1 for g in unique_found if g.has_phenotype_annotations)

    return ValidateGenesResponse(
        found=unique_found,
        not_found=not_found_inputs,
        total_submitted=len(request.genes),
        total_found=len(unique_found),
        total_with_phenotype=total_with_phenotype,
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


def _get_organism_seq_source(db: Session, organism_no: int) -> Optional[str]:
    """
    Get the default seq_source (assembly) for an organism.
    Returns None if not configured.
    """
    organism = db.query(Organism).filter(Organism.organism_no == organism_no).first()
    if not organism:
        return None

    organism_name = organism.organism_name

    SEQ_SOURCE_MAP = {
        'Candida albicans SC5314': 'C. albicans SC5314 Assembly 22',
        'Candida glabrata CBS138': 'C. glabrata CBS138',
        'Candida auris B8441': 'C. auris B8441',
        'Candida dubliniensis CD36': 'C. dubliniensis CD36',
        'Candida parapsilosis CDC317': 'C. parapsilosis CDC317',
    }

    return SEQ_SOURCE_MAP.get(organism_name)


def _get_features_in_current_assembly(db: Session, organism_no: int) -> set[int]:
    """
    Get feature_nos that are in the current genome assembly.
    """
    seq_source = _get_organism_seq_source(db, organism_no)

    current_features_query = (
        db.query(FeatLocation.feature_no)
        .join(Seq, Seq.seq_no == FeatLocation.root_seq_no)
        .join(Feature, Feature.feature_no == FeatLocation.feature_no)
        .filter(Feature.organism_no == organism_no)
        .filter(FeatLocation.is_loc_current == 'Y')
        .filter(Seq.is_seq_current == 'Y')
        .filter(Feature.feature_type != 'allele')
    )

    if seq_source:
        current_features_query = current_features_query.filter(Seq.source == seq_source)
    else:
        current_features_query = (
            current_features_query
            .join(GenomeVersion, GenomeVersion.genome_version_no == Seq.genome_version_no)
            .filter(GenomeVersion.is_ver_current == 'Y')
        )

    return set(row.feature_no for row in current_features_query.distinct().all())


def _get_deleted_feature_nos(db: Session, organism_no: int) -> set[int]:
    """Get feature_nos for deleted features."""
    deleted_query = (
        db.query(Feature.feature_no)
        .join(FeatProperty, FeatProperty.feature_no == Feature.feature_no)
        .filter(Feature.organism_no == organism_no)
        .filter(FeatProperty.property_type == 'feature_qualifier')
        .filter(FeatProperty.property_value.like('Delete%'))
    )
    return set(row.feature_no for row in deleted_query.all())


def _get_valid_background_feature_nos(
    db: Session,
    organism_no: int,
    feature_nos: Optional[list[int]] = None,
) -> set[int]:
    """
    Get valid feature_nos for the background set.
    """
    current_assembly_features = _get_features_in_current_assembly(db, organism_no)
    deleted_features = _get_deleted_feature_nos(db, organism_no)
    valid_features = current_assembly_features - deleted_features

    if feature_nos is not None:
        valid_features = valid_features & set(feature_nos)

    return valid_features


def _get_phenotype_annotations(
    db: Session,
    feature_nos: list[int],
) -> dict[int, set[int]]:
    """
    Get phenotype annotations for features.

    Returns dict mapping feature_no -> set of phenotype_no values.
    """
    if not feature_nos:
        return {}

    annotations: dict[int, set[int]] = defaultdict(set)

    for chunk in _chunk_list(feature_nos):
        query = (
            db.query(PhenoAnnotation.feature_no, PhenoAnnotation.phenotype_no)
            .filter(PhenoAnnotation.feature_no.in_(chunk))
        )
        for feature_no, phenotype_no in query.all():
            annotations[feature_no].add(phenotype_no)

    return dict(annotations)


def _calculate_enrichment(
    query_annotations: dict[int, set[int]],
    background_annotations: dict[int, set[int]],
    query_size: int,
    background_size: int,
    p_value_cutoff: float,
    min_genes_in_term: int,
) -> list[tuple[int, int, int, int, int, float]]:
    """
    Calculate enrichment using hypergeometric test.

    P(X >= k) = hypergeom.sf(k-1, N, K, n)

    Where:
    - N = background set size (total genes in background)
    - K = genes in background annotated to term
    - n = query set size
    - k = genes in query annotated to term

    Returns list of (phenotype_no, k, n, K, N, p_value) tuples for significant terms.
    """
    N = background_size
    n = query_size

    if N == 0 or n == 0:
        return []

    # Count genes per phenotype in query and background
    query_term_counts: dict[int, set[int]] = defaultdict(set)
    background_term_counts: dict[int, set[int]] = defaultdict(set)

    for feature_no, phenotype_nos in query_annotations.items():
        for phenotype_no in phenotype_nos:
            query_term_counts[phenotype_no].add(feature_no)

    for feature_no, phenotype_nos in background_annotations.items():
        for phenotype_no in phenotype_nos:
            background_term_counts[phenotype_no].add(feature_no)

    # Calculate p-values for each term
    results = []
    for phenotype_no, query_features in query_term_counts.items():
        k = len(query_features)
        K = len(background_term_counts.get(phenotype_no, set()))

        if k < min_genes_in_term:
            continue

        if K == 0:
            continue

        # Hypergeometric test: P(X >= k)
        p_value = hypergeom.sf(k - 1, N, K, n)

        if p_value <= p_value_cutoff:
            results.append((phenotype_no, k, n, K, N, p_value))

    return results


def _apply_multiple_testing_correction(
    results: list[tuple[int, int, int, int, int, float]],
    method: MultipleCorrectionMethod,
    p_value_cutoff: float,
) -> list[tuple[int, int, int, int, int, float, Optional[float]]]:
    """
    Apply multiple testing correction.

    Returns list of (phenotype_no, k, n, K, N, p_value, fdr) tuples.
    """
    if not results:
        return []

    if method == MultipleCorrectionMethod.NONE:
        return [(pheno_no, k, n, K, N, p_val, None) for pheno_no, k, n, K, N, p_val in results]

    n_tests = len(results)

    if method == MultipleCorrectionMethod.BONFERRONI:
        corrected = []
        for pheno_no, k, n, K, N, p_val in results:
            corrected_p = min(p_val * n_tests, 1.0)
            if corrected_p <= p_value_cutoff:
                corrected.append((pheno_no, k, n, K, N, p_val, corrected_p))
        return corrected

    elif method == MultipleCorrectionMethod.BENJAMINI_HOCHBERG:
        # Benjamini-Hochberg FDR
        sorted_results = sorted(results, key=lambda x: x[5])

        fdr_values = []
        for i, (pheno_no, k, n, K, N, p_val) in enumerate(sorted_results):
            rank = i + 1
            fdr = (p_val * n_tests) / rank
            fdr_values.append((pheno_no, k, n, K, N, p_val, fdr))

        # Enforce monotonicity
        for i in range(len(fdr_values) - 2, -1, -1):
            pheno_no, k, n, K, N, p_val, fdr = fdr_values[i]
            next_fdr = fdr_values[i + 1][6]
            if fdr > next_fdr:
                fdr_values[i] = (pheno_no, k, n, K, N, p_val, next_fdr)

        # Cap FDR at 1.0 and filter by cutoff
        corrected = []
        for pheno_no, k, n, K, N, p_val, fdr in fdr_values:
            fdr = min(fdr, 1.0)
            if fdr <= p_value_cutoff:
                corrected.append((pheno_no, k, n, K, N, p_val, fdr))

        return corrected

    return [(pheno_no, k, n, K, N, p_val, None) for pheno_no, k, n, K, N, p_val in results]


def run_phenotype_enrichment(
    db: Session,
    request: PhenotypeEnrichmentRequest,
) -> PhenotypeEnrichmentResponse:
    """
    Run phenotype enrichment analysis.

    Args:
        db: Database session
        request: Analysis request parameters

    Returns:
        PhenotypeEnrichmentResponse with enriched phenotypes or error
    """
    start_time = time.time()
    warnings = []

    logger.info(f"Phenotype Enrichment started with {len(request.genes)} genes")

    # Step 1: Validate and get query genes
    step_start = time.time()
    query_feature_nos, not_found_genes = _get_feature_nos_for_genes(
        db, request.genes, request.organism_no
    )
    logger.info(f"Step 1 (validate genes): {time.time() - step_start:.2f}s - found {len(query_feature_nos)} genes")

    if not query_feature_nos:
        return PhenotypeEnrichmentResponse(
            success=False,
            error="No valid genes found in the database",
            warnings=warnings,
        )

    # Step 2: Build background set
    step_start = time.time()
    background_type = "default"

    valid_features = _get_valid_background_feature_nos(db, request.organism_no)
    logger.info(f"Step 2a (valid features): {time.time() - step_start:.2f}s - {len(valid_features)} features")

    step_start = time.time()
    if request.background_genes:
        background_type = "custom"
        background_feature_nos, bg_not_found = _get_feature_nos_for_genes(
            db, request.background_genes, request.organism_no
        )
        if bg_not_found:
            warnings.append(f"{len(bg_not_found)} background genes not found")
        background_feature_nos = [f for f in background_feature_nos if f in valid_features]
    else:
        # Default: all gene-level features in current assembly
        GENE_LEVEL_FEATURE_TYPES = {
            'ORF', 'tRNA', 'snoRNA', 'rRNA', 'snRNA', 'ncRNA',
            'pseudogene', 'blocked_reading_frame',
            'long_terminal_repeat', 'repeat_region', 'retrotransposon', 'centromere',
        }

        gene_level_features = set(
            row.feature_no for row in
            db.query(Feature.feature_no)
            .filter(Feature.organism_no == request.organism_no)
            .filter(Feature.feature_type.in_(GENE_LEVEL_FEATURE_TYPES))
            .all()
        )

        background_feature_nos = list(valid_features & gene_level_features)
    logger.info(f"Step 2b (background set): {time.time() - step_start:.2f}s - {len(background_feature_nos)} genes")

    if not background_feature_nos:
        return PhenotypeEnrichmentResponse(
            success=False,
            error="Background set is empty with the specified filters",
            warnings=warnings,
        )

    # Ensure query genes are subset of background
    query_feature_nos = [f for f in query_feature_nos if f in set(background_feature_nos)]

    if not query_feature_nos:
        return PhenotypeEnrichmentResponse(
            success=False,
            error="No query genes found in background set",
            warnings=warnings,
        )

    # Step 3: Get phenotype annotations
    step_start = time.time()
    query_annotations = _get_phenotype_annotations(db, query_feature_nos)
    logger.info(f"Step 3a (query phenotype annotations): {time.time() - step_start:.2f}s")

    step_start = time.time()
    background_annotations = _get_phenotype_annotations(db, background_feature_nos)
    logger.info(f"Step 3b (background phenotype annotations): {time.time() - step_start:.2f}s")

    # Filter to genes with phenotype annotations
    query_genes_with_phenotype = [f for f in query_feature_nos if f in query_annotations]

    if not query_genes_with_phenotype:
        return PhenotypeEnrichmentResponse(
            success=False,
            error="No query genes have phenotype annotations",
            warnings=warnings,
        )

    # Step 4: Calculate enrichment
    step_start = time.time()
    enrichment_results = _calculate_enrichment(
        {f: query_annotations[f] for f in query_genes_with_phenotype},
        background_annotations,
        len(query_feature_nos),
        len(background_feature_nos),
        request.p_value_cutoff,
        request.min_genes_in_term,
    )
    logger.info(f"Step 4 (calculate enrichment): {time.time() - step_start:.2f}s - {len(enrichment_results)} terms")

    # Step 5: Apply multiple testing correction
    step_start = time.time()
    corrected_results = _apply_multiple_testing_correction(
        enrichment_results,
        request.correction_method,
        request.p_value_cutoff,
    )
    logger.info(f"Step 5 (multiple testing correction): {time.time() - step_start:.2f}s - {len(corrected_results)} terms")

    if not corrected_results:
        result = PhenotypeEnrichmentResult(
            query_genes_submitted=len(request.genes),
            query_genes_found=len(query_feature_nos),
            query_genes_with_phenotype=len(query_genes_with_phenotype),
            query_genes_not_found=not_found_genes,
            background_size=len(background_feature_nos),
            background_type=background_type,
            p_value_cutoff=request.p_value_cutoff,
            correction_method=request.correction_method.value,
            enriched_phenotypes=[],
            total_enriched_phenotypes=0,
        )
        return PhenotypeEnrichmentResponse(
            success=True,
            result=result,
            warnings=warnings + ["No significantly enriched phenotypes found"],
        )

    # Step 6: Build enriched phenotype objects
    step_start = time.time()
    phenotype_nos = [r[0] for r in corrected_results]
    phenotype_records = []
    for chunk in _chunk_list(phenotype_nos):
        phenotype_records.extend(db.query(Phenotype).filter(Phenotype.phenotype_no.in_(chunk)).all())
    pheno_no_to_pheno = {p.phenotype_no: p for p in phenotype_records}

    # Get feature info for genes in query
    feature_records = []
    for chunk in _chunk_list(query_genes_with_phenotype):
        feature_records.extend(db.query(Feature).filter(Feature.feature_no.in_(chunk)).all())
    feature_no_to_feature = {f.feature_no: f for f in feature_records}

    logger.info(f"Step 6a (fetch phenotype/feature info): {time.time() - step_start:.2f}s")

    # Build EnrichedPhenotype objects
    enriched_phenotypes = []

    for pheno_no, k, n, K, N, p_val, fdr in corrected_results:
        phenotype = pheno_no_to_pheno.get(pheno_no)
        if not phenotype:
            continue

        # Build gene hits
        gene_hits = []
        for feature_no, phenotype_nos in query_annotations.items():
            if pheno_no in phenotype_nos:
                feature = feature_no_to_feature.get(feature_no)
                if feature:
                    gene_hits.append(GeneHit(
                        feature_no=feature_no,
                        systematic_name=feature.feature_name,
                        gene_name=feature.gene_name,
                    ))

        # Calculate frequencies
        query_frequency = (k / n) * 100 if n > 0 else 0.0
        background_frequency = (K / N) * 100 if N > 0 else 0.0
        fold_enrichment = (k / n) / (K / N) if K > 0 and N > 0 and n > 0 else 0.0

        enriched_phenotype = EnrichedPhenotype(
            phenotype_no=pheno_no,
            observable=phenotype.observable,
            mutant_type=phenotype.mutant_type,
            qualifier=phenotype.qualifier,
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

        enriched_phenotypes.append(enriched_phenotype)

    # Sort by p-value
    enriched_phenotypes.sort(key=lambda x: x.p_value)

    result = PhenotypeEnrichmentResult(
        query_genes_submitted=len(request.genes),
        query_genes_found=len(query_feature_nos),
        query_genes_with_phenotype=len(query_genes_with_phenotype),
        query_genes_not_found=not_found_genes,
        background_size=len(background_feature_nos),
        background_type=background_type,
        p_value_cutoff=request.p_value_cutoff,
        correction_method=request.correction_method.value,
        enriched_phenotypes=enriched_phenotypes,
        total_enriched_phenotypes=len(enriched_phenotypes),
    )

    total_time = time.time() - start_time
    logger.info(f"Phenotype Enrichment completed in {total_time:.2f}s - {result.total_enriched_phenotypes} enriched phenotypes")

    return PhenotypeEnrichmentResponse(
        success=True,
        result=result,
        warnings=warnings,
    )
