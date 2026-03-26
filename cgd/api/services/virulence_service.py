"""
Virulence Factor Service - Business logic for querying virulence-related genes.

This service dynamically maps existing data to virulence categories:
- Gene name patterns → categories (e.g., ALS* → Adhesins, SAP* → Secreted Enzymes)
- Phenotype observables → categories (e.g., "biofilm formation" → Biofilm)
- GO annotations → categories (e.g., pathogenesis GO terms → Host Interaction)
- Literature topics → categories (e.g., Disease topics → Host Interaction)
"""
from __future__ import annotations

import logging
from typing import Optional
from collections import defaultdict

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, or_, distinct

from cgd.schemas.virulence_schema import (
    VIRULENCE_CATEGORIES,
    VirulenceCategory,
    VirulenceCategoriesResponse,
    VirulenceFactor,
    VirulenceFactorsResponse,
    VirulenceFactorDetail,
    VirulenceCategoryMatch,
    VirulenceStats,
    VirulenceCategoryStats,
    VirulenceOrganismStats,
)
from cgd.models.models import (
    Feature,
    Organism,
    PhenoAnnotation,
    Phenotype,
    GoAnnotation,
    Go,
    RefProperty,
    RefpropFeat,
    ExptProperty,
    ExptExptprop,
)

logger = logging.getLogger(__name__)


def _convert_goid_to_int(goid_str: str) -> Optional[int]:
    """Convert GO:XXXXXXX format to integer."""
    if goid_str.startswith("GO:"):
        try:
            return int(goid_str[3:])
        except ValueError:
            return None
    return None


def _query_by_gene_patterns(
    db: Session,
    patterns: list[str],
    organism_abbrevs: Optional[list[str]] = None,
) -> list[tuple[Feature, str]]:
    """
    Query features matching gene name patterns.

    Args:
        db: Database session
        patterns: List of SQL LIKE patterns (e.g., ["ALS%", "HWP%"])
        organism_abbrevs: Optional list of organism abbreviations to filter

    Returns:
        List of (Feature, matched_pattern) tuples
    """
    results = []

    for pattern in patterns:
        query = (
            db.query(Feature)
            .join(Feature.organism)
            .filter(func.lower(Feature.feature_type) == 'orf')
            .filter(func.upper(Feature.gene_name).like(func.upper(pattern)))
        )

        if organism_abbrevs:
            query = query.filter(
                func.upper(Organism.organism_abbrev).in_([o.upper() for o in organism_abbrevs])
            )

        features = query.all()
        for f in features:
            results.append((f, f"gene pattern: {f.gene_name}"))

    return results


def _query_by_phenotype_observables(
    db: Session,
    observables: list[str],
    organism_abbrevs: Optional[list[str]] = None,
) -> list[tuple[Feature, str]]:
    """
    Query features with phenotype annotations matching observable patterns.

    Args:
        db: Database session
        observables: List of SQL LIKE patterns for observables
        organism_abbrevs: Optional list of organism abbreviations to filter

    Returns:
        List of (Feature, match_reason) tuples
    """
    results = []

    for obs_pattern in observables:
        query = (
            db.query(Feature, Phenotype.observable)
            .join(PhenoAnnotation, PhenoAnnotation.feature_no == Feature.feature_no)
            .join(Phenotype, PhenoAnnotation.phenotype_no == Phenotype.phenotype_no)
            .join(Feature.organism)
            .filter(func.lower(Feature.feature_type) == 'orf')
            .filter(func.upper(Phenotype.observable).like(func.upper(obs_pattern)))
            .distinct()
        )

        if organism_abbrevs:
            query = query.filter(
                func.upper(Organism.organism_abbrev).in_([o.upper() for o in organism_abbrevs])
            )

        for feature, observable in query.all():
            results.append((feature, f"phenotype: {observable}"))

    return results


def _query_by_go_terms(
    db: Session,
    go_ids: list[str],
    organism_abbrevs: Optional[list[str]] = None,
) -> list[tuple[Feature, str]]:
    """
    Query features with GO annotations matching the given GO IDs.

    Args:
        db: Database session
        go_ids: List of GO IDs (e.g., ["GO:0007155", "GO:0044406"])
        organism_abbrevs: Optional list of organism abbreviations to filter

    Returns:
        List of (Feature, match_reason) tuples
    """
    results = []

    # Convert GO IDs to integers
    goids = [_convert_goid_to_int(g) for g in go_ids]
    goids = [g for g in goids if g is not None]

    if not goids:
        return results

    query = (
        db.query(Feature, Go.goid, Go.go_term)
        .join(GoAnnotation, GoAnnotation.feature_no == Feature.feature_no)
        .join(Go, GoAnnotation.go_no == Go.go_no)
        .join(Feature.organism)
        .filter(func.lower(Feature.feature_type) == 'orf')
        .filter(Go.goid.in_(goids))
        .distinct()
    )

    if organism_abbrevs:
        query = query.filter(
            func.upper(Organism.organism_abbrev).in_([o.upper() for o in organism_abbrevs])
        )

    for feature, goid, go_term in query.all():
        results.append((feature, f"GO: {go_term} (GO:{goid:07d})"))

    return results


def _query_by_headline_patterns(
    db: Session,
    patterns: list[str],
    organism_abbrevs: Optional[list[str]] = None,
) -> list[tuple[Feature, str]]:
    """
    Query features with headlines matching patterns.

    Args:
        db: Database session
        patterns: List of SQL LIKE patterns for headlines
        organism_abbrevs: Optional list of organism abbreviations to filter

    Returns:
        List of (Feature, match_reason) tuples
    """
    results = []

    for pattern in patterns:
        query = (
            db.query(Feature)
            .join(Feature.organism)
            .filter(func.lower(Feature.feature_type) == 'orf')
            .filter(func.upper(Feature.headline).like(func.upper(pattern)))
        )

        if organism_abbrevs:
            query = query.filter(
                func.upper(Organism.organism_abbrev).in_([o.upper() for o in organism_abbrevs])
            )

        for feature in query.all():
            results.append((feature, f"headline: {feature.headline[:50]}..."))

    return results


def _query_by_virulence_model(
    db: Session,
    organism_abbrevs: Optional[list[str]] = None,
) -> list[tuple[Feature, str]]:
    """
    Query features that have been tested in virulence/animal models.
    This looks for phenotype annotations with experiment properties indicating
    virulence model testing.

    Args:
        db: Database session
        organism_abbrevs: Optional list of organism abbreviations to filter

    Returns:
        List of (Feature, match_reason) tuples
    """
    # Look for phenotype annotations with experiment type containing "virulence"
    # or observables like "virulence" or property values like "mouse model"
    virulence_patterns = ["%virulence%", "%mouse%", "%animal model%", "%galleria%"]

    results = []

    for pattern in virulence_patterns:
        # Search in observables
        query = (
            db.query(Feature, Phenotype.observable)
            .join(PhenoAnnotation, PhenoAnnotation.feature_no == Feature.feature_no)
            .join(Phenotype, PhenoAnnotation.phenotype_no == Phenotype.phenotype_no)
            .join(Feature.organism)
            .filter(func.lower(Feature.feature_type) == 'orf')
            .filter(func.upper(Phenotype.observable).like(func.upper(pattern)))
            .distinct()
        )

        if organism_abbrevs:
            query = query.filter(
                func.upper(Organism.organism_abbrev).in_([o.upper() for o in organism_abbrevs])
            )

        for feature, observable in query.all():
            results.append((feature, f"virulence model: {observable}"))

    # Also search in experiment properties
    query = (
        db.query(Feature, ExptProperty.property_value)
        .join(PhenoAnnotation, PhenoAnnotation.feature_no == Feature.feature_no)
        .join(ExptExptprop, ExptExptprop.experiment_no == PhenoAnnotation.experiment_no)
        .join(ExptProperty, ExptExptprop.expt_property_no == ExptProperty.expt_property_no)
        .join(Feature.organism)
        .filter(func.lower(Feature.feature_type) == 'orf')
        .filter(
            or_(
                func.upper(ExptProperty.property_value).like('%VIRULENCE%'),
                func.upper(ExptProperty.property_value).like('%MOUSE%'),
                func.upper(ExptProperty.property_value).like('%GALLERIA%'),
            )
        )
        .distinct()
    )

    if organism_abbrevs:
        query = query.filter(
            func.upper(Organism.organism_abbrev).in_([o.upper() for o in organism_abbrevs])
        )

    for feature, prop_value in query.all():
        results.append((feature, f"virulence model: {prop_value[:50]}"))

    return results


def _query_by_literature_topics(
    db: Session,
    topics: list[str],
    organism_abbrevs: Optional[list[str]] = None,
) -> list[tuple[Feature, str]]:
    """
    Query features linked to references with specific topics.

    Args:
        db: Database session
        topics: List of topic names (e.g., ["Disease"])
        organism_abbrevs: Optional list of organism abbreviations to filter

    Returns:
        List of (Feature, match_reason) tuples
    """
    results = []

    query = (
        db.query(Feature, RefProperty.property_value)
        .join(RefpropFeat, RefpropFeat.feature_no == Feature.feature_no)
        .join(RefProperty, RefpropFeat.ref_property_no == RefProperty.ref_property_no)
        .join(Feature.organism)
        .filter(func.lower(Feature.feature_type) == 'orf')
        .filter(func.upper(RefProperty.property_type) == 'TOPIC')
        .filter(func.upper(RefProperty.property_value).in_([t.upper() for t in topics]))
        .distinct()
    )

    if organism_abbrevs:
        query = query.filter(
            func.upper(Organism.organism_abbrev).in_([o.upper() for o in organism_abbrevs])
        )

    for feature, topic in query.all():
        results.append((feature, f"literature topic: {topic}"))

    return results


def get_virulence_categories(
    db: Session,
    organism: Optional[str] = None,
) -> VirulenceCategoriesResponse:
    """
    Get all virulence categories with gene counts.

    Args:
        db: Database session
        organism: Optional organism abbreviation to filter counts

    Returns:
        VirulenceCategoriesResponse with category list and total gene count
    """
    organism_filter = [organism] if organism else None
    all_genes: set[int] = set()
    categories = []

    for cat_key, cat_config in VIRULENCE_CATEGORIES.items():
        category_genes: set[int] = set()
        rules = cat_config.get("rules", {})

        # Query by gene patterns
        if "gene_patterns" in rules:
            for feature, _ in _query_by_gene_patterns(db, rules["gene_patterns"], organism_filter):
                category_genes.add(feature.feature_no)

        # Query by phenotype observables
        if "phenotype_observables" in rules:
            for feature, _ in _query_by_phenotype_observables(db, rules["phenotype_observables"], organism_filter):
                category_genes.add(feature.feature_no)

        # Query by GO terms
        if "go_terms" in rules:
            for feature, _ in _query_by_go_terms(db, rules["go_terms"], organism_filter):
                category_genes.add(feature.feature_no)

        # Query by headlines
        if "headlines" in rules:
            for feature, _ in _query_by_headline_patterns(db, rules["headlines"], organism_filter):
                category_genes.add(feature.feature_no)

        # Query by virulence model
        if rules.get("phenotype_has_virulence_model"):
            for feature, _ in _query_by_virulence_model(db, organism_filter):
                category_genes.add(feature.feature_no)

        # Query by literature topics
        if "literature_topics" in rules:
            for feature, _ in _query_by_literature_topics(db, rules["literature_topics"], organism_filter):
                category_genes.add(feature.feature_no)

        all_genes.update(category_genes)
        categories.append(VirulenceCategory(
            key=cat_key,
            name=cat_config["name"],
            description=cat_config["description"],
            count=len(category_genes),
        ))

    return VirulenceCategoriesResponse(
        categories=categories,
        total_genes=len(all_genes),
    )


def get_virulence_factors(
    db: Session,
    categories: Optional[list[str]] = None,
    organisms: Optional[list[str]] = None,
    search_term: Optional[str] = None,
    page: int = 1,
    page_size: int = 25,
) -> VirulenceFactorsResponse:
    """
    Search virulence factors by criteria.

    Args:
        db: Database session
        categories: List of category keys to filter
        organisms: List of organism abbreviations to filter
        search_term: Keyword search for gene name or headline
        page: Page number (1-indexed)
        page_size: Results per page

    Returns:
        VirulenceFactorsResponse with paginated results
    """
    # If no categories specified, return empty
    if not categories:
        return VirulenceFactorsResponse(
            items=[],
            total_count=0,
            page=page,
            page_size=page_size,
            categories_searched=[],
        )

    # Collect genes matching each category
    # gene_data: feature_no -> {feature, categories, match_reasons}
    gene_data: dict[int, dict] = {}

    for cat_key in categories:
        if cat_key not in VIRULENCE_CATEGORIES:
            continue

        cat_config = VIRULENCE_CATEGORIES[cat_key]
        rules = cat_config.get("rules", {})

        def add_gene(feature: Feature, match_reason: str, category_name: str):
            """Helper to add or update gene in results."""
            if feature.feature_no not in gene_data:
                gene_data[feature.feature_no] = {
                    "feature": feature,
                    "categories": set(),
                    "match_reasons": set(),
                }
            gene_data[feature.feature_no]["categories"].add(category_name)
            gene_data[feature.feature_no]["match_reasons"].add(match_reason)

        cat_name = cat_config["name"]

        # Query by gene patterns
        if "gene_patterns" in rules:
            for feature, reason in _query_by_gene_patterns(db, rules["gene_patterns"], organisms):
                add_gene(feature, reason, cat_name)

        # Query by phenotype observables
        if "phenotype_observables" in rules:
            for feature, reason in _query_by_phenotype_observables(db, rules["phenotype_observables"], organisms):
                add_gene(feature, reason, cat_name)

        # Query by GO terms
        if "go_terms" in rules:
            for feature, reason in _query_by_go_terms(db, rules["go_terms"], organisms):
                add_gene(feature, reason, cat_name)

        # Query by headlines
        if "headlines" in rules:
            for feature, reason in _query_by_headline_patterns(db, rules["headlines"], organisms):
                add_gene(feature, reason, cat_name)

        # Query by virulence model
        if rules.get("phenotype_has_virulence_model"):
            for feature, reason in _query_by_virulence_model(db, organisms):
                add_gene(feature, reason, cat_name)

        # Query by literature topics
        if "literature_topics" in rules:
            for feature, reason in _query_by_literature_topics(db, rules["literature_topics"], organisms):
                add_gene(feature, reason, cat_name)

    # Apply search term filter
    if search_term:
        search_lower = search_term.lower()
        filtered_data = {}
        for feature_no, data in gene_data.items():
            feature = data["feature"]
            if (
                (feature.gene_name and search_lower in feature.gene_name.lower()) or
                (feature.feature_name and search_lower in feature.feature_name.lower()) or
                (feature.headline and search_lower in feature.headline.lower())
            ):
                filtered_data[feature_no] = data
        gene_data = filtered_data

    # Convert to list and sort
    gene_list = []
    for feature_no, data in gene_data.items():
        feature = data["feature"]
        # Need to load organism relationship
        organism = db.query(Organism).filter(Organism.organism_no == feature.organism_no).first()

        gene_list.append(VirulenceFactor(
            feature_no=feature.feature_no,
            feature_name=feature.feature_name,
            gene_name=feature.gene_name,
            organism=organism.organism_name if organism else "Unknown",
            organism_abbrev=organism.organism_abbrev if organism else "",
            headline=feature.headline,
            description=feature.headline,  # Use headline as description
            categories=sorted(data["categories"]),
            match_reasons=sorted(data["match_reasons"]),
        ))

    # Sort by gene name
    gene_list.sort(key=lambda g: (g.gene_name or g.feature_name or "").lower())

    total_count = len(gene_list)

    # Apply pagination
    offset = (page - 1) * page_size
    paginated = gene_list[offset:offset + page_size]

    return VirulenceFactorsResponse(
        items=paginated,
        total_count=total_count,
        page=page,
        page_size=page_size,
        categories_searched=categories,
    )


def get_virulence_factor_detail(
    db: Session,
    gene_name: str,
) -> Optional[VirulenceFactorDetail]:
    """
    Get detailed virulence information for a specific gene.

    Args:
        db: Database session
        gene_name: Gene name or systematic name

    Returns:
        VirulenceFactorDetail or None if not found
    """
    # Find the feature
    feature = (
        db.query(Feature)
        .options(joinedload(Feature.organism))
        .filter(
            or_(
                func.upper(Feature.feature_name) == func.upper(gene_name),
                func.upper(Feature.gene_name) == func.upper(gene_name),
            )
        )
        .first()
    )

    if not feature:
        return None

    # Check which categories this gene matches
    category_matches = []

    for cat_key, cat_config in VIRULENCE_CATEGORIES.items():
        rules = cat_config.get("rules", {})

        # Check gene patterns
        if "gene_patterns" in rules and feature.gene_name:
            for pattern in rules["gene_patterns"]:
                sql_pattern = pattern.replace("%", "")
                if feature.gene_name.upper().startswith(sql_pattern.upper()):
                    category_matches.append(VirulenceCategoryMatch(
                        category_key=cat_key,
                        category_name=cat_config["name"],
                        match_type="gene_pattern",
                        match_value=feature.gene_name,
                    ))
                    break

        # Check phenotype observables
        if "phenotype_observables" in rules:
            pheno_matches = (
                db.query(Phenotype.observable)
                .join(PhenoAnnotation, PhenoAnnotation.phenotype_no == Phenotype.phenotype_no)
                .filter(PhenoAnnotation.feature_no == feature.feature_no)
                .distinct()
                .all()
            )
            for (observable,) in pheno_matches:
                for obs_pattern in rules["phenotype_observables"]:
                    sql_pattern = obs_pattern.replace("%", ".*")
                    import re
                    if re.search(sql_pattern, observable, re.IGNORECASE):
                        category_matches.append(VirulenceCategoryMatch(
                            category_key=cat_key,
                            category_name=cat_config["name"],
                            match_type="phenotype",
                            match_value=observable,
                        ))
                        break

        # Check GO terms
        if "go_terms" in rules:
            goids = [_convert_goid_to_int(g) for g in rules["go_terms"]]
            goids = [g for g in goids if g is not None]
            if goids:
                go_matches = (
                    db.query(Go.goid, Go.go_term)
                    .join(GoAnnotation, GoAnnotation.go_no == Go.go_no)
                    .filter(GoAnnotation.feature_no == feature.feature_no)
                    .filter(Go.goid.in_(goids))
                    .distinct()
                    .all()
                )
                for goid, go_term in go_matches:
                    category_matches.append(VirulenceCategoryMatch(
                        category_key=cat_key,
                        category_name=cat_config["name"],
                        match_type="go_term",
                        match_value=f"{go_term} (GO:{goid:07d})",
                    ))

        # Check headlines
        if "headlines" in rules and feature.headline:
            for pattern in rules["headlines"]:
                sql_pattern = pattern.replace("%", ".*")
                import re
                if re.search(sql_pattern, feature.headline, re.IGNORECASE):
                    category_matches.append(VirulenceCategoryMatch(
                        category_key=cat_key,
                        category_name=cat_config["name"],
                        match_type="headline",
                        match_value=feature.headline[:100],
                    ))
                    break

        # Check literature topics
        if "literature_topics" in rules:
            topic_matches = (
                db.query(RefProperty.property_value)
                .join(RefpropFeat, RefpropFeat.ref_property_no == RefProperty.ref_property_no)
                .filter(RefpropFeat.feature_no == feature.feature_no)
                .filter(func.upper(RefProperty.property_type) == 'TOPIC')
                .filter(func.upper(RefProperty.property_value).in_([t.upper() for t in rules["literature_topics"]]))
                .distinct()
                .all()
            )
            for (topic,) in topic_matches:
                category_matches.append(VirulenceCategoryMatch(
                    category_key=cat_key,
                    category_name=cat_config["name"],
                    match_type="literature",
                    match_value=topic,
                ))

    organism = feature.organism

    return VirulenceFactorDetail(
        feature_no=feature.feature_no,
        feature_name=feature.feature_name,
        gene_name=feature.gene_name,
        organism=organism.organism_name if organism else "Unknown",
        organism_abbrev=organism.organism_abbrev if organism else "",
        headline=feature.headline,
        description=feature.headline,
        categories=category_matches,
    )


def get_virulence_stats(db: Session) -> VirulenceStats:
    """
    Get summary statistics for virulence factors.

    Args:
        db: Database session

    Returns:
        VirulenceStats with counts per category and organism
    """
    # Get categories with counts
    categories_response = get_virulence_categories(db)

    category_stats = [
        VirulenceCategoryStats(key=cat.key, name=cat.name, count=cat.count)
        for cat in categories_response.categories
    ]

    # Get all genes and count by organism
    all_categories = list(VIRULENCE_CATEGORIES.keys())
    factors_response = get_virulence_factors(db, categories=all_categories, page_size=10000)

    organism_counts: dict[str, dict] = {}
    for factor in factors_response.items:
        abbrev = factor.organism_abbrev
        if abbrev not in organism_counts:
            organism_counts[abbrev] = {
                "organism_abbrev": abbrev,
                "organism_name": factor.organism,
                "count": 0,
            }
        organism_counts[abbrev]["count"] += 1

    organism_stats = [
        VirulenceOrganismStats(**data)
        for data in sorted(organism_counts.values(), key=lambda x: -x["count"])
    ]

    return VirulenceStats(
        total_genes=categories_response.total_genes,
        categories=category_stats,
        organisms=organism_stats,
    )
