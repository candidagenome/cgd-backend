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
import re
from typing import Optional
from collections import defaultdict

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, or_, distinct

from cgd.schemas.virulence_schema import (
    VIRULENCE_CATEGORIES,
    PHENOTYPE_EVIDENCE_TIERS,
    HOUSEKEEPING_GO_TERMS,
    EVIDENCE_WEIGHTS,
    EVIDENCE_TYPES,
    get_confidence_tier,
    extract_evidence_types,
    generate_inclusion_reason,
    split_evidence,
    generate_summary,
    generate_summary_full,
    generate_evidence_breakdown,
    calculate_importance_level,
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
    HomologyGroup,
    FeatHomology,
    RefLink,
    Reference,
    Dbxref,
    DbxrefFeat,
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


# =============================================================================
# EVIDENCE TIER AND SCORING FUNCTIONS
# =============================================================================


def _classify_phenotype_tier(observable: str) -> tuple[int, str]:
    """
    Classify phenotype observable into evidence tiers (1=best, 4=weakest).

    Args:
        observable: The phenotype observable string

    Returns:
        Tuple of (tier_number, tier_name)
    """
    observable_lower = observable.lower()

    for tier_num in sorted(PHENOTYPE_EVIDENCE_TIERS.keys()):
        tier_config = PHENOTYPE_EVIDENCE_TIERS[tier_num]
        for pattern in tier_config["patterns"]:
            # Convert SQL LIKE pattern to regex
            regex_pattern = pattern.replace("%", ".*")
            if re.search(regex_pattern, observable_lower):
                return tier_num, tier_config["name"]

    return 4, "Indirect"


def _is_housekeeping_gene(db: Session, feature: Feature) -> tuple[bool, Optional[str]]:
    """
    Detect if gene is likely housekeeping/essential.

    Methods:
    1. GO annotation to housekeeping terms (translation, DNA replication, etc.)
    2. Conserved across all 5 Candida species (ortholog groups)

    Args:
        db: Database session
        feature: The feature to check

    Returns:
        Tuple of (is_housekeeping, reason_string)
    """
    # Method 1: Check GO annotations for housekeeping terms
    housekeeping_goids = [_convert_goid_to_int(g) for g in HOUSEKEEPING_GO_TERMS]
    housekeeping_goids = [g for g in housekeeping_goids if g is not None]

    if housekeeping_goids:
        go_match = (
            db.query(Go.go_term)
            .join(GoAnnotation, GoAnnotation.go_no == Go.go_no)
            .filter(GoAnnotation.feature_no == feature.feature_no)
            .filter(Go.goid.in_(housekeeping_goids))
            .first()
        )

        if go_match:
            return True, f"GO: {go_match[0]}"

    # Method 2: Check ortholog conservation across Candida species
    ortholog_count = _get_ortholog_count(db, feature)
    if ortholog_count >= 5:
        return True, f"Conserved in {ortholog_count} Candida species"

    return False, None


def _get_ortholog_count(db: Session, feature: Feature) -> int:
    """
    Count how many Candida species have orthologs of this gene.

    Args:
        db: Database session
        feature: The feature to check

    Returns:
        Number of distinct Candida species with orthologs
    """
    # Find homology groups this feature belongs to
    homology_group_nos = (
        db.query(FeatHomology.homology_group_no)
        .filter(FeatHomology.feature_no == feature.feature_no)
        .all()
    )

    if not homology_group_nos:
        return 0

    hg_nos = [h[0] for h in homology_group_nos]

    # Count distinct organisms in these homology groups
    organism_count = (
        db.query(func.count(distinct(Feature.organism_no)))
        .join(FeatHomology, FeatHomology.feature_no == Feature.feature_no)
        .join(HomologyGroup, FeatHomology.homology_group_no == HomologyGroup.homology_group_no)
        .filter(FeatHomology.homology_group_no.in_(hg_nos))
        .filter(HomologyGroup.method == 'CGOB')
        .filter(HomologyGroup.homology_group_type == 'ortholog')
        .scalar()
    )

    return organism_count or 0


def _get_ortholog_details(
    db: Session,
    feature: Feature,
) -> list[dict]:
    """
    Get ortholog details for a feature across Candida species.

    Returns list of dicts with organism_abbrev, gene_name, feature_name for each ortholog.
    Excludes the query gene itself.
    """
    # Find homology groups this feature belongs to
    homology_group_nos = (
        db.query(FeatHomology.homology_group_no)
        .filter(FeatHomology.feature_no == feature.feature_no)
        .all()
    )

    if not homology_group_nos:
        return []

    hg_nos = [h[0] for h in homology_group_nos]

    # Get all orthologs in these groups (excluding the query feature)
    orthologs = (
        db.query(
            Feature.feature_name,
            Feature.gene_name,
            Organism.organism_abbrev,
            Organism.organism_name,
        )
        .join(FeatHomology, FeatHomology.feature_no == Feature.feature_no)
        .join(HomologyGroup, FeatHomology.homology_group_no == HomologyGroup.homology_group_no)
        .join(Organism, Feature.organism_no == Organism.organism_no)
        .filter(FeatHomology.homology_group_no.in_(hg_nos))
        .filter(HomologyGroup.method == 'CGOB')
        .filter(HomologyGroup.homology_group_type == 'ortholog')
        .filter(Feature.feature_no != feature.feature_no)  # Exclude query gene
        .distinct()
        .all()
    )

    # Group by organism and pick one representative gene per organism
    organism_orthologs = {}
    for feat_name, gene_name, org_abbrev, org_name in orthologs:
        if org_abbrev not in organism_orthologs:
            organism_orthologs[org_abbrev] = {
                "organism_abbrev": org_abbrev,
                "organism_name": org_name,
                "gene_name": gene_name or feat_name,
                "feature_name": feat_name,
            }

    return list(organism_orthologs.values())


def _get_paper_count_and_pmids(
    db: Session,
    feature: Feature,
) -> tuple[int, list[int]]:
    """
    Get paper count and PMID list for a feature.

    Args:
        db: Database session
        feature: The feature to get papers for

    Returns:
        Tuple of (paper_count, pmid_list) - all PMIDs sorted by recency
    """
    # Query references via RefLink
    refs = (
        db.query(Reference.pubmed)
        .join(RefLink, RefLink.reference_no == Reference.reference_no)
        .filter(
            RefLink.tab_name == "FEATURE",
            RefLink.primary_key == feature.feature_no,
            Reference.pubmed.isnot(None),
        )
        .distinct()
        .all()
    )

    pmids = [r[0] for r in refs if r[0] is not None]
    paper_count = len(pmids)

    # Sort by PMID descending (most recent first)
    pmids_sorted = sorted(pmids, reverse=True)

    return paper_count, pmids_sorted


def _get_uniprot_and_alphafold(
    db: Session,
    feature: Feature,
) -> tuple[Optional[str], Optional[str]]:
    """
    Get UniProt ID and AlphaFold URL for a feature.

    Prefers SwissProt over TrEMBL entries.

    Args:
        db: Database session
        feature: The feature to get UniProt for

    Returns:
        Tuple of (uniprot_id, alphafold_url) - both may be None
    """
    # Query for UniProt IDs (source='EBI', type in SwissProt/TrEMBL)
    uniprot_entries = (
        db.query(Dbxref.dbxref_id, Dbxref.dbxref_type)
        .join(DbxrefFeat, DbxrefFeat.dbxref_no == Dbxref.dbxref_no)
        .filter(
            DbxrefFeat.feature_no == feature.feature_no,
            Dbxref.source == 'EBI',
            Dbxref.dbxref_type.in_(['SwissProt', 'TrEMBL']),
        )
        .all()
    )

    if not uniprot_entries:
        return None, None

    # Prefer SwissProt over TrEMBL
    uniprot_id = None
    for entry_id, entry_type in uniprot_entries:
        if entry_type == 'SwissProt':
            uniprot_id = entry_id
            break
        elif entry_type == 'TrEMBL' and uniprot_id is None:
            uniprot_id = entry_id

    if uniprot_id:
        alphafold_url = f"https://alphafold.ebi.ac.uk/entry/{uniprot_id}"
        return uniprot_id, alphafold_url

    return None, None


def _calculate_confidence_score(
    match_reasons: list[str],
    evidence_tier: int,
    is_housekeeping: bool,
) -> int:
    """
    Calculate confidence score (0-20 range) based on evidence quality.

    Manual GO evidence codes (IDA, IMP, IGI, IPI, etc.) are weighted higher
    than computational evidence (IEA).

    Args:
        match_reasons: List of match reason strings
        evidence_tier: The best evidence tier (1-4)
        is_housekeeping: Whether the gene is a housekeeping gene

    Returns:
        Confidence score (0-20)
    """
    # Manual GO evidence codes (experimental and author statement)
    MANUAL_GO_CODES = {'IDA', 'IMP', 'IGI', 'IPI', 'IEP', 'TAS', 'NAS', 'IC', 'EXP'}

    score = 0

    for reason in match_reasons:
        reason_lower = reason.lower()

        if "phenotype:" in reason_lower:
            # Check if it's a virulence phenotype (tier 1)
            if "virulence" in reason_lower:
                score += EVIDENCE_WEIGHTS["virulence_model"]
            elif evidence_tier == 1:
                score += EVIDENCE_WEIGHTS["tier1_phenotype"]
            elif evidence_tier == 2:
                score += EVIDENCE_WEIGHTS["tier2_phenotype"]
            # Tier 3 and 4 phenotypes don't add points
        elif reason_lower.startswith("go:"):
            # Extract evidence code if present (format: "GO: term (GO:XXXXXXX) [IDA]")
            evidence_code = None
            if "[" in reason and "]" in reason:
                code_start = reason.rfind("[") + 1
                code_end = reason.rfind("]")
                evidence_code = reason[code_start:code_end].upper()

            # Check for host interaction GO terms (symbiont-host related)
            is_virulence_go = any(t in reason_lower for t in ["host", "symbiont", "interaction", "evasion"])

            if is_virulence_go:
                # Virulence-related GO terms get higher weight
                if evidence_code and evidence_code in MANUAL_GO_CODES:
                    score += EVIDENCE_WEIGHTS["virulence_go"] + 1  # +4 for manual
                else:
                    score += EVIDENCE_WEIGHTS["virulence_go"]  # +3 for IEA
            else:
                # Other GO terms (MF, etc.) - still valuable with manual evidence
                if evidence_code and evidence_code in MANUAL_GO_CODES:
                    score += 2  # Manual non-virulence GO
                else:
                    score += 1  # IEA non-virulence GO
        elif "literature topic: disease" in reason_lower:
            score += EVIDENCE_WEIGHTS["disease_literature"]
        elif "gene pattern:" in reason_lower:
            score += EVIDENCE_WEIGHTS["gene_pattern"]
        elif "headline:" in reason_lower:
            score += EVIDENCE_WEIGHTS["keyword_match"]

    if is_housekeeping:
        score += EVIDENCE_WEIGHTS["housekeeping_penalty"]

    return max(0, min(20, score))  # Clamp to 0-20 range


def _get_best_evidence_tier(match_reasons: list[str]) -> tuple[int, str]:
    """
    Determine the best (lowest number) evidence tier from match reasons.

    Args:
        match_reasons: List of match reason strings

    Returns:
        Tuple of (best_tier_number, tier_name)
    """
    best_tier = 4
    best_tier_name = "Indirect"

    for reason in match_reasons:
        if reason.startswith("phenotype:"):
            observable = reason.replace("phenotype: ", "")
            tier, tier_name = _classify_phenotype_tier(observable)
            if tier < best_tier:
                best_tier = tier
                best_tier_name = tier_name
        elif reason.startswith("virulence model:"):
            # Virulence model evidence is tier 1
            if 1 < best_tier:
                best_tier = 1
                best_tier_name = "Direct Virulence"

    return best_tier, best_tier_name


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

    Excludes phenotypes with "Normal" qualifier since these indicate
    no effect on the phenotype (e.g., "virulence: Normal" means no virulence defect).

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
            # Exclude "Normal" qualifier - these indicate no phenotypic effect
            .filter(
                or_(
                    Phenotype.qualifier.is_(None),
                    func.upper(Phenotype.qualifier) != 'NORMAL'
                )
            )
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

    Returns GO annotations with evidence codes. Manual evidence codes
    (IDA, IMP, IGI, IPI, etc.) are considered stronger than computational (IEA).

    Args:
        db: Database session
        go_ids: List of GO IDs (e.g., ["GO:0007155", "GO:0044406"])
        organism_abbrevs: Optional list of organism abbreviations to filter

    Returns:
        List of (Feature, match_reason) tuples with evidence code in reason
    """
    results = []

    # Convert GO IDs to integers
    goids = [_convert_goid_to_int(g) for g in go_ids]
    goids = [g for g in goids if g is not None]

    if not goids:
        return results

    query = (
        db.query(Feature, Go.goid, Go.go_term, GoAnnotation.go_evidence)
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

    for feature, goid, go_term, evidence_code in query.all():
        # Include evidence code in the match reason
        evidence_str = f" [{evidence_code}]" if evidence_code else ""
        results.append((feature, f"GO: {go_term} (GO:{goid:07d}){evidence_str}"))

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
    Query features that have virulence phenotype annotations.

    Looks for phenotype annotations where the observable contains "virulence"
    and the qualifier is NOT "Normal" (which would indicate no virulence defect).

    Note: "mouse", "galleria" etc. are virulence MODEL types, not observables.
    The observable is "virulence" and the model type is stored in experiment properties.

    Args:
        db: Database session
        organism_abbrevs: Optional list of organism abbreviations to filter

    Returns:
        List of (Feature, match_reason) tuples
    """
    results = []
    seen_features = set()  # Track to avoid duplicates

    # Search for virulence observables with non-Normal qualifier
    query = (
        db.query(Feature, Phenotype.observable)
        .join(PhenoAnnotation, PhenoAnnotation.feature_no == Feature.feature_no)
        .join(Phenotype, PhenoAnnotation.phenotype_no == Phenotype.phenotype_no)
        .join(Feature.organism)
        .filter(func.lower(Feature.feature_type) == 'orf')
        .filter(func.upper(Phenotype.observable).like('%VIRULENCE%'))
        # Exclude "Normal" qualifier - these indicate no virulence defect
        .filter(
            or_(
                Phenotype.qualifier.is_(None),
                func.upper(Phenotype.qualifier) != 'NORMAL'
            )
        )
        .distinct()
    )

    if organism_abbrevs:
        query = query.filter(
            func.upper(Organism.organism_abbrev).in_([o.upper() for o in organism_abbrevs])
        )

    for feature, observable in query.all():
        if feature.feature_no not in seen_features:
            seen_features.add(feature.feature_no)
            results.append((feature, f"phenotype: {observable}"))

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
    max_evidence_tier: Optional[int] = None,
    min_confidence_score: Optional[int] = None,
    hide_housekeeping: bool = False,
    sort_by: str = "confidence_score",
    sort_order: str = "desc",
    evidence_types: Optional[list[str]] = None,
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
        max_evidence_tier: Only include tiers <= this value (1=best, 4=weakest)
        min_confidence_score: Only include scores >= this value
        hide_housekeeping: If True, exclude housekeeping genes
        sort_by: Field to sort by ("confidence_score", "gene_name", "evidence_tier")
        sort_order: Sort order ("asc" or "desc")
        evidence_types: Filter by evidence types (GO, PHE, KW)

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

    # Compute evidence quality fields for each gene
    for feature_no, data in gene_data.items():
        feature = data["feature"]
        match_reasons_list = list(data["match_reasons"])

        # Determine best evidence tier from phenotype matches
        evidence_tier, evidence_tier_name = _get_best_evidence_tier(match_reasons_list)
        data["evidence_tier"] = evidence_tier
        data["evidence_tier_name"] = evidence_tier_name

        # Check housekeeping status
        is_hk, hk_reason = _is_housekeeping_gene(db, feature)
        data["is_housekeeping"] = is_hk
        data["housekeeping_reason"] = hk_reason

        # Get ortholog count and details
        data["ortholog_count"] = _get_ortholog_count(db, feature)
        data["orthologs"] = _get_ortholog_details(db, feature)

        # Calculate confidence score
        data["confidence_score"] = _calculate_confidence_score(
            match_reasons_list, evidence_tier, is_hk
        )

        # Compute quick win fields
        data["confidence_tier"] = get_confidence_tier(data["confidence_score"])
        data["evidence_types"] = extract_evidence_types(match_reasons_list)
        data["inclusion_reason"] = generate_inclusion_reason(
            match_reasons_list, sorted(data["categories"])
        )

        # Get paper count and PMIDs
        paper_count, pmids = _get_paper_count_and_pmids(db, feature)
        data["paper_count"] = paper_count
        data["pmids"] = pmids

        # Get UniProt ID and AlphaFold URL
        uniprot_id, alphafold_url = _get_uniprot_and_alphafold(db, feature)
        data["uniprot_id"] = uniprot_id
        data["alphafold_url"] = alphafold_url

        # Split evidence into direct and indirect
        direct_evidence, indirect_evidence = split_evidence(match_reasons_list)
        data["direct_evidence"] = direct_evidence
        data["indirect_evidence"] = indirect_evidence

    # Apply evidence quality filters
    if max_evidence_tier is not None:
        gene_data = {
            k: v for k, v in gene_data.items()
            if v["evidence_tier"] <= max_evidence_tier
        }

    if min_confidence_score is not None:
        gene_data = {
            k: v for k, v in gene_data.items()
            if v["confidence_score"] >= min_confidence_score
        }

    if hide_housekeeping:
        gene_data = {
            k: v for k, v in gene_data.items()
            if not v["is_housekeeping"]
        }

    # Filter by evidence types (GO, PHE, KW)
    if evidence_types:
        evidence_types_upper = [et.upper() for et in evidence_types]
        gene_data = {
            k: v for k, v in gene_data.items()
            if any(et in evidence_types_upper for et in v["evidence_types"])
        }

    # Convert to list
    gene_list = []
    for feature_no, data in gene_data.items():
        feature = data["feature"]
        # Need to load organism relationship
        organism = db.query(Organism).filter(Organism.organism_no == feature.organism_no).first()

        # Generate summary, importance, and evidence breakdown
        display_name = feature.gene_name or feature.feature_name
        cats_sorted = sorted(data["categories"])

        summary = generate_summary(
            gene_name=display_name,
            categories=cats_sorted,
            direct_evidence=data["direct_evidence"],
            indirect_evidence=data["indirect_evidence"],
            headline=feature.headline,
            confidence_tier=data["confidence_tier"],
            paper_count=data["paper_count"],
        )
        summary_full = generate_summary_full(
            gene_name=display_name,
            categories=cats_sorted,
            direct_evidence=data["direct_evidence"],
            indirect_evidence=data["indirect_evidence"],
            headline=feature.headline,
            confidence_tier=data["confidence_tier"],
            paper_count=data["paper_count"],
        )
        importance_level, importance_label = calculate_importance_level(
            direct_evidence=data["direct_evidence"],
            indirect_evidence=data["indirect_evidence"],
            paper_count=data["paper_count"],
            confidence_score=data["confidence_score"],
        )
        evidence_breakdown = generate_evidence_breakdown(
            direct_evidence=data["direct_evidence"],
            indirect_evidence=data["indirect_evidence"],
            paper_count=data["paper_count"],
            confidence_score=data["confidence_score"],
        )

        gene_list.append(VirulenceFactor(
            feature_no=feature.feature_no,
            feature_name=feature.feature_name,
            gene_name=feature.gene_name,
            organism=organism.organism_name if organism else "Unknown",
            organism_abbrev=organism.organism_abbrev if organism else "",
            headline=feature.headline,
            description=feature.headline,  # Use headline as description
            categories=cats_sorted,
            match_reasons=sorted(data["match_reasons"]),
            evidence_tier=data["evidence_tier"],
            evidence_tier_name=data["evidence_tier_name"],
            confidence_score=data["confidence_score"],
            confidence_tier=data["confidence_tier"],
            is_housekeeping=data["is_housekeeping"],
            housekeeping_reason=data["housekeeping_reason"],
            ortholog_count=data["ortholog_count"],
            inclusion_reason=data["inclusion_reason"],
            evidence_types=data["evidence_types"],
            paper_count=data["paper_count"],
            pmids=data["pmids"],
            direct_evidence=data["direct_evidence"],
            indirect_evidence=data["indirect_evidence"],
            summary=summary,
            summary_full=summary_full,
            evidence_breakdown=evidence_breakdown,
            importance_level=importance_level,
            importance_label=importance_label,
            uniprot_id=data.get("uniprot_id"),
            alphafold_url=data.get("alphafold_url"),
            orthologs=data.get("orthologs", []),
        ))

    # Sort results
    reverse_sort = sort_order.lower() == "desc"
    if sort_by == "confidence_score":
        gene_list.sort(key=lambda g: (g.confidence_score, g.gene_name or g.feature_name or ""),
                       reverse=reverse_sort)
    elif sort_by == "evidence_tier":
        # Lower tier is better, so reverse the reverse for ascending
        gene_list.sort(key=lambda g: (g.evidence_tier, g.gene_name or g.feature_name or ""),
                       reverse=not reverse_sort if sort_order.lower() == "desc" else reverse_sort)
    else:  # Default to gene_name
        gene_list.sort(key=lambda g: (g.gene_name or g.feature_name or "").lower(),
                       reverse=reverse_sort)

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
