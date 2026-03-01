"""
Phenotype Service - Business logic for phenotype search and observable tree endpoints.
"""
from __future__ import annotations

import logging
from typing import Optional
from collections import defaultdict

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, or_, distinct

from cgd.schemas.phenotype_schema import (
    PhenotypeSearchResponse,
    PhenotypeSearchResult,
    PhenotypeSearchQuery,
    PhenotypeSearchSummaryResponse,
    PhenotypeMatchGroup,
    ReferenceForAnnotation,
    CitationLinkForPhenotype,
    ObservableTreeResponse,
    ObservableTerm,
)
from cgd.models.models import (
    Feature,
    PhenoAnnotation,
    Phenotype,
    Experiment,
    ExptExptprop,
    ExptProperty,
    RefLink,
    RefUrl,
    Reference,
    Organism,
    Cv,
    CvTerm,
    CvtermRelationship,
)

logger = logging.getLogger(__name__)


def _build_citation_links_for_phenotype(ref, ref_urls=None) -> list[CitationLinkForPhenotype]:
    """
    Build citation links for a reference in phenotype annotation context.

    Args:
        ref: Reference object with pubmed and dbxref_id
        ref_urls: Optional list of RefUrl objects for additional links

    Returns:
        List of CitationLinkForPhenotype objects
    """
    links = []

    # CGD Paper link (always present) - always use dbxref_id (CGDID)
    links.append(CitationLinkForPhenotype(
        name="CGD Paper",
        url=f"/reference/{ref.dbxref_id}",
        link_type="internal"
    ))

    # PubMed link (if pubmed ID exists)
    if ref.pubmed:
        links.append(CitationLinkForPhenotype(
            name="PubMed",
            url=f"https://pubmed.ncbi.nlm.nih.gov/{ref.pubmed}",
            link_type="external"
        ))

    # Process URLs from ref_url table (if provided)
    if ref_urls:
        for ref_url in ref_urls:
            url_obj = ref_url.url
            if url_obj and url_obj.url:
                url_type = (url_obj.url_type or "").lower()

                # Skip Reference supplement (displayed separately)
                if "supplement" in url_type:
                    links.append(CitationLinkForPhenotype(
                        name="Reference Supplement",
                        url=url_obj.url,
                        link_type="external"
                    ))
                # Skip Reference Data (not shown as full text)
                elif "reference data" in url_type:
                    continue
                # Download Datasets
                elif any(kw in url_type for kw in ["download", "dataset"]):
                    links.append(CitationLinkForPhenotype(
                        name="Download Datasets",
                        url=url_obj.url,
                        link_type="external"
                    ))
                # All other URL types are shown as Full Text
                else:
                    links.append(CitationLinkForPhenotype(
                        name="Full Text",
                        url=url_obj.url,
                        link_type="external"
                    ))

    return links


def search_phenotypes(
    db: Session,
    query: Optional[str] = None,
    observable: Optional[str] = None,
    qualifier: Optional[str] = None,
    experiment_type: Optional[str] = None,
    mutant_type: Optional[str] = None,
    property_value: Optional[str] = None,
    property_type: Optional[str] = None,
    pubmed: Optional[str] = None,
    organism: Optional[str] = None,
    page: int = 1,
    limit: int = 25,
) -> PhenotypeSearchResponse:
    """
    Search phenotype annotations by criteria.

    Args:
        db: Database session
        query: General keyword search across multiple fields
        observable: Observable term to search for (supports wildcards with *)
        qualifier: Qualifier filter
        experiment_type: Experiment type filter
        mutant_type: Mutant type filter
        property_value: Chemical/condition value search
        property_type: Property type filter
        pubmed: PubMed ID search
        organism: Organism abbreviation filter
        page: Page number (1-indexed)
        limit: Results per page

    Returns:
        PhenotypeSearchResponse with paginated results
    """
    # Build base query joining PhenoAnnotation with related tables
    db_query = (
        db.query(PhenoAnnotation)
        .join(PhenoAnnotation.phenotype)
        .join(PhenoAnnotation.feature)
        .join(Feature.organism)
        .options(
            joinedload(PhenoAnnotation.phenotype),
            joinedload(PhenoAnnotation.feature).joinedload(Feature.organism),
            joinedload(PhenoAnnotation.experiment),
        )
        .filter(func.lower(Feature.feature_type) != 'allele')
    )

    # General keyword search - searches observable and qualifier
    if query:
        query_pattern = f"%{query.replace('*', '%')}%"
        db_query = db_query.filter(
            or_(
                func.upper(Phenotype.observable).like(func.upper(query_pattern)),
                func.upper(Phenotype.qualifier).like(func.upper(query_pattern)),
            )
        )

    # Apply specific filters
    if observable:
        # Support wildcard search with * -> %
        observable_pattern = observable.replace('*', '%')
        if '%' in observable_pattern:
            db_query = db_query.filter(func.upper(Phenotype.observable).like(func.upper(observable_pattern)))
        else:
            db_query = db_query.filter(func.upper(Phenotype.observable) == func.upper(observable))

    if qualifier:
        qualifier_pattern = qualifier.replace('*', '%')
        if '%' in qualifier_pattern:
            db_query = db_query.filter(func.upper(Phenotype.qualifier).like(func.upper(qualifier_pattern)))
        else:
            db_query = db_query.filter(func.upper(Phenotype.qualifier) == func.upper(qualifier))

    if experiment_type:
        exp_pattern = experiment_type.replace('*', '%')
        if '%' in exp_pattern:
            db_query = db_query.filter(func.upper(Phenotype.experiment_type).like(func.upper(exp_pattern)))
        else:
            db_query = db_query.filter(func.upper(Phenotype.experiment_type) == func.upper(experiment_type))

    if mutant_type:
        mut_pattern = mutant_type.replace('*', '%')
        if '%' in mut_pattern:
            db_query = db_query.filter(func.upper(Phenotype.mutant_type).like(func.upper(mut_pattern)))
        else:
            db_query = db_query.filter(func.upper(Phenotype.mutant_type) == func.upper(mutant_type))

    # Property value (chemical/condition) search
    if property_value:
        prop_pattern = f"%{property_value.replace('*', '%')}%"
        # Join experiment properties
        db_query = db_query.outerjoin(
            ExptExptprop, PhenoAnnotation.experiment_no == ExptExptprop.experiment_no
        ).outerjoin(
            ExptProperty, ExptExptprop.expt_property_no == ExptProperty.expt_property_no
        )
        prop_filter = func.upper(ExptProperty.property_value).like(func.upper(prop_pattern))
        if property_type:
            prop_filter = prop_filter & (func.upper(ExptProperty.property_type) == func.upper(property_type))
        db_query = db_query.filter(prop_filter).distinct()

    # Organism filter
    if organism:
        db_query = db_query.filter(func.upper(Organism.organism_abbrev) == func.upper(organism))

    # PubMed filter - need to join references
    if pubmed:
        db_query = db_query.join(
            RefLink,
            (RefLink.tab_name == "PHENO_ANNOTATION") & (RefLink.primary_key == PhenoAnnotation.pheno_annotation_no)
        ).join(
            Reference, RefLink.reference_no == Reference.reference_no
        ).filter(Reference.pubmed == pubmed)

    # Get total count before pagination
    total_count = db_query.count()

    # Apply pagination
    offset = (page - 1) * limit
    annotations = db_query.order_by(Feature.gene_name, Feature.feature_name).offset(offset).limit(limit).all()

    # Collect pheno_annotation_nos and experiment_nos for batch loading
    pheno_annotation_nos = [pa.pheno_annotation_no for pa in annotations]
    experiment_nos = [pa.experiment.experiment_no for pa in annotations if pa.experiment]

    # Load references in batch
    ref_link_map: dict[int, list] = {}
    ref_url_map: dict[int, list] = {}

    if pheno_annotation_nos:
        # Load all ref_links for phenotype annotations
        all_ref_links = (
            db.query(RefLink)
            .options(joinedload(RefLink.reference).joinedload(Reference.journal))
            .filter(
                RefLink.tab_name == "PHENO_ANNOTATION",
                RefLink.primary_key.in_(pheno_annotation_nos),
            )
            .all()
        )

        # Build ref_link map and collect reference_nos
        all_ref_nos = set()
        for rl in all_ref_links:
            if rl.reference:
                all_ref_nos.add(rl.reference.reference_no)
                if rl.primary_key not in ref_link_map:
                    ref_link_map[rl.primary_key] = []
                ref_link_map[rl.primary_key].append(rl)

        # Load ref_urls for all references
        if all_ref_nos:
            ref_url_query = (
                db.query(RefUrl)
                .options(joinedload(RefUrl.url))
                .filter(RefUrl.reference_no.in_(list(all_ref_nos)))
                .all()
            )
            for ref_url in ref_url_query:
                if ref_url.reference_no not in ref_url_map:
                    ref_url_map[ref_url.reference_no] = []
                ref_url_map[ref_url.reference_no].append(ref_url)

    # Batch load experiment properties (strain_background)
    strain_map: dict[int, str] = {}
    if experiment_nos:
        expt_props = (
            db.query(ExptExptprop.experiment_no, ExptProperty.property_value)
            .join(ExptProperty, ExptExptprop.expt_property_no == ExptProperty.expt_property_no)
            .filter(
                ExptExptprop.experiment_no.in_(experiment_nos),
                ExptProperty.property_type == 'strain_background'
            )
            .all()
        )
        for exp_no, prop_value in expt_props:
            strain_map[exp_no] = prop_value

    # Build results
    results = []
    for pa in annotations:
        phenotype = pa.phenotype
        feature = pa.feature
        organism = feature.organism
        experiment = pa.experiment

        # Get strain from batch-loaded map
        strain = None
        if experiment:
            strain = strain_map.get(experiment.experiment_no)

        # Build references
        references = []
        annotation_ref_links = ref_link_map.get(pa.pheno_annotation_no, [])
        for rl in annotation_ref_links:
            ref = rl.reference
            if ref:
                ref_urls = ref_url_map.get(ref.reference_no, [])
                references.append(ReferenceForAnnotation(
                    reference_no=ref.reference_no,
                    pubmed=ref.pubmed,
                    dbxref_id=ref.dbxref_id,
                    citation=ref.citation,
                    journal_name=ref.journal.full_name if ref.journal else None,
                    year=ref.year,
                    links=_build_citation_links_for_phenotype(ref, ref_urls),
                ))

        results.append(PhenotypeSearchResult(
            feature_name=feature.feature_name,
            gene_name=feature.gene_name,
            organism=organism.organism_name if organism else "Unknown",
            observable=phenotype.observable,
            qualifier=phenotype.qualifier,
            experiment_type=phenotype.experiment_type,
            mutant_type=phenotype.mutant_type,
            experiment_comment=experiment.experiment_comment if experiment else None,
            strain=strain,
            references=references,
        ))

    return PhenotypeSearchResponse(
        query=PhenotypeSearchQuery(
            query=query,
            observable=observable,
            qualifier=qualifier,
            experiment_type=experiment_type,
            mutant_type=mutant_type,
            property_value=property_value,
            property_type=property_type,
            pubmed=pubmed,
            organism=organism,
        ),
        total_results=total_count,
        page=page,
        limit=limit,
        results=results,
    )


def search_phenotypes_summary(
    db: Session,
    query: Optional[str] = None,
) -> PhenotypeSearchSummaryResponse:
    """
    Get summary of phenotype search results grouped by observable.

    Args:
        db: Database session
        query: Keyword to search for

    Returns:
        PhenotypeSearchSummaryResponse with counts grouped by observable
    """
    if not query:
        return PhenotypeSearchSummaryResponse(
            query="",
            total_results=0,
            direct_matches=[],
            related_matches=[],
        )

    query_pattern = f"%{query.replace('*', '%')}%"

    # Get counts for observables that directly match the query (in observable name)
    direct_matches_query = (
        db.query(
            Phenotype.observable,
            func.count(PhenoAnnotation.pheno_annotation_no).label('count')
        )
        .join(PhenoAnnotation, PhenoAnnotation.phenotype_no == Phenotype.phenotype_no)
        .join(Feature, PhenoAnnotation.feature_no == Feature.feature_no)
        .filter(func.lower(Feature.feature_type) != 'allele')
        .filter(func.upper(Phenotype.observable).like(func.upper(query_pattern)))
        .group_by(Phenotype.observable)
        .order_by(func.count(PhenoAnnotation.pheno_annotation_no).desc())
        .all()
    )

    direct_matches = [
        PhenotypeMatchGroup(observable=obs, count=cnt, is_direct_match=True)
        for obs, cnt in direct_matches_query
    ]

    # Get counts for observables matched via qualifier, experiment comment, or properties (related matches)
    # First, get annotations that match in qualifier or experiment comment
    related_via_qualifier_comment = (
        db.query(
            Phenotype.observable,
            PhenoAnnotation.pheno_annotation_no
        )
        .join(PhenoAnnotation, PhenoAnnotation.phenotype_no == Phenotype.phenotype_no)
        .join(Feature, PhenoAnnotation.feature_no == Feature.feature_no)
        .outerjoin(Experiment, PhenoAnnotation.experiment_no == Experiment.experiment_no)
        .filter(func.lower(Feature.feature_type) != 'allele')
        .filter(~func.upper(Phenotype.observable).like(func.upper(query_pattern)))  # Exclude direct matches
        .filter(
            or_(
                func.upper(Phenotype.qualifier).like(func.upper(query_pattern)),
                func.upper(Experiment.experiment_comment).like(func.upper(query_pattern)),
            )
        )
        .all()
    )

    # Get annotations that match in experiment properties
    related_via_properties = (
        db.query(
            Phenotype.observable,
            PhenoAnnotation.pheno_annotation_no
        )
        .join(PhenoAnnotation, PhenoAnnotation.phenotype_no == Phenotype.phenotype_no)
        .join(Feature, PhenoAnnotation.feature_no == Feature.feature_no)
        .join(ExptExptprop, PhenoAnnotation.experiment_no == ExptExptprop.experiment_no)
        .join(ExptProperty, ExptExptprop.expt_property_no == ExptProperty.expt_property_no)
        .filter(func.lower(Feature.feature_type) != 'allele')
        .filter(~func.upper(Phenotype.observable).like(func.upper(query_pattern)))  # Exclude direct matches
        .filter(func.upper(ExptProperty.property_value).like(func.upper(query_pattern)))
        .all()
    )

    # Combine and count by observable (use set to avoid duplicates)
    related_annotation_map: dict[str, set] = defaultdict(set)
    for obs, pa_no in related_via_qualifier_comment:
        related_annotation_map[obs].add(pa_no)
    for obs, pa_no in related_via_properties:
        related_annotation_map[obs].add(pa_no)

    related_matches = [
        PhenotypeMatchGroup(observable=obs, count=len(pa_nos), is_direct_match=False)
        for obs, pa_nos in sorted(related_annotation_map.items(), key=lambda x: -len(x[1]))
    ]

    total_results = sum(m.count for m in direct_matches) + sum(m.count for m in related_matches)

    return PhenotypeSearchSummaryResponse(
        query=query,
        total_results=total_results,
        direct_matches=direct_matches,
        related_matches=related_matches,
    )


def get_observable_tree(db: Session) -> ObservableTreeResponse:
    """
    Get hierarchical tree of observable terms.

    Returns observable terms with annotation counts, organized hierarchically
    using the CV term relationships if available, or as a flat list otherwise.
    """
    # First, get all distinct observables from the Phenotype table with counts
    observable_counts = (
        db.query(
            Phenotype.observable,
            func.count(PhenoAnnotation.pheno_annotation_no).label('count')
        )
        .join(PhenoAnnotation, PhenoAnnotation.phenotype_no == Phenotype.phenotype_no)
        .group_by(Phenotype.observable)
        .order_by(Phenotype.observable)
        .all()
    )

    # Build a map of observable -> count
    count_map = {obs: cnt for obs, cnt in observable_counts}

    # Try to get the observable CV and its term hierarchy
    observable_cv = (
        db.query(Cv)
        .filter(func.lower(Cv.cv_name) == 'observable')
        .first()
    )

    if observable_cv:
        # Get all CV terms for the observable CV
        cv_terms = (
            db.query(CvTerm)
            .filter(CvTerm.cv_no == observable_cv.cv_no)
            .all()
        )

        # Build a map of term_name -> CvTerm
        term_map = {t.term_name: t for t in cv_terms}
        term_no_map = {t.cv_term_no: t for t in cv_terms}

        # Get all relationships for these terms
        relationships = (
            db.query(CvtermRelationship)
            .filter(
                or_(
                    CvtermRelationship.parent_cv_term_no.in_([t.cv_term_no for t in cv_terms]),
                    CvtermRelationship.child_cv_term_no.in_([t.cv_term_no for t in cv_terms]),
                )
            )
            .all()
        )

        # Build parent -> children map
        children_map: dict[int, list[int]] = defaultdict(list)
        has_parent: set[int] = set()

        for rel in relationships:
            if rel.parent_cv_term_no in term_no_map and rel.child_cv_term_no in term_no_map:
                children_map[rel.parent_cv_term_no].append(rel.child_cv_term_no)
                has_parent.add(rel.child_cv_term_no)

        # Find root terms (terms that have no parent)
        root_term_nos = [t.cv_term_no for t in cv_terms if t.cv_term_no not in has_parent]

        # Recursively build tree
        def build_tree_node(term_no: int) -> ObservableTerm:
            term = term_no_map[term_no]
            term_name = term.term_name
            count = count_map.get(term_name, 0)

            children = []
            for child_no in sorted(children_map.get(term_no, []), key=lambda x: term_no_map[x].term_name):
                children.append(build_tree_node(child_no))

            return ObservableTerm(
                term=term_name,
                count=count,
                children=children,
            )

        # Build tree from roots
        tree = []
        for root_no in sorted(root_term_nos, key=lambda x: term_no_map[x].term_name):
            tree.append(build_tree_node(root_no))

        # If tree is empty but we have observables, fall back to flat list
        if not tree and observable_counts:
            tree = [
                ObservableTerm(term=obs, count=cnt, children=[])
                for obs, cnt in observable_counts
            ]

        return ObservableTreeResponse(tree=tree)

    else:
        # No observable CV found, return flat list of observables
        tree = [
            ObservableTerm(term=obs, count=cnt, children=[])
            for obs, cnt in observable_counts
        ]

        return ObservableTreeResponse(tree=tree)
