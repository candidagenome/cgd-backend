"""
Search Service - handles quick search across multiple entity types.
"""
from __future__ import annotations

import re
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import func, or_

from cgd.schemas.search_schema import (
    SearchResult,
    SearchResponse,
    ResolveResponse,
    SearchResultLink,
    AutocompleteSuggestion,
    AutocompleteResponse,
    CategorySearchResponse,
    PaginationInfo,
)
from cgd.models.models import (
    Feature,
    Go,
    Phenotype,
    Reference,
    Alias,
    FeatAlias,
    Organism,
    RefUrl,
    Url,
)


def _normalize_query(query: str) -> str:
    """
    Normalize search query:
    - Strip whitespace
    - Convert wildcards (* to %)
    """
    normalized = query.strip()
    # Convert user wildcards to SQL wildcards
    normalized = normalized.replace('*', '%')
    return normalized


def _get_like_pattern(query: str) -> str:
    """Get pattern for LIKE search (case-insensitive)."""
    normalized = _normalize_query(query)
    # If no wildcards provided, wrap in % for contains search
    if '%' not in normalized:
        return f'%{normalized}%'
    return normalized


def _format_goid(goid: int) -> str:
    """Format GOID as GO:XXXXXXX (7-digit padded)."""
    return f"GO:{goid:07d}"


def _get_organism_name(organism: Optional[Organism]) -> Optional[str]:
    """Get organism display name."""
    if organism:
        return organism.organism_name
    return None


def _highlight_text(text: Optional[str], query: str) -> Optional[str]:
    """
    Highlight matching query text with <mark> tags.

    Case-insensitive matching that preserves original case.

    Args:
        text: The text to highlight
        query: The search query to highlight

    Returns:
        Text with <mark> tags around matching portions, or None if text is None
    """
    if not text or not query:
        return text

    # Remove wildcards from query for highlighting
    clean_query = query.strip().replace('*', '').replace('%', '')
    if not clean_query:
        return text

    # Case-insensitive search and replace while preserving original case
    pattern = re.compile(re.escape(clean_query), re.IGNORECASE)

    def replacer(match):
        return f"<mark>{match.group(0)}</mark>"

    return pattern.sub(replacer, text)


def _build_reference_links(db: Session, ref: Reference) -> list[SearchResultLink]:
    """
    Build citation links for a reference.

    Returns links for CGD Paper, PubMed, Full Text, Reference Supplement, etc.
    """
    links = []

    # CGD Paper link (always present)
    links.append(SearchResultLink(
        name="CGD Paper",
        url=f"/reference/{ref.dbxref_id}",
        link_type="internal"
    ))

    # PubMed link (if pubmed ID exists)
    if ref.pubmed:
        links.append(SearchResultLink(
            name="PubMed",
            url=f"https://pubmed.ncbi.nlm.nih.gov/{ref.pubmed}",
            link_type="external"
        ))

    # Get URLs from ref_url table
    ref_urls = (
        db.query(RefUrl)
        .filter(RefUrl.reference_no == ref.reference_no)
        .all()
    )

    for ref_url in ref_urls:
        url_obj = ref_url.url
        if url_obj and url_obj.url:
            url_type = (url_obj.url_type or "").lower()

            if "supplement" in url_type:
                links.append(SearchResultLink(
                    name="Reference Supplement",
                    url=url_obj.url,
                    link_type="external"
                ))
            elif "reference data" in url_type:
                continue  # Skip Reference Data
            elif any(kw in url_type for kw in ["download", "dataset"]):
                links.append(SearchResultLink(
                    name="Download Datasets",
                    url=url_obj.url,
                    link_type="external"
                ))
            else:
                # All other URL types shown as Full Text
                links.append(SearchResultLink(
                    name="Full Text",
                    url=url_obj.url,
                    link_type="external"
                ))

    return links


def resolve_identifier(db: Session, query: str) -> ResolveResponse:
    """
    Check if query is an exact identifier match for a locus or reference.

    Checks in order:
    1. Feature gene_name (exact, case-insensitive)
    2. Feature feature_name (exact, case-insensitive)
    3. Feature dbxref_id (exact, case-insensitive) - e.g., CAL0001571
    4. Reference dbxref_id (exact, case-insensitive) - e.g., CAL0080639

    Returns ResolveResponse with redirect_url if found.
    """
    normalized = query.strip()
    upper_query = normalized.upper()

    # 1. Check Feature by gene_name (exact match)
    # Use the gene_name in URL so locus page shows all organisms with this gene
    feature = (
        db.query(Feature)
        .filter(func.upper(Feature.gene_name) == upper_query)
        .first()
    )
    if feature:
        return ResolveResponse(
            query=query,
            resolved=True,
            redirect_url=f"/locus/{feature.gene_name}",
            entity_type="locus",
            entity_name=feature.gene_name,
        )

    # 2. Check Feature by feature_name (exact match)
    feature = (
        db.query(Feature)
        .filter(func.upper(Feature.feature_name) == upper_query)
        .first()
    )
    if feature:
        return ResolveResponse(
            query=query,
            resolved=True,
            redirect_url=f"/locus/{feature.feature_name}",
            entity_type="locus",
            entity_name=feature.gene_name or feature.feature_name,
        )

    # 3. Check Feature by dbxref_id (exact match) - e.g., CAL0001571
    feature = (
        db.query(Feature)
        .filter(func.upper(Feature.dbxref_id) == upper_query)
        .first()
    )
    if feature:
        return ResolveResponse(
            query=query,
            resolved=True,
            redirect_url=f"/locus/{feature.feature_name}",
            entity_type="locus",
            entity_name=feature.gene_name or feature.feature_name,
        )

    # 4. Check Reference by dbxref_id (exact match) - e.g., CAL0080639
    reference = (
        db.query(Reference)
        .filter(func.upper(Reference.dbxref_id) == upper_query)
        .first()
    )
    if reference:
        return ResolveResponse(
            query=query,
            resolved=True,
            redirect_url=f"/reference/{reference.dbxref_id}",
            entity_type="reference",
            entity_name=f"PMID:{reference.pubmed}" if reference.pubmed else reference.dbxref_id,
        )

    # No exact match found
    return ResolveResponse(
        query=query,
        resolved=False,
    )


def search_genes(db: Session, query: str, limit: int = 20) -> list[SearchResult]:
    """
    Search genes/loci by gene_name, feature_name, or aliases.

    Returns SearchResult list with category="gene".
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Search in Feature table: gene_name, feature_name, dbxref_id
    feature_query = (
        db.query(Feature)
        .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
        .filter(
            or_(
                func.upper(Feature.gene_name).like(upper_pattern),
                func.upper(Feature.feature_name).like(upper_pattern),
                func.upper(Feature.dbxref_id).like(upper_pattern),
            )
        )
        .limit(limit)
    )

    for feat in feature_query:
        display_name = feat.gene_name or feat.feature_name
        results.append(SearchResult(
            category="gene",
            id=feat.dbxref_id,
            name=display_name,
            description=feat.headline,
            link=f"/locus/{feat.feature_name}",
            organism=_get_organism_name(feat.organism),
            highlighted_name=_highlight_text(display_name, query),
            highlighted_description=_highlight_text(feat.headline, query),
        ))

    # If we have room, also search aliases
    remaining = limit - len(results)
    if remaining > 0:
        # Get feature_nos already in results to avoid duplicates
        found_feature_nos = {feat.feature_no for feat in feature_query}

        alias_query = (
            db.query(Feature, Alias)
            .join(FeatAlias, Feature.feature_no == FeatAlias.feature_no)
            .join(Alias, FeatAlias.alias_no == Alias.alias_no)
            .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
            .filter(func.upper(Alias.alias_name).like(upper_pattern))
            .limit(remaining + len(found_feature_nos))  # Extra to account for potential duplicates
        )

        for feat, alias in alias_query:
            if feat.feature_no not in found_feature_nos:
                display_name = feat.gene_name or feat.feature_name
                description = f"Alias: {alias.alias_name} - {feat.headline}" if feat.headline else f"Alias: {alias.alias_name}"
                results.append(SearchResult(
                    category="gene",
                    id=feat.dbxref_id,
                    name=display_name,
                    description=description,
                    link=f"/locus/{feat.feature_name}",
                    organism=_get_organism_name(feat.organism),
                    highlighted_name=_highlight_text(display_name, query),
                    highlighted_description=_highlight_text(description, query),
                ))
                found_feature_nos.add(feat.feature_no)
                if len(results) >= limit:
                    break

    return results[:limit]


def search_go_terms(db: Session, query: str, limit: int = 20) -> list[SearchResult]:
    """
    Search GO terms by go_term or goid.

    Returns SearchResult list with category="go_term".
    """
    results = []
    normalized = _normalize_query(query)

    # Check if query looks like a GO ID (GO:XXXXXXX or just numeric)
    goid_numeric = None
    if normalized.upper().startswith('GO:'):
        try:
            goid_numeric = int(normalized[3:])
        except ValueError:
            pass
    else:
        try:
            goid_numeric = int(normalized)
        except ValueError:
            pass

    # If it's a valid GO ID, search exact match first
    if goid_numeric is not None:
        go_exact = db.query(Go).filter(Go.goid == goid_numeric).first()
        if go_exact:
            description = go_exact.go_definition[:200] + "..." if go_exact.go_definition and len(go_exact.go_definition) > 200 else go_exact.go_definition
            results.append(SearchResult(
                category="go_term",
                id=_format_goid(go_exact.goid),
                name=go_exact.go_term,
                description=description,
                link=f"/go/{_format_goid(go_exact.goid)}",
                organism=None,
                highlighted_name=_highlight_text(go_exact.go_term, query),
                highlighted_description=_highlight_text(description, query),
            ))

    # Search by term name
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    remaining = limit - len(results)
    if remaining > 0:
        found_goids = {r.id for r in results}

        go_query = (
            db.query(Go)
            .filter(func.upper(Go.go_term).like(upper_pattern))
            .limit(remaining + len(found_goids))
        )

        for go in go_query:
            formatted_goid = _format_goid(go.goid)
            if formatted_goid not in found_goids:
                description = go.go_definition[:200] + "..." if go.go_definition and len(go.go_definition) > 200 else go.go_definition
                results.append(SearchResult(
                    category="go_term",
                    id=formatted_goid,
                    name=go.go_term,
                    description=description,
                    link=f"/go/{formatted_goid}",
                    organism=None,
                    highlighted_name=_highlight_text(go.go_term, query),
                    highlighted_description=_highlight_text(description, query),
                ))
                if len(results) >= limit:
                    break

    return results[:limit]


def search_phenotypes(db: Session, query: str, limit: int = 20) -> list[SearchResult]:
    """
    Search phenotypes by observable.

    Returns SearchResult list with category="phenotype".
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Search distinct observables
    pheno_query = (
        db.query(Phenotype.observable)
        .filter(func.upper(Phenotype.observable).like(upper_pattern))
        .distinct()
        .limit(limit)
    )

    for (observable,) in pheno_query:
        results.append(SearchResult(
            category="phenotype",
            id=observable,
            name=observable,
            description=None,
            link=f"/phenotype/search?observable={observable}",
            organism=None,
            highlighted_name=_highlight_text(observable, query),
            highlighted_description=None,
        ))

    return results


def search_references(db: Session, query: str, limit: int = 20) -> list[SearchResult]:
    """
    Search references by PubMed ID, dbxref_id (CGDID), or citation.

    Returns SearchResult list with category="reference".
    """
    results = []
    normalized = _normalize_query(query)
    upper_query = normalized.upper()

    # Check if query is a numeric PubMed ID
    pubmed_id = None
    try:
        pubmed_id = int(normalized)
    except ValueError:
        pass

    # If it's a valid PubMed ID, search exact match first
    if pubmed_id is not None:
        ref_exact = db.query(Reference).filter(Reference.pubmed == pubmed_id).first()
        if ref_exact:
            name = f"PMID:{ref_exact.pubmed}" if ref_exact.pubmed else ref_exact.dbxref_id
            results.append(SearchResult(
                category="reference",
                id=ref_exact.dbxref_id,
                name=name,
                description=ref_exact.citation,
                link=f"/reference/{ref_exact.dbxref_id}",
                organism=None,
                links=_build_reference_links(db, ref_exact),
                highlighted_name=_highlight_text(name, query),
                highlighted_description=_highlight_text(ref_exact.citation, query),
            ))

    # Check if query matches a dbxref_id (CGDID like CAL0080639)
    if not results:
        ref_by_dbxref = db.query(Reference).filter(func.upper(Reference.dbxref_id) == upper_query).first()
        if ref_by_dbxref:
            name = f"PMID:{ref_by_dbxref.pubmed}" if ref_by_dbxref.pubmed else ref_by_dbxref.dbxref_id
            results.append(SearchResult(
                category="reference",
                id=ref_by_dbxref.dbxref_id,
                name=name,
                description=ref_by_dbxref.citation,
                link=f"/reference/{ref_by_dbxref.dbxref_id}",
                organism=None,
                links=_build_reference_links(db, ref_by_dbxref),
                highlighted_name=_highlight_text(name, query),
                highlighted_description=_highlight_text(ref_by_dbxref.citation, query),
            ))

    # Search by citation text
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    remaining = limit - len(results)
    if remaining > 0:
        found_ref_nos = {r.id for r in results}

        ref_query = (
            db.query(Reference)
            .filter(func.upper(Reference.citation).like(upper_pattern))
            .limit(remaining + len(found_ref_nos))
        )

        for ref in ref_query:
            if ref.dbxref_id not in found_ref_nos:
                name = f"PMID:{ref.pubmed}" if ref.pubmed else ref.dbxref_id
                results.append(SearchResult(
                    category="reference",
                    id=ref.dbxref_id,
                    name=name,
                    description=ref.citation,
                    link=f"/reference/{ref.dbxref_id}",
                    links=_build_reference_links(db, ref),
                    organism=None,
                    highlighted_name=_highlight_text(name, query),
                    highlighted_description=_highlight_text(ref.citation, query),
                ))
                if len(results) >= limit:
                    break

    return results[:limit]


def quick_search(db: Session, query: str, limit: int = 20) -> SearchResponse:
    """
    Search all categories (genes, GO terms, phenotypes, references).

    Returns results grouped by category.
    """
    # Search all categories with the same limit per category
    genes = search_genes(db, query, limit)
    go_terms = search_go_terms(db, query, limit)
    phenotypes = search_phenotypes(db, query, limit)
    references = search_references(db, query, limit)

    # Build response
    results_by_category = {}
    if genes:
        results_by_category["genes"] = genes
    if go_terms:
        results_by_category["go_terms"] = go_terms
    if phenotypes:
        results_by_category["phenotypes"] = phenotypes
    if references:
        results_by_category["references"] = references

    total = len(genes) + len(go_terms) + len(phenotypes) + len(references)

    return SearchResponse(
        query=query,
        total_results=total,
        results_by_category=results_by_category,
    )


def get_autocomplete_suggestions(
    db: Session,
    query: str,
    limit: int = 10,
) -> AutocompleteResponse:
    """
    Get autocomplete suggestions for a search query.

    Optimized for speed with prefix matching. Returns suggestions
    prioritized by category: genes > GO terms > phenotypes > references.

    Args:
        db: Database session
        query: Search query (minimum 2 characters recommended)
        limit: Maximum total suggestions to return

    Returns:
        AutocompleteResponse with flat list of suggestions
    """
    suggestions: list[AutocompleteSuggestion] = []
    normalized = query.strip()

    if len(normalized) < 1:
        return AutocompleteResponse(query=query, suggestions=[])

    # Use prefix matching for speed (starts with)
    prefix_pattern = f"{normalized.upper()}%"
    # Also prepare contains pattern as fallback
    contains_pattern = f"%{normalized.upper()}%"

    # Track how many slots remain
    remaining = limit

    # 1. Search genes (highest priority) - prefix match on gene_name and feature_name
    if remaining > 0:
        gene_limit = min(remaining, 5)  # Cap genes at 5 to leave room for others

        # Prefix match on gene_name (most relevant)
        gene_prefix_query = (
            db.query(Feature.gene_name, Feature.feature_name, Feature.headline)
            .filter(
                Feature.gene_name.isnot(None),
                func.upper(Feature.gene_name).like(prefix_pattern)
            )
            .distinct()
            .limit(gene_limit)
            .all()
        )

        seen_genes = set()
        for gene_name, feature_name, headline in gene_prefix_query:
            if gene_name and gene_name not in seen_genes:
                description = headline[:80] + "..." if headline and len(headline) > 80 else headline
                suggestions.append(AutocompleteSuggestion(
                    text=gene_name,
                    category="gene",
                    link=f"/locus/{feature_name}",
                    description=description,
                    highlighted_text=_highlight_text(gene_name, query),
                    highlighted_description=_highlight_text(description, query),
                ))
                seen_genes.add(gene_name)

        # If we need more genes, try feature_name prefix
        if len(suggestions) < gene_limit:
            extra_needed = gene_limit - len(suggestions)
            feat_prefix_query = (
                db.query(Feature.gene_name, Feature.feature_name, Feature.headline)
                .filter(func.upper(Feature.feature_name).like(prefix_pattern))
                .distinct()
                .limit(extra_needed + len(seen_genes))
                .all()
            )

            for gene_name, feature_name, headline in feat_prefix_query:
                display = gene_name or feature_name
                if display not in seen_genes and len(suggestions) < gene_limit:
                    description = headline[:80] + "..." if headline and len(headline) > 80 else headline
                    suggestions.append(AutocompleteSuggestion(
                        text=display,
                        category="gene",
                        link=f"/locus/{feature_name}",
                        description=description,
                        highlighted_text=_highlight_text(display, query),
                        highlighted_description=_highlight_text(description, query),
                    ))
                    seen_genes.add(display)

        remaining = limit - len(suggestions)

    # 2. Search GO terms - prefix match on go_term
    if remaining > 0:
        go_limit = min(remaining, 3)

        # Check if it looks like a GO ID
        if normalized.upper().startswith('GO:'):
            try:
                goid_numeric = int(normalized[3:])
                go_exact = db.query(Go).filter(Go.goid == goid_numeric).first()
                if go_exact:
                    text = f"{_format_goid(go_exact.goid)} - {go_exact.go_term}"
                    suggestions.append(AutocompleteSuggestion(
                        text=text,
                        category="go_term",
                        link=f"/go/{_format_goid(go_exact.goid)}",
                        description=go_exact.go_aspect,
                        highlighted_text=_highlight_text(text, query),
                        highlighted_description=_highlight_text(go_exact.go_aspect, query),
                    ))
                    go_limit -= 1
            except ValueError:
                pass

        if go_limit > 0:
            go_query = (
                db.query(Go.goid, Go.go_term, Go.go_aspect)
                .filter(func.upper(Go.go_term).like(prefix_pattern))
                .limit(go_limit)
                .all()
            )

            for goid, go_term, go_aspect in go_query:
                formatted_goid = _format_goid(goid)
                text = f"{formatted_goid} - {go_term}"
                suggestions.append(AutocompleteSuggestion(
                    text=text,
                    category="go_term",
                    link=f"/go/{formatted_goid}",
                    description=go_aspect,
                    highlighted_text=_highlight_text(text, query),
                    highlighted_description=_highlight_text(go_aspect, query),
                ))

        remaining = limit - len(suggestions)

    # 3. Search phenotypes - prefix match on observable
    if remaining > 0:
        pheno_limit = min(remaining, 2)

        pheno_query = (
            db.query(Phenotype.observable)
            .filter(func.upper(Phenotype.observable).like(prefix_pattern))
            .distinct()
            .limit(pheno_limit)
            .all()
        )

        for (observable,) in pheno_query:
            suggestions.append(AutocompleteSuggestion(
                text=observable,
                category="phenotype",
                link=f"/phenotype/search?observable={observable}",
                description="Phenotype",
                highlighted_text=_highlight_text(observable, query),
                highlighted_description=None,
            ))

        remaining = limit - len(suggestions)

    # 4. Search references - only if query is numeric (PubMed ID)
    if remaining > 0:
        try:
            pubmed_id = int(normalized)
            ref = db.query(Reference).filter(Reference.pubmed == pubmed_id).first()
            if ref:
                text = f"PMID:{ref.pubmed}"
                description = ref.citation[:80] + "..." if len(ref.citation) > 80 else ref.citation
                suggestions.append(AutocompleteSuggestion(
                    text=text,
                    category="reference",
                    link=f"/reference/{ref.dbxref_id}",
                    description=description,
                    highlighted_text=_highlight_text(text, query),
                    highlighted_description=_highlight_text(description, query),
                ))
        except ValueError:
            # Not a numeric query, skip reference search for autocomplete
            pass

    return AutocompleteResponse(query=query, suggestions=suggestions)


def _count_genes(db: Session, query: str) -> int:
    """Count total genes matching the query."""
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Count features matching directly
    feature_count = (
        db.query(func.count(Feature.feature_no))
        .filter(
            or_(
                func.upper(Feature.gene_name).like(upper_pattern),
                func.upper(Feature.feature_name).like(upper_pattern),
                func.upper(Feature.dbxref_id).like(upper_pattern),
            )
        )
        .scalar()
    )

    # Count features matching via aliases (excluding already counted)
    alias_subq = (
        db.query(FeatAlias.feature_no)
        .join(Alias, FeatAlias.alias_no == Alias.alias_no)
        .filter(func.upper(Alias.alias_name).like(upper_pattern))
        .distinct()
        .subquery()
    )

    alias_count = (
        db.query(func.count(Feature.feature_no))
        .filter(
            Feature.feature_no.in_(db.query(alias_subq.c.feature_no)),
            ~or_(
                func.upper(Feature.gene_name).like(upper_pattern),
                func.upper(Feature.feature_name).like(upper_pattern),
                func.upper(Feature.dbxref_id).like(upper_pattern),
            )
        )
        .scalar()
    )

    return feature_count + alias_count


def _count_go_terms(db: Session, query: str) -> int:
    """Count total GO terms matching the query."""
    normalized = _normalize_query(query)
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    count = (
        db.query(func.count(Go.go_no))
        .filter(func.upper(Go.go_term).like(upper_pattern))
        .scalar()
    )

    # Check if query is a GO ID
    goid_numeric = None
    if normalized.upper().startswith('GO:'):
        try:
            goid_numeric = int(normalized[3:])
        except ValueError:
            pass
    else:
        try:
            goid_numeric = int(normalized)
        except ValueError:
            pass

    if goid_numeric is not None:
        exact_exists = db.query(Go).filter(Go.goid == goid_numeric).first()
        if exact_exists:
            # Check if not already in the term search
            term_match = db.query(Go).filter(
                Go.goid == goid_numeric,
                func.upper(Go.go_term).like(upper_pattern)
            ).first()
            if not term_match:
                count += 1

    return count


def _count_phenotypes(db: Session, query: str) -> int:
    """Count total distinct phenotype observables matching the query."""
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    count = (
        db.query(func.count(func.distinct(Phenotype.observable)))
        .filter(func.upper(Phenotype.observable).like(upper_pattern))
        .scalar()
    )
    return count


def _count_references(db: Session, query: str) -> int:
    """Count total references matching the query."""
    normalized = _normalize_query(query)
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Count by citation
    count = (
        db.query(func.count(Reference.reference_no))
        .filter(func.upper(Reference.citation).like(upper_pattern))
        .scalar()
    )

    # Check for PubMed ID match
    try:
        pubmed_id = int(normalized)
        pubmed_match = db.query(Reference).filter(Reference.pubmed == pubmed_id).first()
        if pubmed_match:
            # Check if not already in citation search
            citation_match = db.query(Reference).filter(
                Reference.pubmed == pubmed_id,
                func.upper(Reference.citation).like(upper_pattern)
            ).first()
            if not citation_match:
                count += 1
    except ValueError:
        pass

    # Check for dbxref_id match
    dbxref_match = db.query(Reference).filter(
        func.upper(Reference.dbxref_id) == normalized.upper()
    ).first()
    if dbxref_match:
        citation_match = db.query(Reference).filter(
            Reference.dbxref_id == dbxref_match.dbxref_id,
            func.upper(Reference.citation).like(upper_pattern)
        ).first()
        if not citation_match:
            count += 1

    return count


def search_category_paginated(
    db: Session,
    query: str,
    category: str,
    page: int = 1,
    page_size: int = 20,
) -> CategorySearchResponse:
    """
    Search within a specific category with pagination.

    Args:
        db: Database session
        query: Search query string
        category: Category to search (genes, go_terms, phenotypes, references)
        page: Page number (1-indexed)
        page_size: Number of results per page

    Returns:
        CategorySearchResponse with paginated results and metadata
    """
    offset = (page - 1) * page_size

    # Get total count and results based on category
    if category == "genes":
        total_count = _count_genes(db, query)
        results = _search_genes_paginated(db, query, offset, page_size)
    elif category == "go_terms":
        total_count = _count_go_terms(db, query)
        results = _search_go_terms_paginated(db, query, offset, page_size)
    elif category == "phenotypes":
        total_count = _count_phenotypes(db, query)
        results = _search_phenotypes_paginated(db, query, offset, page_size)
    elif category == "references":
        total_count = _count_references(db, query)
        results = _search_references_paginated(db, query, offset, page_size)
    else:
        total_count = 0
        results = []

    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 0

    pagination = PaginationInfo(
        page=page,
        page_size=page_size,
        total_items=total_count,
        total_pages=total_pages,
        has_next=page < total_pages,
        has_prev=page > 1,
    )

    return CategorySearchResponse(
        query=query,
        category=category,
        results=results,
        pagination=pagination,
    )


def _search_genes_paginated(
    db: Session, query: str, offset: int, limit: int
) -> list[SearchResult]:
    """Search genes with pagination."""
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Get features matching directly
    feature_query = (
        db.query(Feature)
        .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
        .filter(
            or_(
                func.upper(Feature.gene_name).like(upper_pattern),
                func.upper(Feature.feature_name).like(upper_pattern),
                func.upper(Feature.dbxref_id).like(upper_pattern),
            )
        )
        .offset(offset)
        .limit(limit)
    )

    for feat in feature_query:
        display_name = feat.gene_name or feat.feature_name
        results.append(SearchResult(
            category="gene",
            id=feat.dbxref_id,
            name=display_name,
            description=feat.headline,
            link=f"/locus/{feat.feature_name}",
            organism=_get_organism_name(feat.organism),
            highlighted_name=_highlight_text(display_name, query),
            highlighted_description=_highlight_text(feat.headline, query),
        ))

    # If we need more results, search aliases
    remaining = limit - len(results)
    if remaining > 0 and len(results) < limit:
        found_feature_nos = {feat.feature_no for feat in feature_query}

        # Adjust offset for alias search
        alias_offset = max(0, offset - len(found_feature_nos)) if offset > 0 else 0

        alias_query = (
            db.query(Feature, Alias)
            .join(FeatAlias, Feature.feature_no == FeatAlias.feature_no)
            .join(Alias, FeatAlias.alias_no == Alias.alias_no)
            .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
            .filter(
                func.upper(Alias.alias_name).like(upper_pattern),
                ~Feature.feature_no.in_(found_feature_nos) if found_feature_nos else True
            )
            .offset(alias_offset)
            .limit(remaining)
        )

        for feat, alias in alias_query:
            display_name = feat.gene_name or feat.feature_name
            description = f"Alias: {alias.alias_name} - {feat.headline}" if feat.headline else f"Alias: {alias.alias_name}"
            results.append(SearchResult(
                category="gene",
                id=feat.dbxref_id,
                name=display_name,
                description=description,
                link=f"/locus/{feat.feature_name}",
                organism=_get_organism_name(feat.organism),
                highlighted_name=_highlight_text(display_name, query),
                highlighted_description=_highlight_text(description, query),
            ))

    return results


def _search_go_terms_paginated(
    db: Session, query: str, offset: int, limit: int
) -> list[SearchResult]:
    """Search GO terms with pagination."""
    results = []
    normalized = _normalize_query(query)
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Check for GO ID exact match first (only on first page)
    goid_numeric = None
    if normalized.upper().startswith('GO:'):
        try:
            goid_numeric = int(normalized[3:])
        except ValueError:
            pass
    else:
        try:
            goid_numeric = int(normalized)
        except ValueError:
            pass

    if goid_numeric is not None and offset == 0:
        go_exact = db.query(Go).filter(Go.goid == goid_numeric).first()
        if go_exact:
            description = go_exact.go_definition[:200] + "..." if go_exact.go_definition and len(go_exact.go_definition) > 200 else go_exact.go_definition
            results.append(SearchResult(
                category="go_term",
                id=_format_goid(go_exact.goid),
                name=go_exact.go_term,
                description=description,
                link=f"/go/{_format_goid(go_exact.goid)}",
                organism=None,
                highlighted_name=_highlight_text(go_exact.go_term, query),
                highlighted_description=_highlight_text(description, query),
            ))

    # Search by term name
    remaining = limit - len(results)
    adjusted_offset = offset if offset == 0 else offset - (1 if goid_numeric else 0)

    if remaining > 0:
        found_goids = {r.id for r in results}

        go_query = (
            db.query(Go)
            .filter(func.upper(Go.go_term).like(upper_pattern))
            .offset(max(0, adjusted_offset))
            .limit(remaining + len(found_goids))
        )

        for go in go_query:
            formatted_goid = _format_goid(go.goid)
            if formatted_goid not in found_goids:
                description = go.go_definition[:200] + "..." if go.go_definition and len(go.go_definition) > 200 else go.go_definition
                results.append(SearchResult(
                    category="go_term",
                    id=formatted_goid,
                    name=go.go_term,
                    description=description,
                    link=f"/go/{formatted_goid}",
                    organism=None,
                    highlighted_name=_highlight_text(go.go_term, query),
                    highlighted_description=_highlight_text(description, query),
                ))
                if len(results) >= limit:
                    break

    return results[:limit]


def _search_phenotypes_paginated(
    db: Session, query: str, offset: int, limit: int
) -> list[SearchResult]:
    """Search phenotypes with pagination."""
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    pheno_query = (
        db.query(Phenotype.observable)
        .filter(func.upper(Phenotype.observable).like(upper_pattern))
        .distinct()
        .offset(offset)
        .limit(limit)
    )

    for (observable,) in pheno_query:
        results.append(SearchResult(
            category="phenotype",
            id=observable,
            name=observable,
            description=None,
            link=f"/phenotype/search?observable={observable}",
            organism=None,
            highlighted_name=_highlight_text(observable, query),
            highlighted_description=None,
        ))

    return results


def _search_references_paginated(
    db: Session, query: str, offset: int, limit: int
) -> list[SearchResult]:
    """Search references with pagination."""
    results = []
    normalized = _normalize_query(query)
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Check for PubMed ID or dbxref exact match first (only on first page)
    if offset == 0:
        # PubMed ID match
        try:
            pubmed_id = int(normalized)
            ref_exact = db.query(Reference).filter(Reference.pubmed == pubmed_id).first()
            if ref_exact:
                name = f"PMID:{ref_exact.pubmed}" if ref_exact.pubmed else ref_exact.dbxref_id
                results.append(SearchResult(
                    category="reference",
                    id=ref_exact.dbxref_id,
                    name=name,
                    description=ref_exact.citation,
                    link=f"/reference/{ref_exact.dbxref_id}",
                    organism=None,
                    links=_build_reference_links(db, ref_exact),
                    highlighted_name=_highlight_text(name, query),
                    highlighted_description=_highlight_text(ref_exact.citation, query),
                ))
        except ValueError:
            pass

        # dbxref_id match
        if not results:
            ref_by_dbxref = db.query(Reference).filter(
                func.upper(Reference.dbxref_id) == normalized.upper()
            ).first()
            if ref_by_dbxref:
                name = f"PMID:{ref_by_dbxref.pubmed}" if ref_by_dbxref.pubmed else ref_by_dbxref.dbxref_id
                results.append(SearchResult(
                    category="reference",
                    id=ref_by_dbxref.dbxref_id,
                    name=name,
                    description=ref_by_dbxref.citation,
                    link=f"/reference/{ref_by_dbxref.dbxref_id}",
                    organism=None,
                    links=_build_reference_links(db, ref_by_dbxref),
                    highlighted_name=_highlight_text(name, query),
                    highlighted_description=_highlight_text(ref_by_dbxref.citation, query),
                ))

    # Search by citation
    remaining = limit - len(results)
    found_ref_ids = {r.id for r in results}
    adjusted_offset = offset if offset == 0 else offset - len(found_ref_ids)

    if remaining > 0:
        ref_query = (
            db.query(Reference)
            .filter(func.upper(Reference.citation).like(upper_pattern))
            .offset(max(0, adjusted_offset))
            .limit(remaining + len(found_ref_ids))
        )

        for ref in ref_query:
            if ref.dbxref_id not in found_ref_ids:
                name = f"PMID:{ref.pubmed}" if ref.pubmed else ref.dbxref_id
                results.append(SearchResult(
                    category="reference",
                    id=ref.dbxref_id,
                    name=name,
                    description=ref.citation,
                    link=f"/reference/{ref.dbxref_id}",
                    links=_build_reference_links(db, ref),
                    organism=None,
                    highlighted_name=_highlight_text(name, query),
                    highlighted_description=_highlight_text(ref.citation, query),
                ))
                if len(results) >= limit:
                    break

    return results[:limit]
