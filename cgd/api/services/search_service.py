"""
Search Service - handles quick search across multiple entity types.
"""
from __future__ import annotations

from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, or_

from cgd.schemas.search_schema import SearchResult, SearchResponse
from cgd.models.models import (
    Feature,
    Go,
    Phenotype,
    Reference,
    Alias,
    FeatAlias,
    Organism,
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


def search_genes(db: Session, query: str, limit: int = 20) -> list[SearchResult]:
    """
    Search genes/loci by gene_name, feature_name, or aliases.

    Returns SearchResult list with category="gene".
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Search in Feature table: gene_name, feature_name
    feature_query = (
        db.query(Feature)
        .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
        .filter(
            or_(
                func.upper(Feature.gene_name).like(upper_pattern),
                func.upper(Feature.feature_name).like(upper_pattern),
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
                results.append(SearchResult(
                    category="gene",
                    id=feat.dbxref_id,
                    name=display_name,
                    description=f"Alias: {alias.alias_name} - {feat.headline}" if feat.headline else f"Alias: {alias.alias_name}",
                    link=f"/locus/{feat.feature_name}",
                    organism=_get_organism_name(feat.organism),
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
            results.append(SearchResult(
                category="go_term",
                id=_format_goid(go_exact.goid),
                name=go_exact.go_term,
                description=go_exact.go_definition[:200] + "..." if go_exact.go_definition and len(go_exact.go_definition) > 200 else go_exact.go_definition,
                link=f"/go/{_format_goid(go_exact.goid)}",
                organism=None,
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
                results.append(SearchResult(
                    category="go_term",
                    id=formatted_goid,
                    name=go.go_term,
                    description=go.go_definition[:200] + "..." if go.go_definition and len(go.go_definition) > 200 else go.go_definition,
                    link=f"/go/{formatted_goid}",
                    organism=None,
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
            link=f"/phenotype?observable={observable}",
            organism=None,
        ))

    return results


def search_references(db: Session, query: str, limit: int = 20) -> list[SearchResult]:
    """
    Search references by PubMed ID or citation.

    Returns SearchResult list with category="reference".
    """
    results = []
    normalized = _normalize_query(query)

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
            results.append(SearchResult(
                category="reference",
                id=ref_exact.dbxref_id,
                name=f"PMID:{ref_exact.pubmed}" if ref_exact.pubmed else ref_exact.dbxref_id,
                description=ref_exact.citation[:200] + "..." if len(ref_exact.citation) > 200 else ref_exact.citation,
                link=f"/reference/{ref_exact.dbxref_id}",
                organism=None,
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
                results.append(SearchResult(
                    category="reference",
                    id=ref.dbxref_id,
                    name=f"PMID:{ref.pubmed}" if ref.pubmed else ref.dbxref_id,
                    description=ref.citation[:200] + "..." if len(ref.citation) > 200 else ref.citation,
                    link=f"/reference/{ref.dbxref_id}",
                    organism=None,
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
