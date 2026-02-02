"""
Search Service - handles quick search across multiple entity types using Elasticsearch.
"""
from __future__ import annotations

import logging
from typing import Optional

from elasticsearch import Elasticsearch
from sqlalchemy.orm import Session
from sqlalchemy import func

from cgd.core.elasticsearch import get_es_client, INDEX_NAME
from cgd.schemas.search_schema import (
    SearchResult,
    SearchResponse,
    ResolveResponse,
    SearchResultLink,
    AutocompleteSuggestion,
    AutocompleteResponse,
)
from cgd.models.models import (
    Feature,
    Reference,
    Organism,
    RefUrl,
)

logger = logging.getLogger(__name__)


def _format_goid(goid: int) -> str:
    """Format GOID as GO:XXXXXXX (7-digit padded)."""
    return f"GO:{goid:07d}"


def _get_organism_name(organism: Optional[Organism]) -> Optional[str]:
    """Get organism display name."""
    if organism:
        return organism.organism_name
    return None


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

    This function uses SQL for exact ID lookups (not Elasticsearch).

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


def _es_search(
    es: Elasticsearch,
    query: str,
    entity_type: Optional[str] = None,
    limit: int = 20
) -> list[dict]:
    """
    Execute Elasticsearch search query.

    Args:
        es: Elasticsearch client
        query: Search query string
        entity_type: Optional filter by type (gene, go_term, phenotype, reference)
        limit: Maximum results to return

    Returns:
        List of hit source documents
    """
    # Build query
    must_clauses = []
    should_clauses = []

    # Filter by type if specified
    if entity_type:
        must_clauses.append({"term": {"type": entity_type}})

    # Exact matches (highest boost)
    should_clauses.extend([
        {"term": {"gene_name.keyword": {"value": query, "boost": 10}}},
        {"term": {"feature_name": {"value": query.lower(), "boost": 10}}},
        {"term": {"goid": {"value": query.upper(), "boost": 10}}},
        {"term": {"dbxref_id": {"value": query.upper(), "boost": 10}}},
    ])

    # Try numeric match for pubmed
    try:
        pubmed_id = int(query)
        should_clauses.append({"term": {"pubmed": {"value": pubmed_id, "boost": 10}}})
    except ValueError:
        pass

    # Prefix matches (high boost)
    should_clauses.extend([
        {"prefix": {"gene_name.keyword": {"value": query.upper(), "boost": 5}}},
        {"prefix": {"feature_name": {"value": query.lower(), "boost": 5}}},
        {"prefix": {"go_term.keyword": {"value": query.lower(), "boost": 5}}},
        {"prefix": {"observable.keyword": {"value": query.lower(), "boost": 4}}},
    ])

    # Full-text matches with fuzziness
    should_clauses.append({
        "multi_match": {
            "query": query,
            "fields": [
                "name^3",
                "gene_name^3",
                "go_term^2",
                "headline",
                "aliases",
                "go_definition",
                "citation",
                "observable"
            ],
            "type": "best_fields",
            "fuzziness": "AUTO"
        }
    })

    body = {
        "size": limit,
        "query": {
            "bool": {
                "must": must_clauses if must_clauses else [{"match_all": {}}],
                "should": should_clauses,
                "minimum_should_match": 1 if not must_clauses else 0
            }
        },
        "sort": ["_score", {"type": {"order": "asc"}}]
    }

    try:
        response = es.search(index=INDEX_NAME, body=body)
        return [hit["_source"] for hit in response["hits"]["hits"]]
    except Exception as e:
        logger.error(f"Elasticsearch search error: {e}")
        return []


def _hit_to_search_result(hit: dict, db: Optional[Session] = None) -> SearchResult:
    """Convert Elasticsearch hit to SearchResult."""
    entity_type = hit.get("type", "")

    # Build description
    description = None
    if entity_type == "gene":
        description = hit.get("headline")
        if hit.get("aliases"):
            alias_text = f"Aliases: {hit['aliases']}"
            if description:
                description = f"{description} ({alias_text})"
            else:
                description = alias_text
    elif entity_type == "go_term":
        go_def = hit.get("go_definition")
        if go_def:
            description = go_def[:200] + "..." if len(go_def) > 200 else go_def
    elif entity_type == "reference":
        description = hit.get("citation")
    elif entity_type == "phenotype":
        description = "Phenotype"

    # Map type to category
    category_map = {
        "gene": "gene",
        "go_term": "go_term",
        "phenotype": "phenotype",
        "reference": "reference"
    }

    return SearchResult(
        category=category_map.get(entity_type, entity_type),
        id=hit.get("id", ""),
        name=hit.get("name", ""),
        description=description,
        link=hit.get("link", ""),
        organism=hit.get("organism"),
    )


def search_genes(query: str, limit: int = 20, es: Optional[Elasticsearch] = None) -> list[SearchResult]:
    """Search genes using Elasticsearch."""
    if es is None:
        es = get_es_client()

    hits = _es_search(es, query, entity_type="gene", limit=limit)
    return [_hit_to_search_result(hit) for hit in hits]


def search_go_terms(query: str, limit: int = 20, es: Optional[Elasticsearch] = None) -> list[SearchResult]:
    """Search GO terms using Elasticsearch."""
    if es is None:
        es = get_es_client()

    hits = _es_search(es, query, entity_type="go_term", limit=limit)
    return [_hit_to_search_result(hit) for hit in hits]


def search_phenotypes(query: str, limit: int = 20, es: Optional[Elasticsearch] = None) -> list[SearchResult]:
    """Search phenotypes using Elasticsearch."""
    if es is None:
        es = get_es_client()

    hits = _es_search(es, query, entity_type="phenotype", limit=limit)
    return [_hit_to_search_result(hit) for hit in hits]


def search_references(
    query: str,
    limit: int = 20,
    db: Optional[Session] = None,
    es: Optional[Elasticsearch] = None
) -> list[SearchResult]:
    """Search references using Elasticsearch."""
    if es is None:
        es = get_es_client()

    hits = _es_search(es, query, entity_type="reference", limit=limit)
    results = []

    for hit in hits:
        result = _hit_to_search_result(hit)

        # Add reference links if db session provided
        if db and hit.get("id"):
            ref = db.query(Reference).filter(Reference.dbxref_id == hit["id"]).first()
            if ref:
                result.links = _build_reference_links(db, ref)

        results.append(result)

    return results


def quick_search(db: Session, query: str, limit: int = 20) -> SearchResponse:
    """
    Search all categories using Elasticsearch.

    Returns results grouped by category.
    """
    es = get_es_client()

    # Search all types at once, get more results to properly group
    all_hits = _es_search(es, query, entity_type=None, limit=limit * 4)

    # Group by type
    genes = []
    go_terms = []
    phenotypes = []
    references = []

    for hit in all_hits:
        entity_type = hit.get("type", "")
        result = _hit_to_search_result(hit)

        if entity_type == "gene" and len(genes) < limit:
            genes.append(result)
        elif entity_type == "go_term" and len(go_terms) < limit:
            go_terms.append(result)
        elif entity_type == "phenotype" and len(phenotypes) < limit:
            phenotypes.append(result)
        elif entity_type == "reference" and len(references) < limit:
            # Add reference links
            if hit.get("id"):
                ref = db.query(Reference).filter(Reference.dbxref_id == hit["id"]).first()
                if ref:
                    result.links = _build_reference_links(db, ref)
            references.append(result)

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
    Get autocomplete suggestions using Elasticsearch prefix matching.

    Optimized for speed with prefix matching. Returns suggestions
    prioritized by category: genes > GO terms > phenotypes > references.

    Args:
        db: Database session (kept for interface compatibility)
        query: Search query (minimum 1 character)
        limit: Maximum total suggestions to return

    Returns:
        AutocompleteResponse with flat list of suggestions
    """
    normalized = query.strip()

    if len(normalized) < 1:
        return AutocompleteResponse(query=query, suggestions=[])

    es = get_es_client()

    # Build prefix-focused query for autocomplete
    should_clauses = [
        # Exact matches
        {"term": {"gene_name.keyword": {"value": normalized, "boost": 10}}},
        {"term": {"gene_name.keyword": {"value": normalized.upper(), "boost": 10}}},

        # Prefix matches (primary for autocomplete)
        {"prefix": {"gene_name.keyword": {"value": normalized.upper(), "boost": 8}}},
        {"prefix": {"feature_name": {"value": normalized.lower(), "boost": 7}}},
        {"prefix": {"go_term.keyword": {"value": normalized.lower(), "boost": 6}}},
        {"prefix": {"observable.keyword": {"value": normalized.lower(), "boost": 5}}},

        # Match phrase prefix for partial word matching
        {"match_phrase_prefix": {"name": {"query": normalized, "boost": 3}}},
        {"match_phrase_prefix": {"aliases": {"query": normalized, "boost": 2}}},
    ]

    # Add pubmed exact match if numeric
    try:
        pubmed_id = int(normalized)
        should_clauses.append({"term": {"pubmed": {"value": pubmed_id, "boost": 10}}})
    except ValueError:
        pass

    body = {
        "size": limit * 2,  # Get extra to deduplicate
        "query": {
            "bool": {
                "should": should_clauses,
                "minimum_should_match": 1
            }
        },
        "sort": ["_score", {"type": {"order": "asc"}}]  # Genes first
    }

    try:
        response = es.search(index=INDEX_NAME, body=body)
        hits = response["hits"]["hits"]
    except Exception as e:
        logger.error(f"Elasticsearch autocomplete error: {e}")
        return AutocompleteResponse(query=query, suggestions=[])

    # Convert hits to suggestions, avoiding duplicates
    suggestions: list[AutocompleteSuggestion] = []
    seen_texts = set()

    for hit in hits:
        if len(suggestions) >= limit:
            break

        source = hit["_source"]
        entity_type = source.get("type", "")

        # Determine display text and description based on type
        if entity_type == "gene":
            text = source.get("gene_name") or source.get("feature_name") or source.get("name", "")
            description = source.get("headline")
            if description and len(description) > 80:
                description = description[:80] + "..."
            category = "gene"
        elif entity_type == "go_term":
            goid = source.get("goid", "")
            go_term = source.get("go_term", source.get("name", ""))
            text = f"{goid} - {go_term}"
            description = source.get("go_aspect")
            category = "go_term"
        elif entity_type == "phenotype":
            text = source.get("observable") or source.get("name", "")
            description = "Phenotype"
            category = "phenotype"
        elif entity_type == "reference":
            pubmed = source.get("pubmed")
            text = f"PMID:{pubmed}" if pubmed else source.get("id", "")
            description = source.get("citation")
            if description and len(description) > 80:
                description = description[:80] + "..."
            category = "reference"
        else:
            continue

        # Skip duplicates
        if text.lower() in seen_texts:
            continue
        seen_texts.add(text.lower())

        suggestions.append(AutocompleteSuggestion(
            text=text,
            category=category,
            link=source.get("link", ""),
            description=description,
        ))

    return AutocompleteResponse(query=query, suggestions=suggestions)
