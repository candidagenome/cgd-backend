"""
Elasticsearch Search Service - handles search queries using Elasticsearch.

This provides fast text search as an alternative to the Oracle-based search_service.
"""
from __future__ import annotations

import logging
import re
from typing import Optional

from elasticsearch import Elasticsearch

from cgd.core.elasticsearch import INDEX_NAME, get_es_client
from cgd.schemas.search_schema import (
    SearchResult,
    SearchResponse,
    AutocompleteSuggestion,
    AutocompleteResponse,
    CategorySearchResponse,
    TextSearchResult,
    TextSearchCategoryResult,
    TextSearchResponse,
    TextSearchCategoryPagedResponse,
)

logger = logging.getLogger(__name__)

# Organism display priority (lower index = higher priority)
ORGANISM_PRIORITY = [
    'Candida albicans SC5314',
    'Candida glabrata CBS138',
    'Candida auris B8441',
    'Candida dubliniensis CD36',
    'Candida parapsilosis CDC317',
]


def _get_organism_priority(organism_name: Optional[str]) -> int:
    """Get sort priority for an organism (lower = higher priority)."""
    if not organism_name:
        return 999
    try:
        return ORGANISM_PRIORITY.index(organism_name)
    except ValueError:
        return 998


def _highlight_text(text: Optional[str], query: str) -> Optional[str]:
    """
    Highlight matching query text with <mark> tags.
    """
    if not text or not query:
        return text

    clean_query = query.strip().replace('*', '').replace('%', '')
    if not clean_query:
        return text

    pattern = re.compile(re.escape(clean_query), re.IGNORECASE)

    def replacer(match):
        return f"<mark>{match.group(0)}</mark>"

    return pattern.sub(replacer, text)


def _extract_highlight(highlights: dict, field: str, fallback: Optional[str]) -> Optional[str]:
    """Extract highlighted text from ES response, or return fallback with manual highlighting."""
    if highlights and field in highlights:
        # ES returns list of highlighted fragments
        return highlights[field][0]
    return fallback


def _build_es_query(query: str, size: int = 100) -> dict:
    """
    Build Elasticsearch query for multi-field search.

    Uses multi_match with cross_fields for best relevance.
    """
    return {
        "query": {
            "bool": {
                "should": [
                    # Exact matches on keyword fields (highest boost)
                    {"term": {"gene_name.keyword": {"value": query.upper(), "boost": 10}}},
                    {"term": {"feature_name": {"value": query.upper(), "boost": 10}}},
                    {"term": {"goid": {"value": query.upper(), "boost": 10}}},
                    # Prefix matches (high boost for autocomplete)
                    {"prefix": {"gene_name.keyword": {"value": query.upper(), "boost": 5}}},
                    {"prefix": {"feature_name": {"value": query.upper(), "boost": 5}}},
                    {"prefix": {"go_term.keyword": {"value": query.lower(), "boost": 5}}},
                    {"prefix": {"observable.keyword": {"value": query.lower(), "boost": 5}}},
                    # Full-text search across all fields
                    {
                        "multi_match": {
                            "query": query,
                            "fields": [
                                "name^3",
                                "gene_name^3",
                                "feature_name^2",
                                "aliases^2",
                                "headline",
                                "go_term^3",
                                "go_definition",
                                "observable^3",
                                "citation",
                            ],
                            "type": "best_fields",
                            "fuzziness": "AUTO",
                        }
                    },
                ],
                "minimum_should_match": 1,
            }
        },
        "size": size,
        "highlight": {
            "fields": {
                "name": {},
                "gene_name": {},
                "headline": {},
                "go_term": {},
                "go_definition": {},
                "observable": {},
                "citation": {},
                "aliases": {},
            },
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"],
        },
        "aggs": {
            "by_type": {
                "terms": {"field": "type", "size": 10}
            }
        },
    }


def _build_autocomplete_query(query: str, size: int = 50) -> dict:
    """
    Build Elasticsearch query optimized for autocomplete.

    Prioritizes prefix matching for fast suggestions.
    """
    return {
        "query": {
            "bool": {
                "should": [
                    # Exact match (highest priority)
                    {"term": {"gene_name.keyword": {"value": query.upper(), "boost": 20}}},
                    {"term": {"feature_name": {"value": query.upper(), "boost": 20}}},
                    # Prefix match (high priority for autocomplete)
                    {"prefix": {"gene_name.keyword": {"value": query.upper(), "boost": 10}}},
                    {"prefix": {"feature_name": {"value": query.upper(), "boost": 8}}},
                    {"prefix": {"name.keyword": {"value": query, "boost": 5}}},
                    {"prefix": {"go_term.keyword": {"value": query.lower(), "boost": 5}}},
                    {"prefix": {"observable.keyword": {"value": query.lower(), "boost": 5}}},
                    # Fallback to contains match
                    {
                        "multi_match": {
                            "query": query,
                            "fields": ["name^2", "gene_name^2", "go_term^2", "observable^2"],
                            "type": "phrase_prefix",
                        }
                    },
                ],
                "minimum_should_match": 1,
            }
        },
        "size": size,
        "highlight": {
            "fields": {
                "name": {},
                "gene_name": {},
                "go_term": {},
                "observable": {},
            },
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"],
        },
    }


def _parse_gene_result(hit: dict, query: str) -> SearchResult:
    """Parse ES hit into SearchResult for gene type."""
    source = hit["_source"]
    highlights = hit.get("highlight", {})

    display_name = source.get("gene_name") or source.get("feature_name") or source.get("name")
    headline = source.get("headline")

    # Use ES highlighting if available, otherwise manual
    highlighted_name = _extract_highlight(highlights, "gene_name", None)
    if not highlighted_name:
        highlighted_name = _extract_highlight(highlights, "name", None)
    if not highlighted_name:
        highlighted_name = _highlight_text(display_name, query)

    highlighted_desc = _extract_highlight(highlights, "headline", None)
    if not highlighted_desc and headline:
        highlighted_desc = _highlight_text(headline, query)

    return SearchResult(
        category="gene",
        id=source.get("dbxref_id") or source.get("id", ""),
        name=display_name or "",
        description=headline,
        link=source.get("link") or f"/locus/{display_name}",
        organism=source.get("organism"),
        highlighted_name=highlighted_name,
        highlighted_description=highlighted_desc,
    )


def _parse_go_result(hit: dict, query: str) -> SearchResult:
    """Parse ES hit into SearchResult for GO term type."""
    source = hit["_source"]
    highlights = hit.get("highlight", {})

    go_term = source.get("go_term") or source.get("name")
    goid = source.get("goid") or source.get("id")
    go_definition = source.get("go_definition")

    # Truncate long definitions
    description = go_definition
    if description and len(description) > 200:
        description = description[:200] + "..."

    highlighted_name = _extract_highlight(highlights, "go_term", None)
    if not highlighted_name:
        highlighted_name = _highlight_text(go_term, query)

    highlighted_desc = _extract_highlight(highlights, "go_definition", None)
    if not highlighted_desc and description:
        highlighted_desc = _highlight_text(description, query)

    return SearchResult(
        category="go_term",
        id=goid or "",
        name=go_term or "",
        description=description,
        link=source.get("link") or f"/go/{goid}",
        organism=None,
        highlighted_name=highlighted_name,
        highlighted_description=highlighted_desc,
    )


def _parse_phenotype_result(hit: dict, query: str) -> SearchResult:
    """Parse ES hit into SearchResult for phenotype type."""
    source = hit["_source"]
    highlights = hit.get("highlight", {})

    observable = source.get("observable") or source.get("name")

    highlighted_name = _extract_highlight(highlights, "observable", None)
    if not highlighted_name:
        highlighted_name = _highlight_text(observable, query)

    return SearchResult(
        category="phenotype",
        id=observable or "",
        name=observable or "",
        description=None,
        link=source.get("link") or f"/phenotype/search?observable={observable}",
        organism=None,
        highlighted_name=highlighted_name,
        highlighted_description=None,
    )


def _parse_reference_result(hit: dict, query: str) -> SearchResult:
    """Parse ES hit into SearchResult for reference type."""
    source = hit["_source"]
    highlights = hit.get("highlight", {})

    pubmed = source.get("pubmed")
    dbxref_id = source.get("id")
    citation = source.get("citation")

    name = f"PMID:{pubmed}" if pubmed else dbxref_id or ""

    highlighted_name = _highlight_text(name, query)
    highlighted_desc = _extract_highlight(highlights, "citation", None)
    if not highlighted_desc and citation:
        highlighted_desc = _highlight_text(citation, query)

    return SearchResult(
        category="reference",
        id=dbxref_id or "",
        name=name,
        description=citation,
        link=source.get("link") or f"/reference/{dbxref_id}",
        organism=None,
        highlighted_name=highlighted_name,
        highlighted_description=highlighted_desc,
    )


def _parse_hit(hit: dict, query: str) -> Optional[SearchResult]:
    """Parse ES hit into SearchResult based on document type."""
    doc_type = hit["_source"].get("type")

    if doc_type == "gene":
        return _parse_gene_result(hit, query)
    elif doc_type == "go_term":
        return _parse_go_result(hit, query)
    elif doc_type == "phenotype":
        return _parse_phenotype_result(hit, query)
    elif doc_type == "reference":
        return _parse_reference_result(hit, query)
    else:
        logger.warning(f"Unknown document type: {doc_type}")
        return None


def quick_search(
    es: Elasticsearch,
    query: str,
    limit: int = 20,
) -> SearchResponse:
    """
    Quick search across all categories using Elasticsearch.

    Returns results grouped by category with counts.
    """
    # Build and execute ES query
    es_query = _build_es_query(query, size=limit * 5)  # Fetch extra for filtering

    try:
        response = es.search(index=INDEX_NAME, body=es_query)
    except Exception as e:
        logger.error(f"Elasticsearch query failed: {e}")
        # Return empty response on error
        return SearchResponse(
            query=query,
            total_results=0,
            results_by_category={},
            counts_by_category={},
        )

    # Parse results by category
    results_by_category: dict[str, list[SearchResult]] = {
        "genes": [],
        "go_terms": [],
        "phenotypes": [],
        "references": [],
    }

    for hit in response["hits"]["hits"]:
        result = _parse_hit(hit, query)
        if not result:
            continue

        # Map category names
        category_map = {
            "gene": "genes",
            "go_term": "go_terms",
            "phenotype": "phenotypes",
            "reference": "references",
        }
        cat_key = category_map.get(result.category, result.category)

        if cat_key in results_by_category and len(results_by_category[cat_key]) < limit:
            results_by_category[cat_key].append(result)

    # Sort genes by organism priority
    if results_by_category["genes"]:
        results_by_category["genes"].sort(
            key=lambda r: (_get_organism_priority(r.organism), r.name or '')
        )

    # Get counts from aggregations
    counts_by_category: dict[str, int] = {}
    type_to_category = {
        "gene": "genes",
        "go_term": "go_terms",
        "phenotype": "phenotypes",
        "reference": "references",
    }

    for bucket in response.get("aggregations", {}).get("by_type", {}).get("buckets", []):
        doc_type = bucket["key"]
        count = bucket["doc_count"]
        cat_key = type_to_category.get(doc_type)
        if cat_key:
            counts_by_category[cat_key] = count

    # Remove empty categories
    results_by_category = {k: v for k, v in results_by_category.items() if v}
    counts_by_category = {k: v for k, v in counts_by_category.items() if v > 0}

    total = sum(counts_by_category.values())

    return SearchResponse(
        query=query,
        total_results=total,
        results_by_category=results_by_category,
        counts_by_category=counts_by_category,
    )


def get_autocomplete_suggestions(
    es: Elasticsearch,
    query: str,
    limit: int = 10,
) -> AutocompleteResponse:
    """
    Get autocomplete suggestions using Elasticsearch.

    Returns suggestions prioritized by category: genes > GO terms > phenotypes > references.
    """
    if len(query.strip()) < 1:
        return AutocompleteResponse(query=query, suggestions=[])

    # Build and execute ES query
    es_query = _build_autocomplete_query(query, size=limit * 4)

    try:
        response = es.search(index=INDEX_NAME, body=es_query)
    except Exception as e:
        logger.error(f"Elasticsearch autocomplete query failed: {e}")
        return AutocompleteResponse(query=query, suggestions=[])

    # Collect suggestions by category
    genes: list[AutocompleteSuggestion] = []
    go_terms: list[AutocompleteSuggestion] = []
    phenotypes: list[AutocompleteSuggestion] = []
    references: list[AutocompleteSuggestion] = []

    seen_texts = set()

    for hit in response["hits"]["hits"]:
        source = hit["_source"]
        highlights = hit.get("highlight", {})
        doc_type = source.get("type")

        if doc_type == "gene":
            display_name = source.get("gene_name") or source.get("feature_name") or source.get("name")
            if display_name and display_name not in seen_texts:
                seen_texts.add(display_name)
                headline = source.get("headline")
                description = headline[:80] + "..." if headline and len(headline) > 80 else headline

                highlighted_text = _extract_highlight(highlights, "gene_name", None)
                if not highlighted_text:
                    highlighted_text = _highlight_text(display_name, query)

                genes.append(AutocompleteSuggestion(
                    text=display_name,
                    category="gene",
                    link=source.get("link") or f"/locus/{display_name}",
                    description=description,
                    highlighted_text=highlighted_text,
                    highlighted_description=_highlight_text(description, query) if description else None,
                ))

        elif doc_type == "go_term":
            go_term = source.get("go_term") or source.get("name")
            goid = source.get("goid") or source.get("id")
            text = f"{goid} - {go_term}" if goid and go_term else go_term or goid

            if text and text not in seen_texts:
                seen_texts.add(text)
                go_aspect = source.get("go_aspect")

                highlighted_text = _extract_highlight(highlights, "go_term", None)
                if highlighted_text and goid:
                    highlighted_text = f"{goid} - {highlighted_text}"
                elif not highlighted_text:
                    highlighted_text = _highlight_text(text, query)

                go_terms.append(AutocompleteSuggestion(
                    text=text,
                    category="go_term",
                    link=source.get("link") or f"/go/{goid}",
                    description=go_aspect,
                    highlighted_text=highlighted_text,
                    highlighted_description=_highlight_text(go_aspect, query) if go_aspect else None,
                ))

        elif doc_type == "phenotype":
            observable = source.get("observable") or source.get("name")
            if observable and observable not in seen_texts:
                seen_texts.add(observable)

                highlighted_text = _extract_highlight(highlights, "observable", None)
                if not highlighted_text:
                    highlighted_text = _highlight_text(observable, query)

                phenotypes.append(AutocompleteSuggestion(
                    text=observable,
                    category="phenotype",
                    link=source.get("link") or f"/phenotype/search?observable={observable}",
                    description="Phenotype",
                    highlighted_text=highlighted_text,
                    highlighted_description=None,
                ))

        elif doc_type == "reference":
            pubmed = source.get("pubmed")
            dbxref_id = source.get("id")
            text = f"PMID:{pubmed}" if pubmed else dbxref_id

            if text and text not in seen_texts:
                seen_texts.add(text)
                citation = source.get("citation")
                description = citation[:80] + "..." if citation and len(citation) > 80 else citation

                references.append(AutocompleteSuggestion(
                    text=text,
                    category="reference",
                    link=source.get("link") or f"/reference/{dbxref_id}",
                    description=description,
                    highlighted_text=_highlight_text(text, query),
                    highlighted_description=_highlight_text(description, query) if description else None,
                ))

    # Sort genes by organism priority
    genes.sort(key=lambda s: _get_organism_priority(None))  # TODO: track organism in suggestion

    # Combine suggestions with priority limits
    suggestions: list[AutocompleteSuggestion] = []

    # Add up to 5 genes
    suggestions.extend(genes[:5])
    # Add up to 3 GO terms
    suggestions.extend(go_terms[:3])
    # Add up to 2 phenotypes
    suggestions.extend(phenotypes[:2])
    # Add references if we have room
    remaining = limit - len(suggestions)
    if remaining > 0:
        suggestions.extend(references[:remaining])

    return AutocompleteResponse(query=query, suggestions=suggestions[:limit])


def check_es_available(es: Elasticsearch) -> bool:
    """Check if Elasticsearch is available and the index exists."""
    try:
        return es.indices.exists(index=INDEX_NAME)
    except Exception as e:
        logger.warning(f"Elasticsearch not available: {e}")
        return False


# Mapping from API category names to ES document types
CATEGORY_TO_ES_TYPE = {
    "genes": "gene",
    "go_terms": "go_term",
    "phenotypes": "phenotype",
    "references": "reference",
}

# Categories supported by ES (others fall back to Oracle)
ES_SUPPORTED_CATEGORIES = set(CATEGORY_TO_ES_TYPE.keys())

# Text search category mapping (subset that ES supports)
TEXT_CATEGORY_TO_ES_TYPE = {
    "genes": "gene",
    "descriptions": "gene",  # Uses headline field
    "go_terms": "go_term",
    "phenotypes": "phenotype",
    "abstracts": "reference",  # Uses citation field (abstract not indexed yet)
}

TEXT_CATEGORY_DISPLAY_NAMES = {
    "genes": "Gene names",
    "descriptions": "General Descriptions",
    "go_terms": "Gene ontology",
    "phenotypes": "Phenotypes",
    "abstracts": "Paper Abstracts",
}


def _build_category_query(query: str, es_type: str, size: int = 1000) -> dict:
    """Build ES query for a specific category/type."""
    # Define fields to search based on type
    fields_by_type = {
        "gene": ["gene_name^3", "feature_name^2", "aliases^2", "headline", "dbxref_id"],
        "go_term": ["go_term^3", "goid^2", "go_definition"],
        "phenotype": ["observable^3"],
        "reference": ["citation^2", "title"],
    }

    fields = fields_by_type.get(es_type, ["name"])

    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"type": es_type}},
                ],
                "should": [
                    # Exact/prefix matches
                    {"prefix": {"name.keyword": {"value": query, "boost": 5}}},
                    # Full-text search
                    {
                        "multi_match": {
                            "query": query,
                            "fields": fields,
                            "type": "best_fields",
                            "fuzziness": "AUTO",
                        }
                    },
                ],
                "minimum_should_match": 1,
            }
        },
        "size": size,
        "highlight": {
            "fields": {
                "name": {},
                "gene_name": {},
                "headline": {},
                "go_term": {},
                "go_definition": {},
                "observable": {},
                "citation": {},
                "aliases": {},
            },
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"],
        },
        "aggs": {
            "by_organism": {
                "terms": {"field": "organism", "size": 20}
            }
        },
    }


def search_category(
    es: Elasticsearch,
    query: str,
    category: str,
) -> Optional[CategorySearchResponse]:
    """
    Search within a specific category using Elasticsearch.

    Returns None if category is not supported by ES (caller should fall back to Oracle).
    """
    if category not in ES_SUPPORTED_CATEGORIES:
        return None

    es_type = CATEGORY_TO_ES_TYPE[category]
    es_query = _build_category_query(query, es_type)

    try:
        response = es.search(index=INDEX_NAME, body=es_query)
    except Exception as e:
        logger.error(f"Elasticsearch category search failed: {e}")
        return None

    # Parse results
    results: list[SearchResult] = []
    for hit in response["hits"]["hits"]:
        result = _parse_hit(hit, query)
        if result:
            results.append(result)

    # Sort genes by organism priority
    if category == "genes":
        results.sort(key=lambda r: (_get_organism_priority(r.organism), r.name or ''))

    # Get organism counts from aggregations
    organism_counts: Optional[dict[str, int]] = None
    if category == "genes":
        org_buckets = response.get("aggregations", {}).get("by_organism", {}).get("buckets", [])
        if org_buckets:
            organism_counts = {
                bucket["key"]: bucket["doc_count"]
                for bucket in org_buckets
                if bucket["key"]
            }

    return CategorySearchResponse(
        query=query,
        category=category,
        results=results,
        total_count=len(results),
        organism_counts=organism_counts,
    )


def _parse_text_search_result(hit: dict, query: str, category: str) -> TextSearchResult:
    """Parse ES hit into TextSearchResult."""
    source = hit["_source"]
    highlights = hit.get("highlight", {})
    doc_type = source.get("type")

    if doc_type == "gene":
        display_name = source.get("gene_name") or source.get("feature_name") or source.get("name")
        headline = source.get("headline")

        highlighted_name = _extract_highlight(highlights, "gene_name", None)
        if not highlighted_name:
            highlighted_name = _highlight_text(display_name, query)

        highlighted_desc = _extract_highlight(highlights, "headline", None)
        if not highlighted_desc and headline:
            highlighted_desc = _highlight_text(headline, query)

        # For "descriptions" category, use headline as match context
        match_context = None
        if category == "descriptions" and headline:
            match_context = _extract_highlight(highlights, "headline", headline)

        return TextSearchResult(
            category=category,
            id=source.get("dbxref_id") or source.get("id", ""),
            name=display_name or "",
            description=headline,
            link=source.get("link") or f"/locus/{display_name}",
            organism=source.get("organism"),
            match_context=match_context,
            highlighted_name=highlighted_name,
            highlighted_description=highlighted_desc,
        )

    elif doc_type == "go_term":
        go_term = source.get("go_term") or source.get("name")
        goid = source.get("goid") or source.get("id")
        go_definition = source.get("go_definition")

        description = go_definition
        if description and len(description) > 200:
            description = description[:200] + "..."

        highlighted_name = _extract_highlight(highlights, "go_term", None)
        if not highlighted_name:
            highlighted_name = _highlight_text(go_term, query)

        return TextSearchResult(
            category="go_terms",
            id=goid or "",
            name=go_term or "",
            description=description,
            link=source.get("link") or f"/go/{goid}",
            organism=None,
            highlighted_name=highlighted_name,
            highlighted_description=_highlight_text(description, query) if description else None,
        )

    elif doc_type == "phenotype":
        observable = source.get("observable") or source.get("name")

        highlighted_name = _extract_highlight(highlights, "observable", None)
        if not highlighted_name:
            highlighted_name = _highlight_text(observable, query)

        return TextSearchResult(
            category="phenotypes",
            id=observable or "",
            name=observable or "",
            description=None,
            link=source.get("link") or f"/phenotype/search?observable={observable}",
            organism=None,
            highlighted_name=highlighted_name,
        )

    elif doc_type == "reference":
        pubmed = source.get("pubmed")
        dbxref_id = source.get("id")
        citation = source.get("citation")

        name = f"PMID:{pubmed}" if pubmed else dbxref_id or ""

        highlighted_desc = _extract_highlight(highlights, "citation", None)
        if not highlighted_desc and citation:
            highlighted_desc = _highlight_text(citation, query)

        return TextSearchResult(
            category="abstracts",
            id=dbxref_id or "",
            name=name,
            description=citation,
            link=source.get("link") or f"/reference/{dbxref_id}",
            organism=None,
            match_context=highlighted_desc,
            highlighted_name=_highlight_text(name, query),
            highlighted_description=highlighted_desc,
        )

    # Fallback
    return TextSearchResult(
        category=category,
        id=source.get("id", ""),
        name=source.get("name", ""),
        description=source.get("description"),
        link=source.get("link"),
        organism=source.get("organism"),
    )


def _build_text_search_query(query: str, size_per_type: int = 10) -> dict:
    """Build ES query for text search across all types."""
    return {
        "query": {
            "multi_match": {
                "query": query,
                "fields": [
                    "name^3",
                    "gene_name^3",
                    "feature_name^2",
                    "aliases^2",
                    "headline^2",
                    "go_term^3",
                    "go_definition",
                    "observable^3",
                    "citation",
                ],
                "type": "best_fields",
                "fuzziness": "AUTO",
            }
        },
        "size": size_per_type * 5,  # Fetch extra to distribute across categories
        "highlight": {
            "fields": {
                "name": {},
                "gene_name": {},
                "headline": {},
                "go_term": {},
                "go_definition": {},
                "observable": {},
                "citation": {},
                "aliases": {},
            },
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"],
        },
        "aggs": {
            "by_type": {
                "terms": {"field": "type", "size": 10}
            }
        },
    }


def text_search(
    es: Elasticsearch,
    query: str,
    limit: int = 10,
) -> Optional[TextSearchResponse]:
    """
    Text search across ES-indexed categories.

    Returns results for categories: genes, descriptions, go_terms, phenotypes, abstracts.
    Returns None on error (caller should fall back to Oracle).

    Note: This only covers ES-indexed categories. Oracle-only categories like
    colleagues, authors, pathways, paragraphs, etc. require Oracle fallback.
    """
    es_query = _build_text_search_query(query, limit)

    try:
        response = es.search(index=INDEX_NAME, body=es_query)
    except Exception as e:
        logger.error(f"Elasticsearch text search failed: {e}")
        return None

    # Group results by category
    results_by_category: dict[str, list[TextSearchResult]] = {
        "genes": [],
        "descriptions": [],
        "go_terms": [],
        "phenotypes": [],
        "abstracts": [],
    }

    for hit in response["hits"]["hits"]:
        doc_type = hit["_source"].get("type")

        if doc_type == "gene":
            # Add to both genes and descriptions (if headline matches)
            gene_result = _parse_text_search_result(hit, query, "genes")
            if len(results_by_category["genes"]) < limit:
                results_by_category["genes"].append(gene_result)

            # Check if headline contains the query for descriptions category
            headline = hit["_source"].get("headline", "")
            if headline and query.lower() in headline.lower():
                desc_result = _parse_text_search_result(hit, query, "descriptions")
                if len(results_by_category["descriptions"]) < limit:
                    results_by_category["descriptions"].append(desc_result)

        elif doc_type == "go_term":
            result = _parse_text_search_result(hit, query, "go_terms")
            if len(results_by_category["go_terms"]) < limit:
                results_by_category["go_terms"].append(result)

        elif doc_type == "phenotype":
            result = _parse_text_search_result(hit, query, "phenotypes")
            if len(results_by_category["phenotypes"]) < limit:
                results_by_category["phenotypes"].append(result)

        elif doc_type == "reference":
            result = _parse_text_search_result(hit, query, "abstracts")
            if len(results_by_category["abstracts"]) < limit:
                results_by_category["abstracts"].append(result)

    # Sort genes by organism priority
    if results_by_category["genes"]:
        results_by_category["genes"].sort(
            key=lambda r: (_get_organism_priority(r.organism), r.name or '')
        )

    # Build category results
    categories: list[TextSearchCategoryResult] = []
    total_results = 0

    for cat_key, results in results_by_category.items():
        if results:
            display_name = TEXT_CATEGORY_DISPLAY_NAMES.get(cat_key, cat_key)
            categories.append(TextSearchCategoryResult(
                category=cat_key,
                display_name=display_name,
                count=len(results),
                results=results,
            ))
            total_results += len(results)

    return TextSearchResponse(
        query=query,
        total_results=total_results,
        categories=categories,
    )


def text_search_category(
    es: Elasticsearch,
    query: str,
    category: str,
) -> Optional[TextSearchCategoryPagedResponse]:
    """
    Text search within a specific category using Elasticsearch.

    Returns None if category is not supported by ES (caller should fall back to Oracle).
    """
    if category not in TEXT_CATEGORY_TO_ES_TYPE:
        return None

    es_type = TEXT_CATEGORY_TO_ES_TYPE[category]

    # Special handling for descriptions - search headline field specifically
    if category == "descriptions":
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "gene"}},
                        {"match": {"headline": query}},
                    ]
                }
            },
            "size": 1000,
            "highlight": {
                "fields": {"headline": {}},
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
            },
            "aggs": {
                "by_organism": {"terms": {"field": "organism", "size": 20}}
            },
        }
    else:
        es_query = _build_category_query(query, es_type, size=1000)

    try:
        response = es.search(index=INDEX_NAME, body=es_query)
    except Exception as e:
        logger.error(f"Elasticsearch text search category failed: {e}")
        return None

    # Parse results
    results: list[TextSearchResult] = []
    for hit in response["hits"]["hits"]:
        result = _parse_text_search_result(hit, query, category)
        results.append(result)

    # Sort genes/descriptions by organism priority
    if category in ["genes", "descriptions"]:
        results.sort(key=lambda r: (_get_organism_priority(r.organism), r.name or ''))

    # Get organism counts
    organism_counts: Optional[dict[str, int]] = None
    if category in ["genes", "descriptions"]:
        org_buckets = response.get("aggregations", {}).get("by_organism", {}).get("buckets", [])
        if org_buckets:
            organism_counts = {
                bucket["key"]: bucket["doc_count"]
                for bucket in org_buckets
                if bucket["key"]
            }

    return TextSearchCategoryPagedResponse(
        query=query,
        category=category,
        results=results,
        total_count=len(results),
        organism_counts=organism_counts,
    )


def get_es_supported_categories() -> set[str]:
    """Return the set of categories supported by ES for text search."""
    return set(TEXT_CATEGORY_TO_ES_TYPE.keys())
