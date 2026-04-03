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


def _build_wildcard_query_for_match_mode(
    field: str,
    query: str,
    match_mode: str = "exact",
    case_insensitive: bool = True,
) -> dict:
    """
    Build wildcard query based on match_mode.

    Args:
        field: The field to search (should be a .keyword field for exact matching)
        query: The search query
        match_mode: One of "exact", "all", "any"
            - exact: Search for the exact phrase (default, current behavior)
            - all: All words must appear (AND logic)
            - any: Any word can appear (OR logic)
        case_insensitive: Whether to do case-insensitive matching

    Returns:
        ES query dict (bool clause or single wildcard)
    """
    query_lower = query.lower()

    if match_mode == "exact":
        # Current behavior: search for exact phrase
        return {"wildcard": {field: {"value": f"*{query_lower}*", "case_insensitive": case_insensitive}}}

    # Split query into words for all/any mode
    words = query_lower.split()
    if len(words) <= 1:
        # Single word, same as exact
        return {"wildcard": {field: {"value": f"*{query_lower}*", "case_insensitive": case_insensitive}}}

    # Build wildcard for each word
    word_queries = [
        {"wildcard": {field: {"value": f"*{word}*", "case_insensitive": case_insensitive}}}
        for word in words
    ]

    if match_mode == "all":
        # All words must match (AND)
        return {"bool": {"must": word_queries}}
    else:  # match_mode == "any"
        # Any word can match (OR)
        return {"bool": {"should": word_queries, "minimum_should_match": 1}}


def _extract_highlight(highlights: dict, field: str, fallback: Optional[str]) -> Optional[str]:
    """Extract highlighted text from ES response, or return fallback with manual highlighting."""
    if highlights and field in highlights:
        # ES returns list of highlighted fragments
        return highlights[field][0]
    return fallback


def _build_autocomplete_query(query: str, size: int = 50) -> dict:
    """
    Build Elasticsearch query optimized for autocomplete.

    Prioritizes prefix matching for fast suggestions.
    Uses case-insensitive wildcard for genes to match regardless of case.
    """
    query_upper = query.upper()
    query_lower = query.lower()

    return {
        "query": {
            "bool": {
                "should": [
                    # Exact match (highest priority) - genes
                    {"term": {"gene_name.keyword": {"value": query_upper, "boost": 30}}},
                    {"term": {"feature_name": {"value": query_upper, "boost": 30}}},
                    # Case-insensitive wildcard for gene prefix (high priority)
                    {"wildcard": {"gene_name.keyword": {"value": f"{query_lower}*", "case_insensitive": True, "boost": 25}}},
                    {"wildcard": {"feature_name": {"value": f"{query_upper}*", "boost": 20}}},
                    # Prefix match for GO terms and phenotypes
                    {"prefix": {"go_term.keyword": {"value": query_lower, "boost": 8}}},
                    {"prefix": {"observable.keyword": {"value": query_lower, "boost": 5}}},
                    # Fallback to contains match (only on text fields, not keyword)
                    {
                        "multi_match": {
                            "query": query,
                            "fields": ["gene_name^3", "go_term^2", "observable^2"],
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


def _parse_ortholog_result(hit: dict, query: str) -> SearchResult:
    """Parse ES hit into SearchResult for ortholog type."""
    source = hit["_source"]
    highlights = hit.get("highlight", {})

    # CGD gene info (the gene this ortholog maps to)
    cgd_gene_name = source.get("cgd_gene_name") or source.get("gene_name") or source.get("name")
    cgd_feature_name = source.get("cgd_feature_name") or source.get("feature_name")
    cgd_gene_id = source.get("cgd_gene_id") or source.get("id")

    # Ortholog info (the related gene from another organism)
    ortholog_display = source.get("ortholog_display")
    ortholog_name = source.get("ortholog_name")
    ortholog_organism = source.get("ortholog_organism")
    ortholog_type = source.get("ortholog_type", "Ortholog")
    ortholog_source = source.get("ortholog_source")

    # Build description for display
    if ortholog_display:
        description = f"{ortholog_display} ({ortholog_type})"
    elif ortholog_name:
        description = f"{ortholog_source}: {ortholog_name}"
    else:
        description = None

    highlighted_name = _extract_highlight(highlights, "cgd_gene_name", None)
    if not highlighted_name:
        highlighted_name = _extract_highlight(highlights, "gene_name", None)
    if not highlighted_name:
        highlighted_name = _highlight_text(cgd_gene_name, query)

    return SearchResult(
        category="orthologs",
        id=cgd_gene_id or "",
        name=cgd_gene_name or "",
        description=description,
        link=source.get("link") or f"/locus/{cgd_feature_name or cgd_gene_name}",
        organism=source.get("organism"),  # Organism of the CGD gene
        highlighted_name=highlighted_name,
        highlighted_description=_highlight_text(ortholog_display, query) if ortholog_display else None,
        # New ortholog relationship fields
        ortholog_display=ortholog_display,
        ortholog_organism=ortholog_organism,
        ortholog_type=ortholog_type,
        cgd_gene_name=cgd_gene_name,
        cgd_gene_id=cgd_gene_id,
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
    elif doc_type == "ortholog":
        return _parse_ortholog_result(hit, query)
    else:
        logger.warning(f"Unknown document type: {doc_type}")
        return None


def _build_quick_search_type_query(query: str, doc_type: str, size: int = 20) -> dict:
    """
    Build ES query for quick search filtered to a specific document type.

    Uses the same restrictive matching as the main quick search query.
    """
    query_upper = query.upper()
    query_lower = query.lower()

    # Build type-specific should clauses
    should_clauses = []

    if doc_type == "gene":
        should_clauses = [
            {"term": {"gene_name.keyword": {"value": query_upper, "boost": 15}}},
            {"term": {"feature_name": {"value": query_upper, "boost": 15}}},
            {"prefix": {"gene_name.keyword": {"value": query_upper, "boost": 10}}},
            {"prefix": {"feature_name": {"value": query_upper, "boost": 10}}},
            {"match": {"aliases": {"query": query, "boost": 8}}},
            {"term": {"dbxref_id": {"value": query_upper, "boost": 20}}},
        ]
    elif doc_type == "go_term":
        # Search go_term only
        should_clauses = [
            {"term": {"goid": {"value": query_upper, "boost": 20}}},
            {"prefix": {"go_term.keyword": {"value": query_lower, "boost": 8}}},
            {"match": {"go_term": {"query": query, "boost": 5}}},
        ]
    elif doc_type == "phenotype":
        should_clauses = [
            {"prefix": {"observable.keyword": {"value": query_lower, "boost": 8}}},
            {"match": {"observable": {"query": query, "boost": 5}}},
        ]
    elif doc_type == "reference":
        # Search by PubMed ID (numeric) or title
        if query.isdigit():
            should_clauses = [
                {"term": {"pubmed": {"value": int(query), "boost": 20}}},
            ]
        else:
            # For non-numeric queries, search title
            should_clauses = [
                {"match": {"title": {"query": query, "boost": 5}}},
            ]
    elif doc_type == "ortholog":
        should_clauses = [
            {"term": {"ortholog_name.keyword": {"value": query_upper, "boost": 10}}},
            {"prefix": {"ortholog_name.keyword": {"value": query_upper, "boost": 8}}},
            {"match": {"ortholog_name": {"query": query, "boost": 5}}},
        ]

    if not should_clauses:
        # Fallback to generic name match
        should_clauses = [{"match": {"name": {"query": query}}}]

    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"type": doc_type}},
                ],
                "should": should_clauses,
                "minimum_should_match": 1,
            }
        },
        "size": size,
        "highlight": {
            "fields": {
                "gene_name": {},
                "feature_name": {},
                "aliases": {},
                "go_term": {},
                                "observable": {},
                "ortholog_name": {},
                "title": {},
            },
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"],
        },
    }


def _build_quick_search_counts_query(query: str) -> dict:
    """
    Build ES query to get counts per type for quick search.

    Uses the same restrictive matching as the main quick search query.
    """
    query_upper = query.upper()
    query_lower = query.lower()

    # Build should clauses for all searchable fields
    should_clauses = [
        # Gene fields
        {"term": {"dbxref_id": {"value": query_upper, "boost": 20}}},
        {"term": {"gene_name.keyword": {"value": query_upper, "boost": 15}}},
        {"term": {"feature_name": {"value": query_upper, "boost": 15}}},
        {"prefix": {"gene_name.keyword": {"value": query_upper, "boost": 10}}},
        {"prefix": {"feature_name": {"value": query_upper, "boost": 10}}},
        {"match": {"aliases": {"query": query, "boost": 8}}},

        # GO term fields - search go_term only
        {"term": {"goid": {"value": query_upper, "boost": 20}}},
        {"prefix": {"go_term.keyword": {"value": query_lower, "boost": 8}}},
        {"match": {"go_term": {"query": query, "boost": 5}}},

        # Phenotype fields
        {"prefix": {"observable.keyword": {"value": query_lower, "boost": 8}}},
        {"match": {"observable": {"query": query, "boost": 5}}},

        # Ortholog fields
        {"term": {"ortholog_name.keyword": {"value": query_upper, "boost": 10}}},
        {"prefix": {"ortholog_name.keyword": {"value": query_upper, "boost": 8}}},
        {"match": {"ortholog_name": {"query": query, "boost": 5}}},

        # Reference fields - search title
        {"match": {"title": {"query": query, "boost": 3}}},

        # External ID
        {"term": {"external_id": {"value": query_upper, "boost": 15}}},
        {"term": {"external_id": {"value": query_lower, "boost": 15}}},
    ]

    # Add PubMed ID search if query is numeric
    if query.isdigit():
        should_clauses.append({"term": {"pubmed": {"value": int(query), "boost": 20}}})

    return {
        "query": {
            "bool": {
                "should": should_clauses,
                "minimum_should_match": 1,
            }
        },
        "size": 0,
        "aggs": {
            "by_type": {
                "terms": {"field": "type", "size": 20}
            }
        },
    }


def quick_search(
    es: Elasticsearch,
    query: str,
    limit: int = 20,
) -> SearchResponse:
    """
    Quick search across all categories using Elasticsearch.

    Returns results grouped by category with counts.
    Uses the same query logic as text search (via search_category) for consistent counts.
    """
    # Categories to search in quick search
    quick_search_categories = ["genes", "go_terms", "phenotypes", "references", "orthologs"]

    try:
        results_by_category: dict[str, list[SearchResult]] = {}
        counts_by_category: dict[str, int] = {}

        for category in quick_search_categories:
            # Use search_category which uses the same query logic as text search
            cat_response = search_category(es, query, category)

            if cat_response and cat_response.total_count > 0:
                counts_by_category[category] = cat_response.total_count
                # Limit results for quick search
                results_by_category[category] = cat_response.results[:limit]

        total = sum(counts_by_category.values())

        return SearchResponse(
            query=query,
            total_results=total,
            results_by_category=results_by_category,
            counts_by_category=counts_by_category,
        )

    except Exception as e:
        logger.error(f"Elasticsearch quick search failed: {e}")
        # Return empty response on error
        return SearchResponse(
            query=query,
            total_results=0,
            results_by_category={},
            counts_by_category={},
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
    "orthologs": "ortholog",
}

# Categories supported by ES (others fall back to Oracle)
ES_SUPPORTED_CATEGORIES = set(CATEGORY_TO_ES_TYPE.keys())

# Text search category mapping - ALL categories now supported by ES
TEXT_CATEGORY_TO_ES_TYPE = {
    "genes": "gene",
    "descriptions": "gene",  # Uses headline/name_description field
    "go_terms": "go_term",
    "phenotypes": "phenotype",
    "abstracts": "reference",  # Uses abstract field
    "paper_titles": "reference",  # Uses title field
    "paragraphs": "paragraph",
    "authors": "author",
    "colleagues": "colleague",
    "pathways": "pathway",
    "notes": "note",
    "external_ids": "external_id",
    "orthologs": "ortholog",
    "literature_topics": "literature_topic",
    "name_descriptions": "gene",  # Uses name_description field
}

TEXT_CATEGORY_DISPLAY_NAMES = {
    "genes": "Gene names",
    "descriptions": "General Descriptions",
    "go_terms": "Gene ontology",
    "phenotypes": "Phenotypes",
    "abstracts": "Paper Abstracts",
    "paper_titles": "Paper Titles",
    "paragraphs": "Paragraphs",
    "authors": "Authors",
    "colleagues": "Colleagues",
    "pathways": "Biochemical pathways",
    "notes": "Notes",
    "external_ids": "External IDs",
    "orthologs": "Orthologs/Best Hits",
    "literature_topics": "Literature Topics",
    "name_descriptions": "Name Descriptions",
}


def _build_category_query(query: str, es_type: str, size: int = 1000) -> dict:
    """Build ES query for a specific category/type."""
    # Define fields to search based on type
    fields_by_type = {
        "gene": ["gene_name^3", "feature_name^2", "aliases^2", "headline", "name_description", "dbxref_id"],
        # Search go_term only
        "go_term": ["go_term^3", "goid^2"],
        "phenotype": ["observable^3"],
        # Search title only (pubmed handled separately for numeric queries)
        "reference": ["title^2"],
        "paragraph": ["paragraph_text^3", "gene_name^2", "feature_name"],
        "author": ["author_name^3", "citation"],
        "colleague": ["last_name^3", "other_last_name^2", "first_name", "institution"],
        "pathway": ["pathway_name^3", "pathway_id", "related_genes"],
        "note": ["note_text^3", "gene_name", "feature_name"],
        "external_id": ["external_id^3", "source", "description", "gene_name"],
        "ortholog": ["gene_name^3", "feature_name^2", "ortholog_name^3", "related_genes", "external_id"],
        "literature_topic": ["literature_topic^3", "citation"],
    }

    fields = fields_by_type.get(es_type, ["name"])

    # For reference type, use wildcard to match Oracle's LIKE behavior
    if es_type == "reference":
        if query.isdigit():
            # Numeric query: search pubmed
            return {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": es_type}},
                        ],
                        "should": [
                            {"wildcard": {"title.keyword": {"value": f"*{query.lower()}*", "case_insensitive": True}}},
                            {"term": {"pubmed": {"value": int(query), "boost": 10}}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
                "size": size,
                "highlight": {
                    "fields": {"title": {}},
                    "pre_tags": ["<mark>"],
                    "post_tags": ["</mark>"],
                },
            }
        else:
            # Non-numeric query: use wildcard on title
            return {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": es_type}},
                            {"wildcard": {"title.keyword": {"value": f"*{query.lower()}*", "case_insensitive": True}}},
                        ]
                    }
                },
                "size": size,
                "highlight": {
                    "fields": {"title": {}},
                    "pre_tags": ["<mark>"],
                    "post_tags": ["</mark>"],
                },
            }

    # For go_term type, use wildcard to match Oracle's LIKE behavior
    if es_type == "go_term":
        # Check if query looks like a GO ID
        query_upper = query.upper()
        if query_upper.startswith("GO:") or query.isdigit():
            # Search by goid
            goid_value = query_upper.replace("GO:", "") if query_upper.startswith("GO:") else query
            return {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": es_type}},
                        ],
                        "should": [
                            {"wildcard": {"go_term.keyword": {"value": f"*{query.lower()}*", "case_insensitive": True}}},
                            {"wildcard": {"goid": {"value": f"*{goid_value}*"}}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
                "size": size,
                "highlight": {
                    "fields": {"go_term": {}, "goid": {}},
                    "pre_tags": ["<mark>"],
                    "post_tags": ["</mark>"],
                },
            }
        else:
            # Non-GO ID query: use wildcard on go_term only
            return {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": es_type}},
                            {"wildcard": {"go_term.keyword": {"value": f"*{query.lower()}*", "case_insensitive": True}}},
                        ]
                    }
                },
                "size": size,
                "highlight": {
                    "fields": {"go_term": {}},
                    "pre_tags": ["<mark>"],
                    "post_tags": ["</mark>"],
                },
            }

    # For phenotype type, use wildcard to match Oracle's LIKE behavior
    if es_type == "phenotype":
        return {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": es_type}},
                        {"wildcard": {"observable.keyword": {"value": f"*{query.lower()}*", "case_insensitive": True}}},
                    ]
                }
            },
            "size": size,
            "highlight": {
                "fields": {"observable": {}},
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
            },
        }

    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"type": es_type}},
                    # Full-text search - same logic as text search counts query
                    {
                        "multi_match": {
                            "query": query,
                            "fields": fields,
                            "type": "best_fields",
                            "fuzziness": "AUTO",
                        }
                    },
                ],
            }
        },
        "size": size,
        "highlight": {
            "fields": {
                "name": {},
                "gene_name": {},
                "headline": {},
                "go_term": {},
                                "observable": {},
                "title": {},
                "aliases": {},
                "paragraph_text": {},
                "author_name": {},
                "last_name": {},
                "pathway_name": {},
                "note_text": {},
                "external_id": {},
                "ortholog_name": {},
                "related_genes": {},
                "literature_topic": {},
                "name_description": {},
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


def _build_restrictive_gene_query(query: str, es_type: str, size: int = 1000) -> dict:
    """
    Build restrictive ES query for genes/orthologs category search.

    For genes: matches gene names, aliases, feature names.
    For orthologs: matches ortholog names (the ortholog that points to a CGD gene).
    """
    query_upper = query.upper()

    if es_type == "ortholog":
        # For orthologs, search by the ORTHOLOG name (not CGD gene name)
        # This finds: "CGD genes that have an ortholog matching the query"
        return {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "ortholog"}},
                    ],
                    "should": [
                        # Exact ortholog name match
                        {"term": {"ortholog_name.keyword": {"value": query_upper, "boost": 15}}},
                        # Prefix match
                        {"prefix": {"ortholog_name.keyword": {"value": query_upper, "boost": 10}}},
                        # Text match
                        {"match": {"ortholog_name": {"query": query, "boost": 8}}},
                    ],
                    "minimum_should_match": 1,
                }
            },
            "size": size,
            "highlight": {
                "fields": {
                    "ortholog_name": {},
                    "ortholog_display": {},
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
    else:
        # For genes, search by gene name
        return {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": es_type}},
                    ],
                    "should": [
                        # Exact gene name match
                        {"term": {"gene_name.keyword": {"value": query_upper, "boost": 15}}},
                        {"term": {"feature_name": {"value": query_upper, "boost": 15}}},
                        # Prefix match on gene names
                        {"prefix": {"gene_name.keyword": {"value": query_upper, "boost": 10}}},
                        {"prefix": {"feature_name": {"value": query_upper, "boost": 10}}},
                        # Search in aliases
                        {"match": {"aliases": {"query": query, "boost": 8}}},
                    ],
                    "minimum_should_match": 1,
                }
            },
            "size": size,
            "highlight": {
                "fields": {
                    "gene_name": {},
                    "feature_name": {},
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

    # Use restrictive query for genes and orthologs (match quick search behavior)
    if category in ("genes", "orthologs"):
        es_query = _build_restrictive_gene_query(query, es_type)
    else:
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

    # Sort genes and orthologs by organism priority
    if category in ("genes", "orthologs"):
        results.sort(key=lambda r: (_get_organism_priority(r.organism), r.name or ''))

    # Get organism counts from aggregations
    organism_counts: Optional[dict[str, int]] = None
    if category in ("genes", "orthologs"):
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
        abstract = source.get("abstract")
        title = source.get("title")

        name = f"PMID:{pubmed}" if pubmed else dbxref_id or ""

        # Use appropriate field based on category
        if category == "abstracts" and abstract:
            match_context = _extract_highlight(highlights, "abstract", None)
            if not match_context:
                match_context = _highlight_text(abstract[:300] + "..." if len(abstract) > 300 else abstract, query)
        elif category == "paper_titles" and title:
            match_context = _extract_highlight(highlights, "title", None)
            if not match_context:
                match_context = _highlight_text(title, query)
        else:
            match_context = _extract_highlight(highlights, "citation", None)
            if not match_context and citation:
                match_context = _highlight_text(citation, query)

        # Use the category passed in (paper_titles or abstracts)
        result_category = category if category in ("paper_titles", "abstracts") else "abstracts"

        return TextSearchResult(
            category=result_category,
            id=dbxref_id or "",
            name=name,
            description=citation,
            link=source.get("link") or f"/reference/{dbxref_id}",
            organism=None,
            match_context=match_context,
            highlighted_name=_highlight_text(name, query),
            highlighted_description=match_context,
        )

    elif doc_type == "paragraph":
        display_name = source.get("gene_name") or source.get("feature_name") or source.get("name")
        paragraph_text = source.get("paragraph_text", "")

        highlighted_name = _highlight_text(display_name, query)
        match_context = _extract_highlight(highlights, "paragraph_text", None)
        if not match_context and paragraph_text:
            match_context = _highlight_text(paragraph_text[:300] + "..." if len(paragraph_text) > 300 else paragraph_text, query)

        return TextSearchResult(
            category="paragraphs",
            id=source.get("id", ""),
            name=display_name or "",
            description=paragraph_text[:200] + "..." if len(paragraph_text) > 200 else paragraph_text,
            link=source.get("link"),
            organism=source.get("organism"),
            match_context=match_context,
            highlighted_name=highlighted_name,
        )

    elif doc_type == "author":
        author_name = source.get("author_name") or source.get("name")
        citation = source.get("citation")

        highlighted_name = _extract_highlight(highlights, "author_name", None)
        if not highlighted_name:
            highlighted_name = _highlight_text(author_name, query)

        return TextSearchResult(
            category="authors",
            id=source.get("id", ""),
            name=author_name or "",
            description=citation,
            link=source.get("link"),
            organism=None,
            highlighted_name=highlighted_name,
            highlighted_description=_highlight_text(citation, query) if citation else None,
        )

    elif doc_type == "colleague":
        full_name = source.get("name")
        description = source.get("description")

        highlighted_name = _extract_highlight(highlights, "last_name", None)
        if not highlighted_name:
            highlighted_name = _highlight_text(full_name, query)

        return TextSearchResult(
            category="colleagues",
            id=source.get("id", ""),
            name=full_name or "",
            description=description,
            link=source.get("link"),
            organism=None,
            highlighted_name=highlighted_name,
            highlighted_description=_highlight_text(description, query) if description else None,
        )

    elif doc_type == "pathway":
        pathway_name = source.get("pathway_name") or source.get("name")
        pathway_id = source.get("pathway_id")

        highlighted_name = _extract_highlight(highlights, "pathway_name", None)
        if not highlighted_name:
            highlighted_name = _highlight_text(pathway_name, query)

        return TextSearchResult(
            category="pathways",
            id=pathway_id or source.get("id", ""),
            name=pathway_name or "",
            description=None,
            link=source.get("link"),
            organism=None,
            highlighted_name=highlighted_name,
        )

    elif doc_type == "note":
        display_name = source.get("gene_name") or source.get("feature_name") or source.get("name")
        note_text = source.get("note_text", "")

        highlighted_name = _highlight_text(display_name, query)
        match_context = _extract_highlight(highlights, "note_text", None)
        if not match_context and note_text:
            match_context = _highlight_text(note_text[:300] + "..." if len(note_text) > 300 else note_text, query)

        return TextSearchResult(
            category="notes",
            id=source.get("id", ""),
            name=display_name or "",
            description=note_text[:200] + "..." if len(note_text) > 200 else note_text,
            link=source.get("link"),
            organism=source.get("organism"),
            match_context=match_context,
            highlighted_name=highlighted_name,
        )

    elif doc_type == "external_id":
        display_name = source.get("gene_name") or source.get("feature_name") or source.get("name")
        external_id = source.get("external_id")
        ext_source = source.get("source")

        description = f"{ext_source}: {external_id}" if ext_source else external_id

        highlighted_name = _highlight_text(display_name, query)

        return TextSearchResult(
            category="external_ids",
            id=external_id or source.get("id", ""),
            name=display_name or "",
            description=description,
            link=source.get("link"),
            organism=source.get("organism"),
            highlighted_name=highlighted_name,
            highlighted_description=_highlight_text(description, query),
        )

    elif doc_type == "ortholog":
        display_name = source.get("gene_name") or source.get("feature_name") or source.get("name")
        ortholog_name = source.get("ortholog_name")
        ortholog_source = source.get("ortholog_source")

        description = f"{ortholog_source}: {ortholog_name}" if ortholog_name and ortholog_source else None

        highlighted_name = _highlight_text(display_name, query)

        return TextSearchResult(
            category="orthologs",
            id=source.get("id", ""),
            name=display_name or "",
            description=description,
            link=source.get("link"),
            organism=source.get("organism"),
            homology_group_no=source.get("homology_group_no"),
            highlighted_name=highlighted_name,
            highlighted_description=_highlight_text(description, query) if description else None,
        )

    elif doc_type == "literature_topic":
        topic = source.get("literature_topic")
        citation = source.get("citation")
        pubmed = source.get("pubmed")

        name = f"PMID:{pubmed}" if pubmed else source.get("name", "")

        highlighted_name = _extract_highlight(highlights, "literature_topic", None)
        if not highlighted_name:
            highlighted_name = _highlight_text(topic, query)

        return TextSearchResult(
            category="literature_topics",
            id=source.get("id", ""),
            name=name,
            description=f"{topic}: {citation}" if citation else topic,
            link=source.get("link"),
            organism=None,
            highlighted_name=highlighted_name,
            highlighted_description=_highlight_text(citation, query) if citation else None,
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


def _get_text_search_fields() -> list[str]:
    """Get the list of fields to search for text search.

    Search fields:
    - GO terms: go_term + goid
    - References: title (+ pubmed for numeric queries)
    """
    return [
        "name^3",
        "gene_name^3",
        "feature_name^2",
        "aliases^2",
        "headline^2",
        "name_description",
        "go_term^3",
        "goid^2",
        "observable^3",
        "title",
        "paragraph_text",
        "author_name^2",
        "last_name^2",
        "other_last_name",
        "pathway_name",
        "note_text",
        "external_id",
        "ortholog_name",
        "related_genes",
        "literature_topic",
    ]


def _build_text_search_counts_query(query: str) -> dict:
    """Build ES query to get counts per type for text search."""
    # For numeric queries, also search pubmed field for references
    if query.isdigit():
        return {
            "query": {
                "bool": {
                    "should": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": _get_text_search_fields(),
                                "type": "best_fields",
                                "fuzziness": "AUTO",
                            }
                        },
                        {"term": {"pubmed": {"value": int(query), "boost": 10}}},
                    ],
                    "minimum_should_match": 1,
                }
            },
            "size": 0,
            "aggs": {
                "by_type": {
                    "terms": {"field": "type", "size": 20}
                }
            },
        }

    return {
        "query": {
            "multi_match": {
                "query": query,
                "fields": _get_text_search_fields(),
                "type": "best_fields",
                "fuzziness": "AUTO",
            }
        },
        "size": 0,
        "aggs": {
            "by_type": {
                "terms": {"field": "type", "size": 20}
            }
        },
    }


def _build_text_search_type_query(query: str, doc_type: str, size: int = 10) -> dict:
    """Build ES query for text search filtered to a specific type."""
    # For reference type with numeric query, also search pubmed
    if doc_type == "reference" and query.isdigit():
        return {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": doc_type}},
                    ],
                    "should": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": _get_text_search_fields(),
                                "type": "best_fields",
                                "fuzziness": "AUTO",
                            }
                        },
                        {"term": {"pubmed": {"value": int(query), "boost": 10}}},
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
                                    "observable": {},
                    "title": {},
                    "aliases": {},
                    "paragraph_text": {},
                    "author_name": {},
                    "last_name": {},
                    "pathway_name": {},
                    "note_text": {},
                    "external_id": {},
                    "ortholog_name": {},
                    "literature_topic": {},
                },
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
            },
        }

    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"type": doc_type}},
                    {
                        "multi_match": {
                            "query": query,
                            "fields": _get_text_search_fields(),
                            "type": "best_fields",
                            "fuzziness": "AUTO",
                        }
                    },
                ]
            }
        },
        "size": size,
        "highlight": {
            "fields": {
                "name": {},
                "gene_name": {},
                "headline": {},
                "go_term": {},
                                "observable": {},
                "title": {},
                "aliases": {},
                "paragraph_text": {},
                "author_name": {},
                "last_name": {},
                "pathway_name": {},
                "note_text": {},
                "external_id": {},
                "ortholog_name": {},
                "literature_topic": {},
            },
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"],
        },
    }


def text_search(
    es: Elasticsearch,
    query: str,
    limit: int = 10,
    match_mode: str = "all",
) -> Optional[TextSearchResponse]:
    """
    Text search across all ES-indexed categories.

    Args:
        es: Elasticsearch client
        query: Search query string
        limit: Max results per category
        match_mode: One of "exact", "all", "any"
            - exact: Search for the exact phrase
            - all: All words must appear (AND logic) - default
            - any: Any word can appear (OR logic)

    Returns results for all categories now that ES indexes everything.
    Returns None on error (caller should fall back to Oracle).
    """
    # Mapping from ES doc type to category name
    type_to_category = {
        "gene": "genes",
        "go_term": "go_terms",
        "phenotype": "phenotypes",
        "reference": "abstracts",  # References show as "abstracts" category
        "paragraph": "paragraphs",
        "author": "authors",
        "colleague": "colleagues",
        "pathway": "pathways",
        "note": "notes",
        "external_id": "external_ids",
        "ortholog": "orthologs",
        "literature_topic": "literature_topics",
    }

    try:
        # Step 1: Get counts per type using aggregation
        counts_query = _build_text_search_counts_query(query)
        counts_response = es.search(index=INDEX_NAME, body=counts_query)

        type_counts = {}
        for bucket in counts_response.get("aggregations", {}).get("by_type", {}).get("buckets", []):
            type_counts[bucket["key"]] = bucket["doc_count"]

        # Step 2: Query each type that has results to get sample results
        results_by_category: dict[str, list[TextSearchResult]] = {}
        counts_by_category: dict[str, int] = {}

        for doc_type, count in type_counts.items():
            if count == 0:
                continue

            category = type_to_category.get(doc_type)
            if not category:
                continue

            # Store the actual count
            counts_by_category[category] = count

            # Fetch sample results for this type
            type_query = _build_text_search_type_query(query, doc_type, limit)
            type_response = es.search(index=INDEX_NAME, body=type_query)

            results = []
            for hit in type_response["hits"]["hits"]:
                result = _parse_text_search_result(hit, query, category)
                results.append(result)

            if results:
                results_by_category[category] = results

        # Step 3: Handle special categories derived from genes
        # descriptions: genes with headline containing query
        # name_descriptions: genes with name_description containing query
        if "gene" in type_counts and type_counts["gene"] > 0:
            # Query for descriptions (headline matches)
            desc_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": "gene"}},
                            {"match": {"headline": query}},
                        ]
                    }
                },
                "size": limit,
                "highlight": {
                    "fields": {"headline": {}},
                    "pre_tags": ["<mark>"],
                    "post_tags": ["</mark>"],
                },
            }
            desc_response = es.search(index=INDEX_NAME, body=desc_query)
            desc_count = desc_response["hits"]["total"]["value"]
            if desc_count > 0:
                counts_by_category["descriptions"] = desc_count
                desc_results = []
                for hit in desc_response["hits"]["hits"]:
                    result = _parse_text_search_result(hit, query, "descriptions")
                    desc_results.append(result)
                if desc_results:
                    results_by_category["descriptions"] = desc_results

            # Query for name_descriptions
            nd_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": "gene"}},
                            {"match": {"name_description": query}},
                        ]
                    }
                },
                "size": limit,
                "highlight": {
                    "fields": {"name_description": {}},
                    "pre_tags": ["<mark>"],
                    "post_tags": ["</mark>"],
                },
            }
            nd_response = es.search(index=INDEX_NAME, body=nd_query)
            nd_count = nd_response["hits"]["total"]["value"]
            if nd_count > 0:
                counts_by_category["name_descriptions"] = nd_count
                nd_results = []
                for hit in nd_response["hits"]["hits"]:
                    result = _parse_text_search_result(hit, query, "name_descriptions")
                    nd_results.append(result)
                if nd_results:
                    results_by_category["name_descriptions"] = nd_results

        # Step 4: Handle paper_titles category (references with title matching)
        # This is separate from abstracts which searches abstract field
        # Use wildcard to match Oracle's LIKE behavior (finds substring matches)
        if "reference" in type_counts and type_counts["reference"] > 0:
            pt_wildcard = _build_wildcard_query_for_match_mode("title.keyword", query, match_mode)
            pt_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": "reference"}},
                            pt_wildcard,
                        ]
                    }
                },
                "size": limit,
                "highlight": {
                    "fields": {"title": {}},
                    "pre_tags": ["<mark>"],
                    "post_tags": ["</mark>"],
                },
            }
            pt_response = es.search(index=INDEX_NAME, body=pt_query)
            pt_count = pt_response["hits"]["total"]["value"]
            if pt_count > 0:
                counts_by_category["paper_titles"] = pt_count
                pt_results = []
                for hit in pt_response["hits"]["hits"]:
                    result = _parse_text_search_result(hit, query, "paper_titles")
                    pt_results.append(result)
                if pt_results:
                    results_by_category["paper_titles"] = pt_results

        # Step 5: Override GO terms count with wildcard query to match Oracle LIKE behavior
        # The aggregation uses fuzziness which returns more results than Oracle
        # NOTE: Oracle doesn't apply match_mode to GO terms, so always use "exact" phrase matching
        if "go_term" in type_counts and type_counts["go_term"] > 0:
            go_wildcard = _build_wildcard_query_for_match_mode("go_term.keyword", query, "exact")
            go_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": "go_term"}},
                            go_wildcard,
                        ]
                    }
                },
                "size": limit,
                "highlight": {
                    "fields": {"go_term": {}},
                    "pre_tags": ["<mark>"],
                    "post_tags": ["</mark>"],
                },
            }
            go_response = es.search(index=INDEX_NAME, body=go_query)
            go_count = go_response["hits"]["total"]["value"]
            # Override the fuzzy count with the exact wildcard count
            counts_by_category["go_terms"] = go_count
            if go_count > 0:
                go_results = []
                for hit in go_response["hits"]["hits"]:
                    result = _parse_text_search_result(hit, query, "go_terms")
                    go_results.append(result)
                if go_results:
                    results_by_category["go_terms"] = go_results

        # Step 6: Override phenotypes count with wildcard query to match Oracle LIKE behavior
        # NOTE: Oracle doesn't apply match_mode to phenotypes, so always use "exact" phrase matching
        if "phenotype" in type_counts and type_counts["phenotype"] > 0:
            ph_wildcard = _build_wildcard_query_for_match_mode("observable.keyword", query, "exact")
            ph_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": "phenotype"}},
                            ph_wildcard,
                        ]
                    }
                },
                "size": limit,
                "highlight": {
                    "fields": {"observable": {}},
                    "pre_tags": ["<mark>"],
                    "post_tags": ["</mark>"],
                },
            }
            ph_response = es.search(index=INDEX_NAME, body=ph_query)
            ph_count = ph_response["hits"]["total"]["value"]
            # Override the fuzzy count with the exact wildcard count
            counts_by_category["phenotypes"] = ph_count
            if ph_count > 0:
                ph_results = []
                for hit in ph_response["hits"]["hits"]:
                    result = _parse_text_search_result(hit, query, "phenotypes")
                    ph_results.append(result)
                if ph_results:
                    results_by_category["phenotypes"] = ph_results
            else:
                # Remove phenotypes from results if count is 0
                results_by_category.pop("phenotypes", None)

        # Step 7: Override genes count with wildcard query to match Oracle LIKE behavior
        # Oracle searches gene_name, feature_name, dbxref_id - ES fuzzy match is too broad
        # NOTE: Oracle doesn't apply match_mode to genes, so always use "exact" phrase matching
        if "gene" in type_counts and type_counts["gene"] > 0:
            # For genes, we search multiple fields - build wildcard for each field
            gene_name_wc = _build_wildcard_query_for_match_mode("gene_name.keyword", query, "exact")
            feature_name_wc = _build_wildcard_query_for_match_mode("feature_name", query, "exact", case_insensitive=False)
            dbxref_wc = _build_wildcard_query_for_match_mode("dbxref_id", query, "exact", case_insensitive=False)
            gene_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": "gene"}},
                        ],
                        "should": [
                            gene_name_wc,
                            feature_name_wc,
                            dbxref_wc,
                        ],
                        "minimum_should_match": 1,
                    }
                },
                "size": limit,
                "highlight": {
                    "fields": {"gene_name": {}, "feature_name": {}},
                    "pre_tags": ["<mark>"],
                    "post_tags": ["</mark>"],
                },
            }
            gene_response = es.search(index=INDEX_NAME, body=gene_query)
            gene_count = gene_response["hits"]["total"]["value"]
            # Override the fuzzy count with the exact wildcard count
            counts_by_category["genes"] = gene_count
            if gene_count > 0:
                gene_results = []
                for hit in gene_response["hits"]["hits"]:
                    result = _parse_text_search_result(hit, query, "genes")
                    gene_results.append(result)
                if gene_results:
                    results_by_category["genes"] = gene_results
            else:
                # Remove genes from results if count is 0
                results_by_category.pop("genes", None)

        # Step 8: Override descriptions count with wildcard query
        if "gene" in type_counts:
            desc_wildcard = _build_wildcard_query_for_match_mode("headline.keyword", query, match_mode)
            desc_wc_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": "gene"}},
                            desc_wildcard,
                        ]
                    }
                },
                "size": limit,
            }
            desc_wc_response = es.search(index=INDEX_NAME, body=desc_wc_query)
            desc_wc_count = desc_wc_response["hits"]["total"]["value"]
            counts_by_category["descriptions"] = desc_wc_count
            if desc_wc_count > 0:
                desc_wc_results = []
                for hit in desc_wc_response["hits"]["hits"]:
                    result = _parse_text_search_result(hit, query, "descriptions")
                    desc_wc_results.append(result)
                if desc_wc_results:
                    results_by_category["descriptions"] = desc_wc_results
            else:
                results_by_category.pop("descriptions", None)

        # Step 9: Override name_descriptions count with wildcard query
        # NOTE: Oracle doesn't apply match_mode to name_descriptions, so always use "exact" phrase matching
        if "gene" in type_counts:
            nd_wildcard = _build_wildcard_query_for_match_mode("name_description.keyword", query, "exact")
            nd_wc_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": "gene"}},
                            nd_wildcard,
                        ]
                    }
                },
                "size": limit,
            }
            nd_wc_response = es.search(index=INDEX_NAME, body=nd_wc_query)
            nd_wc_count = nd_wc_response["hits"]["total"]["value"]
            counts_by_category["name_descriptions"] = nd_wc_count
            if nd_wc_count > 0:
                nd_wc_results = []
                for hit in nd_wc_response["hits"]["hits"]:
                    result = _parse_text_search_result(hit, query, "name_descriptions")
                    nd_wc_results.append(result)
                if nd_wc_results:
                    results_by_category["name_descriptions"] = nd_wc_results
            else:
                results_by_category.pop("name_descriptions", None)

        # Step 10: Override paragraphs count with wildcard query
        # NOTE: Oracle doesn't apply match_mode to paragraphs, so always use "exact" phrase matching
        if "paragraph" in type_counts:
            para_wildcard = _build_wildcard_query_for_match_mode("paragraph_text.keyword", query, "exact")
            para_wc_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": "paragraph"}},
                            para_wildcard,
                        ]
                    }
                },
                "size": limit,
            }
            para_wc_response = es.search(index=INDEX_NAME, body=para_wc_query)
            para_wc_count = para_wc_response["hits"]["total"]["value"]
            counts_by_category["paragraphs"] = para_wc_count
            if para_wc_count > 0:
                para_wc_results = []
                for hit in para_wc_response["hits"]["hits"]:
                    result = _parse_text_search_result(hit, query, "paragraphs")
                    para_wc_results.append(result)
                if para_wc_results:
                    results_by_category["paragraphs"] = para_wc_results
            else:
                results_by_category.pop("paragraphs", None)

        # Step 11: Override notes count with wildcard query
        if "note" in type_counts:
            note_wildcard = _build_wildcard_query_for_match_mode("note_text.keyword", query, match_mode)
            note_wc_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": "note"}},
                            note_wildcard,
                        ]
                    }
                },
                "size": limit,
            }
            note_wc_response = es.search(index=INDEX_NAME, body=note_wc_query)
            note_wc_count = note_wc_response["hits"]["total"]["value"]
            counts_by_category["notes"] = note_wc_count
            if note_wc_count > 0:
                note_wc_results = []
                for hit in note_wc_response["hits"]["hits"]:
                    result = _parse_text_search_result(hit, query, "notes")
                    note_wc_results.append(result)
                if note_wc_results:
                    results_by_category["notes"] = note_wc_results
            else:
                results_by_category.pop("notes", None)

        # Step 12: Override authors count with wildcard query
        # NOTE: Oracle doesn't apply match_mode to authors, so always use "exact" phrase matching
        if "author" in type_counts:
            auth_wildcard = _build_wildcard_query_for_match_mode("author_name.keyword", query, "exact")
            auth_wc_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": "author"}},
                            auth_wildcard,
                        ]
                    }
                },
                "size": limit,
            }
            auth_wc_response = es.search(index=INDEX_NAME, body=auth_wc_query)
            auth_wc_count = auth_wc_response["hits"]["total"]["value"]
            counts_by_category["authors"] = auth_wc_count
            if auth_wc_count > 0:
                auth_wc_results = []
                for hit in auth_wc_response["hits"]["hits"]:
                    result = _parse_text_search_result(hit, query, "authors")
                    auth_wc_results.append(result)
                if auth_wc_results:
                    results_by_category["authors"] = auth_wc_results
            else:
                results_by_category.pop("authors", None)

        # Step 13: Remove categories that don't have wildcard overrides
        # These categories used fuzzy matching in Step 2 which doesn't respect match_mode
        # Remove them to ensure total_results only counts properly filtered results
        categories_without_override = ["abstracts", "colleagues", "pathways", "external_ids", "orthologs", "literature_topics"]
        for cat in categories_without_override:
            counts_by_category.pop(cat, None)
            results_by_category.pop(cat, None)

        # Sort gene-related categories by organism priority
        for cat in ["genes", "descriptions", "paragraphs", "notes", "external_ids", "orthologs", "name_descriptions"]:
            if cat in results_by_category and results_by_category[cat]:
                results_by_category[cat].sort(
                    key=lambda r: (_get_organism_priority(r.organism), r.name or '')
                )

        # Build category results with actual counts
        categories: list[TextSearchCategoryResult] = []
        total_results = 0

        for cat_key, results in results_by_category.items():
            if results:
                display_name = TEXT_CATEGORY_DISPLAY_NAMES.get(cat_key, cat_key)
                actual_count = counts_by_category.get(cat_key, len(results))
                categories.append(TextSearchCategoryResult(
                    category=cat_key,
                    display_name=display_name,
                    count=actual_count,  # Use actual count, not just sample size
                    results=results,
                ))
                total_results += actual_count

        return TextSearchResponse(
            query=query,
            total_results=total_results,
            categories=categories,
        )

    except Exception as e:
        import traceback
        logger.error(f"Elasticsearch text search failed: {e}")
        logger.error(f"Query: {query}, match_mode: {match_mode}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


def text_search_category(
    es: Elasticsearch,
    query: str,
    category: str,
    match_mode: str = "all",
) -> Optional[TextSearchCategoryPagedResponse]:
    """
    Text search within a specific category using Elasticsearch.

    Args:
        es: Elasticsearch client
        query: Search query string
        category: Category to search
        match_mode: One of "exact", "all", "any"
            - exact: Search for the exact phrase
            - all: All words must appear (AND logic) - default
            - any: Any word can appear (OR logic)

    Returns None if category is not supported by ES (caller should fall back to Oracle).
    """
    if category not in TEXT_CATEGORY_TO_ES_TYPE:
        return None

    es_type = TEXT_CATEGORY_TO_ES_TYPE[category]

    # Special handling for certain gene-based categories - use wildcard to match Oracle
    if category == "descriptions":
        desc_wildcard = _build_wildcard_query_for_match_mode("headline.keyword", query, match_mode)
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "gene"}},
                        desc_wildcard,
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
    elif category == "name_descriptions":
        # NOTE: Oracle doesn't apply match_mode to name_descriptions, so always use "exact"
        nd_wildcard = _build_wildcard_query_for_match_mode("name_description.keyword", query, "exact")
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "gene"}},
                        nd_wildcard,
                    ]
                }
            },
            "size": 1000,
            "highlight": {
                "fields": {"name_description": {}},
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
            },
            "aggs": {
                "by_organism": {"terms": {"field": "organism", "size": 20}}
            },
        }
    elif category == "paper_titles":
        # Use wildcard to match Oracle's LIKE behavior (finds substring matches)
        pt_wildcard = _build_wildcard_query_for_match_mode("title.keyword", query, match_mode)
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "reference"}},
                        pt_wildcard,
                    ]
                }
            },
            "size": 1000,
            "highlight": {
                "fields": {"title": {}},
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
            },
        }
    elif category == "paragraphs":
        # NOTE: Oracle doesn't apply match_mode to paragraphs, so always use "exact"
        para_wildcard = _build_wildcard_query_for_match_mode("paragraph_text.keyword", query, "exact")
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "paragraph"}},
                        para_wildcard,
                    ]
                }
            },
            "size": 1000,
            "highlight": {
                "fields": {"paragraph_text": {}},
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
            },
        }
    elif category == "notes":
        note_wildcard = _build_wildcard_query_for_match_mode("note_text.keyword", query, match_mode)
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "note"}},
                        note_wildcard,
                    ]
                }
            },
            "size": 1000,
            "highlight": {
                "fields": {"note_text": {}},
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
            },
        }
    elif category == "authors":
        # NOTE: Oracle doesn't apply match_mode to authors, so always use "exact"
        auth_wildcard = _build_wildcard_query_for_match_mode("author_name.keyword", query, "exact")
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "author"}},
                        auth_wildcard,
                    ]
                }
            },
            "size": 1000,
            "highlight": {
                "fields": {"author_name": {}},
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
            },
        }
    elif category == "genes":
        # For genes, we search multiple fields
        # NOTE: Oracle doesn't apply match_mode to genes, so always use "exact"
        gene_name_wc = _build_wildcard_query_for_match_mode("gene_name.keyword", query, "exact")
        feature_name_wc = _build_wildcard_query_for_match_mode("feature_name", query, "exact", case_insensitive=False)
        dbxref_wc = _build_wildcard_query_for_match_mode("dbxref_id", query, "exact", case_insensitive=False)
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "gene"}},
                    ],
                    "should": [
                        gene_name_wc,
                        feature_name_wc,
                        dbxref_wc,
                    ],
                    "minimum_should_match": 1,
                }
            },
            "size": 1000,
            "highlight": {
                "fields": {"gene_name": {}, "feature_name": {}},
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
            },
            "aggs": {
                "by_organism": {"terms": {"field": "organism", "size": 20}}
            },
        }
    elif category == "go_terms":
        # NOTE: Oracle doesn't apply match_mode to go_terms, so always use "exact"
        go_wildcard = _build_wildcard_query_for_match_mode("go_term.keyword", query, "exact")
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "go_term"}},
                        go_wildcard,
                    ]
                }
            },
            "size": 1000,
            "highlight": {
                "fields": {"go_term": {}},
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
            },
        }
    elif category == "phenotypes":
        # NOTE: Oracle doesn't apply match_mode to phenotypes, so always use "exact"
        ph_wildcard = _build_wildcard_query_for_match_mode("observable.keyword", query, "exact")
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "phenotype"}},
                        ph_wildcard,
                    ]
                }
            },
            "size": 1000,
            "highlight": {
                "fields": {"observable": {}},
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
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
