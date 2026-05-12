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
    SearchResultLink,
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
    'Candida tropicalis MYA-3404',
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


def _build_reference_links(
    dbxref_id: Optional[str],
    pubmed: Optional[str],
    full_text_url: Optional[str] = None
) -> list[SearchResultLink]:
    """
    Build citation links for a reference in ES search results.

    Generates links for:
    - CGD Paper (internal link to reference page)
    - PubMed (external link to NCBI PubMed)
    - Full Text (external link if available)

    Args:
        dbxref_id: CGD reference ID (e.g., CAL0125222)
        pubmed: PubMed ID
        full_text_url: Full text URL if available

    Returns:
        List of SearchResultLink objects
    """
    links = []

    # CGD Paper link (always present if we have dbxref_id)
    if dbxref_id:
        links.append(SearchResultLink(
            name="CGD Paper",
            url=f"/reference/{dbxref_id}",
            link_type="internal"
        ))

    # PubMed link (if pubmed ID exists)
    if pubmed:
        links.append(SearchResultLink(
            name="PubMed",
            url=f"https://pubmed.ncbi.nlm.nih.gov/{pubmed}",
            link_type="external"
        ))

    # Full Text link (if available)
    if full_text_url:
        links.append(SearchResultLink(
            name="Full Text",
            url=full_text_url,
            link_type="external"
        ))

    return links


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

    Prioritizes exact gene name matches, then prefix matching.
    Uses case-insensitive matching for genes.
    Only returns types that autocomplete handles: gene, go_term, phenotype, reference.

    Priority order (using constant_score to guarantee exact matches always win):
    1. Exact gene name match - constant score 10000
    2. Gene name prefix match - constant score 5000
    3. Feature name prefix match - constant score 4000
    4. CGDID prefix match - constant score 3500
    5. GO term prefix match - constant score 1500
    6. Phenotype prefix match - constant score 1000
    7. Headline/description match - TF-IDF with boost 1 (max ~500)
    """
    query_upper = query.upper()
    query_lower = query.lower()

    return {
        "query": {
            "bool": {
                # Only return types that autocomplete handles
                "filter": [
                    {"terms": {"type": ["gene", "go_term", "phenotype", "reference"]}}
                ],
                "should": [
                    # Exact gene name match (highest priority) - use constant_score to guarantee
                    # fixed score that beats TF-IDF headline scores
                    {"constant_score": {"filter": {"wildcard": {"gene_name.keyword": {"value": query_lower, "case_insensitive": True}}}, "boost": 10000}},
                    {"constant_score": {"filter": {"wildcard": {"feature_name": {"value": query_upper, "case_insensitive": True}}}, "boost": 10000}},
                    # Gene name prefix match (very high priority)
                    {"constant_score": {"filter": {"wildcard": {"gene_name.keyword": {"value": f"{query_lower}*", "case_insensitive": True}}}, "boost": 5000}},
                    # Feature name prefix match (high priority)
                    {"constant_score": {"filter": {"wildcard": {"feature_name": {"value": f"{query_upper}*", "case_insensitive": True}}}, "boost": 4000}},
                    # CGDID prefix match
                    {"constant_score": {"filter": {"wildcard": {"dbxref_id": {"value": f"{query_upper}*", "case_insensitive": True}}}, "boost": 3500}},
                    # Prefix match for GO terms
                    {"constant_score": {"filter": {"prefix": {"go_term.keyword": {"value": query_lower}}}, "boost": 1500}},
                    # Prefix match for phenotypes
                    {"constant_score": {"filter": {"prefix": {"observable.keyword": {"value": query_lower}}}, "boost": 1000}},
                    # Gene headline/description match (for searches like "actin")
                    # Use constant_score to ensure headline matches rank between GO term matches (1500)
                    # and name prefix matches (4000+), but below exact name matches (10000)
                    {
                        "constant_score": {
                            "filter": {
                                "bool": {
                                    "must": [
                                        {"match_phrase_prefix": {"headline": query}},
                                        {"term": {"type": "gene"}}
                                    ]
                                }
                            },
                            "boost": 2000
                        }
                    },
                    # General headline/description match as fallback
                    {
                        "multi_match": {
                            "query": query,
                            "fields": [
                                "headline",
                                "name_description",
                            ],
                            "type": "phrase_prefix",
                            "boost": 1,
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
                "dbxref_id": {},
                "go_term": {},
                "observable": {},
                "headline": {},
                "name_description": {},
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
    """Parse ES hit into SearchResult for ortholog type.

    Each document represents one gene in an ortholog group. The fields are:
    - gene_name: this gene's name
    - feature_name: this gene's feature name
    - organism: this gene's organism
    - all_gene_names: space-separated list of all gene names in the group
    - related_orthologs: list of other orthologs in the group
    """
    source = hit["_source"]
    highlights = hit.get("highlight", {})

    # This gene's info
    gene_name = source.get("gene_name")
    feature_name = source.get("feature_name")
    organism = source.get("organism")
    dbxref_id = source.get("dbxref_id") or source.get("id")
    ortholog_source = source.get("ortholog_source", "Ortholog")

    # Display name: use "name/feature" format when both exist and are different
    if gene_name and feature_name and gene_name != feature_name:
        display_name = f"{gene_name}/{feature_name}"
    else:
        display_name = gene_name or feature_name or ""

    highlighted_name = _extract_highlight(highlights, "gene_name", None)
    if not highlighted_name:
        highlighted_name = _extract_highlight(highlights, "all_gene_names", None)
    if not highlighted_name:
        highlighted_name = _highlight_text(display_name, query)

    # Link to this gene's locus page
    gene_link = f"/locus/{feature_name}" if feature_name else source.get("link")

    return SearchResult(
        category="orthologs",
        id=dbxref_id or "",
        name=display_name,
        description=None,
        link=gene_link,
        organism=organism,
        highlighted_name=highlighted_name,
        highlighted_description=None,
        # Relationship fields for frontend
        ortholog_display=display_name,
        ortholog_organism=organism,
        ortholog_type=ortholog_source,
        cgd_gene_name=gene_name,
        cgd_gene_id=dbxref_id,
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
        # Search all_gene_names and all_feature_names to find matching ortholog groups
        should_clauses = [
            {"wildcard": {"all_gene_names": {"value": f"*{query_upper}*", "case_insensitive": True, "boost": 10}}},
            {"wildcard": {"all_feature_names": {"value": f"*{query_upper}*", "case_insensitive": True, "boost": 8}}},
            {"match": {"gene_name": {"query": query, "boost": 5}}},
        ]

    if not should_clauses:
        # Fallback to generic name match
        should_clauses = [{"match": {"name": {"query": query}}}]

    # Build must clauses - add CGOB/BLAST RBH/BLAST filter for orthologs
    must_clauses = [{"term": {"type": doc_type}}]
    if doc_type == "ortholog":
        # Include CGOB (curated), BLAST RBH (reciprocal best hits), and BLAST (best hits)
        # Don't filter by organism - show ALL orthologs in the cluster (transitive)
        must_clauses.append({"terms": {"ortholog_source": ["CGOB", "BLAST RBH", "BLAST"]}})

    return {
        "query": {
            "bool": {
                "must": must_clauses,
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
                "all_gene_names": {},
                "all_feature_names": {},
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
        # Gene product/description fields
        {"match": {"headline": {"query": query, "boost": 4}}},
        {"match": {"name_description": {"query": query, "boost": 3}}},

        # GO term fields - search go_term only
        {"term": {"goid": {"value": query_upper, "boost": 20}}},
        {"prefix": {"go_term.keyword": {"value": query_lower, "boost": 8}}},
        {"match": {"go_term": {"query": query, "boost": 5}}},

        # Phenotype fields
        {"prefix": {"observable.keyword": {"value": query_lower, "boost": 8}}},
        {"match": {"observable": {"query": query, "boost": 5}}},

        # Ortholog fields - search all gene/feature names in the group
        {"wildcard": {"all_gene_names": {"value": f"*{query_upper}*", "case_insensitive": True, "boost": 10}}},
        {"wildcard": {"all_feature_names": {"value": f"*{query_upper}*", "case_insensitive": True, "boost": 8}}},

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
    # Include "descriptions" to find genes by their headline/description text
    quick_search_categories = ["genes", "descriptions", "go_terms", "phenotypes", "references", "orthologs"]

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

    # First pass: collect ALL gene occurrences to track organisms and find best data
    # gene_organisms: tracks all organisms for each gene name
    # gene_best_data: stores the best data for each gene (prefer C. albicans)
    # gene_max_score: tracks the max ES score for each gene (for sorting by relevance)
    gene_organisms: dict[str, set] = {}  # gene_name -> set of organisms
    gene_best_data: dict[str, dict] = {}  # gene_name -> best data dict
    gene_max_score: dict[str, float] = {}  # gene_name -> max ES score

    for hit in response["hits"]["hits"]:
        source = hit["_source"]
        highlights = hit.get("highlight", {})
        doc_type = source.get("type")
        score = hit.get("_score", 0.0)

        if doc_type == "gene":
            display_name = source.get("gene_name") or source.get("feature_name") or source.get("name")
            organism = source.get("organism")
            if display_name:
                # Track ALL organisms for this gene name
                if display_name not in gene_organisms:
                    gene_organisms[display_name] = set()
                if organism:
                    gene_organisms[display_name].add(organism)

                # Track max ES score for this gene name
                if display_name not in gene_max_score:
                    gene_max_score[display_name] = score
                else:
                    gene_max_score[display_name] = max(gene_max_score[display_name], score)

                # Build data for this occurrence
                headline = source.get("headline")
                description = headline[:80] + "..." if headline and len(headline) > 80 else headline

                highlighted_text = _extract_highlight(highlights, "gene_name", None)
                if not highlighted_text:
                    highlighted_text = _highlight_text(display_name, query)

                highlighted_desc = _extract_highlight(highlights, "headline", None)
                if not highlighted_desc:
                    highlighted_desc = _extract_highlight(highlights, "name_description", None)
                if not highlighted_desc and description:
                    highlighted_desc = _highlight_text(description, query)

                current_data = {
                    "text": display_name,
                    "link": f"/locus/{display_name}",
                    "description": description,
                    "organism": organism,
                    "highlighted_text": highlighted_text,
                    "highlighted_description": highlighted_desc,
                }

                # Keep best data: prefer C. albicans SC5314, then first occurrence
                if display_name not in gene_best_data:
                    gene_best_data[display_name] = current_data
                elif organism == "Candida albicans SC5314":
                    # Replace with C. albicans data (better description usually)
                    gene_best_data[display_name] = current_data

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

    # Second pass: create gene suggestions from best data
    # Only show organism if gene exists in exactly one organism
    for gene_name, data in gene_best_data.items():
        num_organisms = len(gene_organisms.get(gene_name, set()))
        show_organism = num_organisms == 1
        genes.append(AutocompleteSuggestion(
            text=gene_name,
            category="gene",
            link=data["link"],
            description=data["description"],
            organism=data["organism"] if show_organism else None,
            highlighted_text=data["highlighted_text"],
            highlighted_description=data["highlighted_description"],
        ))

    # Sort genes: primary by ES score (descending), secondary by organism priority
    # This ensures exact name matches (high score) always appear first
    genes.sort(key=lambda s: (-gene_max_score.get(s.text, 0), _get_organism_priority(s.organism)))

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
    "descriptions": "gene",  # Uses headline/name_description field
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


def _build_category_query(query: str, es_type: str, size: int = 10000) -> dict:
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
        "ortholog": ["gene_name^3", "feature_name^2", "all_gene_names^3", "all_feature_names^2"],
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
                "all_gene_names": {},
                "all_feature_names": {},
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


def _build_restrictive_gene_query(query: str, es_type: str, size: int = 10000) -> dict:
    """
    Build restrictive ES query for genes/orthologs category search.

    For genes: matches gene names, aliases, feature names.
    For orthologs: matches ortholog names (the ortholog that points to a CGD gene).
    """
    query_upper = query.upper()

    if es_type == "ortholog":
        # For orthologs, search all_gene_names and all_feature_names to find matching groups
        # This finds all orthologs of a gene (e.g., HOG1 finds all Candida orthologs)
        # Show all orthologs in cluster (transitive) - don't filter by organism
        # Use wildcard instead of match to avoid partial word matches (e.g., "3" matching POX1-3)
        return {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "ortholog"}},
                        {"terms": {"ortholog_source": ["CGOB", "BLAST RBH", "BLAST"]}},
                    ],
                    "should": [
                        # Match any gene name in the ortholog group
                        {"wildcard": {"all_gene_names": {"value": f"*{query_upper}*", "case_insensitive": True, "boost": 15}}},
                        # Match any feature name in the ortholog group
                        {"wildcard": {"all_feature_names": {"value": f"*{query_upper}*", "case_insensitive": True, "boost": 10}}},
                        # Also match this gene's specific name/feature
                        {"term": {"gene_name.keyword": {"value": query_upper, "boost": 15}}},
                        {"term": {"feature_name": {"value": query_upper, "boost": 15}}},
                    ],
                    "minimum_should_match": 1,
                }
            },
            "size": size,
            "highlight": {
                "fields": {
                    "gene_name": {},
                    "feature_name": {},
                    "all_gene_names": {},
                    "all_feature_names": {},
                },
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
            },
            "aggs": {
                # Aggregate by organism
                "by_organism": {
                    "terms": {"field": "organism", "size": 20}
                }
            },
        }
    else:
        # For genes, search by gene name only (not description)
        # Use restrictive matching to avoid partial word matches
        return {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": es_type}},
                    ],
                    "should": [
                        # CGDID exact/prefix match (high priority)
                        {"wildcard": {"dbxref_id": {"value": f"{query_upper}*", "case_insensitive": True, "boost": 20}}},
                        # Exact gene name match
                        {"term": {"gene_name.keyword": {"value": query_upper, "boost": 15}}},
                        {"term": {"feature_name": {"value": query_upper, "boost": 15}}},
                        # Prefix match on gene names
                        {"prefix": {"gene_name.keyword": {"value": query_upper, "boost": 10}}},
                        {"prefix": {"feature_name": {"value": query_upper, "boost": 10}}},
                        # Search in aliases - use wildcard to match exact alias, not tokenized words
                        {"wildcard": {"aliases": {"value": f"*{query_upper}*", "case_insensitive": True, "boost": 8}}},
                    ],
                    "minimum_should_match": 1,
                }
            },
            "size": size,
            "highlight": {
                "fields": {
                    "gene_name": {},
                    "feature_name": {},
                    "dbxref_id": {},
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


def _build_descriptions_query(query: str, size: int = 10000) -> dict:
    """
    Build ES query for searching genes by their headline/description.

    This allows finding genes by their product description (e.g., enzyme names).
    """
    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"type": "gene"}},
                    {
                        "multi_match": {
                            "query": query,
                            "fields": ["headline^2", "name_description"],
                            "type": "phrase_prefix",
                        }
                    },
                ],
            }
        },
        "size": size,
        "highlight": {
            "fields": {
                "headline": {},
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
    elif category == "descriptions":
        # Search genes by their headline/description text
        es_query = _build_descriptions_query(query)
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

    # Sort genes, descriptions, and orthologs by organism priority
    if category in ("genes", "descriptions", "orthologs"):
        results.sort(key=lambda r: (_get_organism_priority(r.organism), r.name or ''))

    # Get organism counts from aggregations
    organism_counts: Optional[dict[str, int]] = None
    if category in ("genes", "descriptions", "orthologs"):
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
        full_text_url = source.get("full_text_url")

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

        # For paper_titles, use title as citation (contains full formatted citation)
        citation_text = title if category == "paper_titles" else citation

        # Build links for CGD Paper, PubMed, and Full Text
        links = _build_reference_links(dbxref_id, pubmed, full_text_url)

        return TextSearchResult(
            category=result_category,
            id=dbxref_id or "",
            name=name,
            description=citation,
            link=source.get("link") or f"/reference/{dbxref_id}",
            organism=None,
            match_context=match_context,
            links=links,
            highlighted_name=_highlight_text(name, query),
            highlighted_description=match_context,
            citation=citation_text,
            highlighted_citation=_highlight_text(citation_text, query) if citation_text else None,
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
        pubmed = source.get("pubmed")
        link = source.get("link")
        full_text_url = source.get("full_text_url")

        # Extract dbxref_id from link (format: /reference/CAL0125222)
        dbxref_id = link.split("/")[-1] if link and link.startswith("/reference/") else None

        highlighted_name = _extract_highlight(highlights, "author_name", None)
        if not highlighted_name:
            highlighted_name = _highlight_text(author_name, query)

        # Build links for CGD Paper, PubMed, and Full Text
        links = _build_reference_links(dbxref_id, pubmed, full_text_url)

        return TextSearchResult(
            category="authors",
            id=source.get("id", ""),
            name=author_name or "",
            description=citation,
            link=link,
            organism=None,
            links=links,
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
        # Each document is one gene in an ortholog group
        gene_name = source.get("gene_name")
        feature_name = source.get("feature_name")
        organism = source.get("organism")
        related_orthologs = source.get("related_orthologs", [])

        # Display name: use "name/feature" format when both exist and are different
        if gene_name and feature_name and gene_name != feature_name:
            display_name = f"{gene_name}/{feature_name}"
        else:
            display_name = gene_name or feature_name or ""

        highlighted_name = _highlight_text(display_name, query)

        # Link to the gene's locus page
        gene_link = f"/locus/{feature_name}" if feature_name else source.get("link")

        return TextSearchResult(
            category="orthologs",
            id=source.get("dbxref_id") or source.get("id", ""),
            name=display_name,
            description=None,
            link=gene_link,
            organism=organism,
            homology_group_no=source.get("homology_group_no"),
            highlighted_name=highlighted_name,
            highlighted_description=None,
            ortholog_display=display_name,  # Unique per gene for AG Grid row deduplication
            related_orthologs=related_orthologs,  # Other genes in the ortholog group
        )

    elif doc_type == "literature_topic":
        topic = source.get("literature_topic")
        citation = source.get("citation")
        pubmed = source.get("pubmed")
        link = source.get("link")
        full_text_url = source.get("full_text_url")

        # Extract dbxref_id from link (format: /reference/CAL0125222)
        dbxref_id = link.split("/")[-1] if link and link.startswith("/reference/") else None

        name = f"PMID:{pubmed}" if pubmed else source.get("name", "")

        highlighted_name = _extract_highlight(highlights, "literature_topic", None)
        if not highlighted_name:
            highlighted_name = _highlight_text(topic, query)

        # Build links for CGD Paper, PubMed, and Full Text
        links = _build_reference_links(dbxref_id, pubmed, full_text_url)

        return TextSearchResult(
            category="literature_topics",
            id=source.get("id", ""),
            name=name,
            description=f"{topic}: {citation}" if citation else topic,
            link=link,
            organism=None,
            links=links,
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
    - Genes: gene_name, feature_name, dbxref_id (CGDID), aliases, headline
    - GO terms: go_term + goid
    - References: title (+ pubmed for numeric queries)
    """
    return [
        "name^3",
        "gene_name^3",
        "feature_name^2",
        "dbxref_id^3",  # CGDID - primary identifier for genes
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
        "all_gene_names",
        "all_feature_names",
        "literature_topic",
    ]


def _is_cgdid_query(query: str) -> bool:
    """Check if query looks like a CGDID (e.g., CAL0000191211)."""
    import re
    # CGDIDs start with 2-4 uppercase letters followed by digits
    return bool(re.match(r'^[A-Z]{2,4}\d+$', query.upper()))


def _build_text_search_counts_query(query: str) -> dict:
    """Build ES query to get counts per type for text search."""
    should_clauses = [
        {
            "multi_match": {
                "query": query,
                "fields": _get_text_search_fields(),
                "type": "best_fields",
                "fuzziness": "AUTO",
            }
        },
    ]

    # For numeric queries, also search pubmed field for references
    if query.isdigit():
        should_clauses.append(
            {"term": {"pubmed": {"value": int(query), "boost": 10}}}
        )

    # For CGDID-like queries, add exact term match on dbxref_id
    if _is_cgdid_query(query):
        should_clauses.append(
            {"term": {"dbxref_id": {"value": query.upper(), "boost": 20}}}
        )

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


def _build_text_search_type_query(query: str, doc_type: str, size: int = 10) -> dict:
    """Build ES query for text search filtered to a specific type."""
    highlight_config = {
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
            "all_gene_names": {},
            "all_feature_names": {},
            "literature_topic": {},
            "dbxref_id": {},
        },
        "pre_tags": ["<mark>"],
        "post_tags": ["</mark>"],
    }

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
            "highlight": highlight_config,
        }

    # For gene type with CGDID-like query, also search dbxref_id
    if doc_type == "gene" and _is_cgdid_query(query):
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
                        {"term": {"dbxref_id": {"value": query.upper(), "boost": 20}}},
                    ],
                    "minimum_should_match": 1,
                }
            },
            "size": size,
            "highlight": highlight_config,
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
        "highlight": highlight_config,
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
            dbxref_wc = _build_wildcard_query_for_match_mode("dbxref_id", query, "exact", case_insensitive=True)
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

        # Step 13: Override orthologs count with wildcard query
        if "ortholog" in type_counts:
            # Search ortholog groups by any gene name or feature name in the group
            # Each document is one gene in an ortholog group; searching returns all
            # members of groups where any member matches the query
            all_genes_wildcard = _build_wildcard_query_for_match_mode("all_gene_names", query, match_mode)
            all_features_wildcard = _build_wildcard_query_for_match_mode("all_feature_names", query, match_mode)
            ortholog_wc_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": "ortholog"}},
                            {"terms": {"ortholog_source": ["CGOB", "BLAST RBH", "BLAST"]}},
                        ],
                        "should": [
                            all_genes_wildcard,
                            all_features_wildcard,
                        ],
                        "minimum_should_match": 1,
                    }
                },
                "size": limit,
            }
            ortholog_wc_response = es.search(index=INDEX_NAME, body=ortholog_wc_query)
            ortholog_wc_count = ortholog_wc_response["hits"]["total"]["value"]
            counts_by_category["orthologs"] = ortholog_wc_count
            if ortholog_wc_count > 0:
                ortholog_wc_results = []
                for hit in ortholog_wc_response["hits"]["hits"]:
                    result = _parse_text_search_result(hit, query, "orthologs")
                    ortholog_wc_results.append(result)
                if ortholog_wc_results:
                    results_by_category["orthologs"] = ortholog_wc_results
            else:
                results_by_category.pop("orthologs", None)

        # Step 14: Remove categories that don't have wildcard overrides
        # These categories used fuzzy matching in Step 2 which doesn't respect match_mode
        # Remove them to ensure total_results only counts properly filtered results
        categories_without_override = ["abstracts", "colleagues", "pathways", "external_ids", "literature_topics"]
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
            "size": 10000,
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
            "size": 10000,
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
            "size": 10000,
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
            "size": 10000,
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
            "size": 10000,
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
            "size": 10000,
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
        dbxref_wc = _build_wildcard_query_for_match_mode("dbxref_id", query, "exact", case_insensitive=True)
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
            "size": 10000,
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
            "size": 10000,
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
            "size": 10000,
            "highlight": {
                "fields": {"observable": {}},
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
            },
        }
    elif category == "orthologs":
        # Search all_gene_names and all_feature_names to find matching ortholog groups
        # Include CGOB, BLAST RBH, and BLAST
        # Show all orthologs in cluster (transitive) - don't filter by organism
        all_genes_wildcard = _build_wildcard_query_for_match_mode("all_gene_names", query, match_mode)
        all_features_wildcard = _build_wildcard_query_for_match_mode("all_feature_names", query, match_mode)
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "ortholog"}},
                        {"terms": {"ortholog_source": ["CGOB", "BLAST RBH", "BLAST"]}},
                    ],
                    "should": [
                        all_genes_wildcard,
                        all_features_wildcard,
                    ],
                    "minimum_should_match": 1,
                }
            },
            "size": 10000,
            "highlight": {
                "fields": {"gene_name": {}, "all_gene_names": {}, "all_feature_names": {}},
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
            },
            "aggs": {
                # Aggregate by organism
                "by_organism": {"terms": {"field": "organism", "size": 20}}
            },
        }
    else:
        es_query = _build_category_query(query, es_type, size=10000)

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


def get_ortholog_organisms(
    es: Elasticsearch,
    gene_name_or_feature: str,
) -> list[dict]:
    """
    Get list of organisms that have orthologs for a given gene.

    Uses ES aggregation to efficiently find unique ortholog organisms
    and returns the feature_name for navigation.

    Args:
        es: Elasticsearch client
        gene_name_or_feature: Gene name or feature name to search for

    Returns:
        List of dicts with {organism, feature_name} for each ortholog organism
    """
    query_upper = gene_name_or_feature.upper()

    # Query ortholog docs where this gene appears in the ortholog group
    # Search all_gene_names and all_feature_names to find matching groups
    es_query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"type": "ortholog"}},
                ],
                "should": [
                    # Search all gene names in the group
                    {"wildcard": {"all_gene_names": {"value": f"*{query_upper}*", "case_insensitive": True, "boost": 10}}},
                    # Search all feature names in the group
                    {"wildcard": {"all_feature_names": {"value": f"*{query_upper}*", "case_insensitive": True, "boost": 10}}},
                ],
                "minimum_should_match": 1,
            }
        },
        "size": 0,
        "aggs": {
            "by_organism": {
                "terms": {
                    "field": "organism",
                    "size": 50,
                },
                "aggs": {
                    "feature_info": {
                        "top_hits": {
                            "size": 1,
                            "_source": ["feature_name", "organism"],
                        }
                    }
                }
            }
        },
    }

    try:
        response = es.search(index=INDEX_NAME, body=es_query)
        buckets = response.get("aggregations", {}).get("by_organism", {}).get("buckets", [])

        results = []
        for bucket in buckets:
            organism = bucket["key"]
            # Get the feature_name from the top hit
            top_hits = bucket.get("feature_info", {}).get("hits", {}).get("hits", [])
            if top_hits:
                source = top_hits[0].get("_source", {})
                feature_name = source.get("feature_name")
                if organism and feature_name:
                    results.append({
                        "organism": organism,
                        "feature_name": feature_name,
                    })

        # Sort by organism priority
        results.sort(key=lambda x: _get_organism_priority(x["organism"]))
        return results

    except Exception as e:
        logger.error(f"Error fetching ortholog organisms for {gene_name_or_feature}: {e}")
        return []


# =============================================================================
# VIRULENCE FACTOR SEARCH (ES-based)
# =============================================================================

def search_virulence_factors(
    es: Elasticsearch,
    categories: list[str] | None = None,
    organisms: list[str] | None = None,
    search_term: str | None = None,
    page: int = 1,
    page_size: int = 25,
    max_evidence_tier: int | None = None,
    min_confidence_score: int | None = None,
    hide_housekeeping: bool = False,
    sort_by: str = "confidence_score",
    sort_order: str = "desc",
    evidence_types: list[str] | None = None,
    min_paper_count: int | None = None,
    max_paper_count: int | None = None,
) -> dict | None:
    """
    Search virulence factors using Elasticsearch.

    Args:
        es: Elasticsearch client
        categories: List of category keys to filter (e.g., ["adhesins", "biofilm"])
        organisms: List of organism abbreviations to filter
        search_term: Optional keyword search
        page: Page number (1-indexed)
        page_size: Results per page
        max_evidence_tier: Only include genes with evidence tier <= this value
        min_confidence_score: Only include genes with confidence score >= this value
        hide_housekeeping: If True, exclude housekeeping genes
        sort_by: Field to sort by (confidence_score, gene_name, evidence_tier)
        sort_order: Sort order (asc, desc)
        evidence_types: Filter by evidence types (GO, PHE, KW)
        min_paper_count: Only include genes with >= this many papers
        max_paper_count: Only include genes with <= this many papers

    Returns:
        Dict with items, total_count, page, page_size, categories_searched
        Returns None on error (caller should fall back to Oracle).
    """
    if not categories:
        return {
            "items": [],
            "total_count": 0,
            "page": page,
            "page_size": page_size,
            "categories_searched": [],
        }

    # Build ES query
    must_clauses = [{"term": {"type": "virulence_factor"}}]

    # Filter by categories
    if categories:
        must_clauses.append({"terms": {"categories": categories}})

    # Filter by organisms
    if organisms:
        # Use organism abbreviations as-is (they come in correct format from frontend)
        must_clauses.append({"terms": {"organism_abbrev.keyword": organisms}})

    # Search term filter
    if search_term:
        must_clauses.append({
            "bool": {
                "should": [
                    {"wildcard": {"gene_name.keyword": {"value": f"*{search_term.lower()}*", "case_insensitive": True}}},
                    {"wildcard": {"feature_name": {"value": f"*{search_term.upper()}*"}}},
                    {"wildcard": {"headline.keyword": {"value": f"*{search_term.lower()}*", "case_insensitive": True}}},
                ],
                "minimum_should_match": 1,
            }
        })

    # Evidence quality filters
    if max_evidence_tier is not None:
        must_clauses.append({"range": {"evidence_tier": {"lte": max_evidence_tier}}})

    if min_confidence_score is not None:
        must_clauses.append({"range": {"confidence_score": {"gte": min_confidence_score}}})

    if hide_housekeeping:
        must_clauses.append({"term": {"is_housekeeping": False}})

    # Filter by evidence types (GO, PHE, KW)
    if evidence_types:
        must_clauses.append({"terms": {"evidence_types.keyword": [et.upper() for et in evidence_types]}})

    # Filter by paper count
    if min_paper_count is not None:
        must_clauses.append({"range": {"paper_count": {"gte": min_paper_count}}})

    if max_paper_count is not None:
        must_clauses.append({"range": {"paper_count": {"lte": max_paper_count}}})

    # Build sort clause
    if sort_by == "confidence_score":
        sort_clause = [
            {"confidence_score": {"order": sort_order}},
            {"gene_name.keyword": {"order": "asc", "missing": "_last"}},
        ]
    elif sort_by == "evidence_tier":
        # For evidence tier, lower is better, so reverse the sort direction
        tier_order = "asc" if sort_order == "desc" else "desc"
        sort_clause = [
            {"evidence_tier": {"order": tier_order}},
            {"gene_name.keyword": {"order": "asc", "missing": "_last"}},
        ]
    else:  # gene_name
        sort_clause = [
            {"gene_name.keyword": {"order": sort_order, "missing": "_last"}},
        ]

    es_query = {
        "query": {
            "bool": {
                "must": must_clauses,
            }
        },
        "size": 10000,  # Get all for pagination
        "sort": sort_clause,
    }

    try:
        response = es.search(index=INDEX_NAME, body=es_query)
        total_count = response["hits"]["total"]["value"]

        # Parse results
        all_items = []
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            all_items.append({
                "feature_no": source.get("feature_no"),
                "feature_name": source.get("feature_name"),
                "gene_name": source.get("gene_name"),
                "organism": source.get("organism"),
                "organism_abbrev": source.get("organism_abbrev"),
                "headline": source.get("headline"),
                "description": source.get("headline"),
                "categories": source.get("category_names", []),
                "match_reasons": source.get("match_reasons", []),
                # Evidence quality fields
                "evidence_tier": source.get("evidence_tier", 4),
                "evidence_tier_name": source.get("evidence_tier_name", "Indirect"),
                "confidence_score": source.get("confidence_score", 0),
                "confidence_tier": source.get("confidence_tier", "Low"),
                "is_housekeeping": source.get("is_housekeeping", False),
                "housekeeping_reason": source.get("housekeeping_reason"),
                "ortholog_count": source.get("ortholog_count", 0),
                # Quick win fields
                "inclusion_reason": source.get("inclusion_reason", ""),
                "evidence_types": source.get("evidence_types", []),
                # Paper/reference fields
                "paper_count": source.get("paper_count", 0),
                "pmids": source.get("pmids", []),
                # Split evidence fields
                "direct_evidence": source.get("direct_evidence", []),
                "indirect_evidence": source.get("indirect_evidence", []),
                # Summary and importance fields
                "summary": source.get("summary", ""),
                "summary_full": source.get("summary_full", ""),
                "importance_level": source.get("importance_level", "low"),
                "importance_label": source.get("importance_label", "Indirect evidence"),
                "evidence_breakdown": source.get("evidence_breakdown", {}),
                # Structural data links
                "uniprot_id": source.get("uniprot_id"),
                "alphafold_url": source.get("alphafold_url"),
                # Cross-species ortholog data
                "orthologs": source.get("orthologs", []),
            })

        # Apply pagination
        offset = (page - 1) * page_size
        paginated = all_items[offset:offset + page_size]

        return {
            "items": paginated,
            "total_count": total_count,
            "page": page,
            "page_size": page_size,
            "categories_searched": categories,
        }

    except Exception as e:
        logger.error(f"Elasticsearch virulence search failed: {e}")
        return None


def get_virulence_categories_es(
    es: Elasticsearch,
    organism: str | None = None,
) -> dict | None:
    """
    Get virulence categories with counts using Elasticsearch.

    Args:
        es: Elasticsearch client
        organism: Optional organism abbreviation to filter

    Returns:
        Dict with categories list and total_genes count
        Returns None on error.
    """
    from cgd.schemas.virulence_schema import VIRULENCE_CATEGORIES

    must_clauses = [{"term": {"type": "virulence_factor"}}]

    if organism:
        must_clauses.append({"term": {"organism_abbrev.keyword": organism}})

    es_query = {
        "query": {
            "bool": {
                "must": must_clauses,
            }
        },
        "size": 0,
        "aggs": {
            "by_category": {
                "terms": {"field": "categories", "size": 20}
            },
            "unique_genes": {
                "cardinality": {"field": "feature_no"}
            },
        },
    }

    try:
        response = es.search(index=INDEX_NAME, body=es_query)

        # Parse category counts
        category_counts = {}
        for bucket in response.get("aggregations", {}).get("by_category", {}).get("buckets", []):
            category_counts[bucket["key"]] = bucket["doc_count"]

        # Build category list
        categories = []
        for cat_key, cat_config in VIRULENCE_CATEGORIES.items():
            categories.append({
                "key": cat_key,
                "name": cat_config["name"],
                "description": cat_config["description"],
                "count": category_counts.get(cat_key, 0),
            })

        total_genes = response.get("aggregations", {}).get("unique_genes", {}).get("value", 0)

        return {
            "categories": categories,
            "total_genes": total_genes,
        }

    except Exception as e:
        logger.error(f"Elasticsearch virulence categories failed: {e}")
        return None


def get_virulence_stats_es(es: Elasticsearch) -> dict | None:
    """
    Get virulence statistics using Elasticsearch.

    Returns:
        Dict with total_genes, categories stats, organisms stats
        Returns None on error.
    """
    from cgd.schemas.virulence_schema import VIRULENCE_CATEGORIES

    es_query = {
        "query": {
            "term": {"type": "virulence_factor"}
        },
        "size": 0,
        "aggs": {
            "by_category": {
                "terms": {"field": "categories", "size": 20}
            },
            "by_organism": {
                "terms": {"field": "organism_abbrev.keyword", "size": 20}
            },
            "unique_genes": {
                "cardinality": {"field": "feature_no"}
            },
        },
    }

    try:
        response = es.search(index=INDEX_NAME, body=es_query)

        # Parse category stats
        category_stats = []
        category_counts = {}
        for bucket in response.get("aggregations", {}).get("by_category", {}).get("buckets", []):
            category_counts[bucket["key"]] = bucket["doc_count"]

        for cat_key, cat_config in VIRULENCE_CATEGORIES.items():
            category_stats.append({
                "key": cat_key,
                "name": cat_config["name"],
                "count": category_counts.get(cat_key, 0),
            })

        # Parse organism stats
        organism_stats = []
        for bucket in response.get("aggregations", {}).get("by_organism", {}).get("buckets", []):
            organism_stats.append({
                "organism_abbrev": bucket["key"],
                "organism_name": bucket["key"],  # We could map this but abbrev is sufficient
                "count": bucket["doc_count"],
            })

        total_genes = response.get("aggregations", {}).get("unique_genes", {}).get("value", 0)

        return {
            "total_genes": total_genes,
            "categories": category_stats,
            "organisms": organism_stats,
        }

    except Exception as e:
        logger.error(f"Elasticsearch virulence stats failed: {e}")
        return None
