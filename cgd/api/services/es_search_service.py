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


def _build_quick_search_query(query: str, size: int = 100) -> dict:
    """
    Build Elasticsearch query for quick search.

    This is a RESTRICTIVE search matching the old Perl CGD quick search behavior:
    - Gene names, aliases, ORF names (NOT descriptions/abstracts)
    - GO term names and synonyms (NOT definitions)
    - Phenotype observables
    - Exact matches on: PubMed ID, GOID, External ID, CGDID
    - Ortholog names

    For full-text search across all content, use the /text endpoint instead.
    """
    query_upper = query.upper()
    query_lower = query.lower()

    # Build should clauses
    should_clauses = [
        # === EXACT ID MATCHES (highest priority) ===
        # CGDID (CAL0001234)
        {"term": {"dbxref_id": {"value": query_upper, "boost": 20}}},
        # GOID (GO:0001234)
        {"term": {"goid": {"value": query_upper, "boost": 20}}},
        # External ID
        {"term": {"external_id": {"value": query_upper, "boost": 15}}},
        {"term": {"external_id": {"value": query_lower, "boost": 15}}},

        # === GENE NAME MATCHES ===
        # Exact gene name match
        {"term": {"gene_name.keyword": {"value": query_upper, "boost": 15}}},
        {"term": {"feature_name": {"value": query_upper, "boost": 15}}},
        # Prefix match on gene names
        {"prefix": {"gene_name.keyword": {"value": query_upper, "boost": 10}}},
        {"prefix": {"feature_name": {"value": query_upper, "boost": 10}}},
        # Search in aliases
        {"match": {"aliases": {"query": query, "boost": 8}}},

        # === GO TERM MATCHES ===
        # Prefix match on GO term names
        {"prefix": {"go_term.keyword": {"value": query_lower, "boost": 8}}},
        # Search GO term and synonyms (but NOT definitions)
        {
            "multi_match": {
                "query": query,
                "fields": ["go_term^3", "go_synonyms^2"],
                "type": "phrase_prefix",
                "boost": 5,
            }
        },

        # === PHENOTYPE MATCHES ===
        {"prefix": {"observable.keyword": {"value": query_lower, "boost": 8}}},
        {"match": {"observable": {"query": query, "boost": 5}}},

        # === ORTHOLOG MATCHES (search by ortholog name) ===
        {"term": {"ortholog_name.keyword": {"value": query_upper, "boost": 10}}},
        {"prefix": {"ortholog_name.keyword": {"value": query_upper, "boost": 8}}},
        {"match": {"ortholog_name": {"query": query, "boost": 5}}},
    ]

    # Only add PubMed ID search if query is numeric
    if query.isdigit():
        should_clauses.append({"term": {"pubmed": {"value": int(query), "boost": 20}}})

    return {
        "query": {
            "bool": {
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
                "go_synonyms": {},
                "observable": {},
                "ortholog_name": {},
            },
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"],
        },
        "aggs": {
            "by_type": {
                "terms": {"field": "type", "size": 20}
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


def quick_search(
    es: Elasticsearch,
    query: str,
    limit: int = 20,
) -> SearchResponse:
    """
    Quick search across all categories using Elasticsearch.

    Returns results grouped by category with counts.
    """
    # Build and execute ES query (restrictive quick search, not full-text)
    es_query = _build_quick_search_query(query, size=limit * 5)  # Fetch extra for filtering

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
        "orthologs": [],
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
            "orthologs": "orthologs",
        }
        cat_key = category_map.get(result.category, result.category)

        if cat_key in results_by_category and len(results_by_category[cat_key]) < limit:
            results_by_category[cat_key].append(result)

    # Sort genes and orthologs by organism priority
    for cat in ["genes", "orthologs"]:
        if results_by_category[cat]:
            results_by_category[cat].sort(
                key=lambda r: (_get_organism_priority(r.organism), r.name or '')
            )

    # Get counts from aggregations
    counts_by_category: dict[str, int] = {}
    type_to_category = {
        "gene": "genes",
        "go_term": "go_terms",
        "phenotype": "phenotypes",
        "reference": "references",
        "ortholog": "orthologs",
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
        "go_term": ["go_term^3", "goid^2", "go_definition", "go_synonyms"],
        "phenotype": ["observable^3"],
        "reference": ["citation^2", "title", "abstract"],
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
                "go_synonyms": {},
                "observable": {},
                "citation": {},
                "aliases": {},
                "abstract": {},
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

        name = f"PMID:{pubmed}" if pubmed else dbxref_id or ""

        # Use abstract for abstracts category, citation otherwise
        if category == "abstracts" and abstract:
            match_context = _extract_highlight(highlights, "abstract", None)
            if not match_context:
                match_context = _highlight_text(abstract[:300] + "..." if len(abstract) > 300 else abstract, query)
        else:
            match_context = _extract_highlight(highlights, "citation", None)
            if not match_context and citation:
                match_context = _highlight_text(citation, query)

        return TextSearchResult(
            category="abstracts",
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
                    "name_description",
                    "go_term^3",
                    "go_definition",
                    "go_synonyms",
                    "observable^3",
                    "citation",
                    "abstract",
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
                ],
                "type": "best_fields",
                "fuzziness": "AUTO",
            }
        },
        "size": size_per_type * 15,  # Fetch extra to distribute across all categories
        "highlight": {
            "fields": {
                "name": {},
                "gene_name": {},
                "headline": {},
                "go_term": {},
                "go_definition": {},
                "go_synonyms": {},
                "observable": {},
                "citation": {},
                "aliases": {},
                "abstract": {},
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
        "aggs": {
            "by_type": {
                "terms": {"field": "type", "size": 20}
            }
        },
    }


def text_search(
    es: Elasticsearch,
    query: str,
    limit: int = 10,
) -> Optional[TextSearchResponse]:
    """
    Text search across all ES-indexed categories.

    Returns results for all categories now that ES indexes everything.
    Returns None on error (caller should fall back to Oracle).
    """
    es_query = _build_text_search_query(query, limit)

    try:
        response = es.search(index=INDEX_NAME, body=es_query)
    except Exception as e:
        logger.error(f"Elasticsearch text search failed: {e}")
        return None

    # Group results by category - all categories now supported
    results_by_category: dict[str, list[TextSearchResult]] = {
        "genes": [],
        "descriptions": [],
        "go_terms": [],
        "phenotypes": [],
        "abstracts": [],
        "paragraphs": [],
        "authors": [],
        "colleagues": [],
        "pathways": [],
        "notes": [],
        "external_ids": [],
        "orthologs": [],
        "literature_topics": [],
        "name_descriptions": [],
    }

    for hit in response["hits"]["hits"]:
        doc_type = hit["_source"].get("type")

        if doc_type == "gene":
            # Add to genes category
            gene_result = _parse_text_search_result(hit, query, "genes")
            if len(results_by_category["genes"]) < limit:
                results_by_category["genes"].append(gene_result)

            # Check if headline contains the query for descriptions category
            headline = hit["_source"].get("headline", "")
            if headline and query.lower() in headline.lower():
                desc_result = _parse_text_search_result(hit, query, "descriptions")
                if len(results_by_category["descriptions"]) < limit:
                    results_by_category["descriptions"].append(desc_result)

            # Check if name_description contains the query
            name_desc = hit["_source"].get("name_description", "")
            if name_desc and query.lower() in name_desc.lower():
                nd_result = _parse_text_search_result(hit, query, "name_descriptions")
                if len(results_by_category["name_descriptions"]) < limit:
                    results_by_category["name_descriptions"].append(nd_result)

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

        elif doc_type == "paragraph":
            result = _parse_text_search_result(hit, query, "paragraphs")
            if len(results_by_category["paragraphs"]) < limit:
                results_by_category["paragraphs"].append(result)

        elif doc_type == "author":
            result = _parse_text_search_result(hit, query, "authors")
            if len(results_by_category["authors"]) < limit:
                results_by_category["authors"].append(result)

        elif doc_type == "colleague":
            result = _parse_text_search_result(hit, query, "colleagues")
            if len(results_by_category["colleagues"]) < limit:
                results_by_category["colleagues"].append(result)

        elif doc_type == "pathway":
            result = _parse_text_search_result(hit, query, "pathways")
            if len(results_by_category["pathways"]) < limit:
                results_by_category["pathways"].append(result)

        elif doc_type == "note":
            result = _parse_text_search_result(hit, query, "notes")
            if len(results_by_category["notes"]) < limit:
                results_by_category["notes"].append(result)

        elif doc_type == "external_id":
            result = _parse_text_search_result(hit, query, "external_ids")
            if len(results_by_category["external_ids"]) < limit:
                results_by_category["external_ids"].append(result)

        elif doc_type == "ortholog":
            result = _parse_text_search_result(hit, query, "orthologs")
            if len(results_by_category["orthologs"]) < limit:
                results_by_category["orthologs"].append(result)

        elif doc_type == "literature_topic":
            result = _parse_text_search_result(hit, query, "literature_topics")
            if len(results_by_category["literature_topics"]) < limit:
                results_by_category["literature_topics"].append(result)

    # Sort gene-related categories by organism priority
    for cat in ["genes", "descriptions", "paragraphs", "notes", "external_ids", "orthologs", "name_descriptions"]:
        if results_by_category[cat]:
            results_by_category[cat].sort(
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

    # Special handling for certain gene-based categories
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
    elif category == "name_descriptions":
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "gene"}},
                        {"match": {"name_description": query}},
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
