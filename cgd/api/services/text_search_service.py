"""
Text Search Service - handles comprehensive text search across all CGD categories.

This service implements the text search functionality previously handled by
the legacy Perl TextSearch.pm and TextSearchPage.pm modules.
"""
from __future__ import annotations

import re
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import func, or_, and_

from cgd.schemas.search_schema import (
    TextSearchResult,
    TextSearchCategoryResult,
    TextSearchResponse,
    TextSearchCategoryPagedResponse,
    SearchResultLink,
)
from cgd.models.models import (
    Feature,
    FeatRelationship,
    Go,
    GoSynonym,
    GoGosyn,
    Phenotype,
    Reference,
    Alias,
    FeatAlias,
    Organism,
    Colleague,
    Author,
    AuthorEditor,
    Paragraph,
    FeatPara,
    Abstract,
    Note,
    NoteLink,
    Dbxref,
    DbxrefFeat,
    HomologyGroup,
    DbxrefHomology,
    FeatHomology,
    RefProperty,
    RefUrl,
)


# Category display names for the frontend
CATEGORY_DISPLAY_NAMES = {
    "genes": "Genes / Loci",
    "descriptions": "Locus Descriptions",
    "go_terms": "GO Terms",
    "colleagues": "Colleagues",
    "authors": "Authors",
    "pathways": "Pathways",
    "paragraphs": "Locus Summary Paragraphs",
    "abstracts": "Paper Abstracts",
    "name_descriptions": "Gene Name Descriptions",
    "phenotypes": "Phenotypes",
    "notes": "History Notes",
    "external_ids": "External Database IDs",
    "orthologs": "Orthologs / Best Hits",
    "literature_topics": "Literature Topics",
}

# Order in which categories are displayed
CATEGORY_ORDER = [
    "genes", "descriptions", "go_terms", "colleagues", "authors",
    "pathways", "paragraphs", "abstracts", "name_descriptions",
    "phenotypes", "notes", "external_ids", "orthologs", "literature_topics"
]

# Ortholog sources (from other MODs)
ORTHOLOG_SOURCES = ["SGD", "POMBASE", "AspGD", "CGD"]


def _normalize_query(query: str) -> str:
    """
    Normalize search query:
    - Strip whitespace
    - Convert wildcards (* to %)
    """
    normalized = query.strip()
    normalized = normalized.replace('*', '%')
    return normalized


def _get_like_pattern(query: str) -> str:
    """Get pattern for LIKE search (case-insensitive)."""
    normalized = _normalize_query(query)
    if '%' not in normalized:
        return f'%{normalized}%'
    return normalized


def _parse_search_terms(query: str) -> list[str]:
    """
    Parse search query into individual terms.
    Splits on whitespace, but keeps quoted phrases together.
    """
    import shlex
    try:
        # Use shlex to handle quoted strings
        terms = shlex.split(query.strip())
    except ValueError:
        # If shlex fails (e.g., unbalanced quotes), fall back to simple split
        terms = query.strip().split()
    return [t for t in terms if t]


def _build_multi_term_filter(column, query: str, match_mode: str = "all"):
    """
    Build a SQLAlchemy filter for multi-term search.

    Args:
        column: SQLAlchemy column to search
        query: Search query string (may contain multiple terms)
        match_mode: "all" (AND) or "any" (OR)

    Returns:
        SQLAlchemy filter expression
    """
    terms = _parse_search_terms(query)
    if not terms:
        return None

    # Build LIKE patterns for each term
    conditions = []
    for term in terms:
        pattern = _get_like_pattern(term)
        upper_pattern = pattern.upper()
        conditions.append(func.upper(column).like(upper_pattern))

    if len(conditions) == 1:
        return conditions[0]

    if match_mode == "any":
        return or_(*conditions)
    else:  # "all" is the default
        return and_(*conditions)


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
    Supports multi-term queries by highlighting each term.
    """
    if not text or not query:
        return text

    # Parse into individual terms
    terms = _parse_search_terms(query)
    if not terms:
        return text

    result = text
    for term in terms:
        clean_term = term.strip().replace('*', '').replace('%', '')
        if not clean_term:
            continue
        pattern = re.compile(re.escape(clean_term), re.IGNORECASE)

        def replacer(match):
            return f"<mark>{match.group(0)}</mark>"

        result = pattern.sub(replacer, result)

    return result


def _truncate_text(text: Optional[str], max_length: int = 300) -> Optional[str]:
    """Truncate text to max_length with ellipsis."""
    if not text:
        return text
    if len(text) <= max_length:
        return text
    return text[:max_length] + "..."


def _extract_context_around_match(
    text: Optional[str],
    query: str,
    context_chars: int = 100
) -> Optional[str]:
    """
    Extract text context around the matching keyword.

    Finds the first occurrence of the query in the text and extracts
    characters before and after it, adding ellipsis if truncated.

    Args:
        text: The full text to search in
        query: The search query
        context_chars: Number of characters to show before and after match

    Returns:
        Extracted context with ellipsis, or truncated text if no match found
    """
    if not text or not query:
        return _truncate_text(text, context_chars * 2)

    # Clean the query (remove wildcards)
    clean_query = query.strip().replace('*', '').replace('%', '')
    if not clean_query:
        return _truncate_text(text, context_chars * 2)

    # Find the match position (case-insensitive)
    text_lower = text.lower()
    query_lower = clean_query.lower()
    match_pos = text_lower.find(query_lower)

    if match_pos == -1:
        # No match found, just truncate from beginning
        return _truncate_text(text, context_chars * 2)

    # Calculate start and end positions for context
    start = max(0, match_pos - context_chars)
    end = min(len(text), match_pos + len(clean_query) + context_chars)

    # Extract the context
    context = text[start:end]

    # Add ellipsis if we truncated
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(text) else ""

    return f"{prefix}{context}{suffix}"


def _build_citation_links_for_search(ref, ref_urls=None) -> list[SearchResultLink]:
    """
    Build citation links for a reference in text search results.

    Generates links for:
    - CGD Paper (internal link to reference page)
    - PubMed (external link to NCBI PubMed)
    - Full Text (if available)

    Args:
        ref: Reference object with pubmed and dbxref_id
        ref_urls: List of RefUrl objects linked to this reference

    Returns:
        List of SearchResultLink objects
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

    # Process URLs from ref_url relationship
    if ref_urls:
        for ref_url in ref_urls:
            url_obj = ref_url.url
            if url_obj and url_obj.url:
                url_type = (url_obj.url_type or "").lower()

                # Full Text / LINKOUT
                if "full text" in url_type or "linkout" in url_type:
                    links.append(SearchResultLink(
                        name="Full Text",
                        url=url_obj.url,
                        link_type="external"
                    ))
                    break  # Only add one full text link

    return links


# =============================================================================
# Assembly 21/22 Deduplication Helper
# =============================================================================

def _get_assembly21_feature_nos_to_exclude(db: Session, feature_nos: set[int]) -> set[int]:
    """
    Identify Assembly 21 features that have Assembly 22 equivalents.

    In CGD, the same gene may have both Assembly 21 (orf19.XXXX) and
    Assembly 22 (C7_XXXXX_A) feature records. We prefer Assembly 22.

    The relationship is stored in FeatRelationship where:
    - child_feature_no = Assembly 21 feature
    - parent_feature_no = Assembly 22 feature
    - relationship_type = 'Assembly 21 Primary Allele'
    - rank = 3

    This also excludes alleles of Assembly 21 features (e.g., orf19.8514 is an
    allele of orf19.895, and should also be excluded when orf19.895 has an
    Assembly 22 equivalent).

    Args:
        db: Database session
        feature_nos: Set of feature_no values to check

    Returns:
        Set of feature_no values that are Assembly 21 versions with
        Assembly 22 equivalents (these should be excluded from results)
    """
    if not feature_nos:
        return set()

    to_exclude = set()

    # Find Assembly 21 features that have Assembly 22 parents
    a21_relationships = (
        db.query(FeatRelationship.child_feature_no)
        .filter(
            FeatRelationship.child_feature_no.in_(feature_nos),
            FeatRelationship.relationship_type == 'Assembly 21 Primary Allele',
            FeatRelationship.rank == 3,
        )
        .all()
    )
    direct_a21_features = {rel[0] for rel in a21_relationships}
    to_exclude.update(direct_a21_features)

    # Also find alleles of Assembly 21 features that have Assembly 22 equivalents
    # These are features where parent has 'Assembly 21 Primary Allele' relationship
    # and the child is an allele (relationship_type='allele')
    if feature_nos:
        # Find features in our set that are alleles of any feature
        allele_relationships = (
            db.query(FeatRelationship.child_feature_no, FeatRelationship.parent_feature_no)
            .filter(
                FeatRelationship.child_feature_no.in_(feature_nos),
                FeatRelationship.relationship_type == 'allele',
                FeatRelationship.rank == 3,
            )
            .all()
        )

        # Check if parent features have Assembly 22 equivalents
        parent_feature_nos = {rel[1] for rel in allele_relationships}
        if parent_feature_nos:
            parents_with_a22 = (
                db.query(FeatRelationship.child_feature_no)
                .filter(
                    FeatRelationship.child_feature_no.in_(parent_feature_nos),
                    FeatRelationship.relationship_type == 'Assembly 21 Primary Allele',
                    FeatRelationship.rank == 3,
                )
                .all()
            )
            parents_to_exclude = {rel[0] for rel in parents_with_a22}

            # Exclude alleles whose parents have Assembly 22 equivalents
            for child_no, parent_no in allele_relationships:
                if parent_no in parents_to_exclude:
                    to_exclude.add(child_no)

    return to_exclude


# =============================================================================
# Individual Category Search Functions
# =============================================================================

def _get_a21_exclusion_subquery(db: Session):
    """
    Build a subquery that returns all feature_nos to exclude:
    - Direct Assembly 21 features (have 'Assembly 21 Primary Allele' relationship)
    - Alleles of Assembly 21 features (parent has 'Assembly 21 Primary Allele' relationship)

    Returns a subquery that can be used with ~Feature.feature_no.in_(subquery)
    """
    # Direct Assembly 21 features
    direct_a21 = (
        db.query(FeatRelationship.child_feature_no.label('feature_no'))
        .filter(
            FeatRelationship.relationship_type == 'Assembly 21 Primary Allele',
            FeatRelationship.rank == 3,
        )
    )

    # Alleles of Assembly 21 features
    # Join allele relationships with A21 relationships to find alleles whose parents are A21
    alleles_of_a21 = (
        db.query(FeatRelationship.child_feature_no.label('feature_no'))
        .filter(
            FeatRelationship.relationship_type == 'allele',
            FeatRelationship.rank == 3,
            FeatRelationship.parent_feature_no.in_(
                db.query(FeatRelationship.child_feature_no)
                .filter(
                    FeatRelationship.relationship_type == 'Assembly 21 Primary Allele',
                    FeatRelationship.rank == 3,
                )
            )
        )
    )

    # Union both
    return direct_a21.union(alleles_of_a21).subquery()


def search_genes(db: Session, query: str, limit: int = 20) -> list[TextSearchResult]:
    """
    Search genes/loci by gene_name, feature_name, dbxref_id, or aliases.
    Returns TextSearchResult list with category="genes".

    Note: Filters out Assembly 21 features that have Assembly 22 equivalents
    to avoid duplicate results for the same gene.
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Subquery to get Assembly 21 feature_nos to exclude (includes alleles)
    a21_subq = _get_a21_exclusion_subquery(db)

    # Search in Feature table: gene_name, feature_name, dbxref_id
    # Exclude Assembly 21 features directly in SQL
    feature_query = (
        db.query(Feature)
        .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
        .filter(
            or_(
                func.upper(Feature.gene_name).like(upper_pattern),
                func.upper(Feature.feature_name).like(upper_pattern),
                func.upper(Feature.dbxref_id).like(upper_pattern),
            ),
            ~Feature.feature_no.in_(db.query(a21_subq.c.feature_no))
        )
        .limit(limit)
    )

    # Collect features from direct match
    found_feature_nos = set()
    for feat in feature_query:
        found_feature_nos.add(feat.feature_no)
        display_name = feat.gene_name or feat.feature_name
        results.append(TextSearchResult(
            category="genes",
            id=feat.dbxref_id,
            name=display_name,
            description=feat.headline,
            link=f"/locus/{feat.gene_name or feat.feature_name}",
            organism=_get_organism_name(feat.organism),
            highlighted_name=_highlight_text(display_name, query),
            highlighted_description=_highlight_text(feat.headline, query),
        ))

    # Search aliases if we need more results
    remaining = limit - len(results)
    if remaining > 0:
        alias_query = (
            db.query(Feature, Alias)
            .join(FeatAlias, Feature.feature_no == FeatAlias.feature_no)
            .join(Alias, FeatAlias.alias_no == Alias.alias_no)
            .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
            .filter(
                func.upper(Alias.alias_name).like(upper_pattern),
                ~Feature.feature_no.in_(db.query(a21_subq.c.feature_no))
            )
            .limit(remaining + len(found_feature_nos))  # Extra to account for duplicates
        )

        for feat, alias in alias_query:
            if feat.feature_no not in found_feature_nos:
                found_feature_nos.add(feat.feature_no)
                display_name = feat.gene_name or feat.feature_name
                description = f"Alias: {alias.alias_name}"
                if feat.headline:
                    description += f" - {feat.headline}"
                results.append(TextSearchResult(
                    category="genes",
                    id=feat.dbxref_id,
                    name=display_name,
                    description=description,
                    link=f"/locus/{feat.gene_name or feat.feature_name}",
                    organism=_get_organism_name(feat.organism),
                    highlighted_name=_highlight_text(display_name, query),
                    highlighted_description=_highlight_text(description, query),
                ))
                if len(results) >= limit:
                    break

    return results


def search_descriptions(db: Session, query: str, limit: int = 20) -> list[TextSearchResult]:
    """
    Search locus descriptions (headline field).
    Returns TextSearchResult list with category="descriptions".

    Note: Filters out Assembly 21 features that have Assembly 22 equivalents
    to avoid duplicate results for the same gene.
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Subquery to get Assembly 21 feature_nos to exclude (includes alleles)
    a21_subq = _get_a21_exclusion_subquery(db)

    # Query with Assembly 21 exclusion built into the SQL
    feature_query = (
        db.query(Feature)
        .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
        .filter(
            func.upper(Feature.headline).like(upper_pattern),
            ~Feature.feature_no.in_(db.query(a21_subq.c.feature_no))
        )
        .limit(limit)
    )

    for feat in feature_query:
        display_name = feat.gene_name or feat.feature_name
        results.append(TextSearchResult(
            category="descriptions",
            id=feat.dbxref_id,
            name=display_name,
            description=feat.headline,
            link=f"/locus/{feat.gene_name or feat.feature_name}",
            organism=_get_organism_name(feat.organism),
            highlighted_name=_highlight_text(display_name, query),
            highlighted_description=_highlight_text(feat.headline, query),
        ))

    return results


def search_go_terms(db: Session, query: str, limit: int = 20) -> list[TextSearchResult]:
    """
    Search GO terms by go_term or go_synonym.
    Returns TextSearchResult list with category="go_terms".
    """
    results = []
    normalized = _normalize_query(query)
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

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

    found_go_nos = set()

    # If it's a valid GO ID, search exact match first
    if goid_numeric is not None:
        go_exact = db.query(Go).filter(Go.goid == goid_numeric).first()
        if go_exact:
            description = _truncate_text(go_exact.go_definition, 200)
            results.append(TextSearchResult(
                category="go_terms",
                id=_format_goid(go_exact.goid),
                name=go_exact.go_term,
                description=description,
                link=f"/go/{_format_goid(go_exact.goid)}",
                highlighted_name=_highlight_text(go_exact.go_term, query),
                highlighted_description=_highlight_text(description, query),
            ))
            found_go_nos.add(go_exact.go_no)

    # Search by term name
    remaining = limit - len(results)
    if remaining > 0:
        go_query = (
            db.query(Go)
            .filter(func.upper(Go.go_term).like(upper_pattern))
            .limit(remaining + len(found_go_nos))
        )

        for go in go_query:
            if go.go_no not in found_go_nos:
                description = _truncate_text(go.go_definition, 200)
                results.append(TextSearchResult(
                    category="go_terms",
                    id=_format_goid(go.goid),
                    name=go.go_term,
                    description=description,
                    link=f"/go/{_format_goid(go.goid)}",
                    highlighted_name=_highlight_text(go.go_term, query),
                    highlighted_description=_highlight_text(description, query),
                ))
                found_go_nos.add(go.go_no)
                if len(results) >= limit:
                    break

    # Search by synonym
    remaining = limit - len(results)
    if remaining > 0:
        synonym_query = (
            db.query(Go, GoSynonym)
            .join(GoGosyn, Go.go_no == GoGosyn.go_no)
            .join(GoSynonym, GoGosyn.go_synonym_no == GoSynonym.go_synonym_no)
            .filter(func.upper(GoSynonym.go_synonym).like(upper_pattern))
            .limit(remaining + len(found_go_nos))
        )

        for go, synonym in synonym_query:
            if go.go_no not in found_go_nos:
                description = f"Synonym: {synonym.go_synonym}"
                results.append(TextSearchResult(
                    category="go_terms",
                    id=_format_goid(go.goid),
                    name=go.go_term,
                    description=description,
                    link=f"/go/{_format_goid(go.goid)}",
                    highlighted_name=_highlight_text(go.go_term, query),
                    highlighted_description=_highlight_text(description, query),
                ))
                found_go_nos.add(go.go_no)
                if len(results) >= limit:
                    break

    return results[:limit]


def search_colleagues(db: Session, query: str, limit: int = 20) -> list[TextSearchResult]:
    """
    Search colleagues by last_name or other_last_name.
    Returns TextSearchResult list with category="colleagues".
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    colleague_query = (
        db.query(Colleague)
        .filter(
            or_(
                func.upper(Colleague.last_name).like(upper_pattern),
                func.upper(Colleague.other_last_name).like(upper_pattern),
            )
        )
        .limit(limit)
    )

    for colleague in colleague_query:
        display_name = f"{colleague.first_name} {colleague.last_name}"
        if colleague.suffix:
            display_name += f", {colleague.suffix}"

        description_parts = []
        if colleague.institution:
            description_parts.append(colleague.institution)
        if colleague.city:
            description_parts.append(colleague.city)
        if colleague.country:
            description_parts.append(colleague.country)
        description = ", ".join(description_parts) if description_parts else None

        results.append(TextSearchResult(
            category="colleagues",
            id=str(colleague.colleague_no),
            name=display_name,
            description=description,
            link=f"/colleague/{colleague.colleague_no}",
            highlighted_name=_highlight_text(display_name, query),
            highlighted_description=_highlight_text(description, query),
        ))

    return results


def search_authors(db: Session, query: str, limit: int = 20) -> list[TextSearchResult]:
    """
    Search authors by author_name and link to their references.
    Returns TextSearchResult list with category="authors".
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Find authors matching the query
    author_query = (
        db.query(Author)
        .filter(func.upper(Author.author_name).like(upper_pattern))
        .limit(limit * 2)  # Get extra since we'll deduplicate by reference
    )

    seen_refs = set()

    for author in author_query:
        # Get references for this author
        author_refs = (
            db.query(AuthorEditor, Reference)
            .join(Reference, AuthorEditor.reference_no == Reference.reference_no)
            .filter(AuthorEditor.author_no == author.author_no)
            .order_by(AuthorEditor.author_order)
            .limit(5)  # Limit refs per author
        )

        for ae, ref in author_refs:
            if ref.reference_no not in seen_refs and len(results) < limit:
                name = f"PMID:{ref.pubmed}" if ref.pubmed else ref.dbxref_id
                description = f"Author: {author.author_name}"
                if ref.citation:
                    description += f" - {_truncate_text(ref.citation, 150)}"

                results.append(TextSearchResult(
                    category="authors",
                    id=ref.dbxref_id,
                    name=name,
                    description=description,
                    link=f"/reference/{ref.dbxref_id}",
                    highlighted_name=_highlight_text(name, query),
                    highlighted_description=_highlight_text(description, query),
                ))
                seen_refs.add(ref.reference_no)

        if len(results) >= limit:
            break

    return results[:limit]


def search_pathways(db: Session, query: str, limit: int = 20) -> list[TextSearchResult]:
    """
    Search pathways (Dbxref with source='CalbiCyc').
    Returns TextSearchResult list with category="pathways".
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Pathways are in Dbxref with source='CalbiCyc'
    pathway_query = (
        db.query(Dbxref, Feature)
        .join(DbxrefFeat, Dbxref.dbxref_no == DbxrefFeat.dbxref_no)
        .join(Feature, DbxrefFeat.feature_no == Feature.feature_no)
        .filter(
            Dbxref.source == 'CalbiCyc',
            func.upper(Dbxref.description).like(upper_pattern)
        )
        .limit(limit)
    )

    seen_pathways = set()
    for dbxref, feat in pathway_query:
        if dbxref.dbxref_no not in seen_pathways:
            # Build pathway URL (external CalbiCyc link)
            pathway_url = f"http://pathway.stanford.edu/cgd/new-image?object={dbxref.dbxref_id}"

            display_name = dbxref.description or dbxref.dbxref_id
            gene_name = feat.gene_name or feat.feature_name

            results.append(TextSearchResult(
                category="pathways",
                id=dbxref.dbxref_id,
                name=display_name,
                description=f"Gene: {gene_name}",
                link=pathway_url,
                highlighted_name=_highlight_text(display_name, query),
                highlighted_description=_highlight_text(f"Gene: {gene_name}", query),
            ))
            seen_pathways.add(dbxref.dbxref_no)

    return results[:limit]


def search_paragraphs(db: Session, query: str, limit: int = 20) -> list[TextSearchResult]:
    """
    Search locus summary paragraphs.
    Returns TextSearchResult list with category="paragraphs".
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    para_query = (
        db.query(Paragraph, Feature)
        .join(FeatPara, Paragraph.paragraph_no == FeatPara.paragraph_no)
        .join(Feature, FeatPara.feature_no == Feature.feature_no)
        .filter(func.upper(Paragraph.paragraph_text).like(upper_pattern))
        .limit(limit)
    )

    for para, feat in para_query:
        display_name = feat.gene_name or feat.feature_name
        # Extract context around matching keyword
        description = _extract_context_around_match(para.paragraph_text, query, 120)

        results.append(TextSearchResult(
            category="paragraphs",
            id=feat.dbxref_id,
            name=display_name,
            description=description,
            link=f"/locus/{feat.gene_name or feat.feature_name}#summaryParagraph",
            organism=_get_organism_name(feat.organism) if hasattr(feat, 'organism') else None,
            highlighted_name=_highlight_text(display_name, query),
            highlighted_description=_highlight_text(description, query),
        ))

    return results


def search_abstracts(
    db: Session,
    query: str,
    limit: int = 20,
    search_field: str = "both",
    match_mode: str = "all"
) -> list[TextSearchResult]:
    """
    Search paper abstracts and/or titles.

    Args:
        db: Database session
        query: Search query string
        limit: Maximum results to return
        search_field: "title", "abstract", or "both" (default)
        match_mode: "all" (AND) or "any" (OR) for multi-term queries

    Returns TextSearchResult list with category="abstracts".
    """
    results = []

    # Build the base query joining Abstract and Reference
    base_query = (
        db.query(Abstract, Reference)
        .join(Reference, Abstract.reference_no == Reference.reference_no)
    )

    # Build filter based on search_field and match_mode
    if search_field == "title":
        # Search only in title
        title_filter = _build_multi_term_filter(Reference.title, query, match_mode)
        if title_filter is None:
            return results
        abstract_query = base_query.filter(title_filter).limit(limit)
    elif search_field == "abstract":
        # Search only in abstract
        abstract_filter = _build_multi_term_filter(Abstract.abstract, query, match_mode)
        if abstract_filter is None:
            return results
        abstract_query = base_query.filter(abstract_filter).limit(limit)
    else:  # "both" - search in either title or abstract
        title_filter = _build_multi_term_filter(Reference.title, query, match_mode)
        abstract_filter = _build_multi_term_filter(Abstract.abstract, query, match_mode)
        if title_filter is None or abstract_filter is None:
            return results
        # Match in title OR abstract (document matches if either field matches the terms)
        abstract_query = base_query.filter(or_(title_filter, abstract_filter)).limit(limit)

    for abstract, ref in abstract_query:
        # Use citation as name (plain text, no link)
        name = ref.citation or f"PMID:{ref.pubmed}" if ref.pubmed else ref.dbxref_id

        # Build description based on search_field
        if search_field == "title":
            # Show the full title when searching by title
            description = f"Title: {ref.title}" if ref.title else None
        elif search_field == "abstract":
            # Show abstract snippet
            description = _extract_context_around_match(abstract.abstract, query, 120)
        else:  # "both"
            # Show title if it matches, otherwise show abstract snippet
            title_matches = False
            if ref.title:
                terms = _parse_search_terms(query)
                for term in terms:
                    if term.lower() in ref.title.lower():
                        title_matches = True
                        break
            if title_matches:
                description = f"Title: {ref.title}"
            else:
                description = _extract_context_around_match(abstract.abstract, query, 120)

        # Use PMID as ID if available, otherwise use dbxref_id
        display_id = f"PMID:{ref.pubmed}" if ref.pubmed else ref.dbxref_id

        # Get ref_url for building links
        ref_urls = (
            db.query(RefUrl)
            .filter(RefUrl.reference_no == ref.reference_no)
            .all()
        )
        links = _build_citation_links_for_search(ref, ref_urls)

        results.append(TextSearchResult(
            category="abstracts",
            id=display_id,
            name=name,
            description=description,
            link=None,  # No link on citation - use links array instead
            links=links,
            highlighted_name=_highlight_text(name, query),
            highlighted_description=_highlight_text(description, query),
        ))

    return results


def search_name_descriptions(db: Session, query: str, limit: int = 20) -> list[TextSearchResult]:
    """
    Search gene name descriptions (name_description field).
    Returns TextSearchResult list with category="name_descriptions".
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    feature_query = (
        db.query(Feature)
        .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
        .filter(
            Feature.name_description.isnot(None),
            func.upper(Feature.name_description).like(upper_pattern)
        )
        .limit(limit)
    )

    for feat in feature_query:
        display_name = feat.gene_name or feat.feature_name
        results.append(TextSearchResult(
            category="name_descriptions",
            id=feat.dbxref_id,
            name=display_name,
            description=feat.name_description,
            link=f"/locus/{feat.gene_name or feat.feature_name}",
            organism=_get_organism_name(feat.organism),
            highlighted_name=_highlight_text(display_name, query),
            highlighted_description=_highlight_text(feat.name_description, query),
        ))

    return results


def search_phenotypes(db: Session, query: str, limit: int = 20) -> list[TextSearchResult]:
    """
    Search phenotypes by observable.
    Returns TextSearchResult list with category="phenotypes".
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    pheno_query = (
        db.query(Phenotype.observable)
        .filter(func.upper(Phenotype.observable).like(upper_pattern))
        .distinct()
        .limit(limit)
    )

    for (observable,) in pheno_query:
        results.append(TextSearchResult(
            category="phenotypes",
            id=observable,
            name=observable,
            description=None,
            link=f"/phenotype/search?observable={observable}",
            highlighted_name=_highlight_text(observable, query),
            highlighted_description=None,
        ))

    return results


def search_notes(db: Session, query: str, limit: int = 20) -> list[TextSearchResult]:
    """
    Search notes (history notes) linked to features or references.
    Returns TextSearchResult list with category="notes".
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Search notes linked to features
    note_query = (
        db.query(Note, NoteLink)
        .join(NoteLink, Note.note_no == NoteLink.note_no)
        .filter(func.upper(Note.note).like(upper_pattern))
        .limit(limit)
    )

    for note, note_link in note_query:
        # Determine link based on tab_name
        link = None
        link_name = f"Note {note.note_no}"
        organism_name = None

        if note_link.tab_name.upper() == 'FEATURE':
            # Look up the feature with organism
            feat = (
                db.query(Feature)
                .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
                .filter(Feature.feature_no == note_link.primary_key)
                .first()
            )
            if feat:
                link = f"/locus/{feat.feature_name}"
                link_name = feat.gene_name or feat.feature_name
                organism_name = _get_organism_name(feat.organism)
        elif note_link.tab_name.upper() == 'REFERENCE':
            ref = db.query(Reference).filter(
                Reference.reference_no == note_link.primary_key
            ).first()
            if ref:
                link = f"/reference/{ref.dbxref_id}"
                link_name = f"PMID:{ref.pubmed}" if ref.pubmed else ref.dbxref_id

        if link:
            # Extract context around matching keyword
            description = _extract_context_around_match(note.note, query, 120)
            results.append(TextSearchResult(
                category="notes",
                id=str(note.note_no),
                name=link_name,
                description=description,
                link=link,
                organism=organism_name,
                match_context=note.note_type,
                highlighted_name=_highlight_text(link_name, query),
                highlighted_description=_highlight_text(description, query),
            ))

    return results[:limit]


def search_external_ids(db: Session, query: str, limit: int = 20) -> list[TextSearchResult]:
    """
    Search external database IDs (Dbxref linked to features).
    Returns TextSearchResult list with category="external_ids".
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Exclude pathway and ortholog sources
    excluded_sources = ['CalbiCyc'] + ORTHOLOG_SOURCES

    dbxref_query = (
        db.query(Dbxref, Feature)
        .join(DbxrefFeat, Dbxref.dbxref_no == DbxrefFeat.dbxref_no)
        .join(Feature, DbxrefFeat.feature_no == Feature.feature_no)
        .filter(
            func.upper(Dbxref.dbxref_id).like(upper_pattern),
            ~Dbxref.source.in_(excluded_sources)
        )
        .limit(limit)
    )

    for dbxref, feat in dbxref_query:
        display_name = feat.gene_name or feat.feature_name
        description = f"{dbxref.source}: {dbxref.dbxref_id}"
        if dbxref.description:
            description += f" - {dbxref.description}"

        results.append(TextSearchResult(
            category="external_ids",
            id=dbxref.dbxref_id,
            name=display_name,
            description=description,
            link=f"/locus/{feat.gene_name or feat.feature_name}",
            organism=_get_organism_name(feat.organism) if hasattr(feat, 'organism') else None,
            highlighted_name=_highlight_text(display_name, query),
            highlighted_description=_highlight_text(description, query),
        ))

    return results


def search_orthologs(db: Session, query: str, limit: int = 20) -> list[TextSearchResult]:
    """
    Search orthologs and best hits from other databases (SGD, POMBASE, AspGD, CGD).
    Returns TextSearchResult list with category="orthologs".
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Search in Dbxref where source is one of the ortholog sources
    # and dbxref_id or description matches
    ortholog_query = (
        db.query(Dbxref, Feature)
        .join(DbxrefFeat, Dbxref.dbxref_no == DbxrefFeat.dbxref_no)
        .join(Feature, DbxrefFeat.feature_no == Feature.feature_no)
        .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
        .filter(
            Dbxref.source.in_(ORTHOLOG_SOURCES),
            or_(
                func.upper(Dbxref.dbxref_id).like(upper_pattern),
                func.upper(Dbxref.description).like(upper_pattern),
            )
        )
        .limit(limit)
    )

    for dbxref, feat in ortholog_query:
        display_name = feat.gene_name or feat.feature_name
        ortholog_name = dbxref.description or dbxref.dbxref_id
        description = f"Ortholog: {ortholog_name} ({dbxref.source})"

        results.append(TextSearchResult(
            category="orthologs",
            id=feat.dbxref_id,
            name=display_name,
            description=description,
            link=f"/locus/{feat.gene_name or feat.feature_name}",
            organism=_get_organism_name(feat.organism) if hasattr(feat, 'organism') else None,
            highlighted_name=_highlight_text(display_name, query),
            highlighted_description=_highlight_text(description, query),
        ))

    return results[:limit]


# =============================================================================
# Count Functions for Pagination
# =============================================================================

def _count_genes(db: Session, query: str) -> int:
    """
    Count total genes matching the query.

    Note: Excludes Assembly 21 features that have Assembly 22 equivalents
    to avoid counting duplicates.

    Uses UNION of direct and alias matches to ensure consistent counting
    with the search_genes function.
    """
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Subquery for features matching directly (gene_name, feature_name, or dbxref_id)
    # Use label() to ensure column name is consistent in UNION
    direct_subq = (
        db.query(Feature.feature_no.label('fno'))
        .filter(
            or_(
                func.upper(Feature.gene_name).like(upper_pattern),
                func.upper(Feature.feature_name).like(upper_pattern),
                func.upper(Feature.dbxref_id).like(upper_pattern),
            )
        )
    )

    # Subquery for features matching via aliases
    alias_subq = (
        db.query(Feature.feature_no.label('fno'))
        .join(FeatAlias, Feature.feature_no == FeatAlias.feature_no)
        .join(Alias, FeatAlias.alias_no == Alias.alias_no)
        .filter(func.upper(Alias.alias_name).like(upper_pattern))
    )

    # Union of both to get all matching feature_nos (distinct)
    all_matches = direct_subq.union(alias_subq).subquery()

    # Subquery to get Assembly 21 feature_nos to exclude (includes alleles)
    a21_subq = _get_a21_exclusion_subquery(db)

    # Count distinct feature_nos, excluding Assembly 21 duplicates
    # Use labeled column name 'fno' from the UNION subquery
    total_count = (
        db.query(func.count(all_matches.c.fno))
        .filter(
            ~all_matches.c.fno.in_(db.query(a21_subq.c.feature_no))
        )
        .scalar()
    )

    return total_count or 0


def _count_genes_by_organism(db: Session, query: str) -> dict[str, int]:
    """
    Count genes matching the query grouped by organism.

    Returns a dict mapping organism name to count.
    """
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Subquery for features matching directly (gene_name, feature_name, or dbxref_id)
    direct_subq = (
        db.query(
            Feature.feature_no.label('fno'),
            Feature.organism_no.label('org_no')
        )
        .filter(
            or_(
                func.upper(Feature.gene_name).like(upper_pattern),
                func.upper(Feature.feature_name).like(upper_pattern),
                func.upper(Feature.dbxref_id).like(upper_pattern),
            )
        )
    )

    # Subquery for features matching via aliases
    alias_subq = (
        db.query(
            Feature.feature_no.label('fno'),
            Feature.organism_no.label('org_no')
        )
        .join(FeatAlias, Feature.feature_no == FeatAlias.feature_no)
        .join(Alias, FeatAlias.alias_no == Alias.alias_no)
        .filter(func.upper(Alias.alias_name).like(upper_pattern))
    )

    # Union of both to get all matching features with their organism_no
    all_matches = direct_subq.union(alias_subq).subquery()

    # Subquery to get Assembly 21 feature_nos to exclude (includes alleles)
    a21_subq = _get_a21_exclusion_subquery(db)

    # Join with Organism to get organism names and count by organism
    # Exclude Assembly 21 duplicates
    organism_counts = (
        db.query(
            Organism.organism_name,
            func.count(func.distinct(all_matches.c.fno))
        )
        .join(Organism, all_matches.c.org_no == Organism.organism_no)
        .filter(~all_matches.c.fno.in_(db.query(a21_subq.c.feature_no)))
        .group_by(Organism.organism_name)
        .all()
    )

    return {name: count for name, count in organism_counts if name}


def _count_descriptions(db: Session, query: str) -> int:
    """
    Count total description matches.

    Note: Excludes Assembly 21 features that have Assembly 22 equivalents
    to avoid counting duplicates.
    """
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Subquery to get Assembly 21 feature_nos to exclude (includes alleles)
    a21_subq = _get_a21_exclusion_subquery(db)

    return (
        db.query(func.count(Feature.feature_no))
        .filter(
            func.upper(Feature.headline).like(upper_pattern),
            ~Feature.feature_no.in_(db.query(a21_subq.c.feature_no))
        )
        .scalar()
    )


def _count_go_terms(db: Session, query: str) -> int:
    """Count total GO terms matching the query."""
    normalized = _normalize_query(query)
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Count by term name
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
            term_match = db.query(Go).filter(
                Go.goid == goid_numeric,
                func.upper(Go.go_term).like(upper_pattern)
            ).first()
            if not term_match:
                count += 1

    # Count synonym matches (distinct GO terms)
    synonym_count = (
        db.query(func.count(func.distinct(Go.go_no)))
        .join(GoGosyn, Go.go_no == GoGosyn.go_no)
        .join(GoSynonym, GoGosyn.go_synonym_no == GoSynonym.go_synonym_no)
        .filter(
            func.upper(GoSynonym.go_synonym).like(upper_pattern),
            ~func.upper(Go.go_term).like(upper_pattern)  # Exclude already counted
        )
        .scalar()
    )

    return count + synonym_count


def _count_colleagues(db: Session, query: str) -> int:
    """Count total colleagues matching the query."""
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    return (
        db.query(func.count(Colleague.colleague_no))
        .filter(
            or_(
                func.upper(Colleague.last_name).like(upper_pattern),
                func.upper(Colleague.other_last_name).like(upper_pattern),
            )
        )
        .scalar()
    )


def _count_authors(db: Session, query: str) -> int:
    """Count total references by matching authors."""
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Count distinct references for matching authors
    return (
        db.query(func.count(func.distinct(Reference.reference_no)))
        .join(AuthorEditor, Reference.reference_no == AuthorEditor.reference_no)
        .join(Author, AuthorEditor.author_no == Author.author_no)
        .filter(func.upper(Author.author_name).like(upper_pattern))
        .scalar()
    )


def _count_pathways(db: Session, query: str) -> int:
    """Count total pathways matching the query."""
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    return (
        db.query(func.count(func.distinct(Dbxref.dbxref_no)))
        .filter(
            Dbxref.source == 'CalbiCyc',
            func.upper(Dbxref.description).like(upper_pattern)
        )
        .scalar()
    )


def _count_paragraphs(db: Session, query: str) -> int:
    """Count total paragraphs matching the query."""
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    return (
        db.query(func.count(Paragraph.paragraph_no))
        .filter(func.upper(Paragraph.paragraph_text).like(upper_pattern))
        .scalar()
    )


def _count_abstracts(
    db: Session,
    query: str,
    search_field: str = "both",
    match_mode: str = "all"
) -> int:
    """
    Count total abstracts/titles matching the query.

    Args:
        db: Database session
        query: Search query string
        search_field: "title", "abstract", or "both" (default)
        match_mode: "all" (AND) or "any" (OR) for multi-term queries
    """
    base_query = (
        db.query(func.count(Abstract.reference_no))
        .join(Reference, Abstract.reference_no == Reference.reference_no)
    )

    if search_field == "title":
        title_filter = _build_multi_term_filter(Reference.title, query, match_mode)
        if title_filter is None:
            return 0
        return base_query.filter(title_filter).scalar() or 0
    elif search_field == "abstract":
        abstract_filter = _build_multi_term_filter(Abstract.abstract, query, match_mode)
        if abstract_filter is None:
            return 0
        return base_query.filter(abstract_filter).scalar() or 0
    else:  # "both"
        title_filter = _build_multi_term_filter(Reference.title, query, match_mode)
        abstract_filter = _build_multi_term_filter(Abstract.abstract, query, match_mode)
        if title_filter is None or abstract_filter is None:
            return 0
        return base_query.filter(or_(title_filter, abstract_filter)).scalar() or 0


def _count_name_descriptions(db: Session, query: str) -> int:
    """Count total name_description matches."""
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    return (
        db.query(func.count(Feature.feature_no))
        .filter(
            Feature.name_description.isnot(None),
            func.upper(Feature.name_description).like(upper_pattern)
        )
        .scalar()
    )


def _count_phenotypes(db: Session, query: str) -> int:
    """Count total distinct phenotype observables matching the query."""
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    return (
        db.query(func.count(func.distinct(Phenotype.observable)))
        .filter(func.upper(Phenotype.observable).like(upper_pattern))
        .scalar()
    )


def _count_notes(db: Session, query: str) -> int:
    """Count total notes matching the query."""
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    return (
        db.query(func.count(Note.note_no))
        .filter(func.upper(Note.note).like(upper_pattern))
        .scalar()
    )


def _count_external_ids(db: Session, query: str) -> int:
    """Count total external IDs matching the query."""
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    excluded_sources = ['CalbiCyc'] + ORTHOLOG_SOURCES

    return (
        db.query(func.count(func.distinct(Dbxref.dbxref_no)))
        .join(DbxrefFeat, Dbxref.dbxref_no == DbxrefFeat.dbxref_no)
        .filter(
            func.upper(Dbxref.dbxref_id).like(upper_pattern),
            ~Dbxref.source.in_(excluded_sources)
        )
        .scalar()
    )


def _count_orthologs(db: Session, query: str) -> int:
    """Count total orthologs matching the query."""
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    return (
        db.query(func.count(func.distinct(Feature.feature_no)))
        .join(DbxrefFeat, Feature.feature_no == DbxrefFeat.feature_no)
        .join(Dbxref, DbxrefFeat.dbxref_no == Dbxref.dbxref_no)
        .filter(
            Dbxref.source.in_(ORTHOLOG_SOURCES),
            or_(
                func.upper(Dbxref.dbxref_id).like(upper_pattern),
                func.upper(Dbxref.description).like(upper_pattern),
            )
        )
        .scalar()
    )


# =============================================================================
# Literature Topics Search
# =============================================================================

def search_literature_topics(db: Session, query: str, limit: int = 20) -> list[TextSearchResult]:
    """
    Search literature topics (RefProperty with property_type='literature_topic').
    Returns TextSearchResult list with category="literature_topics".
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    # Find literature topics matching the query
    topic_query = (
        db.query(RefProperty, Reference)
        .join(Reference, RefProperty.reference_no == Reference.reference_no)
        .filter(
            RefProperty.property_type == "literature_topic",
            func.upper(RefProperty.property_value).like(upper_pattern)
        )
        .order_by(RefProperty.property_value, Reference.year.desc())
        .limit(limit)
    )

    seen_refs = set()

    for prop, ref in topic_query:
        if ref.reference_no in seen_refs:
            continue
        seen_refs.add(ref.reference_no)

        # Use citation as name (plain text, no link), topic as description
        name = ref.citation or f"PMID:{ref.pubmed}" if ref.pubmed else ref.dbxref_id
        description = f"Topic: {prop.property_value}"

        # Use PMID as ID if available, otherwise use dbxref_id
        display_id = f"PMID:{ref.pubmed}" if ref.pubmed else ref.dbxref_id

        # Get ref_url for building links
        ref_urls = (
            db.query(RefUrl)
            .filter(RefUrl.reference_no == ref.reference_no)
            .all()
        )
        links = _build_citation_links_for_search(ref, ref_urls)

        results.append(TextSearchResult(
            category="literature_topics",
            id=display_id,
            name=name,
            description=description,
            link=None,  # No link on citation - use links array instead
            links=links,
            highlighted_name=_highlight_text(name, query),
            highlighted_description=_highlight_text(description, query),
        ))

        if len(results) >= limit:
            break

    return results


def _count_literature_topics(db: Session, query: str) -> int:
    """Count total literature topics matching the query."""
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    return (
        db.query(func.count(RefProperty.ref_property_no))
        .filter(
            RefProperty.property_type == "literature_topic",
            func.upper(RefProperty.property_value).like(upper_pattern)
        )
        .scalar()
    )


# Mapping of category to search and count functions
CATEGORY_SEARCH_FUNCTIONS = {
    "genes": search_genes,
    "descriptions": search_descriptions,
    "go_terms": search_go_terms,
    "colleagues": search_colleagues,
    "authors": search_authors,
    "pathways": search_pathways,
    "paragraphs": search_paragraphs,
    "abstracts": search_abstracts,
    "name_descriptions": search_name_descriptions,
    "phenotypes": search_phenotypes,
    "notes": search_notes,
    "external_ids": search_external_ids,
    "orthologs": search_orthologs,
    "literature_topics": search_literature_topics,
}

CATEGORY_COUNT_FUNCTIONS = {
    "genes": _count_genes,
    "descriptions": _count_descriptions,
    "go_terms": _count_go_terms,
    "colleagues": _count_colleagues,
    "authors": _count_authors,
    "pathways": _count_pathways,
    "paragraphs": _count_paragraphs,
    "abstracts": _count_abstracts,
    "name_descriptions": _count_name_descriptions,
    "phenotypes": _count_phenotypes,
    "notes": _count_notes,
    "external_ids": _count_external_ids,
    "orthologs": _count_orthologs,
    "literature_topics": _count_literature_topics,
}


# =============================================================================
# Main Entry Points
# =============================================================================

def text_search(
    db: Session,
    query: str,
    limit_per_category: int = 10,
    category_filter: Optional[str] = None,
    search_field: str = "both",
    match_mode: str = "all",
) -> TextSearchResponse:
    """
    Search all categories (or a single category if filtered).

    Args:
        db: Database session
        query: Search query string
        limit_per_category: Maximum results per category
        category_filter: If set, only search this category (e.g., "orthologs")
        search_field: For abstracts category - "title", "abstract", or "both" (default)
        match_mode: For multi-term queries - "all" (AND) or "any" (OR)

    Returns:
        TextSearchResponse with results grouped by category
    """
    categories_to_search = [category_filter] if category_filter else CATEGORY_ORDER
    results_list = []
    total_results = 0

    for category in categories_to_search:
        if category not in CATEGORY_SEARCH_FUNCTIONS:
            continue

        search_func = CATEGORY_SEARCH_FUNCTIONS[category]
        count_func = CATEGORY_COUNT_FUNCTIONS[category]

        # For abstracts category, pass the extra parameters
        if category == "abstracts":
            results = search_func(
                db, query, limit_per_category,
                search_field=search_field, match_mode=match_mode
            )
            count = count_func(db, query, search_field=search_field, match_mode=match_mode)
        else:
            results = search_func(db, query, limit_per_category)
            count = count_func(db, query)

        if results or count > 0:
            results_list.append(TextSearchCategoryResult(
                category=category,
                display_name=CATEGORY_DISPLAY_NAMES.get(category, category),
                count=count,
                results=results,
            ))
            total_results += count

    return TextSearchResponse(
        query=query,
        total_results=total_results,
        categories=results_list,
    )


def text_search_category(
    db: Session,
    query: str,
    category: str,
    search_field: str = "both",
    match_mode: str = "all",
) -> TextSearchCategoryPagedResponse:
    """
    Search within a specific category, returning all results.

    Args:
        db: Database session
        query: Search query string
        category: Category to search
        search_field: For abstracts category - "title", "abstract", or "both" (default)
        match_mode: For multi-term queries - "all" (AND) or "any" (OR)

    Returns:
        TextSearchCategoryPagedResponse with all results
    """
    if category not in CATEGORY_SEARCH_FUNCTIONS:
        return TextSearchCategoryPagedResponse(
            query=query,
            category=category,
            results=[],
            total_count=0,
        )

    count_func = CATEGORY_COUNT_FUNCTIONS[category]
    search_func = CATEGORY_SEARCH_FUNCTIONS[category]

    # For abstracts category, pass the extra parameters
    if category == "abstracts":
        total_count = count_func(db, query, search_field=search_field, match_mode=match_mode)
        all_results = search_func(
            db, query, limit=50000,
            search_field=search_field, match_mode=match_mode
        )
    else:
        total_count = count_func(db, query)
        all_results = search_func(db, query, limit=50000)

    # Get organism counts for genes category
    organism_counts = None
    if category == "genes":
        organism_counts = _count_genes_by_organism(db, query)

    return TextSearchCategoryPagedResponse(
        query=query,
        category=category,
        results=all_results,
        total_count=total_count,
        organism_counts=organism_counts,
    )
