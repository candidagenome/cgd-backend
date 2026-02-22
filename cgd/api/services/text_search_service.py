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
    PaginationInfo,
)
from cgd.models.models import (
    Feature,
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


def _truncate_text(text: Optional[str], max_length: int = 300) -> Optional[str]:
    """Truncate text to max_length with ellipsis."""
    if not text:
        return text
    if len(text) <= max_length:
        return text
    return text[:max_length] + "..."


# =============================================================================
# Individual Category Search Functions
# =============================================================================

def search_genes(db: Session, query: str, limit: int = 20) -> list[TextSearchResult]:
    """
    Search genes/loci by gene_name, feature_name, dbxref_id, or aliases.
    Returns TextSearchResult list with category="genes".
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

    found_feature_nos = set()
    for feat in feature_query:
        display_name = feat.gene_name or feat.feature_name
        results.append(TextSearchResult(
            category="genes",
            id=feat.dbxref_id,
            name=display_name,
            description=feat.headline,
            link=f"/locus/{feat.feature_name}",
            organism=_get_organism_name(feat.organism),
            highlighted_name=_highlight_text(display_name, query),
            highlighted_description=_highlight_text(feat.headline, query),
        ))
        found_feature_nos.add(feat.feature_no)

    # Search aliases if we have room
    remaining = limit - len(results)
    if remaining > 0:
        alias_query = (
            db.query(Feature, Alias)
            .join(FeatAlias, Feature.feature_no == FeatAlias.feature_no)
            .join(Alias, FeatAlias.alias_no == Alias.alias_no)
            .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
            .filter(func.upper(Alias.alias_name).like(upper_pattern))
            .limit(remaining + len(found_feature_nos))
        )

        for feat, alias in alias_query:
            if feat.feature_no not in found_feature_nos:
                display_name = feat.gene_name or feat.feature_name
                description = f"Alias: {alias.alias_name}"
                if feat.headline:
                    description += f" - {feat.headline}"
                results.append(TextSearchResult(
                    category="genes",
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


def search_descriptions(db: Session, query: str, limit: int = 20) -> list[TextSearchResult]:
    """
    Search locus descriptions (headline field).
    Returns TextSearchResult list with category="descriptions".
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    feature_query = (
        db.query(Feature)
        .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
        .filter(func.upper(Feature.headline).like(upper_pattern))
        .limit(limit)
    )

    for feat in feature_query:
        display_name = feat.gene_name or feat.feature_name
        results.append(TextSearchResult(
            category="descriptions",
            id=feat.dbxref_id,
            name=display_name,
            description=feat.headline,
            link=f"/locus/{feat.feature_name}",
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
        description = _truncate_text(para.paragraph_text, 250)

        results.append(TextSearchResult(
            category="paragraphs",
            id=feat.dbxref_id,
            name=display_name,
            description=description,
            link=f"/locus/{feat.feature_name}#summaryParagraph",
            organism=_get_organism_name(feat.organism) if hasattr(feat, 'organism') else None,
            highlighted_name=_highlight_text(display_name, query),
            highlighted_description=_highlight_text(description, query),
        ))

    return results


def search_abstracts(db: Session, query: str, limit: int = 20) -> list[TextSearchResult]:
    """
    Search paper abstracts.
    Returns TextSearchResult list with category="abstracts".
    """
    results = []
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    abstract_query = (
        db.query(Abstract, Reference)
        .join(Reference, Abstract.reference_no == Reference.reference_no)
        .filter(func.upper(Abstract.abstract).like(upper_pattern))
        .limit(limit)
    )

    for abstract, ref in abstract_query:
        name = f"PMID:{ref.pubmed}" if ref.pubmed else ref.dbxref_id
        description = _truncate_text(abstract.abstract, 250)

        results.append(TextSearchResult(
            category="abstracts",
            id=ref.dbxref_id,
            name=name,
            description=description,
            link=f"/reference/{ref.dbxref_id}",
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
            link=f"/locus/{feat.feature_name}",
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

        if note_link.tab_name.upper() == 'FEATURE':
            # Look up the feature
            feat = db.query(Feature).filter(
                Feature.feature_no == note_link.primary_key
            ).first()
            if feat:
                link = f"/locus/{feat.feature_name}"
                link_name = feat.gene_name or feat.feature_name
        elif note_link.tab_name.upper() == 'REFERENCE':
            ref = db.query(Reference).filter(
                Reference.reference_no == note_link.primary_key
            ).first()
            if ref:
                link = f"/reference/{ref.dbxref_id}"
                link_name = f"PMID:{ref.pubmed}" if ref.pubmed else ref.dbxref_id

        if link:
            description = _truncate_text(note.note, 200)
            results.append(TextSearchResult(
                category="notes",
                id=str(note.note_no),
                name=link_name,
                description=description,
                link=link,
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
            link=f"/locus/{feat.feature_name}",
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
            link=f"/locus/{feat.feature_name}",
            organism=_get_organism_name(feat.organism) if hasattr(feat, 'organism') else None,
            highlighted_name=_highlight_text(display_name, query),
            highlighted_description=_highlight_text(description, query),
        ))

    return results[:limit]


# =============================================================================
# Count Functions for Pagination
# =============================================================================

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


def _count_descriptions(db: Session, query: str) -> int:
    """Count total description matches."""
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    return (
        db.query(func.count(Feature.feature_no))
        .filter(func.upper(Feature.headline).like(upper_pattern))
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


def _count_abstracts(db: Session, query: str) -> int:
    """Count total abstracts matching the query."""
    like_pattern = _get_like_pattern(query)
    upper_pattern = like_pattern.upper()

    return (
        db.query(func.count(Abstract.reference_no))
        .filter(func.upper(Abstract.abstract).like(upper_pattern))
        .scalar()
    )


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

        name = f"PMID:{ref.pubmed}" if ref.pubmed else ref.dbxref_id
        description = f"Topic: {prop.property_value}"
        if ref.citation:
            description += f" - {_truncate_text(ref.citation, 150)}"

        results.append(TextSearchResult(
            category="literature_topics",
            id=ref.dbxref_id,
            name=name,
            description=description,
            link=f"/reference/{ref.dbxref_id}",
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
) -> TextSearchResponse:
    """
    Search all categories (or a single category if filtered).

    Args:
        db: Database session
        query: Search query string
        limit_per_category: Maximum results per category
        category_filter: If set, only search this category (e.g., "orthologs")

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

        # Get results
        results = search_func(db, query, limit_per_category)
        # Get total count for this category
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


def text_search_category_paginated(
    db: Session,
    query: str,
    category: str,
    page: int = 1,
    page_size: int = 20,
) -> TextSearchCategoryPagedResponse:
    """
    Search within a specific category with pagination.

    Args:
        db: Database session
        query: Search query string
        category: Category to search
        page: Page number (1-indexed)
        page_size: Number of results per page

    Returns:
        TextSearchCategoryPagedResponse with paginated results
    """
    if category not in CATEGORY_SEARCH_FUNCTIONS:
        return TextSearchCategoryPagedResponse(
            query=query,
            category=category,
            results=[],
            pagination=PaginationInfo(
                page=page,
                page_size=page_size,
                total_items=0,
                total_pages=0,
                has_next=False,
                has_prev=False,
            ),
        )

    count_func = CATEGORY_COUNT_FUNCTIONS[category]
    total_count = count_func(db, query)

    # For pagination, we need to implement offset/limit
    # For now, we'll fetch more and slice
    # This is simpler but not optimal for large result sets
    search_func = CATEGORY_SEARCH_FUNCTIONS[category]
    offset = (page - 1) * page_size
    # Fetch enough to satisfy the current page
    all_results = search_func(db, query, offset + page_size)
    paginated_results = all_results[offset:offset + page_size]

    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 0

    return TextSearchCategoryPagedResponse(
        query=query,
        category=category,
        results=paginated_results,
        pagination=PaginationInfo(
            page=page,
            page_size=page_size,
            total_items=total_count,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_prev=page > 1,
        ),
    )
