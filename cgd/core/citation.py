"""
Reference citation formatting.

One citation format for every way a reference enters CGD (the weekly PubMed
loader, the temp-reference loader, and Curator Central), e.g.

    Ropars J et al. (2018) Gene flow contributes to diversification of the
    major fungal pathogen Candida albicans. Nat Commun 9(1):2253
"""
from __future__ import annotations

from typing import Optional, Sequence

CITATION_MAX = 480  # REFERENCE.CITATION is VARCHAR2(480)


def format_citation(
    year: Optional[int | str],
    title: Optional[str],
    journal: Optional[str],
    volume: Optional[str],
    issue: Optional[str],
    pages: Optional[str],
    authors: Sequence[str],
) -> Optional[str]:
    """
    Build a citation string.

    Args:
        year: Publication year
        title: Article title
        journal: Journal abbreviation (MEDLINE TA, e.g. "Nat Commun")
        volume, issue, pages: Journal volume, issue and pagination
        authors: Author names in order, "Last Initials" (e.g. "Ropars J")

    Returns:
        "First A et al. (year) Title. Journal vol(issue):pages", truncated to
        fit the CITATION column; None without a title or authors.
    """
    authors = [a for a in authors if a]
    if not title or not authors:
        return None

    if len(authors) == 1:
        author_str = authors[0]
    elif len(authors) == 2:
        author_str = f"{authors[0]} and {authors[1]}"
    else:
        author_str = f"{authors[0]} et al."

    parts = [f"{author_str} ({year or 'n.d.'})", title]
    if journal:
        journal_part = journal
        if volume:
            journal_part += f" {volume}"
        if issue:
            journal_part += f"({issue})"
        if pages:
            journal_part += f":{pages}"
        parts.append(journal_part)

    citation = " ".join(parts)
    if len(citation) > CITATION_MAX:
        citation = citation[:CITATION_MAX - 3] + "..."
    return citation
