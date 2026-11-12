"""Tests for cgd.utils.citation.format_citation."""
from cgd.utils.citation import CITATION_MAX, format_citation

TITLE = "Gene flow contributes to diversification of the major fungal pathogen Candida albicans."


def test_three_or_more_authors():
    assert format_citation(2018, TITLE, "Nat Commun", "9", "1", "2253", ["Ropars J", "Maufrais C", "Diogo D"]) == (
        f"Ropars J et al. (2018) {TITLE} Nat Commun 9(1):2253")


def test_one_and_two_authors():
    assert format_citation(2020, "T.", "J", "1", None, "2-3", ["Smith J"]) == "Smith J (2020) T. J 1:2-3"
    assert format_citation(2020, "T.", "J", "1", None, None, ["Smith J", "Doe A"]) == "Smith J and Doe A (2020) T. J 1"


def test_missing_parts():
    assert format_citation(None, "T.", None, "1", "2", "3", ["Smith J"]) == "Smith J (n.d.) T."
    assert format_citation(2020, "T.", "J", None, "2", None, ["", "Smith J"]) == "Smith J (2020) T. J(2)"


def test_requires_title_and_authors():
    assert format_citation(2020, "", "J", "1", None, None, ["Smith J"]) is None
    assert format_citation(2020, "T.", "J", "1", None, None, []) is None


def test_truncates_to_column_size():
    citation = format_citation(2020, "x" * 600, "J", "1", None, None, ["Smith J"])
    assert len(citation) == CITATION_MAX and citation.endswith("...")
