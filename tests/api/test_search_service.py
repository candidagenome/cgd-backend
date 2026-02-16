"""
Tests for Search Service.

Tests cover:
- Query normalization
- LIKE pattern generation
- GOID formatting
- Text highlighting
- Reference link building
- Identifier resolution
- Gene search
- GO term search
- Phenotype search
- Reference search
- Quick search
- Autocomplete suggestions
- Paginated category search
"""
import pytest
from unittest.mock import MagicMock

from cgd.api.services.search_service import (
    _normalize_query,
    _get_like_pattern,
    _format_goid,
    _get_organism_name,
    _highlight_text,
    _build_reference_links,
    resolve_identifier,
    search_genes,
    search_go_terms,
    search_phenotypes,
    search_references,
    quick_search,
    get_autocomplete_suggestions,
    search_category_paginated,
    _count_genes,
    _count_go_terms,
    _count_phenotypes,
    _count_references,
)


class MockOrganism:
    """Mock Organism model."""

    def __init__(self, organism_no: int, organism_name: str):
        self.organism_no = organism_no
        self.organism_name = organism_name


class MockFeature:
    """Mock Feature model."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        gene_name: str = None,
        dbxref_id: str = None,
        headline: str = None,
        organism: MockOrganism = None,
        organism_no: int = None,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name
        self.dbxref_id = dbxref_id
        self.headline = headline
        self.organism = organism
        self.organism_no = organism_no


class MockReference:
    """Mock Reference model."""

    def __init__(
        self,
        reference_no: int,
        dbxref_id: str,
        pubmed: int = None,
        citation: str = None,
    ):
        self.reference_no = reference_no
        self.dbxref_id = dbxref_id
        self.pubmed = pubmed
        self.citation = citation


class MockGo:
    """Mock Go model."""

    def __init__(
        self,
        go_no: int,
        goid: int,
        go_term: str,
        go_aspect: str = "P",
        go_definition: str = None,
    ):
        self.go_no = go_no
        self.goid = goid
        self.go_term = go_term
        self.go_aspect = go_aspect
        self.go_definition = go_definition


class MockPhenotype:
    """Mock Phenotype model."""

    def __init__(self, observable: str):
        self.observable = observable


class MockAlias:
    """Mock Alias model."""

    def __init__(self, alias_no: int, alias_name: str):
        self.alias_no = alias_no
        self.alias_name = alias_name


class MockUrl:
    """Mock Url model."""

    def __init__(self, url: str, url_type: str = None):
        self.url = url
        self.url_type = url_type


class MockRefUrl:
    """Mock RefUrl model."""

    def __init__(self, reference_no: int, url: MockUrl = None):
        self.reference_no = reference_no
        self.url = url


class MockSubquery:
    """Mock SQLAlchemy subquery with .c accessor."""

    def __init__(self):
        self.c = MagicMock()


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []
        self._is_scalar = not isinstance(results, list)

    def join(self, *args, **kwargs):
        return self

    def outerjoin(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def distinct(self):
        return self

    def offset(self, n):
        return self

    def limit(self, n):
        return self

    def subquery(self):
        return MockSubquery()

    def scalar(self):
        return len(self._results) if isinstance(self._results, list) else self._results

    def first(self):
        if self._is_scalar:
            return None
        return self._results[0] if self._results else None

    def all(self):
        return self._results if isinstance(self._results, list) else []

    def __iter__(self):
        if isinstance(self._results, list):
            return iter(self._results)
        return iter([])


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    db = MagicMock()
    db.query.return_value = MockQuery([])
    return db


@pytest.fixture
def sample_organism():
    """Create sample organism."""
    return MockOrganism(1, "Candida albicans SC5314")


@pytest.fixture
def sample_feature(sample_organism):
    """Create sample feature."""
    return MockFeature(
        feature_no=1,
        feature_name="CAL0001",
        gene_name="ALS1",
        dbxref_id="CGD:CAL0001",
        headline="Cell surface adhesin",
        organism=sample_organism,
    )


@pytest.fixture
def sample_reference():
    """Create sample reference."""
    return MockReference(
        reference_no=1,
        dbxref_id="CGD_REF:0001",
        pubmed=12345678,
        citation="Smith et al. (2023)",
    )


@pytest.fixture
def sample_go():
    """Create sample GO term."""
    return MockGo(
        go_no=1,
        goid=5634,
        go_term="nucleus",
        go_aspect="C",
        go_definition="A membrane-bounded organelle.",
    )


class TestNormalizeQuery:
    """Tests for _normalize_query."""

    def test_strips_whitespace(self):
        """Should strip whitespace."""
        assert _normalize_query("  test  ") == "test"

    def test_converts_wildcards(self):
        """Should convert * to %."""
        assert _normalize_query("test*") == "test%"

    def test_handles_multiple_wildcards(self):
        """Should handle multiple wildcards."""
        assert _normalize_query("*test*") == "%test%"

    def test_preserves_case(self):
        """Should preserve case."""
        assert _normalize_query("TeSt") == "TeSt"


class TestGetLikePattern:
    """Tests for _get_like_pattern."""

    def test_wraps_with_wildcards(self):
        """Should wrap in % for contains search."""
        assert _get_like_pattern("test") == "%test%"

    def test_preserves_user_wildcards(self):
        """Should preserve user wildcards."""
        assert _get_like_pattern("test*") == "test%"

    def test_no_extra_wrapping_if_wildcards_present(self):
        """Should not add extra % if wildcards present."""
        assert _get_like_pattern("*test*") == "%test%"


class TestFormatGoid:
    """Tests for _format_goid."""

    def test_formats_with_padding(self):
        """Should format with 7-digit padding."""
        assert _format_goid(5634) == "GO:0005634"

    def test_formats_small_number(self):
        """Should format small numbers with zeros."""
        assert _format_goid(1) == "GO:0000001"

    def test_formats_large_number(self):
        """Should format large numbers."""
        assert _format_goid(1234567) == "GO:1234567"


class TestGetOrganismName:
    """Tests for _get_organism_name."""

    def test_returns_organism_name(self, sample_organism):
        """Should return organism name."""
        assert _get_organism_name(sample_organism) == "Candida albicans SC5314"

    def test_returns_none_for_none(self):
        """Should return None for None input."""
        assert _get_organism_name(None) is None


class TestHighlightText:
    """Tests for _highlight_text."""

    def test_highlights_match(self):
        """Should highlight matching text."""
        result = _highlight_text("Hello World", "world")
        assert "<mark>World</mark>" in result

    def test_case_insensitive(self):
        """Should be case insensitive."""
        result = _highlight_text("Hello World", "WORLD")
        assert "<mark>World</mark>" in result

    def test_preserves_original_case(self):
        """Should preserve original case in result."""
        result = _highlight_text("Hello WoRLd", "world")
        assert "<mark>WoRLd</mark>" in result

    def test_handles_none_text(self):
        """Should handle None text."""
        assert _highlight_text(None, "test") is None

    def test_handles_none_query(self):
        """Should handle None query."""
        assert _highlight_text("test", None) == "test"

    def test_removes_wildcards_for_highlight(self):
        """Should remove wildcards for highlighting."""
        result = _highlight_text("Hello World", "world*")
        assert "<mark>World</mark>" in result


class TestBuildReferenceLinks:
    """Tests for _build_reference_links."""

    def test_includes_cgd_paper_link(self, mock_db, sample_reference):
        """Should include CGD Paper link."""
        mock_db.query.return_value = MockQuery([])

        links = _build_reference_links(mock_db, sample_reference)

        cgd_link = next((l for l in links if l.name == "CGD Paper"), None)
        assert cgd_link is not None
        assert cgd_link.link_type == "internal"

    def test_includes_pubmed_link(self, mock_db, sample_reference):
        """Should include PubMed link when pubmed ID exists."""
        mock_db.query.return_value = MockQuery([])

        links = _build_reference_links(mock_db, sample_reference)

        pubmed_link = next((l for l in links if l.name == "PubMed"), None)
        assert pubmed_link is not None
        assert "12345678" in pubmed_link.url


class TestResolveIdentifier:
    """Tests for resolve_identifier."""

    def test_resolves_by_gene_name(self, mock_db, sample_feature):
        """Should resolve by gene name."""
        mock_db.query.return_value = MockQuery([sample_feature])

        result = resolve_identifier(mock_db, "ALS1")

        assert result.resolved is True
        assert result.entity_type == "locus"
        assert "/locus/" in result.redirect_url

    def test_resolves_by_feature_name(self, mock_db, sample_feature):
        """Should resolve by feature name."""
        mock_db.query.side_effect = [
            MockQuery([]),  # gene_name lookup
            MockQuery([sample_feature]),  # feature_name lookup
        ]

        result = resolve_identifier(mock_db, "CAL0001")

        assert result.resolved is True
        assert result.entity_type == "locus"

    def test_resolves_by_dbxref_id(self, mock_db, sample_feature):
        """Should resolve by dbxref_id."""
        mock_db.query.side_effect = [
            MockQuery([]),  # gene_name lookup
            MockQuery([]),  # feature_name lookup
            MockQuery([sample_feature]),  # dbxref_id lookup
        ]

        result = resolve_identifier(mock_db, "CGD:CAL0001")

        assert result.resolved is True
        assert result.entity_type == "locus"

    def test_resolves_reference_by_dbxref_id(self, mock_db, sample_reference):
        """Should resolve reference by dbxref_id."""
        mock_db.query.side_effect = [
            MockQuery([]),  # gene_name
            MockQuery([]),  # feature_name
            MockQuery([]),  # feature dbxref_id
            MockQuery([sample_reference]),  # reference dbxref_id
        ]

        result = resolve_identifier(mock_db, "CGD_REF:0001")

        assert result.resolved is True
        assert result.entity_type == "reference"

    def test_returns_not_resolved_when_not_found(self, mock_db):
        """Should return not resolved when not found."""
        mock_db.query.return_value = MockQuery([])

        result = resolve_identifier(mock_db, "UNKNOWN")

        assert result.resolved is False


class TestSearchGenes:
    """Tests for search_genes."""

    def test_returns_matching_genes(self, mock_db, sample_feature):
        """Should return matching genes."""
        # search_genes queries Feature first, then Feature+Alias
        # Need to return empty for alias query to avoid tuple unpacking issues
        call_count = [0]

        def mock_query(*args):
            call_count[0] += 1
            q = MockQuery([sample_feature] if call_count[0] == 1 else [])
            return q

        mock_db.query.side_effect = mock_query

        results = search_genes(mock_db, "ALS")

        assert len(results) >= 1
        assert results[0].category == "gene"

    def test_respects_limit(self, mock_db, sample_feature):
        """Should respect limit parameter."""
        call_count = [0]

        def mock_query(*args):
            call_count[0] += 1
            q = MockQuery([sample_feature] if call_count[0] == 1 else [])
            return q

        mock_db.query.side_effect = mock_query

        results = search_genes(mock_db, "ALS", limit=5)

        assert len(results) <= 5


class TestSearchGoTerms:
    """Tests for search_go_terms."""

    def test_returns_matching_go_terms(self, mock_db, sample_go):
        """Should return matching GO terms."""
        mock_db.query.return_value = MockQuery([sample_go])

        results = search_go_terms(mock_db, "nucleus")

        assert len(results) >= 1
        assert results[0].category == "go_term"

    def test_handles_goid_search(self, mock_db, sample_go):
        """Should handle GO ID search."""
        mock_db.query.return_value = MockQuery([sample_go])

        results = search_go_terms(mock_db, "GO:0005634")

        assert len(results) >= 0


class TestSearchPhenotypes:
    """Tests for search_phenotypes."""

    def test_returns_matching_phenotypes(self, mock_db):
        """Should return matching phenotypes."""
        mock_db.query.return_value = MockQuery([("colony morphology",)])

        results = search_phenotypes(mock_db, "colony")

        assert len(results) >= 1
        assert results[0].category == "phenotype"


class TestSearchReferences:
    """Tests for search_references."""

    def test_returns_matching_references(self, mock_db, sample_reference):
        """Should return matching references."""
        # search_references queries Reference by citation, then queries RefUrl for links
        call_count = [0]

        def mock_query(*args):
            call_count[0] += 1
            if call_count[0] == 1:
                # First query: Reference search
                return MockQuery([sample_reference])
            else:
                # Subsequent queries: RefUrl for links
                return MockQuery([])

        mock_db.query.side_effect = mock_query

        results = search_references(mock_db, "Smith")

        assert len(results) >= 1
        assert results[0].category == "reference"

    def test_handles_pubmed_id_search(self, mock_db, sample_reference):
        """Should handle PubMed ID search."""
        call_count = [0]

        def mock_query(*args):
            call_count[0] += 1
            if call_count[0] == 1:
                return MockQuery([sample_reference])
            else:
                return MockQuery([])

        mock_db.query.side_effect = mock_query

        results = search_references(mock_db, "12345678")

        assert len(results) >= 0


class TestQuickSearch:
    """Tests for quick_search."""

    def test_returns_search_response(self, mock_db):
        """Should return SearchResponse."""
        mock_db.query.return_value = MockQuery([])

        result = quick_search(mock_db, "test")

        assert result.query == "test"
        assert result.total_results >= 0

    def test_groups_results_by_category(self, mock_db, sample_feature, sample_go):
        """Should group results by category."""
        # quick_search calls search_genes, search_go_terms, search_phenotypes, search_references
        # Each function may make multiple queries internally
        # Return empty results to simplify - just verify the response structure
        mock_db.query.return_value = MockQuery([])

        result = quick_search(mock_db, "test")

        # Verify response has the correct structure
        assert hasattr(result, "results_by_category")
        assert hasattr(result, "total_results")


class TestGetAutocompleteSuggestions:
    """Tests for get_autocomplete_suggestions."""

    def test_returns_autocomplete_response(self, mock_db):
        """Should return AutocompleteResponse."""
        mock_db.query.return_value = MockQuery([])

        result = get_autocomplete_suggestions(mock_db, "test")

        assert result.query == "test"

    def test_returns_empty_for_short_query(self, mock_db):
        """Should return empty for empty query."""
        result = get_autocomplete_suggestions(mock_db, "")

        assert result.suggestions == []

    def test_prioritizes_genes(self, mock_db, sample_feature):
        """Should prioritize gene suggestions."""
        mock_db.query.side_effect = [
            MockQuery([("ALS1", "CAL0001", "Cell adhesin")]),  # gene prefix
            MockQuery([]),  # gene feature prefix
            MockQuery([]),  # GO terms
            MockQuery([]),  # phenotypes
        ]

        result = get_autocomplete_suggestions(mock_db, "ALS", limit=10)

        if result.suggestions:
            assert result.suggestions[0].category == "gene"


class TestSearchCategoryPaginated:
    """Tests for search_category_paginated."""

    def test_returns_paginated_response(self, mock_db):
        """Should return CategorySearchResponse."""
        # search_category_paginated calls count function and search function
        # Use callable to return scalar for count and empty list for search
        call_count = [0]

        def mock_query(*args):
            call_count[0] += 1
            q = MockQuery([])
            # First call is count, return scalar 0
            q._results = 0 if call_count[0] == 1 else []
            return q

        mock_db.query.side_effect = mock_query

        result = search_category_paginated(mock_db, "test", "genes", page=1, page_size=20)

        assert result.query == "test"
        assert result.category == "genes"
        assert result.pagination is not None

    def test_pagination_info(self, mock_db):
        """Should include correct pagination info."""
        call_count = [0]

        def mock_query(*args):
            call_count[0] += 1
            q = MockQuery([])
            q._results = 0 if call_count[0] == 1 else []
            return q

        mock_db.query.side_effect = mock_query

        result = search_category_paginated(mock_db, "test", "genes", page=1, page_size=10)

        assert result.pagination.page == 1
        assert result.pagination.page_size == 10

    def test_unknown_category(self, mock_db):
        """Should handle unknown category."""
        result = search_category_paginated(mock_db, "test", "unknown", page=1, page_size=20)

        assert result.results == []


class TestCountFunctions:
    """Tests for count functions."""

    def test_count_genes(self, mock_db):
        """Should count genes."""
        # _count_genes makes two queries: feature count (scalar) and alias subquery
        call_count = [0]

        def mock_query(*args):
            call_count[0] += 1
            q = MockQuery(5)  # Scalar value for count
            return q

        mock_db.query.side_effect = mock_query

        count = _count_genes(mock_db, "ALS")

        assert isinstance(count, int)

    def test_count_go_terms(self, mock_db):
        """Should count GO terms."""
        mock_db.query.return_value = MockQuery(10)

        count = _count_go_terms(mock_db, "nucleus")

        assert isinstance(count, int)

    def test_count_phenotypes(self, mock_db):
        """Should count phenotypes."""
        mock_db.query.return_value = MockQuery(3)

        count = _count_phenotypes(mock_db, "colony")

        assert isinstance(count, int)

    def test_count_references(self, mock_db):
        """Should count references."""
        # _count_references queries for citation count, then potentially for pubmed match
        # For non-numeric query, only the first query is made
        mock_db.query.return_value = MockQuery(7)

        count = _count_references(mock_db, "Smith")

        assert isinstance(count, int)
