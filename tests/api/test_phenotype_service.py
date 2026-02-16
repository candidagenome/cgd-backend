"""
Tests for Phenotype Service.

Tests cover:
- Citation link building
- Phenotype search with filters
- Observable tree building
"""
import pytest
from unittest.mock import MagicMock, PropertyMock

from cgd.api.services.phenotype_service import (
    _build_citation_links_for_phenotype,
    search_phenotypes,
    get_observable_tree,
)


class MockReference:
    """Mock Reference model."""

    def __init__(
        self,
        reference_no: int,
        dbxref_id: str,
        pubmed: int = None,
        citation: str = None,
        year: int = None,
    ):
        self.reference_no = reference_no
        self.dbxref_id = dbxref_id
        self.pubmed = pubmed
        self.citation = citation
        self.year = year
        self.journal = None


class MockUrl:
    """Mock Url model."""

    def __init__(self, url: str, url_type: str = None):
        self.url = url
        self.url_type = url_type


class MockRefUrl:
    """Mock RefUrl model."""

    def __init__(self, reference_no: int, url: MockUrl):
        self.reference_no = reference_no
        self.url = url


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
        organism: MockOrganism = None,
        feature_type: str = "ORF",
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name
        self.organism = organism
        self.feature_type = feature_type


class MockPhenotype:
    """Mock Phenotype model."""

    def __init__(
        self,
        phenotype_no: int,
        observable: str,
        qualifier: str = None,
        experiment_type: str = None,
        mutant_type: str = None,
    ):
        self.phenotype_no = phenotype_no
        self.observable = observable
        self.qualifier = qualifier
        self.experiment_type = experiment_type
        self.mutant_type = mutant_type


class MockExperiment:
    """Mock Experiment model."""

    def __init__(self, experiment_no: int, experiment_comment: str = None):
        self.experiment_no = experiment_no
        self.experiment_comment = experiment_comment


class MockPhenoAnnotation:
    """Mock PhenoAnnotation model."""

    def __init__(
        self,
        pheno_annotation_no: int,
        feature: MockFeature,
        phenotype: MockPhenotype,
        experiment: MockExperiment = None,
    ):
        self.pheno_annotation_no = pheno_annotation_no
        self.feature = feature
        self.phenotype = phenotype
        self.experiment = experiment


class MockCv:
    """Mock Cv model."""

    def __init__(self, cv_no: int, cv_name: str):
        self.cv_no = cv_no
        self.cv_name = cv_name


class MockCvTerm:
    """Mock CvTerm model."""

    def __init__(self, cv_term_no: int, cv_no: int, term_name: str):
        self.cv_term_no = cv_term_no
        self.cv_no = cv_no
        self.term_name = term_name


class MockCvtermRelationship:
    """Mock CvtermRelationship model."""

    def __init__(self, parent_cv_term_no: int, child_cv_term_no: int):
        self.parent_cv_term_no = parent_cv_term_no
        self.child_cv_term_no = child_cv_term_no


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []

    def join(self, *args, **kwargs):
        return self

    def outerjoin(self, *args, **kwargs):
        return self

    def options(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def group_by(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def offset(self, n):
        return self

    def limit(self, n):
        return self

    def count(self):
        return len(self._results)

    def first(self):
        return self._results[0] if self._results else None

    def all(self):
        return self._results


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
        organism=sample_organism,
    )


@pytest.fixture
def sample_phenotype():
    """Create sample phenotype."""
    return MockPhenotype(
        phenotype_no=1,
        observable="colony morphology",
        qualifier="abnormal",
        experiment_type="classical genetics",
        mutant_type="null",
    )


@pytest.fixture
def sample_experiment():
    """Create sample experiment."""
    return MockExperiment(experiment_no=1, experiment_comment="Test experiment")


@pytest.fixture
def sample_pheno_annotation(sample_feature, sample_phenotype, sample_experiment):
    """Create sample phenotype annotation."""
    return MockPhenoAnnotation(
        pheno_annotation_no=1,
        feature=sample_feature,
        phenotype=sample_phenotype,
        experiment=sample_experiment,
    )


class TestBuildCitationLinksForPhenotype:
    """Tests for _build_citation_links_for_phenotype."""

    def test_always_includes_cgd_paper_link(self):
        """Should always include CGD Paper link."""
        ref = MockReference(1, "CAL0000001")

        result = _build_citation_links_for_phenotype(ref)

        assert len(result) >= 1
        cgd_link = result[0]
        assert cgd_link.name == "CGD Paper"
        assert "/reference/CAL0000001" in cgd_link.url
        assert cgd_link.link_type == "internal"

    def test_includes_pubmed_link_when_available(self):
        """Should include PubMed link when pubmed ID exists."""
        ref = MockReference(1, "CAL0000001", pubmed=12345678)

        result = _build_citation_links_for_phenotype(ref)

        assert len(result) == 2
        pubmed_link = result[1]
        assert pubmed_link.name == "PubMed"
        assert "12345678" in pubmed_link.url
        assert pubmed_link.link_type == "external"

    def test_no_pubmed_link_when_not_available(self):
        """Should not include PubMed link when pubmed is None."""
        ref = MockReference(1, "CAL0000001", pubmed=None)

        result = _build_citation_links_for_phenotype(ref)

        assert len(result) == 1
        assert result[0].name == "CGD Paper"

    def test_includes_reference_supplement_link(self):
        """Should include Reference Supplement link from ref_urls."""
        ref = MockReference(1, "CAL0000001")
        url = MockUrl("http://example.com/supplement.pdf", "Reference Supplement")
        ref_url = MockRefUrl(1, url)

        result = _build_citation_links_for_phenotype(ref, [ref_url])

        supplement_links = [l for l in result if l.name == "Reference Supplement"]
        assert len(supplement_links) == 1
        assert supplement_links[0].url == "http://example.com/supplement.pdf"

    def test_includes_download_datasets_link(self):
        """Should include Download Datasets link for download URL types."""
        ref = MockReference(1, "CAL0000001")
        url = MockUrl("http://example.com/data.zip", "Download Datasets")
        ref_url = MockRefUrl(1, url)

        result = _build_citation_links_for_phenotype(ref, [ref_url])

        download_links = [l for l in result if l.name == "Download Datasets"]
        assert len(download_links) == 1

    def test_skips_reference_data_urls(self):
        """Should skip Reference Data URL type."""
        ref = MockReference(1, "CAL0000001")
        url = MockUrl("http://example.com/data", "Reference Data")
        ref_url = MockRefUrl(1, url)

        result = _build_citation_links_for_phenotype(ref, [ref_url])

        # Should only have CGD Paper link, not the reference data
        assert len(result) == 1

    def test_includes_full_text_for_other_urls(self):
        """Should include Full Text link for other URL types."""
        ref = MockReference(1, "CAL0000001")
        url = MockUrl("http://journal.com/article", "Publisher")
        ref_url = MockRefUrl(1, url)

        result = _build_citation_links_for_phenotype(ref, [ref_url])

        full_text_links = [l for l in result if l.name == "Full Text"]
        assert len(full_text_links) == 1

    def test_handles_url_with_no_url_type(self):
        """Should handle URL with no url_type."""
        ref = MockReference(1, "CAL0000001")
        url = MockUrl("http://example.com/article", None)
        ref_url = MockRefUrl(1, url)

        result = _build_citation_links_for_phenotype(ref, [ref_url])

        # Should be treated as Full Text
        full_text_links = [l for l in result if l.name == "Full Text"]
        assert len(full_text_links) == 1


class TestSearchPhenotypes:
    """Tests for search_phenotypes."""

    def test_returns_empty_results_when_no_data(self, mock_db):
        """Should return empty results when no phenotype annotations."""
        mock_query = MockQuery([])
        mock_db.query.return_value = mock_query

        result = search_phenotypes(mock_db)

        assert result.total_results == 0
        assert result.results == []
        assert result.page == 1

    def test_returns_correct_pagination_info(self, mock_db, sample_pheno_annotation):
        """Should return correct pagination info."""
        mock_db.query.side_effect = [
            MockQuery([sample_pheno_annotation]),  # Main query
            MockQuery([]),  # RefLink query
            MockQuery([]),  # ExptProperty query
        ]

        result = search_phenotypes(mock_db, page=2, limit=10)

        assert result.page == 2
        assert result.limit == 10

    def test_builds_result_with_phenotype_data(self, mock_db, sample_pheno_annotation):
        """Should build result with phenotype data."""
        mock_db.query.side_effect = [
            MockQuery([sample_pheno_annotation]),  # Main query
            MockQuery([]),  # RefLink query
            MockQuery([]),  # ExptProperty query
        ]

        result = search_phenotypes(mock_db)

        assert len(result.results) == 1
        pheno_result = result.results[0]
        assert pheno_result.feature_name == "CAL0001"
        assert pheno_result.gene_name == "ALS1"
        assert pheno_result.observable == "colony morphology"
        assert pheno_result.qualifier == "abnormal"
        assert pheno_result.experiment_type == "classical genetics"
        assert pheno_result.mutant_type == "null"

    def test_includes_organism_name(self, mock_db, sample_pheno_annotation):
        """Should include organism name in result."""
        mock_db.query.side_effect = [
            MockQuery([sample_pheno_annotation]),  # Main query
            MockQuery([]),  # RefLink query
            MockQuery([]),  # ExptProperty query
        ]

        result = search_phenotypes(mock_db)

        assert result.results[0].organism == "Candida albicans SC5314"

    def test_includes_experiment_comment(self, mock_db, sample_pheno_annotation):
        """Should include experiment comment in result."""
        mock_db.query.side_effect = [
            MockQuery([sample_pheno_annotation]),  # Main query
            MockQuery([]),  # RefLink query
            MockQuery([]),  # ExptProperty query
        ]

        result = search_phenotypes(mock_db)

        assert result.results[0].experiment_comment == "Test experiment"

    def test_query_info_with_filters(self, mock_db):
        """Should include query info in response."""
        mock_query = MockQuery([])
        mock_db.query.return_value = mock_query

        result = search_phenotypes(
            mock_db,
            observable="morphology",
            qualifier="abnormal",
            experiment_type="genetics",
            mutant_type="null",
        )

        assert result.query.observable == "morphology"
        assert result.query.qualifier == "abnormal"
        assert result.query.experiment_type == "genetics"
        assert result.query.mutant_type == "null"


class TestGetObservableTree:
    """Tests for get_observable_tree."""

    def test_returns_empty_tree_when_no_observables(self, mock_db):
        """Should return empty tree when no observables."""
        mock_db.query.side_effect = [
            MockQuery([]),  # observable counts
            MockQuery([]),  # cv lookup
        ]

        result = get_observable_tree(mock_db)

        assert result.tree == []

    def test_returns_flat_tree_when_no_cv(self, mock_db):
        """Should return flat tree when no observable CV exists."""
        observable_counts = [
            ("colony morphology", 10),
            ("growth rate", 5),
        ]
        mock_db.query.side_effect = [
            MockQuery(observable_counts),  # observable counts
            MockQuery([]),  # cv lookup - not found
        ]

        result = get_observable_tree(mock_db)

        assert len(result.tree) == 2
        assert result.tree[0].term == "colony morphology"
        assert result.tree[0].count == 10
        assert result.tree[1].term == "growth rate"
        assert result.tree[1].count == 5

    def test_includes_annotation_counts(self, mock_db):
        """Should include annotation counts for each term."""
        observable_counts = [
            ("morphology", 15),
        ]
        mock_db.query.side_effect = [
            MockQuery(observable_counts),
            MockQuery([]),  # No CV
        ]

        result = get_observable_tree(mock_db)

        assert result.tree[0].count == 15

    def test_children_default_to_empty_list(self, mock_db):
        """Should have empty children list for flat terms."""
        observable_counts = [
            ("term1", 1),
        ]
        mock_db.query.side_effect = [
            MockQuery(observable_counts),
            MockQuery([]),  # No CV
        ]

        result = get_observable_tree(mock_db)

        assert result.tree[0].children == []

    def test_builds_tree_with_cv_relationships(self, mock_db):
        """Should build hierarchical tree when CV exists."""
        observable_counts = [
            ("parent term", 5),
            ("child term", 3),
        ]
        cv = MockCv(1, "observable")
        cv_terms = [
            MockCvTerm(1, 1, "parent term"),
            MockCvTerm(2, 1, "child term"),
        ]
        relationships = [
            MockCvtermRelationship(1, 2),  # parent -> child
        ]

        mock_db.query.side_effect = [
            MockQuery(observable_counts),  # observable counts
            MockQuery([cv]),  # cv lookup
            MockQuery(cv_terms),  # cv terms
            MockQuery(relationships),  # relationships
        ]

        result = get_observable_tree(mock_db)

        # Should have one root (parent term) with one child
        assert len(result.tree) == 1
        assert result.tree[0].term == "parent term"
        assert result.tree[0].count == 5
        assert len(result.tree[0].children) == 1
        assert result.tree[0].children[0].term == "child term"
        assert result.tree[0].children[0].count == 3

    def test_fallback_to_flat_when_empty_tree_from_cv(self, mock_db):
        """Should fall back to flat list when CV produces empty tree."""
        observable_counts = [
            ("term1", 1),
        ]
        cv = MockCv(1, "observable")
        cv_terms = []  # No terms in CV

        mock_db.query.side_effect = [
            MockQuery(observable_counts),  # observable counts
            MockQuery([cv]),  # cv lookup
            MockQuery(cv_terms),  # no cv terms
            MockQuery([]),  # no relationships
        ]

        result = get_observable_tree(mock_db)

        # Should fall back to flat list
        assert len(result.tree) == 1
        assert result.tree[0].term == "term1"
