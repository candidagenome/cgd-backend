"""
Tests for Database Search Service.

Tests cover:
- Phenotype search with various query patterns
- Phenotype text formatting
- Phenotype details retrieval with associated features
- Autocomplete value retrieval (observable, qualifier, experiment_type, mutant_type)
"""
import pytest
from unittest.mock import MagicMock, PropertyMock

from cgd.api.services.curation.db_search_service import DbSearchService


class MockPhenotype:
    """Mock Phenotype model for testing."""

    def __init__(
        self,
        phenotype_no: int,
        observable: str = None,
        qualifier: str = None,
        experiment_type: str = None,
        mutant_type: str = None,
        source: str = None,
    ):
        self.phenotype_no = phenotype_no
        self.observable = observable
        self.qualifier = qualifier
        self.experiment_type = experiment_type
        self.mutant_type = mutant_type
        self.source = source


class MockFeature:
    """Mock Feature model for testing."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        gene_name: str = None,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name


class MockPhenoAnnotation:
    """Mock PhenoAnnotation model for testing."""

    def __init__(
        self,
        pheno_annotation_no: int,
        phenotype_no: int,
        feature_no: int,
    ):
        self.pheno_annotation_no = pheno_annotation_no
        self.phenotype_no = phenotype_no
        self.feature_no = feature_no


class MockQuery:
    """Mock SQLAlchemy query object for testing."""

    def __init__(self, results=None):
        self._results = results or []

    def filter(self, *args, **kwargs):
        return self

    def join(self, *args, **kwargs):
        return self

    def distinct(self):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def limit(self, n):
        self._limit = n
        return self

    def first(self):
        return self._results[0] if self._results else None

    def all(self):
        if hasattr(self, '_limit'):
            return self._results[:self._limit]
        return self._results


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    db = MagicMock()
    return db


@pytest.fixture
def sample_phenotypes():
    """Sample phenotypes for testing."""
    return [
        MockPhenotype(1, "cell morphology", "abnormal", "classical genetics", "null", "CGD"),
        MockPhenotype(2, "cell morphology", "normal", "classical genetics", "null", "CGD"),
        MockPhenotype(3, "biofilm formation", "decreased", "plate assay", "deletion", "CGD"),
        MockPhenotype(4, "virulence", "reduced", "mouse model", "deletion", "CGD"),
        MockPhenotype(5, "filamentous growth", "increased", "plate assay", "overexpression", "CGD"),
    ]


@pytest.fixture
def sample_features():
    """Sample features for testing."""
    return [
        MockFeature(101, "orf19.1", "ACT1"),
        MockFeature(102, "orf19.2", "EFG1"),
        MockFeature(103, "orf19.3", "CPH1"),
    ]


class TestFormatPhenotypeText:
    """Tests for phenotype text formatting."""

    def test_format_with_all_fields(self, mock_db):
        """Format with observable, qualifier, and mutant_type."""
        service = DbSearchService(mock_db)
        phenotype = MockPhenotype(
            1, "cell morphology", "abnormal", "classical genetics", "null"
        )
        result = service._format_phenotype_text(phenotype)
        assert result == "cell morphology (abnormal) [null]"

    def test_format_with_observable_only(self, mock_db):
        """Format with only observable."""
        service = DbSearchService(mock_db)
        phenotype = MockPhenotype(1, "cell morphology")
        result = service._format_phenotype_text(phenotype)
        assert result == "cell morphology"

    def test_format_with_observable_and_qualifier(self, mock_db):
        """Format with observable and qualifier only."""
        service = DbSearchService(mock_db)
        phenotype = MockPhenotype(1, "cell morphology", "abnormal")
        result = service._format_phenotype_text(phenotype)
        assert result == "cell morphology (abnormal)"

    def test_format_with_observable_and_mutant_type(self, mock_db):
        """Format with observable and mutant_type only."""
        service = DbSearchService(mock_db)
        phenotype = MockPhenotype(1, "cell morphology", None, None, "null")
        result = service._format_phenotype_text(phenotype)
        assert result == "cell morphology [null]"

    def test_format_with_no_fields(self, mock_db):
        """Format returns phenotype_no when no fields set."""
        service = DbSearchService(mock_db)
        phenotype = MockPhenotype(42)
        result = service._format_phenotype_text(phenotype)
        assert result == "42"


class TestSearchPhenotypes:
    """Tests for phenotype search functionality."""

    def test_search_returns_matching_phenotypes(self, mock_db, sample_phenotypes):
        """Search should return phenotypes matching the query."""
        mock_db.query.return_value = MockQuery(sample_phenotypes[:2])

        service = DbSearchService(mock_db)
        results = service.search_phenotypes("morphology")

        assert len(results) == 2
        assert results[0]["phenotype_no"] == 1
        assert results[0]["observable"] == "cell morphology"

    def test_search_returns_all_fields(self, mock_db, sample_phenotypes):
        """Search results should include all phenotype fields."""
        mock_db.query.return_value = MockQuery([sample_phenotypes[0]])

        service = DbSearchService(mock_db)
        results = service.search_phenotypes("morphology")

        result = results[0]
        assert "phenotype_no" in result
        assert "observable" in result
        assert "qualifier" in result
        assert "experiment_type" in result
        assert "mutant_type" in result
        assert "source" in result
        assert "display_text" in result

    def test_search_generates_display_text(self, mock_db, sample_phenotypes):
        """Search results should include formatted display text."""
        mock_db.query.return_value = MockQuery([sample_phenotypes[0]])

        service = DbSearchService(mock_db)
        results = service.search_phenotypes("morphology")

        assert results[0]["display_text"] == "cell morphology (abnormal) [null]"

    def test_search_empty_results(self, mock_db):
        """Search with no matches should return empty list."""
        mock_db.query.return_value = MockQuery([])

        service = DbSearchService(mock_db)
        results = service.search_phenotypes("nonexistent")

        assert results == []

    def test_search_respects_limit(self, mock_db, sample_phenotypes):
        """Search should respect the limit parameter."""
        mock_db.query.return_value = MockQuery(sample_phenotypes)

        service = DbSearchService(mock_db)
        results = service.search_phenotypes("cell", limit=2)

        assert len(results) <= 2

    def test_search_default_limit(self, mock_db):
        """Search should have default limit of 100."""
        mock_query = MockQuery([])
        mock_db.query.return_value = mock_query

        service = DbSearchService(mock_db)
        service.search_phenotypes("test")

        # Verify limit was called (default is 100)
        assert hasattr(mock_query, '_limit')
        assert mock_query._limit == 100


class TestGetPhenotypeDetails:
    """Tests for phenotype details retrieval."""

    def test_get_details_returns_phenotype(self, mock_db, sample_phenotypes):
        """Get details should return phenotype information."""
        # First query returns phenotype
        mock_phenotype_query = MockQuery([sample_phenotypes[0]])
        # Second query returns empty annotations
        mock_annotation_query = MockQuery([])

        mock_db.query.side_effect = [mock_phenotype_query, mock_annotation_query]

        service = DbSearchService(mock_db)
        result = service.get_phenotype_details(1)

        assert result is not None
        assert result["phenotype_no"] == 1
        assert result["observable"] == "cell morphology"
        assert result["qualifier"] == "abnormal"

    def test_get_details_includes_features(self, mock_db, sample_phenotypes, sample_features):
        """Get details should include associated features."""
        # First query returns phenotype
        mock_phenotype_query = MockQuery([sample_phenotypes[0]])

        # Create annotations linking phenotype to features
        annotations = [
            (MockPhenoAnnotation(1001, 1, 101), sample_features[0]),
            (MockPhenoAnnotation(1002, 1, 102), sample_features[1]),
        ]
        mock_annotation_query = MockQuery(annotations)

        mock_db.query.side_effect = [mock_phenotype_query, mock_annotation_query]

        service = DbSearchService(mock_db)
        result = service.get_phenotype_details(1)

        assert result["feature_count"] == 2
        assert len(result["features"]) == 2
        assert result["features"][0]["feature_name"] == "orf19.1"
        assert result["features"][0]["gene_name"] == "ACT1"
        assert result["features"][1]["feature_name"] == "orf19.2"

    def test_get_details_not_found(self, mock_db):
        """Get details should return None for non-existent phenotype."""
        mock_db.query.return_value = MockQuery([])

        service = DbSearchService(mock_db)
        result = service.get_phenotype_details(9999)

        assert result is None

    def test_get_details_includes_display_text(self, mock_db, sample_phenotypes):
        """Get details should include formatted display text."""
        mock_phenotype_query = MockQuery([sample_phenotypes[0]])
        mock_annotation_query = MockQuery([])
        mock_db.query.side_effect = [mock_phenotype_query, mock_annotation_query]

        service = DbSearchService(mock_db)
        result = service.get_phenotype_details(1)

        assert "display_text" in result
        assert result["display_text"] == "cell morphology (abnormal) [null]"

    def test_get_details_zero_features(self, mock_db, sample_phenotypes):
        """Get details should handle phenotype with no features."""
        mock_phenotype_query = MockQuery([sample_phenotypes[0]])
        mock_annotation_query = MockQuery([])
        mock_db.query.side_effect = [mock_phenotype_query, mock_annotation_query]

        service = DbSearchService(mock_db)
        result = service.get_phenotype_details(1)

        assert result["feature_count"] == 0
        assert result["features"] == []


class TestGetObservableValues:
    """Tests for observable autocomplete values."""

    def test_returns_distinct_values(self, mock_db):
        """Should return distinct observable values."""
        mock_db.query.return_value = MockQuery([
            ("cell morphology",),
            ("biofilm formation",),
            ("virulence",),
        ])

        service = DbSearchService(mock_db)
        results = service.get_observable_values()

        assert len(results) == 3
        assert "cell morphology" in results
        assert "biofilm formation" in results

    def test_filters_none_values(self, mock_db):
        """Should filter out None values."""
        mock_db.query.return_value = MockQuery([
            ("cell morphology",),
            (None,),
            ("virulence",),
        ])

        service = DbSearchService(mock_db)
        results = service.get_observable_values()

        assert None not in results
        assert len(results) == 2

    def test_returns_empty_list(self, mock_db):
        """Should return empty list when no observables."""
        mock_db.query.return_value = MockQuery([])

        service = DbSearchService(mock_db)
        results = service.get_observable_values()

        assert results == []


class TestGetQualifierValues:
    """Tests for qualifier autocomplete values."""

    def test_returns_distinct_values(self, mock_db):
        """Should return distinct qualifier values."""
        mock_db.query.return_value = MockQuery([
            ("abnormal",),
            ("decreased",),
            ("increased",),
            ("normal",),
        ])

        service = DbSearchService(mock_db)
        results = service.get_qualifier_values()

        assert len(results) == 4
        assert "abnormal" in results
        assert "decreased" in results

    def test_filters_none_values(self, mock_db):
        """Should filter out None values."""
        mock_db.query.return_value = MockQuery([
            ("abnormal",),
            (None,),
        ])

        service = DbSearchService(mock_db)
        results = service.get_qualifier_values()

        assert None not in results


class TestGetExperimentTypes:
    """Tests for experiment type autocomplete values."""

    def test_returns_distinct_values(self, mock_db):
        """Should return distinct experiment type values."""
        mock_db.query.return_value = MockQuery([
            ("classical genetics",),
            ("plate assay",),
            ("mouse model",),
        ])

        service = DbSearchService(mock_db)
        results = service.get_experiment_types()

        assert len(results) == 3
        assert "classical genetics" in results
        assert "plate assay" in results

    def test_filters_none_values(self, mock_db):
        """Should filter out None values."""
        mock_db.query.return_value = MockQuery([
            ("classical genetics",),
            (None,),
        ])

        service = DbSearchService(mock_db)
        results = service.get_experiment_types()

        assert None not in results


class TestGetMutantTypes:
    """Tests for mutant type autocomplete values."""

    def test_returns_distinct_values(self, mock_db):
        """Should return distinct mutant type values."""
        mock_db.query.return_value = MockQuery([
            ("null",),
            ("deletion",),
            ("overexpression",),
        ])

        service = DbSearchService(mock_db)
        results = service.get_mutant_types()

        assert len(results) == 3
        assert "null" in results
        assert "deletion" in results

    def test_filters_none_values(self, mock_db):
        """Should filter out None values."""
        mock_db.query.return_value = MockQuery([
            ("null",),
            (None,),
            ("deletion",),
        ])

        service = DbSearchService(mock_db)
        results = service.get_mutant_types()

        assert None not in results
        assert len(results) == 2

    def test_returns_empty_list(self, mock_db):
        """Should return empty list when no mutant types."""
        mock_db.query.return_value = MockQuery([])

        service = DbSearchService(mock_db)
        results = service.get_mutant_types()

        assert results == []


class TestServiceInitialization:
    """Tests for service initialization."""

    def test_stores_db_session(self, mock_db):
        """Service should store the database session."""
        service = DbSearchService(mock_db)
        assert service.db is mock_db
