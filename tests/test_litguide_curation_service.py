"""
Tests for Literature Guide Curation Service.

Tests cover:
- Feature lookup by name and number
- Getting feature literature (curated and uncurated)
- Adding/removing topic associations
- Curation status management
- Reference search
"""
import pytest
from unittest.mock import MagicMock
from datetime import datetime

from cgd.api.services.curation.litguide_curation_service import (
    LitGuideCurationService,
    LitGuideCurationError,
    LITERATURE_TOPICS,
    CURATION_STATUSES,
)


class MockFeature:
    """Mock Feature model."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        gene_name: str = None,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name


class MockReference:
    """Mock Reference model."""

    def __init__(
        self,
        reference_no: int,
        pubmed: int = None,
        citation: str = "Test Citation",
        title: str = "Test Title",
        year: int = 2024,
    ):
        self.reference_no = reference_no
        self.pubmed = pubmed
        self.citation = citation
        self.title = title
        self.year = year


class MockRefProperty:
    """Mock RefProperty model."""

    def __init__(
        self,
        ref_property_no: int,
        reference_no: int,
        property_type: str,
        property_value: str,
    ):
        self.ref_property_no = ref_property_no
        self.reference_no = reference_no
        self.property_type = property_type
        self.property_value = property_value
        self.date_last_reviewed = datetime.now()


class MockRefpropFeat:
    """Mock RefpropFeat model."""

    def __init__(self, refprop_feat_no: int, ref_property_no: int, feature_no: int):
        self.refprop_feat_no = refprop_feat_no
        self.ref_property_no = ref_property_no
        self.feature_no = feature_no


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []
        self._count = len(self._results)

    def join(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args):
        return self

    def offset(self, n):
        return self

    def limit(self, n):
        return self

    def label(self, name):
        return self

    def first(self):
        return self._results[0] if self._results else None

    def all(self):
        return self._results

    def count(self):
        return self._count


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    db = MagicMock()
    db.query.return_value = MockQuery([])
    return db


@pytest.fixture
def sample_features():
    """Create sample features."""
    return [
        MockFeature(1, "CAL0001", "ALS1"),
        MockFeature(2, "CAL0002", None),
    ]


@pytest.fixture
def sample_references():
    """Create sample references."""
    return [
        MockReference(1, 12345678, "Smith et al. (2024)", "Test Paper 1"),
        MockReference(2, 87654321, "Doe et al. (2023)", "Test Paper 2"),
    ]


class TestConstants:
    """Tests for service constants."""

    def test_literature_topics(self):
        """Should define valid literature topics."""
        assert "Gene Product" in LITERATURE_TOPICS
        assert "Phenotype" in LITERATURE_TOPICS
        assert "Expression" in LITERATURE_TOPICS
        assert "Disease" in LITERATURE_TOPICS

    def test_curation_statuses(self):
        """Should define valid curation statuses."""
        assert "Not Yet Curated" in CURATION_STATUSES
        assert "High Priority" in CURATION_STATUSES
        assert "Done: Curated" in CURATION_STATUSES


class TestGetFeatureByName:
    """Tests for getting feature by name."""

    def test_returns_feature_when_found(self, mock_db, sample_features):
        """Should return feature when found."""
        mock_db.query.return_value = MockQuery([sample_features[0]])

        service = LitGuideCurationService(mock_db)
        result = service.get_feature_by_name("CAL0001")

        assert result is not None
        assert result.feature_no == 1

    def test_returns_none_when_not_found(self, mock_db):
        """Should return None when not found."""
        mock_db.query.return_value = MockQuery([])

        service = LitGuideCurationService(mock_db)
        result = service.get_feature_by_name("UNKNOWN")

        assert result is None


class TestGetFeatureByNo:
    """Tests for getting feature by number."""

    def test_returns_feature_when_found(self, mock_db, sample_features):
        """Should return feature when found."""
        mock_db.query.return_value = MockQuery([sample_features[0]])

        service = LitGuideCurationService(mock_db)
        result = service.get_feature_by_no(1)

        assert result is not None
        assert result.feature_name == "CAL0001"

    def test_returns_none_when_not_found(self, mock_db):
        """Should return None when not found."""
        mock_db.query.return_value = MockQuery([])

        service = LitGuideCurationService(mock_db)
        result = service.get_feature_by_no(999)

        assert result is None


class TestGetFeatureLiterature:
    """Tests for getting feature literature."""

    def test_raises_for_unknown_feature(self, mock_db):
        """Should raise error for unknown feature."""
        mock_db.query.return_value = MockQuery([])

        service = LitGuideCurationService(mock_db)

        with pytest.raises(LitGuideCurationError) as exc_info:
            service.get_feature_literature(999)

        assert "not found" in str(exc_info.value)

    def test_returns_empty_lists_for_feature_without_literature(
        self, mock_db, sample_features
    ):
        """Should return empty lists when no literature."""
        mock_db.query.side_effect = [
            MockQuery([sample_features[0]]),  # Feature lookup
            MockQuery([]),  # Curated query
            MockQuery([]),  # Uncurated query
        ]

        service = LitGuideCurationService(mock_db)
        result = service.get_feature_literature(1)

        assert result["feature_no"] == 1
        assert result["curated"] == []
        assert result["uncurated"] == []


class TestAddTopicAssociation:
    """Tests for adding topic associations."""

    def test_raises_for_invalid_topic(self, mock_db):
        """Should raise error for invalid topic."""
        service = LitGuideCurationService(mock_db)

        with pytest.raises(LitGuideCurationError) as exc_info:
            service.add_topic_association(1, 1, "Invalid Topic", "curator1")

        assert "Invalid topic" in str(exc_info.value)

    def test_raises_for_unknown_feature(self, mock_db):
        """Should raise error for unknown feature."""
        mock_db.query.return_value = MockQuery([])

        service = LitGuideCurationService(mock_db)

        with pytest.raises(LitGuideCurationError) as exc_info:
            service.add_topic_association(999, 1, "Phenotype", "curator1")

        assert "Feature" in str(exc_info.value) and "not found" in str(exc_info.value)

    def test_raises_for_unknown_reference(self, mock_db, sample_features):
        """Should raise error for unknown reference."""
        mock_db.query.side_effect = [
            MockQuery([sample_features[0]]),  # Feature found
            MockQuery([]),  # Reference not found
        ]

        service = LitGuideCurationService(mock_db)

        with pytest.raises(LitGuideCurationError) as exc_info:
            service.add_topic_association(1, 999, "Phenotype", "curator1")

        assert "Reference" in str(exc_info.value) and "not found" in str(exc_info.value)

    def test_raises_for_existing_association(
        self, mock_db, sample_features, sample_references
    ):
        """Should raise error if association already exists."""
        ref_prop = MockRefProperty(1, 1, "Topic", "Phenotype")
        existing_link = MockRefpropFeat(1, 1, 1)

        mock_db.query.side_effect = [
            MockQuery([sample_features[0]]),  # Feature found
            MockQuery([sample_references[0]]),  # Reference found
            MockQuery([ref_prop]),  # RefProperty exists
            MockQuery([existing_link]),  # Link already exists
        ]

        service = LitGuideCurationService(mock_db)

        with pytest.raises(LitGuideCurationError) as exc_info:
            service.add_topic_association(1, 1, "Phenotype", "curator1")

        assert "already associated" in str(exc_info.value)

    def test_creates_new_association(self, mock_db, sample_features, sample_references):
        """Should create new topic association."""
        ref_prop = MockRefProperty(1, 1, "Topic", "Phenotype")

        mock_db.query.side_effect = [
            MockQuery([sample_features[0]]),  # Feature found
            MockQuery([sample_references[0]]),  # Reference found
            MockQuery([ref_prop]),  # RefProperty exists
            MockQuery([]),  # No existing link
        ]

        service = LitGuideCurationService(mock_db)
        service.add_topic_association(1, 1, "Phenotype", "curator1")

        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()


class TestRemoveTopicAssociation:
    """Tests for removing topic associations."""

    def test_raises_for_unknown_association(self, mock_db):
        """Should raise error for unknown association."""
        mock_db.query.return_value = MockQuery([])

        service = LitGuideCurationService(mock_db)

        with pytest.raises(LitGuideCurationError) as exc_info:
            service.remove_topic_association(999, "curator1")

        assert "not found" in str(exc_info.value)

    def test_removes_association(self, mock_db):
        """Should remove topic association."""
        link = MockRefpropFeat(1, 1, 1)
        mock_db.query.return_value = MockQuery([link])

        service = LitGuideCurationService(mock_db)
        result = service.remove_topic_association(1, "curator1")

        assert result is True
        mock_db.delete.assert_called_once()
        mock_db.commit.assert_called_once()


class TestSetReferenceCurationStatus:
    """Tests for setting curation status."""

    def test_raises_for_invalid_status(self, mock_db):
        """Should raise error for invalid status."""
        service = LitGuideCurationService(mock_db)

        with pytest.raises(LitGuideCurationError) as exc_info:
            service.set_reference_curation_status(1, "Invalid Status", "curator1")

        assert "Invalid status" in str(exc_info.value)

    def test_raises_for_unknown_reference(self, mock_db):
        """Should raise error for unknown reference."""
        mock_db.query.return_value = MockQuery([])

        service = LitGuideCurationService(mock_db)

        with pytest.raises(LitGuideCurationError) as exc_info:
            service.set_reference_curation_status(999, "High Priority", "curator1")

        assert "not found" in str(exc_info.value)

    def test_updates_existing_status(self, mock_db, sample_references):
        """Should update existing curation status."""
        existing_prop = MockRefProperty(1, 1, "Curation status", "Not Yet Curated")

        mock_db.query.side_effect = [
            MockQuery([sample_references[0]]),  # Reference found
            MockQuery([existing_prop]),  # Existing property
        ]

        service = LitGuideCurationService(mock_db)
        result = service.set_reference_curation_status(1, "Done: Curated", "curator1")

        assert result == 1
        assert existing_prop.property_value == "Done: Curated"

    def test_creates_new_status(self, mock_db, sample_references):
        """Should create new curation status property."""
        mock_db.query.side_effect = [
            MockQuery([sample_references[0]]),  # Reference found
            MockQuery([]),  # No existing property
        ]

        service = LitGuideCurationService(mock_db)
        service.set_reference_curation_status(1, "High Priority", "curator1")

        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()


class TestGetReferenceCurationStatus:
    """Tests for getting curation status."""

    def test_returns_status_when_exists(self, mock_db):
        """Should return curation status when exists."""
        prop = MockRefProperty(1, 1, "Curation status", "Done: Curated")
        mock_db.query.return_value = MockQuery([prop])

        service = LitGuideCurationService(mock_db)
        result = service.get_reference_curation_status(1)

        assert result == "Done: Curated"

    def test_returns_none_when_not_exists(self, mock_db):
        """Should return None when no status."""
        mock_db.query.return_value = MockQuery([])

        service = LitGuideCurationService(mock_db)
        result = service.get_reference_curation_status(1)

        assert result is None


class TestSearchReferences:
    """Tests for searching references."""

    def test_searches_by_pubmed(self, mock_db, sample_references):
        """Should search by PubMed ID."""
        mock_query = MockQuery([sample_references[0]])
        mock_query._count = 1
        mock_db.query.side_effect = [
            mock_query,  # Search query
            MockQuery([]),  # Curation status
        ]

        service = LitGuideCurationService(mock_db)
        results, total = service.search_references("12345678")

        assert total == 1

    def test_searches_by_title(self, mock_db, sample_references):
        """Should search by title."""
        mock_query = MockQuery(sample_references)
        mock_query._count = 2
        mock_db.query.side_effect = [
            mock_query,  # Search query
            MockQuery([]),  # Curation status for ref 1
            MockQuery([]),  # Curation status for ref 2
        ]

        service = LitGuideCurationService(mock_db)
        results, total = service.search_references("Test")

        assert total == 2

    def test_returns_empty_for_no_matches(self, mock_db):
        """Should return empty list when no matches."""
        mock_db.query.return_value = MockQuery([])

        service = LitGuideCurationService(mock_db)
        results, total = service.search_references("nonexistent")

        assert results == []
        assert total == 0


class TestServiceInitialization:
    """Tests for service initialization."""

    def test_stores_db_session(self, mock_db):
        """Should store database session."""
        service = LitGuideCurationService(mock_db)
        assert service.db is mock_db


class TestLitGuideCurationError:
    """Tests for the error class."""

    def test_exception_message(self):
        """Should store error message."""
        error = LitGuideCurationError("Test error")
        assert str(error) == "Test error"

    def test_is_exception(self):
        """Should be an Exception."""
        assert issubclass(LitGuideCurationError, Exception)
