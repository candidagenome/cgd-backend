"""
Tests for Link Curation Service.

Tests cover:
- Feature info lookup
- Available links retrieval
- Feature links retrieval
- Link updates (add/remove)
- FEAT_URL and DBXREF_URL management
"""
import pytest
from unittest.mock import MagicMock

from cgd.api.services.curation.link_curation_service import (
    LinkCurationService,
    LinkCurationError,
)


class MockFeature:
    """Mock Feature model."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        gene_name: str = None,
        feature_type: str = "ORF",
        dbxref_id: str = None,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name
        self.feature_type = feature_type
        self.dbxref_id = dbxref_id


class MockUrl:
    """Mock Url model."""

    def __init__(self, url_no: int, url: str, url_type: str = "External"):
        self.url_no = url_no
        self.url = url
        self.url_type = url_type


class MockWebDisplay:
    """Mock WebDisplay model."""

    def __init__(
        self,
        web_display_no: int,
        url_no: int,
        web_page_name: str,
        label_name: str,
        label_location: str = "Resources",
        label_type: str = "External",
    ):
        self.web_display_no = web_display_no
        self.url_no = url_no
        self.web_page_name = web_page_name
        self.label_name = label_name
        self.label_location = label_location
        self.label_type = label_type


class MockFeatUrl:
    """Mock FeatUrl model."""

    def __init__(self, feat_url_no: int, feature_no: int, url_no: int):
        self.feat_url_no = feat_url_no
        self.feature_no = feature_no
        self.url_no = url_no


class MockDbxref:
    """Mock Dbxref model."""

    def __init__(self, dbxref_no: int, dbxref_id: str):
        self.dbxref_no = dbxref_no
        self.dbxref_id = dbxref_id


class MockDbxrefUrl:
    """Mock DbxrefUrl model."""

    def __init__(self, dbxref_url_no: int, dbxref_no: int, url_no: int):
        self.dbxref_url_no = dbxref_url_no
        self.dbxref_no = dbxref_no
        self.url_no = url_no


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []

    def join(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args):
        return self

    def group_by(self, *args):
        return self

    def first(self):
        return self._results[0] if self._results else None

    def all(self):
        return self._results

    def scalar(self):
        return self._results[0] if self._results else 0


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
        MockFeature(1, "CAL0001", "ALS1", "ORF", "CGD:CAL0001"),
        MockFeature(2, "CAL0002", None, "ORF", "CGD:CAL0002"),
    ]


class TestConstants:
    """Tests for service constants."""

    def test_web_page_names(self):
        """Should define valid web page names."""
        assert "Locus" in LinkCurationService.WEB_PAGE_NAMES
        assert "Protein" in LinkCurationService.WEB_PAGE_NAMES
        assert "Phenotype" in LinkCurationService.WEB_PAGE_NAMES


class TestGetFeatureInfo:
    """Tests for getting feature info."""

    def test_returns_feature_info(self, mock_db, sample_features):
        """Should return feature info when found."""
        mock_db.query.return_value = MockQuery([sample_features[0]])

        service = LinkCurationService(mock_db)
        result = service.get_feature_info("CAL0001", "C_albicans_SC5314")

        assert result is not None
        assert result["feature_no"] == 1
        assert result["feature_name"] == "CAL0001"
        assert result["gene_name"] == "ALS1"
        assert result["dbxref_id"] == "CGD:CAL0001"

    def test_returns_none_when_not_found(self, mock_db):
        """Should return None when feature not found."""
        mock_db.query.return_value = MockQuery([])

        service = LinkCurationService(mock_db)
        result = service.get_feature_info("UNKNOWN", "C_albicans_SC5314")

        assert result is None


class TestGetAvailableLinks:
    """Tests for getting available links."""

    def test_returns_empty_for_no_links(self, mock_db):
        """Should return empty list when no links available."""
        mock_db.query.side_effect = [
            MockQuery([]),  # WebDisplay query
            MockQuery([]),  # feat_url_counts
            MockQuery([]),  # dbxref_url_counts
            MockQuery([0]),  # total features count
        ]

        service = LinkCurationService(mock_db)
        results = service.get_available_links("ORF")

        assert results == []

    def test_returns_links_with_usage(self, mock_db):
        """Should return links with usage counts."""
        url = MockUrl(1, "https://example.com/gene/{}", "External")
        web_display = MockWebDisplay(1, 1, "Locus", "Example DB")

        mock_db.query.side_effect = [
            MockQuery([(web_display, url)]),  # WebDisplay + Url
            MockQuery([(1, 50)]),  # feat_url_counts: url_no=1 has 50 features
            MockQuery([]),  # dbxref_url_counts
            MockQuery([100]),  # total features count
        ]

        service = LinkCurationService(mock_db)
        results = service.get_available_links("ORF")

        assert len(results) == 1
        assert results[0]["url_no"] == 1
        assert results[0]["label_name"] == "Example DB"
        assert results[0]["usage_count"] == 50


class TestGetFeatureLinks:
    """Tests for getting feature links."""

    def test_returns_feat_url_links(self, mock_db, sample_features):
        """Should return FEAT_URL links."""
        mock_db.query.side_effect = [
            MockQuery([(1,), (2,)]),  # FeatUrl query - url_nos
            MockQuery([sample_features[0]]),  # Feature lookup
            MockQuery([]),  # Dbxref lookup
        ]

        service = LinkCurationService(mock_db)
        results = service.get_feature_links(1)

        assert len(results) >= 2
        assert results[0]["link_table"] == "FEAT_URL"

    def test_returns_dbxref_url_links(self, mock_db, sample_features):
        """Should return DBXREF_URL links."""
        dbxref = MockDbxref(10, "CGD:CAL0001")

        mock_db.query.side_effect = [
            MockQuery([]),  # FeatUrl query - empty
            MockQuery([sample_features[0]]),  # Feature lookup
            MockQuery([dbxref]),  # Dbxref lookup
            MockQuery([(3,)]),  # DbxrefUrl query
        ]

        service = LinkCurationService(mock_db)
        results = service.get_feature_links(1)

        assert len(results) == 1
        assert results[0]["url_no"] == 3
        assert results[0]["link_table"] == "DBXREF_URL"


class TestUpdateFeatureLinks:
    """Tests for updating feature links."""

    def test_raises_for_unknown_feature(self, mock_db):
        """Should raise error for unknown feature."""
        mock_db.query.return_value = MockQuery([])

        service = LinkCurationService(mock_db)

        with pytest.raises(LinkCurationError) as exc_info:
            service.update_feature_links(
                feature_no=999,
                selected_links=[],
                curator_userid="curator1",
            )

        assert "not found" in str(exc_info.value)

    def test_adds_new_links(self, mock_db, sample_features):
        """Should add new links."""
        mock_db.query.side_effect = [
            MockQuery([sample_features[0]]),  # Feature lookup
            MockQuery([]),  # FeatUrl for get_feature_links
            MockQuery([sample_features[0]]),  # Feature in get_feature_links
            MockQuery([]),  # Dbxref in get_feature_links
            MockQuery([]),  # Existing check for _add_feat_url
        ]

        service = LinkCurationService(mock_db)
        result = service.update_feature_links(
            feature_no=1,
            selected_links=[{"url_no": 5, "link_table": "FEAT_URL"}],
            curator_userid="curator1",
        )

        assert result["added"] == 1
        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()

    def test_removes_old_links(self, mock_db, sample_features):
        """Should remove old links."""
        existing_feat_url = MockFeatUrl(1, 1, 5)

        mock_db.query.side_effect = [
            MockQuery([sample_features[0]]),  # Feature lookup
            MockQuery([(5,)]),  # FeatUrl for get_feature_links
            MockQuery([sample_features[0]]),  # Feature in get_feature_links
            MockQuery([]),  # Dbxref in get_feature_links
            MockQuery([existing_feat_url]),  # FeatUrl lookup for removal
        ]

        service = LinkCurationService(mock_db)
        result = service.update_feature_links(
            feature_no=1,
            selected_links=[],  # Empty - remove all
            curator_userid="curator1",
        )

        assert result["removed"] == 1
        mock_db.delete.assert_called_once()


class TestAddFeatUrl:
    """Tests for adding FEAT_URL entries."""

    def test_adds_new_feat_url(self, mock_db):
        """Should add new FEAT_URL."""
        mock_db.query.return_value = MockQuery([])  # No existing

        service = LinkCurationService(mock_db)
        service._add_feat_url(1, 5)

        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()

    def test_skips_existing_feat_url(self, mock_db):
        """Should skip if already exists."""
        existing = MockFeatUrl(1, 1, 5)
        mock_db.query.return_value = MockQuery([existing])

        service = LinkCurationService(mock_db)
        service._add_feat_url(1, 5)

        mock_db.add.assert_not_called()


class TestRemoveFeatUrl:
    """Tests for removing FEAT_URL entries."""

    def test_removes_feat_url(self, mock_db):
        """Should remove FEAT_URL."""
        existing = MockFeatUrl(1, 1, 5)
        mock_db.query.return_value = MockQuery([existing])

        service = LinkCurationService(mock_db)
        service._remove_feat_url(1, 5)

        mock_db.delete.assert_called_once()
        mock_db.flush.assert_called_once()

    def test_handles_nonexistent(self, mock_db):
        """Should handle nonexistent gracefully."""
        mock_db.query.return_value = MockQuery([])

        service = LinkCurationService(mock_db)
        service._remove_feat_url(1, 5)

        mock_db.delete.assert_not_called()


class TestAddDbxrefUrl:
    """Tests for adding DBXREF_URL entries."""

    def test_skips_feature_without_dbxref(self, mock_db):
        """Should skip if feature has no dbxref_id."""
        feature = MockFeature(1, "CAL0001", dbxref_id=None)

        service = LinkCurationService(mock_db)
        service._add_dbxref_url(feature, 5)

        mock_db.add.assert_not_called()

    def test_adds_dbxref_url(self, mock_db, sample_features):
        """Should add DBXREF_URL."""
        dbxref = MockDbxref(10, "CGD:CAL0001")
        mock_db.query.side_effect = [
            MockQuery([dbxref]),  # Dbxref lookup
            MockQuery([]),  # No existing DbxrefUrl
        ]

        service = LinkCurationService(mock_db)
        service._add_dbxref_url(sample_features[0], 5)

        mock_db.add.assert_called_once()


class TestRemoveDbxrefUrl:
    """Tests for removing DBXREF_URL entries."""

    def test_skips_feature_without_dbxref(self, mock_db):
        """Should skip if feature has no dbxref_id."""
        feature = MockFeature(1, "CAL0001", dbxref_id=None)

        service = LinkCurationService(mock_db)
        service._remove_dbxref_url(feature, 5)

        mock_db.delete.assert_not_called()

    def test_removes_dbxref_url(self, mock_db, sample_features):
        """Should remove DBXREF_URL."""
        dbxref = MockDbxref(10, "CGD:CAL0001")
        dbxref_url = MockDbxrefUrl(1, 10, 5)
        mock_db.query.side_effect = [
            MockQuery([dbxref]),  # Dbxref lookup
            MockQuery([dbxref_url]),  # DbxrefUrl lookup
        ]

        service = LinkCurationService(mock_db)
        service._remove_dbxref_url(sample_features[0], 5)

        mock_db.delete.assert_called_once()


class TestServiceInitialization:
    """Tests for service initialization."""

    def test_stores_db_session(self, mock_db):
        """Should store database session."""
        service = LinkCurationService(mock_db)
        assert service.db is mock_db


class TestLinkCurationError:
    """Tests for the error class."""

    def test_exception_message(self):
        """Should store error message."""
        error = LinkCurationError("Test error")
        assert str(error) == "Test error"

    def test_is_exception(self):
        """Should be an Exception."""
        assert issubclass(LinkCurationError, Exception)
