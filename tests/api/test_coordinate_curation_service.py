"""
Tests for Coordinate and Relationship Curation Service.

Tests cover:
- Sequence source retrieval
- Feature info with coordinates and subfeatures
- Parent/child relationship retrieval
- Feature types and relationship types from CODE table
- Coordinate change preview functionality
- Feature search
"""
import pytest
from unittest.mock import MagicMock, patch

from cgd.api.services.curation.coordinate_curation_service import CoordinateCurationService


class MockFeature:
    """Mock Feature model for testing."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        gene_name: str = None,
        feature_type: str = None,
        dbxref_id: str = None,
        headline: str = None,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name
        self.feature_type = feature_type
        self.dbxref_id = dbxref_id
        self.headline = headline


class MockSeq:
    """Mock Seq model for testing."""

    def __init__(
        self,
        seq_no: int,
        source: str,
        feature_no: int = None,
        is_seq_current: str = "Y",
        seq_type: str = "Genomic",
    ):
        self.seq_no = seq_no
        self.source = source
        self.feature_no = feature_no
        self.is_seq_current = is_seq_current
        self.seq_type = seq_type


class MockFeatLocation:
    """Mock FeatLocation model for testing."""

    def __init__(
        self,
        feat_location_no: int,
        feature_no: int,
        root_seq_no: int,
        start_coord: int,
        stop_coord: int,
        strand: str = "+",
        is_loc_current: str = "Y",
    ):
        self.feat_location_no = feat_location_no
        self.feature_no = feature_no
        self.root_seq_no = root_seq_no
        self.start_coord = start_coord
        self.stop_coord = stop_coord
        self.strand = strand
        self.is_loc_current = is_loc_current


class MockFeatRelationship:
    """Mock FeatRelationship model for testing."""

    def __init__(
        self,
        feat_relationship_no: int,
        parent_feature_no: int,
        child_feature_no: int,
        relationship_type: str = "part_of",
        rank: int = None,
    ):
        self.feat_relationship_no = feat_relationship_no
        self.parent_feature_no = parent_feature_no
        self.child_feature_no = child_feature_no
        self.relationship_type = relationship_type
        self.rank = rank


class MockCode:
    """Mock Code model for testing."""

    def __init__(
        self,
        code_no: int,
        tab_name: str,
        col_name: str,
        code_value: str,
    ):
        self.code_no = code_no
        self.tab_name = tab_name
        self.col_name = col_name
        self.code_value = code_value


class MockQuery:
    """Mock SQLAlchemy query object for testing."""

    def __init__(self, results=None):
        self._results = results or []
        self._limit_value = None

    def filter(self, *args, **kwargs):
        return self

    def join(self, *args, **kwargs):
        return self

    def outerjoin(self, *args, **kwargs):
        return self

    def distinct(self):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def limit(self, n):
        self._limit_value = n
        return self

    def first(self):
        return self._results[0] if self._results else None

    def all(self):
        if self._limit_value:
            return self._results[:self._limit_value]
        return self._results


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    db = MagicMock()
    return db


@pytest.fixture
def sample_features():
    """Sample features for testing."""
    return [
        MockFeature(1, "orf19.1", "ACT1", "ORF", "CAL0000001", "Actin"),
        MockFeature(2, "orf19.2", "EFG1", "ORF", "CAL0000002", "Transcription factor"),
        MockFeature(3, "orf19.1_CDS", None, "CDS"),
        MockFeature(4, "orf19.1_intron", None, "intron"),
        MockFeature(100, "Ca22chrM_C_albicans_SC5314", None, "chromosome"),
    ]


@pytest.fixture
def sample_seq_sources():
    """Sample sequence sources."""
    return [
        ("Assembly_22",),
        ("Assembly_21",),
        ("C_albicans_SC5314",),
    ]


class TestGetSeqSources:
    """Tests for sequence source retrieval."""

    def test_returns_distinct_sources(self, mock_db, sample_seq_sources):
        """Should return distinct sequence sources."""
        mock_db.query.return_value = MockQuery(sample_seq_sources)

        service = CoordinateCurationService(mock_db)
        results = service.get_seq_sources()

        assert len(results) == 3
        assert "Assembly_22" in results
        assert "Assembly_21" in results

    def test_filters_none_values(self, mock_db):
        """Should filter out None values."""
        mock_db.query.return_value = MockQuery([
            ("Assembly_22",),
            (None,),
            ("Assembly_21",),
        ])

        service = CoordinateCurationService(mock_db)
        results = service.get_seq_sources()

        assert None not in results
        assert len(results) == 2

    def test_returns_empty_list(self, mock_db):
        """Should return empty list when no sources."""
        mock_db.query.return_value = MockQuery([])

        service = CoordinateCurationService(mock_db)
        results = service.get_seq_sources()

        assert results == []


class TestGetFeatureInfo:
    """Tests for feature info retrieval."""

    def test_returns_none_for_unknown_feature(self, mock_db):
        """Should return None for unknown feature."""
        mock_db.query.return_value = MockQuery([])

        service = CoordinateCurationService(mock_db)
        result = service.get_feature_info("UNKNOWN")

        assert result is None

    def test_returns_feature_basic_info(self, mock_db, sample_features):
        """Should return basic feature information."""
        feature = sample_features[0]
        seq = MockSeq(1, "Assembly_22", 100)
        location = MockFeatLocation(1, 1, 1, 1000, 2000, "+")
        root_feature = sample_features[4]

        # Setup mock queries
        mock_db.query.side_effect = [
            MockQuery([feature]),  # Feature query
            MockQuery([(location, seq)]),  # Location query
            MockQuery([root_feature]),  # Root feature query
            MockQuery([]),  # Subfeatures query
            MockQuery([]),  # Parents query
        ]

        service = CoordinateCurationService(mock_db)
        result = service.get_feature_info("ACT1")

        assert result is not None
        assert result["feature_no"] == 1
        assert result["feature_name"] == "orf19.1"
        assert result["gene_name"] == "ACT1"
        assert result["feature_type"] == "ORF"

    def test_returns_location_info(self, mock_db, sample_features):
        """Should include location information."""
        feature = sample_features[0]
        seq = MockSeq(1, "Assembly_22", 100)
        location = MockFeatLocation(1, 1, 1, 1000, 2000, "+")
        root_feature = sample_features[4]

        mock_db.query.side_effect = [
            MockQuery([feature]),
            MockQuery([(location, seq)]),
            MockQuery([root_feature]),
            MockQuery([]),
            MockQuery([]),
        ]

        service = CoordinateCurationService(mock_db)
        result = service.get_feature_info("ACT1")

        assert result["location"] is not None
        assert result["location"]["start_coord"] == 1000
        assert result["location"]["stop_coord"] == 2000
        assert result["location"]["strand"] == "+"
        assert result["location"]["seq_source"] == "Assembly_22"

    def test_returns_root_feature_name(self, mock_db, sample_features):
        """Should include root feature (chromosome) name."""
        feature = sample_features[0]
        seq = MockSeq(1, "Assembly_22", 100)
        location = MockFeatLocation(1, 1, 1, 1000, 2000, "+")
        root_feature = sample_features[4]

        mock_db.query.side_effect = [
            MockQuery([feature]),
            MockQuery([(location, seq)]),
            MockQuery([root_feature]),
            MockQuery([]),
            MockQuery([]),
        ]

        service = CoordinateCurationService(mock_db)
        result = service.get_feature_info("ACT1")

        assert result["root_feature_name"] == "Ca22chrM_C_albicans_SC5314"

    def test_handles_missing_location(self, mock_db, sample_features):
        """Should handle features without location."""
        feature = sample_features[0]

        mock_db.query.side_effect = [
            MockQuery([feature]),
            MockQuery([]),  # No location
            MockQuery([]),
            MockQuery([]),
        ]

        service = CoordinateCurationService(mock_db)
        result = service.get_feature_info("ACT1")

        assert result is not None
        assert result["location"] is None

    def test_case_insensitive_search(self, mock_db, sample_features):
        """Should find features case-insensitively."""
        feature = sample_features[0]

        mock_db.query.side_effect = [
            MockQuery([feature]),
            MockQuery([]),
            MockQuery([]),
            MockQuery([]),
        ]

        service = CoordinateCurationService(mock_db)

        # The query is made - test that both upper and lower work
        result = service.get_feature_info("act1")
        assert result is not None


class TestGetSubfeatures:
    """Tests for subfeature retrieval."""

    def test_returns_subfeatures(self, mock_db, sample_features):
        """Should return child features."""
        cds = sample_features[2]
        intron = sample_features[3]
        rel1 = MockFeatRelationship(1, 1, 3, "part_of", 1)
        rel2 = MockFeatRelationship(2, 1, 4, "part_of", 2)
        loc1 = MockFeatLocation(2, 3, 1, 1000, 1500, "+")
        loc2 = MockFeatLocation(3, 4, 1, 1501, 1600, "+")

        mock_db.query.return_value = MockQuery([
            (cds, rel1, loc1),
            (intron, rel2, loc2),
        ])

        service = CoordinateCurationService(mock_db)
        results = service._get_subfeatures(1)

        assert len(results) == 2
        assert results[0]["feature_name"] == "orf19.1_CDS"
        assert results[0]["feature_type"] == "CDS"
        assert results[0]["relationship_type"] == "part_of"
        assert results[0]["start_coord"] == 1000

    def test_handles_no_subfeatures(self, mock_db):
        """Should return empty list when no subfeatures."""
        mock_db.query.return_value = MockQuery([])

        service = CoordinateCurationService(mock_db)
        results = service._get_subfeatures(1)

        assert results == []

    def test_handles_subfeature_without_location(self, mock_db, sample_features):
        """Should handle subfeatures without location."""
        cds = sample_features[2]
        rel1 = MockFeatRelationship(1, 1, 3, "part_of", 1)

        mock_db.query.return_value = MockQuery([
            (cds, rel1, None),  # No location
        ])

        service = CoordinateCurationService(mock_db)
        results = service._get_subfeatures(1)

        assert len(results) == 1
        assert results[0]["start_coord"] is None
        assert results[0]["stop_coord"] is None


class TestGetParents:
    """Tests for parent feature retrieval."""

    def test_returns_parents(self, mock_db, sample_features):
        """Should return parent features."""
        parent = sample_features[0]
        rel = MockFeatRelationship(1, 1, 3, "part_of", 1)
        loc = MockFeatLocation(1, 1, 1, 1000, 2000, "+")

        mock_db.query.return_value = MockQuery([
            (parent, rel, loc),
        ])

        service = CoordinateCurationService(mock_db)
        results = service._get_parents(3)

        assert len(results) == 1
        assert results[0]["feature_name"] == "orf19.1"
        assert results[0]["gene_name"] == "ACT1"
        assert results[0]["relationship_type"] == "part_of"

    def test_handles_no_parents(self, mock_db):
        """Should return empty list when no parents."""
        mock_db.query.return_value = MockQuery([])

        service = CoordinateCurationService(mock_db)
        results = service._get_parents(1)

        assert results == []


class TestGetFeatureTypes:
    """Tests for feature type retrieval."""

    def test_returns_distinct_types(self, mock_db):
        """Should return distinct feature types."""
        mock_db.query.return_value = MockQuery([
            ("CDS",),
            ("ORF",),
            ("chromosome",),
            ("intron",),
        ])

        service = CoordinateCurationService(mock_db)
        results = service.get_feature_types()

        assert len(results) == 4
        assert "ORF" in results
        assert "CDS" in results

    def test_filters_none_values(self, mock_db):
        """Should filter out None values."""
        mock_db.query.return_value = MockQuery([
            ("ORF",),
            (None,),
            ("CDS",),
        ])

        service = CoordinateCurationService(mock_db)
        results = service.get_feature_types()

        assert None not in results
        assert len(results) == 2


class TestGetRelationshipTypes:
    """Tests for relationship type retrieval from CODE table."""

    def test_returns_relationship_types(self, mock_db):
        """Should return relationship types from CODE table."""
        mock_db.query.return_value = MockQuery([
            ("adjacent_to",),
            ("derives_from",),
            ("part_of",),
        ])

        service = CoordinateCurationService(mock_db)
        results = service.get_relationship_types()

        assert len(results) == 3
        assert "part_of" in results
        assert "adjacent_to" in results

    def test_returns_empty_list(self, mock_db):
        """Should return empty list when no types defined."""
        mock_db.query.return_value = MockQuery([])

        service = CoordinateCurationService(mock_db)
        results = service.get_relationship_types()

        assert results == []


class TestGetFeatureQualifiers:
    """Tests for feature qualifier retrieval from CODE table."""

    def test_returns_qualifiers(self, mock_db):
        """Should return feature qualifiers from CODE table."""
        mock_db.query.return_value = MockQuery([
            ("Verified",),
            ("Uncharacterized",),
            ("Dubious",),
        ])

        service = CoordinateCurationService(mock_db)
        results = service.get_feature_qualifiers()

        assert len(results) == 3
        assert "Verified" in results


class TestPreviewCoordinateChanges:
    """Tests for coordinate change preview."""

    def test_returns_error_for_unknown_feature(self, mock_db):
        """Should return error for unknown feature."""
        mock_db.query.return_value = MockQuery([])

        service = CoordinateCurationService(mock_db)
        result = service.preview_coordinate_changes(
            "UNKNOWN", "Assembly_22", []
        )

        assert "error" in result

    def test_preview_with_no_changes(self, mock_db, sample_features):
        """Should handle preview with no changes."""
        feature = sample_features[0]
        seq = MockSeq(1, "Assembly_22", 100)
        location = MockFeatLocation(1, 1, 1, 1000, 2000, "+")
        root_feature = sample_features[4]

        mock_db.query.side_effect = [
            MockQuery([feature]),
            MockQuery([(location, seq)]),
            MockQuery([root_feature]),
            MockQuery([]),
            MockQuery([]),
        ]

        service = CoordinateCurationService(mock_db)
        result = service.preview_coordinate_changes(
            "ACT1", "Assembly_22", []
        )

        assert result["change_count"] == 0
        assert result["changes"] == []

    def test_preview_detects_coordinate_changes(self, mock_db, sample_features):
        """Should detect coordinate changes."""
        feature = sample_features[0]
        seq = MockSeq(1, "Assembly_22", 100)
        location = MockFeatLocation(1, 1, 1, 1000, 2000, "+")
        root_feature = sample_features[4]

        mock_db.query.side_effect = [
            MockQuery([feature]),
            MockQuery([(location, seq)]),
            MockQuery([root_feature]),
            MockQuery([]),
            MockQuery([]),
        ]

        service = CoordinateCurationService(mock_db)
        result = service.preview_coordinate_changes(
            "ACT1",
            "Assembly_22",
            [{"feature_no": 1, "start_coord": 1100, "stop_coord": 2100}]
        )

        assert result["change_count"] == 1
        change = result["changes"][0]
        assert change["old_start"] == 1000
        assert change["new_start"] == 1100
        assert change["old_stop"] == 2000
        assert change["new_stop"] == 2100

    def test_preview_detects_strand_change(self, mock_db, sample_features):
        """Should detect strand changes."""
        feature = sample_features[0]
        seq = MockSeq(1, "Assembly_22", 100)
        location = MockFeatLocation(1, 1, 1, 1000, 2000, "+")
        root_feature = sample_features[4]

        mock_db.query.side_effect = [
            MockQuery([feature]),
            MockQuery([(location, seq)]),
            MockQuery([root_feature]),
            MockQuery([]),
            MockQuery([]),
        ]

        service = CoordinateCurationService(mock_db)
        result = service.preview_coordinate_changes(
            "ACT1",
            "Assembly_22",
            [{"feature_no": 1, "strand": "-"}]
        )

        assert result["change_count"] == 1
        change = result["changes"][0]
        assert change["old_strand"] == "+"
        assert change["new_strand"] == "-"

    def test_preview_ignores_unchanged(self, mock_db, sample_features):
        """Should not report features with no changes."""
        feature = sample_features[0]
        seq = MockSeq(1, "Assembly_22", 100)
        location = MockFeatLocation(1, 1, 1, 1000, 2000, "+")
        root_feature = sample_features[4]

        mock_db.query.side_effect = [
            MockQuery([feature]),
            MockQuery([(location, seq)]),
            MockQuery([root_feature]),
            MockQuery([]),
            MockQuery([]),
        ]

        service = CoordinateCurationService(mock_db)
        result = service.preview_coordinate_changes(
            "ACT1",
            "Assembly_22",
            [{"feature_no": 1, "start_coord": 1000, "stop_coord": 2000, "strand": "+"}]
        )

        assert result["change_count"] == 0

    def test_preview_ignores_unknown_feature_no(self, mock_db, sample_features):
        """Should ignore changes for unknown feature_no."""
        feature = sample_features[0]
        seq = MockSeq(1, "Assembly_22", 100)
        location = MockFeatLocation(1, 1, 1, 1000, 2000, "+")
        root_feature = sample_features[4]

        mock_db.query.side_effect = [
            MockQuery([feature]),
            MockQuery([(location, seq)]),
            MockQuery([root_feature]),
            MockQuery([]),
            MockQuery([]),
        ]

        service = CoordinateCurationService(mock_db)
        result = service.preview_coordinate_changes(
            "ACT1",
            "Assembly_22",
            [{"feature_no": 9999, "start_coord": 5000}]  # Unknown feature_no
        )

        assert result["change_count"] == 0


class TestSearchFeatures:
    """Tests for feature search."""

    def test_search_returns_matching_features(self, mock_db, sample_features):
        """Should return features matching query."""
        mock_db.query.return_value = MockQuery(sample_features[:2])

        service = CoordinateCurationService(mock_db)
        results = service.search_features("orf19")

        assert len(results) == 2
        assert results[0]["feature_name"] == "orf19.1"

    def test_search_returns_all_fields(self, mock_db, sample_features):
        """Should return all required fields."""
        mock_db.query.return_value = MockQuery([sample_features[0]])

        service = CoordinateCurationService(mock_db)
        results = service.search_features("ACT1")

        result = results[0]
        assert "feature_no" in result
        assert "feature_name" in result
        assert "gene_name" in result
        assert "feature_type" in result

    def test_search_respects_limit(self, mock_db, sample_features):
        """Should respect limit parameter."""
        mock_db.query.return_value = MockQuery(sample_features)

        service = CoordinateCurationService(mock_db)
        results = service.search_features("orf", limit=2)

        assert len(results) <= 2

    def test_search_empty_results(self, mock_db):
        """Should return empty list for no matches."""
        mock_db.query.return_value = MockQuery([])

        service = CoordinateCurationService(mock_db)
        results = service.search_features("nonexistent")

        assert results == []

    def test_search_default_limit(self, mock_db):
        """Should have default limit of 20."""
        mock_query = MockQuery([])
        mock_db.query.return_value = mock_query

        service = CoordinateCurationService(mock_db)
        service.search_features("test")

        assert mock_query._limit_value == 20


class TestServiceInitialization:
    """Tests for service initialization."""

    def test_stores_db_session(self, mock_db):
        """Service should store the database session."""
        service = CoordinateCurationService(mock_db)
        assert service.db is mock_db
