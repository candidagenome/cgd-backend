"""
Tests for Sequence Curation Service.

Tests cover:
- Root sequence retrieval
- Sequence segment extraction
- Change preview (insertion, deletion, substitution)
- Affected features detection
- Nearby features search
"""
import pytest
from unittest.mock import MagicMock

from cgd.api.services.curation.sequence_curation_service import (
    SequenceCurationService,
)


class MockFeature:
    """Mock Feature model."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        feature_type: str = "chromosome",
        gene_name: str = None,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.feature_type = feature_type
        self.gene_name = gene_name


class MockSeq:
    """Mock Seq model."""

    def __init__(
        self,
        seq_no: int,
        feature_no: int,
        seq_length: int,
        residues: str,
        seq_type: str = "Genomic",
        is_seq_current: str = "Y",
        source: str = "GenBank",
    ):
        self.seq_no = seq_no
        self.feature_no = feature_no
        self.seq_length = seq_length
        self.residues = residues
        self.seq_type = seq_type
        self.is_seq_current = is_seq_current
        self.source = source


class MockFeatLocation:
    """Mock FeatLocation model."""

    def __init__(
        self,
        feat_location_no: int,
        feature_no: int,
        root_seq_no: int,
        start_coord: int,
        stop_coord: int,
        strand: str = "W",
        is_loc_current: str = "Y",
    ):
        self.feat_location_no = feat_location_no
        self.feature_no = feature_no
        self.root_seq_no = root_seq_no
        self.start_coord = start_coord
        self.stop_coord = stop_coord
        self.strand = strand
        self.is_loc_current = is_loc_current


class MockRow:
    """Mock row result with named attributes."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


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

    def limit(self, n):
        return self

    def label(self, name):
        return self

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
def sample_chromosome():
    """Create sample chromosome feature with sequence."""
    feature = MockFeature(1, "ChrA", "chromosome")
    seq = MockSeq(
        seq_no=1,
        feature_no=1,
        seq_length=100,
        residues="ATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGC",
        source="GenBank Assembly",
    )
    return feature, seq


class TestConstants:
    """Tests for service constants."""

    def test_note_type(self):
        """Should define note type constant."""
        from cgd.api.services.curation.sequence_curation_service import NOTE_TYPE
        assert NOTE_TYPE == "Sequence change"

    def test_source(self):
        """Should define source constant."""
        from cgd.api.services.curation.sequence_curation_service import SOURCE
        assert SOURCE == "CGD"


class TestGetRootSequences:
    """Tests for getting root sequences."""

    def test_returns_empty_for_no_sequences(self, mock_db):
        """Should return empty list when no sequences."""
        mock_db.query.return_value = MockQuery([])

        service = SequenceCurationService(mock_db)
        results = service.get_root_sequences()

        assert results == []

    def test_groups_by_assembly(self, mock_db):
        """Should group sequences by assembly."""
        rows = [
            MockRow(
                feature_no=1,
                feature_name="ChrA",
                feature_type="chromosome",
                seq_no=1,
                seq_length=1000000,
                seq_source="Assembly2020",
            ),
            MockRow(
                feature_no=2,
                feature_name="ChrB",
                feature_type="chromosome",
                seq_no=2,
                seq_length=800000,
                seq_source="Assembly2020",
            ),
            MockRow(
                feature_no=3,
                feature_name="ChrA",
                feature_type="chromosome",
                seq_no=3,
                seq_length=1000100,
                seq_source="Assembly2022",
            ),
        ]
        mock_db.query.return_value = MockQuery(rows)

        service = SequenceCurationService(mock_db)
        results = service.get_root_sequences()

        assert len(results) == 2
        assemblies = {r["assembly"] for r in results}
        assert "Assembly2020" in assemblies
        assert "Assembly2022" in assemblies


class TestGetSequenceSegment:
    """Tests for getting sequence segments."""

    def test_returns_none_for_unknown_feature(self, mock_db):
        """Should return None for unknown feature."""
        mock_db.query.return_value = MockQuery([])

        service = SequenceCurationService(mock_db)
        result = service.get_sequence_segment("Unknown", 1, 100)

        assert result is None

    def test_returns_segment(self, mock_db, sample_chromosome):
        """Should return sequence segment."""
        feature, seq = sample_chromosome
        mock_db.query.return_value = MockQuery([(feature, seq)])

        service = SequenceCurationService(mock_db)
        result = service.get_sequence_segment("ChrA", 1, 10)

        assert result is not None
        assert result["feature_name"] == "ChrA"
        assert result["start"] == 1
        assert result["end"] == 10
        assert len(result["sequence"]) == 10
        assert result["sequence"] == "ATGCATGCAT"

    def test_handles_out_of_range_start(self, mock_db, sample_chromosome):
        """Should handle start position beyond sequence length."""
        feature, seq = sample_chromosome
        mock_db.query.return_value = MockQuery([(feature, seq)])

        service = SequenceCurationService(mock_db)
        result = service.get_sequence_segment("ChrA", 200, 10)

        assert result["error"] is not None
        assert "exceeds sequence length" in result["error"]

    def test_adjusts_negative_start(self, mock_db, sample_chromosome):
        """Should adjust negative start to 1."""
        feature, seq = sample_chromosome
        mock_db.query.return_value = MockQuery([(feature, seq)])

        service = SequenceCurationService(mock_db)
        result = service.get_sequence_segment("ChrA", -5, 10)

        assert result["start"] == 1

    def test_clips_end_to_sequence_length(self, mock_db, sample_chromosome):
        """Should clip end position to sequence length."""
        feature, seq = sample_chromosome
        mock_db.query.return_value = MockQuery([(feature, seq)])

        service = SequenceCurationService(mock_db)
        result = service.get_sequence_segment("ChrA", 95, 20)

        assert result["end"] == 100  # Clipped to seq_length


class TestPreviewChanges:
    """Tests for previewing changes."""

    def test_returns_error_for_unknown_feature(self, mock_db):
        """Should return error for unknown feature."""
        mock_db.query.return_value = MockQuery([])

        service = SequenceCurationService(mock_db)
        result = service.preview_changes("Unknown", [])

        assert "error" in result

    def test_previews_insertion(self, mock_db, sample_chromosome):
        """Should preview insertion change."""
        feature, seq = sample_chromosome
        mock_db.query.side_effect = [
            MockQuery([(feature, seq)]),  # Feature/seq lookup
            MockQuery([]),  # Affected features
        ]

        service = SequenceCurationService(mock_db)
        result = service.preview_changes("ChrA", [
            {"type": "insertion", "position": 10, "sequence": "NNNN"}
        ])

        assert result["old_length"] == 100
        assert result["new_length"] == 104
        assert result["net_change"] == 4
        assert len(result["changes"]) == 1
        assert result["changes"][0]["type"] == "insertion"

    def test_previews_deletion(self, mock_db, sample_chromosome):
        """Should preview deletion change."""
        feature, seq = sample_chromosome
        mock_db.query.side_effect = [
            MockQuery([(feature, seq)]),  # Feature/seq lookup
            MockQuery([]),  # Affected features
        ]

        service = SequenceCurationService(mock_db)
        result = service.preview_changes("ChrA", [
            {"type": "deletion", "start": 10, "end": 15}
        ])

        assert result["old_length"] == 100
        assert result["new_length"] == 94
        assert result["net_change"] == -6
        assert result["changes"][0]["type"] == "deletion"

    def test_previews_substitution(self, mock_db, sample_chromosome):
        """Should preview substitution change."""
        feature, seq = sample_chromosome
        mock_db.query.side_effect = [
            MockQuery([(feature, seq)]),  # Feature/seq lookup
            MockQuery([]),  # Affected features
        ]

        service = SequenceCurationService(mock_db)
        result = service.preview_changes("ChrA", [
            {"type": "substitution", "start": 10, "end": 12, "sequence": "NNN"}
        ])

        # Substitution replaces 3 bases with 3 bases
        assert result["old_length"] == 100
        assert result["new_length"] == 100
        assert result["net_change"] == 0
        assert result["changes"][0]["type"] == "substitution"


class TestGetContext:
    """Tests for getting sequence context."""

    def test_returns_context_around_position(self, mock_db):
        """Should return context around position."""
        service = SequenceCurationService(mock_db)
        sequence = "ATGCATGCATGCATGCATGC"

        result = service._get_context(sequence, 10, 5)

        # Should get 10 chars before and some after
        assert len(result) > 5


class TestGetAffectedFeatures:
    """Tests for getting affected features."""

    def test_returns_empty_for_no_changes(self, mock_db):
        """Should return empty list for no changes."""
        service = SequenceCurationService(mock_db)
        result = service._get_affected_features(1, [], 0)

        assert result == []

    def test_finds_overlapping_features(self, mock_db):
        """Should find features overlapping with changes."""
        feature = MockFeature(2, "CAL0001", "ORF", "ALS1")
        location = MockFeatLocation(1, 2, 1, 100, 500, "W")

        mock_db.query.return_value = MockQuery([(feature, location)])

        service = SequenceCurationService(mock_db)
        result = service._get_affected_features(
            1,
            [{"type": "deletion", "start": 200, "end": 300}],
            -100
        )

        assert len(result) == 1
        assert result[0]["feature_name"] == "CAL0001"
        assert result[0]["is_overlapping"] is True


class TestGetNearbyFeatures:
    """Tests for getting nearby features."""

    def test_returns_empty_for_unknown_feature(self, mock_db):
        """Should return empty list for unknown feature."""
        mock_db.query.return_value = MockQuery([])

        service = SequenceCurationService(mock_db)
        result = service.get_nearby_features("Unknown", 1000)

        assert result == []

    def test_finds_features_in_range(self, mock_db, sample_chromosome):
        """Should find features within range."""
        feature, seq = sample_chromosome
        gene = MockFeature(2, "CAL0001", "ORF", "ALS1")
        location = MockFeatLocation(1, 2, 1, 40, 60, "W")

        mock_db.query.side_effect = [
            MockQuery([(feature, seq)]),  # Root seq lookup
            MockQuery([(gene, location)]),  # Features in range
        ]

        service = SequenceCurationService(mock_db)
        result = service.get_nearby_features("ChrA", 50, range_size=20)

        assert len(result) == 1
        assert result[0]["feature_name"] == "CAL0001"
        assert result[0]["gene_name"] == "ALS1"


class TestServiceInitialization:
    """Tests for service initialization."""

    def test_stores_db_session(self, mock_db):
        """Should store database session."""
        service = SequenceCurationService(mock_db)
        assert service.db is mock_db
