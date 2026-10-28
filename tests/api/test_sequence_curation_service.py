"""
Tests for Sequence Curation Service.

Tests cover:
- Root sequence retrieval
- Sequence segment extraction
- Change preview (insertion, deletion, substitution)
- Affected features detection
- Nearby features search
- Edit normalization, application and coordinate adjustment (indels)
- Reference identifier resolution (reference_no or PMID)
"""
import pytest
from unittest.mock import MagicMock

from cgd.api.services.curation.sequence_curation_service import (
    SequenceCurationError,
    SequenceCurationService,
    adjust_coordinate,
    apply_edits,
    edit_containing,
    normalize_changes,
    overlaps_edit,
    resolve_reference_nos,
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


SEQ = "AAAAACCCCCGGGGGTTTTT"   # 20 bp; positions 1-5 A, 6-10 C, 11-15 G, 16-20 T


class TestNormalizeChanges:
    """Tests for turning curator changes into original-coordinate edits."""

    def test_insertion_is_an_empty_window_after_its_position(self):
        (edit,) = normalize_changes([{"type": "insertion", "position": 5, "sequence": "tt"}], SEQ)
        assert (edit.start, edit.end, edit.new, edit.old, edit.delta) == (6, 5, "TT", "", 2)

    def test_deletion_records_the_removed_residues(self):
        (edit,) = normalize_changes([{"type": "deletion", "start": 6, "end": 8}], SEQ)
        assert (edit.old, edit.new, edit.delta) == ("CCC", "", -3)

    def test_sorted_by_position_whatever_the_entry_order(self):
        edits = normalize_changes([
            {"type": "deletion", "start": 16, "end": 16},
            {"type": "insertion", "position": 2, "sequence": "G"},
        ], SEQ)
        assert [e.type for e in edits] == ["insertion", "deletion"]

    def test_rejects_overlapping_changes(self):
        with pytest.raises(SequenceCurationError, match="overlap"):
            normalize_changes([
                {"type": "deletion", "start": 6, "end": 10},
                {"type": "substitution", "start": 9, "end": 12, "sequence": "AAAA"},
            ], SEQ)

    def test_rejects_an_insertion_inside_a_deletion(self):
        with pytest.raises(SequenceCurationError, match="overlap"):
            normalize_changes([
                {"type": "deletion", "start": 6, "end": 10},
                {"type": "insertion", "position": 7, "sequence": "A"},
            ], SEQ)

    def test_allows_an_insertion_just_before_a_deletion(self):
        edits = normalize_changes([
            {"type": "deletion", "start": 6, "end": 10},
            {"type": "insertion", "position": 5, "sequence": "A"},
        ], SEQ)
        assert len(edits) == 2

    def test_rejects_non_nucleotide_characters(self):
        with pytest.raises(SequenceCurationError, match="IUPAC"):
            normalize_changes([{"type": "insertion", "position": 1, "sequence": "AXZ"}], SEQ)

    def test_rejects_coordinates_outside_the_sequence(self):
        with pytest.raises(SequenceCurationError, match="outside"):
            normalize_changes([{"type": "deletion", "start": 18, "end": 25}], SEQ)

    def test_rejects_a_stale_preview(self):
        with pytest.raises(SequenceCurationError, match="re-run the preview"):
            normalize_changes(
                [{"type": "deletion", "start": 6, "end": 8, "old_sequence": "GGG"}], SEQ
            )


class TestApplyEditsAndAdjustCoordinates:
    """Tests for the edited sequence and the coordinate mapping."""

    def test_applies_mixed_edits_in_original_coordinates(self):
        edits = normalize_changes([
            {"type": "substitution", "start": 11, "end": 15, "sequence": "AC"},
            {"type": "insertion", "position": 0, "sequence": "TT"},
            {"type": "deletion", "start": 6, "end": 10},
        ], SEQ)
        assert apply_edits(SEQ, edits) == "TT" + "AAAAA" + "AC" + "TTTTT"

    def test_coordinates_shift_by_edits_ending_before_them(self):
        edits = normalize_changes([
            {"type": "insertion", "position": 5, "sequence": "GG"},   # +2 after 5
            {"type": "deletion", "start": 11, "end": 12},             # -2 after 12
        ], SEQ)
        assert adjust_coordinate(5, edits) == 5      # before the insertion
        assert adjust_coordinate(6, edits) == 8      # after it
        assert adjust_coordinate(13, edits) == 13    # after both
        new = apply_edits(SEQ, edits)
        # residue identity is preserved for every coordinate outside the edits
        for coord in (1, 5, 6, 10, 13, 20):
            assert new[adjust_coordinate(coord, edits) - 1] == SEQ[coord - 1]

    def test_boundary_inside_a_length_changing_edit_is_flagged(self):
        edits = normalize_changes([{"type": "deletion", "start": 6, "end": 10}], SEQ)
        assert edit_containing(8, edits) is not None
        assert edit_containing(5, edits) is None
        assert edit_containing(11, edits) is None

    def test_equal_length_substitution_never_flags_a_boundary(self):
        edits = normalize_changes(
            [{"type": "substitution", "start": 6, "end": 10, "sequence": "AAAAA"}], SEQ
        )
        assert edit_containing(8, edits) is None

    def test_overlap_counts_a_feature_spanning_an_insertion_point(self):
        edits = normalize_changes([{"type": "insertion", "position": 10, "sequence": "A"}], SEQ)
        assert overlaps_edit(5, 15, edits)
        assert overlaps_edit(15, 5, edits)           # Crick: start > stop
        assert not overlaps_edit(1, 10, edits)       # ends at the insertion point
        assert not overlaps_edit(11, 20, edits)      # starts just after it

    def test_frameshift_fix_merges_two_orfs(self):
        """
        Mirrors the KAR5 case: an extra base splits one ORF in two, and deleting
        it joins them into a single reading frame.
        """
        orf = "ATG" + "GCT" * 5 + "TAA"
        broken = "CC" + orf[:9] + "T" + orf[9:] + "CC"
        edits = normalize_changes([{"type": "deletion", "start": 12, "end": 12}], broken)
        fixed = apply_edits(broken, edits)
        start, stop = 3, adjust_coordinate(len(broken) - 2, edits)
        assert fixed[start - 1:stop] == orf


class TestPreviewWithIndels:
    """Tests for preview coordinates with length-changing edits."""

    def test_overlapping_feature_keeps_start_and_moves_stop(self, mock_db, sample_chromosome):
        feature, seq = sample_chromosome
        orf = MockFeature(2, "ORF1", "ORF")
        downstream = MockFeature(3, "ORF2", "ORF")
        mock_db.query.side_effect = [
            MockQuery([(feature, seq)]),
            MockQuery([
                (orf, MockFeatLocation(1, 2, 1, 10, 60, "W")),
                (downstream, MockFeatLocation(2, 3, 1, 90, 70, "C")),
            ]),
        ]
        service = SequenceCurationService(mock_db)
        result = service.preview_changes("ChrA", [
            {"type": "deletion", "start": 30, "end": 32},
            {"type": "insertion", "position": 20, "sequence": "A"},
        ])
        assert result["net_change"] == -2
        first, second = result["affected_features"]
        assert (first["new_start"], first["new_stop"]) == (10, 58)
        assert first["is_overlapping"] and not first["boundary_in_edit"]
        assert (second["new_start"], second["new_stop"]) == (88, 68)
        assert second["is_downstream"]

    def test_flags_a_feature_end_inside_a_deletion(self, mock_db, sample_chromosome):
        feature, seq = sample_chromosome
        orf = MockFeature(2, "ORF1", "ORF")
        mock_db.query.side_effect = [
            MockQuery([(feature, seq)]),
            MockQuery([(orf, MockFeatLocation(1, 2, 1, 10, 31, "W"))]),
        ]
        service = SequenceCurationService(mock_db)
        result = service.preview_changes("ChrA", [{"type": "deletion", "start": 30, "end": 32}])
        assert result["affected_features"][0]["boundary_in_edit"] is True

    def test_invalid_change_raises(self, mock_db, sample_chromosome):
        feature, seq = sample_chromosome
        mock_db.query.side_effect = [MockQuery([(feature, seq)])]
        service = SequenceCurationService(mock_db)
        with pytest.raises(SequenceCurationError):
            service.preview_changes("ChrA", [{"type": "deletion", "start": 50, "end": 40}])


class TestResolveReferenceNos:
    """Curator-entered references may be CGD reference_nos or PMIDs."""

    def test_empty(self, mock_db):
        assert resolve_reference_nos(mock_db, []) == ([], [])
        mock_db.query.assert_not_called()

    def test_reference_no_pmid_and_unknown(self, mock_db):
        mock_db.query.side_effect = [
            MockQuery([(68332,)]),                # reference_no lookup
            MockQuery([(37561787, 68332), (111, 5)]),  # PMID lookup
        ]
        resolved, unknown = resolve_reference_nos(mock_db, [68332, 37561787, 111, 38362496])
        # the PMID of an already-listed reference collapses onto it
        assert resolved == [68332, 5]
        assert unknown == [38362496]

    def test_all_reference_nos_skips_pmid_lookup(self, mock_db):
        mock_db.query.side_effect = [MockQuery([(7,), (9,)])]
        assert resolve_reference_nos(mock_db, [7, 9, 7]) == ([7, 9], [])
        assert mock_db.query.call_count == 1

    def test_apply_changes_rejects_unknown_reference_before_writing(self, mock_db):
        mock_db.query.side_effect = [MockQuery([]), MockQuery([])]
        service = SequenceCurationService(mock_db)
        with pytest.raises(SequenceCurationError, match="38362496"):
            service.apply_changes(
                "ChrA", [{"type": "deletion", "start": 5, "end": 5}], "note", "CUR",
                reference_nos=[38362496], dry_run=False,
            )
        mock_db.add.assert_not_called()
