"""
Tests for Sequence Service.

Tests cover:
- Reverse complement calculation
- FASTA header formatting
- Sequence formatting
- Getting sequence by feature
- Getting sequence by coordinates
- Flanking region handling
"""
import pytest
from unittest.mock import MagicMock

from cgd.api.services.sequence_service import (
    _reverse_complement,
    _format_fasta_header,
    _format_sequence,
    get_sequence_by_feature,
    get_sequence_by_coordinates,
    format_as_fasta,
)
from cgd.schemas.sequence_schema import SeqType


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
        organism_no: int = 1,
        organism: MockOrganism = None,
        feature_type: str = "ORF",
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name
        self.dbxref_id = dbxref_id
        self.organism_no = organism_no
        self.organism = organism
        self.feature_type = feature_type


class MockSeq:
    """Mock Seq model."""

    def __init__(
        self,
        seq_no: int,
        feature_no: int,
        residues: str,
        seq_type: str = "genomic",
        is_seq_current: str = "Y",
        feature: MockFeature = None,
    ):
        self.seq_no = seq_no
        self.feature_no = feature_no
        self.residues = residues
        self.seq_type = seq_type
        self.is_seq_current = is_seq_current
        self.feature = feature


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


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []

    def outerjoin(self, *args, **kwargs):
        return self

    def join(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
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
        organism=sample_organism,
    )


@pytest.fixture
def sample_seq(sample_feature):
    """Create sample sequence."""
    return MockSeq(
        seq_no=1,
        feature_no=1,
        residues="atgcatgcatgc",
        seq_type="genomic",
    )


class TestReverseComplement:
    """Tests for reverse complement function."""

    def test_simple_sequence(self):
        """Should return correct reverse complement."""
        assert _reverse_complement("ATGC") == "GCAT"

    def test_lowercase_sequence(self):
        """Should handle lowercase input."""
        assert _reverse_complement("atgc") == "gcat"

    def test_empty_sequence(self):
        """Should handle empty sequence."""
        assert _reverse_complement("") == ""

    def test_longer_sequence(self):
        """Should handle longer sequences."""
        # AAAAGGGGCCCCTTTT is a palindrome - reverse complement equals original
        result = _reverse_complement("AAAAGGGGCCCCTTTT")
        assert result == "AAAAGGGGCCCCTTTT"


class TestFormatFastaHeader:
    """Tests for FASTA header formatting."""

    def test_with_gene_name(self):
        """Should use gene_name as primary identifier."""
        result = _format_fasta_header(
            feature_name="CAL0001",
            gene_name="ALS1",
        )
        assert result.startswith(">ALS1")

    def test_without_gene_name(self):
        """Should use feature_name when no gene_name."""
        result = _format_fasta_header(
            feature_name="CAL0001",
            gene_name=None,
        )
        assert result.startswith(">CAL0001")

    def test_with_dbxref_id(self):
        """Should include CGDID."""
        result = _format_fasta_header(
            feature_name="CAL0001",
            dbxref_id="CGD:CAL0001",
        )
        assert "CGDID:CGD:CAL0001" in result

    def test_with_coordinates(self):
        """Should include coordinates."""
        result = _format_fasta_header(
            feature_name="CAL0001",
            chromosome="Chr1",
            start=100,
            end=500,
            strand="W",
        )
        assert "Chr1:100-500(+)" in result

    def test_crick_strand(self):
        """Should show minus for Crick strand."""
        result = _format_fasta_header(
            feature_name="CAL0001",
            chromosome="Chr1",
            start=100,
            end=500,
            strand="C",
        )
        assert "(-)" in result


class TestFormatSequence:
    """Tests for sequence formatting."""

    def test_wraps_at_line_width(self):
        """Should wrap sequence at specified width."""
        seq = "A" * 100
        result = _format_sequence(seq, line_width=60)
        lines = result.split("\n")
        assert len(lines[0]) == 60
        assert len(lines[1]) == 40

    def test_short_sequence(self):
        """Should not wrap short sequences."""
        seq = "ATGC"
        result = _format_sequence(seq, line_width=60)
        assert result == "ATGC"


class TestGetSequenceByFeature:
    """Tests for getting sequence by feature."""

    def test_returns_none_for_unknown_feature(self, mock_db):
        """Should return None for unknown feature."""
        mock_db.query.return_value = MockQuery([])

        result = get_sequence_by_feature(mock_db, "UNKNOWN")

        assert result is None

    def test_finds_feature_by_gene_name(self, mock_db, sample_feature, sample_seq):
        """Should find feature by gene name."""
        mock_db.query.side_effect = [
            MockQuery([sample_feature]),  # Found by gene_name
            MockQuery([sample_seq]),  # Sequence
            MockQuery([]),  # Location (none)
        ]

        result = get_sequence_by_feature(mock_db, "ALS1")

        assert result is not None
        assert result.info.gene_name == "ALS1"

    def test_finds_feature_by_feature_name(self, mock_db, sample_feature, sample_seq):
        """Should find feature by feature name."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Not found by gene_name
            MockQuery([sample_feature]),  # Found by feature_name
            MockQuery([sample_seq]),  # Sequence
            MockQuery([]),  # Location
        ]

        result = get_sequence_by_feature(mock_db, "CAL0001")

        assert result is not None
        assert result.info.feature_name == "CAL0001"

    def test_finds_feature_by_dbxref_id(self, mock_db, sample_feature, sample_seq):
        """Should find feature by dbxref_id."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Not by gene_name
            MockQuery([]),  # Not by feature_name
            MockQuery([sample_feature]),  # Found by dbxref_id
            MockQuery([sample_seq]),  # Sequence
            MockQuery([]),  # Location
        ]

        result = get_sequence_by_feature(mock_db, "CGD:CAL0001")

        assert result is not None

    def test_returns_none_when_no_sequence(self, mock_db, sample_feature):
        """Should return None when feature has no sequence."""
        mock_db.query.side_effect = [
            MockQuery([sample_feature]),  # Feature found
            MockQuery([]),  # No sequence
        ]

        result = get_sequence_by_feature(mock_db, "ALS1")

        assert result is None

    def test_returns_uppercase_sequence(self, mock_db, sample_feature, sample_seq):
        """Should return uppercase sequence."""
        mock_db.query.side_effect = [
            MockQuery([sample_feature]),
            MockQuery([sample_seq]),  # Has lowercase residues
            MockQuery([]),
        ]

        result = get_sequence_by_feature(mock_db, "ALS1")

        assert result.sequence == "ATGCATGCATGC"

    def test_includes_fasta_header(self, mock_db, sample_feature, sample_seq):
        """Should include FASTA header."""
        mock_db.query.side_effect = [
            MockQuery([sample_feature]),
            MockQuery([sample_seq]),
            MockQuery([]),
        ]

        result = get_sequence_by_feature(mock_db, "ALS1")

        assert result.fasta_header.startswith(">")
        assert "ALS1" in result.fasta_header

    def test_protein_sequence_type(self, mock_db, sample_feature):
        """Should request protein sequence type."""
        protein_seq = MockSeq(1, 1, "MKATGC", "protein")
        mock_db.query.side_effect = [
            MockQuery([sample_feature]),
            MockQuery([protein_seq]),
            MockQuery([]),
        ]

        result = get_sequence_by_feature(mock_db, "ALS1", seq_type=SeqType.PROTEIN)

        assert result.sequence == "MKATGC"
        assert result.info.seq_type == "protein"

    def test_reverse_complement_option(self, mock_db, sample_feature, sample_seq):
        """Should apply reverse complement when requested."""
        mock_db.query.side_effect = [
            MockQuery([sample_feature]),
            MockQuery([sample_seq]),  # "atgcatgcatgc"
            MockQuery([]),
        ]

        result = get_sequence_by_feature(
            mock_db, "ALS1", reverse_complement=True
        )

        # Reverse complement of ATGCATGCATGC
        assert result.sequence == "GCATGCATGCAT"


class TestGetSequenceByCoordinates:
    """Tests for getting sequence by coordinates."""

    def test_returns_none_for_unknown_chromosome(self, mock_db):
        """Should return None for unknown chromosome."""
        mock_db.query.side_effect = [
            MockQuery([]),  # First search
            MockQuery([]),  # Second search
        ]

        result = get_sequence_by_coordinates(mock_db, "ChrUnknown", 1, 100)

        assert result is None

    def test_extracts_sequence_region(self, mock_db):
        """Should extract correct sequence region."""
        chr_feature = MockFeature(1, "Chr1", feature_type="chromosome")
        chr_seq = MockSeq(1, 1, "AAAAATGCATGCTTTT", "genomic", feature=chr_feature)

        mock_db.query.return_value = MockQuery([chr_seq])

        result = get_sequence_by_coordinates(mock_db, "Chr1", 6, 13)

        assert result.sequence == "TGCATGCT"

    def test_returns_uppercase(self, mock_db):
        """Should return uppercase sequence."""
        chr_feature = MockFeature(1, "Chr1", feature_type="chromosome")
        chr_seq = MockSeq(1, 1, "aaaaatgcatgctttt", "genomic", feature=chr_feature)

        mock_db.query.return_value = MockQuery([chr_seq])

        result = get_sequence_by_coordinates(mock_db, "Chr1", 1, 10)

        assert result.sequence == result.sequence.upper()

    def test_crick_strand_reverse_complements(self, mock_db):
        """Should reverse complement for Crick strand."""
        chr_feature = MockFeature(1, "Chr1", feature_type="chromosome")
        chr_seq = MockSeq(1, 1, "ATGC", "genomic", feature=chr_feature)

        mock_db.query.return_value = MockQuery([chr_seq])

        result = get_sequence_by_coordinates(mock_db, "Chr1", 1, 4, strand="C")

        assert result.sequence == "GCAT"

    def test_includes_fasta_header(self, mock_db):
        """Should include FASTA header with coordinates."""
        chr_feature = MockFeature(1, "Chr1", feature_type="chromosome")
        chr_seq = MockSeq(1, 1, "ATGCATGC", "genomic", feature=chr_feature)

        mock_db.query.return_value = MockQuery([chr_seq])

        result = get_sequence_by_coordinates(mock_db, "Chr1", 1, 8)

        assert "Chr1:1-8" in result.fasta_header

    def test_handles_out_of_bounds(self, mock_db):
        """Should handle coordinates beyond sequence length."""
        chr_feature = MockFeature(1, "Chr1", feature_type="chromosome")
        chr_seq = MockSeq(1, 1, "ATGC", "genomic", feature=chr_feature)

        mock_db.query.return_value = MockQuery([chr_seq])

        result = get_sequence_by_coordinates(mock_db, "Chr1", 1, 100)

        # Should return whatever is available
        assert result.sequence == "ATGC"
        assert result.length == 4


class TestFormatAsFasta:
    """Tests for formatting as FASTA."""

    def test_formats_correctly(self):
        """Should format header and sequence."""
        result = format_as_fasta(">test", "ATGCATGC", line_width=4)

        assert result == ">test\nATGC\nATGC"

    def test_preserves_header(self):
        """Should preserve header as-is."""
        result = format_as_fasta(">Gene1 CGDID:123", "ATGC")

        assert result.startswith(">Gene1 CGDID:123\n")
