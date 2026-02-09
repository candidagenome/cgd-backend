"""
Tests for Chromosome Service.

Tests cover:
- Organism info extraction
- Chromosome lookup by name
- Current location retrieval
- Sequence source retrieval
- History summary
- Chromosome detail endpoint
- Chromosome history endpoint
- Chromosome references endpoint
- Chromosome summary notes endpoint
- Chromosome list endpoint
"""
import pytest
from unittest.mock import MagicMock
from datetime import datetime
from fastapi import HTTPException

from cgd.api.services.chromosome_service import (
    _get_organism_info,
    _get_chromosome_by_name,
    _get_current_location,
    _get_seq_source,
    _get_history_summary,
    get_chromosome,
    get_chromosome_history,
    get_chromosome_references,
    get_chromosome_summary_notes,
    list_chromosomes,
)


class MockOrganism:
    """Mock Organism model."""

    def __init__(
        self,
        organism_no: int,
        organism_name: str,
        organism_abbrev: str = None,
        taxon_id: int = 0,
    ):
        self.organism_no = organism_no
        self.organism_name = organism_name
        self.organism_abbrev = organism_abbrev
        self.taxon_id = taxon_id


class MockFeature:
    """Mock Feature model."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        feature_type: str = "chromosome",
        dbxref_id: str = None,
        headline: str = None,
        organism_no: int = 1,
        organism: MockOrganism = None,
        feat_alias: list = None,
        seq: list = None,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.feature_type = feature_type
        self.dbxref_id = dbxref_id
        self.headline = headline
        self.organism_no = organism_no
        self.organism = organism
        self.feat_alias = feat_alias or []
        self.seq = seq or []


class MockFeatAlias:
    """Mock FeatAlias model."""

    def __init__(self, alias):
        self.alias = alias


class MockAlias:
    """Mock Alias model."""

    def __init__(self, alias_name: str, alias_type: str = None):
        self.alias_name = alias_name
        self.alias_type = alias_type


class MockFeatLocation:
    """Mock FeatLocation model."""

    def __init__(
        self,
        feature_no: int,
        start_coord: int,
        stop_coord: int,
        is_loc_current: str = "Y",
    ):
        self.feature_no = feature_no
        self.start_coord = start_coord
        self.stop_coord = stop_coord
        self.is_loc_current = is_loc_current


class MockSeq:
    """Mock Seq model."""

    def __init__(
        self,
        seq_no: int,
        feature_no: int,
        source: str = None,
        is_seq_current: str = "Y",
    ):
        self.seq_no = seq_no
        self.feature_no = feature_no
        self.source = source
        self.is_seq_current = is_seq_current


class MockSeqChangeArchive:
    """Mock SeqChangeArchive model."""

    def __init__(
        self,
        seq_change_archive_no: int,
        seq_no: int,
        date_created: datetime = None,
        change_start_coord: int = None,
        change_stop_coord: int = None,
        seq_change_type: str = None,
        old_seq: str = None,
        new_seq: str = None,
    ):
        self.seq_change_archive_no = seq_change_archive_no
        self.seq_no = seq_no
        self.date_created = date_created or datetime.now()
        self.change_start_coord = change_start_coord
        self.change_stop_coord = change_stop_coord
        self.seq_change_type = seq_change_type
        self.old_seq = old_seq
        self.new_seq = new_seq


class MockNote:
    """Mock Note model."""

    def __init__(
        self,
        note_no: int,
        note: str,
        note_type: str = None,
        date_created: datetime = None,
    ):
        self.note_no = note_no
        self.note = note
        self.note_type = note_type
        self.date_created = date_created or datetime.now()


class MockNoteLink:
    """Mock NoteLink model."""

    def __init__(
        self,
        note_no: int,
        tab_name: str,
        primary_key: int,
        note: MockNote = None,
    ):
        self.note_no = note_no
        self.tab_name = tab_name
        self.primary_key = primary_key
        self.note = note


class MockReference:
    """Mock Reference model."""

    def __init__(
        self,
        reference_no: int,
        pubmed: int = None,
        citation: str = None,
        title: str = None,
        year: int = None,
    ):
        self.reference_no = reference_no
        self.pubmed = pubmed
        self.citation = citation
        self.title = title
        self.year = year


class MockRefLink:
    """Mock RefLink model."""

    def __init__(
        self,
        reference_no: int,
        tab_name: str,
        primary_key: int,
        reference: MockReference = None,
    ):
        self.reference_no = reference_no
        self.tab_name = tab_name
        self.primary_key = primary_key
        self.reference = reference


class MockParagraph:
    """Mock Paragraph model."""

    def __init__(
        self,
        paragraph_no: int,
        paragraph_text: str,
        date_edited: datetime = None,
    ):
        self.paragraph_no = paragraph_no
        self.paragraph_text = paragraph_text
        self.date_edited = date_edited


class MockFeatPara:
    """Mock FeatPara model."""

    def __init__(
        self,
        feature_no: int,
        paragraph_no: int,
        paragraph_order: int,
        paragraph: MockParagraph = None,
    ):
        self.feature_no = feature_no
        self.paragraph_no = paragraph_no
        self.paragraph_order = paragraph_order
        self.paragraph = paragraph


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []

    def options(self, *args, **kwargs):
        return self

    def join(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def first(self):
        return self._results[0] if self._results else None

    def all(self):
        return self._results

    def count(self):
        return len(self._results)


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    db = MagicMock()
    db.query.return_value = MockQuery([])
    return db


@pytest.fixture
def sample_organism():
    """Create sample organism."""
    return MockOrganism(1, "Candida albicans SC5314", "C_albicans_SC5314", 5476)


@pytest.fixture
def sample_chromosome(sample_organism):
    """Create sample chromosome feature."""
    return MockFeature(
        feature_no=1,
        feature_name="Chr1",
        feature_type="chromosome",
        dbxref_id="CGD:CHR001",
        headline="Chromosome 1",
        organism=sample_organism,
    )


@pytest.fixture
def sample_contig(sample_organism):
    """Create sample contig feature."""
    return MockFeature(
        feature_no=2,
        feature_name="Contig1",
        feature_type="contig",
        organism=sample_organism,
    )


class TestGetOrganismInfo:
    """Tests for _get_organism_info."""

    def test_returns_organism_name_and_taxon(self, sample_chromosome):
        """Should return organism name and taxon_id."""
        name, taxon = _get_organism_info(sample_chromosome)

        assert name == "Candida albicans SC5314"
        assert taxon == 5476

    def test_returns_organism_no_if_no_name(self):
        """Should return organism_no as string if no name."""
        org = MockOrganism(1, None)
        feature = MockFeature(1, "Chr1", organism=org)

        name, taxon = _get_organism_info(feature)

        assert name == "1"

    def test_handles_no_organism(self):
        """Should handle feature with no organism."""
        feature = MockFeature(1, "Chr1", organism=None)

        name, taxon = _get_organism_info(feature)

        assert taxon == 0


class TestGetChromosomeByName:
    """Tests for _get_chromosome_by_name."""

    def test_raises_404_if_not_found(self, mock_db):
        """Should raise 404 if chromosome not found."""
        mock_db.query.return_value = MockQuery([])

        with pytest.raises(HTTPException) as exc_info:
            _get_chromosome_by_name(mock_db, "Unknown")

        assert exc_info.value.status_code == 404

    def test_raises_400_if_not_chromosome(self, mock_db, sample_organism):
        """Should raise 400 if feature is not chromosome or contig."""
        orf = MockFeature(1, "Gene1", feature_type="ORF", organism=sample_organism)
        mock_db.query.return_value = MockQuery([orf])

        with pytest.raises(HTTPException) as exc_info:
            _get_chromosome_by_name(mock_db, "Gene1")

        assert exc_info.value.status_code == 400
        assert "not a chromosome or contig" in exc_info.value.detail

    def test_returns_chromosome_feature(self, mock_db, sample_chromosome):
        """Should return chromosome feature."""
        mock_db.query.return_value = MockQuery([sample_chromosome])

        result = _get_chromosome_by_name(mock_db, "Chr1")

        assert result.feature_name == "Chr1"
        assert result.feature_type == "chromosome"

    def test_returns_contig_feature(self, mock_db, sample_contig):
        """Should return contig feature."""
        mock_db.query.return_value = MockQuery([sample_contig])

        result = _get_chromosome_by_name(mock_db, "Contig1")

        assert result.feature_type == "contig"


class TestGetCurrentLocation:
    """Tests for _get_current_location."""

    def test_returns_coordinates(self, mock_db):
        """Should return start and stop coordinates."""
        location = MockFeatLocation(1, 1, 1000000)
        mock_db.query.return_value = MockQuery([location])

        start, stop, _ = _get_current_location(mock_db, 1)

        assert start == 1
        assert stop == 1000000

    def test_returns_none_if_no_location(self, mock_db):
        """Should return None if no current location."""
        mock_db.query.return_value = MockQuery([])

        start, stop, _ = _get_current_location(mock_db, 1)

        assert start is None
        assert stop is None


class TestGetSeqSource:
    """Tests for _get_seq_source."""

    def test_returns_source(self, mock_db):
        """Should return sequence source."""
        seq = MockSeq(1, 1, source="Assembly 27")
        mock_db.query.return_value = MockQuery([seq])

        result = _get_seq_source(mock_db, 1)

        assert result == "Assembly 27"

    def test_returns_none_if_no_seq(self, mock_db):
        """Should return None if no current sequence."""
        mock_db.query.return_value = MockQuery([])

        result = _get_seq_source(mock_db, 1)

        assert result is None


class TestGetHistorySummary:
    """Tests for _get_history_summary."""

    def test_returns_summary_with_no_history(self, mock_db):
        """Should return zero counts when no history."""
        mock_db.query.side_effect = [
            MockQuery([]),  # seq query
            MockQuery([]),  # annotation notes
            MockQuery([]),  # curatorial notes count
        ]

        result = _get_history_summary(mock_db, 1)

        assert result.sequence_updates == 0
        assert result.annotation_updates == 0
        assert result.curatorial_notes == 0

    def test_counts_sequence_changes(self, mock_db):
        """Should count sequence changes."""
        seq = MockSeq(1, 1)
        change = MockSeqChangeArchive(1, 1)
        mock_db.query.side_effect = [
            MockQuery([seq]),  # seq query
            MockQuery([change]),  # sequence changes
            MockQuery([]),  # annotation notes
            MockQuery([]),  # curatorial notes count
        ]

        result = _get_history_summary(mock_db, 1)

        assert result.sequence_updates == 1


class TestGetChromosome:
    """Tests for get_chromosome."""

    def test_returns_chromosome_response(self, mock_db, sample_chromosome):
        """Should return chromosome response."""
        mock_db.query.side_effect = [
            MockQuery([sample_chromosome]),  # chromosome lookup
            MockQuery([]),  # location
            MockQuery([]),  # seq source
            MockQuery([]),  # history - seq
            MockQuery([]),  # history - annotation
            MockQuery([]),  # history - curatorial
        ]

        result = get_chromosome(mock_db, "Chr1")

        assert result.result.feature_name == "Chr1"
        assert result.result.organism_name == "Candida albicans SC5314"

    def test_includes_aliases(self, mock_db, sample_organism):
        """Should include aliases in response."""
        alias = MockAlias("Chromosome_1", "Systematic")
        feat_alias = MockFeatAlias(alias)
        chromosome = MockFeature(
            1, "Chr1", feature_type="chromosome",
            dbxref_id="CGD:CHR001",  # Required field
            organism=sample_organism, feat_alias=[feat_alias]
        )
        mock_db.query.side_effect = [
            MockQuery([chromosome]),  # chromosome lookup
            MockQuery([]),  # location
            MockQuery([]),  # seq source
            MockQuery([]),  # history queries
            MockQuery([]),
            MockQuery([]),
        ]

        result = get_chromosome(mock_db, "Chr1")

        assert len(result.result.aliases) == 1
        assert result.result.aliases[0].alias_name == "Chromosome_1"


class TestGetChromosomeHistory:
    """Tests for get_chromosome_history."""

    def test_returns_history_response(self, mock_db, sample_chromosome):
        """Should return history response."""
        mock_db.query.side_effect = [
            MockQuery([sample_chromosome]),  # chromosome lookup
            MockQuery([]),  # seq
            MockQuery([]),  # annotation notes
            MockQuery([]),  # curatorial notes
        ]

        result = get_chromosome_history(mock_db, "Chr1")

        assert result.result.feature_name == "Chr1"
        assert result.result.sequence_changes == []
        assert result.result.annotation_changes == []


class TestGetChromosomeReferences:
    """Tests for get_chromosome_references."""

    def test_returns_references_response(self, mock_db, sample_chromosome):
        """Should return references response."""
        ref = MockReference(
            1, pubmed=12345, citation="Author et al. (2023)", title="Test Paper", year=2023
        )
        ref_link = MockRefLink(1, "FEATURE", 1, reference=ref)
        mock_db.query.side_effect = [
            MockQuery([sample_chromosome]),  # chromosome lookup
            MockQuery([ref_link]),  # ref links
        ]

        result = get_chromosome_references(mock_db, "Chr1")

        assert len(result.references) == 1
        assert result.references[0].pubmed == 12345

    def test_returns_empty_when_no_references(self, mock_db, sample_chromosome):
        """Should return empty list when no references."""
        mock_db.query.side_effect = [
            MockQuery([sample_chromosome]),  # chromosome lookup
            MockQuery([]),  # no ref links
        ]

        result = get_chromosome_references(mock_db, "Chr1")

        assert result.references == []


class TestGetChromosomeSummaryNotes:
    """Tests for get_chromosome_summary_notes."""

    def test_returns_summary_notes(self, mock_db, sample_chromosome):
        """Should return summary notes."""
        para = MockParagraph(1, "This is a summary paragraph.", date_edited=datetime.now())
        feat_para = MockFeatPara(1, 1, 1, paragraph=para)
        mock_db.query.side_effect = [
            MockQuery([sample_chromosome]),  # chromosome lookup
            MockQuery([feat_para]),  # feat_para
        ]

        result = get_chromosome_summary_notes(mock_db, "Chr1")

        assert len(result.summary_notes) == 1
        assert result.summary_notes[0].paragraph_text == "This is a summary paragraph."

    def test_returns_empty_when_no_notes(self, mock_db, sample_chromosome):
        """Should return empty list when no summary notes."""
        mock_db.query.side_effect = [
            MockQuery([sample_chromosome]),  # chromosome lookup
            MockQuery([]),  # no feat_para
        ]

        result = get_chromosome_summary_notes(mock_db, "Chr1")

        assert result.summary_notes == []


class TestListChromosomes:
    """Tests for list_chromosomes."""

    def test_returns_grouped_list(self, mock_db, sample_organism):
        """Should return chromosomes grouped by organism."""
        chr1 = MockFeature(1, "Chr1", feature_type="chromosome", organism=sample_organism)
        chr2 = MockFeature(2, "Chr2", feature_type="chromosome", organism=sample_organism)
        mock_db.query.side_effect = [
            MockQuery([chr1, chr2]),  # chromosomes
            MockQuery([]),  # location for chr1
            MockQuery([]),  # location for chr2
        ]

        result = list_chromosomes(mock_db)

        assert len(result.organisms) == 1
        assert result.organisms[0].organism_name == "Candida albicans SC5314"

    def test_returns_empty_when_no_chromosomes(self, mock_db):
        """Should return empty list when no chromosomes."""
        mock_db.query.return_value = MockQuery([])

        result = list_chromosomes(mock_db)

        assert result.organisms == []

    def test_includes_chromosome_length(self, mock_db, sample_organism):
        """Should include chromosome length when available."""
        chr1 = MockFeature(1, "Chr1", feature_type="chromosome", organism=sample_organism)
        location = MockFeatLocation(1, 1, 1000000)
        mock_db.query.side_effect = [
            MockQuery([chr1]),  # chromosomes
            MockQuery([location]),  # location with coords
        ]

        result = list_chromosomes(mock_db)

        assert result.organisms[0].chromosomes[0].length == 1000000
