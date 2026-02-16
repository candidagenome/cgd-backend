"""
Tests for Note Curation Service.

Tests cover:
- Note retrieval by note_no
- Note details with linked entities
- Note search with pagination
- Note creation with entity links
- Note update
- Note deletion
- Entity linking and unlinking
- Notes for entity retrieval
"""
import pytest
from unittest.mock import MagicMock
from datetime import datetime

from cgd.api.services.curation.note_curation_service import (
    NoteCurationService,
    NoteCurationError,
    NOTE_TYPES,
    LINKABLE_TABLES,
)


class MockNote:
    """Mock Note model for testing."""

    def __init__(
        self,
        note_no: int,
        note: str,
        note_type: str,
        date_created: datetime = None,
        created_by: str = None,
    ):
        self.note_no = note_no
        self.note = note
        self.note_type = note_type
        self.date_created = date_created or datetime.now()
        self.created_by = created_by


class MockNoteLink:
    """Mock NoteLink model for testing."""

    def __init__(
        self,
        note_link_no: int,
        note_no: int,
        tab_name: str,
        primary_key: int,
        date_created: datetime = None,
        created_by: str = None,
    ):
        self.note_link_no = note_link_no
        self.note_no = note_no
        self.tab_name = tab_name
        self.primary_key = primary_key
        self.date_created = date_created or datetime.now()
        self.created_by = created_by


class MockFeature:
    """Mock Feature model for testing."""

    def __init__(self, feature_no: int, feature_name: str, gene_name: str = None):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name


class MockQuery:
    """Mock SQLAlchemy query object for testing."""

    def __init__(self, results=None, count_value=None):
        self._results = results or []
        self._count_value = count_value if count_value is not None else len(self._results)

    def filter(self, *args, **kwargs):
        return self

    def join(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def offset(self, n):
        return self

    def limit(self, n):
        return self

    def first(self):
        return self._results[0] if self._results else None

    def all(self):
        return self._results

    def count(self):
        return self._count_value

    def delete(self):
        return len(self._results)


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    db = MagicMock()
    return db


@pytest.fixture
def sample_notes():
    """Sample notes for testing."""
    return [
        MockNote(1, "This is a curator note about ACT1", "Curator", created_by="curator1"),
        MockNote(2, "History of gene naming", "History", created_by="curator2"),
        MockNote(3, "Sequence update note", "Sequence", created_by="curator1"),
    ]


@pytest.fixture
def sample_note_links():
    """Sample note links for testing."""
    return [
        MockNoteLink(1, 1, "FEATURE", 101),
        MockNoteLink(2, 1, "FEATURE", 102),
        MockNoteLink(3, 2, "REFERENCE", 1001),
    ]


@pytest.fixture
def sample_features():
    """Sample features for testing."""
    return [
        MockFeature(101, "orf19.1", "ACT1"),
        MockFeature(102, "orf19.2", "EFG1"),
    ]


class TestConstants:
    """Tests for service constants."""

    def test_note_types(self):
        """Should have standard note types."""
        assert "Curator" in NOTE_TYPES
        assert "History" in NOTE_TYPES
        assert "Nomenclature" in NOTE_TYPES
        assert "Sequence" in NOTE_TYPES

    def test_linkable_tables(self):
        """Should have standard linkable tables."""
        assert "FEATURE" in LINKABLE_TABLES
        assert "COLLEAGUE" in LINKABLE_TABLES
        assert "REFERENCE" in LINKABLE_TABLES


class TestGetNoteByNo:
    """Tests for note retrieval."""

    def test_returns_note(self, mock_db, sample_notes):
        """Should return note for valid note_no."""
        mock_db.query.return_value = MockQuery([sample_notes[0]])

        service = NoteCurationService(mock_db)
        result = service.get_note_by_no(1)

        assert result is not None
        assert result.note_no == 1

    def test_returns_none_for_unknown(self, mock_db):
        """Should return None for unknown note_no."""
        mock_db.query.return_value = MockQuery([])

        service = NoteCurationService(mock_db)
        result = service.get_note_by_no(999)

        assert result is None


class TestGetNoteDetails:
    """Tests for note details retrieval."""

    def test_returns_note_details(self, mock_db, sample_notes, sample_note_links, sample_features):
        """Should return full note details with links."""
        mock_db.query.side_effect = [
            MockQuery([sample_notes[0]]),  # Note lookup
            MockQuery([sample_note_links[0], sample_note_links[1]]),  # Links lookup
            MockQuery([sample_features[0]]),  # Feature lookup for link 1
            MockQuery([sample_features[1]]),  # Feature lookup for link 2
        ]

        service = NoteCurationService(mock_db)
        result = service.get_note_details(1)

        assert result["note_no"] == 1
        assert result["note"] == "This is a curator note about ACT1"
        assert result["note_type"] == "Curator"
        assert len(result["linked_entities"]) == 2

    def test_raises_for_unknown_note(self, mock_db):
        """Should raise error for unknown note."""
        mock_db.query.return_value = MockQuery([])

        service = NoteCurationService(mock_db)

        with pytest.raises(NoteCurationError) as exc_info:
            service.get_note_details(999)

        assert "not found" in str(exc_info.value)

    def test_includes_entity_names_for_features(self, mock_db, sample_notes, sample_note_links, sample_features):
        """Should include feature names in linked entities."""
        mock_db.query.side_effect = [
            MockQuery([sample_notes[0]]),
            MockQuery([sample_note_links[0]]),
            MockQuery([sample_features[0]]),
        ]

        service = NoteCurationService(mock_db)
        result = service.get_note_details(1)

        entity = result["linked_entities"][0]
        assert entity["entity_name"] == "orf19.1"
        assert entity["gene_name"] == "ACT1"


class TestSearchNotes:
    """Tests for note search."""

    def test_search_returns_notes(self, mock_db, sample_notes):
        """Should return matching notes."""
        mock_db.query.return_value = MockQuery(sample_notes, count_value=3)

        service = NoteCurationService(mock_db)
        results, total = service.search_notes()

        assert len(results) == 3
        assert total == 3

    def test_search_by_query(self, mock_db, sample_notes):
        """Should filter by query string."""
        mock_db.query.return_value = MockQuery([sample_notes[0]], count_value=1)

        service = NoteCurationService(mock_db)
        results, total = service.search_notes(query="ACT1")

        assert len(results) == 1
        assert total == 1

    def test_search_by_note_type(self, mock_db, sample_notes):
        """Should filter by note type."""
        mock_db.query.return_value = MockQuery([sample_notes[0]], count_value=1)

        service = NoteCurationService(mock_db)
        results, total = service.search_notes(note_type="Curator")

        assert len(results) == 1

    def test_search_truncates_long_notes(self, mock_db):
        """Should truncate notes longer than 200 chars."""
        long_note = MockNote(1, "x" * 300, "Curator")
        mock_db.query.return_value = MockQuery([long_note], count_value=1)

        service = NoteCurationService(mock_db)
        results, _ = service.search_notes()

        assert len(results[0]["note"]) == 203  # 200 + "..."
        assert results[0]["note"].endswith("...")

    def test_search_pagination(self, mock_db, sample_notes):
        """Should support pagination."""
        mock_db.query.return_value = MockQuery(sample_notes[:2], count_value=10)

        service = NoteCurationService(mock_db)
        results, total = service.search_notes(page=1, page_size=2)

        assert len(results) == 2
        assert total == 10


class TestCreateNote:
    """Tests for note creation."""

    def test_creates_note(self, mock_db):
        """Should create note with valid data."""
        mock_db.query.return_value = MockQuery([])  # No existing note

        service = NoteCurationService(mock_db)
        service.create_note("Test note", "Curator", "curator1")

        mock_db.add.assert_called()
        mock_db.commit.assert_called_once()

    def test_raises_for_invalid_note_type(self, mock_db):
        """Should raise error for invalid note type."""
        service = NoteCurationService(mock_db)

        with pytest.raises(NoteCurationError) as exc_info:
            service.create_note("Test note", "InvalidType", "curator1")

        assert "Invalid note type" in str(exc_info.value)

    def test_raises_for_duplicate_note(self, mock_db, sample_notes):
        """Should raise error for duplicate note."""
        mock_db.query.return_value = MockQuery([sample_notes[0]])

        service = NoteCurationService(mock_db)

        with pytest.raises(NoteCurationError) as exc_info:
            service.create_note("This is a curator note about ACT1", "Curator", "curator1")

        assert "already exists" in str(exc_info.value)

    def test_creates_entity_links(self, mock_db):
        """Should create entity links with note."""
        mock_db.query.return_value = MockQuery([])

        service = NoteCurationService(mock_db)
        service.create_note(
            "Test note",
            "Curator",
            "curator1",
            linked_entities=[{"tab_name": "FEATURE", "primary_key": 101}]
        )

        # Should call add twice (note + link)
        assert mock_db.add.call_count >= 2

    def test_skips_invalid_table_name(self, mock_db):
        """Should skip links with invalid table names."""
        mock_db.query.return_value = MockQuery([])

        service = NoteCurationService(mock_db)
        service.create_note(
            "Test note",
            "Curator",
            "curator1",
            linked_entities=[{"tab_name": "INVALID_TABLE", "primary_key": 101}]
        )

        # Should only add the note, not the invalid link
        mock_db.commit.assert_called_once()


class TestUpdateNote:
    """Tests for note update."""

    def test_updates_note_text(self, mock_db, sample_notes):
        """Should update note text."""
        mock_db.query.return_value = MockQuery([sample_notes[0]])

        service = NoteCurationService(mock_db)
        result = service.update_note(1, "curator1", note_text="Updated text")

        assert result is True
        assert sample_notes[0].note == "Updated text"
        mock_db.commit.assert_called_once()

    def test_updates_note_type(self, mock_db, sample_notes):
        """Should update note type."""
        mock_db.query.return_value = MockQuery([sample_notes[0]])

        service = NoteCurationService(mock_db)
        result = service.update_note(1, "curator1", note_type="History")

        assert result is True
        assert sample_notes[0].note_type == "History"

    def test_raises_for_unknown_note(self, mock_db):
        """Should raise error for unknown note."""
        mock_db.query.return_value = MockQuery([])

        service = NoteCurationService(mock_db)

        with pytest.raises(NoteCurationError) as exc_info:
            service.update_note(999, "curator1", note_text="Updated")

        assert "not found" in str(exc_info.value)

    def test_raises_for_invalid_note_type(self, mock_db, sample_notes):
        """Should raise error for invalid note type."""
        mock_db.query.return_value = MockQuery([sample_notes[0]])

        service = NoteCurationService(mock_db)

        with pytest.raises(NoteCurationError) as exc_info:
            service.update_note(1, "curator1", note_type="InvalidType")

        assert "Invalid note type" in str(exc_info.value)


class TestDeleteNote:
    """Tests for note deletion."""

    def test_deletes_note(self, mock_db, sample_notes):
        """Should delete note and links."""
        mock_db.query.side_effect = [
            MockQuery([sample_notes[0]]),  # Note lookup
            MockQuery([]),  # NoteLink delete
        ]

        service = NoteCurationService(mock_db)
        result = service.delete_note(1, "curator1")

        assert result is True
        mock_db.delete.assert_called_once()
        mock_db.commit.assert_called_once()

    def test_raises_for_unknown_note(self, mock_db):
        """Should raise error for unknown note."""
        mock_db.query.return_value = MockQuery([])

        service = NoteCurationService(mock_db)

        with pytest.raises(NoteCurationError) as exc_info:
            service.delete_note(999, "curator1")

        assert "not found" in str(exc_info.value)


class TestLinkNoteToEntity:
    """Tests for entity linking."""

    def test_creates_link(self, mock_db, sample_notes):
        """Should create link to entity."""
        mock_db.query.side_effect = [
            MockQuery([sample_notes[0]]),  # Note lookup
            MockQuery([]),  # No existing link
        ]

        service = NoteCurationService(mock_db)
        service.link_note_to_entity(1, "FEATURE", 101, "curator1")

        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()

    def test_raises_for_unknown_note(self, mock_db):
        """Should raise error for unknown note."""
        mock_db.query.return_value = MockQuery([])

        service = NoteCurationService(mock_db)

        with pytest.raises(NoteCurationError) as exc_info:
            service.link_note_to_entity(999, "FEATURE", 101, "curator1")

        assert "not found" in str(exc_info.value)

    def test_raises_for_invalid_table(self, mock_db, sample_notes):
        """Should raise error for invalid table name."""
        mock_db.query.return_value = MockQuery([sample_notes[0]])

        service = NoteCurationService(mock_db)

        with pytest.raises(NoteCurationError) as exc_info:
            service.link_note_to_entity(1, "INVALID_TABLE", 101, "curator1")

        assert "Invalid table name" in str(exc_info.value)

    def test_raises_for_existing_link(self, mock_db, sample_notes, sample_note_links):
        """Should raise error for duplicate link."""
        mock_db.query.side_effect = [
            MockQuery([sample_notes[0]]),  # Note lookup
            MockQuery([sample_note_links[0]]),  # Existing link
        ]

        service = NoteCurationService(mock_db)

        with pytest.raises(NoteCurationError) as exc_info:
            service.link_note_to_entity(1, "FEATURE", 101, "curator1")

        assert "already linked" in str(exc_info.value)

    def test_normalizes_table_name(self, mock_db, sample_notes):
        """Should normalize table name to uppercase."""
        mock_db.query.side_effect = [
            MockQuery([sample_notes[0]]),
            MockQuery([]),
        ]

        service = NoteCurationService(mock_db)
        service.link_note_to_entity(1, "feature", 101, "curator1")

        # Should work without raising error (lowercase converted to uppercase)
        mock_db.add.assert_called_once()


class TestUnlinkNoteFromEntity:
    """Tests for entity unlinking."""

    def test_removes_link(self, mock_db, sample_note_links):
        """Should remove entity link."""
        mock_db.query.return_value = MockQuery([sample_note_links[0]])

        service = NoteCurationService(mock_db)
        result = service.unlink_note_from_entity(1, "curator1")

        assert result is True
        mock_db.delete.assert_called_once()
        mock_db.commit.assert_called_once()

    def test_raises_for_unknown_link(self, mock_db):
        """Should raise error for unknown link."""
        mock_db.query.return_value = MockQuery([])

        service = NoteCurationService(mock_db)

        with pytest.raises(NoteCurationError) as exc_info:
            service.unlink_note_from_entity(999, "curator1")

        assert "not found" in str(exc_info.value)


class TestGetNotesForEntity:
    """Tests for getting notes linked to entity."""

    def test_returns_notes(self, mock_db, sample_notes, sample_note_links):
        """Should return notes linked to entity."""
        mock_db.query.return_value = MockQuery([
            (sample_notes[0], sample_note_links[0]),
        ])

        service = NoteCurationService(mock_db)
        results = service.get_notes_for_entity("FEATURE", 101)

        assert len(results) == 1
        assert results[0]["note_no"] == 1
        assert results[0]["note_link_no"] == 1

    def test_returns_empty_for_no_links(self, mock_db):
        """Should return empty list for entity with no notes."""
        mock_db.query.return_value = MockQuery([])

        service = NoteCurationService(mock_db)
        results = service.get_notes_for_entity("FEATURE", 999)

        assert results == []

    def test_normalizes_table_name(self, mock_db, sample_notes, sample_note_links):
        """Should normalize table name to uppercase."""
        mock_db.query.return_value = MockQuery([
            (sample_notes[0], sample_note_links[0]),
        ])

        service = NoteCurationService(mock_db)
        results = service.get_notes_for_entity("feature", 101)

        assert len(results) == 1


class TestServiceInitialization:
    """Tests for service initialization."""

    def test_stores_db_session(self, mock_db):
        """Service should store the database session."""
        service = NoteCurationService(mock_db)
        assert service.db is mock_db


class TestNoteCurationError:
    """Tests for custom exception."""

    def test_exception_message(self):
        """Should store and return error message."""
        error = NoteCurationError("Test error message")
        assert str(error) == "Test error message"

    def test_is_exception(self):
        """Should be an Exception subclass."""
        error = NoteCurationError("Test")
        assert isinstance(error, Exception)
