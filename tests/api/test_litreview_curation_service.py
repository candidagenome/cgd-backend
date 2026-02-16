"""
Tests for Literature Review Curation Service.

Tests cover:
- Getting pending papers from REF_TEMP
- Triage operations (add, high priority, discard)
- Batch triage processing
- Reference creation from REF_TEMP
- Curation status management
"""
import pytest
from unittest.mock import MagicMock
from datetime import datetime

from cgd.api.services.curation.litreview_curation_service import (
    LitReviewCurationService,
    LitReviewError,
    PROPERTY_TYPE,
    HIGH_PRIORITY,
    NOT_YET_CURATED,
    REF_SOURCE,
)


class MockRefTemp:
    """Mock RefTemp model."""

    def __init__(
        self,
        ref_temp_no: int,
        pubmed: int,
        citation: str = "Smith et al. (2024) J Cell Biol",
        abstract: str = "Test abstract",
        fulltext_url: str = None,
        date_created: datetime = None,
    ):
        self.ref_temp_no = ref_temp_no
        self.pubmed = pubmed
        self.citation = citation
        self.abstract = abstract
        self.fulltext_url = fulltext_url
        self.date_created = date_created or datetime.now()


class MockReference:
    """Mock Reference model."""

    def __init__(
        self,
        reference_no: int,
        pubmed: int = None,
        citation: str = "Test Citation",
    ):
        self.reference_no = reference_no
        self.pubmed = pubmed
        self.citation = citation


class MockRefBad:
    """Mock RefBad model."""

    def __init__(self, pubmed: int):
        self.pubmed = pubmed


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


class MockOrganism:
    """Mock Organism model."""

    def __init__(self, organism_abbrev: str, organism_name: str):
        self.organism_abbrev = organism_abbrev
        self.organism_name = organism_name


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args):
        return self

    def offset(self, n):
        return self

    def limit(self, n):
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
def sample_ref_temps():
    """Create sample REF_TEMP records."""
    return [
        MockRefTemp(1, 12345678, "Smith et al. (2024) J Cell Biol"),
        MockRefTemp(2, 87654321, "Doe et al. (2023) Nature"),
    ]


class TestConstants:
    """Tests for service constants."""

    def test_property_type(self):
        """Should define property type."""
        assert PROPERTY_TYPE == "curation_status"

    def test_high_priority(self):
        """Should define high priority status."""
        assert HIGH_PRIORITY == "High Priority"

    def test_not_yet_curated(self):
        """Should define not yet curated status."""
        assert NOT_YET_CURATED == "Not yet curated"

    def test_ref_source(self):
        """Should define reference source."""
        assert REF_SOURCE == "Curator Triage"


class TestGetPendingPapers:
    """Tests for getting pending papers."""

    def test_returns_empty_for_no_papers(self, mock_db):
        """Should return empty list when no papers."""
        mock_db.query.side_effect = [
            MockQuery([0]),  # Count
            MockQuery([]),  # Papers
        ]

        service = LitReviewCurationService(mock_db)
        result = service.get_pending_papers()

        assert result["papers"] == []
        assert result["total"] == 0

    def test_returns_papers(self, mock_db, sample_ref_temps):
        """Should return pending papers."""
        mock_db.query.side_effect = [
            MockQuery([2]),  # Count
            MockQuery(sample_ref_temps),  # Papers
            MockQuery([]),  # Check existing ref for paper 1
            MockQuery([]),  # Check existing ref for paper 2
        ]

        service = LitReviewCurationService(mock_db)
        result = service.get_pending_papers()

        assert len(result["papers"]) == 2
        assert result["papers"][0]["pubmed"] == 12345678

    def test_excludes_already_imported(self, mock_db, sample_ref_temps):
        """Should exclude papers already in Reference table."""
        existing_ref = MockReference(1, 12345678)

        mock_db.query.side_effect = [
            MockQuery([2]),  # Count
            MockQuery(sample_ref_temps),  # Papers
            MockQuery([existing_ref]),  # Paper 1 exists in Reference
            MockQuery([sample_ref_temps[0]]),  # Get REF_TEMP for auto-delete
            MockQuery([]),  # Paper 2 not in Reference
        ]

        service = LitReviewCurationService(mock_db)
        result = service.get_pending_papers()

        # Only paper 2 should be returned
        assert len(result["papers"]) == 1
        assert result["papers"][0]["pubmed"] == 87654321


class TestGetPaperByPubmed:
    """Tests for getting paper by PubMed ID."""

    def test_returns_paper_when_found(self, mock_db, sample_ref_temps):
        """Should return paper when found."""
        mock_db.query.return_value = MockQuery([sample_ref_temps[0]])

        service = LitReviewCurationService(mock_db)
        result = service.get_paper_by_pubmed(12345678)

        assert result is not None
        assert result["pubmed"] == 12345678

    def test_returns_none_when_not_found(self, mock_db):
        """Should return None when not found."""
        mock_db.query.return_value = MockQuery([])

        service = LitReviewCurationService(mock_db)
        result = service.get_paper_by_pubmed(99999999)

        assert result is None


class TestGetOrganisms:
    """Tests for getting organisms."""

    def test_returns_organisms(self, mock_db):
        """Should return list of organisms."""
        organisms = [
            MockOrganism("C_albicans_SC5314", "Candida albicans"),
            MockOrganism("C_glabrata_CBS138", "Candida glabrata"),
        ]
        mock_db.query.return_value = MockQuery(organisms)

        service = LitReviewCurationService(mock_db)
        result = service.get_organisms()

        assert len(result) == 2
        assert result[0]["organism_abbrev"] == "C_albicans_SC5314"


class TestTriageAdd:
    """Tests for triage add operation."""

    def test_returns_existing_reference(self, mock_db):
        """Should return existing reference if already imported."""
        existing_ref = MockReference(1, 12345678)
        mock_db.query.return_value = MockQuery([existing_ref])

        service = LitReviewCurationService(mock_db)
        result = service.triage_add(12345678, "curator1")

        assert result["success"] is False
        assert result["reference_no"] == 1
        assert "already exists" in result["messages"][0]

    def test_creates_reference(self, mock_db, sample_ref_temps):
        """Should create reference from REF_TEMP."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Not in Reference
            MockQuery([sample_ref_temps[0]]),  # Get REF_TEMP
            MockQuery([]),  # Check existing RefProperty
            MockQuery([sample_ref_temps[0]]),  # Get REF_TEMP for delete
        ]

        service = LitReviewCurationService(mock_db)
        result = service.triage_add(12345678, "curator1")

        assert result["success"] is True
        assert "Created reference" in result["messages"][0]


class TestTriageHighPriority:
    """Tests for triage high priority operation."""

    def test_returns_existing_reference(self, mock_db):
        """Should return existing reference if already imported."""
        existing_ref = MockReference(1, 12345678)
        mock_db.query.return_value = MockQuery([existing_ref])

        service = LitReviewCurationService(mock_db)
        result = service.triage_high_priority(12345678, "curator1")

        assert result["success"] is False
        assert result["reference_no"] == 1

    def test_creates_reference_with_status(self, mock_db, sample_ref_temps):
        """Should create reference with high priority status."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Not in Reference
            MockQuery([sample_ref_temps[0]]),  # Get REF_TEMP
            MockQuery([]),  # Check existing RefProperty
            MockQuery([sample_ref_temps[0]]),  # Get REF_TEMP for delete
        ]

        service = LitReviewCurationService(mock_db)
        result = service.triage_high_priority(12345678, "curator1")

        assert result["success"] is True
        assert "High Priority" in str(result["messages"])


class TestTriageDiscard:
    """Tests for triage discard operation."""

    def test_handles_already_discarded(self, mock_db):
        """Should handle paper already in REF_BAD."""
        existing_bad = MockRefBad(12345678)
        mock_db.query.return_value = MockQuery([existing_bad])

        service = LitReviewCurationService(mock_db)
        result = service.triage_discard(12345678, "curator1")

        assert result["success"] is True
        assert "already in discard list" in result["messages"][0]

    def test_prevents_discarding_imported(self, mock_db):
        """Should prevent discarding papers in Reference table."""
        existing_ref = MockReference(1, 12345678)
        mock_db.query.side_effect = [
            MockQuery([]),  # Not in REF_BAD
            MockQuery([existing_ref]),  # In Reference table
        ]

        service = LitReviewCurationService(mock_db)
        result = service.triage_discard(12345678, "curator1")

        assert result["success"] is False
        assert "cannot discard" in result["messages"][0]

    def test_adds_to_ref_bad(self, mock_db, sample_ref_temps):
        """Should add paper to REF_BAD."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Not in REF_BAD
            MockQuery([]),  # Not in Reference
            MockQuery([sample_ref_temps[0]]),  # Get REF_TEMP for delete
        ]

        service = LitReviewCurationService(mock_db)
        result = service.triage_discard(12345678, "curator1")

        assert result["success"] is True
        mock_db.add.assert_called_once()


class TestTriageBatch:
    """Tests for batch triage processing."""

    def test_handles_missing_data(self, mock_db):
        """Should handle missing pubmed or action."""
        service = LitReviewCurationService(mock_db)
        result = service.triage_batch([{"pubmed": None}], "curator1")

        assert result["results"][0]["success"] is False
        assert "Missing" in result["results"][0]["messages"][0]

    def test_handles_unknown_action(self, mock_db):
        """Should handle unknown action."""
        service = LitReviewCurationService(mock_db)
        result = service.triage_batch(
            [{"pubmed": 12345678, "action": "unknown"}],
            "curator1"
        )

        assert result["results"][0]["success"] is False
        assert "Unknown action" in result["results"][0]["messages"][0]

    def test_processes_multiple_actions(self, mock_db, sample_ref_temps):
        """Should process multiple actions."""
        # Setup for discard action
        mock_db.query.side_effect = [
            MockQuery([]),  # Not in REF_BAD
            MockQuery([]),  # Not in Reference
            MockQuery([sample_ref_temps[0]]),  # Get REF_TEMP for delete
        ]

        service = LitReviewCurationService(mock_db)
        result = service.triage_batch(
            [{"pubmed": 12345678, "action": "discard"}],
            "curator1"
        )

        assert result["total_processed"] == 1


class TestCreateReferenceFromRefTemp:
    """Tests for creating reference from REF_TEMP."""

    def test_raises_for_missing_ref_temp(self, mock_db):
        """Should raise error if REF_TEMP not found."""
        mock_db.query.return_value = MockQuery([])

        service = LitReviewCurationService(mock_db)

        with pytest.raises(LitReviewError) as exc_info:
            service._create_reference_from_ref_temp(99999999, "curator1")

        assert "not found in review queue" in str(exc_info.value)

    def test_creates_reference(self, mock_db, sample_ref_temps):
        """Should create reference from REF_TEMP."""
        mock_db.query.return_value = MockQuery([sample_ref_temps[0]])

        service = LitReviewCurationService(mock_db)
        service._create_reference_from_ref_temp(12345678, "curator1")

        # Should have added Reference and possibly Abstract
        assert mock_db.add.call_count >= 1


class TestSetCurationStatus:
    """Tests for setting curation status."""

    def test_updates_existing_status(self, mock_db):
        """Should update existing curation status."""
        existing_prop = MockRefProperty(1, 1, PROPERTY_TYPE, NOT_YET_CURATED)
        mock_db.query.return_value = MockQuery([existing_prop])

        service = LitReviewCurationService(mock_db)
        result = service._set_curation_status(1, HIGH_PRIORITY, "curator1")

        assert result == 1
        assert existing_prop.property_value == HIGH_PRIORITY

    def test_creates_new_status(self, mock_db):
        """Should create new curation status."""
        mock_db.query.return_value = MockQuery([])

        service = LitReviewCurationService(mock_db)
        service._set_curation_status(1, HIGH_PRIORITY, "curator1")

        mock_db.add.assert_called_once()


class TestLinkToFeature:
    """Tests for linking reference to feature."""

    def test_returns_error_for_unknown_feature(self, mock_db):
        """Should return error for unknown feature."""
        mock_db.query.return_value = MockQuery([])

        service = LitReviewCurationService(mock_db)
        result = service._link_to_feature(1, 1, "UNKNOWN", None, "curator1")

        assert result["success"] is False
        assert "not found" in result["message"]

    def test_links_to_feature(self, mock_db):
        """Should link reference to feature."""
        feature = MockFeature(1, "CAL0001", "ALS1")
        mock_db.query.side_effect = [
            MockQuery([feature]),  # Feature found
            MockQuery([]),  # No existing link
        ]

        service = LitReviewCurationService(mock_db)
        result = service._link_to_feature(1, 1, "CAL0001", None, "curator1")

        assert result["success"] is True
        assert result["feature_name"] == "CAL0001"


class TestDeleteFromRefTemp:
    """Tests for deleting from REF_TEMP."""

    def test_deletes_ref_temp(self, mock_db, sample_ref_temps):
        """Should delete from REF_TEMP."""
        mock_db.query.return_value = MockQuery([sample_ref_temps[0]])

        service = LitReviewCurationService(mock_db)
        result = service._delete_from_ref_temp(12345678)

        assert result is True
        mock_db.delete.assert_called_once()

    def test_handles_not_found(self, mock_db):
        """Should handle REF_TEMP not found."""
        mock_db.query.return_value = MockQuery([])

        service = LitReviewCurationService(mock_db)
        result = service._delete_from_ref_temp(99999999)

        assert result is False


class TestServiceInitialization:
    """Tests for service initialization."""

    def test_stores_db_session(self, mock_db):
        """Should store database session."""
        service = LitReviewCurationService(mock_db)
        assert service.db is mock_db


class TestLitReviewError:
    """Tests for the error class."""

    def test_exception_message(self):
        """Should store error message."""
        error = LitReviewError("Test error")
        assert str(error) == "Test error"

    def test_is_exception(self):
        """Should be an Exception."""
        assert issubclass(LitReviewError, Exception)
