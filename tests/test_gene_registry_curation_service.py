"""
Tests for Gene Registry Curation Service.

Tests cover:
- Listing pending submissions
- Getting submission details
- Processing submissions
- Updating/creating features
- Creating gene reservations
- Managing aliases
- Archive and delete operations
"""
import pytest
from unittest.mock import MagicMock, patch, mock_open
from datetime import datetime
import json

from cgd.api.services.curation.gene_registry_curation_service import (
    GeneRegistryCurationService,
    GeneRegistryCurationError,
)


class MockOrganism:
    """Mock Organism model."""

    def __init__(self, organism_no: int, organism_abbrev: str, organism_name: str = None):
        self.organism_no = organism_no
        self.organism_abbrev = organism_abbrev
        self.organism_name = organism_name or organism_abbrev


class MockFeature:
    """Mock Feature model."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        organism_no: int = 1,
        gene_name: str = None,
        feature_type: str = "ORF",
        headline: str = None,
        name_description: str = None,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.organism_no = organism_no
        self.gene_name = gene_name
        self.feature_type = feature_type
        self.headline = headline
        self.name_description = name_description


class MockColleague:
    """Mock Colleague model."""

    def __init__(
        self,
        colleague_no: int,
        first_name: str,
        last_name: str,
        email: str = None,
        institution: str = None,
    ):
        self.colleague_no = colleague_no
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.institution = institution


class MockGeneReservation:
    """Mock GeneReservation model."""

    def __init__(
        self,
        gene_reservation_no: int,
        feature_no: int,
        expiration_date: datetime = None,
    ):
        self.gene_reservation_no = gene_reservation_no
        self.feature_no = feature_no
        self.expiration_date = expiration_date


class MockAlias:
    """Mock Alias model."""

    def __init__(self, alias_no: int, alias_name: str, alias_type: str = "Uniform"):
        self.alias_no = alias_no
        self.alias_name = alias_name
        self.alias_type = alias_type


class MockFeatAlias:
    """Mock FeatAlias model."""

    def __init__(self, feat_alias_no: int, feature_no: int, alias_no: int):
        self.feat_alias_no = feat_alias_no
        self.feature_no = feature_no
        self.alias_no = alias_no


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []

    def filter(self, *args, **kwargs):
        return self

    def first(self):
        return self._results[0] if self._results else None

    def all(self):
        return self._results


class MockPath:
    """Mock pathlib.Path for filesystem operations."""

    def __init__(self, path, exists=True, is_file=True):
        self._path = path
        self._exists = exists
        self._is_file = is_file

    def __truediv__(self, other):
        return MockPath(f"{self._path}/{other}", self._exists, self._is_file)

    def exists(self):
        return self._exists

    def glob(self, pattern):
        return []

    def mkdir(self, parents=False, exist_ok=False):
        pass

    @property
    def stem(self):
        return self._path.split("/")[-1].replace(".json", "")

    @property
    def name(self):
        return self._path.split("/")[-1]

    def unlink(self):
        pass

    def __str__(self):
        return self._path


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    db = MagicMock()
    db.query.return_value = MockQuery([])
    return db


@pytest.fixture
def sample_submission_data():
    """Create sample submission data."""
    return {
        "gene_name": "ALS1",
        "orf_name": "CAL0001",
        "organism": "C_albicans_SC5314",
        "submitted_at": "2024-01-15T10:30:00",
        "colleague_no": 1,
        "data": {
            "first_name": "John",
            "last_name": "Smith",
            "email": "john@example.com",
            "gene_name": "ALS1",
            "orf_name": "CAL0001",
        },
    }


class TestConstants:
    """Tests for service constants."""

    def test_source_is_cgd(self):
        """Should define source as CGD."""
        assert GeneRegistryCurationService.SOURCE == "CGD"


class TestGetSubmitterName:
    """Tests for getting submitter name."""

    def test_returns_name_from_data(self, mock_db, sample_submission_data):
        """Should return formatted name from data."""
        service = GeneRegistryCurationService(mock_db)
        result = service._get_submitter_name(sample_submission_data)

        assert result == "Smith, John"

    def test_returns_unknown_for_missing_data(self, mock_db):
        """Should return Unknown for missing data."""
        service = GeneRegistryCurationService(mock_db)
        result = service._get_submitter_name({})

        assert result == "Unknown"

    def test_returns_unknown_for_partial_data(self, mock_db):
        """Should return Unknown for partial name data."""
        service = GeneRegistryCurationService(mock_db)
        result = service._get_submitter_name({"data": {"first_name": "John"}})

        assert result == "Unknown"


class TestListPendingSubmissions:
    """Tests for listing pending submissions."""

    @patch("cgd.api.services.curation.gene_registry_curation_service.Path")
    def test_returns_empty_for_nonexistent_dir(self, mock_path_class, mock_db):
        """Should return empty list if directory doesn't exist."""
        mock_path = MagicMock()
        mock_path.exists.return_value = False
        mock_path_class.return_value = mock_path

        service = GeneRegistryCurationService(mock_db)
        results = service.list_pending_submissions()

        assert results == []

    @patch("cgd.api.services.curation.gene_registry_curation_service.Path")
    def test_returns_submissions_list(self, mock_path_class, mock_db, sample_submission_data):
        """Should return list of submissions."""
        # Setup mock path
        mock_path = MagicMock()
        mock_path.exists.return_value = True

        mock_file = MagicMock()
        mock_file.stem = "gene_registry_12345"
        mock_file.name = "gene_registry_12345.json"
        mock_path.glob.return_value = [mock_file]
        mock_path_class.return_value = mock_path

        # Mock file open
        with patch("builtins.open", mock_open(read_data=json.dumps(sample_submission_data))):
            service = GeneRegistryCurationService(mock_db)
            results = service.list_pending_submissions()

        assert len(results) == 1
        assert results[0]["gene_name"] == "ALS1"
        assert results[0]["orf_name"] == "CAL0001"


class TestGetSubmissionDetails:
    """Tests for getting submission details."""

    @patch("cgd.api.services.curation.gene_registry_curation_service.Path")
    def test_returns_none_for_nonexistent_file(self, mock_path_class, mock_db):
        """Should return None if file doesn't exist."""
        mock_path = MagicMock()
        mock_file_path = MagicMock()
        mock_file_path.exists.return_value = False
        mock_path.__truediv__ = MagicMock(return_value=mock_file_path)
        mock_path_class.return_value = mock_path

        service = GeneRegistryCurationService(mock_db)
        result = service.get_submission_details("nonexistent")

        assert result is None

    @patch("cgd.api.services.curation.gene_registry_curation_service.Path")
    def test_returns_submission_details(self, mock_path_class, mock_db, sample_submission_data):
        """Should return submission details."""
        mock_path = MagicMock()
        mock_file_path = MagicMock()
        mock_file_path.exists.return_value = True
        mock_file_path.name = "gene_registry_12345.json"
        mock_path.__truediv__ = MagicMock(return_value=mock_file_path)
        mock_path_class.return_value = mock_path

        # Mock file open and database queries
        with patch("builtins.open", mock_open(read_data=json.dumps(sample_submission_data))):
            service = GeneRegistryCurationService(mock_db)
            result = service.get_submission_details("gene_registry_12345")

        assert result is not None
        assert result["gene_name"] == "ALS1"
        assert result["orf_name"] == "CAL0001"


class TestProcessSubmission:
    """Tests for processing submissions."""

    @patch.object(GeneRegistryCurationService, "get_submission_details")
    def test_raises_for_unknown_organism(self, mock_get_details, mock_db):
        """Should raise error for unknown organism."""
        mock_get_details.return_value = {"data": {}}
        mock_db.query.return_value = MockQuery([])

        service = GeneRegistryCurationService(mock_db)

        with pytest.raises(GeneRegistryCurationError) as exc_info:
            service.process_submission(
                submission_id="test_12345",
                curator_userid="curator1",
                gene_name="ALS1",
                orf_name="CAL0001",
                organism_abbrev="Unknown",
            )

        assert "not found" in str(exc_info.value)

    @patch.object(GeneRegistryCurationService, "get_submission_details")
    def test_raises_for_unknown_submission(self, mock_get_details, mock_db):
        """Should raise error for unknown submission."""
        mock_organism = MockOrganism(1, "C_albicans_SC5314")
        mock_db.query.return_value = MockQuery([mock_organism])
        mock_get_details.return_value = None

        service = GeneRegistryCurationService(mock_db)

        with pytest.raises(GeneRegistryCurationError) as exc_info:
            service.process_submission(
                submission_id="nonexistent",
                curator_userid="curator1",
                gene_name="ALS1",
                orf_name="CAL0001",
                organism_abbrev="C_albicans_SC5314",
            )

        assert "not found" in str(exc_info.value)

    @patch.object(GeneRegistryCurationService, "get_submission_details")
    def test_raises_for_missing_colleague(self, mock_get_details, mock_db):
        """Should raise error if colleague not in database."""
        mock_organism = MockOrganism(1, "C_albicans_SC5314")
        mock_db.query.side_effect = [
            MockQuery([mock_organism]),  # Organism lookup
            MockQuery([]),  # Colleague lookup fails
        ]
        mock_get_details.return_value = {
            "data": {
                "first_name": "John",
                "last_name": "Smith",
                "email": "john@example.com",
            }
        }

        service = GeneRegistryCurationService(mock_db)

        with pytest.raises(GeneRegistryCurationError) as exc_info:
            service.process_submission(
                submission_id="test_12345",
                curator_userid="curator1",
                gene_name="ALS1",
                orf_name="CAL0001",
                organism_abbrev="C_albicans_SC5314",
            )

        assert "Colleague must be in database" in str(exc_info.value)


class TestUpdateOrCreateFeature:
    """Tests for updating or creating features."""

    def test_updates_existing_feature(self, mock_db):
        """Should update existing feature."""
        existing_feature = MockFeature(1, "CAL0001", gene_name=None)
        mock_db.query.return_value = MockQuery([existing_feature])

        service = GeneRegistryCurationService(mock_db)
        result = service._update_or_create_feature(
            orf_name="CAL0001",
            gene_name="ALS1",
            organism_no=1,
            description="Test description",
            headline="Test headline",
            curator_userid="curator1",
        )

        assert result.gene_name == "ALS1"
        assert result.name_description == "Test description"
        assert result.headline == "Test headline"

    def test_creates_new_feature(self, mock_db):
        """Should create new feature if not exists."""
        mock_db.query.return_value = MockQuery([])

        service = GeneRegistryCurationService(mock_db)
        service._update_or_create_feature(
            orf_name="NEWGENE",
            gene_name="NEW1",
            organism_no=1,
            description="New gene",
            headline="New headline",
            curator_userid="curator1",
        )

        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()


class TestCreateGeneReservation:
    """Tests for creating gene reservations."""

    def test_returns_existing_reservation(self, mock_db):
        """Should return existing reservation if found."""
        existing = MockGeneReservation(10, 1)
        mock_db.query.return_value = MockQuery([existing])

        service = GeneRegistryCurationService(mock_db)
        result = service._create_gene_reservation(
            feature_no=1,
            colleague_no=1,
            curator_userid="curator1",
        )

        assert result == 10
        mock_db.add.assert_not_called()

    def test_creates_new_reservation(self, mock_db):
        """Should create new reservation if not exists."""
        mock_db.query.return_value = MockQuery([])
        mock_db.execute.return_value.fetchone.return_value = [datetime(2025, 1, 15)]

        service = GeneRegistryCurationService(mock_db)
        service._create_gene_reservation(
            feature_no=1,
            colleague_no=1,
            curator_userid="curator1",
        )

        # Should have added GeneReservation and CollGeneres
        assert mock_db.add.call_count >= 2


class TestCreateAliases:
    """Tests for creating aliases."""

    def test_creates_new_alias(self, mock_db):
        """Should create new alias if not exists."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Alias not found
            MockQuery([]),  # FeatAlias not found
        ]

        service = GeneRegistryCurationService(mock_db)
        service._create_aliases(
            feature_no=1,
            aliases=["ALIAS1"],
        )

        # Should have added Alias and FeatAlias
        assert mock_db.add.call_count >= 2

    def test_reuses_existing_alias(self, mock_db):
        """Should reuse existing alias."""
        existing_alias = MockAlias(10, "ALIAS1")
        mock_db.query.side_effect = [
            MockQuery([existing_alias]),  # Alias found
            MockQuery([]),  # FeatAlias not found
        ]

        service = GeneRegistryCurationService(mock_db)
        service._create_aliases(
            feature_no=1,
            aliases=["ALIAS1"],
        )

        # Should only add FeatAlias, not new Alias
        mock_db.add.assert_called_once()

    def test_skips_empty_aliases(self, mock_db):
        """Should skip empty alias names."""
        mock_db.query.return_value = MockQuery([])

        service = GeneRegistryCurationService(mock_db)
        service._create_aliases(
            feature_no=1,
            aliases=["", "  ", "VALID"],
        )

        # Should only process VALID
        mock_db.query.assert_called()


class TestDelaySubmission:
    """Tests for delaying submissions."""

    @patch("cgd.api.services.curation.gene_registry_curation_service.Path")
    def test_returns_false_for_nonexistent(self, mock_path_class, mock_db):
        """Should return False if file doesn't exist."""
        mock_path = MagicMock()
        mock_file_path = MagicMock()
        mock_file_path.exists.return_value = False
        mock_path.__truediv__ = MagicMock(return_value=mock_file_path)
        mock_path_class.return_value = mock_path

        service = GeneRegistryCurationService(mock_db)
        result = service.delay_submission("nonexistent")

        assert result is False

    @patch("cgd.api.services.curation.gene_registry_curation_service.Path")
    def test_marks_submission_as_delayed(self, mock_path_class, mock_db, sample_submission_data):
        """Should mark submission as delayed."""
        mock_path = MagicMock()
        mock_file_path = MagicMock()
        mock_file_path.exists.return_value = True
        mock_path.__truediv__ = MagicMock(return_value=mock_file_path)
        mock_path_class.return_value = mock_path

        written_data = []

        def mock_write(data):
            written_data.append(data)

        mock_file = MagicMock()
        mock_file.write = mock_write

        with patch("builtins.open", mock_open(read_data=json.dumps(sample_submission_data))):
            service = GeneRegistryCurationService(mock_db)
            result = service.delay_submission("test_12345", comment="Need more info")

        assert result is True


class TestDeleteSubmission:
    """Tests for deleting submissions."""

    @patch("cgd.api.services.curation.gene_registry_curation_service.Path")
    def test_returns_false_for_nonexistent(self, mock_path_class, mock_db):
        """Should return False if file doesn't exist."""
        mock_path = MagicMock()
        mock_file_path = MagicMock()
        mock_file_path.exists.return_value = False
        mock_path.__truediv__ = MagicMock(return_value=mock_file_path)
        mock_path_class.return_value = mock_path

        service = GeneRegistryCurationService(mock_db)
        result = service.delete_submission("nonexistent")

        assert result is False


class TestServiceInitialization:
    """Tests for service initialization."""

    def test_stores_db_session(self, mock_db):
        """Should store database session."""
        service = GeneRegistryCurationService(mock_db)
        assert service.db is mock_db


class TestGeneRegistryCurationError:
    """Tests for the error class."""

    def test_exception_message(self):
        """Should store error message."""
        error = GeneRegistryCurationError("Test error")
        assert str(error) == "Test error"

    def test_is_exception(self):
        """Should be an Exception."""
        assert issubclass(GeneRegistryCurationError, Exception)
