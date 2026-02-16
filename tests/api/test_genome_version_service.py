"""
Tests for Genome Version Service.

Tests cover:
- Getting genome version config (strains list)
- Getting genome version history with pagination
- Handling unknown strains
"""
import pytest
from unittest.mock import MagicMock
from datetime import datetime

from cgd.api.services.genome_version_service import (
    get_genome_version_config,
    get_genome_version_history,
    _get_strains,
    _get_strain_display_name,
    DEFAULT_STRAIN_ABBREV,
    VERSION_FORMAT_EXPLANATION,
)


class MockOrganism:
    """Mock Organism model."""

    def __init__(
        self,
        organism_no: int,
        organism_abbrev: str,
        organism_name: str,
        taxonomic_rank: str = "Strain",
        organism_order: int = 1,
    ):
        self.organism_no = organism_no
        self.organism_abbrev = organism_abbrev
        self.organism_name = organism_name
        self.taxonomic_rank = taxonomic_rank
        self.organism_order = organism_order


class MockGenomeVersion:
    """Mock GenomeVersion model."""

    def __init__(
        self,
        genome_version_no: int,
        organism_no: int,
        genome_version: str,
        is_ver_current: str = "Y",
        description: str = None,
        date_created: datetime = None,
    ):
        self.genome_version_no = genome_version_no
        self.organism_no = organism_no
        self.genome_version = genome_version
        self.is_ver_current = is_ver_current
        self.description = description
        self.date_created = date_created or datetime.now()


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
def sample_strains():
    """Create sample strain organisms."""
    return [
        MockOrganism(1, "C_albicans_SC5314", "Candida albicans SC5314", "Strain", 1),
        MockOrganism(2, "C_glabrata_CBS138", "Candida glabrata CBS138", "Strain", 2),
    ]


@pytest.fixture
def sample_genome_versions():
    """Create sample genome versions."""
    return [
        MockGenomeVersion(1, 1, "s02-m01-r01", "Y", "Major release"),
        MockGenomeVersion(2, 1, "s02-m01-r02", "N", "Minor update"),
        MockGenomeVersion(3, 1, "s01-m01-r01", "N", "Initial version"),
    ]


class TestConstants:
    """Tests for service constants."""

    def test_default_strain(self):
        """Should define default strain."""
        assert DEFAULT_STRAIN_ABBREV == "C_albicans_SC5314"

    def test_version_format_explanation(self):
        """Should define version format explanation."""
        assert "sXX-mYY-rZZ" in VERSION_FORMAT_EXPLANATION


class TestGetStrains:
    """Tests for getting strains."""

    def test_returns_strains_with_strain_rank(self, mock_db, sample_strains):
        """Should return organisms with taxonomic_rank='Strain'."""
        mock_db.query.return_value = MockQuery(sample_strains)

        result = _get_strains(mock_db)

        assert len(result) == 2
        assert result[0].organism_abbrev == "C_albicans_SC5314"


class TestGetStrainDisplayName:
    """Tests for getting strain display names."""

    def test_returns_organism_name(self, sample_strains):
        """Should return organism name."""
        result = _get_strain_display_name(sample_strains[0])
        assert result == "Candida albicans SC5314"


class TestGetGenomeVersionConfig:
    """Tests for getting genome version config."""

    def test_returns_config_with_strains(self, mock_db, sample_strains):
        """Should return config with strain list."""
        mock_db.query.return_value = MockQuery(sample_strains)

        result = get_genome_version_config(mock_db)

        assert len(result.seq_sources) == 2
        assert result.default_seq_source == DEFAULT_STRAIN_ABBREV
        assert result.version_format_explanation == VERSION_FORMAT_EXPLANATION

    def test_uses_first_strain_as_default_if_default_not_found(self, mock_db):
        """Should use first strain as default if default not in list."""
        other_strain = MockOrganism(1, "OTHER_STRAIN", "Other Strain")
        mock_db.query.return_value = MockQuery([other_strain])

        result = get_genome_version_config(mock_db)

        assert result.default_seq_source == "OTHER_STRAIN"

    def test_returns_empty_sources_when_no_strains(self, mock_db):
        """Should return empty sources when no strains."""
        mock_db.query.return_value = MockQuery([])

        result = get_genome_version_config(mock_db)

        assert result.seq_sources == []


class TestGetGenomeVersionHistory:
    """Tests for getting genome version history."""

    def test_returns_error_for_unknown_strain(self, mock_db):
        """Should return error for unknown strain."""
        mock_db.query.return_value = MockQuery([])

        result = get_genome_version_history(mock_db, "UNKNOWN")

        assert result.success is False
        assert "not found" in result.error

    def test_returns_versions_for_valid_strain(
        self, mock_db, sample_strains, sample_genome_versions
    ):
        """Should return genome versions for valid strain."""
        mock_db.query.side_effect = [
            MockQuery([sample_strains[0]]),  # Organism lookup
            MockQuery([3]),  # Total count
            MockQuery(sample_genome_versions),  # Versions
        ]

        result = get_genome_version_history(mock_db, "C_albicans_SC5314")

        assert result.success is True
        assert result.seq_source == "C_albicans_SC5314"
        assert len(result.versions) == 3
        assert result.total_count == 3

    def test_marks_current_version(
        self, mock_db, sample_strains, sample_genome_versions
    ):
        """Should mark current version correctly."""
        mock_db.query.side_effect = [
            MockQuery([sample_strains[0]]),
            MockQuery([3]),
            MockQuery(sample_genome_versions),
        ]

        result = get_genome_version_history(mock_db, "C_albicans_SC5314")

        # First version is current (is_ver_current = "Y")
        assert result.versions[0].is_current is True
        assert result.versions[1].is_current is False

    def test_marks_major_versions(
        self, mock_db, sample_strains, sample_genome_versions
    ):
        """Should mark major versions (ending in r01)."""
        mock_db.query.side_effect = [
            MockQuery([sample_strains[0]]),
            MockQuery([3]),
            MockQuery(sample_genome_versions),
        ]

        result = get_genome_version_history(mock_db, "C_albicans_SC5314")

        # Versions ending in r01 are major
        assert result.versions[0].is_major_version is True  # s02-m01-r01
        assert result.versions[1].is_major_version is False  # s02-m01-r02
        assert result.versions[2].is_major_version is True  # s01-m01-r01

    def test_pagination(self, mock_db, sample_strains, sample_genome_versions):
        """Should handle pagination correctly."""
        mock_db.query.side_effect = [
            MockQuery([sample_strains[0]]),
            MockQuery([10]),  # Total count
            MockQuery(sample_genome_versions[:2]),  # Page 1 with 2 items
        ]

        result = get_genome_version_history(
            mock_db, "C_albicans_SC5314", page=1, page_size=2
        )

        assert result.page == 1
        assert result.page_size == 2
        assert result.total_count == 10
        assert result.total_pages == 5

    def test_empty_history(self, mock_db, sample_strains):
        """Should handle strain with no versions."""
        mock_db.query.side_effect = [
            MockQuery([sample_strains[0]]),
            MockQuery([0]),  # No versions
            MockQuery([]),
        ]

        result = get_genome_version_history(mock_db, "C_albicans_SC5314")

        assert result.success is True
        assert result.versions == []
        assert result.total_count == 0
        assert result.total_pages == 0
