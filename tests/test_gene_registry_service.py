"""
Tests for Gene Registry Service.

Tests cover:
- Email masking
- Organism display name lookup
- Feature qualifier lookup
- Gene name validation
- Gene registry search
- Gene registry config
- Gene registry submission
"""
import pytest
from unittest.mock import MagicMock, patch

from cgd.api.services.gene_registry_service import (
    _mask_email,
    _get_organism_display_name,
    _get_feature_qualifier,
    validate_gene_name,
    search_gene_registry,
    get_gene_registry_config,
    submit_gene_registry,
    GENE_NAME_PATTERN,
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
        parent_organism_no: int = None,
    ):
        self.organism_no = organism_no
        self.organism_abbrev = organism_abbrev
        self.organism_name = organism_name
        self.taxonomic_rank = taxonomic_rank
        self.organism_order = organism_order
        self.parent_organism_no = parent_organism_no


class MockFeature:
    """Mock Feature model."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        gene_name: str = None,
        organism_no: int = 1,
        feature_type: str = "ORF",
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name
        self.organism_no = organism_no
        self.feature_type = feature_type


class MockAlias:
    """Mock Alias model."""

    def __init__(self, alias_no: int, alias_name: str):
        self.alias_no = alias_no
        self.alias_name = alias_name


class MockFeatProperty:
    """Mock FeatProperty model."""

    def __init__(self, feature_no: int, property_type: str, property_value: str):
        self.feature_no = feature_no
        self.property_type = property_type
        self.property_value = property_value


class MockColleague:
    """Mock Colleague model."""

    def __init__(
        self,
        colleague_no: int,
        last_name: str,
        first_name: str,
        other_last_name: str = None,
        suffix: str = None,
        email: str = None,
        institution: str = None,
        work_phone: str = None,
    ):
        self.colleague_no = colleague_no
        self.last_name = last_name
        self.first_name = first_name
        self.other_last_name = other_last_name
        self.suffix = suffix
        self.email = email
        self.institution = institution
        self.work_phone = work_phone


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []

    def join(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def distinct(self):
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
    return MockOrganism(1, "C_albicans_SC5314", "Candida albicans SC5314")


@pytest.fixture
def sample_species():
    """Create sample species organism."""
    return MockOrganism(
        2, "C_albicans", "Candida albicans", taxonomic_rank="Species"
    )


@pytest.fixture
def sample_feature():
    """Create sample feature."""
    return MockFeature(feature_no=1, feature_name="CAL0001", organism_no=1)


class TestMaskEmail:
    """Tests for _mask_email."""

    def test_masks_email_correctly(self):
        """Should mask middle characters of email local part."""
        result = _mask_email("john.doe@example.com")
        assert result == "j******e@example.com"

    def test_handles_short_local_part(self):
        """Should handle short local parts."""
        result = _mask_email("ab@example.com")
        assert result == "a*@example.com"

    def test_handles_single_char_local(self):
        """Should handle single character local part."""
        result = _mask_email("a@example.com")
        assert result == "a*@example.com"

    def test_returns_none_for_none(self):
        """Should return None for None input."""
        result = _mask_email(None)
        assert result is None

    def test_returns_email_without_at(self):
        """Should return as-is if no @ symbol."""
        result = _mask_email("notanemail")
        assert result == "notanemail"


class TestGeneNamePattern:
    """Tests for gene name pattern."""

    def test_valid_gene_name(self):
        """Should match valid gene names."""
        assert GENE_NAME_PATTERN.match("ALS1")
        assert GENE_NAME_PATTERN.match("als1")
        assert GENE_NAME_PATTERN.match("CDC10")
        assert GENE_NAME_PATTERN.match("ADH123")

    def test_invalid_gene_names(self):
        """Should not match invalid gene names."""
        assert not GENE_NAME_PATTERN.match("AL1")  # Only 2 letters
        assert not GENE_NAME_PATTERN.match("ALSE")  # No number
        assert not GENE_NAME_PATTERN.match("ALS")  # No number
        assert not GENE_NAME_PATTERN.match("1ALS")  # Starts with number


class TestGetOrganismDisplayName:
    """Tests for _get_organism_display_name."""

    def test_returns_abbrev_for_unknown_organism(self, mock_db):
        """Should return abbreviation for unknown organism."""
        mock_db.query.return_value = MockQuery([])

        result = _get_organism_display_name(mock_db, "UNKNOWN")

        assert result == "UNKNOWN"

    def test_returns_name_for_species(self, mock_db, sample_species):
        """Should return organism name for species."""
        mock_db.query.return_value = MockQuery([sample_species])

        result = _get_organism_display_name(mock_db, "C_albicans")

        assert result == "Candida albicans"

    def test_returns_parent_name_for_strain(self, mock_db):
        """Should return parent species name for strain."""
        strain = MockOrganism(
            1, "C_albicans_SC5314", "Candida albicans SC5314",
            taxonomic_rank="Strain", parent_organism_no=2
        )
        species = MockOrganism(
            2, "C_albicans", "Candida albicans", taxonomic_rank="Species"
        )

        mock_db.query.side_effect = [
            MockQuery([strain]),  # First query returns strain
            MockQuery([species]),  # Second query returns parent species
        ]

        result = _get_organism_display_name(mock_db, "C_albicans_SC5314")

        assert result == "Candida albicans"


class TestGetFeatureQualifier:
    """Tests for _get_feature_qualifier."""

    def test_returns_qualifier_value(self, mock_db):
        """Should return qualifier property value."""
        mock_db.query.return_value = MockQuery([("Verified",)])

        result = _get_feature_qualifier(mock_db, 1)

        assert result == "Verified"

    def test_returns_none_when_no_qualifier(self, mock_db):
        """Should return None when no qualifier."""
        mock_db.query.return_value = MockQuery([])

        result = _get_feature_qualifier(mock_db, 1)

        assert result is None


class TestValidateGeneName:
    """Tests for validate_gene_name."""

    def test_returns_error_for_unknown_organism(self, mock_db):
        """Should return error for unknown organism."""
        mock_db.query.return_value = MockQuery([])

        result = validate_gene_name(mock_db, "ALS1", None, "UNKNOWN")

        assert result.is_valid is False
        assert "Unknown organism" in result.errors[0]

    def test_invalid_format(self, mock_db, sample_organism):
        """Should detect invalid gene name format."""
        mock_db.query.side_effect = [
            MockQuery([sample_organism]),  # Organism lookup
            MockQuery([]),  # Existing gene lookup
            MockQuery([]),  # Alias lookup
        ]

        result = validate_gene_name(mock_db, "INVALID", None, "C_albicans_SC5314")

        assert result.format_valid is False
        assert "not an acceptable gene name" in result.errors[0]

    def test_valid_format(self, mock_db, sample_organism):
        """Should accept valid gene name format."""
        mock_db.query.side_effect = [
            MockQuery([sample_organism]),  # Organism lookup
            MockQuery([]),  # Existing gene lookup
            MockQuery([]),  # Alias lookup
        ]

        result = validate_gene_name(mock_db, "ALS1", None, "C_albicans_SC5314")

        assert result.format_valid is True
        assert result.is_valid is True

    def test_detects_existing_gene(self, mock_db, sample_organism):
        """Should detect if gene name already exists."""
        existing = MockFeature(1, "CAL0001", gene_name="ALS1")
        mock_db.query.side_effect = [
            MockQuery([sample_organism]),  # Organism lookup
            MockQuery([existing]),  # Existing gene lookup
            MockQuery([]),  # Alias lookup
        ]

        result = validate_gene_name(mock_db, "ALS1", None, "C_albicans_SC5314")

        assert result.gene_exists is True
        assert any("already exists" in w for w in result.warnings)

    def test_detects_gene_is_alias(self, mock_db, sample_organism):
        """Should detect if gene name is an alias."""
        alias = MockAlias(1, "ALS1")
        feature = MockFeature(1, "CAL0001", gene_name="REALNAME")
        mock_db.query.side_effect = [
            MockQuery([sample_organism]),  # Organism lookup
            MockQuery([]),  # Existing gene lookup
            MockQuery([(alias, feature)]),  # Alias lookup
        ]

        result = validate_gene_name(mock_db, "ALS1", None, "C_albicans_SC5314")

        assert result.gene_is_alias is True
        assert result.alias_for == "REALNAME"

    def test_validates_orf_exists(self, mock_db, sample_organism, sample_feature):
        """Should check if ORF exists."""
        mock_db.query.side_effect = [
            MockQuery([sample_organism]),  # Organism lookup
            MockQuery([]),  # Existing gene lookup
            MockQuery([]),  # Alias lookup
            MockQuery([sample_feature]),  # ORF lookup
            MockQuery([]),  # Qualifier lookup
        ]

        result = validate_gene_name(mock_db, "ALS1", "CAL0001", "C_albicans_SC5314")

        assert result.orf_exists is True

    def test_error_for_nonexistent_orf(self, mock_db, sample_organism):
        """Should error if ORF doesn't exist."""
        mock_db.query.side_effect = [
            MockQuery([sample_organism]),  # Organism lookup
            MockQuery([]),  # Existing gene lookup
            MockQuery([]),  # Alias lookup
            MockQuery([]),  # ORF not found
        ]

        result = validate_gene_name(mock_db, "ALS1", "MISSING", "C_albicans_SC5314")

        assert result.orf_exists is False
        assert result.is_valid is False
        assert any("not in the database" in e for e in result.errors)

    def test_detects_deleted_orf(self, mock_db, sample_organism, sample_feature):
        """Should detect deleted ORF."""
        mock_db.query.side_effect = [
            MockQuery([sample_organism]),  # Organism lookup
            MockQuery([]),  # Existing gene lookup
            MockQuery([]),  # Alias lookup
            MockQuery([sample_feature]),  # ORF lookup
            MockQuery([("Deleted",)]),  # Qualifier = Deleted
        ]

        result = validate_gene_name(mock_db, "ALS1", "CAL0001", "C_albicans_SC5314")

        assert result.orf_is_deleted is True
        assert any("Deleted ORF" in e for e in result.errors)

    def test_warns_dubious_orf(self, mock_db, sample_organism, sample_feature):
        """Should warn about dubious ORF."""
        mock_db.query.side_effect = [
            MockQuery([sample_organism]),  # Organism lookup
            MockQuery([]),  # Existing gene lookup
            MockQuery([]),  # Alias lookup
            MockQuery([sample_feature]),  # ORF lookup
            MockQuery([("Dubious",)]),  # Qualifier = Dubious
        ]

        result = validate_gene_name(mock_db, "ALS1", "CAL0001", "C_albicans_SC5314")

        assert result.orf_is_dubious is True
        assert any("Dubious ORF" in w for w in result.warnings)

    def test_warns_orf_already_named(self, mock_db, sample_organism):
        """Should warn if ORF already has gene name."""
        named_feature = MockFeature(1, "CAL0001", gene_name="OTHERGENE")
        mock_db.query.side_effect = [
            MockQuery([sample_organism]),  # Organism lookup
            MockQuery([]),  # Existing gene lookup
            MockQuery([]),  # Alias lookup
            MockQuery([named_feature]),  # ORF with gene name
            MockQuery([]),  # Qualifier
        ]

        result = validate_gene_name(mock_db, "ALS1", "CAL0001", "C_albicans_SC5314")

        assert result.orf_has_gene is True
        assert result.orf_gene_name == "OTHERGENE"


class TestSearchGeneRegistry:
    """Tests for search_gene_registry."""

    def test_returns_success_response(self, mock_db, sample_organism):
        """Should return success response with validation."""
        mock_db.query.side_effect = [
            MockQuery([sample_organism]),  # Organism display name lookup
            MockQuery([sample_organism]),  # Validation organism lookup
            MockQuery([]),  # Existing gene
            MockQuery([]),  # Alias
            MockQuery([]),  # Colleagues search
            MockQuery([]),  # Second colleagues search with wildcard
        ]

        result = search_gene_registry(
            mock_db, "Smith", "ALS1", None, "C_albicans_SC5314"
        )

        assert result.success is True
        assert result.validation is not None

    def test_includes_organism_name(self, mock_db, sample_organism):
        """Should include organism display name."""
        mock_db.query.side_effect = [
            MockQuery([sample_organism]),  # Organism display name
            MockQuery([sample_organism]),  # Validation organism
            MockQuery([]),  # Existing gene
            MockQuery([]),  # Alias
            MockQuery([]),  # Colleagues
            MockQuery([]),  # Second colleagues search with wildcard
        ]

        result = search_gene_registry(
            mock_db, "Smith", "ALS1", None, "C_albicans_SC5314"
        )

        assert "Candida albicans" in result.organism_name

    def test_finds_colleagues(self, mock_db, sample_organism):
        """Should find matching colleagues."""
        colleague = MockColleague(
            1, "Smith", "John",
            email="john.smith@example.com",
            institution="MIT"
        )
        mock_db.query.side_effect = [
            MockQuery([sample_organism]),  # Organism display
            MockQuery([sample_organism]),  # Validation organism
            MockQuery([]),  # Existing gene
            MockQuery([]),  # Alias
            MockQuery([colleague]),  # Colleagues search
            MockQuery([]),  # Colleague URLs
        ]

        result = search_gene_registry(
            mock_db, "Smith", "ALS1", None, "C_albicans_SC5314"
        )

        assert len(result.colleagues) == 1
        assert result.colleagues[0].full_name == "Smith, John"

    def test_appends_wildcard_when_no_results(self, mock_db, sample_organism):
        """Should append wildcard when no results found."""
        colleague = MockColleague(1, "Smithson", "Jane")
        mock_db.query.side_effect = [
            MockQuery([sample_organism]),  # Organism display
            MockQuery([sample_organism]),  # Validation organism
            MockQuery([]),  # Existing gene
            MockQuery([]),  # Alias
            MockQuery([]),  # First search - no results
            MockQuery([colleague]),  # Second search with wildcard
            MockQuery([]),  # URLs
        ]

        result = search_gene_registry(
            mock_db, "Smith", "ALS1", None, "C_albicans_SC5314"
        )

        assert result.wildcard_appended is True
        assert result.search_term == "Smith*"

    def test_can_proceed_when_valid(self, mock_db, sample_organism):
        """Should allow proceeding when validation passes."""
        mock_db.query.side_effect = [
            MockQuery([sample_organism]),  # Organism display
            MockQuery([sample_organism]),  # Validation organism
            MockQuery([]),  # No existing gene
            MockQuery([]),  # No alias
            MockQuery([]),  # Colleagues
            MockQuery([]),  # Second colleagues search with wildcard
        ]

        result = search_gene_registry(
            mock_db, "Smith", "ALS1", None, "C_albicans_SC5314"
        )

        assert result.can_proceed is True

    def test_cannot_proceed_when_gene_exists(self, mock_db, sample_organism):
        """Should not allow proceeding when gene exists."""
        existing = MockFeature(1, "CAL0001", gene_name="ALS1")
        mock_db.query.side_effect = [
            MockQuery([sample_organism]),  # Organism display
            MockQuery([sample_organism]),  # Validation organism
            MockQuery([existing]),  # Existing gene
            MockQuery([]),  # Alias
            MockQuery([]),  # Colleagues
            MockQuery([]),  # Second colleagues search with wildcard
        ]

        result = search_gene_registry(
            mock_db, "Smith", "ALS1", None, "C_albicans_SC5314"
        )

        assert result.can_proceed is False


class TestGetGeneRegistryConfig:
    """Tests for get_gene_registry_config."""

    def test_returns_species_options(self, mock_db, sample_organism):
        """Should return species options."""
        mock_db.query.side_effect = [
            MockQuery([(1,)]),  # Distinct organism_nos
            MockQuery([sample_organism]),  # Organisms
        ]

        result = get_gene_registry_config(mock_db)

        assert len(result.species) >= 1

    def test_returns_default_species(self, mock_db, sample_organism):
        """Should return default species."""
        mock_db.query.side_effect = [
            MockQuery([(1,)]),  # Distinct organism_nos
            MockQuery([sample_organism]),  # Organisms
        ]

        result = get_gene_registry_config(mock_db)

        assert result.default_species is not None

    def test_returns_gene_name_pattern(self, mock_db, sample_organism):
        """Should return gene name pattern."""
        mock_db.query.side_effect = [
            MockQuery([(1,)]),  # Distinct organism_nos
            MockQuery([sample_organism]),  # Organisms
        ]

        result = get_gene_registry_config(mock_db)

        assert result.gene_name_pattern is not None

    def test_falls_back_to_defaults_when_no_organisms(self, mock_db):
        """Should fall back to defaults when no organisms."""
        mock_db.query.side_effect = [
            MockQuery([]),  # No organisms
        ]

        result = get_gene_registry_config(mock_db)

        # Should have fallback defaults
        assert len(result.species) >= 1


class TestSubmitGeneRegistry:
    """Tests for submit_gene_registry."""

    def test_requires_gene_name(self, mock_db):
        """Should require gene name."""
        result = submit_gene_registry(mock_db, {'organism': 'C_albicans_SC5314'})

        assert result['success'] is False
        assert any("Gene name" in e for e in result['errors'])

    def test_requires_organism(self, mock_db):
        """Should require organism."""
        result = submit_gene_registry(mock_db, {'gene_name': 'ALS1'})

        assert result['success'] is False
        assert any("Organism" in e for e in result['errors'])

    def test_requires_colleague_info_when_no_colleague_no(self, mock_db):
        """Should require colleague info when no colleague_no."""
        result = submit_gene_registry(mock_db, {
            'gene_name': 'ALS1',
            'organism': 'C_albicans_SC5314',
        })

        assert result['success'] is False
        assert any("Last name" in e for e in result['errors'])
        assert any("First name" in e for e in result['errors'])
        assert any("Email" in e for e in result['errors'])
        assert any("Organization" in e for e in result['errors'])

    @patch('cgd.api.services.submission_utils.write_gene_registry_submission')
    def test_success_with_existing_colleague(self, mock_write, mock_db):
        """Should succeed with existing colleague."""
        mock_write.return_value = "/tmp/submission.txt"

        result = submit_gene_registry(mock_db, {
            'gene_name': 'ALS1',
            'organism': 'C_albicans_SC5314',
            'colleague_no': 1,
        })

        assert result['success'] is True

    @patch('cgd.api.services.submission_utils.write_gene_registry_submission')
    def test_success_with_new_colleague(self, mock_write, mock_db):
        """Should succeed with new colleague info."""
        mock_write.return_value = "/tmp/submission.txt"

        result = submit_gene_registry(mock_db, {
            'gene_name': 'ALS1',
            'organism': 'C_albicans_SC5314',
            'last_name': 'Smith',
            'first_name': 'John',
            'email': 'john@example.com',
            'institution': 'MIT',
        })

        assert result['success'] is True
        assert 'message' in result
