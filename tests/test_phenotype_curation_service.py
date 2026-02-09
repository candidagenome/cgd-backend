"""
Tests for Phenotype Curation Service.

Tests cover:
- Feature lookup
- Reference validation
- Phenotype get/create
- Experiment get/create
- Experiment property management
- Annotation retrieval for features
- Annotation creation and deletion
- CV term retrieval
"""
import pytest
from unittest.mock import MagicMock
from datetime import datetime

from cgd.api.services.curation.phenotype_curation_service import (
    PhenotypeCurationService,
    PhenotypeCurationError,
)


class MockFeature:
    """Mock Feature model for testing."""

    def __init__(self, feature_no: int, feature_name: str, gene_name: str = None):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name


class MockReference:
    """Mock Reference model for testing."""

    def __init__(self, reference_no: int, pubmed: int = None, citation: str = None):
        self.reference_no = reference_no
        self.pubmed = pubmed
        self.citation = citation


class MockPhenotype:
    """Mock Phenotype model for testing."""

    def __init__(
        self,
        phenotype_no: int,
        experiment_type: str,
        mutant_type: str,
        observable: str,
        qualifier: str = None,
        source: str = "CGD",
    ):
        self.phenotype_no = phenotype_no
        self.experiment_type = experiment_type
        self.mutant_type = mutant_type
        self.observable = observable
        self.qualifier = qualifier
        self.source = source


class MockExperiment:
    """Mock Experiment model for testing."""

    def __init__(
        self,
        experiment_no: int,
        experiment_comment: str = None,
        source: str = "CGD",
    ):
        self.experiment_no = experiment_no
        self.experiment_comment = experiment_comment
        self.source = source


class MockExptProperty:
    """Mock ExptProperty model for testing."""

    def __init__(
        self,
        expt_property_no: int,
        property_type: str,
        property_value: str,
        property_description: str = None,
    ):
        self.expt_property_no = expt_property_no
        self.property_type = property_type
        self.property_value = property_value
        self.property_description = property_description


class MockExptExptprop:
    """Mock ExptExptprop model for testing."""

    def __init__(self, experiment_no: int, expt_property_no: int):
        self.experiment_no = experiment_no
        self.expt_property_no = expt_property_no


class MockPhenoAnnotation:
    """Mock PhenoAnnotation model for testing."""

    def __init__(
        self,
        pheno_annotation_no: int,
        feature_no: int,
        phenotype_no: int,
        experiment_no: int = None,
        date_created: datetime = None,
        created_by: str = None,
    ):
        self.pheno_annotation_no = pheno_annotation_no
        self.feature_no = feature_no
        self.phenotype_no = phenotype_no
        self.experiment_no = experiment_no
        self.date_created = date_created or datetime.now()
        self.created_by = created_by


class MockRefLink:
    """Mock RefLink model for testing."""

    def __init__(
        self,
        reference_no: int,
        tab_name: str,
        col_name: str,
        primary_key: int,
    ):
        self.reference_no = reference_no
        self.tab_name = tab_name
        self.col_name = col_name
        self.primary_key = primary_key


class MockCode:
    """Mock Code model for testing."""

    def __init__(self, code_no: int, tab_name: str, col_name: str, code_value: str):
        self.code_no = code_no
        self.tab_name = tab_name
        self.col_name = col_name
        self.code_value = code_value


class MockCv:
    """Mock Cv model for testing."""

    def __init__(self, cv_no: int, cv_name: str):
        self.cv_no = cv_no
        self.cv_name = cv_name


class MockCvTerm:
    """Mock CvTerm model for testing."""

    def __init__(self, cv_term_no: int, cv_no: int, term_name: str):
        self.cv_term_no = cv_term_no
        self.cv_no = cv_no
        self.term_name = term_name


class MockQuery:
    """Mock SQLAlchemy query object for testing."""

    def __init__(self, results=None):
        self._results = results or []

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

    def delete(self):
        return len(self._results)


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    db = MagicMock()
    return db


@pytest.fixture
def sample_features():
    """Sample features for testing."""
    return [
        MockFeature(1, "orf19.1", "ACT1"),
        MockFeature(2, "orf19.2", "EFG1"),
    ]


@pytest.fixture
def sample_references():
    """Sample references for testing."""
    return [
        MockReference(1001, 12345678, "Smith et al. 2020"),
        MockReference(1002, 87654321, "Jones et al. 2021"),
    ]


@pytest.fixture
def sample_phenotypes():
    """Sample phenotypes for testing."""
    return [
        MockPhenotype(1, "classical genetics", "null", "cell morphology", "abnormal"),
        MockPhenotype(2, "classical genetics", "null", "biofilm formation", "decreased"),
    ]


class TestConstants:
    """Tests for service constants."""

    def test_experiment_types(self, mock_db):
        """Should have standard experiment types."""
        service = PhenotypeCurationService(mock_db)
        assert "classical genetics" in service.EXPERIMENT_TYPES
        assert "RNAi" in service.EXPERIMENT_TYPES
        assert "overexpression" in service.EXPERIMENT_TYPES

    def test_mutant_types(self, mock_db):
        """Should have standard mutant types."""
        service = PhenotypeCurationService(mock_db)
        assert "null" in service.MUTANT_TYPES
        assert "loss of function" in service.MUTANT_TYPES
        assert "gain of function" in service.MUTANT_TYPES

    def test_qualifiers(self, mock_db):
        """Should have standard qualifiers."""
        service = PhenotypeCurationService(mock_db)
        assert "increased" in service.QUALIFIERS
        assert "decreased" in service.QUALIFIERS
        assert "abnormal" in service.QUALIFIERS


class TestGetFeatureByName:
    """Tests for feature lookup."""

    def test_finds_by_feature_name(self, mock_db, sample_features):
        """Should find feature by feature_name."""
        mock_db.query.return_value = MockQuery([sample_features[0]])

        service = PhenotypeCurationService(mock_db)
        result = service.get_feature_by_name("orf19.1")

        assert result is not None
        assert result.feature_name == "orf19.1"

    def test_finds_by_gene_name(self, mock_db, sample_features):
        """Should find feature by gene_name."""
        mock_db.query.return_value = MockQuery([sample_features[0]])

        service = PhenotypeCurationService(mock_db)
        result = service.get_feature_by_name("ACT1")

        assert result is not None
        assert result.gene_name == "ACT1"

    def test_returns_none_for_unknown(self, mock_db):
        """Should return None for unknown feature."""
        mock_db.query.return_value = MockQuery([])

        service = PhenotypeCurationService(mock_db)
        result = service.get_feature_by_name("UNKNOWN")

        assert result is None


class TestValidateReference:
    """Tests for reference validation."""

    def test_valid_reference(self, mock_db, sample_references):
        """Should return reference for valid reference_no."""
        mock_db.query.return_value = MockQuery([sample_references[0]])

        service = PhenotypeCurationService(mock_db)
        result = service.validate_reference(1001)

        assert result is not None
        assert result.reference_no == 1001

    def test_invalid_reference_raises_error(self, mock_db):
        """Should raise error for invalid reference."""
        mock_db.query.return_value = MockQuery([])

        service = PhenotypeCurationService(mock_db)

        with pytest.raises(PhenotypeCurationError) as exc_info:
            service.validate_reference(99999)

        assert "not found" in str(exc_info.value)


class TestGetOrCreatePhenotype:
    """Tests for phenotype get/create."""

    def test_returns_existing_phenotype(self, mock_db, sample_phenotypes):
        """Should return existing phenotype_no if found."""
        mock_db.query.return_value = MockQuery([sample_phenotypes[0]])

        service = PhenotypeCurationService(mock_db)
        result = service.get_or_create_phenotype(
            "classical genetics", "null", "cell morphology", "abnormal", "curator"
        )

        assert result == 1
        mock_db.add.assert_not_called()

    def test_creates_new_phenotype(self, mock_db):
        """Should create new phenotype if not found."""
        mock_db.query.return_value = MockQuery([])

        service = PhenotypeCurationService(mock_db)
        service.get_or_create_phenotype(
            "classical genetics", "null", "new_observable", "abnormal", "curator"
        )

        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()


class TestGetOrCreateExperiment:
    """Tests for experiment get/create."""

    def test_returns_none_without_comment(self, mock_db):
        """Should return None if no comment provided."""
        service = PhenotypeCurationService(mock_db)
        result = service.get_or_create_experiment(None, "curator")

        assert result is None
        mock_db.add.assert_not_called()

    def test_creates_experiment_with_comment(self, mock_db):
        """Should create experiment if comment provided."""
        service = PhenotypeCurationService(mock_db)
        service.get_or_create_experiment("Test experiment", "curator")

        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()


class TestGetOrCreateExptProperty:
    """Tests for experiment property get/create."""

    def test_returns_existing_property(self, mock_db):
        """Should return existing property_no if found."""
        prop = MockExptProperty(1, "strain_background", "SC5314")
        mock_db.query.return_value = MockQuery([prop])

        service = PhenotypeCurationService(mock_db)
        result = service.get_or_create_expt_property(
            "strain_background", "SC5314", None, "curator"
        )

        assert result == 1
        mock_db.add.assert_not_called()

    def test_creates_new_property(self, mock_db):
        """Should create new property if not found."""
        mock_db.query.return_value = MockQuery([])

        service = PhenotypeCurationService(mock_db)
        service.get_or_create_expt_property(
            "strain_background", "new_strain", None, "curator"
        )

        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()


class TestLinkExperimentToProperty:
    """Tests for experiment-property linking."""

    def test_skips_existing_link(self, mock_db):
        """Should not create duplicate link."""
        link = MockExptExptprop(1, 1)
        mock_db.query.return_value = MockQuery([link])

        service = PhenotypeCurationService(mock_db)
        service.link_experiment_to_property(1, 1)

        mock_db.add.assert_not_called()

    def test_creates_new_link(self, mock_db):
        """Should create link if not exists."""
        mock_db.query.return_value = MockQuery([])

        service = PhenotypeCurationService(mock_db)
        service.link_experiment_to_property(1, 1)

        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()


class TestGetAnnotationsForFeature:
    """Tests for annotation retrieval."""

    def test_returns_annotations(self, mock_db, sample_phenotypes, sample_references):
        """Should return annotations for feature."""
        annotation = MockPhenoAnnotation(1, 1, 1)
        ref_link = MockRefLink(1001, "PHENO_ANNOTATION", "PHENO_ANNOTATION_NO", 1)

        mock_db.query.side_effect = [
            MockQuery([annotation]),  # Annotations query
            MockQuery([sample_phenotypes[0]]),  # Phenotype lookup
            MockQuery([]),  # Experiment lookup (no experiment)
            MockQuery([ref_link]),  # RefLink lookup
            MockQuery([sample_references[0]]),  # Reference lookup
        ]

        service = PhenotypeCurationService(mock_db)
        results = service.get_annotations_for_feature(1)

        assert len(results) == 1
        assert results[0]["pheno_annotation_no"] == 1
        assert results[0]["observable"] == "cell morphology"

    def test_returns_empty_for_no_annotations(self, mock_db):
        """Should return empty list for feature with no annotations."""
        mock_db.query.return_value = MockQuery([])

        service = PhenotypeCurationService(mock_db)
        results = service.get_annotations_for_feature(999)

        assert results == []


class TestDeleteAnnotation:
    """Tests for annotation deletion."""

    def test_deletes_annotation(self, mock_db):
        """Should delete annotation and reference links."""
        annotation = MockPhenoAnnotation(1, 1, 1)

        # First query returns annotation, second is for ref_link deletion
        mock_db.query.side_effect = [
            MockQuery([annotation]),
            MockQuery([]),  # RefLink delete query
        ]

        service = PhenotypeCurationService(mock_db)
        result = service.delete_annotation(1, "curator")

        assert result is True
        mock_db.delete.assert_called_once_with(annotation)
        mock_db.commit.assert_called_once()

    def test_raises_for_unknown_annotation(self, mock_db):
        """Should raise error for unknown annotation."""
        mock_db.query.return_value = MockQuery([])

        service = PhenotypeCurationService(mock_db)

        with pytest.raises(PhenotypeCurationError) as exc_info:
            service.delete_annotation(999, "curator")

        assert "not found" in str(exc_info.value)


class TestGetCvTerms:
    """Tests for CV term retrieval."""

    def test_returns_experiment_types_from_code(self, mock_db):
        """Should return experiment types from Code table."""
        codes = [
            MockCode(1, "EXPERIMENT", "EXPERIMENT_TYPE", "classical genetics"),
            MockCode(2, "EXPERIMENT", "EXPERIMENT_TYPE", "RNAi"),
        ]
        mock_db.query.return_value = MockQuery([
            ("classical genetics",),
            ("RNAi",),
        ])

        service = PhenotypeCurationService(mock_db)
        results = service.get_cv_terms("experiment_type")

        assert "classical genetics" in results
        assert "RNAi" in results

    def test_fallback_to_hardcoded_experiment_types(self, mock_db):
        """Should use hardcoded values if DB returns nothing."""
        mock_db.query.return_value = MockQuery([])

        service = PhenotypeCurationService(mock_db)
        results = service.get_cv_terms("experiment_type")

        # Falls back to EXPERIMENT_TYPES constant
        assert "classical genetics" in results

    def test_fallback_to_hardcoded_mutant_types(self, mock_db):
        """Should use hardcoded mutant types if DB returns nothing."""
        mock_db.query.return_value = MockQuery([])

        service = PhenotypeCurationService(mock_db)
        results = service.get_cv_terms("mutant_type")

        assert "null" in results

    def test_fallback_to_hardcoded_qualifiers(self, mock_db):
        """Should use hardcoded qualifiers if DB returns nothing."""
        mock_db.query.return_value = MockQuery([])

        service = PhenotypeCurationService(mock_db)
        results = service.get_cv_terms("qualifier")

        assert "increased" in results
        assert "decreased" in results

    def test_returns_cv_terms_for_ontology(self, mock_db):
        """Should return terms from Cv/CvTerm tables for ontologies."""
        cv = MockCv(1, "observable")

        # "observable" is not in code_mapping, so it goes directly to Cv lookup
        mock_db.query.side_effect = [
            MockQuery([cv]),  # Cv lookup
            MockQuery([("cell morphology",), ("biofilm formation",)]),  # CvTerm lookup
        ]

        service = PhenotypeCurationService(mock_db)
        results = service.get_cv_terms("observable")

        assert "cell morphology" in results
        assert "biofilm formation" in results

    def test_returns_empty_for_unknown_cv(self, mock_db):
        """Should return empty list for unknown CV."""
        mock_db.query.return_value = MockQuery([])

        service = PhenotypeCurationService(mock_db)
        results = service.get_cv_terms("unknown_cv")

        assert results == []


class TestGetPropertyTypes:
    """Tests for property type retrieval."""

    def test_returns_property_types(self, mock_db):
        """Should return distinct property types."""
        mock_db.query.return_value = MockQuery([
            ("strain_background",),
            ("Allele",),
            ("chebi_ontology",),
        ])

        service = PhenotypeCurationService(mock_db)
        results = service.get_property_types()

        assert "strain_background" in results
        assert "Allele" in results

    def test_returns_empty_list(self, mock_db):
        """Should return empty list when no property types."""
        mock_db.query.return_value = MockQuery([])

        service = PhenotypeCurationService(mock_db)
        results = service.get_property_types()

        assert results == []


class TestServiceInitialization:
    """Tests for service initialization."""

    def test_stores_db_session(self, mock_db):
        """Service should store the database session."""
        service = PhenotypeCurationService(mock_db)
        assert service.db is mock_db


class TestPhenotypeCurationError:
    """Tests for custom exception."""

    def test_exception_message(self):
        """Should store and return error message."""
        error = PhenotypeCurationError("Test error message")
        assert str(error) == "Test error message"

    def test_is_exception(self):
        """Should be an Exception subclass."""
        error = PhenotypeCurationError("Test")
        assert isinstance(error, Exception)
