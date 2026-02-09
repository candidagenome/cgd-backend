"""
Tests for GO Curation Service.

Tests cover:
- Feature and GO term lookup
- GO ID validation with aspect checking
- Evidence code validation including IC requirements
- Reference validation with unlink checking
- Qualifier validation by aspect
- Annotation retrieval for features
- Annotation creation with validation
- Annotation update and deletion
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from cgd.api.services.curation.go_curation_service import (
    GoCurationService,
    GoCurationError,
)


class MockFeature:
    """Mock Feature model for testing."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        gene_name: str = None,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name


class MockGo:
    """Mock Go model for testing."""

    def __init__(
        self,
        go_no: int,
        goid: int,
        go_term: str,
        go_aspect: str,
    ):
        self.go_no = go_no
        self.goid = goid
        self.go_term = go_term
        self.go_aspect = go_aspect


class MockReference:
    """Mock Reference model for testing."""

    def __init__(
        self,
        reference_no: int,
        pubmed: int = None,
        citation: str = None,
    ):
        self.reference_no = reference_no
        self.pubmed = pubmed
        self.citation = citation


class MockRefUnlink:
    """Mock RefUnlink model for testing."""

    def __init__(
        self,
        pubmed: int,
        tab_name: str,
        primary_key: int,
    ):
        self.pubmed = pubmed
        self.tab_name = tab_name
        self.primary_key = primary_key


class MockGoAnnotation:
    """Mock GoAnnotation model for testing."""

    def __init__(
        self,
        go_annotation_no: int,
        go_no: int,
        feature_no: int,
        go_evidence: str = "IDA",
        annotation_type: str = "manually curated",
        source: str = "CGD",
        date_last_reviewed: datetime = None,
        date_created: datetime = None,
        created_by: str = None,
        go_ref: list = None,
    ):
        self.go_annotation_no = go_annotation_no
        self.go_no = go_no
        self.feature_no = feature_no
        self.go_evidence = go_evidence
        self.annotation_type = annotation_type
        self.source = source
        self.date_last_reviewed = date_last_reviewed or datetime.now()
        self.date_created = date_created or datetime.now()
        self.created_by = created_by
        self.go_ref = go_ref or []


class MockGoRef:
    """Mock GoRef model for testing."""

    def __init__(
        self,
        go_ref_no: int,
        go_annotation_no: int,
        reference_no: int,
        has_qualifier: str = "N",
        has_supporting_evidence: str = "N",
        go_qualifier: list = None,
    ):
        self.go_ref_no = go_ref_no
        self.go_annotation_no = go_annotation_no
        self.reference_no = reference_no
        self.has_qualifier = has_qualifier
        self.has_supporting_evidence = has_supporting_evidence
        self.go_qualifier = go_qualifier or []


class MockGoQualifier:
    """Mock GoQualifier model for testing."""

    def __init__(self, go_qualifier_no: int, go_ref_no: int, qualifier: str):
        self.go_qualifier_no = go_qualifier_no
        self.go_ref_no = go_ref_no
        self.qualifier = qualifier


class MockQuery:
    """Mock SQLAlchemy query object for testing."""

    def __init__(self, results=None):
        self._results = results or []
        self._count = len(self._results) if results else 0

    def filter(self, *args, **kwargs):
        return self

    def first(self):
        return self._results[0] if self._results else None

    def all(self):
        return self._results

    def count(self):
        return self._count


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
def sample_go_terms():
    """Sample GO terms for testing."""
    return [
        MockGo(1, 8150, "biological_process", "process"),
        MockGo(2, 3674, "molecular_function", "function"),
        MockGo(3, 5575, "cellular_component", "component"),
        MockGo(10, 6412, "translation", "process"),
        MockGo(20, 5198, "structural molecule activity", "function"),
        MockGo(30, 5737, "cytoplasm", "component"),
    ]


@pytest.fixture
def sample_references():
    """Sample references for testing."""
    return [
        MockReference(1001, 12345678, "Smith et al. 2020"),
        MockReference(1002, 87654321, "Jones et al. 2021"),
    ]


class TestEvidenceCodes:
    """Tests for evidence code constants."""

    def test_evidence_codes_list(self, mock_db):
        """Should have standard GO evidence codes."""
        service = GoCurationService(mock_db)
        assert "IDA" in service.EVIDENCE_CODES
        assert "IMP" in service.EVIDENCE_CODES
        assert "IGI" in service.EVIDENCE_CODES
        assert "IC" in service.EVIDENCE_CODES
        assert "TAS" in service.EVIDENCE_CODES
        assert "IEA" in service.EVIDENCE_CODES

    def test_evidence_codes_count(self, mock_db):
        """Should have expected number of evidence codes."""
        service = GoCurationService(mock_db)
        # Standard GO evidence codes
        assert len(service.EVIDENCE_CODES) >= 20


class TestQualifiers:
    """Tests for qualifier constants."""

    def test_function_qualifiers(self, mock_db):
        """Function aspect should have correct qualifiers."""
        service = GoCurationService(mock_db)
        assert "NOT" in service.QUALIFIERS["F"]
        assert "contributes_to" in service.QUALIFIERS["F"]

    def test_process_qualifiers(self, mock_db):
        """Process aspect should have correct qualifiers."""
        service = GoCurationService(mock_db)
        assert "NOT" in service.QUALIFIERS["P"]
        assert "acts_upstream_of" in service.QUALIFIERS["P"]

    def test_component_qualifiers(self, mock_db):
        """Component aspect should have correct qualifiers."""
        service = GoCurationService(mock_db)
        assert "NOT" in service.QUALIFIERS["C"]
        assert "colocalizes_with" in service.QUALIFIERS["C"]
        assert "part_of" in service.QUALIFIERS["C"]


class TestGetFeatureByName:
    """Tests for feature lookup."""

    def test_finds_by_feature_name(self, mock_db, sample_features):
        """Should find feature by feature_name."""
        mock_db.query.return_value = MockQuery([sample_features[0]])

        service = GoCurationService(mock_db)
        result = service.get_feature_by_name("orf19.1")

        assert result is not None
        assert result.feature_name == "orf19.1"

    def test_finds_by_gene_name(self, mock_db, sample_features):
        """Should find feature by gene_name."""
        mock_db.query.return_value = MockQuery([sample_features[0]])

        service = GoCurationService(mock_db)
        result = service.get_feature_by_name("ACT1")

        assert result is not None
        assert result.gene_name == "ACT1"

    def test_returns_none_for_unknown(self, mock_db):
        """Should return None for unknown feature."""
        mock_db.query.return_value = MockQuery([])

        service = GoCurationService(mock_db)
        result = service.get_feature_by_name("UNKNOWN")

        assert result is None


class TestGetGoByGoid:
    """Tests for GO term lookup."""

    def test_finds_go_term(self, mock_db, sample_go_terms):
        """Should find GO term by GOID."""
        mock_db.query.return_value = MockQuery([sample_go_terms[0]])

        service = GoCurationService(mock_db)
        result = service.get_go_by_goid(8150)

        assert result is not None
        assert result.goid == 8150

    def test_returns_none_for_unknown(self, mock_db):
        """Should return None for unknown GOID."""
        mock_db.query.return_value = MockQuery([])

        service = GoCurationService(mock_db)
        result = service.get_go_by_goid(99999999)

        assert result is None


class TestValidateGoid:
    """Tests for GO ID validation."""

    def test_valid_goid(self, mock_db, sample_go_terms):
        """Should return GO term for valid GOID."""
        mock_db.query.return_value = MockQuery([sample_go_terms[0]])

        service = GoCurationService(mock_db)
        result = service.validate_goid(8150)

        assert result is not None
        assert result.goid == 8150

    def test_invalid_goid_raises_error(self, mock_db):
        """Should raise error for invalid GOID."""
        mock_db.query.return_value = MockQuery([])

        service = GoCurationService(mock_db)

        with pytest.raises(GoCurationError) as exc_info:
            service.validate_goid(99999999)

        assert "not found" in str(exc_info.value)

    def test_validates_aspect_match(self, mock_db, sample_go_terms):
        """Should validate aspect matches."""
        mock_db.query.return_value = MockQuery([sample_go_terms[0]])  # process

        service = GoCurationService(mock_db)
        result = service.validate_goid(8150, "P")

        assert result is not None

    def test_validates_aspect_match_full_name(self, mock_db, sample_go_terms):
        """Should accept full aspect names."""
        mock_db.query.return_value = MockQuery([sample_go_terms[0]])  # process

        service = GoCurationService(mock_db)
        result = service.validate_goid(8150, "process")

        assert result is not None

    def test_aspect_mismatch_raises_error(self, mock_db, sample_go_terms):
        """Should raise error for aspect mismatch."""
        mock_db.query.return_value = MockQuery([sample_go_terms[0]])  # process

        service = GoCurationService(mock_db)

        with pytest.raises(GoCurationError) as exc_info:
            service.validate_goid(8150, "F")  # Expecting function

        assert "expected" in str(exc_info.value).lower()


class TestValidateEvidenceCode:
    """Tests for evidence code validation."""

    def test_valid_evidence_code(self, mock_db):
        """Should accept valid evidence codes."""
        service = GoCurationService(mock_db)

        assert service.validate_evidence_code("IDA") == "IDA"
        assert service.validate_evidence_code("IMP") == "IMP"
        assert service.validate_evidence_code("TAS") == "TAS"

    def test_normalizes_case(self, mock_db):
        """Should normalize evidence code to uppercase."""
        service = GoCurationService(mock_db)

        assert service.validate_evidence_code("ida") == "IDA"
        assert service.validate_evidence_code("Imp") == "IMP"

    def test_invalid_evidence_raises_error(self, mock_db):
        """Should raise error for invalid evidence code."""
        service = GoCurationService(mock_db)

        with pytest.raises(GoCurationError) as exc_info:
            service.validate_evidence_code("INVALID")

        assert "Invalid evidence code" in str(exc_info.value)

    def test_ic_requires_from_goid(self, mock_db):
        """IC evidence should require from_goid."""
        service = GoCurationService(mock_db)

        with pytest.raises(GoCurationError) as exc_info:
            service.validate_evidence_code("IC")

        assert "from GO ID" in str(exc_info.value)

    def test_ic_with_from_goid_valid(self, mock_db):
        """IC evidence with from_goid should be valid."""
        service = GoCurationService(mock_db)

        result = service.validate_evidence_code("IC", ic_from_goid=12345)
        assert result == "IC"


class TestValidateReference:
    """Tests for reference validation."""

    def test_valid_reference(self, mock_db, sample_references):
        """Should return reference for valid reference_no."""
        mock_db.query.return_value = MockQuery([sample_references[0]])

        service = GoCurationService(mock_db)
        result = service.validate_reference(1001)

        assert result is not None
        assert result.reference_no == 1001

    def test_invalid_reference_raises_error(self, mock_db):
        """Should raise error for invalid reference."""
        mock_db.query.return_value = MockQuery([])

        service = GoCurationService(mock_db)

        with pytest.raises(GoCurationError) as exc_info:
            service.validate_reference(99999)

        assert "not found" in str(exc_info.value)

    def test_unlinked_reference_raises_error(self, mock_db, sample_references):
        """Should raise error for reference unlinked from feature."""
        # Reference with pubmed 12345678
        ref = sample_references[0]
        unlink = MockRefUnlink(pubmed=12345678, tab_name="FEATURE", primary_key=1)

        mock_db.query.side_effect = [
            MockQuery([ref]),  # Reference lookup
            MockQuery([unlink]),  # RefUnlink lookup - found
        ]

        service = GoCurationService(mock_db)

        with pytest.raises(GoCurationError) as exc_info:
            service.validate_reference(1, feature_no=1)

        assert "unlinked" in str(exc_info.value)

    def test_linked_reference_valid(self, mock_db, sample_references):
        """Should accept reference not unlinked from feature."""
        ref = sample_references[0]

        mock_db.query.side_effect = [
            MockQuery([ref]),  # Reference lookup
            MockQuery([]),  # RefUnlink lookup - not found (not unlinked)
        ]

        service = GoCurationService(mock_db)
        result = service.validate_reference(1001, feature_no=1)

        assert result.reference_no == 1001


class TestValidateQualifiers:
    """Tests for qualifier validation."""

    def test_valid_function_qualifiers(self, mock_db):
        """Should accept valid function qualifiers."""
        service = GoCurationService(mock_db)

        result = service.validate_qualifiers(["NOT"], "F")
        assert result == ["NOT"]

        result = service.validate_qualifiers(["contributes_to"], "function")
        assert result == ["contributes_to"]

    def test_valid_process_qualifiers(self, mock_db):
        """Should accept valid process qualifiers."""
        service = GoCurationService(mock_db)

        result = service.validate_qualifiers(["NOT"], "P")
        assert result == ["NOT"]

        result = service.validate_qualifiers(["acts_upstream_of"], "process")
        assert result == ["acts_upstream_of"]

    def test_valid_component_qualifiers(self, mock_db):
        """Should accept valid component qualifiers."""
        service = GoCurationService(mock_db)

        result = service.validate_qualifiers(["part_of"], "C")
        assert result == ["part_of"]

        result = service.validate_qualifiers(["colocalizes_with"], "component")
        assert result == ["colocalizes_with"]

    def test_invalid_qualifier_raises_error(self, mock_db):
        """Should raise error for invalid qualifier."""
        service = GoCurationService(mock_db)

        with pytest.raises(GoCurationError) as exc_info:
            service.validate_qualifiers(["invalid_qualifier"], "F")

        assert "Invalid qualifier" in str(exc_info.value)

    def test_qualifier_wrong_aspect_raises_error(self, mock_db):
        """Should raise error for qualifier not valid for aspect."""
        service = GoCurationService(mock_db)

        # colocalizes_with is only valid for C
        with pytest.raises(GoCurationError) as exc_info:
            service.validate_qualifiers(["colocalizes_with"], "F")

        assert "Invalid qualifier" in str(exc_info.value)


class TestGetAnnotationsForFeature:
    """Tests for annotation retrieval."""

    def test_returns_annotations(self, mock_db, sample_go_terms, sample_references):
        """Should return annotations for feature."""
        go_qualifier = MockGoQualifier(1, 1, "NOT")
        go_ref = MockGoRef(1, 1, 1001, "Y", "N", [go_qualifier])
        annotation = MockGoAnnotation(
            1, sample_go_terms[0].go_no, 1, "IDA", "manually curated", "CGD",
            go_ref=[go_ref]
        )

        mock_db.query.side_effect = [
            MockQuery([annotation]),  # Annotations query
            MockQuery([sample_go_terms[0]]),  # GO lookup
            MockQuery([sample_references[0]]),  # Reference lookup
        ]

        service = GoCurationService(mock_db)
        results = service.get_annotations_for_feature(1)

        assert len(results) == 1
        assert results[0]["go_annotation_no"] == 1
        assert results[0]["go_evidence"] == "IDA"
        assert len(results[0]["references"]) == 1

    def test_returns_empty_for_no_annotations(self, mock_db):
        """Should return empty list for feature with no annotations."""
        mock_db.query.return_value = MockQuery([])

        service = GoCurationService(mock_db)
        results = service.get_annotations_for_feature(999)

        assert results == []


class TestUpdateDateLastReviewed:
    """Tests for updating review date."""

    def test_updates_date(self, mock_db):
        """Should update date_last_reviewed."""
        annotation = MockGoAnnotation(1, 1, 1)
        mock_db.query.return_value = MockQuery([annotation])

        service = GoCurationService(mock_db)
        result = service.update_date_last_reviewed(1, "curator")

        assert result is True
        mock_db.commit.assert_called_once()

    def test_raises_for_unknown_annotation(self, mock_db):
        """Should raise error for unknown annotation."""
        mock_db.query.return_value = MockQuery([])

        service = GoCurationService(mock_db)

        with pytest.raises(GoCurationError) as exc_info:
            service.update_date_last_reviewed(999, "curator")

        assert "not found" in str(exc_info.value)


class TestDeleteAnnotation:
    """Tests for annotation deletion."""

    def test_deletes_annotation(self, mock_db):
        """Should delete annotation."""
        annotation = MockGoAnnotation(1, 1, 1)
        mock_db.query.return_value = MockQuery([annotation])

        service = GoCurationService(mock_db)
        result = service.delete_annotation(1, "curator")

        assert result is True
        mock_db.delete.assert_called_once_with(annotation)
        mock_db.commit.assert_called_once()

    def test_raises_for_unknown_annotation(self, mock_db):
        """Should raise error for unknown annotation."""
        mock_db.query.return_value = MockQuery([])

        service = GoCurationService(mock_db)

        with pytest.raises(GoCurationError) as exc_info:
            service.delete_annotation(999, "curator")

        assert "not found" in str(exc_info.value)


class TestDeleteReferenceFromAnnotation:
    """Tests for removing reference from annotation."""

    def test_deletes_reference(self, mock_db):
        """Should delete reference when multiple refs exist."""
        go_ref = MockGoRef(1, 1, 1001)
        mock_query = MockQuery([go_ref])
        mock_query._count = 2  # Multiple references

        mock_db.query.return_value = mock_query

        service = GoCurationService(mock_db)
        result = service.delete_reference_from_annotation(1, "curator")

        assert result is True
        mock_db.delete.assert_called_once_with(go_ref)

    def test_raises_for_only_reference(self, mock_db):
        """Should raise error when trying to delete only reference."""
        go_ref = MockGoRef(1, 1, 1001)
        mock_query = MockQuery([go_ref])
        mock_query._count = 1  # Only one reference

        mock_db.query.return_value = mock_query

        service = GoCurationService(mock_db)

        with pytest.raises(GoCurationError) as exc_info:
            service.delete_reference_from_annotation(1, "curator")

        assert "only reference" in str(exc_info.value).lower()

    def test_raises_for_unknown_reference(self, mock_db):
        """Should raise error for unknown go_ref."""
        mock_db.query.return_value = MockQuery([])

        service = GoCurationService(mock_db)

        with pytest.raises(GoCurationError) as exc_info:
            service.delete_reference_from_annotation(999, "curator")

        assert "not found" in str(exc_info.value)


class TestServiceInitialization:
    """Tests for service initialization."""

    def test_stores_db_session(self, mock_db):
        """Service should store the database session."""
        service = GoCurationService(mock_db)
        assert service.db is mock_db


class TestGoCurationError:
    """Tests for custom exception."""

    def test_exception_message(self):
        """Should store and return error message."""
        error = GoCurationError("Test error message")
        assert str(error) == "Test error message"

    def test_is_exception(self):
        """Should be an Exception subclass."""
        error = GoCurationError("Test")
        assert isinstance(error, Exception)
