"""
Tests for Colleague Service.

Tests cover:
- Full name building
- URL retrieval
- Address building
- Colleague search
- Colleague detail retrieval
- Form config
- Colleague submission
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from cgd.api.services.colleague_service import (
    _build_full_name,
    _get_colleague_urls,
    _build_address,
    search_colleagues,
    get_colleague_detail,
    get_colleague_form_config,
    submit_colleague,
    _validate_email,
)


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
        job_title: str = None,
        profession: str = None,
        institution: str = None,
        address1: str = None,
        address2: str = None,
        address3: str = None,
        address4: str = None,
        address5: str = None,
        city: str = None,
        state: str = None,
        region: str = None,
        country: str = None,
        postal_code: str = None,
        work_phone: str = None,
        other_phone: str = None,
        fax: str = None,
        date_modified: datetime = None,
    ):
        self.colleague_no = colleague_no
        self.last_name = last_name
        self.first_name = first_name
        self.other_last_name = other_last_name
        self.suffix = suffix
        self.email = email
        self.job_title = job_title
        self.profession = profession
        self.institution = institution
        self.address1 = address1
        self.address2 = address2
        self.address3 = address3
        self.address4 = address4
        self.address5 = address5
        self.city = city
        self.state = state
        self.region = region
        self.country = country
        self.postal_code = postal_code
        self.work_phone = work_phone
        self.other_phone = other_phone
        self.fax = fax
        self.date_modified = date_modified


class MockUrl:
    """Mock Url model."""

    def __init__(self, url: str, url_type: str = None):
        self.url = url
        self.url_type = url_type


class MockFeature:
    """Mock Feature model."""

    def __init__(self, feature_no: int, feature_name: str, gene_name: str = None):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name


class MockCollRelationship:
    """Mock CollRelationship model."""

    def __init__(
        self,
        colleague_no: int,
        associate_no: int,
        relationship_type: str,
    ):
        self.colleague_no = colleague_no
        self.associate_no = associate_no
        self.relationship_type = relationship_type


class MockCollFeat:
    """Mock CollFeat model."""

    def __init__(self, colleague_no: int, feature_no: int):
        self.colleague_no = colleague_no
        self.feature_no = feature_no


class MockColleagueRemark:
    """Mock ColleagueRemark model."""

    def __init__(self, colleague_no: int, remark: str, remark_type: str = None):
        self.colleague_no = colleague_no
        self.remark = remark
        self.remark_type = remark_type


class MockKeyword:
    """Mock Keyword model."""

    def __init__(self, keyword_no: int, keyword: str, kw_source: str = None):
        self.keyword_no = keyword_no
        self.keyword = keyword
        self.kw_source = kw_source


class MockCode:
    """Mock Code model."""

    def __init__(self, code_value: str):
        self.code_value = code_value


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

    def offset(self, n):
        return self

    def limit(self, n):
        return self

    def count(self):
        return len(self._results)

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
def sample_colleague():
    """Create sample colleague."""
    return MockColleague(
        colleague_no=1,
        last_name="Smith",
        first_name="John",
        email="john.smith@example.com",
        institution="MIT",
        city="Cambridge",
        state="MA",
        country="USA",
        postal_code="02139",
    )


class TestBuildFullName:
    """Tests for _build_full_name."""

    def test_builds_basic_name(self):
        """Should build basic last, first name."""
        result = _build_full_name("Smith", "John")
        assert result == "Smith, John"

    def test_includes_suffix(self):
        """Should include suffix when provided."""
        result = _build_full_name("Smith", "John", "Jr.")
        assert result == "Smith, John Jr."

    def test_handles_no_suffix(self):
        """Should handle None suffix."""
        result = _build_full_name("Smith", "John", None)
        assert result == "Smith, John"


class TestBuildAddress:
    """Tests for _build_address."""

    def test_builds_full_address(self):
        """Should build address with all components."""
        colleague = MockColleague(
            1, "Smith", "John",
            address1="123 Main St",
            city="Cambridge",
            state="MA",
            postal_code="02139",
            country="USA",
        )

        result = _build_address(colleague)

        assert "123 Main St" in result
        assert "Cambridge" in result
        assert "MA 02139" in result
        assert "USA" in result

    def test_handles_empty_address(self):
        """Should return None for empty address."""
        colleague = MockColleague(1, "Smith", "John")

        result = _build_address(colleague)

        assert result is None

    def test_uses_region_when_no_state(self):
        """Should use region when state is not set."""
        colleague = MockColleague(
            1, "Smith", "John",
            city="London",
            region="England",
            country="UK",
        )

        result = _build_address(colleague)

        assert "England" in result


class TestValidateEmail:
    """Tests for _validate_email."""

    def test_valid_email(self):
        """Should accept valid emails."""
        assert _validate_email("test@example.com")
        assert _validate_email("user.name@domain.co.uk")

    def test_invalid_email(self):
        """Should reject invalid emails."""
        assert not _validate_email("invalid")
        assert not _validate_email("no@domain")
        assert not _validate_email("@domain.com")


class TestSearchColleagues:
    """Tests for search_colleagues."""

    def test_returns_error_for_empty_search(self, mock_db):
        """Should return error for empty search term."""
        result = search_colleagues(mock_db, "")

        assert result.success is False
        assert "required" in result.error

    def test_returns_matching_colleagues(self, mock_db, sample_colleague):
        """Should return matching colleagues."""
        # Create a properly structured mock that supports counting
        count_query = MagicMock()
        count_query.count.return_value = 1
        count_query.order_by.return_value = count_query
        count_query.offset.return_value = count_query
        count_query.limit.return_value = count_query
        count_query.all.return_value = [sample_colleague]
        count_query.filter.return_value = count_query

        mock_db.query.side_effect = [
            count_query,  # Base query used for count and results
            MockQuery([]),  # URLs for colleague (tuples)
        ]

        result = search_colleagues(mock_db, "Smith")

        assert result.success is True
        assert len(result.colleagues) == 1
        assert result.colleagues[0].last_name == "Smith"

    def test_appends_wildcard_when_no_results(self, mock_db, sample_colleague):
        """Should append wildcard when no results found."""
        # First query returns no results
        empty_query = MagicMock()
        empty_query.count.return_value = 0
        empty_query.filter.return_value = empty_query

        # Second query with wildcard returns results
        with_wildcard = MagicMock()
        with_wildcard.count.return_value = 1
        with_wildcard.order_by.return_value = with_wildcard
        with_wildcard.offset.return_value = with_wildcard
        with_wildcard.limit.return_value = with_wildcard
        with_wildcard.all.return_value = [sample_colleague]
        with_wildcard.filter.return_value = with_wildcard

        mock_db.query.side_effect = [
            empty_query,  # First count - no results
            with_wildcard,  # Second query with wildcard
            MockQuery([]),  # URLs
        ]

        result = search_colleagues(mock_db, "Sm")

        assert result.success is True
        assert result.wildcard_appended is True
        assert result.search_term == "Sm*"

    def test_pagination_info(self, mock_db, sample_colleague):
        """Should include pagination info."""
        count_query = MagicMock()
        count_query.count.return_value = 25
        count_query.order_by.return_value = count_query
        count_query.offset.return_value = count_query
        count_query.limit.return_value = count_query
        count_query.all.return_value = [sample_colleague]
        count_query.filter.return_value = count_query

        mock_db.query.side_effect = [
            count_query,  # Base query
            MockQuery([]),  # URLs
        ]

        result = search_colleagues(mock_db, "Smith", page=1, page_size=10)

        assert result.total_count == 25
        assert result.page == 1
        assert result.page_size == 10
        assert result.total_pages == 3


class TestGetColleagueDetail:
    """Tests for get_colleague_detail."""

    def test_returns_error_for_unknown_colleague(self, mock_db):
        """Should return error for unknown colleague."""
        mock_db.query.return_value = MockQuery([])

        result = get_colleague_detail(mock_db, 999)

        assert result.success is False
        assert "not found" in result.error

    def test_returns_colleague_detail(self, mock_db, sample_colleague):
        """Should return colleague detail."""
        mock_db.query.side_effect = [
            MockQuery([sample_colleague]),  # Colleague lookup
            MockQuery([]),  # URLs (returns tuples)
            MockQuery([]),  # Lab heads (PI relationships)
            MockQuery([]),  # Lab members
            MockQuery([]),  # Associates
            MockQuery([]),  # Associated features
            MockQuery([]),  # Remarks
            MockQuery([]),  # Keywords (returns tuples)
        ]

        result = get_colleague_detail(mock_db, 1)

        assert result.success is True
        assert result.colleague.last_name == "Smith"
        assert result.colleague.full_name == "Smith, John"

    def test_includes_lab_heads(self, mock_db, sample_colleague):
        """Should include lab heads (PI)."""
        pi = MockColleague(2, "Professor", "Jane")
        relationship = MockCollRelationship(2, 1, "Lab member")

        mock_db.query.side_effect = [
            MockQuery([sample_colleague]),  # Colleague lookup
            MockQuery([]),  # URLs
            MockQuery([relationship]),  # PI relationships
            MockQuery([pi]),  # PI colleague lookup
            MockQuery([]),  # Lab members
            MockQuery([]),  # Associates
            MockQuery([]),  # Associated features
            MockQuery([]),  # Remarks
            MockQuery([]),  # Keywords
        ]

        result = get_colleague_detail(mock_db, 1)

        assert len(result.colleague.lab_heads) == 1
        assert result.colleague.lab_heads[0].full_name == "Professor, Jane"

    def test_includes_research_interests(self, mock_db, sample_colleague):
        """Should include research interests."""
        remark = MockColleagueRemark(1, "Fungal genetics", "Research interest")

        mock_db.query.side_effect = [
            MockQuery([sample_colleague]),  # Colleague lookup
            MockQuery([]),  # URLs
            MockQuery([]),  # Lab heads
            MockQuery([]),  # Lab members
            MockQuery([]),  # Associates
            MockQuery([]),  # Associated features
            MockQuery([remark]),  # Remarks
            MockQuery([]),  # Keywords
        ]

        result = get_colleague_detail(mock_db, 1)

        assert result.colleague.research_interests == "Fungal genetics"

    def test_includes_associated_genes(self, mock_db, sample_colleague):
        """Should include associated genes."""
        coll_feat = MockCollFeat(1, 100)
        feature = MockFeature(100, "CAL0001", "ALS1")

        mock_db.query.side_effect = [
            MockQuery([sample_colleague]),  # Colleague lookup
            MockQuery([]),  # URLs
            MockQuery([]),  # Lab heads
            MockQuery([]),  # Lab members
            MockQuery([]),  # Associates
            MockQuery([coll_feat]),  # Associated features
            MockQuery([feature]),  # Feature lookup
            MockQuery([]),  # Remarks
            MockQuery([]),  # Keywords
        ]

        result = get_colleague_detail(mock_db, 1)

        assert len(result.colleague.associated_genes) == 1
        assert result.colleague.associated_genes[0].gene_name == "ALS1"


class TestGetColleagueFormConfig:
    """Tests for get_colleague_form_config."""

    def test_returns_config(self, mock_db):
        """Should return form config."""
        mock_db.query.side_effect = [
            MockQuery([("USA",), ("Canada",)]),  # Countries
            MockQuery([("MA",), ("CA",)]),  # US states
            MockQuery([]),  # Canadian provinces (will use defaults)
            MockQuery([("Scientist",)]),  # Professions
            MockQuery([("Professor",)]),  # Positions
        ]

        result = get_colleague_form_config(mock_db)

        assert "countries" in result
        assert "USA" in result["countries"]
        assert "us_states" in result
        assert "canadian_provinces" in result
        assert "professions" in result
        assert "positions" in result


class TestSubmitColleague:
    """Tests for submit_colleague."""

    def test_requires_last_name(self, mock_db):
        """Should require last name."""
        result = submit_colleague(mock_db, None, {
            "first_name": "John",
            "email": "john@example.com",
            "institution": "MIT",
        })

        assert result["success"] is False
        assert any("Last name" in e for e in result["errors"])

    def test_requires_email(self, mock_db):
        """Should require email."""
        result = submit_colleague(mock_db, None, {
            "last_name": "Smith",
            "first_name": "John",
            "institution": "MIT",
        })

        assert result["success"] is False
        assert any("Email" in e for e in result["errors"])

    def test_validates_email_format(self, mock_db):
        """Should validate email format."""
        result = submit_colleague(mock_db, None, {
            "last_name": "Smith",
            "first_name": "John",
            "email": "invalid",
            "institution": "MIT",
        })

        assert result["success"] is False
        assert any("Invalid email" in e for e in result["errors"])

    def test_requires_us_state_for_usa(self, mock_db):
        """Should require state for USA."""
        result = submit_colleague(mock_db, None, {
            "last_name": "Smith",
            "first_name": "John",
            "email": "john@example.com",
            "institution": "MIT",
            "country": "USA",
        })

        assert result["success"] is False
        assert any("US state" in e for e in result["errors"])

    @patch('cgd.api.services.submission_utils.write_colleague_submission')
    def test_success_new_colleague(self, mock_write, mock_db):
        """Should succeed with valid new colleague data."""
        mock_write.return_value = "/tmp/submission.txt"

        result = submit_colleague(mock_db, None, {
            "last_name": "Smith",
            "first_name": "John",
            "email": "john@example.com",
            "institution": "MIT",
        })

        assert result["success"] is True
        assert "submitted" in result["message"]

    @patch('cgd.api.services.submission_utils.write_colleague_submission')
    def test_success_update_colleague(self, mock_write, mock_db, sample_colleague):
        """Should succeed with valid update data."""
        mock_write.return_value = "/tmp/submission.txt"
        mock_db.query.return_value = MockQuery([sample_colleague])

        result = submit_colleague(mock_db, 1, {
            "last_name": "Smith",
            "first_name": "John",
            "email": "john@example.com",
            "institution": "MIT",
        })

        assert result["success"] is True
        assert "updated" in result["message"]

    def test_error_update_nonexistent_colleague(self, mock_db):
        """Should error when updating non-existent colleague."""
        mock_db.query.return_value = MockQuery([])

        result = submit_colleague(mock_db, 999, {
            "last_name": "Smith",
            "first_name": "John",
            "email": "john@example.com",
            "institution": "MIT",
        })

        assert result["success"] is False
        assert any("not found" in e for e in result["errors"])
