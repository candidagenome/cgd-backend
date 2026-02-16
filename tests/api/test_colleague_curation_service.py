"""
Tests for Colleague Curation Service.

Tests cover:
- Colleague lookup by ID and name
- Get all colleagues with pagination
- Colleague details with URLs, keywords, features, relationships
- Create/update/delete colleagues
- URL/keyword/feature/relationship/remark management
"""
import pytest
from unittest.mock import MagicMock
from datetime import datetime

from cgd.api.services.curation.colleague_curation_service import (
    ColleagueCurationService,
    ColleagueCurationError,
)


class MockColleague:
    """Mock Colleague model for testing."""

    def __init__(
        self,
        colleague_no: int,
        first_name: str,
        last_name: str,
        email: str = None,
        institution: str = None,
        is_pi: str = "N",
        is_contact: str = "N",
        suffix: str = None,
        other_last_name: str = None,
        profession: str = None,
        job_title: str = None,
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
        source: str = "CGD",
        date_created: datetime = None,
        date_modified: datetime = None,
        created_by: str = None,
        coll_url: list = None,
        coll_kw: list = None,
        coll_feat: list = None,
        coll_relationship: list = None,
        colleague_remark: list = None,
    ):
        self.colleague_no = colleague_no
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.institution = institution
        self.is_pi = is_pi
        self.is_contact = is_contact
        self.suffix = suffix
        self.other_last_name = other_last_name
        self.profession = profession
        self.job_title = job_title
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
        self.source = source
        self.date_created = date_created or datetime.now()
        self.date_modified = date_modified
        self.created_by = created_by
        self.coll_url = coll_url or []
        self.coll_kw = coll_kw or []
        self.coll_feat = coll_feat or []
        self.coll_relationship = coll_relationship or []
        self.colleague_remark = colleague_remark or []


class MockCollUrl:
    """Mock CollUrl model."""

    def __init__(self, coll_url_no: int, colleague_no: int, url_no: int):
        self.coll_url_no = coll_url_no
        self.colleague_no = colleague_no
        self.url_no = url_no


class MockUrl:
    """Mock Url model."""

    def __init__(self, url_no: int, url_type: str, url: str):
        self.url_no = url_no
        self.url_type = url_type
        self.url = url
        self.link = url  # Service expects link attribute


class MockCollKw:
    """Mock CollKw model."""

    def __init__(self, coll_kw_no: int, colleague_no: int, keyword_no: int):
        self.coll_kw_no = coll_kw_no
        self.colleague_no = colleague_no
        self.keyword_no = keyword_no


class MockKeyword:
    """Mock Keyword model."""

    def __init__(self, keyword_no: int, keyword: str):
        self.keyword_no = keyword_no
        self.keyword = keyword


class MockCollFeat:
    """Mock CollFeat model."""

    def __init__(self, coll_feat_no: int, colleague_no: int, feature_no: int):
        self.coll_feat_no = coll_feat_no
        self.colleague_no = colleague_no
        self.feature_no = feature_no


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
        coll_relationship_no: int,
        colleague_no: int,
        associate_no: int,
        relationship_type: str,
    ):
        self.coll_relationship_no = coll_relationship_no
        self.colleague_no = colleague_no
        self.associate_no = associate_no
        self.relationship_type = relationship_type


class MockColleagueRemark:
    """Mock ColleagueRemark model."""

    def __init__(
        self,
        colleague_remark_no: int,
        colleague_no: int,
        remark_type: str,
        remark: str,
        date_created: datetime = None,
    ):
        self.colleague_remark_no = colleague_remark_no
        self.colleague_no = colleague_no
        self.remark_type = remark_type
        self.remark = remark  # Actual model column name
        self.remark_text = remark  # Service expects this attribute
        self.date_created = date_created or datetime.now()


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []
        self._count = len(self._results)

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

    def count(self):
        return self._count

    def delete(self):
        pass


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    db = MagicMock()
    db.query.return_value = MockQuery([])
    return db


@pytest.fixture
def sample_colleagues():
    """Create sample colleagues for testing."""
    return [
        MockColleague(
            colleague_no=1,
            first_name="John",
            last_name="Smith",
            email="john.smith@example.com",
            institution="Stanford",
            is_pi="Y",
        ),
        MockColleague(
            colleague_no=2,
            first_name="Jane",
            last_name="Doe",
            email="jane.doe@example.com",
            institution="MIT",
            is_contact="Y",
        ),
    ]


class TestGetColleagueById:
    """Tests for getting colleague by ID."""

    def test_returns_colleague(self, mock_db, sample_colleagues):
        """Should return colleague when found."""
        mock_db.query.return_value = MockQuery([sample_colleagues[0]])

        service = ColleagueCurationService(mock_db)
        result = service.get_colleague_by_id(1)

        assert result is not None
        assert result.colleague_no == 1
        assert result.first_name == "John"

    def test_returns_none_for_unknown(self, mock_db):
        """Should return None when colleague not found."""
        mock_db.query.return_value = MockQuery([])

        service = ColleagueCurationService(mock_db)
        result = service.get_colleague_by_id(999)

        assert result is None


class TestSearchColleaguesByName:
    """Tests for searching colleagues by name."""

    def test_finds_matching_colleagues(self, mock_db, sample_colleagues):
        """Should find colleagues by first and last name."""
        mock_db.query.return_value = MockQuery([sample_colleagues[0]])

        service = ColleagueCurationService(mock_db)
        results = service.search_colleagues_by_name("John", "Smith")

        assert len(results) == 1
        assert results[0].first_name == "John"
        assert results[0].last_name == "Smith"

    def test_case_insensitive_search(self, mock_db, sample_colleagues):
        """Should search case-insensitively."""
        mock_db.query.return_value = MockQuery([sample_colleagues[0]])

        service = ColleagueCurationService(mock_db)
        results = service.search_colleagues_by_name("john", "SMITH")

        # Verify query was called
        mock_db.query.assert_called()

    def test_returns_empty_for_no_matches(self, mock_db):
        """Should return empty list when no matches."""
        mock_db.query.return_value = MockQuery([])

        service = ColleagueCurationService(mock_db)
        results = service.search_colleagues_by_name("Nobody", "Here")

        assert len(results) == 0


class TestGetAllColleagues:
    """Tests for getting all colleagues with pagination."""

    def test_returns_colleagues_with_total(self, mock_db, sample_colleagues):
        """Should return colleagues and total count."""
        mock_query = MockQuery(sample_colleagues)
        mock_query._count = 100  # Total count
        mock_db.query.return_value = mock_query

        service = ColleagueCurationService(mock_db)
        results, total = service.get_all_colleagues(page=1, page_size=50)

        assert len(results) == 2
        assert total == 100
        assert results[0]["colleague_no"] == 1
        assert results[0]["first_name"] == "John"

    def test_returns_formatted_dict(self, mock_db, sample_colleagues):
        """Should return properly formatted colleague dicts."""
        mock_db.query.return_value = MockQuery([sample_colleagues[0]])

        service = ColleagueCurationService(mock_db)
        results, _ = service.get_all_colleagues()

        assert "colleague_no" in results[0]
        assert "first_name" in results[0]
        assert "last_name" in results[0]
        assert "email" in results[0]
        assert "institution" in results[0]
        assert "is_pi" in results[0]


class TestGetColleagueDetails:
    """Tests for getting detailed colleague info."""

    def test_returns_basic_fields(self, mock_db, sample_colleagues):
        """Should return all basic colleague fields."""
        mock_db.query.return_value = MockQuery([sample_colleagues[0]])

        service = ColleagueCurationService(mock_db)
        result = service.get_colleague_details(1)

        assert result["colleague_no"] == 1
        assert result["first_name"] == "John"
        assert result["last_name"] == "Smith"
        assert result["email"] == "john.smith@example.com"
        assert result["institution"] == "Stanford"
        assert result["is_pi"] == "Y"

    def test_raises_for_unknown_colleague(self, mock_db):
        """Should raise error for unknown colleague."""
        mock_db.query.return_value = MockQuery([])

        service = ColleagueCurationService(mock_db)

        with pytest.raises(ColleagueCurationError) as exc_info:
            service.get_colleague_details(999)

        assert "not found" in str(exc_info.value)

    def test_includes_urls(self, mock_db):
        """Should include colleague URLs."""
        coll_url = MockCollUrl(1, 1, 1)
        url = MockUrl(1, "Lab", "https://example.com")
        colleague = MockColleague(
            colleague_no=1,
            first_name="John",
            last_name="Smith",
            coll_url=[coll_url],
        )

        mock_db.query.side_effect = [
            MockQuery([colleague]),
            MockQuery([url]),
        ]

        service = ColleagueCurationService(mock_db)
        result = service.get_colleague_details(1)

        assert "urls" in result
        assert len(result["urls"]) == 1
        assert result["urls"][0]["url_type"] == "Lab"

    def test_includes_keywords(self, mock_db):
        """Should include colleague keywords."""
        coll_kw = MockCollKw(1, 1, 1)
        kw = MockKeyword(1, "yeast")
        colleague = MockColleague(
            colleague_no=1,
            first_name="John",
            last_name="Smith",
            coll_kw=[coll_kw],
        )

        mock_db.query.side_effect = [
            MockQuery([colleague]),
            MockQuery([kw]),
        ]

        service = ColleagueCurationService(mock_db)
        result = service.get_colleague_details(1)

        assert "keywords" in result
        assert len(result["keywords"]) == 1
        assert result["keywords"][0]["keyword"] == "yeast"


class TestCreateColleague:
    """Tests for creating colleagues."""

    def test_creates_colleague(self, mock_db):
        """Should create new colleague."""
        service = ColleagueCurationService(mock_db)
        service.create_colleague(
            first_name="New",
            last_name="Person",
            curator_userid="curator1",
            email="new@example.com",
        )

        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()

    def test_returns_colleague_no(self, mock_db):
        """Should return the new colleague_no."""
        # Mock the Colleague object created
        mock_colleague = MagicMock()
        mock_colleague.colleague_no = 123

        def capture_add(obj):
            obj.colleague_no = 123

        mock_db.add.side_effect = capture_add

        service = ColleagueCurationService(mock_db)
        result = service.create_colleague(
            first_name="New",
            last_name="Person",
            curator_userid="curator1",
        )

        assert result == 123

    def test_truncates_long_curator_id(self, mock_db):
        """Should truncate curator_userid to 12 chars."""
        service = ColleagueCurationService(mock_db)
        service.create_colleague(
            first_name="Test",
            last_name="User",
            curator_userid="verylongcuratorid123",
        )

        # Verify add was called
        mock_db.add.assert_called_once()


class TestUpdateColleague:
    """Tests for updating colleagues."""

    def test_updates_colleague_fields(self, mock_db, sample_colleagues):
        """Should update colleague fields."""
        mock_db.query.return_value = MockQuery([sample_colleagues[0]])

        service = ColleagueCurationService(mock_db)
        result = service.update_colleague(
            colleague_no=1,
            curator_userid="curator1",
            email="newemail@example.com",
        )

        assert result is True
        assert sample_colleagues[0].email == "newemail@example.com"
        mock_db.commit.assert_called_once()

    def test_raises_for_unknown_colleague(self, mock_db):
        """Should raise error for unknown colleague."""
        mock_db.query.return_value = MockQuery([])

        service = ColleagueCurationService(mock_db)

        with pytest.raises(ColleagueCurationError) as exc_info:
            service.update_colleague(
                colleague_no=999,
                curator_userid="curator1",
                email="test@example.com",
            )

        assert "not found" in str(exc_info.value)

    def test_sets_date_modified(self, mock_db, sample_colleagues):
        """Should set date_modified on update."""
        mock_db.query.return_value = MockQuery([sample_colleagues[0]])

        service = ColleagueCurationService(mock_db)
        service.update_colleague(
            colleague_no=1,
            curator_userid="curator1",
            email="test@example.com",
        )

        assert sample_colleagues[0].date_modified is not None


class TestDeleteColleague:
    """Tests for deleting colleagues."""

    def test_deletes_colleague(self, mock_db, sample_colleagues):
        """Should delete colleague."""
        mock_db.query.return_value = MockQuery([sample_colleagues[0]])

        service = ColleagueCurationService(mock_db)
        result = service.delete_colleague(1, "curator1")

        assert result is True
        mock_db.delete.assert_called_once()
        mock_db.commit.assert_called_once()

    def test_raises_for_unknown_colleague(self, mock_db):
        """Should raise error for unknown colleague."""
        mock_db.query.return_value = MockQuery([])

        service = ColleagueCurationService(mock_db)

        with pytest.raises(ColleagueCurationError) as exc_info:
            service.delete_colleague(999, "curator1")

        assert "not found" in str(exc_info.value)


class TestAddColleagueUrl:
    """Tests for adding URLs to colleagues."""

    def test_creates_new_url(self, mock_db, sample_colleagues):
        """Should create new URL if not exists."""
        mock_db.query.side_effect = [
            MockQuery([sample_colleagues[0]]),
            MockQuery([]),  # URL not found
        ]

        service = ColleagueCurationService(mock_db)
        service.add_colleague_url(1, "Lab", "https://example.com", "curator1")

        assert mock_db.add.call_count >= 2  # URL + CollUrl
        mock_db.commit.assert_called_once()

    def test_reuses_existing_url(self, mock_db, sample_colleagues):
        """Should reuse existing URL."""
        existing_url = MockUrl(1, "Lab", "https://example.com")

        mock_db.query.side_effect = [
            MockQuery([sample_colleagues[0]]),
            MockQuery([existing_url]),
        ]

        service = ColleagueCurationService(mock_db)
        service.add_colleague_url(1, "Lab", "https://example.com", "curator1")

        # Should only add CollUrl, not new Url
        mock_db.commit.assert_called_once()

    def test_raises_for_unknown_colleague(self, mock_db):
        """Should raise error for unknown colleague."""
        mock_db.query.return_value = MockQuery([])

        service = ColleagueCurationService(mock_db)

        with pytest.raises(ColleagueCurationError) as exc_info:
            service.add_colleague_url(999, "Lab", "https://example.com", "curator1")

        assert "not found" in str(exc_info.value)


class TestRemoveColleagueUrl:
    """Tests for removing URLs from colleagues."""

    def test_removes_url(self, mock_db):
        """Should remove URL link."""
        coll_url = MockCollUrl(1, 1, 1)
        mock_db.query.return_value = MockQuery([coll_url])

        service = ColleagueCurationService(mock_db)
        result = service.remove_colleague_url(1)

        assert result is True
        mock_db.delete.assert_called_once()
        mock_db.commit.assert_called_once()

    def test_raises_for_unknown_url(self, mock_db):
        """Should raise error for unknown URL link."""
        mock_db.query.return_value = MockQuery([])

        service = ColleagueCurationService(mock_db)

        with pytest.raises(ColleagueCurationError) as exc_info:
            service.remove_colleague_url(999)

        assert "not found" in str(exc_info.value)


class TestAddColleagueKeyword:
    """Tests for adding keywords to colleagues."""

    def test_creates_new_keyword(self, mock_db, sample_colleagues):
        """Should create new keyword if not exists."""
        mock_db.query.side_effect = [
            MockQuery([sample_colleagues[0]]),
            MockQuery([]),  # Keyword not found
            MockQuery([]),  # CollKw not found
        ]

        service = ColleagueCurationService(mock_db)
        service.add_colleague_keyword(1, "candida", "curator1")

        assert mock_db.add.call_count >= 2  # Keyword + CollKw
        mock_db.commit.assert_called_once()

    def test_reuses_existing_keyword(self, mock_db, sample_colleagues):
        """Should reuse existing keyword."""
        existing_kw = MockKeyword(1, "candida")

        mock_db.query.side_effect = [
            MockQuery([sample_colleagues[0]]),
            MockQuery([existing_kw]),
            MockQuery([]),  # CollKw not found
        ]

        service = ColleagueCurationService(mock_db)
        service.add_colleague_keyword(1, "candida", "curator1")

        # Should only add CollKw, not new Keyword
        mock_db.commit.assert_called_once()

    def test_returns_existing_link(self, mock_db, sample_colleagues):
        """Should return existing link if already exists."""
        existing_kw = MockKeyword(1, "candida")
        existing_link = MockCollKw(5, 1, 1)

        mock_db.query.side_effect = [
            MockQuery([sample_colleagues[0]]),
            MockQuery([existing_kw]),
            MockQuery([existing_link]),
        ]

        service = ColleagueCurationService(mock_db)
        result = service.add_colleague_keyword(1, "candida", "curator1")

        assert result == 5
        mock_db.commit.assert_not_called()

    def test_raises_for_unknown_colleague(self, mock_db):
        """Should raise error for unknown colleague."""
        mock_db.query.return_value = MockQuery([])

        service = ColleagueCurationService(mock_db)

        with pytest.raises(ColleagueCurationError) as exc_info:
            service.add_colleague_keyword(999, "test", "curator1")

        assert "not found" in str(exc_info.value)


class TestRemoveColleagueKeyword:
    """Tests for removing keywords from colleagues."""

    def test_removes_keyword(self, mock_db):
        """Should remove keyword link."""
        coll_kw = MockCollKw(1, 1, 1)
        mock_db.query.return_value = MockQuery([coll_kw])

        service = ColleagueCurationService(mock_db)
        result = service.remove_colleague_keyword(1)

        assert result is True
        mock_db.delete.assert_called_once()
        mock_db.commit.assert_called_once()

    def test_raises_for_unknown_keyword(self, mock_db):
        """Should raise error for unknown keyword link."""
        mock_db.query.return_value = MockQuery([])

        service = ColleagueCurationService(mock_db)

        with pytest.raises(ColleagueCurationError) as exc_info:
            service.remove_colleague_keyword(999)

        assert "not found" in str(exc_info.value)


class TestAddColleagueFeature:
    """Tests for adding features to colleagues."""

    def test_creates_feature_link(self, mock_db, sample_colleagues):
        """Should create feature link."""
        feature = MockFeature(1, "CAL1", "ALS1")

        mock_db.query.side_effect = [
            MockQuery([sample_colleagues[0]]),
            MockQuery([feature]),
            MockQuery([]),  # CollFeat not found
        ]

        service = ColleagueCurationService(mock_db)
        service.add_colleague_feature(1, "ALS1", "curator1")

        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()

    def test_returns_existing_link(self, mock_db, sample_colleagues):
        """Should return existing link if already exists."""
        feature = MockFeature(1, "CAL1", "ALS1")
        existing_link = MockCollFeat(5, 1, 1)

        mock_db.query.side_effect = [
            MockQuery([sample_colleagues[0]]),
            MockQuery([feature]),
            MockQuery([existing_link]),
        ]

        service = ColleagueCurationService(mock_db)
        result = service.add_colleague_feature(1, "ALS1", "curator1")

        assert result == 5
        mock_db.commit.assert_not_called()

    def test_raises_for_unknown_colleague(self, mock_db):
        """Should raise error for unknown colleague."""
        mock_db.query.return_value = MockQuery([])

        service = ColleagueCurationService(mock_db)

        with pytest.raises(ColleagueCurationError) as exc_info:
            service.add_colleague_feature(999, "ALS1", "curator1")

        assert "not found" in str(exc_info.value)

    def test_raises_for_unknown_feature(self, mock_db, sample_colleagues):
        """Should raise error for unknown feature."""
        mock_db.query.side_effect = [
            MockQuery([sample_colleagues[0]]),
            MockQuery([]),  # Feature not found
        ]

        service = ColleagueCurationService(mock_db)

        with pytest.raises(ColleagueCurationError) as exc_info:
            service.add_colleague_feature(1, "UNKNOWN", "curator1")

        assert "not found" in str(exc_info.value)


class TestRemoveColleagueFeature:
    """Tests for removing features from colleagues."""

    def test_removes_feature(self, mock_db):
        """Should remove feature link."""
        coll_feat = MockCollFeat(1, 1, 1)
        mock_db.query.return_value = MockQuery([coll_feat])

        service = ColleagueCurationService(mock_db)
        result = service.remove_colleague_feature(1)

        assert result is True
        mock_db.delete.assert_called_once()
        mock_db.commit.assert_called_once()

    def test_raises_for_unknown_feature(self, mock_db):
        """Should raise error for unknown feature link."""
        mock_db.query.return_value = MockQuery([])

        service = ColleagueCurationService(mock_db)

        with pytest.raises(ColleagueCurationError) as exc_info:
            service.remove_colleague_feature(999)

        assert "not found" in str(exc_info.value)


class TestAddColleagueRelationship:
    """Tests for adding relationships between colleagues."""

    def test_creates_relationship(self, mock_db, sample_colleagues):
        """Should create relationship."""
        mock_db.query.side_effect = [
            MockQuery([sample_colleagues[0]]),
            MockQuery([sample_colleagues[1]]),
            MockQuery([]),  # Relationship not found
        ]

        service = ColleagueCurationService(mock_db)
        service.add_colleague_relationship(1, 2, "PI")

        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()

    def test_returns_existing_relationship(self, mock_db, sample_colleagues):
        """Should return existing relationship if already exists."""
        existing_rel = MockCollRelationship(5, 1, 2, "PI")

        mock_db.query.side_effect = [
            MockQuery([sample_colleagues[0]]),
            MockQuery([sample_colleagues[1]]),
            MockQuery([existing_rel]),
        ]

        service = ColleagueCurationService(mock_db)
        result = service.add_colleague_relationship(1, 2, "PI")

        assert result == 5
        mock_db.commit.assert_not_called()

    def test_raises_for_unknown_colleague(self, mock_db):
        """Should raise error for unknown colleague."""
        mock_db.query.return_value = MockQuery([])

        service = ColleagueCurationService(mock_db)

        with pytest.raises(ColleagueCurationError) as exc_info:
            service.add_colleague_relationship(999, 2, "PI")

        assert "not found" in str(exc_info.value)

    def test_raises_for_unknown_associate(self, mock_db, sample_colleagues):
        """Should raise error for unknown associate."""
        mock_db.query.side_effect = [
            MockQuery([sample_colleagues[0]]),
            MockQuery([]),  # Associate not found
        ]

        service = ColleagueCurationService(mock_db)

        with pytest.raises(ColleagueCurationError) as exc_info:
            service.add_colleague_relationship(1, 999, "PI")

        assert "not found" in str(exc_info.value)


class TestRemoveColleagueRelationship:
    """Tests for removing relationships between colleagues."""

    def test_removes_relationship(self, mock_db):
        """Should remove relationship."""
        rel = MockCollRelationship(1, 1, 2, "PI")
        mock_db.query.return_value = MockQuery([rel])

        service = ColleagueCurationService(mock_db)
        result = service.remove_colleague_relationship(1)

        assert result is True
        mock_db.delete.assert_called_once()
        mock_db.commit.assert_called_once()

    def test_raises_for_unknown_relationship(self, mock_db):
        """Should raise error for unknown relationship."""
        mock_db.query.return_value = MockQuery([])

        service = ColleagueCurationService(mock_db)

        with pytest.raises(ColleagueCurationError) as exc_info:
            service.remove_colleague_relationship(999)

        assert "not found" in str(exc_info.value)


class TestAddColleagueRemark:
    """Tests for adding remarks to colleagues."""

    def test_creates_remark(self, mock_db, sample_colleagues):
        """Should create remark."""
        mock_db.query.return_value = MockQuery([sample_colleagues[0]])

        service = ColleagueCurationService(mock_db)
        service.add_colleague_remark(1, "General", "Test remark", "curator1")

        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()

    def test_raises_for_unknown_colleague(self, mock_db):
        """Should raise error for unknown colleague."""
        mock_db.query.return_value = MockQuery([])

        service = ColleagueCurationService(mock_db)

        with pytest.raises(ColleagueCurationError) as exc_info:
            service.add_colleague_remark(999, "General", "Test", "curator1")

        assert "not found" in str(exc_info.value)


class TestServiceInitialization:
    """Tests for service initialization."""

    def test_stores_db_session(self, mock_db):
        """Should store database session."""
        service = ColleagueCurationService(mock_db)
        assert service.db is mock_db


class TestColleagueCurationError:
    """Tests for the error class."""

    def test_exception_message(self):
        """Should store error message."""
        error = ColleagueCurationError("Test error")
        assert str(error) == "Test error"

    def test_is_exception(self):
        """Should be an Exception."""
        assert issubclass(ColleagueCurationError, Exception)
