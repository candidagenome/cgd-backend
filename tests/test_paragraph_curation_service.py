"""
Tests for Paragraph Curation Service.

Tests cover:
- Feature lookup by name
- Getting paragraphs for a feature
- Paragraph CRUD operations
- Feature-paragraph linking
- Reference link extraction from markup
- Paragraph reordering
"""
import pytest
from unittest.mock import MagicMock
from datetime import datetime

from cgd.api.services.curation.paragraph_curation_service import (
    ParagraphCurationService,
    ParagraphCurationError,
)


class MockFeature:
    """Mock Feature model."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        gene_name: str = None,
        organism_no: int = 1,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name
        self.organism_no = organism_no


class MockOrganism:
    """Mock Organism model."""

    def __init__(self, organism_no: int, organism_abbrev: str, organism_name: str):
        self.organism_no = organism_no
        self.organism_abbrev = organism_abbrev
        self.organism_name = organism_name


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
        self.date_edited = date_edited or datetime.now()


class MockFeatPara:
    """Mock FeatPara model."""

    def __init__(
        self,
        feature_no: int,
        paragraph_no: int,
        paragraph_order: int,
    ):
        self.feature_no = feature_no
        self.paragraph_no = paragraph_no
        self.paragraph_order = paragraph_order


class MockRefLink:
    """Mock RefLink model."""

    def __init__(
        self,
        ref_link_no: int,
        reference_no: int,
        tab_name: str,
        col_name: str,
        primary_key: int,
    ):
        self.ref_link_no = ref_link_no
        self.reference_no = reference_no
        self.tab_name = tab_name
        self.col_name = col_name
        self.primary_key = primary_key


class MockReference:
    """Mock Reference model."""

    def __init__(
        self,
        reference_no: int,
        dbxref_id: str,
        citation: str = "Test Citation",
    ):
        self.reference_no = reference_no
        self.dbxref_id = dbxref_id
        self.citation = citation


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []

    def join(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args):
        return self

    def first(self):
        return self._results[0] if self._results else None

    def all(self):
        return self._results

    def scalar(self):
        return self._results[0] if self._results else None


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    db = MagicMock()
    db.query.return_value = MockQuery([])
    return db


@pytest.fixture
def sample_features():
    """Create sample features."""
    return [
        MockFeature(1, "CAL0001", "ALS1", 1),
        MockFeature(2, "CAL0002", None, 1),
    ]


@pytest.fixture
def sample_organisms():
    """Create sample organisms."""
    return [
        MockOrganism(1, "C_albicans_SC5314", "Candida albicans"),
        MockOrganism(2, "C_glabrata_CBS138", "Candida glabrata"),
    ]


@pytest.fixture
def sample_paragraphs():
    """Create sample paragraphs."""
    return [
        MockParagraph(1, "This is paragraph 1 about ALS1."),
        MockParagraph(2, "This is paragraph 2 with <reference:S000123456>."),
    ]


class TestConstants:
    """Tests for service constants."""

    def test_max_paragraph_length(self):
        """Should define maximum paragraph length."""
        assert ParagraphCurationService.MAX_PARAGRAPH_LENGTH == 4000


class TestGetFeatureByName:
    """Tests for getting feature by name."""

    def test_returns_feature_by_feature_name(self, mock_db, sample_features):
        """Should return feature when found by feature_name."""
        mock_db.query.return_value = MockQuery([sample_features[0]])

        service = ParagraphCurationService(mock_db)
        result = service.get_feature_by_name("CAL0001")

        assert result is not None
        assert result.feature_no == 1

    def test_returns_feature_by_gene_name(self, mock_db, sample_features):
        """Should return feature when found by gene_name."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Not found by feature_name
            MockQuery([sample_features[0]]),  # Found by gene_name
        ]

        service = ParagraphCurationService(mock_db)
        result = service.get_feature_by_name("ALS1")

        assert result is not None
        assert result.gene_name == "ALS1"

    def test_returns_none_when_not_found(self, mock_db):
        """Should return None when feature not found."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Not found by feature_name
            MockQuery([]),  # Not found by gene_name
        ]

        service = ParagraphCurationService(mock_db)
        result = service.get_feature_by_name("UNKNOWN")

        assert result is None

    def test_filters_by_organism(self, mock_db, sample_features):
        """Should filter by organism when provided."""
        mock_db.query.return_value = MockQuery([sample_features[0]])

        service = ParagraphCurationService(mock_db)
        result = service.get_feature_by_name("CAL0001", "C_albicans_SC5314")

        assert result is not None


class TestGetParagraphsForFeature:
    """Tests for getting paragraphs for a feature."""

    def test_raises_for_unknown_feature(self, mock_db):
        """Should raise error for unknown feature."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Feature not found
            MockQuery([]),  # Gene name search
        ]

        service = ParagraphCurationService(mock_db)

        with pytest.raises(ParagraphCurationError) as exc_info:
            service.get_paragraphs_for_feature("UNKNOWN")

        assert "not found" in str(exc_info.value)

    def test_returns_empty_for_feature_without_paragraphs(
        self, mock_db, sample_features, sample_organisms
    ):
        """Should return empty list when no paragraphs."""
        mock_db.query.side_effect = [
            MockQuery([sample_features[0]]),  # Feature found
            MockQuery([]),  # No paragraphs
            MockQuery([sample_organisms[0]]),  # Organism lookup
        ]

        service = ParagraphCurationService(mock_db)
        result = service.get_paragraphs_for_feature("CAL0001")

        assert result["feature_no"] == 1
        assert result["paragraphs"] == []

    def test_returns_paragraphs_with_order(
        self, mock_db, sample_features, sample_paragraphs, sample_organisms
    ):
        """Should return paragraphs with their order."""
        feat_paras = [
            (MockFeatPara(1, 1, 1), sample_paragraphs[0]),
            (MockFeatPara(1, 2, 2), sample_paragraphs[1]),
        ]

        mock_db.query.side_effect = [
            MockQuery([sample_features[0]]),  # Feature found
            MockQuery(feat_paras),  # FeatPara + Paragraph
            MockQuery([]),  # Linked features for para 1
            MockQuery([]),  # Linked features for para 2
            MockQuery([sample_organisms[0]]),  # Organism lookup
        ]

        service = ParagraphCurationService(mock_db)
        result = service.get_paragraphs_for_feature("CAL0001")

        assert len(result["paragraphs"]) == 2
        assert result["paragraphs"][0]["paragraph_order"] == 1
        assert result["paragraphs"][1]["paragraph_order"] == 2


class TestGetParagraphDetails:
    """Tests for getting paragraph details."""

    def test_raises_for_unknown_paragraph(self, mock_db):
        """Should raise error for unknown paragraph."""
        mock_db.query.return_value = MockQuery([])

        service = ParagraphCurationService(mock_db)

        with pytest.raises(ParagraphCurationError) as exc_info:
            service.get_paragraph_details(999)

        assert "not found" in str(exc_info.value)

    def test_returns_paragraph_with_linked_refs(self, mock_db, sample_paragraphs):
        """Should return paragraph with linked references."""
        ref = MockReference(1, "S000123456", "Smith et al. (2024)")
        ref_link = MockRefLink(1, 1, "PARAGRAPH", "PARAGRAPH_NO", 1)

        mock_db.query.side_effect = [
            MockQuery([sample_paragraphs[0]]),  # Paragraph
            MockQuery([]),  # Linked features
            MockQuery([ref_link]),  # RefLink entries
            MockQuery([ref]),  # Reference lookup
        ]

        service = ParagraphCurationService(mock_db)
        result = service.get_paragraph_details(1)

        assert result["paragraph_no"] == 1
        assert len(result["linked_references"]) == 1


class TestCreateParagraph:
    """Tests for creating paragraphs."""

    def test_raises_for_empty_text(self, mock_db):
        """Should raise error for empty paragraph text."""
        service = ParagraphCurationService(mock_db)

        with pytest.raises(ParagraphCurationError) as exc_info:
            service.create_paragraph("", ["CAL0001"], "C_albicans_SC5314", "curator1")

        assert "required" in str(exc_info.value)

    def test_raises_for_text_too_long(self, mock_db):
        """Should raise error for text exceeding max length."""
        service = ParagraphCurationService(mock_db)
        long_text = "x" * 5000

        with pytest.raises(ParagraphCurationError) as exc_info:
            service.create_paragraph(
                long_text, ["CAL0001"], "C_albicans_SC5314", "curator1"
            )

        assert "limit" in str(exc_info.value)

    def test_raises_for_no_features(self, mock_db):
        """Should raise error when no features provided."""
        service = ParagraphCurationService(mock_db)

        with pytest.raises(ParagraphCurationError) as exc_info:
            service.create_paragraph(
                "Test paragraph", [], "C_albicans_SC5314", "curator1"
            )

        assert "At least one feature" in str(exc_info.value)

    def test_creates_new_paragraph(self, mock_db, sample_features):
        """Should create new paragraph and link to feature."""
        mock_db.query.side_effect = [
            MockQuery([]),  # No existing paragraph
            MockQuery([]),  # No ref links to update (current)
            MockQuery([sample_features[0]]),  # Feature lookup
            MockQuery([]),  # Not already linked
            MockQuery([None]),  # Max order (None = first)
        ]

        service = ParagraphCurationService(mock_db)
        result = service.create_paragraph(
            "Test paragraph", ["CAL0001"], "C_albicans_SC5314", "curator1"
        )

        assert result["linked_features"][0]["feature_name"] == "CAL0001"
        mock_db.add.assert_called()
        mock_db.commit.assert_called_once()

    def test_reuses_existing_paragraph(self, mock_db, sample_features, sample_paragraphs):
        """Should reuse existing paragraph with same text."""
        mock_db.query.side_effect = [
            MockQuery([sample_paragraphs[0]]),  # Existing paragraph found
            MockQuery([]),  # Current ref links
            MockQuery([sample_features[0]]),  # Feature lookup
            MockQuery([]),  # Not already linked
            MockQuery([None]),  # Max order
        ]

        service = ParagraphCurationService(mock_db)
        result = service.create_paragraph(
            sample_paragraphs[0].paragraph_text,
            ["CAL0001"],
            "C_albicans_SC5314",
            "curator1",
        )

        assert result["paragraph_no"] == 1

    def test_raises_for_unknown_feature(self, mock_db):
        """Should raise error for unknown feature."""
        mock_db.query.side_effect = [
            MockQuery([]),  # No existing paragraph
            MockQuery([]),  # No ref links
            MockQuery([]),  # Feature not found by name
            MockQuery([]),  # Feature not found by gene
        ]

        service = ParagraphCurationService(mock_db)

        with pytest.raises(ParagraphCurationError) as exc_info:
            service.create_paragraph(
                "Test paragraph", ["UNKNOWN"], "C_albicans_SC5314", "curator1"
            )

        assert "not found" in str(exc_info.value)


class TestUpdateParagraph:
    """Tests for updating paragraphs."""

    def test_raises_for_unknown_paragraph(self, mock_db):
        """Should raise error for unknown paragraph."""
        mock_db.query.return_value = MockQuery([])

        service = ParagraphCurationService(mock_db)

        with pytest.raises(ParagraphCurationError) as exc_info:
            service.update_paragraph(999, "New text", True, "curator1")

        assert "not found" in str(exc_info.value)

    def test_raises_for_text_too_long(self, mock_db, sample_paragraphs):
        """Should raise error for text exceeding max length."""
        mock_db.query.return_value = MockQuery([sample_paragraphs[0]])

        service = ParagraphCurationService(mock_db)
        long_text = "x" * 5000

        with pytest.raises(ParagraphCurationError) as exc_info:
            service.update_paragraph(1, long_text, True, "curator1")

        assert "limit" in str(exc_info.value)

    def test_updates_paragraph_text(self, mock_db, sample_paragraphs):
        """Should update paragraph text."""
        mock_db.query.side_effect = [
            MockQuery([sample_paragraphs[0]]),  # Paragraph found
            MockQuery([]),  # Current ref links
        ]

        service = ParagraphCurationService(mock_db)
        result = service.update_paragraph(1, "Updated text", True, "curator1")

        assert result is True
        assert sample_paragraphs[0].paragraph_text == "Updated text"
        mock_db.commit.assert_called_once()

    def test_updates_date_when_requested(self, mock_db, sample_paragraphs):
        """Should update date_edited when requested."""
        original_date = sample_paragraphs[0].date_edited
        mock_db.query.return_value = MockQuery([sample_paragraphs[0]])

        service = ParagraphCurationService(mock_db)
        service.update_paragraph(1, sample_paragraphs[0].paragraph_text, True, "curator1")

        assert sample_paragraphs[0].date_edited >= original_date


class TestReorderParagraphs:
    """Tests for reordering paragraphs."""

    def test_raises_for_invalid_orders(self, mock_db):
        """Should raise error for non-sequential orders."""
        service = ParagraphCurationService(mock_db)

        with pytest.raises(ParagraphCurationError) as exc_info:
            service.reorder_paragraphs(
                1, [{"paragraph_no": 1, "order": 1}, {"paragraph_no": 2, "order": 3}], "curator1"
            )

        assert "sequential" in str(exc_info.value)

    def test_raises_for_orders_not_starting_at_one(self, mock_db):
        """Should raise error for orders not starting at 1."""
        service = ParagraphCurationService(mock_db)

        with pytest.raises(ParagraphCurationError) as exc_info:
            service.reorder_paragraphs(
                1, [{"paragraph_no": 1, "order": 2}, {"paragraph_no": 2, "order": 3}], "curator1"
            )

        assert "sequential starting at 1" in str(exc_info.value)

    def test_reorders_paragraphs(self, mock_db):
        """Should reorder paragraphs."""
        feat_para_1 = MockFeatPara(1, 1, 1)
        feat_para_2 = MockFeatPara(1, 2, 2)

        mock_db.query.side_effect = [
            MockQuery([feat_para_1]),  # First paragraph
            MockQuery([feat_para_2]),  # Second paragraph
        ]

        service = ParagraphCurationService(mock_db)
        result = service.reorder_paragraphs(
            1,
            [{"paragraph_no": 1, "order": 2}, {"paragraph_no": 2, "order": 1}],
            "curator1",
        )

        assert result is True
        assert feat_para_1.paragraph_order == 2
        assert feat_para_2.paragraph_order == 1

    def test_raises_for_unlinked_paragraph(self, mock_db):
        """Should raise error for paragraph not linked to feature."""
        mock_db.query.return_value = MockQuery([])

        service = ParagraphCurationService(mock_db)

        with pytest.raises(ParagraphCurationError) as exc_info:
            service.reorder_paragraphs(
                1, [{"paragraph_no": 999, "order": 1}], "curator1"
            )

        assert "not linked" in str(exc_info.value)


class TestLinkFeature:
    """Tests for linking paragraphs to features."""

    def test_raises_for_unknown_paragraph(self, mock_db):
        """Should raise error for unknown paragraph."""
        mock_db.query.return_value = MockQuery([])

        service = ParagraphCurationService(mock_db)

        with pytest.raises(ParagraphCurationError) as exc_info:
            service.link_feature(999, "CAL0001", "C_albicans_SC5314", "curator1")

        assert "Paragraph" in str(exc_info.value) and "not found" in str(exc_info.value)

    def test_raises_for_unknown_feature(self, mock_db, sample_paragraphs):
        """Should raise error for unknown feature."""
        mock_db.query.side_effect = [
            MockQuery([sample_paragraphs[0]]),  # Paragraph found
            MockQuery([]),  # Feature not found by name
            MockQuery([]),  # Feature not found by gene
        ]

        service = ParagraphCurationService(mock_db)

        with pytest.raises(ParagraphCurationError) as exc_info:
            service.link_feature(1, "UNKNOWN", "C_albicans_SC5314", "curator1")

        assert "Feature" in str(exc_info.value) and "not found" in str(exc_info.value)

    def test_raises_for_existing_link(self, mock_db, sample_paragraphs, sample_features):
        """Should raise error if already linked."""
        feat_para = MockFeatPara(1, 1, 1)

        mock_db.query.side_effect = [
            MockQuery([sample_paragraphs[0]]),  # Paragraph found
            MockQuery([sample_features[0]]),  # Feature found
            MockQuery([feat_para]),  # Already linked
        ]

        service = ParagraphCurationService(mock_db)

        with pytest.raises(ParagraphCurationError) as exc_info:
            service.link_feature(1, "CAL0001", "C_albicans_SC5314", "curator1")

        assert "already linked" in str(exc_info.value)

    def test_links_paragraph_to_feature(self, mock_db, sample_paragraphs, sample_features):
        """Should link paragraph to feature."""
        mock_db.query.side_effect = [
            MockQuery([sample_paragraphs[0]]),  # Paragraph found
            MockQuery([sample_features[0]]),  # Feature found
            MockQuery([]),  # Not already linked
            MockQuery([None]),  # Max order
        ]

        service = ParagraphCurationService(mock_db)
        result = service.link_feature(1, "CAL0001", "C_albicans_SC5314", "curator1")

        assert result["feature_no"] == 1
        assert result["feature_name"] == "CAL0001"
        mock_db.add.assert_called_once()


class TestUnlinkFeature:
    """Tests for unlinking paragraphs from features."""

    def test_raises_for_not_linked(self, mock_db):
        """Should raise error if not linked."""
        mock_db.query.return_value = MockQuery([])

        service = ParagraphCurationService(mock_db)

        with pytest.raises(ParagraphCurationError) as exc_info:
            service.unlink_feature(1, 1, "curator1")

        assert "not linked" in str(exc_info.value)

    def test_unlinks_paragraph_from_feature(self, mock_db):
        """Should unlink paragraph from feature."""
        feat_para = MockFeatPara(1, 1, 1)
        mock_db.query.return_value = MockQuery([feat_para])

        service = ParagraphCurationService(mock_db)
        result = service.unlink_feature(1, 1, "curator1")

        assert result is True
        mock_db.delete.assert_called_once()
        mock_db.commit.assert_called_once()


class TestGetOrganisms:
    """Tests for getting organisms."""

    def test_returns_organisms(self, mock_db, sample_organisms):
        """Should return list of organisms."""
        mock_db.query.return_value = MockQuery(sample_organisms)

        service = ParagraphCurationService(mock_db)
        result = service.get_organisms()

        assert len(result) == 2
        assert result[0]["organism_abbrev"] == "C_albicans_SC5314"


class TestUpdateReferenceLinks:
    """Tests for updating reference links from markup."""

    def test_extracts_references_from_markup(self, mock_db, sample_paragraphs):
        """Should extract references from markup and add new link."""
        ref = MockReference(1, "S000123456")

        mock_db.query.side_effect = [
            MockQuery([]),  # Current links (none)
            MockQuery([ref]),  # Reference for S000123456
        ]

        service = ParagraphCurationService(mock_db)
        service._update_reference_links(2, "<reference:S000123456> Some text")

        # Should have added a new link for the reference
        mock_db.add.assert_called()

    def test_removes_old_links_not_in_markup(self, mock_db, sample_paragraphs):
        """Should remove links not present in new markup."""
        ref_link = MockRefLink(1, 1, "PARAGRAPH", "PARAGRAPH_NO", 2)
        ref_link.reference_no = 1

        mock_db.query.side_effect = [
            MockQuery([ref_link]),  # Current links
            MockQuery([ref_link]),  # Link to delete (lookup by ref_no)
        ]

        service = ParagraphCurationService(mock_db)
        service._update_reference_links(2, "Text without references")

        # Should have deleted the old link
        mock_db.delete.assert_called()


class TestGetNextParagraphOrder:
    """Tests for getting next paragraph order."""

    def test_returns_one_for_no_paragraphs(self, mock_db):
        """Should return 1 when no existing paragraphs."""
        mock_db.query.return_value = MockQuery([None])

        service = ParagraphCurationService(mock_db)
        result = service._get_next_paragraph_order(1)

        assert result == 1

    def test_returns_max_plus_one(self, mock_db):
        """Should return max order + 1."""
        mock_db.query.return_value = MockQuery([5])

        service = ParagraphCurationService(mock_db)
        result = service._get_next_paragraph_order(1)

        assert result == 6


class TestServiceInitialization:
    """Tests for service initialization."""

    def test_stores_db_session(self, mock_db):
        """Should store database session."""
        service = ParagraphCurationService(mock_db)
        assert service.db is mock_db


class TestParagraphCurationError:
    """Tests for the error class."""

    def test_exception_message(self):
        """Should store error message."""
        error = ParagraphCurationError("Test error")
        assert str(error) == "Test error"

    def test_is_exception(self):
        """Should be an Exception."""
        assert issubclass(ParagraphCurationError, Exception)
