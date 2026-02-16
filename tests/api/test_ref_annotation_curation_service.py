"""
Tests for Reference Annotation Curation Service.

Tests cover:
- Reference lookup
- Getting reference annotations (lit guide, GO, ref_link)
- Literature guide entry management (delete, transfer)
- GO annotation management (delete, transfer)
- REF_LINK management (delete, transfer)
- Bulk operations (delete, transfer)
"""
import pytest
from unittest.mock import MagicMock
from datetime import datetime

from cgd.api.services.curation.ref_annotation_curation_service import (
    RefAnnotationCurationService,
    RefAnnotationCurationError,
)


class MockReference:
    """Mock Reference model."""

    def __init__(
        self,
        reference_no: int,
        pubmed: int = None,
        dbxref_id: str = None,
        citation: str = "Test Citation",
        title: str = "Test Title",
    ):
        self.reference_no = reference_no
        self.pubmed = pubmed
        self.dbxref_id = dbxref_id
        self.citation = citation
        self.title = title


class MockFeature:
    """Mock Feature model."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        gene_name: str = None,
        feature_type: str = "ORF",
        headline: str = None,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name
        self.feature_type = feature_type
        self.headline = headline


class MockRefProperty:
    """Mock RefProperty model."""

    def __init__(
        self,
        ref_property_no: int,
        reference_no: int,
        property_type: str,
        property_value: str,
        source: str = "CGD",
        created_by: str = "curator1",
        date_created: datetime = None,
        date_last_reviewed: datetime = None,
    ):
        self.ref_property_no = ref_property_no
        self.reference_no = reference_no
        self.property_type = property_type
        self.property_value = property_value
        self.source = source
        self.created_by = created_by
        self.date_created = date_created or datetime.now()
        self.date_last_reviewed = date_last_reviewed


class MockRefpropFeat:
    """Mock RefpropFeat model."""

    def __init__(
        self,
        refprop_feat_no: int,
        ref_property_no: int,
        feature_no: int,
        created_by: str = "curator1",
        date_created: datetime = None,
    ):
        self.refprop_feat_no = refprop_feat_no
        self.ref_property_no = ref_property_no
        self.feature_no = feature_no
        self.created_by = created_by
        self.date_created = date_created or datetime.now()


class MockGoRef:
    """Mock GoRef model."""

    def __init__(
        self,
        go_ref_no: int,
        reference_no: int,
        go_annotation_no: int,
        has_qualifier: str = "N",
        has_supporting_evidence: str = "N",
        created_by: str = "curator1",
        date_created: datetime = None,
    ):
        self.go_ref_no = go_ref_no
        self.reference_no = reference_no
        self.go_annotation_no = go_annotation_no
        self.has_qualifier = has_qualifier
        self.has_supporting_evidence = has_supporting_evidence
        self.created_by = created_by
        self.date_created = date_created or datetime.now()


class MockGoAnnotation:
    """Mock GoAnnotation model."""

    def __init__(
        self,
        go_annotation_no: int,
        feature_no: int,
        go_no: int,
        go_evidence: str = "IDA",
    ):
        self.go_annotation_no = go_annotation_no
        self.feature_no = feature_no
        self.go_no = go_no
        self.go_evidence = go_evidence


class MockGo:
    """Mock Go model."""

    def __init__(
        self,
        go_no: int,
        goid: str,
        go_term: str,
        go_aspect: str = "P",
    ):
        self.go_no = go_no
        self.goid = goid
        self.go_term = go_term
        self.go_aspect = go_aspect


class MockGoQualifier:
    """Mock GoQualifier model."""

    def __init__(self, go_qualifier_no: int, go_ref_no: int, qualifier: str):
        self.go_qualifier_no = go_qualifier_no
        self.go_ref_no = go_ref_no
        self.qualifier = qualifier


class MockGorefDbxref:
    """Mock GorefDbxref model."""

    def __init__(
        self,
        goref_dbxref_no: int,
        go_ref_no: int,
        dbxref_no: int,
        support_type: str,
    ):
        self.goref_dbxref_no = goref_dbxref_no
        self.go_ref_no = go_ref_no
        self.dbxref_no = dbxref_no
        self.support_type = support_type


class MockRefLink:
    """Mock RefLink model."""

    def __init__(
        self,
        ref_link_no: int,
        reference_no: int,
        tab_name: str,
        col_name: str,
        primary_key: int,
        created_by: str = "curator1",
        date_created: datetime = None,
    ):
        self.ref_link_no = ref_link_no
        self.reference_no = reference_no
        self.tab_name = tab_name
        self.col_name = col_name
        self.primary_key = primary_key
        self.created_by = created_by
        self.date_created = date_created or datetime.now()


class MockPhenoAnnotation:
    """Mock PhenoAnnotation model."""

    def __init__(self, pheno_annotation_no: int, feature_no: int, phenotype_no: int):
        self.pheno_annotation_no = pheno_annotation_no
        self.feature_no = feature_no
        self.phenotype_no = phenotype_no


class MockPhenotype:
    """Mock Phenotype model."""

    def __init__(self, phenotype_no: int, observable: str, qualifier: str = None):
        self.phenotype_no = phenotype_no
        self.observable = observable
        self.qualifier = qualifier


class MockAlias:
    """Mock Alias model."""

    def __init__(self, alias_no: int, alias_name: str):
        self.alias_no = alias_no
        self.alias_name = alias_name


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
        return self._results[0] if self._results else 0

    def delete(self):
        return len(self._results)


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    db = MagicMock()
    db.query.return_value = MockQuery([])
    return db


@pytest.fixture
def sample_references():
    """Create sample references."""
    return [
        MockReference(1, 12345678, "S000123456", "Smith et al. (2024)", "Test Paper 1"),
        MockReference(2, 87654321, "S000654321", "Doe et al. (2023)", "Test Paper 2"),
    ]


@pytest.fixture
def sample_features():
    """Create sample features."""
    return [
        MockFeature(1, "CAL0001", "ALS1", "ORF", "Cell adhesion"),
        MockFeature(2, "CAL0002", None, "ORF", None),
    ]


class TestGetReferenceByNo:
    """Tests for getting reference by number."""

    def test_returns_reference_when_found(self, mock_db, sample_references):
        """Should return reference when found."""
        mock_db.query.return_value = MockQuery([sample_references[0]])

        service = RefAnnotationCurationService(mock_db)
        result = service.get_reference_by_no(1)

        assert result is not None
        assert result.reference_no == 1

    def test_returns_none_when_not_found(self, mock_db):
        """Should return None when not found."""
        mock_db.query.return_value = MockQuery([])

        service = RefAnnotationCurationService(mock_db)
        result = service.get_reference_by_no(999)

        assert result is None


class TestGetReferenceAnnotations:
    """Tests for getting reference annotations."""

    def test_raises_for_unknown_reference(self, mock_db):
        """Should raise error for unknown reference."""
        mock_db.query.return_value = MockQuery([])

        service = RefAnnotationCurationService(mock_db)

        with pytest.raises(RefAnnotationCurationError) as exc_info:
            service.get_reference_annotations(999)

        assert "not found" in str(exc_info.value)

    def test_returns_empty_annotations(self, mock_db, sample_references):
        """Should return empty lists when no annotations."""
        mock_db.query.side_effect = [
            MockQuery([sample_references[0]]),  # Reference found
            MockQuery([]),  # Lit guide
            MockQuery([]),  # GO annotations
            MockQuery([]),  # Ref links
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service.get_reference_annotations(1)

        assert result["reference"]["reference_no"] == 1
        assert result["lit_guide"] == []
        assert result["go_annotations"] == []


class TestGetLiteratureGuideEntries:
    """Tests for getting literature guide entries."""

    def test_returns_feature_entries(self, mock_db, sample_features):
        """Should return entries with linked features."""
        ref_prop = MockRefProperty(1, 1, "Topic", "Phenotype")
        refprop_feat = MockRefpropFeat(1, 1, 1)

        mock_db.query.side_effect = [
            MockQuery([ref_prop]),  # RefProperty
            MockQuery([(refprop_feat, sample_features[0])]),  # Feature links
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service._get_literature_guide_entries(1)

        assert len(result) == 1
        assert result[0]["type"] == "feature"
        assert result[0]["feature_name"] == "CAL0001"

    def test_returns_non_gene_entries(self, mock_db):
        """Should return entries without features."""
        ref_prop = MockRefProperty(1, 1, "Topic", "Methods")

        mock_db.query.side_effect = [
            MockQuery([ref_prop]),  # RefProperty
            MockQuery([]),  # No feature links
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service._get_literature_guide_entries(1)

        assert len(result) == 1
        assert result[0]["type"] == "non_gene"
        assert result[0]["feature_no"] is None


class TestGetGoAnnotationEntries:
    """Tests for getting GO annotation entries."""

    def test_returns_go_entries(self, mock_db, sample_features):
        """Should return GO annotation entries."""
        go = MockGo(1, "GO:0001234", "test process", "P")
        go_ann = MockGoAnnotation(1, 1, 1, "IDA")
        go_ref = MockGoRef(1, 1, 1)

        mock_db.query.return_value = MockQuery([
            (go_ref, go_ann, sample_features[0], go)
        ])

        service = RefAnnotationCurationService(mock_db)
        result = service._get_go_annotation_entries(1)

        assert len(result) == 1
        assert result[0]["goid"] == "GO:0001234"
        assert result[0]["feature_name"] == "CAL0001"

    def test_includes_qualifier(self, mock_db, sample_features):
        """Should include qualifier when present."""
        go = MockGo(1, "GO:0001234", "test process", "P")
        go_ann = MockGoAnnotation(1, 1, 1, "IDA")
        go_ref = MockGoRef(1, 1, 1, has_qualifier="Y")
        qualifier = MockGoQualifier(1, 1, "NOT")

        mock_db.query.side_effect = [
            MockQuery([(go_ref, go_ann, sample_features[0], go)]),  # Main query
            MockQuery([qualifier]),  # Qualifier lookup
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service._get_go_annotation_entries(1)

        assert result[0]["qualifier"] == "NOT"


class TestGetRefLinkEntries:
    """Tests for getting REF_LINK entries."""

    def test_categorizes_by_table(self, mock_db, sample_features):
        """Should categorize entries by table name."""
        ref_link = MockRefLink(1, 1, "FEATURE", "FEATURE_NO", 1)

        mock_db.query.side_effect = [
            MockQuery([ref_link]),  # RefLink query
            MockQuery([sample_features[0]]),  # Feature lookup
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service._get_ref_link_entries(1)

        assert len(result["feature"]) == 1
        assert result["feature"][0]["feature_name"] == "CAL0001"


class TestDeleteLitGuideEntry:
    """Tests for deleting literature guide entries."""

    def test_raises_for_unknown_refprop_feat(self, mock_db):
        """Should raise error for unknown refprop_feat."""
        mock_db.query.return_value = MockQuery([])

        service = RefAnnotationCurationService(mock_db)

        with pytest.raises(RefAnnotationCurationError) as exc_info:
            service.delete_lit_guide_entry(999, 1, "curator1")

        assert "not found" in str(exc_info.value)

    def test_deletes_refprop_feat(self, mock_db):
        """Should delete refprop_feat entry."""
        refprop_feat = MockRefpropFeat(1, 1, 1)
        ref_prop = MockRefProperty(1, 1, "Topic", "Phenotype")

        mock_db.query.side_effect = [
            MockQuery([refprop_feat]),  # RefpropFeat found
            MockQuery([ref_prop]),  # RefProperty
            MockQuery([0]),  # No remaining features
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service.delete_lit_guide_entry(1, 1, "curator1")

        assert result["success"] is True
        mock_db.delete.assert_called()

    def test_deletes_ref_property_when_no_features(self, mock_db):
        """Should delete ref_property when no features remain."""
        refprop_feat = MockRefpropFeat(1, 1, 1)
        ref_prop = MockRefProperty(1, 1, "Topic", "Phenotype")

        mock_db.query.side_effect = [
            MockQuery([refprop_feat]),  # RefpropFeat
            MockQuery([ref_prop]),  # RefProperty
            MockQuery([0]),  # No remaining features
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service.delete_lit_guide_entry(1, 1, "curator1")

        assert "Deleted ref_property" in str(result["messages"])


class TestTransferLitGuideEntry:
    """Tests for transferring literature guide entries."""

    def test_raises_for_unknown_target(self, mock_db):
        """Should raise error for unknown target reference."""
        mock_db.query.return_value = MockQuery([])

        service = RefAnnotationCurationService(mock_db)

        with pytest.raises(RefAnnotationCurationError) as exc_info:
            service.transfer_lit_guide_entry(1, 1, 999, "curator1")

        assert "not found" in str(exc_info.value)

    def test_raises_for_unknown_ref_property(self, mock_db, sample_references):
        """Should raise error for unknown ref_property."""
        mock_db.query.side_effect = [
            MockQuery([sample_references[1]]),  # Target reference found
            MockQuery([]),  # RefProperty not found
        ]

        service = RefAnnotationCurationService(mock_db)

        with pytest.raises(RefAnnotationCurationError) as exc_info:
            service.transfer_lit_guide_entry(None, 999, 2, "curator1")

        assert "not found" in str(exc_info.value)

    def test_transfers_feature_link(self, mock_db, sample_references):
        """Should transfer feature link to new reference."""
        old_ref_prop = MockRefProperty(1, 1, "Topic", "Phenotype")
        refprop_feat = MockRefpropFeat(1, 1, 1)

        mock_db.query.side_effect = [
            MockQuery([sample_references[1]]),  # Target reference
            MockQuery([old_ref_prop]),  # Old RefProperty
            MockQuery([refprop_feat]),  # RefpropFeat
            MockQuery([]),  # No existing ref_property in target
            MockQuery([]),  # No existing link
            MockQuery([0]),  # No remaining features
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service.transfer_lit_guide_entry(1, 1, 2, "curator1")

        assert result["success"] is True
        mock_db.add.assert_called()


class TestDeleteGoRefEntry:
    """Tests for deleting GO annotation entries."""

    def test_raises_for_unknown_go_ref(self, mock_db):
        """Should raise error for unknown go_ref."""
        mock_db.query.return_value = MockQuery([])

        service = RefAnnotationCurationService(mock_db)

        with pytest.raises(RefAnnotationCurationError) as exc_info:
            service.delete_go_ref_entry(999, "curator1")

        assert "not found" in str(exc_info.value)

    def test_deletes_go_ref(self, mock_db):
        """Should delete go_ref entry."""
        go_ref = MockGoRef(1, 1, 1)

        # Mock the query().filter().delete() chain for GoQualifier and GorefDbxref
        mock_delete_query = MagicMock()
        mock_delete_query.delete.return_value = 0

        mock_db.query.side_effect = [
            MockQuery([go_ref]),  # GoRef found
            mock_delete_query,  # GoQualifier delete
            mock_delete_query,  # GorefDbxref delete
            MockQuery([0]),  # No remaining go_refs (scalar)
            MockQuery([]),  # GoAnnotation lookup
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service.delete_go_ref_entry(1, "curator1")

        assert result["success"] is True
        mock_db.delete.assert_called()

    def test_deletes_go_annotation_when_unused(self, mock_db):
        """Should delete go_annotation when no more go_refs."""
        go_ref = MockGoRef(1, 1, 1)
        go_ann = MockGoAnnotation(1, 1, 1)

        # Mock the query().filter().delete() chain for GoQualifier and GorefDbxref
        mock_delete_query = MagicMock()
        mock_delete_query.delete.return_value = 0

        mock_db.query.side_effect = [
            MockQuery([go_ref]),  # GoRef
            mock_delete_query,  # GoQualifier delete
            mock_delete_query,  # GorefDbxref delete
            MockQuery([0]),  # No remaining go_refs
            MockQuery([go_ann]),  # GoAnnotation
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service.delete_go_ref_entry(1, "curator1")

        assert "Deleted go_annotation" in str(result["messages"])


class TestTransferGoRefEntry:
    """Tests for transferring GO annotation entries."""

    def test_raises_for_unknown_target(self, mock_db):
        """Should raise error for unknown target reference."""
        mock_db.query.return_value = MockQuery([])

        service = RefAnnotationCurationService(mock_db)

        with pytest.raises(RefAnnotationCurationError) as exc_info:
            service.transfer_go_ref_entry(1, 999, "curator1")

        assert "not found" in str(exc_info.value)

    def test_raises_for_unknown_go_ref(self, mock_db, sample_references):
        """Should raise error for unknown go_ref."""
        mock_db.query.side_effect = [
            MockQuery([sample_references[1]]),  # Target reference
            MockQuery([]),  # GoRef not found
        ]

        service = RefAnnotationCurationService(mock_db)

        with pytest.raises(RefAnnotationCurationError) as exc_info:
            service.transfer_go_ref_entry(999, 2, "curator1")

        assert "not found" in str(exc_info.value)

    def test_transfers_go_ref(self, mock_db, sample_references):
        """Should transfer go_ref to new reference."""
        old_go_ref = MockGoRef(1, 1, 1)

        # Mock the query().filter().delete() chain for GoQualifier and GorefDbxref
        mock_delete_query = MagicMock()
        mock_delete_query.delete.return_value = 0

        mock_db.query.side_effect = [
            MockQuery([sample_references[1]]),  # Target reference
            MockQuery([old_go_ref]),  # Old GoRef
            MockQuery([]),  # No existing go_ref in target
            mock_delete_query,  # GoQualifier delete
            mock_delete_query,  # GorefDbxref delete
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service.transfer_go_ref_entry(1, 2, "curator1")

        assert result["success"] is True
        mock_db.add.assert_called()


class TestDeleteRefLinkEntry:
    """Tests for deleting REF_LINK entries."""

    def test_raises_for_unknown_ref_link(self, mock_db):
        """Should raise error for unknown ref_link."""
        mock_db.query.return_value = MockQuery([])

        service = RefAnnotationCurationService(mock_db)

        with pytest.raises(RefAnnotationCurationError) as exc_info:
            service.delete_ref_link_entry(999, "curator1")

        assert "not found" in str(exc_info.value)

    def test_deletes_ref_link(self, mock_db):
        """Should delete ref_link entry."""
        ref_link = MockRefLink(1, 1, "FEATURE", "FEATURE_NO", 1)

        mock_db.query.side_effect = [
            MockQuery([ref_link]),  # RefLink found
            MockQuery([1]),  # Remaining links (not orphaned)
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service.delete_ref_link_entry(1, "curator1")

        assert result["success"] is True
        mock_db.delete.assert_called_once()

    def test_warns_when_orphaned(self, mock_db):
        """Should warn when data becomes orphaned."""
        ref_link = MockRefLink(1, 1, "FEATURE", "FEATURE_NO", 1)

        mock_db.query.side_effect = [
            MockQuery([ref_link]),  # RefLink found
            MockQuery([0]),  # No remaining links
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service.delete_ref_link_entry(1, "curator1")

        assert result["warning"] is not None
        assert "no longer associated" in result["warning"]


class TestTransferRefLinkEntry:
    """Tests for transferring REF_LINK entries."""

    def test_raises_for_unknown_target(self, mock_db):
        """Should raise error for unknown target reference."""
        mock_db.query.return_value = MockQuery([])

        service = RefAnnotationCurationService(mock_db)

        with pytest.raises(RefAnnotationCurationError) as exc_info:
            service.transfer_ref_link_entry(1, 999, "curator1")

        assert "not found" in str(exc_info.value)

    def test_raises_for_unknown_ref_link(self, mock_db, sample_references):
        """Should raise error for unknown ref_link."""
        mock_db.query.side_effect = [
            MockQuery([sample_references[1]]),  # Target reference
            MockQuery([]),  # RefLink not found
        ]

        service = RefAnnotationCurationService(mock_db)

        with pytest.raises(RefAnnotationCurationError) as exc_info:
            service.transfer_ref_link_entry(999, 2, "curator1")

        assert "not found" in str(exc_info.value)

    def test_transfers_ref_link(self, mock_db, sample_references):
        """Should transfer ref_link to new reference."""
        ref_link = MockRefLink(1, 1, "FEATURE", "FEATURE_NO", 1)

        mock_db.query.side_effect = [
            MockQuery([sample_references[1]]),  # Target reference
            MockQuery([ref_link]),  # RefLink found
            MockQuery([]),  # No existing link in target
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service.transfer_ref_link_entry(1, 2, "curator1")

        assert result["success"] is True
        assert ref_link.reference_no == 2


class TestBulkDelete:
    """Tests for bulk delete operations."""

    def test_raises_for_invalid_entry_type(self, mock_db):
        """Should raise error for invalid entry type."""
        service = RefAnnotationCurationService(mock_db)

        with pytest.raises(RefAnnotationCurationError) as exc_info:
            service.bulk_delete(1, "invalid_type", "curator1")

        assert "Invalid entry type" in str(exc_info.value)

    def test_bulk_deletes_lit_guide(self, mock_db):
        """Should bulk delete literature guide entries."""
        ref_prop = MockRefProperty(1, 1, "Topic", "Phenotype")
        refprop_feat = MockRefpropFeat(1, 1, 1)

        mock_db.query.side_effect = [
            MockQuery([ref_prop]),  # RefProperties
            MockQuery([refprop_feat]),  # Children
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service.bulk_delete(1, "lit_guide", "curator1")

        assert result["success"] is True
        mock_db.delete.assert_called()

    def test_bulk_deletes_go_annotation(self, mock_db):
        """Should bulk delete GO annotation entries."""
        go_ref = MockGoRef(1, 1, 1)

        # Mock the query().filter().delete() chain for GoQualifier and GorefDbxref
        mock_delete_query = MagicMock()
        mock_delete_query.delete.return_value = 0

        mock_db.query.side_effect = [
            MockQuery([go_ref]),  # GoRefs
            mock_delete_query,  # GoQualifier delete
            mock_delete_query,  # GorefDbxref delete
            MockQuery([0]),  # No remaining go_refs
            MockQuery([]),  # GoAnnotation
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service.bulk_delete(1, "go_annotation", "curator1")

        assert result["success"] is True

    def test_bulk_deletes_ref_link(self, mock_db):
        """Should bulk delete ref_link entries."""
        ref_link = MockRefLink(1, 1, "FEATURE", "FEATURE_NO", 1)

        mock_db.query.side_effect = [
            MockQuery([ref_link]),  # RefLinks
            MockQuery([0]),  # No remaining links
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service.bulk_delete(1, "ref_link", "curator1")

        assert result["success"] is True
        assert result["count"] == 1


class TestBulkTransfer:
    """Tests for bulk transfer operations."""

    def test_raises_for_unknown_target(self, mock_db):
        """Should raise error for unknown target reference."""
        mock_db.query.return_value = MockQuery([])

        service = RefAnnotationCurationService(mock_db)

        with pytest.raises(RefAnnotationCurationError) as exc_info:
            service.bulk_transfer(1, "lit_guide", 999, "curator1")

        assert "not found" in str(exc_info.value)

    def test_raises_for_invalid_entry_type(self, mock_db, sample_references):
        """Should raise error for invalid entry type."""
        mock_db.query.return_value = MockQuery([sample_references[1]])

        service = RefAnnotationCurationService(mock_db)

        with pytest.raises(RefAnnotationCurationError) as exc_info:
            service.bulk_transfer(1, "invalid_type", 2, "curator1")

        assert "Invalid entry type" in str(exc_info.value)

    def test_bulk_transfers_ref_link(self, mock_db, sample_references):
        """Should bulk transfer ref_link entries."""
        ref_link = MockRefLink(1, 1, "FEATURE", "FEATURE_NO", 1)

        mock_db.query.side_effect = [
            MockQuery([sample_references[1]]),  # Target reference
            MockQuery([ref_link]),  # RefLinks to transfer
            MockQuery([sample_references[1]]),  # Verify target for transfer_ref_link_entry
            MockQuery([ref_link]),  # RefLink
            MockQuery([]),  # No existing link
        ]

        service = RefAnnotationCurationService(mock_db)
        result = service.bulk_transfer(1, "ref_link", 2, "curator1")

        assert result["success"] is True


class TestServiceInitialization:
    """Tests for service initialization."""

    def test_stores_db_session(self, mock_db):
        """Should store database session."""
        service = RefAnnotationCurationService(mock_db)
        assert service.db is mock_db


class TestRefAnnotationCurationError:
    """Tests for the error class."""

    def test_exception_message(self):
        """Should store error message."""
        error = RefAnnotationCurationError("Test error")
        assert str(error) == "Test error"

    def test_is_exception(self):
        """Should be an Exception."""
        assert issubclass(RefAnnotationCurationError, Exception)
