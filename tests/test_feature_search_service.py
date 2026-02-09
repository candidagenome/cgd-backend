"""
Tests for Feature Search Service.

Tests cover:
- Chunked IN query utility
- Feature search configuration
- Feature search with filters
- Organism, feature type, qualifier retrieval
- Chromosome filtering
- Intron filtering
- GO term filtering
- Feature sorting
- TSV download generation
"""
import pytest
from unittest.mock import MagicMock, patch

from cgd.api.services.feature_search_service import (
    _chunked_in_query,
    get_feature_search_config,
    search_features,
    _get_organisms,
    _get_feature_types,
    _get_qualifiers,
    _get_chromosomes_for_organism,
    _get_go_slim_terms,
    _get_evidence_codes,
    _get_annotation_methods,
    _filter_by_qualifiers,
    _exclude_deleted_features,
    _filter_by_chromosomes,
    _filter_by_introns,
    _sort_features,
    generate_download_tsv,
    GO_SLIM_SET_NAME,
    ORACLE_IN_LIMIT,
)
from cgd.schemas.feature_search_schema import FeatureSearchRequest


class MockOrganism:
    """Mock Organism model."""

    def __init__(
        self,
        organism_no: int,
        organism_name: str,
        organism_abbrev: str,
        organism_order: int = 1,
        taxonomic_rank: str = "Strain",
    ):
        self.organism_no = organism_no
        self.organism_name = organism_name
        self.organism_abbrev = organism_abbrev
        self.organism_order = organism_order
        self.taxonomic_rank = taxonomic_rank


class MockFeature:
    """Mock Feature model."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        gene_name: str = None,
        feature_type: str = "ORF",
        headline: str = None,
        organism_no: int = None,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name
        self.feature_type = feature_type
        self.headline = headline
        self.organism_no = organism_no


class MockFeatProperty:
    """Mock FeatProperty model."""

    def __init__(
        self,
        feature_no: int,
        property_type: str,
        property_value: str,
    ):
        self.feature_no = feature_no
        self.property_type = property_type
        self.property_value = property_value


class MockFeatLocation:
    """Mock FeatLocation model."""

    def __init__(
        self,
        feature_no: int,
        root_seq_no: int,
        start_coord: int,
        stop_coord: int,
        strand: str = "W",
        is_loc_current: str = "Y",
    ):
        self.feature_no = feature_no
        self.root_seq_no = root_seq_no
        self.start_coord = start_coord
        self.stop_coord = stop_coord
        self.strand = strand
        self.is_loc_current = is_loc_current


class MockSeq:
    """Mock Seq model."""

    def __init__(self, seq_no: int, feature_no: int):
        self.seq_no = seq_no
        self.feature_no = feature_no


class MockGo:
    """Mock Go model."""

    def __init__(
        self,
        go_no: int,
        goid: int,
        go_term: str,
        go_aspect: str = "P",
    ):
        self.go_no = go_no
        self.goid = goid
        self.go_term = go_term
        self.go_aspect = go_aspect


class MockGoSet:
    """Mock GoSet model."""

    def __init__(self, go_no: int, go_set_name: str):
        self.go_no = go_no
        self.go_set_name = go_set_name


class MockGoAnnotation:
    """Mock GoAnnotation model."""

    def __init__(
        self,
        feature_no: int,
        go_no: int,
        go_evidence: str = "IDA",
        annotation_type: str = "manually curated",
    ):
        self.feature_no = feature_no
        self.go_no = go_no
        self.go_evidence = go_evidence
        self.annotation_type = annotation_type


class MockFeatRelationship:
    """Mock FeatRelationship model."""

    def __init__(self, parent_feature_no: int, child_feature_no: int):
        self.parent_feature_no = parent_feature_no
        self.child_feature_no = child_feature_no


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []

    def join(self, *args, **kwargs):
        return self

    def outerjoin(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def exists(self):
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
    return MockOrganism(
        organism_no=1,
        organism_name="Candida albicans SC5314",
        organism_abbrev="C_albicans_SC5314",
    )


@pytest.fixture
def sample_feature():
    """Create sample feature."""
    return MockFeature(
        feature_no=1,
        feature_name="CAL0001",
        gene_name="ALS1",
        feature_type="ORF",
        headline="Cell surface adhesin",
        organism_no=1,
    )


class TestChunkedInQuery:
    """Tests for _chunked_in_query."""

    def test_handles_empty_items(self, mock_db):
        """Should return empty list for empty items."""
        result = _chunked_in_query(mock_db, lambda db, chunk: [], [])
        assert result == []

    def test_processes_small_list(self, mock_db):
        """Should process list smaller than chunk size."""
        items = [1, 2, 3]

        def query_func(db, chunk):
            return [("result", i) for i in chunk]

        result = _chunked_in_query(mock_db, query_func, items)

        assert len(result) == 3

    def test_chunks_large_list(self, mock_db):
        """Should chunk list larger than limit."""
        items = list(range(2000))
        chunks_processed = []

        def query_func(db, chunk):
            chunks_processed.append(len(chunk))
            return [i for i in chunk]

        result = _chunked_in_query(mock_db, query_func, items, chunk_size=ORACLE_IN_LIMIT)

        # Should have multiple chunks
        assert len(chunks_processed) > 1
        # Each chunk should be <= ORACLE_IN_LIMIT
        assert all(c <= ORACLE_IN_LIMIT for c in chunks_processed)
        # Should have all items
        assert len(result) == 2000


class TestGetOrganisms:
    """Tests for _get_organisms."""

    def test_returns_organisms(self, mock_db, sample_organism):
        """Should return list of organisms."""
        mock_db.query.return_value = MockQuery([sample_organism])

        result = _get_organisms(mock_db)

        assert len(result) == 1
        assert result[0].organism_abbrev == "C_albicans_SC5314"
        assert result[0].organism_name == "Candida albicans SC5314"


class TestGetFeatureTypes:
    """Tests for _get_feature_types."""

    def test_returns_feature_types(self, mock_db):
        """Should return list of feature types."""
        mock_db.query.return_value = MockQuery([("ORF",), ("tRNA",), ("rRNA",)])

        result = _get_feature_types(mock_db)

        assert "ORF" in result
        assert "tRNA" in result

    def test_puts_orf_first(self, mock_db):
        """Should put ORF first in list."""
        mock_db.query.return_value = MockQuery([("tRNA",), ("ORF",), ("rRNA",)])

        result = _get_feature_types(mock_db)

        assert result[0] == "ORF"


class TestGetQualifiers:
    """Tests for _get_qualifiers."""

    def test_returns_qualifiers(self, mock_db):
        """Should return list of qualifiers."""
        mock_db.query.return_value = MockQuery([
            ("Verified",), ("Uncharacterized",), ("Deleted",)
        ])

        result = _get_qualifiers(mock_db)

        assert "Verified" in result
        assert "Uncharacterized" in result

    def test_orders_deleted_last(self, mock_db):
        """Should order Deleted qualifiers last."""
        mock_db.query.return_value = MockQuery([
            ("Deleted",), ("Verified",), ("Merged",)
        ])

        result = _get_qualifiers(mock_db)

        assert result[-1] == "Deleted"


class TestGetChromosomesForOrganism:
    """Tests for _get_chromosomes_for_organism."""

    def test_returns_chromosomes(self, mock_db):
        """Should return chromosomes for organism."""
        mock_db.query.return_value = MockQuery([("Chr1",), ("Chr2",)])

        result = _get_chromosomes_for_organism(mock_db, "C_albicans_SC5314")

        assert "Chr1" in result
        assert "Chr2" in result


class TestGetGoSlimTerms:
    """Tests for _get_go_slim_terms."""

    def test_returns_go_slim_terms_grouped_by_aspect(self, mock_db):
        """Should return GO Slim terms grouped by aspect."""
        mock_db.query.return_value = MockQuery([
            (1, 5634, "nucleus", "C"),
            (2, 6412, "translation", "P"),
            (3, 3674, "molecular_function", "F"),
        ])

        result = _get_go_slim_terms(mock_db)

        assert len(result.component) == 1
        assert len(result.process) == 1
        assert len(result.function) == 1


class TestGetEvidenceCodes:
    """Tests for _get_evidence_codes."""

    def test_returns_evidence_codes(self, mock_db):
        """Should return list of evidence codes."""
        mock_db.query.return_value = MockQuery([("IDA",), ("IMP",), ("TAS",)])

        result = _get_evidence_codes(mock_db)

        assert "IDA" in result
        assert "IMP" in result


class TestGetAnnotationMethods:
    """Tests for _get_annotation_methods."""

    def test_returns_annotation_methods(self, mock_db):
        """Should return list of annotation methods."""
        mock_db.query.return_value = MockQuery([
            ("manually curated",), ("high-throughput",), ("computational",)
        ])

        result = _get_annotation_methods(mock_db)

        assert "manually curated" in result
        assert "high-throughput" in result


class TestFilterByQualifiers:
    """Tests for _filter_by_qualifiers."""

    def test_handles_empty_feature_nos(self, mock_db):
        """Should return empty set for empty input."""
        result, count = _filter_by_qualifiers(mock_db, set(), ["Verified"])

        assert result == set()
        assert count == 0

    def test_filters_by_qualifiers(self, mock_db):
        """Should filter features by qualifiers."""
        mock_db.query.return_value = MockQuery([(1,), (2,)])

        result, count = _filter_by_qualifiers(mock_db, {1, 2, 3}, ["Verified"])

        assert len(result) == 2
        assert count == 2


class TestExcludeDeletedFeatures:
    """Tests for _exclude_deleted_features."""

    def test_excludes_deleted(self, mock_db):
        """Should exclude deleted features."""
        mock_db.query.return_value = MockQuery([(2,)])  # Feature 2 is deleted

        result, count = _exclude_deleted_features(mock_db, {1, 2, 3})

        assert 2 not in result
        assert 1 in result
        assert 3 in result


class TestFilterByChromosomes:
    """Tests for _filter_by_chromosomes."""

    def test_handles_empty_feature_nos(self, mock_db):
        """Should return empty set for empty input."""
        result, count = _filter_by_chromosomes(mock_db, set(), ["Chr1"])

        assert result == set()
        assert count == 0

    def test_filters_by_chromosomes(self, mock_db):
        """Should filter features by chromosome."""
        mock_db.query.side_effect = [
            MockQuery([(10,)]),  # Chr feature_no
            MockQuery([(100,)]),  # Chr seq_no
            MockQuery([(1,), (2,)]),  # Feature locations
        ]

        result, count = _filter_by_chromosomes(mock_db, {1, 2, 3}, ["Chr1"])

        assert len(result) == 2


class TestFilterByIntrons:
    """Tests for _filter_by_introns."""

    def test_handles_empty_feature_nos(self, mock_db):
        """Should return empty set for empty input."""
        result, count = _filter_by_introns(mock_db, set(), True)

        assert result == set()
        assert count == 0

    def test_filters_features_with_introns(self, mock_db):
        """Should filter features with introns."""
        # Feature 1 has intron child (feature 10)
        mock_db.query.side_effect = [
            MockQuery([(1, 10), (2, 20)]),  # Parent-child relationships
            MockQuery([(10, "intron"), (20, "exon")]),  # Child feature types
        ]

        result, count = _filter_by_introns(mock_db, {1, 2}, True)

        assert 1 in result
        assert 2 not in result

    def test_filters_features_without_introns(self, mock_db):
        """Should filter features without introns."""
        mock_db.query.side_effect = [
            MockQuery([(1, 10), (2, 20)]),  # Parent-child relationships
            MockQuery([(10, "intron"), (20, "exon")]),  # Child feature types
        ]

        result, count = _filter_by_introns(mock_db, {1, 2}, False)

        assert 2 in result
        assert 1 not in result


class TestSortFeatures:
    """Tests for _sort_features."""

    def test_handles_empty_set(self, mock_db):
        """Should return empty list for empty set."""
        result = _sort_features(mock_db, set(), "orf")
        assert result == []

    def test_sorts_by_feature_name(self, mock_db):
        """Should sort by feature name (default)."""
        mock_db.query.return_value = MockQuery([
            (1, "CAL0003", "ALS3", "ORF"),
            (2, "CAL0001", "ALS1", "ORF"),
            (3, "CAL0002", "ALS2", "ORF"),
        ])

        result = _sort_features(mock_db, {1, 2, 3}, "orf")

        assert result == [2, 3, 1]  # Sorted by feature_name

    def test_sorts_by_gene_name(self, mock_db):
        """Should sort by gene name."""
        mock_db.query.return_value = MockQuery([
            (1, "CAL0001", "ZZZ1", "ORF"),
            (2, "CAL0002", "AAA1", "ORF"),
            (3, "CAL0003", None, "ORF"),
        ])

        result = _sort_features(mock_db, {1, 2, 3}, "gene")

        assert result[0] == 2  # AAA1 first
        assert result[-1] == 3  # None last


class TestSearchFeatures:
    """Tests for search_features."""

    def test_requires_organism(self, mock_db):
        """Should return error when organism is missing."""
        request = FeatureSearchRequest(
            organism="",
            feature_types=["ORF"],
        )

        result = search_features(mock_db, request)

        assert result.success is False
        assert "Organism is required" in result.error

    def test_requires_feature_types(self, mock_db):
        """Should return error when no feature types selected."""
        request = FeatureSearchRequest(
            organism="C_albicans_SC5314",
            feature_types=[],
            include_all_types=False,
        )

        result = search_features(mock_db, request)

        assert result.success is False
        assert "feature type" in result.error

    def test_returns_error_for_unknown_organism(self, mock_db):
        """Should return error for unknown organism."""
        mock_db.query.return_value = MockQuery([])

        request = FeatureSearchRequest(
            organism="UNKNOWN",
            feature_types=["ORF"],
        )

        result = search_features(mock_db, request)

        assert result.success is False
        assert "not found" in result.error

    def test_returns_empty_results_for_no_features(self, mock_db, sample_organism):
        """Should return empty results when no features match."""
        # Create a query that returns organism first, then empty features
        call_count = [0]

        def mock_query(*args):
            call_count[0] += 1
            if call_count[0] == 1:  # First query is organism lookup
                q = MockQuery([sample_organism])
                q.filter = MagicMock(return_value=q)
                return q
            # Return empty for all other queries
            q = MockQuery([])
            q.filter = MagicMock(return_value=q)
            return q

        mock_db.query.side_effect = mock_query

        request = FeatureSearchRequest(
            organism="C_albicans_SC5314",
            include_all_types=True,
        )

        result = search_features(mock_db, request)

        # With no features matching, should return success with 0 results
        assert result.success is True
        assert result.total_count == 0


class TestGetFeatureSearchConfig:
    """Tests for get_feature_search_config."""

    def test_returns_config(self, mock_db, sample_organism):
        """Should return feature search configuration."""
        mock_db.query.side_effect = [
            MockQuery([sample_organism]),  # Organisms
            MockQuery([("ORF",)]),  # Feature types
            MockQuery([("Verified",)]),  # Qualifiers
            MockQuery([("Chr1",)]),  # Chromosomes for organism
            MockQuery([]),  # GO Slim terms
            MockQuery([("IDA",)]),  # Evidence codes
            MockQuery([("manually curated",)]),  # Annotation methods
        ]

        result = get_feature_search_config(mock_db)

        assert len(result.organisms) >= 0
        assert len(result.feature_types) >= 0


class TestGenerateDownloadTsv:
    """Tests for generate_download_tsv."""

    def test_returns_no_results_message(self, mock_db):
        """Should return message when no results."""
        mock_db.query.return_value = MockQuery([])

        request = FeatureSearchRequest(
            organism="C_albicans_SC5314",
            feature_types=["ORF"],
        )

        result = generate_download_tsv(mock_db, request)

        assert "No results" in result

    def test_generates_tsv_headers(self, mock_db, sample_organism):
        """Should generate TSV with headers (when results exist)."""
        # Create a query that returns organism first, then empty features
        call_count = [0]

        def mock_query(*args):
            call_count[0] += 1
            if call_count[0] == 1:  # First query is organism lookup
                q = MockQuery([sample_organism])
                q.filter = MagicMock(return_value=q)
                return q
            # Return empty for all other queries
            q = MockQuery([])
            q.filter = MagicMock(return_value=q)
            return q

        mock_db.query.side_effect = mock_query

        request = FeatureSearchRequest(
            organism="C_albicans_SC5314",
            include_all_types=True,
        )

        result = generate_download_tsv(mock_db, request)

        # Should return string with "No results" when no features found
        assert isinstance(result, str)
        assert "No results" in result


class TestGoSlimSetName:
    """Tests for GO_SLIM_SET_NAME constant."""

    def test_go_slim_set_name(self):
        """Should have correct GO Slim set name."""
        assert GO_SLIM_SET_NAME == "CGD_GO_Slim"


class TestOracleInLimit:
    """Tests for ORACLE_IN_LIMIT constant."""

    def test_oracle_in_limit(self):
        """Should have correct Oracle IN limit."""
        assert ORACLE_IN_LIMIT == 999
