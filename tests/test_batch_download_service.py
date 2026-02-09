"""
Tests for Batch Download Service.

Tests cover:
- Feature resolution
- FASTA generation (genomic, protein, coding)
- Coordinate TSV generation
- GO GAF generation
- Phenotype TSV generation
- Ortholog TSV generation
- Content compression
- Batch download processing
"""
import pytest
import gzip
from unittest.mock import MagicMock, patch
from datetime import datetime

from cgd.api.services.batch_download_service import (
    resolve_features,
    generate_genomic_fasta,
    generate_protein_fasta,
    generate_coding_fasta,
    generate_coords_tsv,
    generate_go_gaf,
    generate_phenotype_tsv,
    generate_ortholog_tsv,
    compress_content,
    process_batch_download,
    GO_ASPECT_MAP,
)
from cgd.schemas.batch_download_schema import (
    DataType,
    BatchDownloadRequest,
    ResolvedFeature,
)


class MockOrganism:
    """Mock Organism model."""

    def __init__(self, organism_no: int, organism_name: str):
        self.organism_no = organism_no
        self.organism_name = organism_name


class MockFeature:
    """Mock Feature model."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        gene_name: str = None,
        dbxref_id: str = None,
        feature_type: str = "ORF",
        organism: MockOrganism = None,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name
        self.dbxref_id = dbxref_id
        self.feature_type = feature_type
        self.organism = organism


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

    def __init__(self, seq_no: int, feature_no: int, feature: MockFeature = None):
        self.seq_no = seq_no
        self.feature_no = feature_no
        self.feature = feature


class MockGo:
    """Mock Go model."""

    def __init__(self, go_no: int, goid: int, go_term: str, go_aspect: str):
        self.go_no = go_no
        self.goid = goid
        self.go_term = go_term
        self.go_aspect = go_aspect


class MockGoAnnotation:
    """Mock GoAnnotation model."""

    def __init__(
        self,
        feature_no: int,
        go: MockGo,
        go_evidence: str = "IDA",
        source: str = "CGD",
        date_created: datetime = None,
    ):
        self.feature_no = feature_no
        self.go = go
        self.go_evidence = go_evidence
        self.source = source
        self.date_created = date_created or datetime.now()


class MockPhenotype:
    """Mock Phenotype model."""

    def __init__(
        self,
        observable: str,
        qualifier: str = None,
        experiment_type: str = None,
        mutant_type: str = None,
        source: str = "CGD",
    ):
        self.observable = observable
        self.qualifier = qualifier
        self.experiment_type = experiment_type
        self.mutant_type = mutant_type
        self.source = source


class MockPhenoAnnotation:
    """Mock PhenoAnnotation model."""

    def __init__(self, feature_no: int, phenotype: MockPhenotype):
        self.feature_no = feature_no
        self.phenotype = phenotype


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []

    def options(self, *args, **kwargs):
        return self

    def join(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
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
    return MockOrganism(1, "Candida albicans SC5314")


@pytest.fixture
def sample_feature(sample_organism):
    """Create sample feature."""
    return MockFeature(
        feature_no=1,
        feature_name="CAL0001",
        gene_name="ALS1",
        dbxref_id="CGD:CAL0001",
        organism=sample_organism,
    )


@pytest.fixture
def sample_resolved_feature():
    """Create sample resolved feature."""
    return ResolvedFeature(
        feature_no=1,
        feature_name="CAL0001",
        gene_name="ALS1",
        dbxref_id="CGD:CAL0001",
        feature_type="ORF",
        organism_name="Candida albicans SC5314",
        chromosome="Chr1",
        start=1000,
        end=2000,
        strand="W",
    )


class TestGoAspectMap:
    """Tests for GO aspect mapping."""

    def test_function_maps_to_f(self):
        """Should map function to F."""
        assert GO_ASPECT_MAP["function"] == "F"
        assert GO_ASPECT_MAP["molecular_function"] == "F"

    def test_process_maps_to_p(self):
        """Should map process to P."""
        assert GO_ASPECT_MAP["process"] == "P"
        assert GO_ASPECT_MAP["biological_process"] == "P"

    def test_component_maps_to_c(self):
        """Should map component to C."""
        assert GO_ASPECT_MAP["component"] == "C"
        assert GO_ASPECT_MAP["cellular_component"] == "C"


class TestResolveFeatures:
    """Tests for resolve_features."""

    def test_returns_empty_for_empty_queries(self, mock_db):
        """Should return empty lists for empty queries."""
        found, not_found = resolve_features(mock_db, [])
        assert found == []
        assert not_found == []

    def test_finds_feature_by_gene_name(self, mock_db, sample_feature):
        """Should find feature by gene name."""
        mock_db.query.side_effect = [
            MockQuery([sample_feature]),  # Feature lookup
            MockQuery([]),  # Location lookup
        ]

        found, not_found = resolve_features(mock_db, ["ALS1"])

        assert len(found) == 1
        assert found[0].gene_name == "ALS1"
        assert len(not_found) == 0

    def test_marks_not_found(self, mock_db):
        """Should mark queries as not found."""
        mock_db.query.return_value = MockQuery([])

        found, not_found = resolve_features(mock_db, ["UNKNOWN"])

        assert len(found) == 0
        assert len(not_found) == 1
        assert not_found[0].query == "UNKNOWN"

    def test_includes_location_info(self, mock_db, sample_feature):
        """Should include location info when available."""
        chr_feature = MockFeature(10, "Chr1")
        root_seq = MockSeq(100, 10, feature=chr_feature)
        location = MockFeatLocation(1, 100, 1000, 2000, "W")

        mock_db.query.side_effect = [
            MockQuery([sample_feature]),  # Feature lookup
            MockQuery([location]),  # Location lookup
            MockQuery([root_seq]),  # Root seq lookup
        ]

        found, _ = resolve_features(mock_db, ["ALS1"])

        assert found[0].chromosome == "Chr1"
        assert found[0].start == 1000
        assert found[0].end == 2000
        assert found[0].strand == "W"

    def test_skips_empty_queries(self, mock_db):
        """Should skip empty query strings."""
        found, not_found = resolve_features(mock_db, ["", "  "])
        assert found == []
        assert not_found == []


class TestGenerateCoordsTsv:
    """Tests for generate_coords_tsv."""

    def test_generates_header(self, mock_db, sample_resolved_feature):
        """Should generate header line."""
        result = generate_coords_tsv(mock_db, [sample_resolved_feature])
        lines = result.split("\n")
        assert "feature_name" in lines[0]
        assert "chromosome" in lines[0]

    def test_generates_data_row(self, mock_db, sample_resolved_feature):
        """Should generate data rows."""
        result = generate_coords_tsv(mock_db, [sample_resolved_feature])
        assert "CAL0001" in result
        assert "ALS1" in result
        assert "Chr1" in result

    def test_converts_strand(self, mock_db, sample_resolved_feature):
        """Should convert strand W to + and C to -."""
        result = generate_coords_tsv(mock_db, [sample_resolved_feature])
        assert "\t+\t" in result

        feat_crick = ResolvedFeature(
            feature_no=2,
            feature_name="CAL0002",
            dbxref_id="CGD:CAL0002",
            feature_type="ORF",
            strand="C",
        )
        result = generate_coords_tsv(mock_db, [feat_crick])
        assert "\t-\t" in result


class TestGenerateGoGaf:
    """Tests for generate_go_gaf."""

    def test_generates_gaf_header(self, mock_db, sample_resolved_feature):
        """Should generate GAF header."""
        mock_db.query.return_value = MockQuery([])

        result = generate_go_gaf(mock_db, [sample_resolved_feature])

        assert "!gaf-version: 2.2" in result

    def test_generates_gaf_entry(self, mock_db, sample_resolved_feature):
        """Should generate GAF entry for annotation."""
        go = MockGo(1, 6412, "translation", "process")
        annotation = MockGoAnnotation(1, go, "IDA", "CGD")

        mock_db.query.return_value = MockQuery([annotation])

        result = generate_go_gaf(mock_db, [sample_resolved_feature])

        assert "CGD" in result
        assert "GO:0006412" in result
        assert "IDA" in result


class TestGeneratePhenotypeTsv:
    """Tests for generate_phenotype_tsv."""

    def test_generates_header(self, mock_db, sample_resolved_feature):
        """Should generate header line."""
        mock_db.query.return_value = MockQuery([])

        result = generate_phenotype_tsv(mock_db, [sample_resolved_feature])

        assert "feature_name" in result
        assert "observable" in result

    def test_generates_phenotype_row(self, mock_db, sample_resolved_feature):
        """Should generate phenotype data rows."""
        phenotype = MockPhenotype("colony morphology", "abnormal", "classical genetics", "null")
        annotation = MockPhenoAnnotation(1, phenotype)

        mock_db.query.return_value = MockQuery([annotation])

        result = generate_phenotype_tsv(mock_db, [sample_resolved_feature])

        assert "colony morphology" in result
        assert "abnormal" in result


class TestGenerateOrthologTsv:
    """Tests for generate_ortholog_tsv."""

    def test_generates_header(self, mock_db, sample_resolved_feature):
        """Should generate header line."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Homology query
            MockQuery([]),  # Dbxref query
        ]

        result = generate_ortholog_tsv(mock_db, [sample_resolved_feature])

        assert "feature_name" in result
        assert "ortholog_feature" in result


class TestCompressContent:
    """Tests for compress_content."""

    def test_compresses_content(self):
        """Should gzip compress content."""
        content = "Test content for compression"

        result = compress_content(content)

        # Should be bytes
        assert isinstance(result, bytes)
        # Should start with gzip magic number
        assert result[:2] == b'\x1f\x8b'

    def test_decompresses_correctly(self):
        """Should decompress back to original."""
        content = "Test content for compression"

        compressed = compress_content(content)
        decompressed = gzip.decompress(compressed).decode('utf-8')

        assert decompressed == content


class TestProcessBatchDownload:
    """Tests for process_batch_download."""

    @patch('cgd.api.services.batch_download_service.resolve_features')
    @patch('cgd.api.services.batch_download_service.generate_coords_tsv')
    def test_returns_results_dict(self, mock_coords, mock_resolve, mock_db, sample_resolved_feature):
        """Should return results dictionary."""
        mock_resolve.return_value = ([sample_resolved_feature], [])
        mock_coords.return_value = "header\ndata"

        request = BatchDownloadRequest(
            genes=["ALS1"],
            data_types=[DataType.COORDS],
        )

        results, features, not_found = process_batch_download(mock_db, request)

        assert DataType.COORDS in results

    @patch('cgd.api.services.batch_download_service.resolve_features')
    @patch('cgd.api.services.batch_download_service.generate_coords_tsv')
    def test_returns_tsv_extension(self, mock_coords, mock_resolve, mock_db, sample_resolved_feature):
        """Should return TSV extension for coordinate data."""
        mock_resolve.return_value = ([sample_resolved_feature], [])
        mock_coords.return_value = "header\ndata"

        request = BatchDownloadRequest(
            genes=["ALS1"],
            data_types=[DataType.COORDS],
            compress=False,
        )

        results, _, _ = process_batch_download(mock_db, request)

        filename, _ = results[DataType.COORDS]
        assert filename.endswith(".tsv")

    @patch('cgd.api.services.batch_download_service.resolve_features')
    @patch('cgd.api.services.batch_download_service.generate_coords_tsv')
    def test_compresses_when_requested(self, mock_coords, mock_resolve, mock_db, sample_resolved_feature):
        """Should compress content when requested."""
        mock_resolve.return_value = ([sample_resolved_feature], [])
        mock_coords.return_value = "header\ndata"

        request = BatchDownloadRequest(
            genes=["ALS1"],
            data_types=[DataType.COORDS],
            compress=True,
        )

        results, _, _ = process_batch_download(mock_db, request)

        filename, content = results[DataType.COORDS]
        assert filename.endswith(".gz")
        # Content should be gzip compressed
        assert content[:2] == b'\x1f\x8b'

    @patch('cgd.api.services.batch_download_service.resolve_features')
    def test_returns_not_found(self, mock_resolve, mock_db):
        """Should return not found queries."""
        from cgd.schemas.batch_download_schema import FeatureNotFound
        mock_resolve.return_value = ([], [FeatureNotFound(query="UNKNOWN", reason="not found")])

        request = BatchDownloadRequest(
            genes=["UNKNOWN"],
            data_types=[DataType.COORDS],
        )

        results, features, not_found = process_batch_download(mock_db, request)

        assert len(not_found) == 1
        assert not_found[0].query == "UNKNOWN"

    @patch('cgd.api.services.batch_download_service.resolve_features')
    @patch('cgd.api.services.batch_download_service.generate_genomic_fasta')
    def test_fasta_extension_for_sequences(self, mock_fasta, mock_resolve, mock_db, sample_resolved_feature):
        """Should return FASTA extension for sequence data."""
        mock_resolve.return_value = ([sample_resolved_feature], [])
        mock_fasta.return_value = ">test\nATGC"

        request = BatchDownloadRequest(
            genes=["ALS1"],
            data_types=[DataType.GENOMIC],
            compress=False,
        )

        results, _, _ = process_batch_download(mock_db, request)

        filename, _ = results[DataType.GENOMIC]
        assert filename.endswith(".fasta")

    @patch('cgd.api.services.batch_download_service.resolve_features')
    @patch('cgd.api.services.batch_download_service.generate_go_gaf')
    def test_gaf_extension_for_go(self, mock_gaf, mock_resolve, mock_db, sample_resolved_feature):
        """Should return GAF extension for GO data."""
        mock_resolve.return_value = ([sample_resolved_feature], [])
        mock_gaf.return_value = "!gaf-version: 2.2\ndata"

        request = BatchDownloadRequest(
            genes=["ALS1"],
            data_types=[DataType.GO],
            compress=False,
        )

        results, _, _ = process_batch_download(mock_db, request)

        filename, _ = results[DataType.GO]
        assert filename.endswith(".gaf")
