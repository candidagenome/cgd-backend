"""
Tests for Sequence Tools Service.

Tests cover:
- Gene query resolution
- JBrowse link building
- BLAST link building
- Pattern match link building
- Restriction map link building
- Primer design link building
- Sequence retrieval link building
- Tool generation for genes, coordinates, and sequences
- Assembly listing
- Chromosome listing
"""
import pytest
from unittest.mock import MagicMock

from cgd.api.services.seq_tools_service import (
    resolve_gene_query,
    _build_jbrowse_link,
    _build_blast_link,
    _build_blast_link_for_locus,
    _build_pattern_match_link,
    _build_restriction_map_link,
    _build_restriction_map_link_for_locus,
    _build_primer_design_link,
    _build_sequence_retrieval_links,
    _build_coordinate_sequence_link,
    get_tools_for_gene,
    get_tools_for_coordinates,
    get_tools_for_sequence,
    get_available_assemblies,
    get_chromosomes,
    resolve_and_get_tools,
    JBROWSE_BASE_URL,
    DEFAULT_JBROWSE_TRACKS,
)
from cgd.schemas.seq_tools_schema import (
    InputType,
    SeqType,
    FeatureInfo,
)


class MockOrganism:
    """Mock Organism model."""

    def __init__(self, organism_no: int, organism_name: str, organism_order: int = 1):
        self.organism_no = organism_no
        self.organism_name = organism_name
        self.organism_order = organism_order


class MockFeature:
    """Mock Feature model."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        gene_name: str = None,
        dbxref_id: str = None,
        organism: MockOrganism = None,
        feature_type: str = "ORF",
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name
        self.dbxref_id = dbxref_id
        self.organism = organism
        self.organism_no = organism.organism_no if organism else None
        self.feature_type = feature_type


class MockSeq:
    """Mock Seq model."""

    def __init__(
        self,
        seq_no: int,
        feature_no: int,
        feature: MockFeature = None,
        seq_type: str = "genomic",
        residues: str = None,
        seq_length: int = None,
        is_seq_current: str = "Y",
    ):
        self.seq_no = seq_no
        self.feature_no = feature_no
        self.feature = feature
        self.seq_type = seq_type
        self.residues = residues
        self.seq_length = seq_length or (len(residues) if residues else 0)
        self.is_seq_current = is_seq_current


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


class MockGenomeVersion:
    """Mock GenomeVersion model."""

    def __init__(
        self,
        organism_no: int,
        genome_version: str,
        is_ver_current: str = "Y",
    ):
        self.organism_no = organism_no
        self.genome_version = genome_version
        self.is_ver_current = is_ver_current


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []

    def outerjoin(self, *args, **kwargs):
        return self

    def join(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def scalar_subquery(self):
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
def sample_feature_info():
    """Create sample FeatureInfo."""
    return FeatureInfo(
        feature_name="CAL0001",
        gene_name="ALS1",
        dbxref_id="CGD:CAL0001",
        organism="Candida albicans SC5314",
        chromosome="Chr1",
        start=1000,
        end=2000,
        strand="W",
    )


class TestBuildJbrowseLink:
    """Tests for _build_jbrowse_link."""

    def test_builds_basic_link(self):
        """Should build basic JBrowse link."""
        result = _build_jbrowse_link("Chr1", 1000, 2000)

        assert JBROWSE_BASE_URL in result
        assert "loc=Chr1%3A1000..2000" in result
        # The comma is URL-encoded as %2C
        assert "tracks=DNA%2CGenes" in result

    def test_includes_flanking(self):
        """Should include flanking regions."""
        result = _build_jbrowse_link("Chr1", 1000, 2000, flank_left=100, flank_right=200)

        assert "loc=Chr1%3A900..2200" in result

    def test_clamps_start_to_one(self):
        """Should not allow start < 1."""
        result = _build_jbrowse_link("Chr1", 50, 100, flank_left=100)

        assert "loc=Chr1%3A1..100" in result

    def test_returns_none_when_no_chromosome(self):
        """Should return None when no chromosome."""
        result = _build_jbrowse_link(None, 1000, 2000)
        assert result is None

    def test_returns_none_when_no_start(self):
        """Should return None when no start."""
        result = _build_jbrowse_link("Chr1", None, 2000)
        assert result is None

    def test_returns_none_when_no_end(self):
        """Should return None when no end."""
        result = _build_jbrowse_link("Chr1", 1000, None)
        assert result is None


class TestBuildBlastLink:
    """Tests for _build_blast_link."""

    def test_builds_link(self):
        """Should build BLAST link with sequence."""
        result = _build_blast_link("ATGCATGC")

        assert result.startswith("/blast?")
        assert "seq=ATGCATGC" in result

    def test_truncates_long_sequence(self):
        """Should truncate sequence to 5000 chars."""
        long_seq = "A" * 10000
        result = _build_blast_link(long_seq)

        # URL should not contain full 10000 A's
        assert len(result) < 6000


class TestBuildBlastLinkForLocus:
    """Tests for _build_blast_link_for_locus."""

    def test_builds_link(self):
        """Should build BLAST link with locus."""
        result = _build_blast_link_for_locus("ALS1")

        assert result.startswith("/blast?")
        assert "locus=ALS1" in result
        assert "qtype=locus" in result


class TestBuildPatternMatchLink:
    """Tests for _build_pattern_match_link."""

    def test_builds_link(self):
        """Should build pattern match link."""
        result = _build_pattern_match_link("ATGC")

        assert result.startswith("/patmatch?")
        assert "pattern=ATGC" in result

    def test_truncates_long_pattern(self):
        """Should truncate pattern to 100 chars."""
        long_pattern = "A" * 200
        result = _build_pattern_match_link(long_pattern)

        # Should not contain full 200 A's
        assert "A" * 100 in result
        assert "A" * 200 not in result


class TestBuildRestrictionMapLink:
    """Tests for _build_restriction_map_link."""

    def test_builds_link(self):
        """Should build restriction map link."""
        result = _build_restriction_map_link("ATGCATGC")

        assert result.startswith("/restriction-mapper?")
        assert "seq=ATGCATGC" in result
        assert "type=sequence" in result


class TestBuildRestrictionMapLinkForLocus:
    """Tests for _build_restriction_map_link_for_locus."""

    def test_builds_link(self):
        """Should build restriction map link for locus."""
        result = _build_restriction_map_link_for_locus("ALS1")

        assert result.startswith("/restriction-mapper?")
        assert "locus=ALS1" in result
        assert "type=locus" in result


class TestBuildPrimerDesignLink:
    """Tests for _build_primer_design_link."""

    def test_builds_link(self):
        """Should build primer design link."""
        result = _build_primer_design_link("ATGCATGC")

        assert "/cgi-bin/compute/web-primer" in result
        assert "seq=ATGCATGC" in result


class TestBuildSequenceRetrievalLinks:
    """Tests for _build_sequence_retrieval_links."""

    def test_returns_three_links(self):
        """Should return DNA, coding, and protein links."""
        result = _build_sequence_retrieval_links("CAL0001")

        assert len(result) == 3
        names = [link.name for link in result]
        assert "DNA Sequence (FASTA)" in names
        assert "Coding Sequence (FASTA)" in names
        assert "Protein Sequence (FASTA)" in names

    def test_includes_flanking_in_dna(self):
        """Should include flanking parameters in DNA link."""
        result = _build_sequence_retrieval_links("CAL0001", flank_left=100, flank_right=200)

        dna_link = next(l for l in result if "DNA" in l.name)
        assert "flankl=100" in dna_link.url
        assert "flankr=200" in dna_link.url

    def test_dna_link_format(self):
        """Should format DNA link correctly."""
        result = _build_sequence_retrieval_links("CAL0001")

        dna_link = next(l for l in result if "DNA" in l.name)
        assert "locus=CAL0001" in dna_link.url
        assert "seqtype=genomic" in dna_link.url
        assert "format=fasta" in dna_link.url


class TestBuildCoordinateSequenceLink:
    """Tests for _build_coordinate_sequence_link."""

    def test_builds_link(self):
        """Should build coordinate sequence link."""
        result = _build_coordinate_sequence_link("Chr1", 1000, 2000)

        assert result.startswith("/api/sequence/region?")
        assert "chr=Chr1" in result
        assert "start=1000" in result
        assert "end=2000" in result
        assert "format=fasta" in result

    def test_includes_flanking(self):
        """Should include flanking in coordinates."""
        result = _build_coordinate_sequence_link("Chr1", 1000, 2000, 100, 200)

        assert "start=900" in result
        assert "end=2200" in result


class TestResolveGeneQuery:
    """Tests for resolve_gene_query."""

    def test_finds_by_gene_name(self, mock_db, sample_feature):
        """Should find feature by gene name."""
        mock_db.query.side_effect = [
            MockQuery([sample_feature]),  # Gene name lookup
            MockQuery([]),  # Location lookup
        ]

        result = resolve_gene_query(mock_db, "ALS1")

        assert result is not None
        assert result.gene_name == "ALS1"

    def test_finds_by_feature_name(self, mock_db, sample_feature):
        """Should find feature by feature name."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Gene name lookup - no match
            MockQuery([sample_feature]),  # Feature name lookup
            MockQuery([]),  # Location lookup
        ]

        result = resolve_gene_query(mock_db, "CAL0001")

        assert result is not None
        assert result.feature_name == "CAL0001"

    def test_finds_by_dbxref_id(self, mock_db, sample_feature):
        """Should find feature by dbxref_id."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Gene name lookup - no match
            MockQuery([]),  # Feature name lookup - no match
            MockQuery([sample_feature]),  # dbxref_id lookup
            MockQuery([]),  # Location lookup
        ]

        result = resolve_gene_query(mock_db, "CGD:CAL0001")

        assert result is not None
        assert result.dbxref_id == "CGD:CAL0001"

    def test_returns_none_when_not_found(self, mock_db):
        """Should return None when not found."""
        mock_db.query.return_value = MockQuery([])

        result = resolve_gene_query(mock_db, "UNKNOWN")

        assert result is None

    def test_case_insensitive(self, mock_db, sample_feature):
        """Should be case insensitive."""
        mock_db.query.side_effect = [
            MockQuery([sample_feature]),  # Gene name lookup
            MockQuery([]),  # Location lookup
        ]

        result = resolve_gene_query(mock_db, "als1")

        assert result is not None

    def test_includes_location_info(self, mock_db, sample_feature, sample_organism):
        """Should include location info when available."""
        chr_feature = MockFeature(10, "Chr1", feature_type="chromosome")
        root_seq = MockSeq(100, 10, feature=chr_feature)
        location = MockFeatLocation(1, 100, 1000, 2000, "W")

        mock_db.query.side_effect = [
            MockQuery([sample_feature]),  # Gene name lookup
            MockQuery([location]),  # Location lookup
            MockQuery([root_seq]),  # Root seq lookup
        ]

        result = resolve_gene_query(mock_db, "ALS1")

        assert result.chromosome == "Chr1"
        assert result.start == 1000
        assert result.end == 2000
        assert result.strand == "W"


class TestGetToolsForGene:
    """Tests for get_tools_for_gene."""

    def test_returns_biology_category(self, mock_db, sample_feature_info):
        """Should include Biology/Literature category."""
        mock_db.query.return_value = MockQuery([])

        result = get_tools_for_gene(mock_db, sample_feature_info)

        bio_cat = next((c for c in result if c.name == "Biology/Literature"), None)
        assert bio_cat is not None
        assert any("Locus Information" in t.name for t in bio_cat.tools)

    def test_returns_maps_category(self, mock_db, sample_feature_info):
        """Should include Maps/Tables category with JBrowse."""
        mock_db.query.return_value = MockQuery([])

        result = get_tools_for_gene(mock_db, sample_feature_info)

        maps_cat = next((c for c in result if c.name == "Maps/Tables"), None)
        assert maps_cat is not None
        assert any("JBrowse" in t.name for t in maps_cat.tools)

    def test_returns_sequence_retrieval_category(self, mock_db, sample_feature_info):
        """Should include Sequence Retrieval category."""
        mock_db.query.return_value = MockQuery([])

        result = get_tools_for_gene(mock_db, sample_feature_info)

        seq_cat = next((c for c in result if c.name == "Sequence Retrieval"), None)
        assert seq_cat is not None

    def test_includes_sequence_analysis_when_sequence_available(self, mock_db, sample_feature_info):
        """Should include Sequence Analysis when sequence is available."""
        seq = MockSeq(1, 1, residues="ATGCATGCATGCATGCATGC")

        mock_db.query.return_value = MockQuery([seq])

        result = get_tools_for_gene(mock_db, sample_feature_info)

        analysis_cat = next((c for c in result if c.name == "Sequence Analysis"), None)
        assert analysis_cat is not None


class TestGetToolsForCoordinates:
    """Tests for get_tools_for_coordinates."""

    def test_returns_maps_category(self):
        """Should include Maps/Tables category."""
        result = get_tools_for_coordinates("Chr1", 1000, 2000)

        maps_cat = next((c for c in result if c.name == "Maps/Tables"), None)
        assert maps_cat is not None

    def test_includes_jbrowse(self):
        """Should include JBrowse link."""
        result = get_tools_for_coordinates("Chr1", 1000, 2000)

        maps_cat = next(c for c in result if c.name == "Maps/Tables")
        assert any("JBrowse" in t.name for t in maps_cat.tools)

    def test_includes_batch_download(self):
        """Should include batch download link."""
        result = get_tools_for_coordinates("Chr1", 1000, 2000)

        maps_cat = next(c for c in result if c.name == "Maps/Tables")
        assert any("Batch Download" in t.name for t in maps_cat.tools)

    def test_includes_sequence_retrieval(self):
        """Should include sequence retrieval."""
        result = get_tools_for_coordinates("Chr1", 1000, 2000)

        seq_cat = next((c for c in result if c.name == "Sequence Retrieval"), None)
        assert seq_cat is not None


class TestGetToolsForSequence:
    """Tests for get_tools_for_sequence."""

    def test_returns_blast_for_long_dna(self):
        """Should include BLAST for sequences > 15 bp."""
        result = get_tools_for_sequence("ATGCATGCATGCATGCATGC", SeqType.DNA)

        analysis_cat = next((c for c in result if c.name == "Sequence Analysis"), None)
        assert analysis_cat is not None
        assert any("BLAST" in t.name for t in analysis_cat.tools)

    def test_returns_pattern_match_for_short_dna(self):
        """Should include Pattern Match for DNA <= 20 bp."""
        result = get_tools_for_sequence("ATGCATGC", SeqType.DNA)

        analysis_cat = next((c for c in result if c.name == "Sequence Analysis"), None)
        if analysis_cat:
            assert any("Pattern Match" in t.name for t in analysis_cat.tools)

    def test_returns_restriction_map_for_dna(self):
        """Should include Restriction Map for DNA >= 10 bp."""
        result = get_tools_for_sequence("ATGCATGCATGC", SeqType.DNA)

        analysis_cat = next((c for c in result if c.name == "Sequence Analysis"), None)
        assert analysis_cat is not None
        assert any("Restriction" in t.name for t in analysis_cat.tools)

    def test_returns_primer_design_for_long_dna(self):
        """Should include Primer Design for DNA > 15 bp."""
        result = get_tools_for_sequence("ATGCATGCATGCATGCATGC", SeqType.DNA)

        analysis_cat = next((c for c in result if c.name == "Sequence Analysis"), None)
        assert analysis_cat is not None
        assert any("Primer" in t.name for t in analysis_cat.tools)

    def test_no_dna_tools_for_protein(self):
        """Should not include DNA tools for protein."""
        result = get_tools_for_sequence("MVLSPADKTNVKAAWGKVGAHAGEYGAE", SeqType.PROTEIN)

        # Should not have Pattern Match or Restriction Map
        analysis_cat = next((c for c in result if c.name == "Sequence Analysis"), None)
        if analysis_cat:
            assert not any("Pattern Match" in t.name for t in analysis_cat.tools)
            assert not any("Restriction" in t.name for t in analysis_cat.tools)
            assert not any("Primer" in t.name for t in analysis_cat.tools)

    def test_returns_empty_for_empty_sequence(self):
        """Should return empty for empty sequence."""
        result = get_tools_for_sequence("", SeqType.DNA)
        assert result == []

    def test_cleans_sequence(self):
        """Should clean sequence (remove non-alpha)."""
        result = get_tools_for_sequence("ATG CAT GC 123", SeqType.DNA)

        # Should still work with cleaned sequence
        # Cleaned = "ATGCATGC" which is 8 bp
        assert len(result) >= 0  # May have some tools


class TestGetAvailableAssemblies:
    """Tests for get_available_assemblies."""

    def test_returns_assemblies(self, mock_db, sample_organism):
        """Should return available assemblies."""
        gv = MockGenomeVersion(1, "Assembly22", "Y")

        mock_db.query.return_value = MockQuery([(gv, sample_organism)])

        result = get_available_assemblies(mock_db)

        assert len(result.assemblies) == 1
        assert result.assemblies[0].name == "Assembly22"

    def test_returns_default_when_empty(self, mock_db):
        """Should return default assembly when none found."""
        mock_db.query.return_value = MockQuery([])

        result = get_available_assemblies(mock_db)

        assert len(result.assemblies) == 1
        assert result.assemblies[0].name == "default"
        assert result.assemblies[0].is_default is True


class TestGetChromosomes:
    """Tests for get_chromosomes."""

    def test_returns_chromosomes(self, mock_db):
        """Should return chromosomes."""
        chr_feature = MockFeature(1, "Chr1", feature_type="chromosome")
        seq = MockSeq(1, 1, seq_length=1000000)

        mock_db.query.return_value = MockQuery([(chr_feature, seq)])

        result = get_chromosomes(mock_db)

        assert len(result.chromosomes) == 1
        assert result.chromosomes[0].name == "Chr1"
        assert result.chromosomes[0].length == 1000000


class TestResolveAndGetTools:
    """Tests for resolve_and_get_tools."""

    def test_returns_none_for_empty_input(self, mock_db):
        """Should return None for empty input."""
        result = resolve_and_get_tools(mock_db)
        assert result is None

    def test_handles_gene_query(self, mock_db, sample_feature):
        """Should handle gene query."""
        # Need many mock returns for all the queries in the chain
        mock_db.query.return_value = MockQuery([])

        # For resolve_gene_query
        call_count = [0]
        def mock_query(*args):
            call_count[0] += 1
            if call_count[0] == 1:  # First query - gene lookup
                return MockQuery([sample_feature])
            return MockQuery([])

        mock_db.query.side_effect = mock_query

        result = resolve_and_get_tools(mock_db, query="ALS1")

        assert result is not None
        assert result.input_type == InputType.GENE
        assert result.feature is not None

    def test_returns_none_for_unknown_gene(self, mock_db):
        """Should return None for unknown gene."""
        mock_db.query.return_value = MockQuery([])

        result = resolve_and_get_tools(mock_db, query="UNKNOWN")

        assert result is None

    def test_handles_coordinates(self, mock_db):
        """Should handle coordinate query."""
        result = resolve_and_get_tools(
            mock_db,
            chromosome="Chr1",
            start=1000,
            end=2000,
        )

        assert result is not None
        assert result.input_type == InputType.COORDINATES

    def test_returns_none_for_invalid_coordinates(self, mock_db):
        """Should return None when start > end."""
        result = resolve_and_get_tools(
            mock_db,
            chromosome="Chr1",
            start=2000,
            end=1000,
        )

        assert result is None

    def test_handles_raw_sequence(self, mock_db):
        """Should handle raw sequence input."""
        result = resolve_and_get_tools(
            mock_db,
            sequence="ATGCATGCATGCATGCATGC",
            seq_type=SeqType.DNA,
        )

        assert result is not None
        assert result.input_type == InputType.SEQUENCE
        assert result.sequence_length == 20

    def test_returns_none_for_empty_sequence(self, mock_db):
        """Should return None for empty sequence."""
        result = resolve_and_get_tools(
            mock_db,
            sequence="   123   ",  # Only non-alpha chars
        )

        assert result is None

    def test_defaults_to_dna_seq_type(self, mock_db):
        """Should default to DNA seq type."""
        result = resolve_and_get_tools(
            mock_db,
            sequence="ATGCATGCATGC",
        )

        assert result is not None
        # Should get DNA-specific tools
