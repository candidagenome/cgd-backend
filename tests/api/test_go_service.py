"""
Tests for GO Service.

Tests cover:
- GOID formatting and parsing
- Organism name retrieval
- Annotation type normalization
- Species name abbreviation
- Citation link building
- Text capitalization
- GO term info retrieval
- GO evidence codes
- GO hierarchy
"""
import pytest
from unittest.mock import MagicMock, patch
from fastapi import HTTPException

from cgd.api.services.go_service import (
    _format_goid,
    _parse_goid,
    _get_organism_name,
    _normalize_annotation_type,
    _abbreviate_species,
    _build_citation_links,
    _uppercase_first_letters,
    get_go_term_info,
    get_go_evidence_codes,
    get_go_hierarchy,
    ASPECT_NAMES,
    ANNOTATION_TYPE_MAP,
    ANNOTATION_TYPE_LABELS,
)


class MockOrganism:
    """Mock Organism model."""

    def __init__(self, organism_no: int, organism_name: str = None):
        self.organism_no = organism_no
        self.organism_name = organism_name


class MockFeature:
    """Mock Feature model."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        gene_name: str = None,
        organism: MockOrganism = None,
        organism_no: int = None,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name
        self.organism = organism
        self.organism_no = organism_no or (organism.organism_no if organism else None)


class MockReference:
    """Mock Reference model."""

    def __init__(
        self,
        reference_no: int,
        dbxref_id: str,
        pubmed: int = None,
        citation: str = None,
    ):
        self.reference_no = reference_no
        self.dbxref_id = dbxref_id
        self.pubmed = pubmed
        self.citation = citation


class MockUrl:
    """Mock Url model."""

    def __init__(self, url: str, url_type: str = None):
        self.url = url
        self.url_type = url_type


class MockRefUrl:
    """Mock RefUrl model."""

    def __init__(self, reference_no: int, url: MockUrl):
        self.reference_no = reference_no
        self.url = url


class MockGo:
    """Mock Go model."""

    def __init__(
        self,
        go_no: int,
        goid: int,
        go_term: str,
        go_aspect: str = "P",
        go_definition: str = None,
    ):
        self.go_no = go_no
        self.goid = goid
        self.go_term = go_term
        self.go_aspect = go_aspect
        self.go_definition = go_definition


class MockGoSynonym:
    """Mock GoSynonym model."""

    def __init__(self, go_synonym: str):
        self.go_synonym = go_synonym


class MockGoGosyn:
    """Mock GoGosyn model."""

    def __init__(self, go_no: int, go_synonym: MockGoSynonym):
        self.go_no = go_no
        self.go_synonym = go_synonym


class MockGoQualifier:
    """Mock GoQualifier model."""

    def __init__(self, qualifier: str):
        self.qualifier = qualifier


class MockGoRef:
    """Mock GoRef model."""

    def __init__(
        self,
        reference: MockReference,
        go_qualifier: list = None,
        has_qualifier: str = "N",
    ):
        self.reference = reference
        self.go_qualifier = go_qualifier or []
        self.has_qualifier = has_qualifier


class MockGoAnnotation:
    """Mock GoAnnotation model."""

    def __init__(
        self,
        go_no: int,
        feature_no: int,
        feature: MockFeature = None,
        go_ref: list = None,
        annotation_type: str = None,
        go_evidence: str = None,
    ):
        self.go_no = go_no
        self.feature_no = feature_no
        self.feature = feature
        self.go_ref = go_ref or []
        self.annotation_type = annotation_type
        self.go_evidence = go_evidence


class MockGoPath:
    """Mock GoPath model."""

    def __init__(
        self,
        ancestor_go_no: int,
        child_go_no: int,
        generation: int,
        relationship_type: str = "is_a",
    ):
        self.ancestor_go_no = ancestor_go_no
        self.child_go_no = child_go_no
        self.generation = generation
        self.relationship_type = relationship_type


class MockCode:
    """Mock Code model."""

    def __init__(self, code_value: str, description: str = None):
        self.code_value = code_value
        self.description = description


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []

    def options(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def group_by(self, *args, **kwargs):
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
        organism=sample_organism,
    )


@pytest.fixture
def sample_reference():
    """Create sample reference."""
    return MockReference(
        reference_no=1,
        dbxref_id="CGD_REF:CAL0001",
        pubmed=12345678,
        citation="Author et al. (2023)",
    )


@pytest.fixture
def sample_go():
    """Create sample GO term."""
    return MockGo(
        go_no=1,
        goid=5634,
        go_term="nucleus",
        go_aspect="C",
        go_definition="A membrane-bounded organelle.",
    )


class TestFormatGoid:
    """Tests for _format_goid."""

    def test_formats_integer(self):
        """Should format integer GOID with padding."""
        assert _format_goid(5634) == "GO:0005634"

    def test_formats_small_integer(self):
        """Should format small GOID with leading zeros."""
        assert _format_goid(1) == "GO:0000001"

    def test_formats_large_integer(self):
        """Should format large GOID."""
        assert _format_goid(1234567) == "GO:1234567"

    def test_formats_string_number(self):
        """Should format string number."""
        assert _format_goid("5634") == "GO:0005634"

    def test_returns_already_formatted(self):
        """Should return already formatted GOID."""
        assert _format_goid("GO:0005634") == "GO:0005634"


class TestParseGoid:
    """Tests for _parse_goid."""

    def test_parses_full_format(self):
        """Should parse GO:XXXXXXX format."""
        assert _parse_goid("GO:0005634") == 5634

    def test_parses_lowercase(self):
        """Should parse lowercase go: prefix."""
        assert _parse_goid("go:0005634") == 5634

    def test_parses_padded_number(self):
        """Should parse padded number without prefix."""
        assert _parse_goid("0005634") == 5634

    def test_parses_plain_number(self):
        """Should parse plain number."""
        assert _parse_goid("5634") == 5634

    def test_strips_whitespace(self):
        """Should strip whitespace."""
        assert _parse_goid("  GO:0005634  ") == 5634

    def test_raises_on_invalid(self):
        """Should raise ValueError on invalid input."""
        with pytest.raises(ValueError):
            _parse_goid("invalid")


class TestGetOrganismName:
    """Tests for _get_organism_name."""

    def test_returns_organism_name(self, sample_feature):
        """Should return organism_name when available."""
        result = _get_organism_name(sample_feature)
        assert result == "Candida albicans SC5314"

    def test_falls_back_to_display_name(self):
        """Should fall back to display_name."""
        organism = MagicMock()
        organism.organism_name = None
        organism.display_name = "C. albicans"
        feature = MockFeature(1, "CAL0001", organism=organism)
        feature.organism = organism

        result = _get_organism_name(feature)
        assert result == "C. albicans"

    def test_falls_back_to_organism_no(self):
        """Should fall back to organism_no."""
        organism = MagicMock()
        organism.organism_name = None
        organism.display_name = None
        organism.name = None
        feature = MockFeature(1, "CAL0001", organism=organism, organism_no=1)
        feature.organism = organism

        result = _get_organism_name(feature)
        assert result == "1"

    def test_handles_no_organism(self):
        """Should handle feature with no organism."""
        feature = MockFeature(1, "CAL0001", organism=None, organism_no=1)

        result = _get_organism_name(feature)
        assert result == "1"


class TestNormalizeAnnotationType:
    """Tests for _normalize_annotation_type."""

    def test_normalizes_manually_curated(self):
        """Should normalize manually curated."""
        assert _normalize_annotation_type("manually curated") == "manually_curated"

    def test_normalizes_high_throughput(self):
        """Should normalize high-throughput."""
        assert _normalize_annotation_type("high-throughput") == "high_throughput"

    def test_normalizes_computational(self):
        """Should normalize computational."""
        assert _normalize_annotation_type("computational") == "computational"

    def test_returns_default_for_none(self):
        """Should return manually_curated for None."""
        assert _normalize_annotation_type(None) == "manually_curated"

    def test_returns_default_for_empty(self):
        """Should return manually_curated for empty string."""
        assert _normalize_annotation_type("") == "manually_curated"

    def test_handles_unknown_type(self):
        """Should handle unknown type by replacing spaces/hyphens."""
        assert _normalize_annotation_type("other type") == "other_type"


class TestAbbreviateSpecies:
    """Tests for _abbreviate_species."""

    def test_abbreviates_two_word_species(self):
        """Should abbreviate two-word species name."""
        assert _abbreviate_species("Candida albicans") == "C. albicans"

    def test_abbreviates_with_strain(self):
        """Should abbreviate species with strain."""
        assert _abbreviate_species("Candida albicans SC5314") == "C. albicans"

    def test_returns_single_word_unchanged(self):
        """Should return single word unchanged."""
        assert _abbreviate_species("Candida") == "Candida"

    def test_handles_empty_string(self):
        """Should handle empty string."""
        assert _abbreviate_species("") == ""

    def test_handles_none(self):
        """Should handle None."""
        assert _abbreviate_species(None) is None


class TestBuildCitationLinks:
    """Tests for _build_citation_links."""

    def test_includes_cgd_paper_link(self, sample_reference):
        """Should include CGD Paper link."""
        links = _build_citation_links(sample_reference)

        cgd_link = next((l for l in links if l.name == "CGD Paper"), None)
        assert cgd_link is not None
        assert cgd_link.link_type == "internal"
        assert "/reference/CGD_REF:CAL0001" in cgd_link.url

    def test_includes_pubmed_link(self, sample_reference):
        """Should include PubMed link when pubmed ID exists."""
        links = _build_citation_links(sample_reference)

        pubmed_link = next((l for l in links if l.name == "PubMed"), None)
        assert pubmed_link is not None
        assert pubmed_link.link_type == "external"
        assert "12345678" in pubmed_link.url

    def test_no_pubmed_link_when_no_pubmed_id(self):
        """Should not include PubMed link when no pubmed ID."""
        ref = MockReference(1, "CGD_REF:0001", pubmed=None)

        links = _build_citation_links(ref)

        pubmed_link = next((l for l in links if l.name == "PubMed"), None)
        assert pubmed_link is None

    def test_includes_full_text_link(self, sample_reference):
        """Should include Full Text link from ref_urls."""
        url = MockUrl("http://example.com/full.pdf", "Full text")
        ref_url = MockRefUrl(1, url)

        links = _build_citation_links(sample_reference, [ref_url])

        full_text = next((l for l in links if l.name == "Full Text"), None)
        assert full_text is not None
        assert full_text.url == "http://example.com/full.pdf"

    def test_includes_supplement_link(self, sample_reference):
        """Should include Reference Supplement link."""
        url = MockUrl("http://example.com/supplement.pdf", "Reference supplement")
        ref_url = MockRefUrl(1, url)

        links = _build_citation_links(sample_reference, [ref_url])

        supplement = next((l for l in links if l.name == "Reference Supplement"), None)
        assert supplement is not None

    def test_includes_download_link(self, sample_reference):
        """Should include Download Datasets link."""
        url = MockUrl("http://example.com/data.zip", "Download datasets")
        ref_url = MockRefUrl(1, url)

        links = _build_citation_links(sample_reference, [ref_url])

        download = next((l for l in links if l.name == "Download Datasets"), None)
        assert download is not None

    def test_skips_reference_data(self, sample_reference):
        """Should skip Reference Data URLs."""
        url = MockUrl("http://example.com/data", "Reference Data")
        ref_url = MockRefUrl(1, url)

        links = _build_citation_links(sample_reference, [ref_url])

        # Should only have CGD Paper and PubMed
        assert len(links) == 2


class TestUppercaseFirstLetters:
    """Tests for _uppercase_first_letters."""

    def test_capitalizes_words(self):
        """Should capitalize first letter of each word."""
        result = _uppercase_first_letters("hello world")
        assert result == "Hello World"

    def test_skips_common_words(self):
        """Should skip common words."""
        result = _uppercase_first_letters("inferred from sequence")
        assert result == "Inferred from Sequence"

    def test_skips_structural(self):
        """Should skip 'structural'."""
        result = _uppercase_first_letters("structural similarity")
        assert result == "structural Similarity"

    def test_handles_empty_string(self):
        """Should handle empty string."""
        result = _uppercase_first_letters("")
        assert result == ""

    def test_handles_none(self):
        """Should handle None."""
        result = _uppercase_first_letters(None)
        assert result is None


class TestAspectNames:
    """Tests for ASPECT_NAMES constant."""

    def test_cellular_component(self):
        """Should map C to Cellular Component."""
        assert ASPECT_NAMES["C"] == "Cellular Component"

    def test_molecular_function(self):
        """Should map F to Molecular Function."""
        assert ASPECT_NAMES["F"] == "Molecular Function"

    def test_biological_process(self):
        """Should map P to Biological Process."""
        assert ASPECT_NAMES["P"] == "Biological Process"


class TestAnnotationTypeMaps:
    """Tests for ANNOTATION_TYPE_MAP and ANNOTATION_TYPE_LABELS constants."""

    def test_annotation_type_map(self):
        """Should have correct annotation type mapping."""
        assert ANNOTATION_TYPE_MAP["manually curated"] == "manually_curated"
        assert ANNOTATION_TYPE_MAP["high-throughput"] == "high_throughput"
        assert ANNOTATION_TYPE_MAP["computational"] == "computational"

    def test_annotation_type_labels(self):
        """Should have correct annotation type labels."""
        assert ANNOTATION_TYPE_LABELS["manually_curated"] == "Manually Curated"
        assert ANNOTATION_TYPE_LABELS["high_throughput"] == "High-Throughput"
        assert ANNOTATION_TYPE_LABELS["computational"] == "Computational"


class TestGetGoTermInfo:
    """Tests for get_go_term_info."""

    def test_raises_on_invalid_goid(self, mock_db):
        """Should raise HTTPException on invalid GOID."""
        with pytest.raises(HTTPException) as exc_info:
            get_go_term_info(mock_db, "invalid")

        assert exc_info.value.status_code == 400
        assert "Invalid GO identifier" in exc_info.value.detail

    def test_raises_on_not_found(self, mock_db):
        """Should raise HTTPException when GO term not found."""
        mock_db.query.return_value = MockQuery([])

        with pytest.raises(HTTPException) as exc_info:
            get_go_term_info(mock_db, "GO:9999999")

        assert exc_info.value.status_code == 404
        assert "not found" in exc_info.value.detail

    def test_returns_term_info(self, mock_db, sample_go):
        """Should return GO term info."""
        mock_db.query.side_effect = [
            MockQuery([sample_go]),  # Go query
            MockQuery([]),  # GoGosyn query
            MockQuery([]),  # GoAnnotation query
        ]

        result = get_go_term_info(mock_db, "GO:0005634")

        assert result.term.goid == "GO:0005634"
        assert result.term.go_term == "nucleus"
        assert result.term.go_aspect == "C"
        assert result.term.aspect_name == "Cellular Component"

    def test_includes_synonyms(self, mock_db, sample_go):
        """Should include synonyms."""
        synonym = MockGoSynonym("cell nucleus")
        go_gosyn = MockGoGosyn(1, synonym)

        mock_db.query.side_effect = [
            MockQuery([sample_go]),  # Go query
            MockQuery([go_gosyn]),  # GoGosyn query
            MockQuery([]),  # GoAnnotation query
        ]

        result = get_go_term_info(mock_db, "GO:0005634")

        assert "cell nucleus" in result.term.synonyms

    def test_includes_annotations(self, mock_db, sample_go, sample_feature, sample_reference):
        """Should include annotations."""
        go_ref = MockGoRef(sample_reference, [], "N")
        annotation = MockGoAnnotation(
            go_no=1,
            feature_no=1,
            feature=sample_feature,
            go_ref=[go_ref],
            annotation_type="manually curated",
            go_evidence="IDA",
        )

        mock_db.query.side_effect = [
            MockQuery([sample_go]),  # Go query
            MockQuery([]),  # GoGosyn query
            MockQuery([annotation]),  # GoAnnotation query
            MockQuery([]),  # RefUrl query
        ]

        result = get_go_term_info(mock_db, "GO:0005634")

        assert result.total_genes >= 1


class TestGetGoEvidenceCodes:
    """Tests for get_go_evidence_codes."""

    def test_returns_evidence_codes(self, mock_db):
        """Should return evidence codes."""
        code = MockCode("IDA", "inferred from direct assay: example1; example2")

        mock_db.query.return_value = MockQuery([code])

        result = get_go_evidence_codes(mock_db)

        assert len(result.evidence_codes) == 1
        assert result.evidence_codes[0].code == "IDA"

    def test_parses_definition_and_examples(self, mock_db):
        """Should parse definition and examples from description."""
        code = MockCode("IMP", "inferred from mutant phenotype: gene deletion; overexpression")

        mock_db.query.return_value = MockQuery([code])

        result = get_go_evidence_codes(mock_db)

        evidence = result.evidence_codes[0]
        assert evidence.code == "IMP"
        assert "Mutant" in evidence.definition  # Capitalized
        assert len(evidence.examples) == 2

    def test_handles_empty_description(self, mock_db):
        """Should handle empty description."""
        code = MockCode("ND", None)

        mock_db.query.return_value = MockQuery([code])

        result = get_go_evidence_codes(mock_db)

        assert result.evidence_codes[0].code == "ND"
        assert result.evidence_codes[0].definition == ""
        assert result.evidence_codes[0].examples == []

    def test_handles_no_examples(self, mock_db):
        """Should handle description without examples."""
        code = MockCode("TAS", "traceable author statement")

        mock_db.query.return_value = MockQuery([code])

        result = get_go_evidence_codes(mock_db)

        assert result.evidence_codes[0].examples == []


class TestGetGoHierarchy:
    """Tests for get_go_hierarchy."""

    def test_raises_on_invalid_goid(self, mock_db):
        """Should raise HTTPException on invalid GOID."""
        with pytest.raises(HTTPException) as exc_info:
            get_go_hierarchy(mock_db, "invalid")

        assert exc_info.value.status_code == 400

    def test_raises_on_not_found(self, mock_db):
        """Should raise HTTPException when GO term not found."""
        mock_db.query.return_value = MockQuery([])

        with pytest.raises(HTTPException) as exc_info:
            get_go_hierarchy(mock_db, "GO:9999999")

        assert exc_info.value.status_code == 404

    def test_returns_focus_node(self, mock_db, sample_go):
        """Should return focus node."""
        mock_db.query.side_effect = [
            MockQuery([sample_go]),  # Go query
            MockQuery([]),  # Ancestor paths query
            MockQuery([]),  # Descendant paths query
            MockQuery([sample_go]),  # Go records query
            MockQuery([]),  # Annotation counts query
        ]

        result = get_go_hierarchy(mock_db, "GO:0005634")

        assert result.focus_term is not None
        assert result.focus_term.goid == "GO:0005634"
        assert result.focus_term.is_focus is True

    def test_includes_nodes(self, mock_db, sample_go):
        """Should include nodes in response."""
        mock_db.query.side_effect = [
            MockQuery([sample_go]),  # Go query
            MockQuery([]),  # Ancestor paths query
            MockQuery([]),  # Descendant paths query
            MockQuery([sample_go]),  # Go records query
            MockQuery([]),  # Annotation counts query
        ]

        result = get_go_hierarchy(mock_db, "GO:0005634")

        assert len(result.nodes) >= 1

    def test_includes_ancestor_nodes(self, mock_db, sample_go):
        """Should include ancestor nodes."""
        parent_go = MockGo(2, 5575, "cellular_component", "C")
        ancestor_path = MockGoPath(2, 1, 1, "is_a")

        mock_db.query.side_effect = [
            MockQuery([sample_go]),  # Go query
            MockQuery([ancestor_path]),  # Ancestor paths query
            MockQuery([]),  # Descendant paths query
            MockQuery([sample_go, parent_go]),  # Go records query
            MockQuery([]),  # Annotation counts query
            MockQuery([ancestor_path]),  # Inter-node paths query
        ]

        result = get_go_hierarchy(mock_db, "GO:0005634")

        assert len(result.nodes) == 2
        assert result.can_go_up is True

    def test_includes_edges(self, mock_db, sample_go):
        """Should include edges between nodes."""
        parent_go = MockGo(2, 5575, "cellular_component", "C")
        ancestor_path = MockGoPath(2, 1, 1, "is_a")

        mock_db.query.side_effect = [
            MockQuery([sample_go]),  # Go query
            MockQuery([ancestor_path]),  # Ancestor paths query
            MockQuery([]),  # Descendant paths query
            MockQuery([sample_go, parent_go]),  # Go records query
            MockQuery([]),  # Annotation counts query
            MockQuery([ancestor_path]),  # Inter-node paths query
        ]

        result = get_go_hierarchy(mock_db, "GO:0005634")

        assert len(result.edges) >= 1

    def test_includes_descendant_nodes(self, mock_db, sample_go):
        """Should include descendant nodes."""
        child_go = MockGo(3, 5640, "nucleolus", "C")
        descendant_path = MockGoPath(1, 3, 1, "is_a")

        mock_db.query.side_effect = [
            MockQuery([sample_go]),  # Go query
            MockQuery([]),  # Ancestor paths query
            MockQuery([descendant_path]),  # Descendant paths query
            MockQuery([sample_go, child_go]),  # Go records query
            MockQuery([]),  # Annotation counts query
            MockQuery([descendant_path]),  # Inter-node paths query
        ]

        result = get_go_hierarchy(mock_db, "GO:0005634")

        assert len(result.nodes) == 2
        assert result.can_go_down is True

    def test_limits_max_nodes(self, mock_db, sample_go):
        """Should limit to max_nodes."""
        # Create many ancestor paths
        ancestor_paths = [MockGoPath(i, 1, 1) for i in range(2, 50)]
        ancestor_gos = [MockGo(i, 1000 + i, f"term_{i}") for i in range(2, 50)]

        mock_db.query.side_effect = [
            MockQuery([sample_go]),  # Go query
            MockQuery(ancestor_paths),  # Ancestor paths query
            MockQuery([]),  # Descendant paths query
            MockQuery([sample_go] + ancestor_gos),  # Go records query
            MockQuery([]),  # Annotation counts query
            MockQuery([]),  # Inter-node paths query
        ]

        result = get_go_hierarchy(mock_db, "GO:0005634", max_nodes=10)

        assert len(result.nodes) <= 10

    def test_sets_navigation_flags(self, mock_db, sample_go):
        """Should set can_go_up and can_go_down flags."""
        mock_db.query.side_effect = [
            MockQuery([sample_go]),  # Go query
            MockQuery([]),  # Ancestor paths query
            MockQuery([]),  # Descendant paths query
            MockQuery([sample_go]),  # Go records query
            MockQuery([]),  # Annotation counts query
        ]

        result = get_go_hierarchy(mock_db, "GO:0005634")

        assert result.can_go_up is False
        assert result.can_go_down is False
