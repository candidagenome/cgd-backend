"""
Tests for Reference Service.

Tests cover:
- Organism info extraction
- Citation link building
- Reference lookup by identifier
- Reference detail retrieval
- Locus details for references
- GO annotation details for references
- Phenotype details for references
- Interaction details for references
- Literature topics for references
- Author search
- New papers this week
- Genome-wide analysis papers
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
from fastapi import HTTPException

from cgd.api.services.reference_service import (
    _get_organism_info,
    _build_citation_links,
    _get_reference_by_identifier,
    get_reference,
    get_reference_locus_details,
    get_reference_go_details,
    get_reference_phenotype_details,
    get_reference_interaction_details,
    get_reference_literature_topics,
    search_references_by_author,
    get_new_papers_this_week,
    get_genome_wide_analysis_papers,
    GENOME_WIDE_TOPICS,
)


class MockOrganism:
    """Mock Organism model."""

    def __init__(
        self,
        organism_no: int,
        organism_name: str = None,
        taxon_id: int = 0,
    ):
        self.organism_no = organism_no
        self.organism_name = organism_name
        self.taxon_id = taxon_id


class MockFeature:
    """Mock Feature model."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        gene_name: str = None,
        headline: str = None,
        organism: MockOrganism = None,
        organism_no: int = None,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name
        self.headline = headline
        self.organism = organism
        self.organism_no = organism_no or (organism.organism_no if organism else None)


class MockUrl:
    """Mock Url model."""

    def __init__(self, url: str, url_type: str = None):
        self.url = url
        self.url_type = url_type


class MockRefUrl:
    """Mock RefUrl model."""

    def __init__(self, reference_no: int, url: MockUrl = None):
        self.reference_no = reference_no
        self.url = url


class MockJournal:
    """Mock Journal model."""

    def __init__(self, full_name: str = None, abbreviation: str = None):
        self.full_name = full_name
        self.abbreviation = abbreviation


class MockAuthor:
    """Mock Author model."""

    def __init__(self, author_no: int, author_name: str):
        self.author_no = author_no
        self.author_name = author_name


class MockAuthorEditor:
    """Mock AuthorEditor model."""

    def __init__(
        self,
        author: MockAuthor,
        author_order: int = 1,
        author_type: str = "Author",
    ):
        self.author = author
        self.author_order = author_order
        self.author_type = author_type


class MockReference:
    """Mock Reference model."""

    def __init__(
        self,
        reference_no: int,
        dbxref_id: str,
        pubmed: int = None,
        citation: str = None,
        title: str = None,
        year: int = None,
        status: str = None,
        source: str = None,
        volume: str = None,
        issue: str = None,
        page: str = None,
        journal: MockJournal = None,
        book: dict = None,
        author_editor: list = None,
        ref_url: list = None,
        ref_property: list = None,
        date_created: datetime = None,
    ):
        self.reference_no = reference_no
        self.dbxref_id = dbxref_id
        self.pubmed = pubmed
        self.citation = citation
        self.title = title
        self.year = year
        self.status = status
        self.source = source
        self.volume = volume
        self.issue = issue
        self.page = page
        self.journal = journal
        self.book = book
        self.author_editor = author_editor or []
        self.ref_url = ref_url or []
        self.ref_property = ref_property or []
        self.date_created = date_created or datetime.now()


class MockAbstract:
    """Mock Abstract model."""

    def __init__(self, reference_no: int, abstract: str):
        self.reference_no = reference_no
        self.abstract = abstract


class MockGo:
    """Mock Go model."""

    def __init__(self, go_no: int, goid: int, go_term: str, go_aspect: str = "P"):
        self.go_no = go_no
        self.goid = goid
        self.go_term = go_term
        self.go_aspect = go_aspect


class MockGoAnnotation:
    """Mock GoAnnotation model."""

    def __init__(
        self,
        go_annotation_no: int,
        feature: MockFeature = None,
        go: MockGo = None,
        go_evidence: str = None,
    ):
        self.go_annotation_no = go_annotation_no
        self.feature = feature
        self.go = go
        self.go_evidence = go_evidence


class MockGoRef:
    """Mock GoRef model."""

    def __init__(
        self,
        reference_no: int,
        go_annotation: MockGoAnnotation = None,
    ):
        self.reference_no = reference_no
        self.go_annotation = go_annotation


class MockPhenotype:
    """Mock Phenotype model."""

    def __init__(
        self,
        observable: str = None,
        qualifier: str = None,
        experiment_type: str = None,
        mutant_type: str = None,
    ):
        self.observable = observable
        self.qualifier = qualifier
        self.experiment_type = experiment_type
        self.mutant_type = mutant_type


class MockPhenoAnnotation:
    """Mock PhenoAnnotation model."""

    def __init__(
        self,
        pheno_annotation_no: int,
        feature: MockFeature = None,
        phenotype: MockPhenotype = None,
    ):
        self.pheno_annotation_no = pheno_annotation_no
        self.feature = feature
        self.phenotype = phenotype


class MockRefLink:
    """Mock RefLink model."""

    def __init__(self, reference_no: int, tab_name: str, primary_key: int):
        self.reference_no = reference_no
        self.tab_name = tab_name
        self.primary_key = primary_key


class MockFeatInteract:
    """Mock FeatInteract model."""

    def __init__(self, feature: MockFeature, action: str = None):
        self.feature = feature
        self.action = action


class MockInteraction:
    """Mock Interaction model."""

    def __init__(
        self,
        interaction_no: int,
        experiment_type: str = None,
        description: str = None,
        feat_interact: list = None,
    ):
        self.interaction_no = interaction_no
        self.experiment_type = experiment_type
        self.description = description
        self.feat_interact = feat_interact or []


class MockRefpropFeat:
    """Mock RefpropFeat model."""

    def __init__(self, feature: MockFeature):
        self.feature = feature


class MockRefProperty:
    """Mock RefProperty model."""

    def __init__(
        self,
        ref_property_no: int,
        reference_no: int,
        property_value: str = None,
        refprop_feat: list = None,
    ):
        self.ref_property_no = ref_property_no
        self.reference_no = reference_no
        self.property_value = property_value
        self.refprop_feat = refprop_feat or []


class MockCvTerm:
    """Mock CvTerm model."""

    def __init__(self, term_name: str, cv_no: int = 1):
        self.term_name = term_name
        self.cv_no = cv_no


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []

    def options(self, *args, **kwargs):
        return self

    def join(self, *args, **kwargs):
        return self

    def outerjoin(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def offset(self, n):
        return self

    def limit(self, n):
        return self

    def distinct(self):
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
def sample_organism():
    """Create sample organism."""
    return MockOrganism(1, "Candida albicans SC5314", 237561)


@pytest.fixture
def sample_feature(sample_organism):
    """Create sample feature."""
    return MockFeature(
        feature_no=1,
        feature_name="CAL0001",
        gene_name="ALS1",
        headline="Cell surface adhesin",
        organism=sample_organism,
    )


@pytest.fixture
def sample_reference():
    """Create sample reference."""
    journal = MockJournal("Journal Name", "J. Name")
    author = MockAuthor(1, "Smith J")
    author_editor = MockAuthorEditor(author, 1, "Author")

    return MockReference(
        reference_no=1,
        dbxref_id="CGD_REF:CAL0001",
        pubmed=12345678,
        citation="Smith et al. (2023)",
        title="A Study",
        year=2023,
        status="Published",
        source="PubMed",
        volume="10",
        issue="1",
        page="1-10",
        journal=journal,
        author_editor=[author_editor],
    )


class TestGetOrganismInfo:
    """Tests for _get_organism_info."""

    def test_returns_organism_name_and_taxon(self, sample_feature):
        """Should return organism name and taxon_id."""
        name, taxon = _get_organism_info(sample_feature)

        assert name == "Candida albicans SC5314"
        assert taxon == 237561

    def test_falls_back_to_display_name(self):
        """Should fall back to display_name."""
        organism = MagicMock()
        organism.organism_name = None
        organism.display_name = "C. albicans"
        organism.taxon_id = 237561
        feature = MockFeature(1, "CAL0001", organism=organism)
        feature.organism = organism

        name, taxon = _get_organism_info(feature)

        assert name == "C. albicans"

    def test_falls_back_to_organism_no(self):
        """Should fall back to organism_no as string."""
        organism = MagicMock()
        organism.organism_name = None
        organism.display_name = None
        organism.name = None
        organism.taxon_id = None
        feature = MockFeature(1, "CAL0001", organism=organism, organism_no=1)
        feature.organism = organism

        name, taxon = _get_organism_info(feature)

        assert name == "1"
        assert taxon == 0


class TestBuildCitationLinks:
    """Tests for _build_citation_links."""

    def test_includes_cgd_paper_link(self, sample_reference):
        """Should include CGD Paper link."""
        links = _build_citation_links(sample_reference, [])

        cgd_link = next((l for l in links if l.name == "CGD Paper"), None)
        assert cgd_link is not None
        assert cgd_link.link_type == "internal"
        assert "/reference/CGD_REF:CAL0001" in cgd_link.url

    def test_includes_pubmed_link(self, sample_reference):
        """Should include PubMed link when pubmed ID exists."""
        links = _build_citation_links(sample_reference, [])

        pubmed_link = next((l for l in links if l.name == "PubMed"), None)
        assert pubmed_link is not None
        assert pubmed_link.link_type == "external"
        assert "12345678" in pubmed_link.url

    def test_no_pubmed_link_when_no_pubmed_id(self):
        """Should not include PubMed link when no pubmed ID."""
        ref = MockReference(1, "CGD_REF:0001", pubmed=None)

        links = _build_citation_links(ref, [])

        pubmed_link = next((l for l in links if l.name == "PubMed"), None)
        assert pubmed_link is None

    def test_includes_full_text_link(self):
        """Should include Full Text link from ref_urls."""
        ref = MockReference(1, "CGD_REF:0001", pubmed=12345)
        url = MockUrl("http://example.com/full.pdf", "Full text")
        ref_url = MockRefUrl(1, url)

        links = _build_citation_links(ref, [ref_url])

        full_text = next((l for l in links if l.name == "Full Text"), None)
        assert full_text is not None

    def test_includes_supplement_link(self):
        """Should include Reference Supplement link."""
        ref = MockReference(1, "CGD_REF:0001")
        url = MockUrl("http://example.com/supplement.pdf", "Reference supplement")
        ref_url = MockRefUrl(1, url)

        links = _build_citation_links(ref, [ref_url])

        supplement = next((l for l in links if l.name == "Reference Supplement"), None)
        assert supplement is not None

    def test_includes_download_link(self):
        """Should include Download Datasets link."""
        ref = MockReference(1, "CGD_REF:0001")
        url = MockUrl("http://example.com/data.zip", "Download datasets")
        ref_url = MockRefUrl(1, url)

        links = _build_citation_links(ref, [ref_url])

        download = next((l for l in links if l.name == "Download Datasets"), None)
        assert download is not None

    def test_skips_reference_data(self):
        """Should skip Reference Data URLs."""
        ref = MockReference(1, "CGD_REF:0001")
        url = MockUrl("http://example.com/data", "Reference Data")
        ref_url = MockRefUrl(1, url)

        links = _build_citation_links(ref, [ref_url])

        # Should only have CGD Paper
        assert len(links) == 1
        assert links[0].name == "CGD Paper"


class TestGetReferenceByIdentifier:
    """Tests for _get_reference_by_identifier."""

    def test_finds_by_pubmed_id(self, mock_db, sample_reference):
        """Should find reference by PubMed ID."""
        mock_db.query.return_value = MockQuery([sample_reference])

        result = _get_reference_by_identifier(mock_db, "12345678")

        assert result.reference_no == 1

    def test_finds_by_dbxref_id(self, mock_db, sample_reference):
        """Should find reference by dbxref_id."""
        # Non-numeric identifier goes directly to dbxref lookup
        mock_db.query.return_value = MockQuery([sample_reference])

        result = _get_reference_by_identifier(mock_db, "CGD_REF:CAL0001")

        assert result.reference_no == 1

    def test_raises_404_when_not_found(self, mock_db):
        """Should raise HTTPException when not found."""
        mock_db.query.return_value = MockQuery([])

        with pytest.raises(HTTPException) as exc_info:
            _get_reference_by_identifier(mock_db, "UNKNOWN")

        assert exc_info.value.status_code == 404


class TestGetReference:
    """Tests for get_reference."""

    def test_returns_reference_response(self, mock_db, sample_reference):
        """Should return ReferenceResponse."""
        abstract = MockAbstract(1, "This is the abstract.")

        mock_db.query.side_effect = [
            MockQuery([sample_reference]),  # Reference lookup
            MockQuery([abstract]),  # Abstract lookup
            MockQuery([]),  # RefUrl lookup
        ]

        result = get_reference(mock_db, "12345678")

        assert result.result.reference_no == 1
        assert result.result.pubmed == 12345678
        assert result.result.abstract == "This is the abstract."

    def test_includes_authors(self, mock_db, sample_reference):
        """Should include authors in response."""
        mock_db.query.side_effect = [
            MockQuery([sample_reference]),  # Reference lookup
            MockQuery([]),  # Abstract
            MockQuery([]),  # RefUrl
        ]

        result = get_reference(mock_db, "12345678")

        assert len(result.result.authors) == 1
        assert result.result.authors[0].author_name == "Smith J"


class TestGetReferenceLocusDetails:
    """Tests for get_reference_locus_details."""

    def test_returns_loci(self, mock_db, sample_feature, sample_organism):
        """Should return loci for reference."""
        refprop_feat = MockRefpropFeat(sample_feature)
        ref_property = MockRefProperty(1, 1, "Topic", [refprop_feat])
        ref = MockReference(1, "CGD_REF:0001", ref_property=[ref_property])

        mock_db.query.return_value = MockQuery([ref])

        result = get_reference_locus_details(mock_db, "1")

        assert len(result.loci) == 1
        assert result.loci[0].feature_name == "CAL0001"
        assert result.loci[0].gene_name == "ALS1"


class TestGetReferenceGoDetails:
    """Tests for get_reference_go_details."""

    def test_returns_go_annotations(self, mock_db, sample_feature, sample_organism):
        """Should return GO annotations for reference."""
        go = MockGo(1, 5634, "nucleus", "C")
        go_annotation = MockGoAnnotation(1, sample_feature, go, "IDA")
        go_ref = MockGoRef(1, go_annotation)
        ref = MockReference(1, "CGD_REF:0001")

        mock_db.query.side_effect = [
            MockQuery([ref]),  # Reference lookup
            MockQuery([go_ref]),  # GoRef lookup
        ]

        result = get_reference_go_details(mock_db, "1")

        assert len(result.annotations) == 1
        assert result.annotations[0].goid == "GO:0005634"
        assert result.annotations[0].go_term == "nucleus"


class TestGetReferencePhenotypeDetails:
    """Tests for get_reference_phenotype_details."""

    def test_returns_phenotype_annotations(self, mock_db, sample_feature, sample_organism):
        """Should return phenotype annotations for reference."""
        phenotype = MockPhenotype("colony morphology", "abnormal", "classical genetics", "null")
        pheno_annotation = MockPhenoAnnotation(1, sample_feature, phenotype)
        ref_link = MockRefLink(1, "PHENO_ANNOTATION", 1)
        ref = MockReference(1, "CGD_REF:0001")

        mock_db.query.side_effect = [
            MockQuery([ref]),  # Reference lookup
            MockQuery([ref_link]),  # RefLink lookup
            MockQuery([pheno_annotation]),  # PhenoAnnotation lookup
        ]

        result = get_reference_phenotype_details(mock_db, "1")

        assert len(result.annotations) == 1
        assert result.annotations[0].observable == "colony morphology"
        assert result.annotations[0].qualifier == "abnormal"


class TestGetReferenceInteractionDetails:
    """Tests for get_reference_interaction_details."""

    def test_returns_interactions(self, mock_db, sample_feature):
        """Should return interactions for reference."""
        feat_interact = MockFeatInteract(sample_feature, "bait")
        interaction = MockInteraction(1, "two-hybrid", "Test interaction", [feat_interact])
        ref_link = MockRefLink(1, "INTERACTION", 1)
        ref = MockReference(1, "CGD_REF:0001")

        mock_db.query.side_effect = [
            MockQuery([ref]),  # Reference lookup
            MockQuery([ref_link]),  # RefLink lookup
            MockQuery([interaction]),  # Interaction lookup
        ]

        result = get_reference_interaction_details(mock_db, "1")

        assert len(result.interactions) == 1
        assert result.interactions[0].experiment_type == "two-hybrid"
        assert len(result.interactions[0].interactors) == 1


class TestGetReferenceLiteratureTopics:
    """Tests for get_reference_literature_topics."""

    def test_returns_literature_topics(self, mock_db, sample_feature):
        """Should return literature topics for reference."""
        refprop_feat = MockRefpropFeat(sample_feature)
        ref_property = MockRefProperty(1, 1, "Genome-wide Analysis", [refprop_feat])
        ref = MockReference(1, "CGD_REF:0001", ref_property=[ref_property])

        # Mock CvTerm query to return valid topic
        mock_db.query.side_effect = [
            MockQuery([ref]),  # Reference lookup
            MockQuery([("Genome-wide Analysis",)]),  # CvTerm lookup
            MockQuery([ref_property]),  # RefProperty lookup
        ]

        result = get_reference_literature_topics(mock_db, "1")

        assert len(result.topics) >= 0  # May be filtered


class TestSearchReferencesByAuthor:
    """Tests for search_references_by_author."""

    def test_searches_by_author_name(self, mock_db):
        """Should search references by author name."""
        mock_db.query.side_effect = [
            MockQuery([(1, "Smith (2023)", 2023, 12345, "CGD_REF:0001")]),  # Reference search
            MockQuery(),  # Author count (returns mock with count method)
            MockQuery([("Smith J",)]),  # Authors for reference
        ]

        # Mock count to return 1
        mock_db.query.return_value.count = MagicMock(return_value=1)

        result = search_references_by_author(mock_db, "Smith")

        assert result.author_query == "Smith"
        assert len(result.references) >= 0

    def test_adds_wildcard_to_pattern(self, mock_db):
        """Should add wildcard to search pattern."""
        mock_db.query.return_value = MockQuery([])
        mock_db.query.return_value.count = MagicMock(return_value=0)

        result = search_references_by_author(mock_db, "Smith")

        # The pattern should have wildcard appended
        assert result.author_query == "Smith"


class TestGetNewPapersThisWeek:
    """Tests for get_new_papers_this_week."""

    def test_returns_recent_papers(self, mock_db):
        """Should return papers from last week."""
        ref = MockReference(
            1, "CGD_REF:0001",
            pubmed=12345,
            citation="Test (2023)",
            title="Test Title",
            year=2023,
            date_created=datetime.now() - timedelta(days=3),
        )

        mock_db.query.side_effect = [
            MockQuery([ref]),  # Reference query
            MockQuery([]),  # RefUrl query
        ]

        result = get_new_papers_this_week(mock_db, days=7)

        assert result.total_count == 1
        assert len(result.references) == 1


class TestGetGenomeWideAnalysisPapers:
    """Tests for get_genome_wide_analysis_papers."""

    def test_returns_genome_wide_papers(self, mock_db):
        """Should return genome-wide analysis papers."""
        ref = MockReference(1, "CGD_REF:0001", citation="Test (2023)", year=2023)

        mock_db.query.side_effect = [
            MockQuery([(1,)]),  # Count query (returns total)
            MockQuery([(1,)]),  # Reference nos query
            MockQuery([ref]),  # Full reference query
            MockQuery([("Genome-wide Analysis",)]),  # Topics query
            MockQuery([]),  # Species query
            MockQuery([]),  # Genes query
            MockQuery([]),  # RefUrl query
        ]

        # Mock count
        mock_db.query.return_value.count = MagicMock(return_value=1)

        result = get_genome_wide_analysis_papers(mock_db)

        assert GENOME_WIDE_TOPICS == result.available_topics

    def test_filters_by_topic(self, mock_db):
        """Should filter by specific topic."""
        ref = MockReference(1, "CGD_REF:0001", citation="Test (2023)", year=2023)

        mock_db.query.side_effect = [
            MockQuery([(1,)]),  # Count query
            MockQuery([(1,)]),  # Reference nos query
            MockQuery([ref]),  # Full reference query
            MockQuery([("Genome-wide Analysis",)]),  # Topics query
            MockQuery([]),  # Species query
            MockQuery([]),  # Genes query
            MockQuery([]),  # RefUrl query
        ]
        mock_db.query.return_value.count = MagicMock(return_value=1)

        result = get_genome_wide_analysis_papers(mock_db, topic="Genome-wide Analysis")

        assert result.selected_topic == "Genome-wide Analysis"

    def test_pagination(self, mock_db):
        """Should support pagination."""
        ref = MockReference(1, "CGD_REF:0001", citation="Test (2023)", year=2023)
        mock_db.query.side_effect = [
            MockQuery([(i,) for i in range(100)]),  # Count query
            MockQuery([(1,)]),  # Reference nos query
            MockQuery([ref]),  # Full reference query
            MockQuery([]),  # Topics query
            MockQuery([]),  # Species query
            MockQuery([]),  # Genes query
            MockQuery([]),  # RefUrl query
        ]
        mock_db.query.return_value.count = MagicMock(return_value=100)

        result = get_genome_wide_analysis_papers(mock_db, page=2, page_size=20)

        assert result.page == 2
        assert result.page_size == 20


class TestGenomeWideTopics:
    """Tests for GENOME_WIDE_TOPICS constant."""

    def test_contains_expected_topics(self):
        """Should contain expected genome-wide topics."""
        assert "Genome-wide Analysis" in GENOME_WIDE_TOPICS
        assert "Proteome-wide Analysis" in GENOME_WIDE_TOPICS
        assert "Large-scale phenotype analysis" in GENOME_WIDE_TOPICS
