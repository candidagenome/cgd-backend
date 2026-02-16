"""
Tests for Elasticsearch Indexer Service.

Tests cover:
- Index creation and deletion
- GOID formatting
- Document generation for genes, GO terms, phenotypes, references
- Indexing functions
- Full index rebuild
"""
import pytest
from unittest.mock import MagicMock, patch, call

from cgd.api.services.es_indexer import (
    create_index,
    delete_index,
    _format_goid,
    _generate_gene_docs,
    _generate_go_docs,
    _generate_phenotype_docs,
    _generate_reference_docs,
    index_genes,
    index_go_terms,
    index_phenotypes,
    index_references,
    rebuild_index,
)
from cgd.core.elasticsearch import INDEX_NAME


class MockOrganism:
    """Mock Organism model."""

    def __init__(self, organism_no: int, organism_name: str):
        self.organism_no = organism_no
        self.organism_name = organism_name


class MockAlias:
    """Mock Alias model."""

    def __init__(self, alias_name: str):
        self.alias_name = alias_name


class MockFeatAlias:
    """Mock FeatAlias model."""

    def __init__(self, alias: MockAlias):
        self.alias = alias


class MockFeature:
    """Mock Feature model."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        gene_name: str = None,
        dbxref_id: str = None,
        headline: str = None,
        organism: MockOrganism = None,
        feat_alias: list = None,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name
        self.dbxref_id = dbxref_id
        self.headline = headline
        self.organism = organism
        self.feat_alias = feat_alias or []


class MockGo:
    """Mock Go model."""

    def __init__(
        self,
        go_no: int,
        goid: int,
        go_term: str,
        go_aspect: str = None,
        go_definition: str = None,
    ):
        self.go_no = go_no
        self.goid = goid
        self.go_term = go_term
        self.go_aspect = go_aspect
        self.go_definition = go_definition


class MockPhenotype:
    """Mock Phenotype model."""

    def __init__(self, phenotype_no: int, observable: str):
        self.phenotype_no = phenotype_no
        self.observable = observable


class MockReference:
    """Mock Reference model."""

    def __init__(
        self,
        reference_no: int,
        dbxref_id: str,
        pubmed: int = None,
        citation: str = None,
        year: int = None,
    ):
        self.reference_no = reference_no
        self.dbxref_id = dbxref_id
        self.pubmed = pubmed
        self.citation = citation
        self.year = year


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []

    def options(self, *args, **kwargs):
        return self

    def distinct(self):
        return self

    def all(self):
        return self._results


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    db = MagicMock()
    db.query.return_value = MockQuery([])
    return db


@pytest.fixture
def mock_es():
    """Create a mock Elasticsearch client."""
    es = MagicMock()
    es.indices.exists.return_value = False
    return es


@pytest.fixture
def sample_organism():
    """Create sample organism."""
    return MockOrganism(1, "Candida albicans SC5314")


@pytest.fixture
def sample_feature(sample_organism):
    """Create sample feature."""
    alias = MockAlias("ALS1_alias")
    feat_alias = MockFeatAlias(alias)
    return MockFeature(
        feature_no=1,
        feature_name="CAL0001",
        gene_name="ALS1",
        dbxref_id="CGD:CAL0001",
        headline="Cell surface adhesin",
        organism=sample_organism,
        feat_alias=[feat_alias],
    )


@pytest.fixture
def sample_go():
    """Create sample GO term."""
    return MockGo(
        go_no=1,
        goid=6412,
        go_term="translation",
        go_aspect="P",
        go_definition="The cellular process of protein synthesis.",
    )


@pytest.fixture
def sample_reference():
    """Create sample reference."""
    return MockReference(
        reference_no=1,
        dbxref_id="CGD_REF:0001",
        pubmed=12345678,
        citation="Author et al. (2023)",
        year=2023,
    )


class TestFormatGoid:
    """Tests for _format_goid."""

    def test_formats_goid_with_padding(self):
        """Should format GOID with 7-digit padding."""
        assert _format_goid(6412) == "GO:0006412"

    def test_formats_small_goid(self):
        """Should format small GOID with leading zeros."""
        assert _format_goid(1) == "GO:0000001"

    def test_formats_large_goid(self):
        """Should format large GOID."""
        assert _format_goid(1234567) == "GO:1234567"


class TestCreateIndex:
    """Tests for create_index."""

    def test_creates_index_when_not_exists(self, mock_es):
        """Should create index when it doesn't exist."""
        mock_es.indices.exists.return_value = False

        create_index(mock_es)

        mock_es.indices.create.assert_called_once()

    def test_skips_creation_when_exists(self, mock_es):
        """Should skip creation when index exists."""
        mock_es.indices.exists.return_value = True

        create_index(mock_es)

        mock_es.indices.create.assert_not_called()


class TestDeleteIndex:
    """Tests for delete_index."""

    def test_deletes_index_when_exists(self, mock_es):
        """Should delete index when it exists."""
        mock_es.indices.exists.return_value = True

        delete_index(mock_es)

        mock_es.indices.delete.assert_called_once_with(index=INDEX_NAME)

    def test_skips_deletion_when_not_exists(self, mock_es):
        """Should skip deletion when index doesn't exist."""
        mock_es.indices.exists.return_value = False

        delete_index(mock_es)

        mock_es.indices.delete.assert_not_called()


class TestGenerateGeneDocs:
    """Tests for _generate_gene_docs."""

    def test_generates_gene_document(self, mock_db, sample_feature):
        """Should generate gene document with correct fields."""
        mock_db.query.return_value = MockQuery([sample_feature])

        docs = list(_generate_gene_docs(mock_db))

        assert len(docs) == 1
        doc = docs[0]
        assert doc["_index"] == INDEX_NAME
        assert doc["_id"] == "gene_1"
        assert doc["_source"]["type"] == "gene"
        assert doc["_source"]["name"] == "ALS1"
        assert doc["_source"]["gene_name"] == "ALS1"
        assert doc["_source"]["feature_name"] == "CAL0001"

    def test_includes_aliases(self, mock_db, sample_feature):
        """Should include aliases in document."""
        mock_db.query.return_value = MockQuery([sample_feature])

        docs = list(_generate_gene_docs(mock_db))

        assert "ALS1_alias" in docs[0]["_source"]["aliases"]

    def test_uses_feature_name_when_no_gene_name(self, mock_db, sample_organism):
        """Should use feature_name as display name when no gene_name."""
        feature = MockFeature(
            feature_no=1,
            feature_name="CAL0001",
            gene_name=None,
            organism=sample_organism,
        )
        mock_db.query.return_value = MockQuery([feature])

        docs = list(_generate_gene_docs(mock_db))

        assert docs[0]["_source"]["name"] == "CAL0001"

    def test_handles_no_organism(self, mock_db):
        """Should handle feature with no organism."""
        feature = MockFeature(
            feature_no=1,
            feature_name="CAL0001",
            organism=None,
        )
        mock_db.query.return_value = MockQuery([feature])

        docs = list(_generate_gene_docs(mock_db))

        assert docs[0]["_source"]["organism"] is None


class TestGenerateGoDocs:
    """Tests for _generate_go_docs."""

    def test_generates_go_document(self, mock_db, sample_go):
        """Should generate GO document with correct fields."""
        mock_db.query.return_value = MockQuery([sample_go])

        docs = list(_generate_go_docs(mock_db))

        assert len(docs) == 1
        doc = docs[0]
        assert doc["_index"] == INDEX_NAME
        assert doc["_id"] == "go_1"
        assert doc["_source"]["type"] == "go_term"
        assert doc["_source"]["goid"] == "GO:0006412"
        assert doc["_source"]["go_term"] == "translation"
        assert doc["_source"]["go_aspect"] == "P"


class TestGeneratePhenotypeDocs:
    """Tests for _generate_phenotype_docs."""

    def test_generates_phenotype_document(self, mock_db):
        """Should generate phenotype document for observable."""
        mock_db.query.return_value = MockQuery([("colony morphology",)])

        docs = list(_generate_phenotype_docs(mock_db))

        assert len(docs) == 1
        doc = docs[0]
        assert doc["_source"]["type"] == "phenotype"
        assert doc["_source"]["observable"] == "colony morphology"

    def test_generates_link_with_observable(self, mock_db):
        """Should generate link with observable in query."""
        mock_db.query.return_value = MockQuery([("growth rate",)])

        docs = list(_generate_phenotype_docs(mock_db))

        assert "observable=growth rate" in docs[0]["_source"]["link"]


class TestGenerateReferenceDocs:
    """Tests for _generate_reference_docs."""

    def test_generates_reference_document(self, mock_db, sample_reference):
        """Should generate reference document with correct fields."""
        mock_db.query.return_value = MockQuery([sample_reference])

        docs = list(_generate_reference_docs(mock_db))

        assert len(docs) == 1
        doc = docs[0]
        assert doc["_index"] == INDEX_NAME
        assert doc["_id"] == "reference_1"
        assert doc["_source"]["type"] == "reference"
        assert doc["_source"]["pubmed"] == 12345678

    def test_uses_pmid_as_display_name(self, mock_db, sample_reference):
        """Should use PMID as display name when available."""
        mock_db.query.return_value = MockQuery([sample_reference])

        docs = list(_generate_reference_docs(mock_db))

        assert docs[0]["_source"]["name"] == "PMID:12345678"

    def test_uses_dbxref_when_no_pmid(self, mock_db):
        """Should use dbxref_id when no PMID."""
        ref = MockReference(
            reference_no=1,
            dbxref_id="CGD_REF:0001",
            pubmed=None,
        )
        mock_db.query.return_value = MockQuery([ref])

        docs = list(_generate_reference_docs(mock_db))

        assert docs[0]["_source"]["name"] == "CGD_REF:0001"


class TestIndexFunctions:
    """Tests for index functions."""

    @patch('cgd.api.services.es_indexer.bulk')
    def test_index_genes_returns_count(self, mock_bulk, mock_db, mock_es):
        """Should return count of indexed genes."""
        mock_bulk.return_value = (10, [])
        mock_db.query.return_value = MockQuery([])

        result = index_genes(mock_db, mock_es)

        assert result == 10

    @patch('cgd.api.services.es_indexer.bulk')
    def test_index_go_terms_returns_count(self, mock_bulk, mock_db, mock_es):
        """Should return count of indexed GO terms."""
        mock_bulk.return_value = (5, [])
        mock_db.query.return_value = MockQuery([])

        result = index_go_terms(mock_db, mock_es)

        assert result == 5

    @patch('cgd.api.services.es_indexer.bulk')
    def test_index_phenotypes_returns_count(self, mock_bulk, mock_db, mock_es):
        """Should return count of indexed phenotypes."""
        mock_bulk.return_value = (3, [])
        mock_db.query.return_value = MockQuery([])

        result = index_phenotypes(mock_db, mock_es)

        assert result == 3

    @patch('cgd.api.services.es_indexer.bulk')
    def test_index_references_returns_count(self, mock_bulk, mock_db, mock_es):
        """Should return count of indexed references."""
        mock_bulk.return_value = (7, [])
        mock_db.query.return_value = MockQuery([])

        result = index_references(mock_db, mock_es)

        assert result == 7


class TestRebuildIndex:
    """Tests for rebuild_index."""

    @patch('cgd.api.services.es_indexer.index_references')
    @patch('cgd.api.services.es_indexer.index_phenotypes')
    @patch('cgd.api.services.es_indexer.index_go_terms')
    @patch('cgd.api.services.es_indexer.index_genes')
    @patch('cgd.api.services.es_indexer.create_index')
    @patch('cgd.api.services.es_indexer.delete_index')
    def test_calls_all_index_functions(
        self, mock_delete, mock_create, mock_genes, mock_go,
        mock_phenotypes, mock_refs, mock_db, mock_es
    ):
        """Should call all index functions."""
        mock_genes.return_value = 10
        mock_go.return_value = 5
        mock_phenotypes.return_value = 3
        mock_refs.return_value = 7

        result = rebuild_index(mock_db, mock_es)

        mock_delete.assert_called_once()
        mock_create.assert_called_once()
        mock_genes.assert_called_once()
        mock_go.assert_called_once()
        mock_phenotypes.assert_called_once()
        mock_refs.assert_called_once()

    @patch('cgd.api.services.es_indexer.index_references')
    @patch('cgd.api.services.es_indexer.index_phenotypes')
    @patch('cgd.api.services.es_indexer.index_go_terms')
    @patch('cgd.api.services.es_indexer.index_genes')
    @patch('cgd.api.services.es_indexer.create_index')
    @patch('cgd.api.services.es_indexer.delete_index')
    def test_returns_summary(
        self, mock_delete, mock_create, mock_genes, mock_go,
        mock_phenotypes, mock_refs, mock_db, mock_es
    ):
        """Should return summary with counts."""
        mock_genes.return_value = 10
        mock_go.return_value = 5
        mock_phenotypes.return_value = 3
        mock_refs.return_value = 7

        result = rebuild_index(mock_db, mock_es)

        assert result["genes"] == 10
        assert result["go_terms"] == 5
        assert result["phenotypes"] == 3
        assert result["references"] == 7
        assert result["total"] == 25

    @patch('cgd.api.services.es_indexer.index_references')
    @patch('cgd.api.services.es_indexer.index_phenotypes')
    @patch('cgd.api.services.es_indexer.index_go_terms')
    @patch('cgd.api.services.es_indexer.index_genes')
    @patch('cgd.api.services.es_indexer.create_index')
    @patch('cgd.api.services.es_indexer.delete_index')
    def test_refreshes_index(
        self, mock_delete, mock_create, mock_genes, mock_go,
        mock_phenotypes, mock_refs, mock_db, mock_es
    ):
        """Should refresh index after rebuild."""
        mock_genes.return_value = 0
        mock_go.return_value = 0
        mock_phenotypes.return_value = 0
        mock_refs.return_value = 0

        rebuild_index(mock_db, mock_es)

        mock_es.indices.refresh.assert_called_once_with(index=INDEX_NAME)
