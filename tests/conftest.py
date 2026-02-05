"""
Pytest fixtures for CGD backend tests.

Provides mock database sessions and test data for GO Term Finder
and GO Slim Mapper service tests.
"""
import pytest
from unittest.mock import MagicMock, PropertyMock
from typing import Any, List, Optional


class MockFeature:
    """Mock Feature model for testing."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        gene_name: Optional[str] = None,
        organism_no: int = 1,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.gene_name = gene_name
        self.organism_no = organism_no


class MockGo:
    """Mock Go model for testing."""

    def __init__(
        self,
        go_no: int,
        goid: int,
        go_term: str,
        go_aspect: str,
        go_definition: Optional[str] = None,
    ):
        self.go_no = go_no
        self.goid = goid
        self.go_term = go_term
        self.go_aspect = go_aspect
        self.go_definition = go_definition


class MockGoAnnotation:
    """Mock GoAnnotation model for testing."""

    def __init__(
        self,
        go_annotation_no: int,
        feature_no: int,
        go_no: int,
        go_evidence: str = "IDA",
        annotation_type: str = "manually curated",
    ):
        self.go_annotation_no = go_annotation_no
        self.feature_no = feature_no
        self.go_no = go_no
        self.go_evidence = go_evidence
        self.annotation_type = annotation_type


class MockGoPath:
    """Mock GoPath model for testing."""

    def __init__(
        self,
        go_path_no: int,
        ancestor_go_no: int,
        child_go_no: int,
        generation: int = 1,
        relationship_type: str = "is_a",
    ):
        self.go_path_no = go_path_no
        self.ancestor_go_no = ancestor_go_no
        self.child_go_no = child_go_no
        self.generation = generation
        self.relationship_type = relationship_type


class MockGoSet:
    """Mock GoSet model for testing."""

    def __init__(
        self,
        go_set_no: int,
        go_no: int,
        go_set_name: str,
    ):
        self.go_set_no = go_set_no
        self.go_no = go_no
        self.go_set_name = go_set_name


class MockOrganism:
    """Mock Organism model for testing."""

    def __init__(
        self,
        organism_no: int,
        organism_name: str,
        organism_order: int = 1,
    ):
        self.organism_no = organism_no
        self.organism_name = organism_name
        self.organism_order = organism_order


class MockAlias:
    """Mock Alias model for testing."""

    def __init__(self, alias_no: int, alias_name: str):
        self.alias_no = alias_no
        self.alias_name = alias_name


class MockCode:
    """Mock Code model for testing."""

    def __init__(
        self,
        code_no: int,
        tab_name: str,
        col_name: str,
        code_value: str,
        description: Optional[str] = None,
    ):
        self.code_no = code_no
        self.tab_name = tab_name
        self.col_name = col_name
        self.code_value = code_value
        self.description = description


class MockQuery:
    """Mock SQLAlchemy query object for testing."""

    def __init__(self, results: Optional[List[Any]] = None):
        self._results = results or []
        self._filters = []

    def filter(self, *args, **kwargs):
        return self

    def join(self, *args, **kwargs):
        return self

    def distinct(self):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def first(self):
        return self._results[0] if self._results else None

    def all(self):
        return self._results


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    db = MagicMock()
    db.query = MagicMock(return_value=MockQuery())
    return db


@pytest.fixture
def sample_features():
    """Sample features for testing."""
    return [
        MockFeature(1, "orf19.1", "ACT1", 1),
        MockFeature(2, "orf19.2", "TUB1", 1),
        MockFeature(3, "orf19.3", "CDC42", 1),
        MockFeature(4, "orf19.4", "RAS1", 1),
        MockFeature(5, "orf19.5", "HWP1", 1),
        MockFeature(6, "orf19.6", "ALS1", 1),
        MockFeature(7, "orf19.7", "SAP1", 1),
        MockFeature(8, "orf19.8", "EFG1", 1),
        MockFeature(9, "orf19.9", "CPH1", 1),
        MockFeature(10, "orf19.10", "TEC1", 1),
    ]


@pytest.fixture
def sample_go_terms():
    """Sample GO terms for testing."""
    return [
        MockGo(1, 8150, "biological_process", "P"),
        MockGo(2, 3674, "molecular_function", "F"),
        MockGo(3, 5575, "cellular_component", "C"),
        MockGo(10, 6412, "translation", "P"),
        MockGo(11, 6414, "translational elongation", "P"),
        MockGo(12, 6950, "response to stress", "P"),
        MockGo(13, 6970, "response to osmotic stress", "P"),
        MockGo(20, 5198, "structural molecule activity", "F"),
        MockGo(21, 3735, "structural constituent of ribosome", "F"),
        MockGo(30, 5737, "cytoplasm", "C"),
        MockGo(31, 5840, "ribosome", "C"),
    ]


@pytest.fixture
def sample_go_annotations():
    """Sample GO annotations for testing."""
    return [
        # Feature 1 annotated to translation (P)
        MockGoAnnotation(1, 1, 10, "IDA"),
        # Feature 2 annotated to translation (P)
        MockGoAnnotation(2, 2, 10, "IDA"),
        # Feature 3 annotated to translation (P)
        MockGoAnnotation(3, 3, 10, "IMP"),
        # Feature 4 annotated to response to stress (P)
        MockGoAnnotation(4, 4, 12, "IDA"),
        # Feature 5 annotated to response to stress (P)
        MockGoAnnotation(5, 5, 12, "IGI"),
        # Feature 1 also annotated to ribosome (C)
        MockGoAnnotation(6, 1, 31, "IDA"),
        # Feature 2 also annotated to ribosome (C)
        MockGoAnnotation(7, 2, 31, "IDA"),
    ]


@pytest.fixture
def sample_go_paths():
    """Sample GO paths (ancestors) for testing."""
    return [
        # translation -> biological_process
        MockGoPath(1, 1, 10, generation=2),
        # translational elongation -> translation
        MockGoPath(2, 10, 11, generation=1),
        # translational elongation -> biological_process
        MockGoPath(3, 1, 11, generation=3),
        # response to osmotic stress -> response to stress
        MockGoPath(4, 12, 13, generation=1),
        # response to stress -> biological_process
        MockGoPath(5, 1, 12, generation=2),
        # ribosome -> cellular_component
        MockGoPath(6, 3, 31, generation=2),
        # cytoplasm -> cellular_component
        MockGoPath(7, 3, 30, generation=1),
    ]


@pytest.fixture
def sample_organism():
    """Sample organism for testing."""
    return MockOrganism(1, "Candida albicans SC5314", 1)
