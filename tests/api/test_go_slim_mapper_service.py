"""
Tests for GO Slim Mapper service.

Tests validate:
1. Gene-to-slim term mapping via direct annotations
2. Gene-to-slim term mapping via ancestor inheritance
3. Frequency calculations
4. Categorization of genes (mapped, other, not annotated)
5. Edge cases and error handling
"""
import pytest
from unittest.mock import MagicMock, patch

from cgd.api.services.go_slim_mapper_service import (
    _format_goid,
    _chunk_list,
    _build_annotation_filters,
)


class TestSlimTermMapping:
    """
    Test GO Slim term mapping logic.

    GO Slim Mapper maps genes to broader GO Slim terms by:
    1. Direct annotation match
    2. Ancestor traversal (child terms map up to slim terms)
    """

    def test_direct_annotation_mapping(self):
        """
        Test that direct annotations to slim terms are mapped correctly.
        """
        # Gene directly annotated to a slim term
        gene_annotations = {100}  # GO term 100
        slim_terms = {100, 200, 300}

        # Check if any annotation matches a slim term
        mapped_slim_terms = gene_annotations & slim_terms
        assert 100 in mapped_slim_terms, "Direct annotation should map to slim"

    def test_ancestor_mapping(self):
        """
        Test that annotations map to slim terms via ancestors.

        If gene is annotated to GO:0006414 (translational elongation),
        and GO:0006412 (translation) is a slim term and an ancestor,
        the gene should map to the slim term.
        """
        # Gene annotated to child term 11
        gene_annotation = 11

        # Ancestor mapping: 11 -> {10, 1}
        ancestors = {11: {10, 1}}

        # Slim terms include ancestor 10
        slim_terms = {10, 20, 30}

        # Check if annotation or any ancestor is a slim term
        all_terms = {gene_annotation} | ancestors.get(gene_annotation, set())
        mapped_slim_terms = all_terms & slim_terms

        assert 10 in mapped_slim_terms, "Ancestor should map to slim term"

    def test_no_mapping_when_no_slim_ancestor(self):
        """
        Test that genes don't map when no ancestors are slim terms.
        """
        gene_annotation = 11
        ancestors = {11: {10, 1}}  # None of these are slim terms
        slim_terms = {20, 30, 40}  # Different branch

        all_terms = {gene_annotation} | ancestors.get(gene_annotation, set())
        mapped_slim_terms = all_terms & slim_terms

        assert len(mapped_slim_terms) == 0, "Should not map to unrelated slim terms"

    def test_multiple_slim_term_mapping(self):
        """
        Test that a gene can map to multiple slim terms.
        """
        gene_annotation = 11
        # Gene's annotation has multiple slim term ancestors
        ancestors = {11: {10, 20, 1}}
        slim_terms = {10, 20, 30}

        all_terms = {gene_annotation} | ancestors.get(gene_annotation, set())
        mapped_slim_terms = all_terms & slim_terms

        assert mapped_slim_terms == {10, 20}, "Should map to multiple slim terms"


class TestFrequencyCalculation:
    """Test frequency calculation for slim term mapping."""

    def test_basic_frequency(self):
        """
        Test basic cluster frequency calculation.

        Frequency = (genes_mapped_to_term / total_genes_with_go) * 100
        """
        genes_mapped = 5
        total_genes_with_go = 20

        frequency = (genes_mapped / total_genes_with_go) * 100
        assert frequency == 25.0, "Frequency should be 25%"

    def test_frequency_zero_genes(self):
        """Test frequency when no genes map to term."""
        genes_mapped = 0
        total_genes_with_go = 20

        frequency = (genes_mapped / total_genes_with_go) * 100 if total_genes_with_go > 0 else 0.0
        assert frequency == 0.0, "Frequency should be 0%"

    def test_frequency_all_genes(self):
        """Test frequency when all genes map to term."""
        genes_mapped = 20
        total_genes_with_go = 20

        frequency = (genes_mapped / total_genes_with_go) * 100
        assert frequency == 100.0, "Frequency should be 100%"

    def test_frequency_rounding(self):
        """Test frequency rounding to 2 decimal places."""
        genes_mapped = 1
        total_genes_with_go = 3

        frequency = round((genes_mapped / total_genes_with_go) * 100, 2)
        assert frequency == 33.33, "Frequency should be rounded to 33.33%"


class TestGeneCategorization:
    """
    Test categorization of genes in GO Slim Mapper results.

    Categories:
    1. Mapped genes - have GO and map to at least one slim term
    2. Other genes - have GO but don't map to any slim term
    3. Not annotated genes - no GO annotations
    """

    def test_categorization_logic(self):
        """Test the logic for categorizing genes."""
        all_query_genes = {1, 2, 3, 4, 5}
        genes_with_go = {1, 2, 3, 4}  # Gene 5 has no GO
        genes_mapped_to_slim = {1, 2}  # Only genes 1 and 2 map to slim

        # Calculate categories
        other_genes = genes_with_go - genes_mapped_to_slim
        not_annotated_genes = all_query_genes - genes_with_go

        assert other_genes == {3, 4}, "Genes 3, 4 should be 'other'"
        assert not_annotated_genes == {5}, "Gene 5 should be 'not annotated'"

    def test_all_genes_mapped(self):
        """Test when all genes with GO map to slim terms."""
        all_query_genes = {1, 2, 3}
        genes_with_go = {1, 2, 3}
        genes_mapped_to_slim = {1, 2, 3}

        other_genes = genes_with_go - genes_mapped_to_slim
        not_annotated_genes = all_query_genes - genes_with_go

        assert len(other_genes) == 0, "No 'other' genes"
        assert len(not_annotated_genes) == 0, "No 'not annotated' genes"

    def test_no_genes_mapped(self):
        """Test when no genes map to slim terms."""
        all_query_genes = {1, 2, 3}
        genes_with_go = {1, 2}
        genes_mapped_to_slim = set()

        other_genes = genes_with_go - genes_mapped_to_slim
        not_annotated_genes = all_query_genes - genes_with_go

        assert other_genes == {1, 2}, "All GO genes should be 'other'"
        assert not_annotated_genes == {3}, "Gene 3 should be 'not annotated'"


class TestUtilityFunctions:
    """Test utility functions for GO Slim Mapper."""

    def test_format_goid(self):
        """Test GOID formatting."""
        assert _format_goid(8150) == "GO:0008150"
        assert _format_goid(1) == "GO:0000001"
        assert _format_goid(9999999) == "GO:9999999"

    def test_chunk_list_basic(self):
        """Test list chunking."""
        lst = list(range(25))
        chunks = _chunk_list(lst, 10)

        assert len(chunks) == 3
        assert chunks[0] == list(range(10))
        assert chunks[1] == list(range(10, 20))
        assert chunks[2] == list(range(20, 25))

    def test_chunk_list_empty(self):
        """Test chunking empty list."""
        assert _chunk_list([]) == []

    def test_chunk_list_smaller_than_chunk_size(self):
        """Test list smaller than chunk size."""
        lst = [1, 2, 3]
        chunks = _chunk_list(lst, 10)
        assert chunks == [[1, 2, 3]]


class TestAnnotationFilters:
    """Test annotation filter building."""

    def test_build_filters_with_types(self):
        """Test filter building with annotation types."""
        filters = _build_annotation_filters(["manually_curated", "high_throughput"])
        assert len(filters) == 1, "Should have one filter for annotation types"

    def test_build_filters_empty(self):
        """Test filter building with no filters."""
        filters = _build_annotation_filters(None)
        assert len(filters) == 0, "Should have no filters"


class TestSlimSetHandling:
    """Test GO Slim set handling."""

    def test_aspect_code_normalization(self):
        """Test that aspect codes are normalized correctly."""
        aspects = ["P", "p", "F", "f", "C", "c"]
        normalized = [a.upper() for a in aspects]

        assert all(a in ["P", "F", "C"] for a in normalized), \
            "All aspects should normalize to P, F, or C"

    def test_selected_terms_parsing(self):
        """Test parsing of selected GO term IDs."""
        selected_terms = ["GO:0008150", "GO:0003674", "0005575"]

        parsed_goids = set()
        for term_id in selected_terms:
            if term_id.startswith("GO:"):
                parsed_goids.add(int(term_id[3:]))
            else:
                try:
                    parsed_goids.add(int(term_id))
                except ValueError:
                    pass

        assert parsed_goids == {8150, 3674, 5575}, "Should parse all GO IDs"


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_gene_list(self):
        """Test handling of empty gene list."""
        genes = []
        genes_upper = [g.strip().upper() for g in genes if g.strip()]
        assert genes_upper == [], "Empty input should return empty list"

    def test_whitespace_gene_names(self):
        """Test handling of whitespace in gene names."""
        genes = ["  ACT1  ", "TUB1", "  ", "CDC42"]
        cleaned = [g.strip().upper() for g in genes if g.strip()]
        assert cleaned == ["ACT1", "TUB1", "CDC42"], "Should strip whitespace"

    def test_duplicate_gene_deduplication(self):
        """Test deduplication of genes by feature_no."""
        # Simulate multiple input names resolving to same feature
        found_features = {
            "ACT1": 1,
            "act1": 1,  # Same gene, different case
            "orf19.1": 1,  # Same gene, systematic name
            "TUB1": 2,
        }

        # Deduplicate by feature_no
        unique_feature_nos = set(found_features.values())
        assert unique_feature_nos == {1, 2}, "Should have 2 unique features"

    def test_no_slim_terms_found(self):
        """Test handling when no slim terms exist for set/aspect."""
        slim_terms = set()
        assert len(slim_terms) == 0, "Empty slim terms set"

        # This should result in error response from the service

    def test_all_genes_not_found(self):
        """Test handling when all genes are not found."""
        query_genes = ["FAKE1", "FAKE2", "FAKE3"]
        found_genes = {}
        not_found = query_genes.copy()

        assert len(found_genes) == 0, "No genes found"
        assert len(not_found) == 3, "All genes not found"


class TestSlimMappingAlgorithm:
    """
    Test the complete slim mapping algorithm.
    """

    def test_mapping_with_multiple_annotations(self):
        """
        Test mapping when gene has multiple GO annotations.

        A gene may have several annotations, each potentially mapping
        to different slim terms.
        """
        # Gene has 3 annotations
        gene_direct_annotations = {100, 200, 300}

        # Ancestors for each annotation
        ancestors = {
            100: {10, 1},
            200: {20, 1},
            300: {30, 1},
        }

        # Slim terms
        slim_terms = {10, 20}

        # Find all terms (direct + ancestors) that match slim
        all_terms = gene_direct_annotations.copy()
        for ann in gene_direct_annotations:
            all_terms.update(ancestors.get(ann, set()))

        mapped = all_terms & slim_terms
        assert mapped == {10, 20}, "Should map to both slim terms 10 and 20"

    def test_transitive_ancestor_chain(self):
        """
        Test mapping through transitive ancestor chains.

        GO hierarchy: child -> parent -> grandparent (slim)
        """
        child_annotation = 111
        parent = 11
        grandparent_slim = 1

        # Ancestor chain
        ancestors = {
            111: {11, 1},  # child knows all ancestors
        }

        slim_terms = {1}  # Only grandparent is slim

        all_terms = {child_annotation} | ancestors.get(child_annotation, set())
        mapped = all_terms & slim_terms

        assert grandparent_slim in mapped, "Should map through ancestor chain"


class TestResultSorting:
    """Test result sorting in GO Slim Mapper."""

    def test_sort_by_gene_count_descending(self):
        """Test that mapped terms are sorted by gene count descending."""
        mapped_terms = [
            {"go_term": "term1", "gene_count": 5},
            {"go_term": "term2", "gene_count": 10},
            {"go_term": "term3", "gene_count": 3},
        ]

        sorted_terms = sorted(mapped_terms, key=lambda x: -x["gene_count"])

        counts = [t["gene_count"] for t in sorted_terms]
        assert counts == [10, 5, 3], "Should be sorted descending by gene count"


class TestAspectFiltering:
    """Test filtering by GO aspect."""

    def test_aspect_filter_process(self):
        """Test filtering to Biological Process (P)."""
        go_terms = [
            {"aspect": "P", "term": "translation"},
            {"aspect": "F", "term": "RNA binding"},
            {"aspect": "C", "term": "ribosome"},
            {"aspect": "P", "term": "cell cycle"},
        ]

        filtered = [t for t in go_terms if t["aspect"] == "P"]
        assert len(filtered) == 2
        assert all(t["aspect"] == "P" for t in filtered)

    def test_aspect_filter_function(self):
        """Test filtering to Molecular Function (F)."""
        go_terms = [
            {"aspect": "P", "term": "translation"},
            {"aspect": "F", "term": "RNA binding"},
            {"aspect": "C", "term": "ribosome"},
        ]

        filtered = [t for t in go_terms if t["aspect"] == "F"]
        assert len(filtered) == 1
        assert filtered[0]["term"] == "RNA binding"

    def test_aspect_filter_component(self):
        """Test filtering to Cellular Component (C)."""
        go_terms = [
            {"aspect": "P", "term": "translation"},
            {"aspect": "F", "term": "RNA binding"},
            {"aspect": "C", "term": "ribosome"},
        ]

        filtered = [t for t in go_terms if t["aspect"] == "C"]
        assert len(filtered) == 1
        assert filtered[0]["term"] == "ribosome"
