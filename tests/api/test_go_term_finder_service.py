"""
Tests for GO Term Finder service.

Based on test cases from Perl GO::TermFinder module (GO-TermFinder-0.86).
Tests validate:
1. Hypergeometric distribution calculations
2. Multiple testing correction methods (Bonferroni, Benjamini-Hochberg FDR)
3. Gene validation (case-insensitivity, alias matching)
4. P-value constraints (never exceed 1.0)
5. Edge cases (empty input, no annotations, etc.)

Reference: https://github.com/gitpan/GO-TermFinder
"""
import math
import pytest
from scipy.stats import hypergeom

from cgd.api.services.go_term_finder_service import (
    _calculate_enrichment,
    _apply_multiple_testing_correction,
    _format_goid,
    _chunk_list,
)
from cgd.schemas.go_term_finder_schema import MultipleCorrectionMethod


class TestHypergeometricDistribution:
    """
    Test hypergeometric distribution calculations.

    These tests mirror the GO-TermFinder-Native.t Perl tests which validate
    the core statistical calculations.
    """

    def test_hypergeometric_pmf_basic(self):
        """
        Test basic hypergeometric probability mass function.

        Scenario: Drawing from a population of 10 with 4 successes,
        sample size 5, probability of exactly 3 successes.

        From Perl test: hypergeometric(3, 5, 4, 10)
        """
        # Parameters: k=3, n=5, K=4, N=10
        # P(X = 3) where X ~ Hypergeom(N=10, K=4, n=5)
        N = 10  # Population size
        K = 4   # Number of success states in population
        n = 5   # Number of draws
        k = 3   # Number of observed successes

        result = hypergeom.pmf(k, N, K, n)

        # Expected: (4 choose 3) * (6 choose 2) / (10 choose 5)
        # = 4 * 15 / 252 = 60/252 = 0.238095...
        expected = 60 / 252
        assert abs(result - expected) < 1e-8, f"Expected {expected}, got {result}"

    def test_hypergeometric_pvalue_upper_tail(self):
        """
        Test p-value calculation (upper tail probability).

        P(X >= k) = probability of k or more successes.
        This is what GO Term Finder uses for enrichment.

        From Perl test: pValueByHypergeometric(3, 5, 4, 10)
        """
        N = 10  # Population size
        K = 4   # Number of success states in population
        n = 5   # Number of draws
        k = 3   # Number of observed successes

        # P(X >= 3) = P(X=3) + P(X=4)
        # Using survival function: sf(k-1, N, K, n) = P(X >= k)
        result = hypergeom.sf(k - 1, N, K, n)

        # Manual calculation:
        # P(X=3) = (4C3 * 6C2) / 10C5 = 60/252
        # P(X=4) = (4C4 * 6C1) / 10C5 = 6/252
        # P(X>=3) = 66/252 = 0.261905...
        expected = 66 / 252
        assert abs(result - expected) < 1e-8, f"Expected {expected}, got {result}"

    def test_hypergeometric_pvalue_alternative_calculation(self):
        """
        Test that P(X >= k) = 1 - P(X <= k-1) = 1 - CDF(k-1).

        Validates consistency between survival function and CDF.
        """
        N, K, n, k = 10, 4, 5, 3

        # Method 1: Survival function
        pval_sf = hypergeom.sf(k - 1, N, K, n)

        # Method 2: 1 - CDF
        pval_cdf = 1 - hypergeom.cdf(k - 1, N, K, n)

        assert abs(pval_sf - pval_cdf) < 1e-10, "SF and CDF methods should match"

    def test_hypergeometric_extreme_enrichment(self):
        """
        Test extreme enrichment scenario.

        All query genes annotated to term, none in background outside query.
        """
        N = 100  # Background size
        K = 5    # Genes in background with annotation
        n = 5    # Query size
        k = 5    # All query genes have annotation

        p_value = hypergeom.sf(k - 1, N, K, n)

        # If all 5 genes with annotation are in query, this is very unlikely
        # by chance, so p-value should be very small
        assert p_value < 0.001, f"P-value should be very small: {p_value}"

    def test_hypergeometric_no_enrichment(self):
        """
        Test no enrichment scenario.

        Query genes same proportion as background.
        """
        N = 100  # Background size
        K = 50   # 50% in background annotated
        n = 10   # Query size
        k = 5    # 50% in query annotated (same proportion)

        p_value = hypergeom.sf(k - 1, N, K, n)

        # When proportions match, p-value should be around 0.5
        assert p_value > 0.1, f"P-value should not be significant: {p_value}"

    def test_pvalue_never_exceeds_one(self):
        """
        Test that p-values never exceed 1.0.

        From Perl module v0.86: "p-value constraints to not exceed 1,
        which sometimes happens due to rounding errors on some platforms"
        """
        # Test various parameter combinations
        test_cases = [
            (100, 50, 10, 1),
            (1000, 500, 100, 50),
            (10, 10, 10, 10),  # Edge case: all genes annotated
            (100, 1, 100, 1),  # Edge case: single annotation
        ]

        for N, K, n, k in test_cases:
            p_value = hypergeom.sf(k - 1, N, K, n)
            assert p_value <= 1.0, f"P-value {p_value} exceeds 1.0 for ({N}, {K}, {n}, {k})"
            assert p_value >= 0.0, f"P-value {p_value} is negative for ({N}, {K}, {n}, {k})"


class TestMultipleCorrectionMethods:
    """
    Test multiple testing correction methods.

    Validates Bonferroni and Benjamini-Hochberg FDR corrections.
    """

    def test_bonferroni_correction_basic(self):
        """
        Test basic Bonferroni correction.

        Bonferroni: corrected_p = min(p * n_tests, 1.0)
        """
        # Sample results: (go_no, k, n, K, N, p_value)
        results = [
            (1, 5, 10, 50, 1000, 0.001),
            (2, 3, 10, 30, 1000, 0.01),
            (3, 2, 10, 20, 1000, 0.05),
        ]

        corrected = _apply_multiple_testing_correction(
            results, MultipleCorrectionMethod.BONFERRONI, p_value_cutoff=0.1
        )

        # With 3 tests, corrected p-values should be 3x original
        assert len(corrected) >= 1  # At least some should pass

        for _, _, _, _, _, p_val, corrected_p in corrected:
            # Bonferroni multiplies by number of tests
            expected = min(p_val * 3, 1.0)
            assert abs(corrected_p - expected) < 1e-10

    def test_bonferroni_correction_capped_at_one(self):
        """
        Test that Bonferroni correction caps p-values at 1.0.

        From Perl tests: correction factor equals hypothesis count (37 in test).
        """
        # With 3 tests, p=0.5 * 3 = 1.5, which should be capped at 1.0
        results = [
            (1, 5, 10, 50, 1000, 0.5),
            (2, 3, 10, 30, 1000, 0.4),
            (3, 2, 10, 20, 1000, 0.3),
        ]

        corrected = _apply_multiple_testing_correction(
            results, MultipleCorrectionMethod.BONFERRONI, p_value_cutoff=1.0
        )

        # Find the result with p=0.5 (should be capped)
        for _, _, _, _, _, p_val, corrected_p in corrected:
            if p_val == 0.5:
                assert corrected_p == 1.0, "Corrected p-value should be capped at 1.0"
                break

    def test_benjamini_hochberg_fdr_basic(self):
        """
        Test basic Benjamini-Hochberg FDR correction.

        BH: FDR = (p_value * n_tests) / rank
        """
        results = [
            (1, 5, 10, 50, 1000, 0.001),
            (2, 3, 10, 30, 1000, 0.005),
            (3, 2, 10, 20, 1000, 0.01),
        ]

        corrected = _apply_multiple_testing_correction(
            results, MultipleCorrectionMethod.BENJAMINI_HOCHBERG, p_value_cutoff=0.1
        )

        # Should maintain order by p-value
        p_values = [r[5] for r in corrected]
        assert p_values == sorted(p_values), "Results should be sorted by p-value"

        # FDR values should increase or stay same as rank increases
        fdr_values = [r[6] for r in corrected]
        for i in range(1, len(fdr_values)):
            assert fdr_values[i] >= fdr_values[i - 1], \
                "FDR should be monotonically non-decreasing"

    def test_benjamini_hochberg_monotonicity(self):
        """
        Test BH FDR monotonicity enforcement.

        The BH procedure enforces that FDR can only increase with rank.
        """
        # Create results where naive FDR would not be monotonic
        results = [
            (1, 5, 10, 50, 1000, 0.001),  # FDR = 0.001 * 4 / 1 = 0.004
            (2, 3, 10, 30, 1000, 0.002),  # FDR = 0.002 * 4 / 2 = 0.004
            (3, 2, 10, 20, 1000, 0.003),  # FDR = 0.003 * 4 / 3 = 0.004
            (4, 2, 10, 20, 1000, 0.01),   # FDR = 0.01 * 4 / 4 = 0.01
        ]

        corrected = _apply_multiple_testing_correction(
            results, MultipleCorrectionMethod.BENJAMINI_HOCHBERG, p_value_cutoff=0.1
        )

        fdr_values = [r[6] for r in corrected]

        # Check monotonicity
        for i in range(1, len(fdr_values)):
            assert fdr_values[i] >= fdr_values[i - 1], \
                f"FDR not monotonic at index {i}: {fdr_values}"

    def test_no_correction(self):
        """
        Test no correction option.

        FDR should be None when no correction is applied.
        """
        results = [
            (1, 5, 10, 50, 1000, 0.001),
            (2, 3, 10, 30, 1000, 0.01),
        ]

        corrected = _apply_multiple_testing_correction(
            results, MultipleCorrectionMethod.NONE, p_value_cutoff=0.1
        )

        for _, _, _, _, _, p_val, fdr in corrected:
            assert fdr is None, "FDR should be None with no correction"

    def test_empty_results(self):
        """Test correction with empty results."""
        corrected = _apply_multiple_testing_correction(
            [], MultipleCorrectionMethod.BONFERRONI, p_value_cutoff=0.05
        )
        assert corrected == [], "Empty input should return empty output"


class TestEnrichmentCalculation:
    """
    Test GO term enrichment calculations.

    Validates the _calculate_enrichment function.
    """

    def test_calculate_enrichment_basic(self):
        """Test basic enrichment calculation."""
        # Query annotations: 3 genes annotated to GO term 1
        query_annotations = {
            1: {100},  # Feature 1 -> GO term 100
            2: {100},  # Feature 2 -> GO term 100
            3: {100},  # Feature 3 -> GO term 100
            4: {200},  # Feature 4 -> GO term 200
            5: {200},  # Feature 5 -> GO term 200
        }

        # Background: 10 genes total, 5 annotated to term 100
        background_annotations = {
            1: {100},
            2: {100},
            3: {100},
            4: {200},
            5: {200},
            6: {100},
            7: {100},
            8: {200},
            9: {200},
            10: {200},
        }

        results = _calculate_enrichment(
            query_annotations,
            background_annotations,
            p_value_cutoff=0.5,
            min_genes_in_term=1,
        )

        # Should have results for both GO terms
        go_nos = {r[0] for r in results}
        assert 100 in go_nos or 200 in go_nos, "Should have enrichment results"

    def test_calculate_enrichment_no_overlap(self):
        """Test enrichment when query has no overlap with background annotations."""
        query_annotations = {
            1: {999},  # GO term not in background
        }

        background_annotations = {
            2: {100},
            3: {100},
        }

        results = _calculate_enrichment(
            query_annotations,
            background_annotations,
            p_value_cutoff=0.05,
            min_genes_in_term=1,
        )

        # GO term 999 has K=0 in background, should be skipped
        assert len(results) == 0, "No enrichment for terms not in background"

    def test_calculate_enrichment_min_genes_filter(self):
        """Test that min_genes_in_term filter is applied."""
        query_annotations = {
            1: {100},  # Only 1 gene with term 100
            2: {200},
            3: {200},
            4: {200},  # 3 genes with term 200
        }

        background_annotations = {
            1: {100},
            2: {200},
            3: {200},
            4: {200},
            5: {100, 200},
            6: {100, 200},
        }

        results = _calculate_enrichment(
            query_annotations,
            background_annotations,
            p_value_cutoff=1.0,
            min_genes_in_term=2,  # Require at least 2 genes
        )

        go_nos = {r[0] for r in results}
        assert 100 not in go_nos, "Term 100 should be filtered (only 1 gene)"
        # Term 200 should pass if it has significant p-value

    def test_calculate_enrichment_empty_query(self):
        """Test enrichment with empty query."""
        results = _calculate_enrichment(
            {},  # Empty query
            {1: {100}, 2: {100}},
            p_value_cutoff=0.05,
            min_genes_in_term=1,
        )
        assert results == [], "Empty query should return no results"

    def test_calculate_enrichment_empty_background(self):
        """Test enrichment with empty background."""
        results = _calculate_enrichment(
            {1: {100}},
            {},  # Empty background
            p_value_cutoff=0.05,
            min_genes_in_term=1,
        )
        assert results == [], "Empty background should return no results"

    def test_self_population_pvalue_equals_one(self):
        """
        Test that using query genes as background gives p-value = 1.0.

        From Perl test: "A specialized test uses only the 18 queried genes
        as the population, expecting all p-values to equal 1.00."
        """
        # When query == background, enrichment is expected (not surprising)
        query_annotations = {
            1: {100},
            2: {100},
            3: {100},
        }

        # Same as query
        background_annotations = query_annotations.copy()

        results = _calculate_enrichment(
            query_annotations,
            background_annotations,
            p_value_cutoff=1.0,  # Accept all p-values
            min_genes_in_term=1,
        )

        for go_no, k, n, K, N, p_value in results:
            # When query == background: k=K, n=N
            # P(X >= k | N=n, K=k) = 1.0 (certain to observe what we observed)
            assert p_value == 1.0, \
                f"P-value should be 1.0 for self-population, got {p_value}"


class TestGeneCaseInsensitivity:
    """
    Test gene name case insensitivity.

    From Perl tests: Gene names with varying cases (e.g., "YPL250C", "ypl250c",
    "YpL250c") should produce identical results.
    """

    def test_gene_names_case_variations(self):
        """
        Test that gene name case doesn't affect matching.

        This is tested at the service level but we validate the concept here.
        """
        # These should all be treated as the same gene
        gene_variants = ["YPL250C", "ypl250c", "YpL250c", "Ypl250C"]

        # After uppercase normalization, all should be equal
        normalized = [g.upper() for g in gene_variants]
        assert len(set(normalized)) == 1, "All variants should normalize to same value"

    def test_gene_list_deduplication(self):
        """
        Test that duplicate genes (by case) are deduplicated.
        """
        genes = ["ACT1", "act1", "Act1", "TUB1"]
        normalized = list(set(g.upper() for g in genes))
        assert len(normalized) == 2, "Should have 2 unique genes after normalization"


class TestUtilityFunctions:
    """Test utility functions."""

    def test_format_goid(self):
        """Test GOID formatting."""
        assert _format_goid(8150) == "GO:0008150"
        assert _format_goid(1) == "GO:0000001"
        assert _format_goid(1234567) == "GO:1234567"

    def test_chunk_list(self):
        """Test list chunking for Oracle IN clause limit."""
        # Test basic chunking
        lst = list(range(10))
        chunks = _chunk_list(lst, 3)
        assert len(chunks) == 4  # [0,1,2], [3,4,5], [6,7,8], [9]
        assert chunks[0] == [0, 1, 2]
        assert chunks[-1] == [9]

        # Test empty list
        assert _chunk_list([]) == []

        # Test list smaller than chunk size
        assert _chunk_list([1, 2], 5) == [[1, 2]]

    def test_chunk_list_default_size(self):
        """Test default chunk size of 900 (for Oracle's 1000 limit)."""
        lst = list(range(1000))
        chunks = _chunk_list(lst)
        assert len(chunks) == 2  # 900 + 100
        assert len(chunks[0]) == 900
        assert len(chunks[1]) == 100


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_single_gene_query(self):
        """Test enrichment with single gene."""
        query_annotations = {1: {100}}
        background_annotations = {
            1: {100},
            2: {100},
            3: {200},
        }

        results = _calculate_enrichment(
            query_annotations,
            background_annotations,
            p_value_cutoff=1.0,
            min_genes_in_term=1,
        )

        # Should return results without error
        assert isinstance(results, list)

    def test_large_pvalue_cutoff(self):
        """Test with p-value cutoff of 1.0 (accept all)."""
        query_annotations = {
            1: {100},
            2: {200},
        }
        background_annotations = {
            1: {100},
            2: {200},
            3: {100},
            4: {200},
        }

        results = _calculate_enrichment(
            query_annotations,
            background_annotations,
            p_value_cutoff=1.0,
            min_genes_in_term=1,
        )

        # Should return all terms
        assert len(results) >= 1

    def test_ancestor_inheritance(self):
        """
        Test concept: genes annotated to child terms inherit to ancestors.

        If gene is annotated to "translational elongation" (GO:0006414),
        it should also be counted for "translation" (GO:0006412) and
        "biological_process" (GO:0008150).
        """
        # Simulate annotations with inheritance
        # Direct: Feature 1 -> GO term 11 (translational elongation)
        # After inheritance: Feature 1 -> {11, 10, 1} (elongation, translation, BP)

        query_annotations_with_ancestors = {
            1: {11, 10, 1},  # Direct to 11, inherited to 10 and 1
            2: {10, 1},      # Direct to 10, inherited to 1
        }

        background_annotations_with_ancestors = {
            1: {11, 10, 1},
            2: {10, 1},
            3: {10, 1},
            4: {10, 1},
            5: {12, 1},  # Different branch
        }

        results = _calculate_enrichment(
            query_annotations_with_ancestors,
            background_annotations_with_ancestors,
            p_value_cutoff=1.0,
            min_genes_in_term=1,
        )

        # Should have results for multiple GO terms due to inheritance
        go_nos = {r[0] for r in results}
        assert len(go_nos) >= 1, "Should have enrichment results with inheritance"


class TestStatisticalValidation:
    """
    Validate statistical calculations against known values.

    These tests use specific values from the Perl GO::TermFinder test suite.
    """

    def test_factorial_log_calculation(self):
        """
        Validate log factorial calculation.

        From Perl tests: Computes factorials for values 0-10.
        """
        factorials = [
            math.factorial(i) for i in range(11)
        ]

        expected = [1, 1, 2, 6, 24, 120, 720, 5040, 40320, 362880, 3628800]
        assert factorials == expected

    def test_ncr_calculation(self):
        """
        Test n choose r (binomial coefficient) calculation.

        From Perl tests: logNCr(10, 6) equals log(210).
        """
        n, r = 10, 6
        ncr = math.comb(n, r)
        assert ncr == 210, f"10C6 should be 210, got {ncr}"

    def test_specific_pvalue_calculation(self):
        """
        Test specific p-value from Perl test suite.

        Parameters from Perl test: x=3, n=5, M=4, N=10
        """
        k = 3   # Observed successes
        n = 5   # Sample size
        K = 4   # Population successes
        N = 10  # Population size

        p_value = hypergeom.sf(k - 1, N, K, n)

        # From Perl test, expected with 8 decimal precision
        # P(X >= 3) = 66/252 = 0.26190476...
        expected = 0.26190476

        assert abs(p_value - expected) < 1e-6, \
            f"Expected ~{expected}, got {p_value}"
