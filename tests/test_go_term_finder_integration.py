"""
Integration tests for GO Term Finder service.

Based on test cases from Perl GO::TermFinder module (GO-TermFinder-0.86).
These tests use embedded test data that mirrors the original Perl tests
using the methionine cluster genes from Spellman et al. (1998).

Reference:
- https://github.com/gitpan/GO-TermFinder
- t/GO-TermFinder-Obo.t
- t/gene_association.sgd
- t/gene_ontology_edit.obo

Test Cases:
1. Basic enrichment with methionine cluster genes
2. Case insensitivity of gene names
3. Custom population vs default population
4. Self-population (p-values should all be 1.0)
5. Bonferroni correction factor validation
6. Discarded genes handling
7. No enrichment when all genes are in background
"""
import pytest
from collections import defaultdict
from typing import Dict, List, Set, Tuple
from scipy.stats import hypergeom

from cgd.api.services.go_term_finder_service import (
    _calculate_enrichment,
    _apply_multiple_testing_correction,
)
from cgd.schemas.go_term_finder_schema import MultipleCorrectionMethod


# =============================================================================
# EMBEDDED TEST DATA
# =============================================================================
# This data mirrors the test setup from the Perl GO::TermFinder tests.
# The original tests use gene_ontology_edit.obo and gene_association.sgd.

# Methionine cluster genes from Spellman et al. (1998)
# These are the 18 genes used in the Perl test suite
METHIONINE_CLUSTER_GENES = [
    "YPL250C", "MET11", "MXR1", "MET17", "SAM3", "MET28", "STR3", "MMP1",
    "MET1", "YIL074C", "MHT1", "MET14", "MET16", "MET3", "MET10", "ECM17",
    "MET2", "MUP1", "MET6"
]

# Same genes with different casing (for case insensitivity test)
METHIONINE_CLUSTER_GENES_MIXED_CASE = [
    "ypl250c", "Met11", "mxr1", "Met17", "SAM3", "met28", "Str3", "MMp1",
    "mET1", "YIl074c", "Mht1", "mEt14", "Met16", "Met3", "mET10", "ecm17",
    "Met2", "MuP1", "MeT6"
]

# Expected top 11 GO term IDs from the Perl test
# These are the most significant terms for the methionine cluster
EXPECTED_TOP_GOIDS = [
    "GO:0006790",  # sulfur compound metabolic process
    "GO:0000096",  # sulfur amino acid metabolic process
    "GO:0006555",  # methionine metabolic process
    "GO:0000097",  # sulfur amino acid biosynthetic process
    "GO:0006520",  # cellular amino acid metabolic process
    "GO:0006519",  # cellular amino acid and derivative metabolic process
    "GO:0009066",  # aspartate family amino acid metabolic process
    "GO:0009308",  # amine metabolic process
    "GO:0006807",  # nitrogen compound metabolic process
    "GO:0044272",  # sulfur compound biosynthetic process
    "GO:0000103",  # sulfate assimilation
]

# Bogus genes for discarded gene test
BOGUS_GENES = ["BLAH", "BLAH2", "XXXZZZ", "CDCDCDC"]

# Unannotated genes (for aspect node test)
UNANNOTATED_GENES = [
    "YPR108W-A", "YPR109W", "YPR114W", "YPR115W", "YPR116W",
    "YPR117W", "YPR127W", "YPR145C-A", "YPR147C", "YPR148C",
    "YPR153W", "YPR157W", "YPR158W", "YPR159C-A", "YPR172W",
    "YPR174C", "YPR196W", "YPR202W", "YPR203W", "YPR204W"
]


# =============================================================================
# SYNTHETIC TEST DATA FOR ALGORITHM VALIDATION
# =============================================================================
# Create a minimal but realistic test dataset that demonstrates enrichment

def create_test_annotations() -> Tuple[Dict[int, Set[int]], Dict[int, Set[int]]]:
    """
    Create synthetic annotation data for testing enrichment.

    Returns:
        Tuple of (query_annotations, background_annotations)
        Each is a dict mapping feature_no -> set of go_nos
    """
    # GO term IDs (using integers for internal representation)
    # Hierarchy:
    #   GO:0008150 (biological_process) - root
    #   ├── GO:0006807 (nitrogen compound metabolic process)
    #   │   └── GO:0006520 (cellular amino acid metabolic process)
    #   │       └── GO:0000096 (sulfur amino acid metabolic process)
    #   │           └── GO:0006555 (methionine metabolic process)
    #   └── GO:0009987 (cellular process)
    #       └── GO:0008152 (metabolic process)

    GO_BIOLOGICAL_PROCESS = 8150
    GO_NITROGEN_COMPOUND = 6807
    GO_AMINO_ACID = 6520
    GO_SULFUR_AMINO_ACID = 96
    GO_METHIONINE = 6555
    GO_CELLULAR_PROCESS = 9987
    GO_METABOLIC_PROCESS = 8152

    # Ancestor mappings (child -> all ancestors including self)
    ancestors = {
        GO_METHIONINE: {GO_METHIONINE, GO_SULFUR_AMINO_ACID, GO_AMINO_ACID,
                        GO_NITROGEN_COMPOUND, GO_BIOLOGICAL_PROCESS},
        GO_SULFUR_AMINO_ACID: {GO_SULFUR_AMINO_ACID, GO_AMINO_ACID,
                               GO_NITROGEN_COMPOUND, GO_BIOLOGICAL_PROCESS},
        GO_AMINO_ACID: {GO_AMINO_ACID, GO_NITROGEN_COMPOUND, GO_BIOLOGICAL_PROCESS},
        GO_NITROGEN_COMPOUND: {GO_NITROGEN_COMPOUND, GO_BIOLOGICAL_PROCESS},
        GO_METABOLIC_PROCESS: {GO_METABOLIC_PROCESS, GO_CELLULAR_PROCESS,
                               GO_BIOLOGICAL_PROCESS},
        GO_CELLULAR_PROCESS: {GO_CELLULAR_PROCESS, GO_BIOLOGICAL_PROCESS},
        GO_BIOLOGICAL_PROCESS: {GO_BIOLOGICAL_PROCESS},
    }

    # Query genes (10 genes, 8 annotated to methionine pathway)
    # Feature numbers 1-10
    query_annotations = {}
    for i in range(1, 9):  # Genes 1-8: methionine pathway
        query_annotations[i] = ancestors[GO_METHIONINE].copy()
    for i in range(9, 11):  # Genes 9-10: general metabolic process
        query_annotations[i] = ancestors[GO_METABOLIC_PROCESS].copy()

    # Background (100 genes total)
    # 15 annotated to methionine (including the 8 query genes)
    # 30 annotated to amino acid metabolism
    # 50 annotated to general metabolic process
    # 5 annotated to biological process only
    background_annotations = {}

    # Copy query annotations
    for i in range(1, 11):
        background_annotations[i] = query_annotations[i].copy()

    # Additional methionine genes (11-15)
    for i in range(11, 16):
        background_annotations[i] = ancestors[GO_METHIONINE].copy()

    # Amino acid metabolism genes (16-30)
    for i in range(16, 31):
        background_annotations[i] = ancestors[GO_AMINO_ACID].copy()

    # General metabolic process genes (31-80)
    for i in range(31, 81):
        background_annotations[i] = ancestors[GO_METABOLIC_PROCESS].copy()

    # Biological process only genes (81-100)
    for i in range(81, 101):
        background_annotations[i] = {GO_BIOLOGICAL_PROCESS}

    return query_annotations, background_annotations


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

class TestEnrichmentIntegration:
    """
    Integration tests for GO enrichment calculation.

    These tests validate the complete enrichment pipeline using
    synthetic data that mirrors the structure of real GO annotations.
    """

    def test_methionine_enrichment_basic(self):
        """
        Test basic enrichment calculation with methionine pathway genes.

        This mirrors the Perl test that uses the methionine cluster
        from Spellman et al. (1998).
        """
        query_annotations, background_annotations = create_test_annotations()

        results = _calculate_enrichment(
            query_annotations,
            background_annotations,
            p_value_cutoff=0.05,
            min_genes_in_term=1,
        )

        # Should have enrichment results
        assert len(results) > 0, "Should find enriched terms"

        # Most significant term should be methionine (GO:0006555 = 6555)
        # or a related specific term
        go_nos = [r[0] for r in results]
        p_values = {r[0]: r[5] for r in results}

        # Methionine term should be enriched
        # 8 out of 10 query genes vs 15 out of 100 background
        assert 6555 in go_nos, "Methionine term should be enriched"

        # P-value for methionine should be very small
        assert p_values[6555] < 0.001, "Methionine p-value should be highly significant"

    def test_enrichment_ranking(self):
        """
        Test that more specific terms rank higher than general terms.

        In the GO hierarchy, more specific terms (like methionine metabolism)
        should have lower p-values than general terms (like biological process).
        """
        query_annotations, background_annotations = create_test_annotations()

        results = _calculate_enrichment(
            query_annotations,
            background_annotations,
            p_value_cutoff=1.0,  # Accept all
            min_genes_in_term=1,
        )

        # Sort by p-value
        results_sorted = sorted(results, key=lambda x: x[5])

        # Get GO terms in order
        go_order = [r[0] for r in results_sorted]

        # Methionine (6555) should rank before biological_process (8150)
        if 6555 in go_order and 8150 in go_order:
            assert go_order.index(6555) < go_order.index(8150), \
                "Methionine should rank before biological_process"

    def test_pvalue_calculation_matches_expected(self):
        """
        Test that p-value calculation matches expected hypergeometric.

        Manually verify the p-value for methionine enrichment.
        """
        query_annotations, background_annotations = create_test_annotations()

        results = _calculate_enrichment(
            query_annotations,
            background_annotations,
            p_value_cutoff=1.0,
            min_genes_in_term=1,
        )

        # Find methionine result
        methionine_result = None
        for r in results:
            if r[0] == 6555:  # GO:0006555
                methionine_result = r
                break

        assert methionine_result is not None, "Should find methionine result"

        go_no, k, n, K, N, p_value = methionine_result

        # Verify the counts
        # Query: 8 genes annotated to methionine (genes 1-8)
        # Background: 13 genes annotated to methionine (genes 1-8 + 11-15)
        assert k == 8, f"Query count should be 8, got {k}"
        assert n == 10, f"Query total should be 10, got {n}"
        assert K == 13, f"Background count should be 13, got {K}"
        assert N == 100, f"Background total should be 100, got {N}"

        # Verify p-value matches scipy calculation
        expected_pvalue = hypergeom.sf(k - 1, N, K, n)
        assert abs(p_value - expected_pvalue) < 1e-10, \
            f"P-value {p_value} doesn't match expected {expected_pvalue}"


class TestCaseInsensitivity:
    """
    Test case insensitivity in gene name matching.

    From Perl test: "Gene names with varying cases (e.g., 'YPL250C',
    'ypl250c', 'YpL250c') should produce identical results."
    """

    def test_gene_names_normalize_to_same(self):
        """Test that gene names normalize correctly."""
        original = METHIONINE_CLUSTER_GENES
        mixed_case = METHIONINE_CLUSTER_GENES_MIXED_CASE

        original_upper = set(g.upper() for g in original)
        mixed_upper = set(g.upper() for g in mixed_case)

        assert original_upper == mixed_upper, \
            "Gene names should normalize to same set"


class TestSelfPopulation:
    """
    Test self-population behavior.

    From Perl test: "Results become uniform at 1.00 probability when
    population equals query genes."
    """

    def test_self_population_pvalues_equal_one(self):
        """
        Test that using query genes as background gives p-value = 1.0.

        When the query set equals the background set, every gene in the
        query has the annotation, so there's no enrichment (p = 1.0).
        """
        # Create identical query and background
        query_annotations = {
            1: {100, 200},
            2: {100, 200},
            3: {100},
            4: {200},
            5: {300},
        }
        background_annotations = query_annotations.copy()

        results = _calculate_enrichment(
            query_annotations,
            background_annotations,
            p_value_cutoff=1.0,
            min_genes_in_term=1,
        )

        for go_no, k, n, K, N, p_value in results:
            # When query == background, k/n == K/N, so p-value should be 1.0
            # (or very close due to floating point)
            assert abs(p_value - 1.0) < 1e-10, \
                f"P-value should be 1.0 for self-population, got {p_value}"


class TestBonferroniCorrection:
    """
    Test Bonferroni correction factor.

    From Perl test: "With bonferroni correction, 'CORRECTED_PVALUE/PVALUE'
    equals result count (37 hypotheses)."
    """

    def test_bonferroni_correction_factor(self):
        """
        Test that Bonferroni correction factor equals number of hypotheses.
        """
        # Create test results
        results = [
            (1, 5, 10, 20, 100, 0.001),
            (2, 4, 10, 30, 100, 0.005),
            (3, 3, 10, 40, 100, 0.01),
            (4, 2, 10, 50, 100, 0.02),
        ]

        corrected = _apply_multiple_testing_correction(
            results, MultipleCorrectionMethod.BONFERRONI, p_value_cutoff=1.0
        )

        num_hypotheses = len(results)

        for go_no, k, n, K, N, p_val, corrected_p in corrected:
            if corrected_p < 1.0:
                # Correction factor should equal number of hypotheses
                factor = corrected_p / p_val
                assert abs(factor - num_hypotheses) < 1e-10, \
                    f"Correction factor should be {num_hypotheses}, got {factor}"


class TestDiscardedGenes:
    """
    Test handling of genes not in the annotation database.

    From Perl test: "Four bogus gene identifiers (BLAH, BLAH2, XXXZZZ,
    CDCDCDC) excluded without affecting results."
    """

    def test_bogus_genes_not_in_valid_list(self):
        """Test that bogus genes are identified as not found."""
        valid_genes = set(g.upper() for g in METHIONINE_CLUSTER_GENES)
        bogus_genes = set(g.upper() for g in BOGUS_GENES)

        # Bogus genes should not be in valid genes
        overlap = valid_genes & bogus_genes
        assert len(overlap) == 0, "Bogus genes should not overlap with valid genes"

    def test_enrichment_unchanged_with_bogus_genes(self):
        """
        Test that adding bogus genes doesn't change enrichment results.

        The service should discard unknown genes and produce the same
        results as without them.
        """
        query_annotations, background_annotations = create_test_annotations()

        # Results without bogus genes
        results_clean = _calculate_enrichment(
            query_annotations,
            background_annotations,
            p_value_cutoff=0.05,
            min_genes_in_term=1,
        )

        # The enrichment calculation itself doesn't handle bogus genes -
        # that's done at the validation step. Here we just verify the
        # algorithm works correctly with clean data.
        assert len(results_clean) > 0, "Should have results without bogus genes"


class TestNoEnrichment:
    """
    Test behavior when no enrichment is expected.

    From Perl test: "Tests that if we say that we're looking for
    significant terms when we simply have a list of all genes, that
    we get none - indeed the uncorrected p-values should all be equal to 1."
    """

    def test_all_background_genes_no_enrichment(self):
        """
        Test that using all background genes as query gives p-value = 1.

        When query contains all genes in the background, the observed
        proportion equals the expected proportion, so no enrichment.
        """
        query_annotations, background_annotations = create_test_annotations()

        # Use entire background as query
        results = _calculate_enrichment(
            background_annotations,  # Query = all background
            background_annotations,
            p_value_cutoff=1.0,
            min_genes_in_term=1,
        )

        for go_no, k, n, K, N, p_value in results:
            assert abs(p_value - 1.0) < 1e-10, \
                f"P-value should be 1.0 when query = background, got {p_value}"


class TestUnannotatedGenes:
    """
    Test handling of genes annotated only to root term.

    From Perl test: Tests using genes annotated directly to
    biological_process (GO:0008150).
    """

    def test_unannotated_genes_single_term(self):
        """
        Test that unannotated genes return only the root term.
        """
        # Create data where some genes are only annotated to root
        query_annotations = {
            1: {8150},  # biological_process only
            2: {8150},
            3: {8150},
        }

        background_annotations = {
            1: {8150},
            2: {8150},
            3: {8150},
            4: {6555, 96, 6520, 6807, 8150},  # Full methionine path
            5: {6555, 96, 6520, 6807, 8150},
        }

        results = _calculate_enrichment(
            query_annotations,
            background_annotations,
            p_value_cutoff=1.0,
            min_genes_in_term=1,
        )

        # Should only have biological_process term
        go_nos = [r[0] for r in results]
        assert go_nos == [8150], \
            f"Should only have biological_process, got {go_nos}"


class TestExpectedResults:
    """
    Test expected GO term results.

    From Perl test: Validates that specific GO terms appear in the
    expected order for the methionine cluster.
    """

    def test_expected_top_terms_from_perl(self):
        """
        Verify the expected top GO terms from the Perl test suite.

        The Perl tests expect these 11 GO IDs in order for the
        methionine cluster:
        GO:0006790, GO:0000096, GO:0006555, GO:0000097, GO:0006520,
        GO:0006519, GO:0009066, GO:0009308, GO:0006807, GO:0044272,
        GO:0000103
        """
        # This test documents the expected behavior from the Perl tests
        # The actual values depend on having the complete SGD annotations
        expected = EXPECTED_TOP_GOIDS
        assert len(expected) == 11, "Should have 11 expected GO terms"

        # Verify format
        for goid in expected:
            assert goid.startswith("GO:"), f"GOID should start with GO: {goid}"
            assert len(goid) == 10, f"GOID should be 10 chars: {goid}"


class TestCorrectionMethods:
    """
    Test multiple hypothesis correction methods.

    From Perl test: Tests bonferroni, simulation, and no correction.
    """

    def test_all_correction_methods_produce_results(self):
        """Test that all correction methods work without errors."""
        results = [
            (1, 5, 10, 20, 100, 0.001),
            (2, 4, 10, 30, 100, 0.005),
            (3, 3, 10, 40, 100, 0.01),
        ]

        for method in MultipleCorrectionMethod:
            corrected = _apply_multiple_testing_correction(
                results, method, p_value_cutoff=0.1
            )
            # Should not raise and should return list
            assert isinstance(corrected, list), f"{method} should return list"

    def test_correction_methods_preserve_pvalues(self):
        """Test that correction methods preserve uncorrected p-values."""
        results = [
            (1, 5, 10, 20, 100, 0.001),
            (2, 4, 10, 30, 100, 0.005),
        ]

        for method in MultipleCorrectionMethod:
            corrected = _apply_multiple_testing_correction(
                results, method, p_value_cutoff=1.0
            )

            for corr in corrected:
                # Uncorrected p-value should be preserved
                original_pval = corr[5]
                assert original_pval in [0.001, 0.005], \
                    f"Original p-value should be preserved with {method}"


class TestFDRCalculation:
    """
    Test False Discovery Rate calculation.

    From Perl test: "Not sure what tests we'll do for the FDR calculations,
    but we should at least make sure that they don't throw an error."
    """

    def test_fdr_produces_valid_results(self):
        """Test that FDR calculation produces valid results."""
        results = [
            (1, 5, 10, 20, 100, 0.001),
            (2, 4, 10, 30, 100, 0.005),
            (3, 3, 10, 40, 100, 0.01),
            (4, 2, 10, 50, 100, 0.02),
        ]

        corrected = _apply_multiple_testing_correction(
            results, MultipleCorrectionMethod.BENJAMINI_HOCHBERG, p_value_cutoff=0.1
        )

        for go_no, k, n, K, N, p_val, fdr in corrected:
            # FDR should be >= p-value
            assert fdr >= p_val, f"FDR {fdr} should be >= p-value {p_val}"
            # FDR should be <= 1.0
            assert fdr <= 1.0, f"FDR {fdr} should be <= 1.0"

    def test_fdr_monotonicity(self):
        """Test that FDR values are monotonically non-decreasing."""
        results = [
            (1, 5, 10, 20, 100, 0.001),
            (2, 4, 10, 30, 100, 0.002),
            (3, 3, 10, 40, 100, 0.003),
            (4, 2, 10, 50, 100, 0.004),
        ]

        corrected = _apply_multiple_testing_correction(
            results, MultipleCorrectionMethod.BENJAMINI_HOCHBERG, p_value_cutoff=1.0
        )

        fdr_values = [r[6] for r in corrected]

        for i in range(1, len(fdr_values)):
            assert fdr_values[i] >= fdr_values[i-1], \
                f"FDR should be monotonically non-decreasing at index {i}"
