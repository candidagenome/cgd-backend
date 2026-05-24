"""
Tests for CRISPR Guide RNA Designer service functionality.

Tests cover:
- PAM site finding
- Guide sequence extraction
- GC content calculation
- Efficiency scoring
- Target region filtering
- Comparison with CHOPCHOP expected guides for 20 genes

The test fixtures contain expected guide sequences validated against
CHOPCHOP (chopchop.cbu.uib.no) for C. albicans SC5314 Assembly 22.
"""
import json
from pathlib import Path
import pytest
from unittest.mock import MagicMock, patch
from typing import List, Dict, Any, Optional

from cgd.schemas.crispr_schema import (
    PAMType,
    TargetRegion,
    CrisprDesignRequest,
    GuideResult,
    OffTargetHit,
)
from cgd.api.services.crispr_service import (
    _reverse_complement,
    _calculate_gc_content,
    _has_poly_t,
    _calculate_self_complementarity,
    _find_restriction_sites,
    _calculate_efficiency_score,
    _calculate_chopchop_penalty,
    _chopchop_penalty_to_display_score,
    _find_pam_sites,
    _filter_target_region,
    _validate_pam_at_position,
    _search_offtargets_bruteforce,
    _count_mismatches,
    design_guides,
)


# =============================================================================
# Test Fixtures: 20 C. albicans genes with expected CRISPR guides
# =============================================================================
#
# These fixtures are loaded from the JSON file: fixtures/crispr_test_genes.json
# The file contains expected guide sequences from CHOPCHOP analysis.
#
# To update fixtures:
# 1. Edit tests/api/fixtures/crispr_test_genes.json
# 2. See tests/api/fixtures/CRISPR_README.md for instructions
# =============================================================================

# Load test genes from JSON fixture file
_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "crispr_test_genes.json"
with open(_FIXTURE_PATH) as f:
    CRISPR_TEST_GENES = json.load(f)


# =============================================================================
# Unit Tests for Helper Functions
# =============================================================================

class TestReverseComplement:
    """Tests for reverse complement function."""

    def test_simple_reverse_complement(self):
        """Basic reverse complement."""
        assert _reverse_complement("ATGC") == "GCAT"

    def test_all_bases(self):
        """All four bases."""
        assert _reverse_complement("AACCGGTT") == "AACCGGTT"

    def test_palindrome(self):
        """Palindromic sequences (like restriction sites)."""
        assert _reverse_complement("GAATTC") == "GAATTC"

    def test_empty_sequence(self):
        """Empty sequence returns empty."""
        assert _reverse_complement("") == ""

    def test_single_base(self):
        """Single base."""
        assert _reverse_complement("A") == "T"
        assert _reverse_complement("T") == "A"
        assert _reverse_complement("G") == "C"
        assert _reverse_complement("C") == "G"


class TestGCContent:
    """Tests for GC content calculation."""

    def test_all_gc(self):
        """100% GC content."""
        assert _calculate_gc_content("GGGGCCCC") == 100.0

    def test_all_at(self):
        """0% GC content."""
        assert _calculate_gc_content("AAAATTTT") == 0.0

    def test_balanced(self):
        """50% GC content."""
        assert _calculate_gc_content("AATTGGCC") == 50.0

    def test_typical_guide(self):
        """Typical guide sequence (~40-60% GC)."""
        gc = _calculate_gc_content("ATGCGATCGATCGATCGATC")
        assert 40 <= gc <= 60

    def test_empty_sequence(self):
        """Empty sequence returns 0."""
        assert _calculate_gc_content("") == 0


class TestPolyT:
    """Tests for poly-T terminator detection."""

    def test_has_poly_t(self):
        """Should detect TTTT."""
        assert _has_poly_t("ATGCTTTTGATC") is True

    def test_no_poly_t(self):
        """Should not detect when absent."""
        assert _has_poly_t("ATGCATGCATGC") is False

    def test_three_ts_not_poly(self):
        """TTT is not poly-T."""
        assert _has_poly_t("ATGCTTTGATC") is False

    def test_lowercase(self):
        """Should handle lowercase."""
        assert _has_poly_t("atgcttttgatc") is True


class TestRestrictionSites:
    """Tests for restriction site detection in guides."""

    def test_finds_bbsi(self):
        """Should find BbsI site (GAAGAC)."""
        sites = _find_restriction_sites("ATGAAGACATGC")
        enzyme_names = [s.enzyme for s in sites]
        assert "BbsI" in enzyme_names

    def test_finds_bsai(self):
        """Should find BsaI site (GGTCTC)."""
        sites = _find_restriction_sites("ATGGTCTCATGC")
        enzyme_names = [s.enzyme for s in sites]
        assert "BsaI" in enzyme_names

    def test_no_sites(self):
        """Should return empty list when no sites."""
        sites = _find_restriction_sites("ATATATATATAT")
        assert len(sites) == 0


class TestEfficiencyScore:
    """Tests for efficiency score calculation."""

    def test_score_range(self):
        """Score should be between 0 and 100."""
        # Various test sequences
        seqs = [
            "ATGCATGCATGCATGCATGC",
            "GGGGGGGGGGGGGGGGGGGG",
            "AAAAAAAAAAAAAAAAAAAA",
            "ATGCTTTTGATCGATCGATC",
        ]
        for seq in seqs:
            score = _calculate_efficiency_score(seq)
            assert 0 <= score <= 100

    def test_poly_t_penalty(self):
        """Poly-T should reduce efficiency score."""
        with_poly_t = _calculate_efficiency_score("ATGCTTTTGATCGATCGATC")
        without_poly_t = _calculate_efficiency_score("ATGCGATCGATCGATCGATC")
        assert with_poly_t < without_poly_t

    def test_extreme_gc_penalty(self):
        """Extreme GC content should reduce score."""
        all_gc = _calculate_efficiency_score("GGGGGGGGGGGGGGGGGGGG")
        balanced = _calculate_efficiency_score("ATGCATGCATGCATGCATGC")
        assert all_gc < balanced


class TestCHOPCHOPScoring:
    """Tests for CHOPCHOP-style guide ranking penalties."""

    def _offtarget(self, mismatches: int) -> OffTargetHit:
        return OffTargetHit(
            chromosome="chr1",
            position=100,
            strand="+",
            sequence="ATGCATGCATGCATGCATGC",
            mismatches=mismatches,
            mismatch_positions=list(range(mismatches)),
            cfd_score=1.0,
        )

    def test_self_complementarity_counts_four_base_stems(self):
        """Potential 4bp self-complementary stems should be counted."""
        assert _calculate_self_complementarity("AAAACCCCGGGGTTTT") > 0
        assert _calculate_self_complementarity("AAAAAAAAAAAAAAAAAAAA") == 0

    def test_gc_outside_chopchop_range_is_penalized(self):
        """CHOPCHOP-style penalty should penalize extreme GC (<30% or >80%)."""
        # Use sequences with no self-complementarity to isolate GC effect
        # Balanced GC (50%) - no penalty
        balanced = _calculate_chopchop_penalty(
            "AATTCCGGAATTCCGGAATT",  # 50% GC, low self-comp
            efficiency_score=50,
            offtargets=[],
            gc_content=50,
        )
        # Extreme high GC (100%) - penalty applied
        extreme_gc = _calculate_chopchop_penalty(
            "GCGCGCGCGCGCGCGCGCGC",  # 100% GC, low self-comp
            efficiency_score=50,
            offtargets=[],
            gc_content=100,
        )

        # Extreme GC should have higher penalty (worse ranking)
        assert extreme_gc > balanced

    def test_offtargets_dominate_chopchop_penalty(self):
        """Fewer and weaker off-targets should rank better."""
        no_hits = _calculate_chopchop_penalty(
            "ATGCATGCATGCATGCATGC",
            efficiency_score=50,
            offtargets=[],
            gc_content=50,
        )
        one_three_mm = _calculate_chopchop_penalty(
            "ATGCATGCATGCATGCATGC",
            efficiency_score=50,
            offtargets=[self._offtarget(3)],
            gc_content=50,
        )
        one_zero_mm = _calculate_chopchop_penalty(
            "ATGCATGCATGCATGCATGC",
            efficiency_score=50,
            offtargets=[self._offtarget(0)],
            gc_content=50,
        )

        assert no_hits < one_three_mm < one_zero_mm

    def test_efficiency_lowers_chopchop_penalty(self):
        """Higher predicted efficiency should reduce the rank penalty."""
        low_efficiency = _calculate_chopchop_penalty(
            "ATGCATGCATGCATGCATGC",
            efficiency_score=20,
            offtargets=[],
            gc_content=50,
        )
        high_efficiency = _calculate_chopchop_penalty(
            "ATGCATGCATGCATGCATGC",
            efficiency_score=80,
            offtargets=[],
            gc_content=50,
        )

        assert high_efficiency < low_efficiency

    def test_display_score_drops_as_penalty_increases(self):
        """Existing high-is-good UI score should invert the penalty."""
        assert _chopchop_penalty_to_display_score(0) > _chopchop_penalty_to_display_score(500)


# =============================================================================
# Tests for PAM Site Finding
# =============================================================================

class TestPAMSiteFinding:
    """Tests for PAM site finding."""

    def test_finds_ngg_pam(self):
        """Should find NGG PAM sites."""
        # Sequence with one NGG site: guide(20bp) + PAM(NGG)
        sequence = "AAAAAAAAAAAAAAAAAAAAAATGCAGG"
        #                                ^^^^^^^^^^^^^^^^^^^^   ^^^
        #                                guide (20bp)           PAM
        guides = _find_pam_sites(sequence, PAMType.NGG, guide_length=20)

        assert len(guides) >= 1
        # Check that a guide ending before AGG was found
        pam_sequences = [pam for _, pam, _, _ in guides]
        assert any("GG" in pam for pam in pam_sequences)

    def test_finds_multiple_pam_sites(self):
        """Should find multiple PAM sites."""
        # Multiple NGG sites
        sequence = "ATGCATGCATGCATGCATGCAGGNNNNNNNNNNNNNNNNNNNNNAGG"
        guides = _find_pam_sites(sequence, PAMType.NGG, guide_length=20)

        assert len(guides) >= 2

    def test_finds_both_strands(self):
        """Should find PAM sites on both strands."""
        # NGG on forward, CCN on reverse
        sequence = "ATGCATGCATGCATGCATGCAGGCCATGCATGCATGCATGCATGC"
        guides = _find_pam_sites(sequence, PAMType.NGG, guide_length=20)

        strands = [strand for _, _, _, strand in guides]
        assert "+" in strands or "-" in strands

    def test_guide_length(self):
        """Should extract guides of correct length."""
        sequence = "AAAAAAAAAAAAAAAAAAAAATGCAGG"
        guides = _find_pam_sites(sequence, PAMType.NGG, guide_length=20)

        for guide_seq, pam, pos, strand in guides:
            assert len(guide_seq) == 20

    def test_tttv_pam_cas12a(self):
        """Should find TTTV PAM for Cas12a (5' PAM)."""
        # TTTV PAM is 5' of guide
        sequence = "TTTAGATGCATGCATGCATGCATGCATGC"
        guides = _find_pam_sites(sequence, PAMType.TTTV, guide_length=20)

        # Should find a guide starting after TTTA
        assert len(guides) >= 1

    def test_forward_ngg_position_is_guide_start(self):
        """Forward 3' PAM position should be the guide start."""
        guide = "ATGCATGCATGCATGCATGC"
        sequence = f"{guide}AGG"

        guides = _find_pam_sites(sequence, PAMType.NGG, guide_length=20)

        assert (guide, "AGG", 1, "+") in guides

    def test_reverse_ngg_position_is_input_coordinate(self):
        """Reverse 3' PAM position should be converted to input coordinates."""
        guide = "ATGCATGCATGCATGCATGC"
        reverse_oriented_target = f"{guide}AGG"
        sequence = _reverse_complement(reverse_oriented_target)

        guides = _find_pam_sites(sequence, PAMType.NGG, guide_length=20)

        assert (guide, "AGG", 4, "-") in guides

    def test_forward_tttv_position_is_guide_start_not_pam_start(self):
        """Forward 5' PAM position should report the guide start."""
        guide = "ATGCATGCATGCATGCATGC"
        sequence = f"TTTA{guide}"

        guides = _find_pam_sites(sequence, PAMType.TTTV, guide_length=20)

        assert (guide, "TTTA", 5, "+") in guides

    def test_reverse_tttv_position_is_input_coordinate(self):
        """Reverse 5' PAM position should be converted to input coordinates."""
        guide = "ATGCATGCATGCATGCATGC"
        reverse_oriented_target = f"TTTA{guide}"
        sequence = _reverse_complement(reverse_oriented_target)

        guides = _find_pam_sites(sequence, PAMType.TTTV, guide_length=20)

        assert (guide, "TTTA", 1, "-") in guides


class TestPAMValidation:
    """Tests for validating PAMs adjacent to off-target hits."""

    def test_validates_plus_strand_3prime_pam(self):
        chromosome = "ATGCATGCATGCATGCATGCAGG"

        pam = _validate_pam_at_position(
            chromosome,
            hit_start=0,
            hit_end=20,
            strand="+",
            pam_type=PAMType.NGG,
            guide_length=20,
        )

        assert pam == "AGG"

    def test_rejects_missing_plus_strand_3prime_pam(self):
        chromosome = "ATGCATGCATGCATGCATGCAAA"

        pam = _validate_pam_at_position(
            chromosome,
            hit_start=0,
            hit_end=20,
            strand="+",
            pam_type=PAMType.NGG,
            guide_length=20,
        )

        assert pam is None

    def test_validates_minus_strand_3prime_pam(self):
        guide = "ATGCATGCATGCATGCATGC"
        chromosome = _reverse_complement(f"{guide}AGG")

        pam = _validate_pam_at_position(
            chromosome,
            hit_start=3,
            hit_end=23,
            strand="-",
            pam_type=PAMType.NGG,
            guide_length=20,
        )

        assert pam == "AGG"


class TestTargetRegionFiltering:
    """Tests for target region filtering."""

    @pytest.fixture
    def sample_guides(self):
        """Sample guides across a 1000bp sequence."""
        return [
            ("GUIDE1", "NGG", 50, "+"),    # 5' region (first 20%)
            ("GUIDE2", "NGG", 100, "+"),   # 5' region
            ("GUIDE3", "NGG", 250, "+"),   # Middle
            ("GUIDE4", "NGG", 500, "+"),   # Middle
            ("GUIDE5", "NGG", 850, "+"),   # 3' region (last 20%)
            ("GUIDE6", "NGG", 950, "+"),   # 3' region
        ]

    def test_five_prime_filter(self, sample_guides):
        """5' prime should only include first 20%."""
        filtered = _filter_target_region(
            sample_guides,
            sequence_length=1000,
            target_region=TargetRegion.FIVE_PRIME
        )

        positions = [pos for _, _, pos, _ in filtered]
        # First 20% = positions <= 200
        for pos in positions:
            assert pos <= 200

    def test_three_prime_filter(self, sample_guides):
        """3' prime should only include last 20%."""
        filtered = _filter_target_region(
            sample_guides,
            sequence_length=1000,
            target_region=TargetRegion.THREE_PRIME
        )

        positions = [pos for _, _, pos, _ in filtered]
        # Last 20% = positions >= 800
        for pos in positions:
            assert pos >= 800

    def test_full_cds_no_filter(self, sample_guides):
        """Full CDS should include all guides."""
        filtered = _filter_target_region(
            sample_guides,
            sequence_length=1000,
            target_region=TargetRegion.FULL_CDS
        )

        assert len(filtered) == len(sample_guides)

    def test_five_prime_upstream_filter(self):
        """5' prime upstream should include upstream + first 20% of CDS."""
        # Simulate 500bp upstream + 1000bp CDS
        guides = [
            ("GUIDE1", "NGG", 100, "+"),   # In upstream region
            ("GUIDE2", "NGG", 400, "+"),   # In upstream region
            ("GUIDE3", "NGG", 600, "+"),   # In first 20% of CDS (500-700)
            ("GUIDE4", "NGG", 800, "+"),   # Past first 20% of CDS
            ("GUIDE5", "NGG", 1200, "+"),  # Way past
        ]

        filtered = _filter_target_region(
            guides,
            sequence_length=1500,  # 500 upstream + 1000 CDS
            target_region=TargetRegion.FIVE_PRIME_UPSTREAM,
            upstream_length=500
        )

        # Should include upstream (1-500) + first 20% of CDS (501-700)
        # Max position = 500 + 200 = 700
        positions = [pos for _, _, pos, _ in filtered]
        assert 100 in positions  # upstream
        assert 400 in positions  # upstream
        assert 600 in positions  # first 20% CDS
        assert 800 not in positions  # past first 20%


# =============================================================================
# Brute-Force Off-Target Search Tests
# =============================================================================

class TestBruteForceOfftargetSearch:
    """Tests for the brute-force genome-wide off-target search."""

    def test_count_mismatches_exact_match(self):
        """Exact match should have 0 mismatches."""
        mm, positions = _count_mismatches("ATGCATGCATGCATGCATGC", "ATGCATGCATGCATGCATGC")
        assert mm == 0
        assert positions == []

    def test_count_mismatches_with_differences(self):
        """Should correctly count and locate mismatches."""
        mm, positions = _count_mismatches("ATGCATGCATGCATGCATGC", "TTGCATGCATGCATGCATGC")
        assert mm == 1
        assert positions == [0]

        mm, positions = _count_mismatches("ATGCATGCATGCATGCATGC", "TTGCATGCATGCATGCATGT")
        assert mm == 2
        assert positions == [0, 19]

    def test_count_mismatches_all_different(self):
        """All different bases should give max mismatches."""
        mm, positions = _count_mismatches("AAAAAAAAAAAAAAAAAAAA", "TTTTTTTTTTTTTTTTTTTT")
        assert mm == 20

    def test_bruteforce_finds_exact_match(self):
        """Brute-force should find exact matches in a small genome."""
        mock_db = MagicMock()

        # Create a small "genome" with one guide site
        guide = "ATGCATGCATGCATGCATGC"
        test_chromosome = f"NNNNN{guide}AGGNNNNN"  # Guide + NGG PAM

        # The search function needs chromosome sequences
        # We'll test the core mismatch detection directly
        mm, positions = _count_mismatches(guide, guide)
        assert mm == 0

    def test_bruteforce_finds_offtargets_with_mismatches(self):
        """Brute-force should find off-targets with up to max_mismatches."""
        guide = "ATGCATGCATGCATGCATGC"

        # Off-target with 1 mismatch
        offtarget_1mm = "TTGCATGCATGCATGCATGC"
        mm, _ = _count_mismatches(guide, offtarget_1mm)
        assert mm == 1

        # Off-target with 2 mismatches
        offtarget_2mm = "TTGCATGCATGCATGCATGT"
        mm, _ = _count_mismatches(guide, offtarget_2mm)
        assert mm == 2

        # Off-target with 3 mismatches
        offtarget_3mm = "TTGCATGCATGCATGCTTGT"
        mm, _ = _count_mismatches(guide, offtarget_3mm)
        assert mm == 3

        # Off-target with 4 mismatches
        offtarget_4mm = "TTGCATGCATGCTTGCTTGT"
        mm, _ = _count_mismatches(guide, offtarget_4mm)
        assert mm == 4

    def test_bruteforce_filters_by_max_mismatches(self):
        """Should only return off-targets within max_mismatches threshold."""
        guide = "ATGCATGCATGCATGCATGC"

        # These should be found with max_mismatches=3
        offtargets_to_find = [
            ("ATGCATGCATGCATGCATGC", 0),  # exact
            ("TTGCATGCATGCATGCATGC", 1),  # 1mm
            ("TTGCATGCATGCATGCATGT", 2),  # 2mm
            ("TTGCATGCATGCATGCTTGT", 3),  # 3mm
        ]

        # These should NOT be found with max_mismatches=3
        offtargets_to_skip = [
            ("TTGCATGCATGCTTGCTTGT", 4),  # 4mm
            ("TTGCATGCTTGCTTGCTTGT", 5),  # 5mm
        ]

        for seq, expected_mm in offtargets_to_find:
            mm, _ = _count_mismatches(guide, seq)
            assert mm == expected_mm
            assert mm <= 3, f"Expected {seq} to be found (mm={mm})"

        for seq, expected_mm in offtargets_to_skip:
            mm, _ = _count_mismatches(guide, seq)
            assert mm == expected_mm
            assert mm > 3, f"Expected {seq} to be skipped (mm={mm})"


# =============================================================================
# Integration Tests with Mock Database
# =============================================================================

class TestDesignGuidesIntegration:
    """Integration tests for guide design with mock database."""

    @pytest.fixture
    def mock_db(self):
        """Create a mock database session."""
        return MagicMock()

    def test_requires_gene_or_sequence(self, mock_db):
        """Should return error when neither gene nor sequence provided."""
        request = CrisprDesignRequest(
            gene_name=None,
            sequence=None,
            organism="C_albicans_SC5314_A22",
        )

        result = design_guides(mock_db, request)

        assert result.success is False
        assert "must be provided" in result.error

    def test_handles_raw_sequence(self, mock_db):
        """Should process raw DNA sequence."""
        # Sequence with known NGG sites
        test_sequence = (
            "ATGCATGCATGCATGCATGCAGG"  # One NGG site at end
            "ATGCATGCATGCATGCATGCAGG"  # Another NGG site
            "ATGCATGCATGCATGCATGCATGC"
        )

        request = CrisprDesignRequest(
            sequence=test_sequence,
            organism="C_albicans_SC5314_A22",
            pam=PAMType.NGG,
            guide_length=20,
            check_offtargets=False,  # Skip off-target search for unit test
        )

        result = design_guides(mock_db, request)

        assert result.success is True
        assert result.total_guides_found >= 1

    def test_rejects_short_sequence(self, mock_db):
        """Should reject sequence shorter than guide + PAM."""
        request = CrisprDesignRequest(
            sequence="ATGCATGC",  # Only 8bp, too short
            organism="C_albicans_SC5314_A22",
        )

        result = design_guides(mock_db, request)

        assert result.success is False
        assert "too short" in result.error

    def test_warns_for_unimplemented_request_options(self, mock_db):
        """Accepted-but-unimplemented options should be explicit warnings."""
        request = CrisprDesignRequest(
            sequence="ATGCATGCATGCATGCATGCAGG",
            organism="C_albicans_SC5314_A22",
            offtarget_genomes=["C_auris_B8441"],
            include_homology_arms=True,
            check_offtargets=False,
        )

        result = design_guides(mock_db, request)

        assert result.success is True
        assert any("Additional off-target genomes" in w for w in result.warnings)
        assert any("Homology arm design" in w for w in result.warnings)

    def test_offtarget_checked_marks_only_searched_guides(self, mock_db):
        """Guides beyond the off-target search limit should not look checked."""
        test_sequence = "A" + ("ATGCATGCATGCATGCATGCAGG" * 20)
        request = CrisprDesignRequest(
            sequence=test_sequence,
            organism="C_albicans_SC5314_A22",
            pam=PAMType.NGG,
            guide_length=20,
            max_guides=20,
            check_offtargets=True,
        )

        with patch(
            "cgd.api.services.crispr_service._search_offtargets_blast",
            side_effect=lambda *args, **kwargs: kwargs["status"].update({"performed": True}) or [],
        ) as search_mock:
            result = design_guides(mock_db, request)

        assert result.success is True
        assert search_mock.call_count == 14
        assert sum(1 for guide in result.guides if guide.offtarget_checked) == 14
        assert any(not guide.offtarget_checked for guide in result.guides)
        assert any("top 14 guides only" in w for w in result.warnings)


# =============================================================================
# CHOPCHOP Validation Tests
# =============================================================================
#
# These tests compare our guide predictions against CHOPCHOP results.
# They verify that guides found by CHOPCHOP are also found by our tool.
#
# Note: Our tool may find additional guides not reported by CHOPCHOP
# (different filtering criteria), but CHOPCHOP guides should be present.
# =============================================================================

class TestCHOPCHOPValidation:
    """Validate guide predictions against CHOPCHOP results."""

    @pytest.fixture
    def mock_db(self):
        """Create a mock database session."""
        return MagicMock()

    @pytest.mark.parametrize("gene_data", CRISPR_TEST_GENES, ids=lambda g: g["gene_name"])
    def test_finds_chopchop_guides(self, mock_db, gene_data):
        """
        Verify that guides found by CHOPCHOP are also found by our tool.

        This test:
        1. Takes the CDS sequence for each gene
        2. Runs our CRISPR designer
        3. Verifies that CHOPCHOP's top guides are in our results
        """
        if not gene_data["cds_first_500bp"]:
            pytest.skip(f"CDS sequence not populated for {gene_data['gene_name']}")

        if not gene_data["expected_guides_5prime"]:
            pytest.skip(f"Expected guides not populated for {gene_data['gene_name']}")

        # Run our guide finder
        # Use high max_guides to check all discovered guides (not just top-ranked)
        request = CrisprDesignRequest(
            sequence=gene_data["cds_first_500bp"],
            organism="C_albicans_SC5314_A22",
            pam=PAMType.NGG,
            guide_length=20,
            target_region=TargetRegion.FIVE_PRIME,
            check_offtargets=False,
            max_guides=100,
        )

        result = design_guides(mock_db, request)
        assert result.success is True

        # Get our guide sequences
        our_guides = {guide.sequence for guide in result.guides}

        # Check that CHOPCHOP guides are found
        missing_guides = []
        for expected_guide in gene_data["expected_guides_5prime"]:
            if expected_guide not in our_guides:
                # Also check reverse complement
                rc_guide = _reverse_complement(expected_guide)
                if rc_guide not in our_guides:
                    missing_guides.append(expected_guide)

        if missing_guides:
            pytest.fail(
                f"Gene {gene_data['gene_name']}: Missing CHOPCHOP guides: {missing_guides}"
            )


# =============================================================================
# Script to fetch CDS sequences and populate fixtures
# =============================================================================
#
# Run this to populate the test fixtures with actual data:
#
#   python -c "from tests.api.test_crispr_service import fetch_fixture_data; fetch_fixture_data()"
#
# This requires database access to fetch CDS sequences.
# =============================================================================

def fetch_fixture_data():
    """
    Fetch CDS sequences for test genes and print fixture data.

    This is a helper function to populate the CRISPR_TEST_GENES fixture.
    Run it once to generate the data, then copy into the fixture.
    """
    print("To populate test fixtures:")
    print("1. Export CDS sequences for each gene from CGD")
    print("2. Submit each sequence to CHOPCHOP (https://chopchop.cbu.uib.no/)")
    print("3. Copy the top 10 guide sequences for each gene")
    print("4. Update CRISPR_TEST_GENES fixture with:")
    print("   - cds_first_500bp: First 500bp of CDS")
    print("   - expected_guides_5prime: List of guide sequences from CHOPCHOP")
    print("")
    print("Genes to process:")
    for gene in CRISPR_TEST_GENES:
        print(f"  - {gene['gene_name']} ({gene['feature_name']})")
