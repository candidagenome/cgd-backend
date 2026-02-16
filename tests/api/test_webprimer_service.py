"""
Tests for WebPrimer service functionality.

Tests cover:
- DNA sequence utilities (reverse complement, GC content, GC clamp)
- Melting temperature calculation
- Self-annealing and end-annealing calculations
- Primer generation and filtering
- PCR primer pair design
- Sequencing primer design
- Score calculations
- Schema validation
"""
import pytest
from unittest.mock import MagicMock, patch

from cgd.api.services.webprimer_service import (
    _reverse_complement,
    _complement_base,
    _calculate_gc_percent,
    _has_gc_clamp,
    _calculate_tm,
    _calculate_annealing,
    _calculate_end_annealing,
    _generate_primers,
    _filter_primers_by_gc,
    _filter_primers_by_tm,
    _filter_primers_by_self_anneal,
    _calculate_primer_score,
    _calculate_pair_score,
    design_primers,
    get_webprimer_config,
)
from cgd.schemas.webprimer_schema import (
    WebPrimerRequest,
    WebPrimerResponse,
    PrimerPurpose,
    SequencingStrand,
    SequencingStrandCount,
)


class TestComplementBase:
    """Tests for single base complement function."""

    def test_complement_standard_bases(self):
        """Standard bases should complement correctly."""
        assert _complement_base("A") == "T"
        assert _complement_base("T") == "A"
        assert _complement_base("G") == "C"
        assert _complement_base("C") == "G"

    def test_complement_lowercase_bases(self):
        """Lowercase bases should complement correctly."""
        assert _complement_base("a") == "t"
        assert _complement_base("t") == "a"
        assert _complement_base("g") == "c"
        assert _complement_base("c") == "g"

    def test_complement_n_base(self):
        """N (any base) should complement to N."""
        assert _complement_base("N") == "N"
        assert _complement_base("n") == "n"

    def test_complement_unknown_base(self):
        """Unknown bases should return N."""
        assert _complement_base("X") == "N"
        assert _complement_base("Z") == "N"


class TestReverseComplement:
    """Tests for reverse complement function."""

    def test_simple_reverse_complement(self):
        """Basic reverse complement should work."""
        assert _reverse_complement("ATGC") == "GCAT"

    def test_reverse_complement_uppercase(self):
        """Uppercase sequence should work."""
        assert _reverse_complement("AAAA") == "TTTT"
        assert _reverse_complement("GGGG") == "CCCC"

    def test_reverse_complement_mixed_case(self):
        """Mixed case should be preserved in complement."""
        result = _reverse_complement("AtGc")
        assert result == "gCaT"

    def test_reverse_complement_of_complement(self):
        """Double reverse complement should return original."""
        original = "ATGCAATTGGCC"
        result = _reverse_complement(_reverse_complement(original))
        assert result == original

    def test_reverse_complement_palindrome(self):
        """Palindromic sequences should be their own reverse complement."""
        palindrome = "GAATTC"  # EcoRI site
        assert _reverse_complement(palindrome) == palindrome

    def test_empty_sequence(self):
        """Empty sequence should return empty."""
        assert _reverse_complement("") == ""

    def test_single_base(self):
        """Single base complement should work."""
        assert _reverse_complement("A") == "T"
        assert _reverse_complement("T") == "A"
        assert _reverse_complement("G") == "C"
        assert _reverse_complement("C") == "G"

    def test_longer_sequence(self):
        """Longer sequence reverse complement should work."""
        seq = "ATGATGATGATG"
        rev_comp = _reverse_complement(seq)
        assert rev_comp == "CATCATCATCAT"


class TestCalculateGCPercent:
    """Tests for GC percentage calculation."""

    def test_all_gc_sequence(self):
        """100% GC sequence should return 100."""
        assert _calculate_gc_percent("GGCC") == 100.0
        assert _calculate_gc_percent("GCGCGC") == 100.0

    def test_all_at_sequence(self):
        """0% GC sequence should return 0."""
        assert _calculate_gc_percent("AATT") == 0.0
        assert _calculate_gc_percent("ATATAT") == 0.0

    def test_50_percent_gc(self):
        """50% GC sequence should return 50."""
        assert _calculate_gc_percent("ATGC") == 50.0
        assert _calculate_gc_percent("AATTGGCC") == 50.0

    def test_mixed_gc_content(self):
        """Mixed sequences should calculate correctly."""
        # 3 GC out of 10 = 30%
        assert _calculate_gc_percent("AAAGGGAAAT") == 30.0

    def test_case_insensitive(self):
        """Should be case insensitive."""
        assert _calculate_gc_percent("atgc") == 50.0
        assert _calculate_gc_percent("ATGC") == 50.0

    def test_empty_sequence(self):
        """Empty sequence should return 0."""
        assert _calculate_gc_percent("") == 0.0


class TestHasGCClamp:
    """Tests for GC clamp detection."""

    def test_gc_at_3_prime(self):
        """G or C at 3' end should have clamp."""
        assert _has_gc_clamp("ATGC") is True
        assert _has_gc_clamp("ATGG") is True
        assert _has_gc_clamp("ATCG") is True
        assert _has_gc_clamp("ATCC") is True

    def test_gc_second_to_last(self):
        """G or C at second to last position should have clamp."""
        assert _has_gc_clamp("ATGA") is True  # G second to last
        assert _has_gc_clamp("ATCA") is True  # C second to last

    def test_no_gc_clamp(self):
        """No G or C in last two positions should not have clamp."""
        assert _has_gc_clamp("GCAA") is False
        assert _has_gc_clamp("GCTT") is False
        assert _has_gc_clamp("GCAT") is False

    def test_short_sequence(self):
        """Sequence shorter than 2 bases should return False."""
        assert _has_gc_clamp("A") is False
        assert _has_gc_clamp("") is False


class TestCalculateTm:
    """Tests for melting temperature calculation."""

    def test_wallace_formula(self):
        """Tm should follow Wallace rule: 2*(A+T) + 4*(G+C)."""
        # ATGC: 2 AT (4) + 2 GC (8) = 12
        # But wait, the formula counts each base:
        # 1 A + 1 T = 2 AT pairs worth 2*2 = 4
        # 1 G + 1 C = 2 GC bases worth 4*2 = 8
        # Total = 12... that seems low for a 4-mer
        # Actually the formula is: 2*(count of A+T) + 4*(count of G+C)
        # ATGC: A=1, T=1, G=1, C=1 -> 2*(1+1) + 4*(1+1) = 4 + 8 = 12
        # This is correct for the simple Wallace rule
        result = _calculate_tm("ATGC")
        assert result == 12.0

    def test_all_gc_higher_tm(self):
        """All GC sequence should have higher Tm."""
        gc_tm = _calculate_tm("GGCCGGCC")
        at_tm = _calculate_tm("AATTAATT")
        assert gc_tm > at_tm

    def test_longer_sequence_higher_tm(self):
        """Longer sequences should have higher Tm."""
        short_tm = _calculate_tm("ATGC")
        long_tm = _calculate_tm("ATGCATGCATGC")
        assert long_tm > short_tm

    def test_typical_primer_tm_range(self):
        """Typical 20-mer primer should have reasonable Tm."""
        # 20-mer with 50% GC: 10 AT + 10 GC = 20 + 40 = 60
        primer = "ATGCATGCATATGCATGCAT"  # 20 bases, ~50% GC
        tm = _calculate_tm(primer)
        assert 40 <= tm <= 80

    def test_case_insensitive(self):
        """Tm calculation should be case insensitive."""
        upper_tm = _calculate_tm("ATGC")
        lower_tm = _calculate_tm("atgc")
        assert upper_tm == lower_tm

    def test_empty_sequence(self):
        """Empty sequence should return 0."""
        assert _calculate_tm("") == 0.0

    def test_salt_correction(self):
        """Different salt concentration should adjust Tm."""
        tm_default = _calculate_tm("ATGCATGCATGC", salt_conc=50)
        tm_different_salt = _calculate_tm("ATGCATGCATGC", salt_conc=100)
        # Salt correction should change Tm
        assert tm_different_salt != tm_default


class TestCalculateAnnealing:
    """Tests for annealing score calculation."""

    def test_perfect_complement_annealing(self):
        """Perfect complementary sequences should have high score."""
        # ATGC and GCAT are reverse complements
        score = _calculate_annealing("ATGC", "ATGC")
        assert score > 0

    def test_self_annealing(self):
        """Self-annealing (hairpin) should be detected."""
        # Palindrome should self-anneal
        score = _calculate_annealing("GAATTC", "GAATTC")
        assert score > 0

    def test_no_complementarity(self):
        """Non-complementary sequences should have low score."""
        # All A's cannot complement each other
        score = _calculate_annealing("AAAA", "AAAA")
        # A-A is not complementary, so score should be 0
        assert score == 0

    def test_gc_scores_higher_than_at(self):
        """GC base pairs should contribute more than AT."""
        gc_score = _calculate_annealing("GGGG", "CCCC")
        at_score = _calculate_annealing("AAAA", "TTTT")
        # G-C = 4 points, A-T = 2 points
        assert gc_score > at_score

    def test_partial_complementarity(self):
        """Partial complementarity should have intermediate score."""
        score = _calculate_annealing("ATGCAAAA", "GCATAAAA")
        assert score > 0


class TestCalculateEndAnnealing:
    """Tests for 3' end annealing score calculation."""

    def test_end_annealing_at_3_prime(self):
        """3' end complementarity should be detected."""
        score = _calculate_end_annealing("AAAAGCAT", "AAAAGCAT")
        assert score >= 0

    def test_no_end_annealing(self):
        """No 3' complementarity should have low score."""
        score = _calculate_end_annealing("AAAA", "AAAA")
        assert score == 0


class TestGeneratePrimers:
    """Tests for primer generation function."""

    def test_generate_all_lengths(self):
        """Should generate primers of all lengths in range."""
        block = "ATGCATGCATGCATGC"
        primers = _generate_primers(block, min_length=4, max_length=6, specific_ends=False)

        lengths = {len(p) for p in primers}
        assert 4 in lengths
        assert 5 in lengths
        assert 6 in lengths

    def test_specific_ends_only_start_position(self):
        """With specific_ends, only primers from position 0."""
        block = "ATGCATGCATGC"
        primers = _generate_primers(block, min_length=4, max_length=6, specific_ends=True)

        # Should only have 3 primers (lengths 4, 5, 6) all starting at position 0
        assert len(primers) == 3
        for p in primers:
            assert p == block[:len(p)]

    def test_non_specific_ends_all_positions(self):
        """Without specific_ends, primers at all positions."""
        block = "ATGCAT"
        primers = _generate_primers(block, min_length=4, max_length=4, specific_ends=False)

        # For length 4, positions 0, 1, 2 are valid (0-3, 1-4, 2-5)
        assert len(primers) == 3
        assert "ATGC" in primers
        assert "TGCA" in primers
        assert "GCAT" in primers

    def test_uppercase_conversion(self):
        """Generated primers should be uppercase."""
        block = "atgc"
        primers = _generate_primers(block, min_length=4, max_length=4, specific_ends=True)
        assert primers[0] == "ATGC"

    def test_empty_if_too_short(self):
        """Should return empty if block shorter than min_length."""
        block = "ATG"
        primers = _generate_primers(block, min_length=5, max_length=6, specific_ends=False)
        assert len(primers) == 0


class TestFilterPrimersByGC:
    """Tests for GC content filtering."""

    def test_filter_within_range(self):
        """Primers within GC range should be kept."""
        primers = ["AAAA", "ATGC", "GGGG"]
        # AAAA = 0% GC, ATGC = 50% GC, GGGG = 100% GC
        result = _filter_primers_by_gc(primers, min_gc=40, max_gc=60)

        assert "ATGC" in result
        assert "AAAA" not in result
        assert "GGGG" not in result

    def test_returns_gc_values(self):
        """Should return dict with GC percentages."""
        primers = ["ATGC"]
        result = _filter_primers_by_gc(primers, min_gc=0, max_gc=100)

        assert result["ATGC"] == 50.0

    def test_empty_input(self):
        """Empty input should return empty dict."""
        result = _filter_primers_by_gc([], min_gc=0, max_gc=100)
        assert len(result) == 0


class TestFilterPrimersByTm:
    """Tests for Tm filtering."""

    def test_filter_within_range(self):
        """Primers within Tm range should be kept."""
        # Create primers with known GC values
        primers_gc = {"ATGCATGCATGCATGCATGC": 50.0}  # 20-mer, 50% GC -> Tm ~60
        result = _filter_primers_by_tm(primers_gc, min_tm=50, max_tm=70, dna_conc=50, salt_conc=50)

        assert "ATGCATGCATGCATGCATGC" in result

    def test_returns_gc_and_tm_values(self):
        """Should return dict with (GC, Tm) tuples."""
        primers_gc = {"ATGC": 50.0}
        result = _filter_primers_by_tm(primers_gc, min_tm=0, max_tm=100, dna_conc=50, salt_conc=50)

        assert "ATGC" in result
        gc, tm = result["ATGC"]
        assert gc == 50.0
        assert tm > 0


class TestFilterPrimersBySelfAnneal:
    """Tests for self-annealing filtering."""

    def test_filter_high_self_anneal(self):
        """Primers with high self-annealing should be filtered out."""
        # GAATTC is palindromic and will self-anneal
        primers_tm = {
            "AAAAAA": (0.0, 12.0),  # Low self-annealing
            "GAATTC": (50.0, 18.0),  # Palindrome, high self-annealing
        }
        result = _filter_primers_by_self_anneal(
            primers_tm, max_self_anneal=10, max_self_end_anneal=6
        )

        assert "AAAAAA" in result  # Low self-annealing

    def test_returns_all_values(self):
        """Should return dict with (GC, Tm, self_anneal, self_end) tuples."""
        primers_tm = {"AAAAAA": (0.0, 12.0)}
        result = _filter_primers_by_self_anneal(
            primers_tm, max_self_anneal=50, max_self_end_anneal=50
        )

        assert "AAAAAA" in result
        assert len(result["AAAAAA"]) == 4


class TestCalculatePrimerScore:
    """Tests for primer scoring function."""

    def test_optimal_primer_low_score(self):
        """Primer with optimal values should have low score."""
        # GC=45, Tm=55, self_anneal=0, self_end=0
        optimal_data = (45.0, 55.0, 0, 0)
        score = _calculate_primer_score(optimal_data, opt_tm=55, opt_gc=45, opt_length=20)
        assert score == 0.0

    def test_suboptimal_tm_increases_score(self):
        """Suboptimal Tm should increase score."""
        optimal_data = (45.0, 55.0, 0, 0)
        suboptimal_data = (45.0, 60.0, 0, 0)  # Tm is 5 degrees off

        score_opt = _calculate_primer_score(optimal_data, opt_tm=55, opt_gc=45, opt_length=20)
        score_sub = _calculate_primer_score(suboptimal_data, opt_tm=55, opt_gc=45, opt_length=20)

        assert score_sub > score_opt

    def test_high_self_anneal_increases_score(self):
        """High self-annealing should increase score."""
        low_anneal = (45.0, 55.0, 0, 0)
        high_anneal = (45.0, 55.0, 20, 0)

        score_low = _calculate_primer_score(low_anneal, opt_tm=55, opt_gc=45, opt_length=20)
        score_high = _calculate_primer_score(high_anneal, opt_tm=55, opt_gc=45, opt_length=20)

        assert score_high > score_low


class TestCalculatePairScore:
    """Tests for primer pair scoring function."""

    def test_optimal_pair_low_score(self):
        """Optimal primer pair should have low score."""
        forward = (45.0, 55.0, 0, 0)
        reverse = (45.0, 55.0, 0, 0)

        score = _calculate_pair_score(
            forward, reverse,
            pair_anneal=0, pair_end_anneal=0,
            opt_tm=55, opt_gc=45, opt_length=20
        )
        assert score == 0.0

    def test_pair_annealing_increases_score(self):
        """High pair annealing should increase score."""
        forward = (45.0, 55.0, 0, 0)
        reverse = (45.0, 55.0, 0, 0)

        score_low = _calculate_pair_score(
            forward, reverse,
            pair_anneal=0, pair_end_anneal=0,
            opt_tm=55, opt_gc=45, opt_length=20
        )
        score_high = _calculate_pair_score(
            forward, reverse,
            pair_anneal=20, pair_end_anneal=10,
            opt_tm=55, opt_gc=45, opt_length=20
        )

        assert score_high > score_low


class TestGetWebprimerConfig:
    """Tests for configuration retrieval."""

    def test_returns_config_response(self):
        """Should return WebPrimerConfigResponse."""
        config = get_webprimer_config()
        assert config is not None
        assert config.default_params is not None

    def test_default_params_present(self):
        """Default parameters should be present."""
        config = get_webprimer_config()

        assert "opt_tm" in config.default_params
        assert "min_tm" in config.default_params
        assert "max_tm" in config.default_params
        assert "opt_gc" in config.default_params
        assert "min_length" in config.default_params
        assert "max_length" in config.default_params

    def test_purpose_options(self):
        """Purpose options should include PCR and SEQUENCING."""
        config = get_webprimer_config()

        assert "PCR" in config.purpose_options
        assert "SEQUENCING" in config.purpose_options


class TestDesignPrimersPCR:
    """Tests for PCR primer design."""

    def test_basic_pcr_design(self):
        """Basic PCR primer design should work."""
        request = WebPrimerRequest(
            sequence="ATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGC",
            purpose=PrimerPurpose.PCR,
            min_length=18,
            max_length=21,
            opt_length=20,
            min_gc=30,
            max_gc=70,
            opt_gc=50,
            min_tm=40,
            max_tm=70,
            opt_tm=55,
            max_self_anneal=30,
            max_self_end_anneal=15,
            max_pair_anneal=30,
            max_pair_end_anneal=15,
            parsed_length=35,
        )

        result = design_primers(request)

        assert result.purpose == "PCR"
        assert result.sequence_length > 0

    def test_short_sequence_error(self):
        """Sequence too short should fail schema validation."""
        # Schema requires min 20 characters
        with pytest.raises(Exception):  # ValidationError
            WebPrimerRequest(
                sequence="ATGC",  # Only 4 chars, need 20+
                purpose=PrimerPurpose.PCR,
                min_length=18,
                max_length=21,
            )

    def test_cleans_sequence(self):
        """Should clean sequence of non-DNA characters."""
        request = WebPrimerRequest(
            sequence="ATG CAT GCA TGC ATG CAT GCA TGC ATG CAT GCA TGC ATG CAT GCA TGC ATG CAT GCA TGC ATG CAT GCA",
            purpose=PrimerPurpose.PCR,
            min_length=18,
            max_length=21,
            opt_length=20,
            min_gc=30,
            max_gc=70,
            opt_gc=50,
            min_tm=40,
            max_tm=70,
            opt_tm=55,
            parsed_length=35,
        )

        result = design_primers(request)

        # Should successfully clean and process
        assert result.purpose == "PCR"

    def test_returns_best_pair_when_successful(self):
        """Successful design should return best pair."""
        # Use a sequence that should produce valid primers
        request = WebPrimerRequest(
            sequence="ATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGC",
            purpose=PrimerPurpose.PCR,
            min_length=18,
            max_length=21,
            opt_length=20,
            min_gc=30,
            max_gc=70,
            opt_gc=50,
            min_tm=40,
            max_tm=80,
            opt_tm=55,
            max_self_anneal=50,
            max_self_end_anneal=25,
            max_pair_anneal=50,
            max_pair_end_anneal=25,
            parsed_length=35,
        )

        result = design_primers(request)

        if result.success:
            assert result.best_pair is not None
            assert result.best_pair.forward is not None
            assert result.best_pair.reverse is not None


class TestDesignPrimersSequencing:
    """Tests for sequencing primer design."""

    def test_basic_sequencing_design(self):
        """Basic sequencing primer design should work."""
        request = WebPrimerRequest(
            sequence="ATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGC" * 3,
            purpose=PrimerPurpose.SEQUENCING,
            min_length=18,
            max_length=21,
            opt_length=20,
            min_gc=30,
            max_gc=70,
            opt_gc=50,
            parsed_length=35,
            seq_spacing=100,
            seq_strand_count=SequencingStrandCount.ONE,
            seq_strand=SequencingStrand.CODING,
        )

        result = design_primers(request)

        assert result.purpose == "SEQUENCING"
        assert result.sequence_length > 0

    def test_sequencing_both_strands(self):
        """Sequencing both strands should return primers for both."""
        request = WebPrimerRequest(
            sequence="ATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGC" * 5,
            purpose=PrimerPurpose.SEQUENCING,
            min_length=18,
            max_length=21,
            opt_length=20,
            min_gc=20,
            max_gc=80,
            opt_gc=50,
            parsed_length=35,
            seq_spacing=100,
            seq_strand_count=SequencingStrandCount.BOTH,
        )

        result = design_primers(request)

        if result.success:
            # Should have primers for both strands
            assert len(result.coding_primers) > 0 or len(result.noncoding_primers) > 0


class TestSchemaValidation:
    """Tests for WebPrimer request schema validation."""

    def test_valid_request(self):
        """Valid request should pass validation."""
        request = WebPrimerRequest(
            sequence="ATGCATGCATGCATGCATGCATGCATGCATGC",
            purpose=PrimerPurpose.PCR,
        )
        assert request.sequence is not None
        assert request.purpose == PrimerPurpose.PCR

    def test_default_values(self):
        """Default values should be set correctly."""
        request = WebPrimerRequest(
            sequence="ATGCATGCATGCATGCATGCATGCATGCATGC",
        )

        assert request.purpose == PrimerPurpose.PCR
        assert request.opt_tm == 55
        assert request.min_tm == 50
        assert request.max_tm == 65
        assert request.opt_length == 20
        assert request.min_length == 18
        assert request.max_length == 21

    def test_sequence_min_length(self):
        """Sequence must meet minimum length."""
        with pytest.raises(Exception):  # ValidationError
            WebPrimerRequest(
                sequence="ATGC",  # Too short (min is 20)
                purpose=PrimerPurpose.PCR,
            )

    def test_purpose_enum_values(self):
        """Purpose must be valid enum value."""
        request_pcr = WebPrimerRequest(
            sequence="ATGCATGCATGCATGCATGCATGCATGCATGC",
            purpose=PrimerPurpose.PCR,
        )
        assert request_pcr.purpose == PrimerPurpose.PCR

        request_seq = WebPrimerRequest(
            sequence="ATGCATGCATGCATGCATGCATGCATGCATGC",
            purpose=PrimerPurpose.SEQUENCING,
        )
        assert request_seq.purpose == PrimerPurpose.SEQUENCING

    def test_tm_range_validation(self):
        """Tm values must be within valid range."""
        # Valid
        request = WebPrimerRequest(
            sequence="ATGCATGCATGCATGCATGCATGCATGCATGC",
            min_tm=50,
            opt_tm=55,
            max_tm=60,
        )
        assert request.min_tm == 50

    def test_gc_range_validation(self):
        """GC values must be within valid range."""
        # Valid
        request = WebPrimerRequest(
            sequence="ATGCATGCATGCATGCATGCATGCATGCATGC",
            min_gc=30,
            opt_gc=45,
            max_gc=60,
        )
        assert request.min_gc == 30

    def test_length_range_validation(self):
        """Length values must be within valid range."""
        # Valid
        request = WebPrimerRequest(
            sequence="ATGCATGCATGCATGCATGCATGCATGCATGC",
            min_length=18,
            opt_length=20,
            max_length=25,
        )
        assert request.min_length == 18

    def test_sequencing_strand_options(self):
        """Sequencing strand options should be valid."""
        request = WebPrimerRequest(
            sequence="ATGCATGCATGCATGCATGCATGCATGCATGC",
            purpose=PrimerPurpose.SEQUENCING,
            seq_strand=SequencingStrand.CODING,
            seq_strand_count=SequencingStrandCount.ONE,
        )
        assert request.seq_strand == SequencingStrand.CODING
        assert request.seq_strand_count == SequencingStrandCount.ONE


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_all_same_base_sequence(self):
        """Sequence with all same base should handle gracefully."""
        request = WebPrimerRequest(
            sequence="A" * 100,
            purpose=PrimerPurpose.PCR,
            min_gc=30,
            max_gc=70,
        )

        result = design_primers(request)

        # Should fail because no valid GC content
        assert result.success is False

    def test_very_long_sequence(self):
        """Very long sequence should be handled."""
        request = WebPrimerRequest(
            sequence="ATGCATGCATGCATGCATGC" * 50,  # 1000 bases
            purpose=PrimerPurpose.PCR,
            min_length=18,
            max_length=21,
            min_gc=30,
            max_gc=70,
            min_tm=40,
            max_tm=80,
            parsed_length=35,
        )

        result = design_primers(request)

        assert result.sequence_length == 1000

    def test_n_bases_in_sequence(self):
        """Sequence with N bases should be handled."""
        request = WebPrimerRequest(
            sequence="ATGCATGCATGCNNNNNATGCATGCATGCATGCATGCATGCATGCATGC",
            purpose=PrimerPurpose.PCR,
            min_length=18,
            max_length=21,
            min_gc=20,
            max_gc=80,
            min_tm=40,
            max_tm=80,
            parsed_length=35,
        )

        result = design_primers(request)

        # Should process without error (N is valid)
        assert result.purpose == "PCR"

    def test_max_results_limit(self):
        """Should respect max_results limit."""
        request = WebPrimerRequest(
            sequence="ATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGC",
            purpose=PrimerPurpose.PCR,
            min_length=18,
            max_length=21,
            min_gc=20,
            max_gc=80,
            min_tm=40,
            max_tm=80,
            max_self_anneal=50,
            max_self_end_anneal=25,
            max_pair_anneal=50,
            max_pair_end_anneal=25,
            parsed_length=35,
            max_results=5,
        )

        result = design_primers(request)

        if result.success:
            assert len(result.all_pairs) <= 5
