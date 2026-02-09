"""
Tests for Sequence Alignment Service.

Tests cover:
- Sequence cleaning and normalization
- Full alignment with Needleman-Wunsch algorithm
- Simple comparison fallback for very different sequences
- Alignment statistics calculation
- Block formatting for display
- Quick sequence comparison
"""
import pytest

from cgd.api.services.curation.seq_alignment_service import SeqAlignmentService


@pytest.fixture
def service():
    """Create a SeqAlignmentService instance."""
    return SeqAlignmentService()


class TestSequenceCleaning:
    """Tests for sequence cleaning and normalization."""

    def test_removes_whitespace(self, service):
        """Sequences with whitespace should be cleaned."""
        result = service.align_sequences("ATG CGT", "ATG CGT")
        assert result["seq1_length"] == 6
        assert result["seq2_length"] == 6

    def test_removes_newlines(self, service):
        """Sequences with newlines should be cleaned."""
        result = service.align_sequences("ATG\nCGT\nTAA", "ATGCGTTAA")
        assert result["seq1_length"] == 9
        assert result["seq2_length"] == 9
        assert result["identical"] if "identical" in result else result["matches"] == 9

    def test_converts_to_uppercase(self, service):
        """Lowercase sequences should be converted to uppercase."""
        result = service.align_sequences("atgcgt", "ATGCGT")
        assert result["matches"] == 6
        assert result["mismatches"] == 0

    def test_handles_mixed_case_and_whitespace(self, service):
        """Mixed case and whitespace should be handled together."""
        result = service.align_sequences("atg cgt\ntaa", "ATG CGT TAA")
        assert result["seq1_length"] == 9
        assert result["matches"] == 9


class TestIdenticalSequences:
    """Tests for aligning identical sequences."""

    def test_identical_short_sequences(self, service):
        """Identical short sequences should have 100% identity."""
        result = service.align_sequences("ATGCGT", "ATGCGT")
        assert result["matches"] == 6
        assert result["mismatches"] == 0
        assert result["gaps"] == 0
        assert result["identity_percent"] == 100.0

    def test_identical_longer_sequences(self, service):
        """Identical longer sequences should have 100% identity."""
        seq = "ATGCGTAACCGGTTAAGGCCTTAA"
        result = service.align_sequences(seq, seq)
        assert result["matches"] == len(seq)
        assert result["identity_percent"] == 100.0

    def test_aligned_sequences_match_input(self, service):
        """Aligned sequences should match cleaned input for identical seqs."""
        seq = "ATGCGT"
        result = service.align_sequences(seq, seq)
        assert result["aligned_seq1"] == seq
        assert result["aligned_seq2"] == seq
        assert result["symbols"] == "*" * len(seq)


class TestMismatchedSequences:
    """Tests for sequences with mismatches."""

    def test_single_mismatch(self, service):
        """Single mismatch should be detected."""
        result = service.align_sequences("ATGCGT", "ATGCAT")
        # Position 4: C vs A, Position 6: T vs T - wait let me recheck
        # ATGCGT vs ATGCAT - positions 4 and 5 differ (G vs A, T vs T)
        # Actually: A-T-G-C-G-T vs A-T-G-C-A-T
        # Pos 5: G vs A is a mismatch
        assert result["mismatches"] >= 1
        assert result["identity_percent"] < 100.0

    def test_multiple_mismatches(self, service):
        """Multiple mismatches should be counted correctly."""
        result = service.align_sequences("AAAA", "TTTT")
        assert result["mismatches"] == 4
        assert result["matches"] == 0
        assert result["identity_percent"] == 0.0

    def test_mismatch_symbols(self, service):
        """Mismatches should be marked with '.' in symbols."""
        result = service.align_sequences("AAA", "ATA")
        assert "." in result["symbols"]


class TestGappedAlignment:
    """Tests for sequences requiring gaps."""

    def test_insertion_creates_gap(self, service):
        """Insertion in seq2 should create gap in seq1."""
        result = service.align_sequences("ATGCGT", "ATGACGT")
        assert result["gaps"] >= 1
        assert "-" in result["aligned_seq1"] or "-" in result["aligned_seq2"]

    def test_deletion_creates_gap(self, service):
        """Deletion in seq2 should create gap in seq2."""
        result = service.align_sequences("ATGACGT", "ATGCGT")
        assert result["gaps"] >= 1

    def test_gap_symbols(self, service):
        """Gaps should be marked with ' ' in symbols."""
        result = service.align_sequences("ATGCGT", "ATGACGT")
        assert " " in result["symbols"]

    def test_multiple_gaps(self, service):
        """Multiple gaps should be handled correctly."""
        result = service.align_sequences("AAACCCGGG", "AAAGGG")
        # CCC is missing in seq2
        assert result["gaps"] >= 3


class TestAlignmentStatistics:
    """Tests for alignment statistics calculation."""

    def test_lengths_reported_correctly(self, service):
        """Original sequence lengths should be reported."""
        result = service.align_sequences("ATGCGT", "ATGCGTAAA")
        assert result["seq1_length"] == 6
        assert result["seq2_length"] == 9

    def test_aligned_length_includes_gaps(self, service):
        """Aligned length should include gap positions."""
        result = service.align_sequences("AAA", "AAAA")
        assert result["aligned_length"] >= 4

    def test_identity_calculation(self, service):
        """Identity percent should be calculated correctly."""
        result = service.align_sequences("AAAA", "AATA")
        # 3 matches out of 4 = 75%
        assert result["identity_percent"] == 75.0

    def test_statistics_sum_to_aligned_length(self, service):
        """Matches + mismatches + gaps should equal aligned length."""
        result = service.align_sequences("ATGCGTAA", "ATGAGTAAC")
        total = result["matches"] + result["mismatches"] + result["gaps"]
        assert total == result["aligned_length"]


class TestSequenceNames:
    """Tests for custom sequence names."""

    def test_default_names(self, service):
        """Default names should be used when not provided."""
        result = service.align_sequences("ATG", "ATG")
        assert result["seq1_name"] == "Sequence 1"
        assert result["seq2_name"] == "Sequence 2"

    def test_custom_names(self, service):
        """Custom names should be used when provided."""
        result = service.align_sequences("ATG", "ATG", "Current", "New")
        assert result["seq1_name"] == "Current"
        assert result["seq2_name"] == "New"

    def test_names_in_blocks(self, service):
        """Block positions should be included for display."""
        result = service.align_sequences("ATGCGT", "ATGCGT", "Seq1", "Seq2")
        assert len(result["blocks"]) >= 1
        block = result["blocks"][0]
        assert "seq1_start" in block
        assert "seq1_end" in block
        assert "seq2_start" in block
        assert "seq2_end" in block


class TestAlignmentBlocks:
    """Tests for alignment block formatting."""

    def test_short_sequence_single_block(self, service):
        """Short sequences should fit in single block."""
        result = service.align_sequences("ATGCGT", "ATGCGT")
        assert len(result["blocks"]) == 1

    def test_long_sequence_multiple_blocks(self, service):
        """Long sequences should be split into multiple blocks."""
        seq = "A" * 100
        result = service.align_sequences(seq, seq)
        # Default block size is 60
        assert len(result["blocks"]) == 2

    def test_block_contains_sequences_and_symbols(self, service):
        """Each block should contain seq1, seq2, and symbols."""
        result = service.align_sequences("ATGCGT", "ATGCGT")
        block = result["blocks"][0]
        assert "seq1" in block
        assert "seq2" in block
        assert "symbols" in block

    def test_block_positions_are_1_indexed(self, service):
        """Block positions should be 1-indexed (not 0-indexed)."""
        result = service.align_sequences("ATGCGT", "ATGCGT")
        block = result["blocks"][0]
        assert block["seq1_start"] == 1
        assert block["seq2_start"] == 1

    def test_block_end_positions_correct(self, service):
        """Block end positions should be correct."""
        result = service.align_sequences("ATGCGT", "ATGCGT")
        block = result["blocks"][0]
        assert block["seq1_end"] == 6
        assert block["seq2_end"] == 6


class TestSimpleComparison:
    """Tests for simple comparison (fallback for very different sequences)."""

    def test_very_different_lengths_uses_simple(self, service):
        """Very different length sequences should use simple comparison."""
        # 50% length difference triggers simple comparison
        result = service.align_sequences("A" * 10, "A" * 100)
        # Should still produce valid output
        assert "aligned_seq1" in result
        assert "aligned_seq2" in result

    def test_simple_comparison_pads_shorter(self, service):
        """Simple comparison should pad shorter sequence with gaps."""
        result = service._simple_comparison("AAA", "AAAAA")
        aligned1, aligned2, symbols = result
        assert len(aligned1) == len(aligned2) == len(symbols)
        assert aligned1 == "AAA--"
        assert aligned2 == "AAAAA"


class TestCompareSequences:
    """Tests for quick sequence comparison method."""

    def test_identical_sequences(self, service):
        """Identical sequences should be marked as identical."""
        result = service.compare_sequences("ATGCGT", "ATGCGT")
        assert result["identical"] is True
        assert result["difference_count"] == 0

    def test_different_sequences(self, service):
        """Different sequences should not be marked as identical."""
        result = service.compare_sequences("ATGCGT", "ATGCAT")
        assert result["identical"] is False
        assert result["difference_count"] >= 1

    def test_length_difference_calculated(self, service):
        """Length difference should be calculated correctly."""
        result = service.compare_sequences("ATGCGT", "ATGCGTAAA")
        assert result["length_difference"] == 3

    def test_negative_length_difference(self, service):
        """Negative length difference when seq2 is shorter."""
        result = service.compare_sequences("ATGCGTAAA", "ATGCGT")
        assert result["length_difference"] == -3

    def test_differences_list(self, service):
        """Differences should include position and characters."""
        result = service.compare_sequences("AAA", "ATA")
        assert len(result["differences"]) == 1
        diff = result["differences"][0]
        assert diff["position"] == 2
        assert diff["seq1_char"] == "A"
        assert diff["seq2_char"] == "T"

    def test_differences_limited_to_100(self, service):
        """Differences should be limited to first 100."""
        seq1 = "A" * 200
        seq2 = "T" * 200
        result = service.compare_sequences(seq1, seq2)
        assert len(result["differences"]) == 100

    def test_cleans_input_sequences(self, service):
        """Compare should clean sequences like align does."""
        result = service.compare_sequences("atg cgt", "ATG CGT")
        assert result["identical"] is True


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_sequences(self, service):
        """Empty sequences should be handled gracefully."""
        result = service.align_sequences("", "")
        assert result["seq1_length"] == 0
        assert result["seq2_length"] == 0

    def test_single_character_sequences(self, service):
        """Single character sequences should align correctly."""
        result = service.align_sequences("A", "A")
        assert result["matches"] == 1
        assert result["identity_percent"] == 100.0

    def test_single_character_mismatch(self, service):
        """Single character mismatch should work."""
        result = service.align_sequences("A", "T")
        assert result["mismatches"] == 1
        assert result["identity_percent"] == 0.0

    def test_whitespace_only_sequences(self, service):
        """Whitespace-only sequences become empty after cleaning."""
        result = service.align_sequences("   ", "   ")
        assert result["seq1_length"] == 0
        assert result["seq2_length"] == 0

    def test_protein_sequences(self, service):
        """Protein sequences (amino acids) should work."""
        result = service.align_sequences("MVLSPADKTNVKAAWGKVGAHAGEYGAEALERMFLSFPTTKTYFPHFDLSH",
                                         "MVLSPADKTNVKAAWGKVGAHAGEYGAEALERMFLSFPTTKTYFPHFDLSH")
        assert result["identity_percent"] == 100.0

    def test_mixed_nucleotides_and_ambiguity_codes(self, service):
        """Ambiguity codes (N, Y, R, etc.) should be handled."""
        result = service.align_sequences("ATGNGT", "ATGCGT")
        # N vs C is a mismatch
        assert result["mismatches"] >= 1


class TestLargeSequences:
    """Tests for handling large sequences."""

    def test_medium_sequences_use_dp(self, service):
        """Medium sequences should use dynamic programming."""
        seq1 = "ATGC" * 100  # 400 bp
        seq2 = "ATGC" * 100
        result = service.align_sequences(seq1, seq2)
        assert result["identity_percent"] == 100.0

    def test_large_sequences_fallback(self, service):
        """Very large sequences should fall back to simple comparison."""
        # Create sequences that would exceed 10M matrix cells
        seq1 = "A" * 4000
        seq2 = "A" * 3000
        result = service.align_sequences(seq1, seq2)
        # Should still produce output without error
        assert "aligned_seq1" in result
        assert result["seq1_length"] == 4000
        assert result["seq2_length"] == 3000
