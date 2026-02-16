#!/usr/bin/env python3
"""
Unit tests for scripts/patmatch/patmatch_to_nrgrep.py

Tests the Patmatch to nrgrep pattern conversion functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from patmatch.patmatch_to_nrgrep import (
    PatmatchConverter,
    NUCLEOTIDE,
    PEPTIDE,
    COMPLEMENT,
    NUCLEOTIDE_IUPAC,
    PEPTIDE_IUPAC,
)


class TestPatmatchConverterNucleotide:
    """Tests for nucleotide pattern conversion."""

    def test_simple_pattern(self):
        """Test conversion of a simple nucleotide pattern."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("ATG")
        assert result == "(ATG)"

    def test_wildcard_n(self):
        """Test conversion of N wildcard."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("ATN")
        assert result == "(AT.)"

    def test_wildcard_x(self):
        """Test conversion of X wildcard."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("ATX")
        assert result == "(AT.)"

    def test_iupac_r(self):
        """Test conversion of IUPAC R code (A or G)."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("ATR")
        assert "[AG]" in result

    def test_iupac_y(self):
        """Test conversion of IUPAC Y code (C or T)."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("ATY")
        assert "[CT]" in result

    def test_iupac_s(self):
        """Test conversion of IUPAC S code (G or C)."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("ATS")
        assert "[GC]" in result

    def test_iupac_w(self):
        """Test conversion of IUPAC W code (A or T)."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("ATW")
        assert "[AT]" in result

    def test_start_anchor(self):
        """Test conversion of start anchor."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("<ATG")
        assert result.startswith("^")
        assert "ATG" in result

    def test_end_anchor(self):
        """Test conversion of end anchor."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("TAA>")
        assert result.endswith("$")
        assert "TAA" in result

    def test_both_anchors(self):
        """Test conversion with both anchors."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("<ATG>")
        assert result.startswith("^")
        assert result.endswith("$")

    def test_character_class(self):
        """Test conversion of character class."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("AT[CG]")
        assert "[CG]" in result or "[GC]" in result

    def test_whitespace_removed(self):
        """Test that whitespace is removed."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("A T G")
        assert " " not in result
        assert "ATG" in result

    def test_lowercase_converted(self):
        """Test that lowercase is converted to uppercase."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("atg")
        assert "ATG" in result


class TestPatmatchConverterRepetitions:
    """Tests for repetition pattern conversion."""

    def test_exact_repetition(self):
        """Test conversion of exact repetition {n}."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("A{3}")
        # Should produce AAA
        assert "AAA" in result

    def test_minimum_repetition(self):
        """Test conversion of minimum repetition {n,}."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("A{2,}")
        # Should produce AA followed by A*
        assert "AA" in result
        assert "A*" in result

    def test_maximum_repetition(self):
        """Test conversion of maximum repetition {,n}."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("A{,3}")
        # Should produce A?A?A?
        assert "A?" in result

    def test_range_repetition(self):
        """Test conversion of range repetition {m,n}."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("A{2,4}")
        # Should produce AAA?A?
        assert "AA" in result
        assert "A?" in result

    def test_group_repetition(self):
        """Test conversion of group repetition."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("(AT){2}")
        # Should repeat the group
        assert "(AT)(AT)" in result


class TestPatmatchConverterPeptide:
    """Tests for peptide pattern conversion."""

    def test_simple_peptide(self):
        """Test conversion of simple peptide pattern."""
        converter = PatmatchConverter(PEPTIDE)
        result = converter.convert("MET")
        assert result == "(MET)"

    def test_peptide_wildcard(self):
        """Test conversion of peptide X wildcard."""
        converter = PatmatchConverter(PEPTIDE)
        result = converter.convert("MXE")
        assert "M.E" in result

    def test_peptide_iupac_j(self):
        """Test conversion of IUPAC J code (hydrophobic)."""
        converter = PatmatchConverter(PEPTIDE)
        result = converter.convert("MJE")
        assert "[IFVLWMAGCY]" in result

    def test_peptide_iupac_o(self):
        """Test conversion of IUPAC O code (hydrophilic)."""
        converter = PatmatchConverter(PEPTIDE)
        result = converter.convert("MOE")
        assert "[TSHEDQNKR]" in result

    def test_peptide_iupac_b(self):
        """Test conversion of IUPAC B code (D or N)."""
        converter = PatmatchConverter(PEPTIDE)
        result = converter.convert("MBE")
        assert "[DN]" in result

    def test_peptide_iupac_z(self):
        """Test conversion of IUPAC Z code (E or Q)."""
        converter = PatmatchConverter(PEPTIDE)
        result = converter.convert("MZE")
        assert "[EQ]" in result


class TestPatmatchConverterComplement:
    """Tests for reverse complement pattern conversion."""

    def test_simple_complement(self):
        """Test reverse complement of simple pattern."""
        converter = PatmatchConverter(COMPLEMENT)
        result = converter.convert("ATG")
        # Complement of ATG is TAC, reversed is CAT
        assert "CAT" in result

    def test_complement_with_iupac(self):
        """Test reverse complement with IUPAC codes."""
        converter = PatmatchConverter(COMPLEMENT)
        result = converter.convert("ATR")
        # R (A or G) complements to Y (C or T)
        assert "[CT]" in result or "Y" in result

    def test_complement_anchors(self):
        """Test that anchors are swapped in complement."""
        converter = PatmatchConverter(COMPLEMENT)
        result = converter.convert("<ATG>")
        # < and > should be swapped in complement
        # Pattern with < becomes > after complement
        assert "^" in result or "$" in result


class TestNestedBrackets:
    """Tests for nested bracket handling."""

    def test_nested_brackets_removed(self):
        """Test that nested brackets are simplified."""
        converter = PatmatchConverter(NUCLEOTIDE)
        # After IUPAC substitution, we might get nested brackets
        result = converter._remove_nested_brackets("TA[A[CT]]")
        assert "[[" not in result
        assert result == "TA[ACT]"

    def test_duplicate_chars_in_brackets(self):
        """Test that duplicate chars in brackets are removed."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter._remove_nested_brackets("TA[ATAG]")
        # Should remove duplicate A
        assert result == "TA[ATG]"


class TestRepetitionProcessing:
    """Tests for repetition info processing."""

    def test_process_exact_repeat(self):
        """Test processing exact repeat info."""
        converter = PatmatchConverter(NUCLEOTIDE)
        lower, upper = converter._process_repeat_info("3")
        assert lower == 3
        assert upper == 3

    def test_process_min_repeat(self):
        """Test processing minimum repeat info."""
        converter = PatmatchConverter(NUCLEOTIDE)
        lower, upper = converter._process_repeat_info("3,")
        assert lower == 3
        assert upper == -1  # INFINITE

    def test_process_max_repeat(self):
        """Test processing maximum repeat info."""
        converter = PatmatchConverter(NUCLEOTIDE)
        lower, upper = converter._process_repeat_info(",5")
        assert lower == 0
        assert upper == 5

    def test_process_range_repeat(self):
        """Test processing range repeat info."""
        converter = PatmatchConverter(NUCLEOTIDE)
        lower, upper = converter._process_repeat_info("2,5")
        assert lower == 2
        assert upper == 5


class TestMainFunction:
    """Tests for the main function."""

    def test_main_nucleotide(self, capsys):
        """Test main with nucleotide pattern."""
        with patch.object(sys, 'argv', ['prog', '-n', 'ATG']):
            from patmatch.patmatch_to_nrgrep import main
            main()

        captured = capsys.readouterr()
        assert "(ATG)" in captured.out

    def test_main_peptide(self, capsys):
        """Test main with peptide pattern."""
        with patch.object(sys, 'argv', ['prog', '-p', 'MET']):
            from patmatch.patmatch_to_nrgrep import main
            main()

        captured = capsys.readouterr()
        assert "(MET)" in captured.out

    def test_main_complement(self, capsys):
        """Test main with complement pattern."""
        with patch.object(sys, 'argv', ['prog', '-c', 'ATG']):
            from patmatch.patmatch_to_nrgrep import main
            main()

        captured = capsys.readouterr()
        assert "CAT" in captured.out

    def test_main_requires_pattern_type(self):
        """Test that main requires pattern type flag."""
        with patch.object(sys, 'argv', ['prog', 'ATG']):
            with pytest.raises(SystemExit):
                from patmatch.patmatch_to_nrgrep import main
                main()


class TestEdgeCases:
    """Tests for edge cases."""

    def test_empty_pattern(self):
        """Test handling of empty pattern."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("")
        assert result == "()"

    def test_single_char_pattern(self):
        """Test handling of single character pattern."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("A")
        assert result == "(A)"

    def test_all_wildcards(self):
        """Test pattern with all wildcards."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("NNN")
        assert result == "(...)"

    def test_complex_pattern(self):
        """Test complex pattern with multiple features."""
        converter = PatmatchConverter(NUCLEOTIDE)
        result = converter.convert("<ATG[CT]N{3,5}TAA>")
        assert result.startswith("^")
        assert result.endswith("$")
        assert "ATG" in result
        assert "[CT]" in result
        assert "TAA" in result
