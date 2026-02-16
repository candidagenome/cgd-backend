#!/usr/bin/env python3
"""
Unit tests for scripts/blast_summary/separate_patent_matches.py

Tests the functionality to separate GenBank patent vs non-patent matches.
"""

import pytest
from pathlib import Path
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from blast_summary.separate_patent_matches import (
    separate_patent_matches,
    PATENT_PREFIXES,
    main,
)


class TestPatentPrefixes:
    """Tests for patent prefix constants."""

    def test_known_patent_prefixes(self):
        """Test that known patent prefixes are in the set."""
        # Common patent prefixes
        assert 'E' in PATENT_PREFIXES
        assert 'A' in PATENT_PREFIXES
        assert 'AX' in PATENT_PREFIXES
        assert 'BD' in PATENT_PREFIXES
        assert 'I' in PATENT_PREFIXES

    def test_non_patent_prefixes_not_included(self):
        """Test that common non-patent prefixes are not included."""
        # Standard GenBank prefixes
        assert 'NM' not in PATENT_PREFIXES
        assert 'NP' not in PATENT_PREFIXES
        assert 'XP' not in PATENT_PREFIXES
        assert 'AAA' not in PATENT_PREFIXES


class TestSeparatePatentMatches:
    """Tests for separate_patent_matches function."""

    def test_separate_basic_matches(self, temp_dir):
        """Test basic separation of patent and non-patent matches."""
        input_file = temp_dir / "blastSummary.tab"
        input_file.write_text(
            "Query\tField2\tMatch\tF4\tF5\tF6\tF7\tF8\tMatch_class\n"
            "orf19.1\tdata\tgb|E12345.1\td\te\tf\tg\th\tGood_match\n"
            "orf19.2\tdata\tgb|NM_001234.1\td\te\tf\tg\th\tGood_match\n"
        )

        stats = separate_patent_matches("test", input_file, temp_dir)

        assert stats['good_matches'] == 2
        assert stats['patent_matches'] == 1
        assert stats['non_patent_matches'] == 1

        # Check output files
        patent_file = temp_dir / "test_GBpatents_load.tab"
        non_patent_file = temp_dir / "test_GBnonpatents_load.tab"

        assert patent_file.exists()
        assert non_patent_file.exists()

        patent_content = patent_file.read_text()
        non_patent_content = non_patent_file.read_text()

        assert "orf19.1" in patent_content
        assert "E12345.1" in patent_content
        assert "orf19.2" in non_patent_content
        assert "NM_001234.1" in non_patent_content

    def test_skip_non_good_matches(self, temp_dir):
        """Test that non-Good_match entries are skipped."""
        input_file = temp_dir / "blastSummary.tab"
        input_file.write_text(
            "Query\tField2\tMatch\tF4\tF5\tF6\tF7\tF8\tMatch_class\n"
            "orf19.1\tdata\tgb|E12345.1\td\te\tf\tg\th\tGood_match\n"
            "orf19.2\tdata\tgb|NM_001234.1\td\te\tf\tg\th\tPoor_match\n"
            "orf19.3\tdata\tgb|AAA12345.1\td\te\tf\tg\th\tNo_match\n"
        )

        stats = separate_patent_matches("test", input_file, temp_dir)

        assert stats['total_matches'] == 3
        assert stats['good_matches'] == 1  # Only one Good_match

    def test_skip_header(self, temp_dir):
        """Test that the header row is skipped."""
        input_file = temp_dir / "blastSummary.tab"
        input_file.write_text(
            "Query\tField2\tMatch\tF4\tF5\tF6\tF7\tF8\tMatch_class\n"
            "orf19.1\tdata\tgb|NM_001234.1\td\te\tf\tg\th\tGood_match\n"
        )

        stats = separate_patent_matches("test", input_file, temp_dir)

        # Header should not be counted
        assert stats['total_matches'] == 1

    def test_various_patent_prefixes(self, temp_dir):
        """Test various patent accession prefixes."""
        input_file = temp_dir / "blastSummary.tab"
        lines = ["Query\tField2\tMatch\tF4\tF5\tF6\tF7\tF8\tMatch_class\n"]

        patent_prefixes_to_test = ['E', 'BD', 'AX', 'I', 'AR', 'GC']
        for i, prefix in enumerate(patent_prefixes_to_test):
            lines.append(
                f"orf19.{i}\tdata\tgb|{prefix}12345.1\td\te\tf\tg\th\tGood_match\n"
            )

        input_file.write_text("".join(lines))
        stats = separate_patent_matches("test", input_file, temp_dir)

        assert stats['patent_matches'] == len(patent_prefixes_to_test)

    def test_various_non_patent_prefixes(self, temp_dir):
        """Test various non-patent accession prefixes."""
        input_file = temp_dir / "blastSummary.tab"
        lines = ["Query\tField2\tMatch\tF4\tF5\tF6\tF7\tF8\tMatch_class\n"]

        non_patent_prefixes = ['NM', 'NP', 'XP', 'AAA', 'CAA', 'YP']
        for i, prefix in enumerate(non_patent_prefixes):
            lines.append(
                f"orf19.{i}\tdata\tgb|{prefix}12345.1\td\te\tf\tg\th\tGood_match\n"
            )

        input_file.write_text("".join(lines))
        stats = separate_patent_matches("test", input_file, temp_dir)

        assert stats['non_patent_matches'] == len(non_patent_prefixes)

    def test_accession_parsing(self, temp_dir):
        """Test different accession formats are parsed correctly."""
        input_file = temp_dir / "blastSummary.tab"
        input_file.write_text(
            "Query\tField2\tMatch\tF4\tF5\tF6\tF7\tF8\tMatch_class\n"
            "orf19.1\tdata\tgb|NM_001234.1|desc\td\te\tf\tg\th\tGood_match\n"
            "orf19.2\tdata\tref|NP_001234.1|desc\td\te\tf\tg\th\tGood_match\n"
        )

        stats = separate_patent_matches("test", input_file, temp_dir)
        assert stats['non_patent_matches'] == 2

    def test_short_lines_skipped(self, temp_dir):
        """Test that lines with too few fields are skipped."""
        input_file = temp_dir / "blastSummary.tab"
        input_file.write_text(
            "Query\tField2\tMatch\tF4\tF5\tF6\tF7\tF8\tMatch_class\n"
            "orf19.1\tdata\n"  # Too short
            "orf19.2\tdata\tgb|NM_001234.1\td\te\tf\tg\th\tGood_match\n"
        )

        stats = separate_patent_matches("test", input_file, temp_dir)
        assert stats['good_matches'] == 1

    def test_output_file_format(self, temp_dir):
        """Test the format of output files."""
        input_file = temp_dir / "blastSummary.tab"
        input_file.write_text(
            "Query\tField2\tMatch\tF4\tF5\tF6\tF7\tF8\tMatch_class\n"
            "orf19.123\tdata\tgb|NM_001234.1\td\te\tf\tg\th\tGood_match\n"
        )

        separate_patent_matches("test", input_file, temp_dir)

        non_patent_file = temp_dir / "test_GBnonpatents_load.tab"
        content = non_patent_file.read_text()

        # Should be tab-separated: query\taccession
        lines = content.strip().split('\n')
        assert len(lines) == 1

        parts = lines[0].split('\t')
        assert parts[0] == "orf19.123"
        assert parts[1] == "NM_001234.1"

    def test_default_output_dir(self, temp_dir):
        """Test that default output directory is current directory."""
        import os
        original_dir = os.getcwd()

        try:
            os.chdir(temp_dir)
            input_file = temp_dir / "blastSummary.tab"
            input_file.write_text(
                "Query\tField2\tMatch\tF4\tF5\tF6\tF7\tF8\tMatch_class\n"
                "orf19.1\tdata\tgb|NM_001234.1\td\te\tf\tg\th\tGood_match\n"
            )

            stats = separate_patent_matches("test", input_file)

            # Files should be created in current directory
            assert (Path('.') / "test_GBpatents_load.tab").exists()
            assert (Path('.') / "test_GBnonpatents_load.tab").exists()
        finally:
            os.chdir(original_dir)


class TestMainFunction:
    """Tests for the main function."""

    def test_main_basic(self, temp_dir, capsys):
        """Test main function with basic input."""
        input_file = temp_dir / "blastSummary.tab"
        input_file.write_text(
            "Query\tField2\tMatch\tF4\tF5\tF6\tF7\tF8\tMatch_class\n"
            "orf19.1\tdata\tgb|E12345.1\td\te\tf\tg\th\tGood_match\n"
            "orf19.2\tdata\tgb|NM_001234.1\td\te\tf\tg\th\tGood_match\n"
        )

        with patch.object(sys, 'argv', [
            'prog', 'candida', str(input_file), '-o', str(temp_dir)
        ]):
            main()

        captured = capsys.readouterr()
        assert "2 total matches" in captured.out or "Processed 2" in captured.out
        assert "Patent matches: 1" in captured.out
        assert "Non-patent matches: 1" in captured.out

    def test_main_nonexistent_file(self, temp_dir, capsys):
        """Test main function with nonexistent input file."""
        with patch.object(sys, 'argv', [
            'prog', 'candida', str(temp_dir / "nonexistent.tab")
        ]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "not found" in captured.err or "Error" in captured.err

    def test_main_output_dir_option(self, temp_dir, capsys):
        """Test main function with output directory option."""
        input_file = temp_dir / "blastSummary.tab"
        input_file.write_text(
            "Query\tField2\tMatch\tF4\tF5\tF6\tF7\tF8\tMatch_class\n"
            "orf19.1\tdata\tgb|NM_001234.1\td\te\tf\tg\th\tGood_match\n"
        )

        output_dir = temp_dir / "output"
        output_dir.mkdir()

        with patch.object(sys, 'argv', [
            'prog', 'species', str(input_file), '-o', str(output_dir)
        ]):
            main()

        assert (output_dir / "species_GBpatents_load.tab").exists()
        assert (output_dir / "species_GBnonpatents_load.tab").exists()


class TestEdgeCases:
    """Tests for edge cases."""

    def test_empty_file(self, temp_dir):
        """Test handling of empty input file."""
        input_file = temp_dir / "empty.tab"
        input_file.write_text("")

        stats = separate_patent_matches("test", input_file, temp_dir)

        assert stats['total_matches'] == 0
        assert stats['good_matches'] == 0

    def test_header_only_file(self, temp_dir):
        """Test handling of file with only header."""
        input_file = temp_dir / "header.tab"
        input_file.write_text(
            "Query\tField2\tMatch\tF4\tF5\tF6\tF7\tF8\tMatch_class\n"
        )

        stats = separate_patent_matches("test", input_file, temp_dir)

        assert stats['total_matches'] == 0

    def test_accession_without_prefix(self, temp_dir):
        """Test accession without letter prefix is handled."""
        input_file = temp_dir / "numeric.tab"
        input_file.write_text(
            "Query\tField2\tMatch\tF4\tF5\tF6\tF7\tF8\tMatch_class\n"
            "orf19.1\tdata\tgb|12345\td\te\tf\tg\th\tGood_match\n"
        )

        stats = separate_patent_matches("test", input_file, temp_dir)

        # Should be skipped due to no prefix
        assert stats['good_matches'] == 1
        assert stats['patent_matches'] == 0
        assert stats['non_patent_matches'] == 0

    def test_special_species_names(self, temp_dir):
        """Test species names with special characters work."""
        input_file = temp_dir / "blastSummary.tab"
        input_file.write_text(
            "Query\tField2\tMatch\tF4\tF5\tF6\tF7\tF8\tMatch_class\n"
            "orf19.1\tdata\tgb|NM_001234.1\td\te\tf\tg\th\tGood_match\n"
        )

        stats = separate_patent_matches("C_albicans", input_file, temp_dir)

        assert (temp_dir / "C_albicans_GBpatents_load.tab").exists()
        assert (temp_dir / "C_albicans_GBnonpatents_load.tab").exists()
