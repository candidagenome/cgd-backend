#!/usr/bin/env python3
"""
Unit tests for scripts/pathways/swap_gene_for_orf19.py

Tests the ORF19 to gene name swapping functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from pathways.swap_gene_for_orf19 import load_gene_mapping, swap_names, main


class TestLoadGeneMapping:
    """Tests for load_gene_mapping function."""

    def test_load_basic_mapping(self, temp_file):
        """Test loading basic ORF19 to gene mapping."""
        content = """orf19.1\tACT1
orf19.2\tTUB1
orf19.3\tCDC42
"""
        features_file = temp_file("features.tab", content)
        gene_map = load_gene_mapping(features_file)

        assert len(gene_map) == 3
        assert gene_map["orf19.1"] == "ACT1"
        assert gene_map["orf19.2"] == "TUB1"
        assert gene_map["orf19.3"] == "CDC42"

    def test_missing_gene_uses_orf(self, temp_file):
        """Test that missing gene name falls back to ORF19 ID."""
        content = """orf19.1\tACT1
orf19.2\t
orf19.3\tCDC42
"""
        features_file = temp_file("features.tab", content)
        gene_map = load_gene_mapping(features_file)

        assert gene_map["orf19.1"] == "ACT1"
        assert gene_map["orf19.2"] == "orf19.2"  # Falls back to ORF ID
        assert gene_map["orf19.3"] == "CDC42"

    def test_single_column_file(self, temp_file):
        """Test file with only ORF19 IDs."""
        content = """orf19.1
orf19.2
orf19.3
"""
        features_file = temp_file("features.tab", content)
        gene_map = load_gene_mapping(features_file)

        assert len(gene_map) == 3
        assert gene_map["orf19.1"] == "orf19.1"
        assert gene_map["orf19.2"] == "orf19.2"
        assert gene_map["orf19.3"] == "orf19.3"

    def test_extra_columns_ignored(self, temp_file):
        """Test that extra columns are ignored."""
        content = """orf19.1\tACT1\textra1\textra2
orf19.2\tTUB1\textra3
"""
        features_file = temp_file("features.tab", content)
        gene_map = load_gene_mapping(features_file)

        assert gene_map["orf19.1"] == "ACT1"
        assert gene_map["orf19.2"] == "TUB1"

    def test_empty_file(self, temp_file):
        """Test loading empty file."""
        features_file = temp_file("features.tab", "")
        gene_map = load_gene_mapping(features_file)

        assert len(gene_map) == 0


class TestSwapNames:
    """Tests for swap_names function."""

    def test_swap_basic(self, temp_dir):
        """Test basic name swapping."""
        input_file = temp_dir / "input.txt"
        input_file.write_text("""col0\tcol1\torf19.1\tcol3
col0\tcol1\torf19.2\tcol3
""")
        output_file = temp_dir / "output.txt"

        gene_map = {"orf19.1": "ACT1", "orf19.2": "TUB1"}
        swaps = swap_names(input_file, output_file, gene_map, name_column=2)

        assert swaps == 2

        content = output_file.read_text()
        assert "ACT1" in content
        assert "TUB1" in content
        assert "orf19.1" not in content
        assert "orf19.2" not in content

    def test_swap_preserves_other_columns(self, temp_dir):
        """Test that other columns are preserved."""
        input_file = temp_dir / "input.txt"
        input_file.write_text("data1\tdata2\torf19.1\tdata4\n")
        output_file = temp_dir / "output.txt"

        gene_map = {"orf19.1": "ACT1"}
        swap_names(input_file, output_file, gene_map, name_column=2)

        content = output_file.read_text()
        parts = content.strip().split('\t')
        assert parts[0] == "data1"
        assert parts[1] == "data2"
        assert parts[2] == "ACT1"
        assert parts[3] == "data4"

    def test_swap_no_matches(self, temp_dir):
        """Test when no IDs match."""
        input_file = temp_dir / "input.txt"
        input_file.write_text("col0\tcol1\tunknown\tcol3\n")
        output_file = temp_dir / "output.txt"

        gene_map = {"orf19.1": "ACT1"}
        swaps = swap_names(input_file, output_file, gene_map, name_column=2)

        assert swaps == 0

        content = output_file.read_text()
        assert "unknown" in content

    def test_swap_different_column(self, temp_dir):
        """Test swapping in different column."""
        input_file = temp_dir / "input.txt"
        input_file.write_text("orf19.1\tdata\n")
        output_file = temp_dir / "output.txt"

        gene_map = {"orf19.1": "ACT1"}
        swaps = swap_names(input_file, output_file, gene_map, name_column=0)

        assert swaps == 1

        content = output_file.read_text()
        assert "ACT1" in content
        assert content.startswith("ACT1\t")

    def test_swap_short_lines(self, temp_dir):
        """Test handling lines shorter than name_column."""
        input_file = temp_dir / "input.txt"
        input_file.write_text("short\n")
        output_file = temp_dir / "output.txt"

        gene_map = {"orf19.1": "ACT1"}
        swaps = swap_names(input_file, output_file, gene_map, name_column=2)

        assert swaps == 0

        content = output_file.read_text()
        assert "short" in content


class TestMainFunction:
    """Tests for the main function."""

    def test_main_basic(self, temp_dir, capsys):
        """Test basic main function execution."""
        features_file = temp_dir / "features.tab"
        features_file.write_text("orf19.1\tACT1\norf19.2\tTUB1\n")

        input_file = temp_dir / "input.txt"
        input_file.write_text("""col0\tcol1\torf19.1\tcol3
col0\tcol1\torf19.2\tcol3
""")
        output_file = temp_dir / "output.txt"

        with patch.object(sys, 'argv', [
            'prog',
            '--features', str(features_file),
            '--input', str(input_file),
            '--output', str(output_file)
        ]):
            main()

        content = output_file.read_text()
        assert "ACT1" in content
        assert "TUB1" in content

        captured = capsys.readouterr()
        assert "Loaded 2 gene mappings" in captured.err
        assert "Performed 2 name swaps" in captured.err

    def test_main_custom_column(self, temp_dir, capsys):
        """Test main with custom column index."""
        features_file = temp_dir / "features.tab"
        features_file.write_text("orf19.1\tACT1\n")

        input_file = temp_dir / "input.txt"
        input_file.write_text("orf19.1\tdata\n")
        output_file = temp_dir / "output.txt"

        with patch.object(sys, 'argv', [
            'prog',
            '--features', str(features_file),
            '--input', str(input_file),
            '--output', str(output_file),
            '--column', '0'
        ]):
            main()

        content = output_file.read_text()
        assert "ACT1" in content

    def test_main_missing_features_file(self, temp_dir, capsys):
        """Test main with missing features file."""
        input_file = temp_dir / "input.txt"
        input_file.write_text("data\n")

        with patch.object(sys, 'argv', [
            'prog',
            '--features', str(temp_dir / 'nonexistent.tab'),
            '--input', str(input_file)
        ]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "not found" in captured.err

    def test_main_missing_input_file(self, temp_dir, capsys):
        """Test main with missing input file."""
        features_file = temp_dir / "features.tab"
        features_file.write_text("orf19.1\tACT1\n")

        with patch.object(sys, 'argv', [
            'prog',
            '--features', str(features_file),
            '--input', str(temp_dir / 'nonexistent.txt')
        ]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1

    def test_main_stdout_output(self, temp_dir, capsys):
        """Test main outputs to stdout when no output file specified."""
        features_file = temp_dir / "features.tab"
        features_file.write_text("orf19.1\tACT1\n")

        input_file = temp_dir / "input.txt"
        input_file.write_text("col0\tcol1\torf19.1\n")

        with patch.object(sys, 'argv', [
            'prog',
            '--features', str(features_file),
            '--input', str(input_file)
        ]):
            main()

        captured = capsys.readouterr()
        assert "ACT1" in captured.out


class TestEdgeCases:
    """Tests for edge cases."""

    def test_multiple_same_ids(self, temp_dir):
        """Test file with same ID appearing multiple times."""
        input_file = temp_dir / "input.txt"
        input_file.write_text("""col0\tcol1\torf19.1\tcol3
col0\tcol1\torf19.1\tcol3
col0\tcol1\torf19.1\tcol3
""")
        output_file = temp_dir / "output.txt"

        gene_map = {"orf19.1": "ACT1"}
        swaps = swap_names(input_file, output_file, gene_map, name_column=2)

        assert swaps == 3

    def test_special_characters_preserved(self, temp_dir):
        """Test that special characters in other fields are preserved."""
        input_file = temp_dir / "input.txt"
        input_file.write_text("data,with;special|chars\tmore\torf19.1\n")
        output_file = temp_dir / "output.txt"

        gene_map = {"orf19.1": "ACT1"}
        swap_names(input_file, output_file, gene_map, name_column=2)

        content = output_file.read_text()
        assert "data,with;special|chars" in content

    def test_unicode_characters(self, temp_dir):
        """Test handling of unicode characters."""
        input_file = temp_dir / "input.txt"
        input_file.write_text("α-protein\tβ-fold\torf19.1\n")
        output_file = temp_dir / "output.txt"

        gene_map = {"orf19.1": "ACT1"}
        swap_names(input_file, output_file, gene_map, name_column=2)

        content = output_file.read_text()
        assert "α-protein" in content
        assert "β-fold" in content

    def test_empty_input_file(self, temp_dir):
        """Test handling of empty input file."""
        input_file = temp_dir / "input.txt"
        input_file.write_text("")
        output_file = temp_dir / "output.txt"

        gene_map = {"orf19.1": "ACT1"}
        swaps = swap_names(input_file, output_file, gene_map, name_column=2)

        assert swaps == 0
        assert output_file.read_text() == ""
