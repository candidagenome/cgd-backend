#!/usr/bin/env python3
"""
Unit tests for scripts/utilities/get_gff_attrib_combinations.py

Tests the GFF attribute combination extraction functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from utilities.get_gff_attrib_combinations import (
    parse_gff_attributes,
    get_attrib_combinations,
    DEFAULT_ATTRIBS,
)


class TestParseGffAttributes:
    """Tests for parse_gff_attributes function."""

    def test_basic_parsing(self):
        """Test parsing basic attribute string."""
        attr_str = "ID=gene1;Name=ACT1;Alias=orf19.1"
        result = parse_gff_attributes(attr_str)

        assert result["ID"] == "gene1"
        assert result["Name"] == "ACT1"
        assert result["Alias"] == "orf19.1"

    def test_single_attribute(self):
        """Test parsing single attribute."""
        attr_str = "ID=gene1"
        result = parse_gff_attributes(attr_str)

        assert result["ID"] == "gene1"
        assert len(result) == 1

    def test_value_with_equals(self):
        """Test value containing equals sign."""
        attr_str = "Note=a=b;ID=gene1"
        result = parse_gff_attributes(attr_str)

        assert result["Note"] == "a=b"
        assert result["ID"] == "gene1"

    def test_empty_string(self):
        """Test parsing empty string."""
        result = parse_gff_attributes("")
        assert result == {}

    def test_trailing_semicolon(self):
        """Test string with trailing semicolon."""
        attr_str = "ID=gene1;Name=ACT1;"
        result = parse_gff_attributes(attr_str)

        assert result["ID"] == "gene1"
        assert result["Name"] == "ACT1"

    def test_url_encoded_values(self):
        """Test URL-encoded values are preserved."""
        attr_str = "Note=Gene%20with%20spaces;ID=gene1"
        result = parse_gff_attributes(attr_str)

        assert result["Note"] == "Gene%20with%20spaces"


class TestGetAttribCombinations:
    """Tests for get_attrib_combinations function."""

    def test_basic_extraction(self, temp_file):
        """Test basic attribute extraction."""
        gff_content = """##gff-version 3
chr1\t.\tgene\t1\t100\t.\t+\t.\tID=gene1;broad_update_type=new;broad_update_accepted=yes
chr1\t.\tgene\t200\t300\t.\t+\t.\tID=gene2;broad_update_type=modified;broad_update_accepted=no
"""
        gff_file = temp_file("test.gff", gff_content)

        attribs = ["broad_update_type", "broad_update_accepted"]
        result = get_attrib_combinations(gff_file, attribs)

        assert len(result) == 2

    def test_skip_comments(self, temp_file):
        """Test that comment lines are skipped."""
        gff_content = """##gff-version 3
#This is a comment
chr1\t.\tgene\t1\t100\t.\t+\t.\tID=gene1;broad_update_type=new
"""
        gff_file = temp_file("test.gff", gff_content)

        result = get_attrib_combinations(gff_file, ["broad_update_type"])

        assert len(result) == 1

    def test_skip_empty_lines(self, temp_file):
        """Test that empty lines are skipped."""
        gff_content = """##gff-version 3

chr1\t.\tgene\t1\t100\t.\t+\t.\tID=gene1;broad_update_type=new

"""
        gff_file = temp_file("test.gff", gff_content)

        result = get_attrib_combinations(gff_file, ["broad_update_type"])

        assert len(result) == 1

    def test_stop_at_fasta(self, temp_file):
        """Test that processing stops at #FASTA section."""
        gff_content = """##gff-version 3
chr1\t.\tgene\t1\t100\t.\t+\t.\tID=gene1;broad_update_type=new
##FASTA
>chr1
ATGC
"""
        gff_file = temp_file("test.gff", gff_content)

        result = get_attrib_combinations(gff_file, ["broad_update_type"])

        assert len(result) == 1

    def test_short_lines_skipped(self, temp_file):
        """Test that lines with fewer than 9 columns are skipped."""
        gff_content = """##gff-version 3
chr1\t.\tgene\t1\t100
chr1\t.\tgene\t1\t100\t.\t+\t.\tID=gene1;broad_update_type=new
"""
        gff_file = temp_file("test.gff", gff_content)

        result = get_attrib_combinations(gff_file, ["broad_update_type"])

        assert len(result) == 1

    def test_count_duplicates(self, temp_file):
        """Test that duplicate combinations are counted."""
        gff_content = """##gff-version 3
chr1\t.\tgene\t1\t100\t.\t+\t.\tID=gene1;broad_update_type=new
chr1\t.\tgene\t200\t300\t.\t+\t.\tID=gene2;broad_update_type=new
chr1\t.\tgene\t400\t500\t.\t+\t.\tID=gene3;broad_update_type=new
"""
        gff_file = temp_file("test.gff", gff_content)

        result = get_attrib_combinations(gff_file, ["broad_update_type"])

        total = sum(result.values())
        assert total == 3

    def test_missing_attributes_ignored(self, temp_file):
        """Test that lines missing requested attributes are handled."""
        gff_content = """##gff-version 3
chr1\t.\tgene\t1\t100\t.\t+\t.\tID=gene1
chr1\t.\tgene\t200\t300\t.\t+\t.\tID=gene2;broad_update_type=new
"""
        gff_file = temp_file("test.gff", gff_content)

        result = get_attrib_combinations(gff_file, ["broad_update_type"])

        assert len(result) == 1

    def test_empty_file(self, temp_file):
        """Test handling of empty file."""
        gff_file = temp_file("test.gff", "")

        result = get_attrib_combinations(gff_file, DEFAULT_ATTRIBS)

        assert len(result) == 0


class TestDefaultAttribs:
    """Tests for default attributes constant."""

    def test_default_attribs_defined(self):
        """Test that default attributes are defined."""
        assert len(DEFAULT_ATTRIBS) > 0

    def test_default_attribs_content(self):
        """Test that expected attributes are in defaults."""
        assert "broad_update_type" in DEFAULT_ATTRIBS
        assert "broad_update_accepted" in DEFAULT_ATTRIBS


class TestMainFunction:
    """Tests for the main function."""

    def test_main_basic(self, temp_file, capsys):
        """Test basic main function execution."""
        from utilities.get_gff_attrib_combinations import main

        gff_content = """##gff-version 3
chr1\t.\tgene\t1\t100\t.\t+\t.\tID=gene1;broad_update_type=new
"""
        gff_file = temp_file("test.gff", gff_content)

        with patch.object(sys, 'argv', [
            'prog', str(gff_file), '-a', 'broad_update_type'
        ]):
            main()

        captured = capsys.readouterr()
        assert "broad_update_type = new" in captured.out

    def test_main_with_counts(self, temp_file, capsys):
        """Test main with counts flag."""
        from utilities.get_gff_attrib_combinations import main

        gff_content = """##gff-version 3
chr1\t.\tgene\t1\t100\t.\t+\t.\tID=gene1;broad_update_type=new
chr1\t.\tgene\t200\t300\t.\t+\t.\tID=gene2;broad_update_type=new
"""
        gff_file = temp_file("test.gff", gff_content)

        with patch.object(sys, 'argv', [
            'prog', str(gff_file), '-a', 'broad_update_type', '--counts'
        ]):
            main()

        captured = capsys.readouterr()
        assert "(2)" in captured.out

    def test_main_missing_file(self, temp_dir):
        """Test main with missing file."""
        from utilities.get_gff_attrib_combinations import main

        with patch.object(sys, 'argv', [
            'prog', str(temp_dir / 'nonexistent.gff')
        ]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1

    def test_main_default_attributes(self, temp_file, capsys):
        """Test main uses default attributes when none specified."""
        from utilities.get_gff_attrib_combinations import main

        gff_content = """##gff-version 3
chr1\t.\tgene\t1\t100\t.\t+\t.\tID=gene1;broad_update_type=new;broad_update_accepted=yes
"""
        gff_file = temp_file("test.gff", gff_content)

        with patch.object(sys, 'argv', ['prog', str(gff_file)]):
            main()

        captured = capsys.readouterr()
        assert "broad_update_type" in captured.out


class TestEdgeCases:
    """Tests for edge cases."""

    def test_special_characters_in_values(self, temp_file):
        """Test attributes with special characters."""
        gff_content = """##gff-version 3
chr1\t.\tgene\t1\t100\t.\t+\t.\tID=gene1;Note=value%3Bwith%3Bsemicolons
"""
        gff_file = temp_file("test.gff", gff_content)

        result = get_attrib_combinations(gff_file, ["Note"])

        assert len(result) == 1

    def test_multiple_equals_in_value(self, temp_file):
        """Test attribute value with multiple equals signs."""
        gff_content = """##gff-version 3
chr1\t.\tgene\t1\t100\t.\t+\t.\tID=gene1;formula=a=b=c
"""
        gff_file = temp_file("test.gff", gff_content)

        result = get_attrib_combinations(gff_file, ["formula"])

        assert len(result) == 1

    def test_unicode_in_attributes(self, temp_file):
        """Test handling of unicode characters."""
        gff_content = """##gff-version 3
chr1\t.\tgene\t1\t100\t.\t+\t.\tID=gene1;Note=α-helix
"""
        gff_file = temp_file("test.gff", gff_content)

        result = get_attrib_combinations(gff_file, ["Note"])

        assert len(result) == 1
