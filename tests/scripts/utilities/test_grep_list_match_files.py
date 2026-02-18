#!/usr/bin/env python3
"""
Unit tests for scripts/utilities/grep_list_match_files.py

Tests the functionality to search for IDs across multiple files.
"""

import pytest
from pathlib import Path
from io import StringIO
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from utilities.grep_list_match_files import find_id_in_files, main


class TestFindIdInFiles:
    """Tests for find_id_in_files function."""

    def test_id_found_in_single_file(self, temp_dir):
        """Test finding an ID that exists in one file."""
        file1 = temp_dir / "file1.txt"
        file2 = temp_dir / "file2.txt"

        file1.write_text("orf19.1 some data\norf19.2 other data\n")
        file2.write_text("orf19.3 some data\norf19.4 other data\n")

        result = find_id_in_files("orf19.1", [file1, file2])
        assert len(result) == 1
        assert result[0] == file1

    def test_id_found_in_multiple_files(self, temp_dir):
        """Test finding an ID that exists in multiple files."""
        file1 = temp_dir / "file1.txt"
        file2 = temp_dir / "file2.txt"

        file1.write_text("orf19.1 some data\n")
        file2.write_text("orf19.1 some data\n")

        result = find_id_in_files("orf19.1", [file1, file2])
        assert len(result) == 2

    def test_id_not_found(self, temp_dir):
        """Test searching for an ID that doesn't exist."""
        file1 = temp_dir / "file1.txt"
        file1.write_text("orf19.1 some data\n")

        result = find_id_in_files("orf19.999", [file1])
        assert len(result) == 0

    def test_partial_match(self, temp_dir):
        """Test that partial matches are found."""
        file1 = temp_dir / "file1.txt"
        file1.write_text("ID=orf19.123;Name=ACT1\n")

        # Should find orf19.123 as a substring
        result = find_id_in_files("orf19.123", [file1])
        assert len(result) == 1

    def test_empty_file(self, temp_dir):
        """Test searching in an empty file."""
        file1 = temp_dir / "file1.txt"
        file1.write_text("")

        result = find_id_in_files("orf19.1", [file1])
        assert len(result) == 0

    def test_file_read_error(self, temp_dir, capsys):
        """Test handling of unreadable files."""
        nonexistent = temp_dir / "nonexistent.txt"

        # Should handle gracefully and print warning
        result = find_id_in_files("orf19.1", [nonexistent])
        assert len(result) == 0

        # Warning should be printed to stderr
        captured = capsys.readouterr()
        assert "Warning" in captured.err or "Could not read" in captured.err


class TestMainFunction:
    """Tests for the main function."""

    def test_main_found_ids(self, temp_dir, capsys):
        """Test main function with IDs that are found."""
        id_list = temp_dir / "ids.txt"
        id_list.write_text("orf19.1\norf19.2\n")

        file1 = temp_dir / "data.txt"
        file1.write_text("orf19.1 gene data\norf19.2 gene data\n")

        with patch.object(sys, 'argv', ['prog', str(id_list), str(file1)]):
            main()

        captured = capsys.readouterr()
        assert "orf19.1" in captured.out
        assert "orf19.2" in captured.out
        assert "2/2" in captured.err  # Summary

    def test_main_missing_ids(self, temp_dir, capsys):
        """Test main function with IDs that are not found."""
        id_list = temp_dir / "ids.txt"
        id_list.write_text("orf19.1\norf19.999\n")

        file1 = temp_dir / "data.txt"
        file1.write_text("orf19.1 gene data\n")

        with patch.object(sys, 'argv', ['prog', str(id_list), str(file1)]):
            main()

        captured = capsys.readouterr()
        assert "orf19.999" in captured.out  # Missing ID printed
        assert "1 missing" in captured.err

    def test_main_missing_only_flag(self, temp_dir, capsys):
        """Test main function with --missing-only flag."""
        id_list = temp_dir / "ids.txt"
        id_list.write_text("orf19.1\norf19.999\n")

        file1 = temp_dir / "data.txt"
        file1.write_text("orf19.1 gene data\n")

        with patch.object(sys, 'argv', ['prog', str(id_list), str(file1), '-m']):
            main()

        captured = capsys.readouterr()
        # Only missing IDs should be in output
        assert "orf19.999" in captured.out
        # Found IDs should not be in output with -m flag
        lines = captured.out.strip().split('\n')
        found_lines = [l for l in lines if "orf19.1" in l and "--" in l]
        assert len(found_lines) == 0

    def test_main_found_only_flag(self, temp_dir, capsys):
        """Test main function with --found-only flag."""
        id_list = temp_dir / "ids.txt"
        id_list.write_text("orf19.1\norf19.999\n")

        file1 = temp_dir / "data.txt"
        file1.write_text("orf19.1 gene data\n")

        with patch.object(sys, 'argv', ['prog', str(id_list), str(file1), '-f']):
            main()

        captured = capsys.readouterr()
        # Only found IDs should be in output
        assert "orf19.1" in captured.out
        # Missing IDs should not be printed (only in summary)
        lines = captured.out.strip().split('\n')
        missing_lines = [l for l in lines if l.strip() == "orf19.999"]
        assert len(missing_lines) == 0

    def test_main_multiple_search_files(self, temp_dir, capsys):
        """Test main function with multiple search files."""
        id_list = temp_dir / "ids.txt"
        id_list.write_text("orf19.1\n")

        file1 = temp_dir / "data1.txt"
        file2 = temp_dir / "data2.txt"
        file1.write_text("orf19.1 gene data\n")
        file2.write_text("orf19.1 gene data\n")

        with patch.object(sys, 'argv', ['prog', str(id_list), str(file1), str(file2)]):
            main()

        captured = capsys.readouterr()
        # Should show ID found in both files
        assert "orf19.1" in captured.out
        assert "data1.txt" in captured.out
        assert "data2.txt" in captured.out

    def test_main_nonexistent_id_file(self, temp_dir, capsys):
        """Test main function with nonexistent ID list file."""
        id_list = temp_dir / "nonexistent.txt"
        file1 = temp_dir / "data.txt"
        file1.write_text("orf19.1 gene data\n")

        with patch.object(sys, 'argv', ['prog', str(id_list), str(file1)]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "not found" in captured.err

    def test_main_nonexistent_search_file(self, temp_dir, capsys):
        """Test main function with nonexistent search file."""
        id_list = temp_dir / "ids.txt"
        id_list.write_text("orf19.1\n")
        nonexistent = temp_dir / "nonexistent.txt"

        with patch.object(sys, 'argv', ['prog', str(id_list), str(nonexistent)]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1

    def test_main_empty_lines_skipped(self, temp_dir, capsys):
        """Test that empty lines in ID list are skipped."""
        id_list = temp_dir / "ids.txt"
        id_list.write_text("orf19.1\n\norf19.2\n\n")

        file1 = temp_dir / "data.txt"
        file1.write_text("orf19.1 gene data\norf19.2 gene data\n")

        with patch.object(sys, 'argv', ['prog', str(id_list), str(file1)]):
            main()

        captured = capsys.readouterr()
        assert "2/2" in captured.err  # Only 2 IDs processed
