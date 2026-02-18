#!/usr/bin/env python3
"""
Unit tests for scripts/proteins/cull_seq.py

Tests the sequence culling functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch
from io import StringIO

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from proteins.cull_seq import load_cull_list, cull_sequences, main


class TestLoadCullList:
    """Tests for load_cull_list function."""

    def test_load_simple_list(self, temp_file):
        """Test loading a simple ID list."""
        content = """id1
id2
id3
"""
        list_file = temp_file("cull.txt", content)
        ids = load_cull_list(list_file)

        assert len(ids) == 3
        assert "id1" in ids
        assert "id2" in ids
        assert "id3" in ids

    def test_ignore_comments(self, temp_file):
        """Test that comments are ignored."""
        content = """# This is a comment
id1
# Another comment
id2
"""
        list_file = temp_file("cull.txt", content)
        ids = load_cull_list(list_file)

        assert len(ids) == 2
        assert "id1" in ids
        assert "id2" in ids

    def test_ignore_empty_lines(self, temp_file):
        """Test that empty lines are ignored."""
        content = """id1

id2

id3
"""
        list_file = temp_file("cull.txt", content)
        ids = load_cull_list(list_file)

        assert len(ids) == 3

    def test_extract_first_field(self, temp_file):
        """Test extracting only first whitespace-delimited field."""
        content = """id1\textra\tdata
id2 more info
id3\t\t\t
"""
        list_file = temp_file("cull.txt", content)
        ids = load_cull_list(list_file)

        assert len(ids) == 3
        assert "id1" in ids
        assert "extra" not in ids
        assert "id2" in ids
        assert "more" not in ids

    def test_empty_file(self, temp_file):
        """Test loading empty file."""
        list_file = temp_file("cull.txt", "")
        ids = load_cull_list(list_file)

        assert len(ids) == 0


class TestCullSequences:
    """Tests for cull_sequences function."""

    def test_cull_matching_ids(self, temp_dir):
        """Test culling sequences that match IDs."""
        target_file = temp_dir / "target.txt"
        target_file.write_text("""id1\tsequence1
id2\tsequence2
id3\tsequence3
""")
        output_file = temp_dir / "output.txt"

        cull_ids = {"id2"}
        total, kept = cull_sequences(target_file, cull_ids, output_file)

        assert total == 3
        assert kept == 2

        content = output_file.read_text()
        assert "id1" in content
        assert "id2" not in content
        assert "id3" in content

    def test_cull_multiple_ids(self, temp_dir):
        """Test culling multiple IDs."""
        target_file = temp_dir / "target.txt"
        target_file.write_text("""id1\tdata1
id2\tdata2
id3\tdata3
id4\tdata4
""")
        output_file = temp_dir / "output.txt"

        cull_ids = {"id1", "id3"}
        total, kept = cull_sequences(target_file, cull_ids, output_file)

        assert total == 4
        assert kept == 2

        content = output_file.read_text()
        assert "id2" in content
        assert "id4" in content
        assert "id1" not in content
        assert "id3" not in content

    def test_cull_no_matches(self, temp_dir):
        """Test when no IDs match."""
        target_file = temp_dir / "target.txt"
        target_file.write_text("""id1\tdata1
id2\tdata2
""")
        output_file = temp_dir / "output.txt"

        cull_ids = {"nonexistent"}
        total, kept = cull_sequences(target_file, cull_ids, output_file)

        assert total == 2
        assert kept == 2

    def test_cull_all_ids(self, temp_dir):
        """Test when all IDs are culled."""
        target_file = temp_dir / "target.txt"
        target_file.write_text("""id1\tdata1
id2\tdata2
""")
        output_file = temp_dir / "output.txt"

        cull_ids = {"id1", "id2"}
        total, kept = cull_sequences(target_file, cull_ids, output_file)

        assert total == 2
        assert kept == 0

        content = output_file.read_text()
        assert content == ""

    def test_preserve_empty_lines(self, temp_dir):
        """Test that empty lines are preserved."""
        target_file = temp_dir / "target.txt"
        target_file.write_text("""id1\tdata1

id2\tdata2
""")
        output_file = temp_dir / "output.txt"

        cull_ids = set()
        total, kept = cull_sequences(target_file, cull_ids, output_file)

        assert total == 3  # includes empty line
        assert kept == 3

    def test_output_to_stdout(self, temp_dir, capsys):
        """Test output to stdout when no output file specified."""
        target_file = temp_dir / "target.txt"
        target_file.write_text("""id1\tdata1
id2\tdata2
""")

        cull_ids = {"id2"}
        total, kept = cull_sequences(target_file, cull_ids, None)

        assert total == 2
        assert kept == 1

        captured = capsys.readouterr()
        assert "id1" in captured.out
        assert "id2" not in captured.out


class TestMainFunction:
    """Tests for the main function."""

    def test_main_basic(self, temp_dir, capsys):
        """Test basic main function execution."""
        list_file = temp_dir / "cull.txt"
        list_file.write_text("id2\n")

        target_file = temp_dir / "target.txt"
        target_file.write_text("""id1\tdata1
id2\tdata2
id3\tdata3
""")

        output_file = temp_dir / "output.txt"

        with patch.object(sys, 'argv', [
            'prog', str(list_file), str(target_file), '-o', str(output_file)
        ]):
            main()

        content = output_file.read_text()
        assert "id1" in content
        assert "id2" not in content
        assert "id3" in content

    def test_main_verbose(self, temp_dir, capsys):
        """Test main function with verbose output."""
        list_file = temp_dir / "cull.txt"
        list_file.write_text("id2\n")

        target_file = temp_dir / "target.txt"
        target_file.write_text("""id1\tdata1
id2\tdata2
""")

        output_file = temp_dir / "output.txt"

        with patch.object(sys, 'argv', [
            'prog', str(list_file), str(target_file), '-o', str(output_file), '-v'
        ]):
            main()

        captured = capsys.readouterr()
        assert "Loaded 1 IDs" in captured.err
        assert "Total lines: 2" in captured.err
        assert "Kept: 1" in captured.err
        assert "Removed: 1" in captured.err

    def test_main_missing_list_file(self, temp_dir, capsys):
        """Test main with missing list file."""
        target_file = temp_dir / "target.txt"
        target_file.write_text("data")

        with patch.object(sys, 'argv', [
            'prog', str(temp_dir / 'nonexistent.txt'), str(target_file)
        ]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "not found" in captured.err

    def test_main_missing_target_file(self, temp_dir, capsys):
        """Test main with missing target file."""
        list_file = temp_dir / "cull.txt"
        list_file.write_text("id1\n")

        with patch.object(sys, 'argv', [
            'prog', str(list_file), str(temp_dir / 'nonexistent.txt')
        ]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "not found" in captured.err

    def test_main_stdout_output(self, temp_dir, capsys):
        """Test main function outputs to stdout by default."""
        list_file = temp_dir / "cull.txt"
        list_file.write_text("id2\n")

        target_file = temp_dir / "target.txt"
        target_file.write_text("""id1\tdata1
id2\tdata2
""")

        with patch.object(sys, 'argv', ['prog', str(list_file), str(target_file)]):
            main()

        captured = capsys.readouterr()
        assert "id1" in captured.out
        assert "id2" not in captured.out


class TestEdgeCases:
    """Tests for edge cases."""

    def test_tabs_and_spaces_in_target(self, temp_dir):
        """Test handling of various whitespace in target file."""
        target_file = temp_dir / "target.txt"
        target_file.write_text("""id1\tdata
id2  data
id3    data
""")
        output_file = temp_dir / "output.txt"

        cull_ids = {"id2"}
        total, kept = cull_sequences(target_file, cull_ids, output_file)

        assert total == 3
        assert kept == 2

    def test_special_characters_in_ids(self, temp_file, temp_dir):
        """Test IDs with special characters."""
        list_content = "orf19.123\norf19.456\n"
        list_file = temp_file("cull.txt", list_content)

        target_file = temp_dir / "target.txt"
        target_file.write_text("""orf19.123\tdata1
orf19.456\tdata2
orf19.789\tdata3
""")
        output_file = temp_dir / "output.txt"

        cull_ids = load_cull_list(list_file)
        total, kept = cull_sequences(target_file, cull_ids, output_file)

        assert total == 3
        assert kept == 1

        content = output_file.read_text()
        assert "orf19.789" in content
        assert "orf19.123" not in content

    def test_duplicate_ids_in_cull_list(self, temp_file):
        """Test handling of duplicate IDs in cull list."""
        content = """id1
id1
id2
id2
id2
"""
        list_file = temp_file("cull.txt", content)
        ids = load_cull_list(list_file)

        # Set should deduplicate
        assert len(ids) == 2
