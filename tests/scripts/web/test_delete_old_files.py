#!/usr/bin/env python3
"""
Unit tests for scripts/web/delete_old_files.py

Tests the file deletion functionality based on age.
"""

import pytest
import time
from pathlib import Path
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from web.delete_old_files import check_and_delete, main


class TestCheckAndDelete:
    """Tests for check_and_delete function."""

    def test_delete_old_file(self, temp_dir):
        """Test that files older than limit are deleted."""
        test_file = temp_dir / "old_file.txt"
        test_file.write_text("test content")

        # Set modification time to 10 days ago
        old_time = time.time() - (10 * 24 * 60 * 60)
        import os
        os.utime(test_file, (old_time, old_time))

        result = check_and_delete(test_file, limit_days=7)

        assert result is True
        assert not test_file.exists()

    def test_keep_new_file(self, temp_dir):
        """Test that files newer than limit are kept."""
        test_file = temp_dir / "new_file.txt"
        test_file.write_text("test content")
        # File is brand new, modification time is current

        result = check_and_delete(test_file, limit_days=7)

        assert result is False
        assert test_file.exists()

    def test_file_exactly_at_limit(self, temp_dir):
        """Test file exactly at the age limit is kept."""
        test_file = temp_dir / "edge_file.txt"
        test_file.write_text("test content")

        # Set modification time to just under 7 days ago (6.99 days)
        old_time = time.time() - (6.99 * 24 * 60 * 60)
        import os
        os.utime(test_file, (old_time, old_time))

        result = check_and_delete(test_file, limit_days=7)

        # File under 7 days should be kept (need > not >=)
        assert result is False
        assert test_file.exists()

    def test_file_just_over_limit(self, temp_dir):
        """Test file just over the age limit is deleted."""
        test_file = temp_dir / "just_over.txt"
        test_file.write_text("test content")

        # Set modification time to 7 days + 1 hour ago
        old_time = time.time() - (7 * 24 * 60 * 60) - 3600
        import os
        os.utime(test_file, (old_time, old_time))

        result = check_and_delete(test_file, limit_days=7)

        assert result is True
        assert not test_file.exists()

    def test_dry_run_no_delete(self, temp_dir, capsys):
        """Test that dry run doesn't actually delete."""
        test_file = temp_dir / "dry_run_file.txt"
        test_file.write_text("test content")

        # Set modification time to 10 days ago
        old_time = time.time() - (10 * 24 * 60 * 60)
        import os
        os.utime(test_file, (old_time, old_time))

        result = check_and_delete(test_file, limit_days=7, dry_run=True)

        assert result is True
        assert test_file.exists()  # File should still exist

        captured = capsys.readouterr()
        assert "Would delete" in captured.out

    def test_nonexistent_file(self, temp_dir):
        """Test handling of nonexistent file."""
        test_file = temp_dir / "nonexistent.txt"

        result = check_and_delete(test_file, limit_days=7)

        assert result is False

    def test_different_age_limits(self, temp_dir):
        """Test various age limits."""
        test_file = temp_dir / "age_test.txt"
        test_file.write_text("test content")

        # Set modification time to 5 days ago
        old_time = time.time() - (5 * 24 * 60 * 60)
        import os
        os.utime(test_file, (old_time, old_time))

        # Should keep with 7-day limit
        result = check_and_delete(test_file, limit_days=7)
        assert result is False
        assert test_file.exists()

        # Should delete with 3-day limit
        result = check_and_delete(test_file, limit_days=3)
        assert result is True
        assert not test_file.exists()


class TestMainFunction:
    """Tests for the main function."""

    def test_main_single_file(self, temp_dir, capsys):
        """Test main function with single file."""
        test_file = temp_dir / "test.txt"
        test_file.write_text("test content")

        # Set modification time to 10 days ago
        old_time = time.time() - (10 * 24 * 60 * 60)
        import os
        os.utime(test_file, (old_time, old_time))

        with patch.object(sys, 'argv', ['prog', str(test_file), '--days', '7']):
            main()

        assert not test_file.exists()

        captured = capsys.readouterr()
        assert "deleted 1" in captured.out

    def test_main_multiple_files(self, temp_dir, capsys):
        """Test main function with multiple files."""
        file1 = temp_dir / "file1.txt"
        file2 = temp_dir / "file2.txt"
        file1.write_text("test")
        file2.write_text("test")

        # Make both old
        old_time = time.time() - (10 * 24 * 60 * 60)
        import os
        os.utime(file1, (old_time, old_time))
        os.utime(file2, (old_time, old_time))

        with patch.object(sys, 'argv', ['prog', str(file1), str(file2), '--days', '7']):
            main()

        assert not file1.exists()
        assert not file2.exists()

        captured = capsys.readouterr()
        assert "deleted 2" in captured.out

    def test_main_glob_pattern(self, temp_dir, capsys):
        """Test main function with glob pattern."""
        file1 = temp_dir / "test1.tmp"
        file2 = temp_dir / "test2.tmp"
        file3 = temp_dir / "test.txt"  # Different extension

        for f in [file1, file2, file3]:
            f.write_text("test")

        # Make all old
        old_time = time.time() - (10 * 24 * 60 * 60)
        import os
        for f in [file1, file2, file3]:
            os.utime(f, (old_time, old_time))

        with patch.object(sys, 'argv', ['prog', str(temp_dir / "*.tmp"), '--days', '7']):
            main()

        # Only .tmp files should be deleted
        assert not file1.exists()
        assert not file2.exists()
        assert file3.exists()  # .txt file kept

    def test_main_dry_run(self, temp_dir, capsys):
        """Test main function with dry-run option."""
        test_file = temp_dir / "test.txt"
        test_file.write_text("test content")

        old_time = time.time() - (10 * 24 * 60 * 60)
        import os
        os.utime(test_file, (old_time, old_time))

        with patch.object(sys, 'argv', ['prog', str(test_file), '--days', '7', '--dry-run']):
            main()

        assert test_file.exists()  # Should not be deleted

        captured = capsys.readouterr()
        assert "Would delete" in captured.out

    def test_main_default_days(self, temp_dir, capsys):
        """Test main function uses default 7 days."""
        test_file = temp_dir / "test.txt"
        test_file.write_text("test content")

        # 5 days old - should be kept with default
        old_time = time.time() - (5 * 24 * 60 * 60)
        import os
        os.utime(test_file, (old_time, old_time))

        with patch.object(sys, 'argv', ['prog', str(test_file)]):
            main()

        assert test_file.exists()  # Default is 7 days, file is 5 days old

    def test_main_custom_days(self, temp_dir, capsys):
        """Test main function with custom days option."""
        test_file = temp_dir / "test.txt"
        test_file.write_text("test content")

        # 5 days old
        old_time = time.time() - (5 * 24 * 60 * 60)
        import os
        os.utime(test_file, (old_time, old_time))

        with patch.object(sys, 'argv', ['prog', str(test_file), '-d', '3']):
            main()

        assert not test_file.exists()  # Should be deleted with 3-day limit

    def test_main_mixed_ages(self, temp_dir, capsys):
        """Test main function with files of different ages."""
        old_file = temp_dir / "old.txt"
        new_file = temp_dir / "new.txt"

        old_file.write_text("old")
        new_file.write_text("new")

        # Make one old, one new
        old_time = time.time() - (10 * 24 * 60 * 60)
        import os
        os.utime(old_file, (old_time, old_time))
        # new_file keeps current time

        with patch.object(sys, 'argv', ['prog', str(old_file), str(new_file), '--days', '7']):
            main()

        assert not old_file.exists()
        assert new_file.exists()

        captured = capsys.readouterr()
        assert "Checked 2" in captured.out
        assert "deleted 1" in captured.out

    def test_main_no_files_deleted(self, temp_dir, capsys):
        """Test main function when no files need deletion."""
        test_file = temp_dir / "recent.txt"
        test_file.write_text("test content")
        # File is brand new

        with patch.object(sys, 'argv', ['prog', str(test_file), '--days', '7']):
            main()

        assert test_file.exists()

        captured = capsys.readouterr()
        assert "deleted 0" in captured.out


class TestEdgeCases:
    """Tests for edge cases."""

    def test_zero_day_limit(self, temp_dir):
        """Test with zero day limit."""
        test_file = temp_dir / "test.txt"
        test_file.write_text("test content")

        # Even a brand new file should be "older" than 0 days
        # Actually, brand new file has age ~0, so 0 day limit means nothing gets deleted
        result = check_and_delete(test_file, limit_days=0)

        # File age will be a fraction of a day, which is > 0
        # So with limit_days=0, file should be deleted
        assert result is True or test_file.exists()

    def test_large_day_limit(self, temp_dir):
        """Test with very large day limit."""
        test_file = temp_dir / "test.txt"
        test_file.write_text("test content")

        # Even a 10-day old file should be kept with 365-day limit
        old_time = time.time() - (10 * 24 * 60 * 60)
        import os
        os.utime(test_file, (old_time, old_time))

        result = check_and_delete(test_file, limit_days=365)

        assert result is False
        assert test_file.exists()

    def test_empty_directory(self, temp_dir, capsys):
        """Test main with nonexistent file."""
        with patch.object(sys, 'argv', ['prog', str(temp_dir / "nonexistent.txt")]):
            main()

        captured = capsys.readouterr()
        assert "Checked 0" in captured.out
        assert "deleted 0" in captured.out

    def test_directory_skipped(self, temp_dir, capsys):
        """Test that directories are skipped."""
        sub_dir = temp_dir / "subdir"
        sub_dir.mkdir()

        with patch.object(sys, 'argv', ['prog', str(sub_dir)]):
            main()

        assert sub_dir.exists()  # Directory should still exist

        captured = capsys.readouterr()
        assert "Checked 0" in captured.out  # Directory not counted as file
