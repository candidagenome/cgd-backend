#!/usr/bin/env python3
"""
Unit tests for scripts/maintenance/convert_logs_weekly_to_monthly.py

Tests the Apache log conversion from weekly to monthly format.
"""

import gzip
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from maintenance.convert_logs_weekly_to_monthly import (
    MONTH_TO_NUM,
    LOG_DATE_PATTERN,
    find_rotated_logs,
    process_log_file,
    convert_logs,
)


class TestMonthMapping:
    """Tests for month name to number mapping."""

    def test_all_months_present(self):
        """Test that all 12 months are mapped."""
        assert len(MONTH_TO_NUM) == 12

    def test_month_values(self):
        """Test specific month mappings."""
        assert MONTH_TO_NUM["Jan"] == "01"
        assert MONTH_TO_NUM["Jun"] == "06"
        assert MONTH_TO_NUM["Dec"] == "12"


class TestLogDatePattern:
    """Tests for the Apache log date pattern regex."""

    def test_standard_log_line(self):
        """Test parsing a standard Apache log line."""
        line = '192.168.1.1 - - [15/Aug/2024:10:30:45 -0700] "GET / HTTP/1.1" 200'
        match = LOG_DATE_PATTERN.match(line)

        assert match is not None
        assert match.group(1) == "Aug"
        assert match.group(2) == "2024"

    def test_different_months(self):
        """Test parsing logs from different months."""
        for month, num in MONTH_TO_NUM.items():
            line = f'10.0.0.1 - - [01/{month}/2024:00:00:00 +0000] "GET /"'
            match = LOG_DATE_PATTERN.match(line)
            assert match is not None
            assert match.group(1) == month

    def test_various_ip_formats(self):
        """Test parsing logs with various IP formats."""
        ips = ['192.168.1.1', '10.0.0.1', '255.255.255.255', '1.2.3.4']
        for ip in ips:
            line = f'{ip} - - [01/Jan/2024:00:00:00 +0000] "GET /"'
            match = LOG_DATE_PATTERN.match(line)
            assert match is not None

    def test_non_matching_lines(self):
        """Test that non-Apache log lines don't match."""
        non_matching = [
            "Just some text",
            "[01/Jan/2024] Log message",
            "192.168.1.1 [01/Jan/2024:00:00:00]",
        ]
        for line in non_matching:
            match = LOG_DATE_PATTERN.match(line)
            assert match is None


class TestFindRotatedLogs:
    """Tests for find_rotated_logs function."""

    def test_find_gzipped_logs(self, temp_dir):
        """Test finding gzipped rotated logs."""
        # Create mock log files
        for i in [1, 2, 3]:
            log_file = temp_dir / f"access_log.{i}.gz"
            with gzip.open(log_file, 'wt') as f:
                f.write("test\n")

        logs = find_rotated_logs(temp_dir)

        assert len(logs) == 3
        # Should be sorted by rotation number descending
        assert logs[0][0] == 3
        assert logs[1][0] == 2
        assert logs[2][0] == 1

    def test_ignore_non_rotated_logs(self, temp_dir):
        """Test that non-rotated logs are ignored."""
        # Create various log files
        (temp_dir / "access_log").write_text("test")
        (temp_dir / "access_log.1.gz").write_bytes(gzip.compress(b"test"))
        (temp_dir / "error_log.1.gz").write_bytes(gzip.compress(b"test"))
        (temp_dir / "access_log.txt").write_text("test")

        logs = find_rotated_logs(temp_dir)

        # Should only find the rotated access_log
        assert len(logs) == 1
        assert logs[0][0] == 1

    def test_empty_directory(self, temp_dir):
        """Test handling of empty directory."""
        logs = find_rotated_logs(temp_dir)
        assert logs == []


class TestProcessLogFile:
    """Tests for process_log_file function."""

    def test_process_single_month(self, temp_dir):
        """Test processing logs from a single month."""
        log_file = temp_dir / "access_log.1.gz"
        log_content = "\n".join([
            '192.168.1.1 - - [01/Aug/2024:10:00:00 -0700] "GET / HTTP/1.1"',
            '192.168.1.2 - - [15/Aug/2024:11:00:00 -0700] "GET /page HTTP/1.1"',
            '192.168.1.3 - - [31/Aug/2024:12:00:00 -0700] "GET /other HTTP/1.1"',
        ])

        with gzip.open(log_file, 'wt', encoding='utf-8') as f:
            f.write(log_content)

        month_data = process_log_file(log_file)

        assert "202408" in month_data
        assert len(month_data["202408"]) == 3

    def test_process_multiple_months(self, temp_dir):
        """Test processing logs spanning multiple months."""
        log_file = temp_dir / "access_log.1.gz"
        log_content = "\n".join([
            '192.168.1.1 - - [31/Jul/2024:23:59:59 -0700] "GET / HTTP/1.1"',
            '192.168.1.2 - - [01/Aug/2024:00:00:00 -0700] "GET / HTTP/1.1"',
            '192.168.1.3 - - [01/Sep/2024:00:00:00 -0700] "GET / HTTP/1.1"',
        ])

        with gzip.open(log_file, 'wt', encoding='utf-8') as f:
            f.write(log_content)

        month_data = process_log_file(log_file)

        assert "202407" in month_data
        assert "202408" in month_data
        assert "202409" in month_data
        assert len(month_data) == 3

    def test_process_year_boundary(self, temp_dir):
        """Test processing logs crossing year boundary."""
        log_file = temp_dir / "access_log.1.gz"
        log_content = "\n".join([
            '192.168.1.1 - - [31/Dec/2023:23:59:59 -0700] "GET / HTTP/1.1"',
            '192.168.1.2 - - [01/Jan/2024:00:00:00 -0700] "GET / HTTP/1.1"',
        ])

        with gzip.open(log_file, 'wt', encoding='utf-8') as f:
            f.write(log_content)

        month_data = process_log_file(log_file)

        assert "202312" in month_data
        assert "202401" in month_data

    def test_skip_non_matching_lines(self, temp_dir):
        """Test that non-matching lines are skipped."""
        log_file = temp_dir / "access_log.1.gz"
        log_content = "\n".join([
            '192.168.1.1 - - [01/Aug/2024:10:00:00 -0700] "GET / HTTP/1.1"',
            'This is not a valid log line',
            '# Comment line',
            '',
        ])

        with gzip.open(log_file, 'wt', encoding='utf-8') as f:
            f.write(log_content)

        month_data = process_log_file(log_file)

        assert "202408" in month_data
        assert len(month_data["202408"]) == 1

    def test_handle_encoding_errors(self, temp_dir):
        """Test handling of encoding errors in log files."""
        log_file = temp_dir / "access_log.1.gz"

        # Create log with some valid content
        valid_content = '192.168.1.1 - - [01/Aug/2024:10:00:00 -0700] "GET / HTTP/1.1"\n'

        with gzip.open(log_file, 'wt', encoding='utf-8') as f:
            f.write(valid_content)

        # Should handle gracefully
        month_data = process_log_file(log_file)
        assert "202408" in month_data


class TestConvertLogs:
    """Tests for convert_logs function."""

    def test_convert_logs_basic(self, temp_dir, monkeypatch):
        """Test basic log conversion."""
        # Create rotated log files
        for i in [1, 2]:
            log_file = temp_dir / f"access_log.{i}.gz"
            log_content = f'192.168.1.{i} - - [01/Aug/2024:10:00:00 -0700] "GET / HTTP/1.1"\n'
            with gzip.open(log_file, 'wt', encoding='utf-8') as f:
                f.write(log_content)

        # Mock the module-level constants
        monkeypatch.setattr(
            'maintenance.convert_logs_weekly_to_monthly.WEB_LOG_DIR',
            temp_dir
        )

        result = convert_logs()

        assert result is True

        # Check output file was created
        monthly_file = temp_dir / "access_log.202408.gz"
        assert monthly_file.exists()

        # Verify content
        with gzip.open(monthly_file, 'rt', encoding='utf-8') as f:
            content = f.read()
            assert "192.168.1" in content

    def test_convert_logs_no_rotated_logs(self, temp_dir, monkeypatch):
        """Test conversion when no rotated logs exist."""
        monkeypatch.setattr(
            'maintenance.convert_logs_weekly_to_monthly.WEB_LOG_DIR',
            temp_dir
        )

        result = convert_logs()

        assert result is True

    def test_convert_logs_nonexistent_directory(self, temp_dir, monkeypatch):
        """Test handling of nonexistent directory."""
        nonexistent = temp_dir / "nonexistent"
        monkeypatch.setattr(
            'maintenance.convert_logs_weekly_to_monthly.WEB_LOG_DIR',
            nonexistent
        )

        with pytest.raises(SystemExit):
            convert_logs()

    def test_convert_logs_multiple_months(self, temp_dir, monkeypatch):
        """Test conversion with logs spanning multiple months."""
        log_file = temp_dir / "access_log.1.gz"
        log_content = "\n".join([
            '192.168.1.1 - - [15/Jul/2024:10:00:00 -0700] "GET / HTTP/1.1"',
            '192.168.1.2 - - [15/Aug/2024:10:00:00 -0700] "GET / HTTP/1.1"',
            '192.168.1.3 - - [15/Sep/2024:10:00:00 -0700] "GET / HTTP/1.1"',
        ])

        with gzip.open(log_file, 'wt', encoding='utf-8') as f:
            f.write(log_content)

        monkeypatch.setattr(
            'maintenance.convert_logs_weekly_to_monthly.WEB_LOG_DIR',
            temp_dir
        )

        result = convert_logs()

        assert result is True

        # Check all monthly files were created
        assert (temp_dir / "access_log.202407.gz").exists()
        assert (temp_dir / "access_log.202408.gz").exists()
        assert (temp_dir / "access_log.202409.gz").exists()


class TestMainFunction:
    """Tests for the main function."""

    def test_main_success(self, temp_dir, monkeypatch):
        """Test main function returns 0 on success."""
        monkeypatch.setattr(
            'maintenance.convert_logs_weekly_to_monthly.WEB_LOG_DIR',
            temp_dir
        )

        from maintenance.convert_logs_weekly_to_monthly import main
        result = main()

        assert result == 0

    def test_main_failure(self, temp_dir, monkeypatch):
        """Test main function returns 1 on failure."""
        nonexistent = temp_dir / "nonexistent"
        monkeypatch.setattr(
            'maintenance.convert_logs_weekly_to_monthly.WEB_LOG_DIR',
            nonexistent
        )

        from maintenance.convert_logs_weekly_to_monthly import main

        # Should exit with code 1 due to nonexistent directory
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1


class TestEdgeCases:
    """Tests for edge cases."""

    def test_empty_log_file(self, temp_dir):
        """Test handling of empty log file."""
        log_file = temp_dir / "access_log.1.gz"
        with gzip.open(log_file, 'wt', encoding='utf-8') as f:
            f.write("")

        month_data = process_log_file(log_file)
        assert month_data == {}

    def test_large_rotation_numbers(self, temp_dir):
        """Test handling of large rotation numbers."""
        for i in [1, 10, 100]:
            log_file = temp_dir / f"access_log.{i}.gz"
            with gzip.open(log_file, 'wt') as f:
                f.write("test\n")

        logs = find_rotated_logs(temp_dir)

        assert len(logs) == 3
        # Should be sorted descending
        assert logs[0][0] == 100
        assert logs[1][0] == 10
        assert logs[2][0] == 1

    def test_preserve_original_lines(self, temp_dir):
        """Test that original log lines are preserved exactly."""
        log_file = temp_dir / "access_log.1.gz"
        original_line = '192.168.1.1 - - [01/Aug/2024:10:30:45 -0700] "GET /path?query=value HTTP/1.1" 200 1234 "http://example.com" "Mozilla/5.0"\n'

        with gzip.open(log_file, 'wt', encoding='utf-8') as f:
            f.write(original_line)

        month_data = process_log_file(log_file)

        assert "202408" in month_data
        assert month_data["202408"][0] == original_line
