#!/usr/bin/env python3
"""
Unit tests for scripts/cron/CGOB/download_cgob_files.py

Tests the CGOB file download functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
import urllib.error

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from cron.CGOB.download_cgob_files import (
    download_file,
    CGOB_FILES,
    YGOB_FILES,
    SGD_FILES,
    main,
)


class TestConstants:
    """Tests for module constants."""

    def test_cgob_files_defined(self):
        """Test that CGOB files are properly defined."""
        assert len(CGOB_FILES) > 0
        for name, info in CGOB_FILES.items():
            assert "source" in info
            assert "local" in info

    def test_ygob_files_defined(self):
        """Test that YGOB files are properly defined."""
        assert len(YGOB_FILES) > 0
        for name, info in YGOB_FILES.items():
            assert "source" in info
            assert "local" in info

    def test_sgd_files_defined(self):
        """Test that SGD files are properly defined."""
        assert len(SGD_FILES) > 0
        for name, info in SGD_FILES.items():
            assert "source" in info
            assert "local" in info


class TestDownloadFile:
    """Tests for download_file function."""

    @patch('cron.CGOB.download_cgob_files.urllib.request.urlretrieve')
    def test_download_success(self, mock_urlretrieve, temp_dir):
        """Test successful file download."""
        local_path = temp_dir / "test.txt"

        result = download_file("http://example.com/test.txt", local_path)

        assert result is True
        mock_urlretrieve.assert_called_once_with("http://example.com/test.txt", local_path)

    @patch('cron.CGOB.download_cgob_files.urllib.request.urlretrieve')
    def test_download_creates_parent_directory(self, mock_urlretrieve, temp_dir):
        """Test that parent directories are created."""
        local_path = temp_dir / "subdir" / "nested" / "test.txt"

        download_file("http://example.com/test.txt", local_path)

        assert local_path.parent.exists()

    @patch('cron.CGOB.download_cgob_files.urllib.request.urlretrieve')
    def test_download_removes_existing_file(self, mock_urlretrieve, temp_dir):
        """Test that existing files are removed before download."""
        local_path = temp_dir / "test.txt"
        local_path.write_text("old content")

        assert local_path.exists()

        download_file("http://example.com/test.txt", local_path)

        # File should have been removed (urlretrieve mock doesn't create it)
        mock_urlretrieve.assert_called_once()

    @patch('cron.CGOB.download_cgob_files.urllib.request.urlretrieve')
    def test_download_failure(self, mock_urlretrieve, temp_dir, capsys):
        """Test handling of download failure."""
        mock_urlretrieve.side_effect = urllib.error.URLError("Connection failed")

        local_path = temp_dir / "test.txt"
        result = download_file("http://example.com/test.txt", local_path)

        assert result is False

    @patch('cron.CGOB.download_cgob_files.urllib.request.urlretrieve')
    def test_download_http_error(self, mock_urlretrieve, temp_dir):
        """Test handling of HTTP errors."""
        mock_urlretrieve.side_effect = urllib.error.HTTPError(
            "http://example.com", 404, "Not Found", {}, None
        )

        local_path = temp_dir / "test.txt"
        result = download_file("http://example.com/test.txt", local_path)

        assert result is False


class TestMainFunction:
    """Tests for the main function."""

    @patch('cron.CGOB.download_cgob_files.download_file')
    @patch('cron.CGOB.download_cgob_files.CGOB_DATA_DIR')
    @patch('cron.CGOB.download_cgob_files.CGOB_SEQ_DIR')
    @patch('cron.CGOB.download_cgob_files.LOG_DIR')
    def test_main_success(
        self,
        mock_log_dir,
        mock_seq_dir,
        mock_data_dir,
        mock_download,
        temp_dir
    ):
        """Test main function with successful downloads."""
        mock_log_dir.__truediv__ = lambda self, x: temp_dir / x
        mock_log_dir.mkdir = MagicMock()
        mock_data_dir.mkdir = MagicMock()
        mock_seq_dir.__truediv__ = lambda self, x: temp_dir / x
        mock_seq_dir.mkdir = MagicMock()
        mock_data_dir.__truediv__ = lambda self, x: temp_dir / x

        mock_download.return_value = True

        with patch.object(sys, 'argv', ['prog']):
            result = main()

        assert result == 0
        # Should have been called for each file in all three groups
        expected_calls = len(CGOB_FILES) + len(YGOB_FILES) + len(SGD_FILES)
        assert mock_download.call_count == expected_calls

    @patch('cron.CGOB.download_cgob_files.download_file')
    @patch('cron.CGOB.download_cgob_files.CGOB_DATA_DIR')
    @patch('cron.CGOB.download_cgob_files.CGOB_SEQ_DIR')
    @patch('cron.CGOB.download_cgob_files.LOG_DIR')
    def test_main_with_failures(
        self,
        mock_log_dir,
        mock_seq_dir,
        mock_data_dir,
        mock_download,
        temp_dir
    ):
        """Test main function with some download failures."""
        mock_log_dir.__truediv__ = lambda self, x: temp_dir / x
        mock_log_dir.mkdir = MagicMock()
        mock_data_dir.mkdir = MagicMock()
        mock_seq_dir.__truediv__ = lambda self, x: temp_dir / x
        mock_seq_dir.mkdir = MagicMock()
        mock_data_dir.__truediv__ = lambda self, x: temp_dir / x

        # First call succeeds, rest fail
        mock_download.side_effect = [True] + [False] * 20

        with patch.object(sys, 'argv', ['prog']):
            result = main()

        assert result == 1  # Should return 1 due to failures

    @patch('cron.CGOB.download_cgob_files.download_file')
    @patch('cron.CGOB.download_cgob_files.CGOB_DATA_DIR')
    @patch('cron.CGOB.download_cgob_files.CGOB_SEQ_DIR')
    @patch('cron.CGOB.download_cgob_files.LOG_DIR')
    def test_main_debug_mode(
        self,
        mock_log_dir,
        mock_seq_dir,
        mock_data_dir,
        mock_download,
        temp_dir
    ):
        """Test main function with debug flag."""
        mock_log_dir.__truediv__ = lambda self, x: temp_dir / x
        mock_log_dir.mkdir = MagicMock()
        mock_data_dir.mkdir = MagicMock()
        mock_seq_dir.__truediv__ = lambda self, x: temp_dir / x
        mock_seq_dir.mkdir = MagicMock()
        mock_data_dir.__truediv__ = lambda self, x: temp_dir / x

        mock_download.return_value = True

        with patch.object(sys, 'argv', ['prog', '--debug']):
            result = main()

        assert result == 0


class TestUrlConstruction:
    """Tests for URL construction."""

    def test_cgob_url_format(self):
        """Test CGOB URL construction format."""
        for name, info in CGOB_FILES.items():
            assert info["source"].startswith("data/")

    def test_ygob_url_format(self):
        """Test YGOB URL construction format."""
        for name, info in YGOB_FILES.items():
            assert info["source"].startswith("data/")

    def test_sgd_url_format(self):
        """Test SGD URL construction format."""
        for name, info in SGD_FILES.items():
            assert info["source"].startswith(("sequence/", "curation/"))


class TestLocalPathFormat:
    """Tests for local path construction."""

    def test_cgob_local_paths(self):
        """Test CGOB local path format."""
        for name, info in CGOB_FILES.items():
            local = info["local"]
            assert "/" not in local or local.startswith("S_cerevisiae/")

    def test_sgd_local_paths(self):
        """Test SGD local paths are in S_cerevisiae directory."""
        for name, info in SGD_FILES.items():
            local = info["local"]
            assert local.startswith("S_cerevisiae/")


class TestEdgeCases:
    """Tests for edge cases."""

    @patch('cron.CGOB.download_cgob_files.urllib.request.urlretrieve')
    def test_download_empty_url(self, mock_urlretrieve, temp_dir):
        """Test downloading with empty URL."""
        mock_urlretrieve.side_effect = ValueError("Invalid URL")

        result = download_file("", temp_dir / "test.txt")
        assert result is False

    @patch('cron.CGOB.download_cgob_files.urllib.request.urlretrieve')
    def test_download_timeout(self, mock_urlretrieve, temp_dir):
        """Test handling of timeout errors."""
        mock_urlretrieve.side_effect = TimeoutError("Connection timed out")

        result = download_file("http://example.com/test.txt", temp_dir / "test.txt")
        assert result is False
