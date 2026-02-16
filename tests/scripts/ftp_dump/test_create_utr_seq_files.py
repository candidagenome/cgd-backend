#!/usr/bin/env python3
"""
Unit tests for scripts/ftp_dump/create_utr_seq_files.py

Tests the UTR sequence file creation functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from ftp_dump.create_utr_seq_files import (
    get_chromosome_lengths,
    get_chromosome_number_to_roman,
    get_feature_qualifiers,
    format_feature_type,
)


class TestGetChromosomeLengths:
    """Tests for get_chromosome_lengths function."""

    def test_basic_lengths(self, mock_db_session):
        """Test getting basic chromosome lengths."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, 1000000),
            (2, 2000000),
            (3, 1500000),
        ]

        result = get_chromosome_lengths(mock_db_session)

        assert result[1] == 1000000
        assert result[2] == 2000000
        assert result[3] == 1500000

    def test_empty_result(self, mock_db_session):
        """Test with no chromosomes."""
        mock_db_session.execute.return_value.fetchall.return_value = []

        result = get_chromosome_lengths(mock_db_session)

        assert result == {}


class TestGetChromosomeNumberToRoman:
    """Tests for get_chromosome_number_to_roman function."""

    def test_basic_mapping(self, mock_db_session):
        """Test getting basic chromosome number to Roman mapping."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "I"),
            (2, "II"),
            (3, "III"),
        ]

        result = get_chromosome_number_to_roman(mock_db_session)

        assert result[1] == "I"
        assert result[2] == "II"
        assert result[3] == "III"

    def test_empty_result(self, mock_db_session):
        """Test with no chromosomes."""
        mock_db_session.execute.return_value.fetchall.return_value = []

        result = get_chromosome_number_to_roman(mock_db_session)

        assert result == {}


class TestGetFeatureQualifiers:
    """Tests for get_feature_qualifiers function."""

    def test_basic_qualifiers(self, mock_db_session):
        """Test getting basic feature qualifiers."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("ORF19.1", "Verified"),
            ("ORF19.2", "Uncharacterized"),
            ("ORF19.3", "Dubious"),
        ]

        result = get_feature_qualifiers(mock_db_session)

        assert result["ORF19.1"] == "Verified"
        assert result["ORF19.2"] == "Uncharacterized"
        assert result["ORF19.3"] == "Dubious"

    def test_empty_result(self, mock_db_session):
        """Test with no qualifiers."""
        mock_db_session.execute.return_value.fetchall.return_value = []

        result = get_feature_qualifiers(mock_db_session)

        assert result == {}


class TestFormatFeatureType:
    """Tests for format_feature_type function."""

    def test_single_type(self):
        """Test with single type."""
        result = format_feature_type("ORF")
        assert result == "ORF"

    def test_strip_pipes(self):
        """Test stripping leading/trailing pipes."""
        result = format_feature_type("|ORF|")
        assert result == "ORF"

    def test_multiple_types(self):
        """Test with multiple types."""
        result = format_feature_type("pseudogene|ORF")
        # Non-ORF types should come first
        assert "pseudogene" in result
        assert "ORF" in result

    def test_duplicate_removal(self):
        """Test duplicate type removal."""
        result = format_feature_type("ORF|ORF|ORF")
        assert result.count("ORF") == 1

    def test_orf_prioritization(self):
        """Test that ORF types come after non-ORF types."""
        result = format_feature_type("Verified ORF|pseudogene")
        parts = result.split()
        # pseudogene should come before Verified ORF
        assert result.index("pseudogene") < result.index("Verified")

    def test_empty_string(self):
        """Test with empty string."""
        result = format_feature_type("")
        assert result == ""


class TestMainFunction:
    """Tests for the main function."""

    @patch('ftp_dump.create_utr_seq_files.SessionLocal')
    def test_main_basic(self, mock_session_local, temp_dir):
        """Test basic main function execution."""
        from ftp_dump.create_utr_seq_files import main

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

        # Mock database queries
        mock_session.execute.return_value.fetchall.return_value = []

        with patch.object(sys, 'argv', ['prog', '500']):
            with patch('ftp_dump.create_utr_seq_files.UTR_DIR', temp_dir):
                with patch('ftp_dump.create_utr_seq_files.LOG_DIR', temp_dir):
                    result = main()

        assert result == 0

    def test_main_missing_argument(self):
        """Test main with missing UTR length argument."""
        from ftp_dump.create_utr_seq_files import main

        with patch.object(sys, 'argv', ['prog']):
            with pytest.raises(SystemExit):
                main()


class TestEdgeCases:
    """Tests for edge cases."""

    def test_format_feature_type_with_spaces(self):
        """Test feature type with spaces."""
        result = format_feature_type("Verified ORF|Uncharacterized ORF")
        assert "Verified" in result
        assert "Uncharacterized" in result

    def test_format_feature_type_mixed_case(self):
        """Test feature type with mixed case ORF."""
        result = format_feature_type("verified orf|pseudogene")
        # Should still detect "orf" even in lowercase
        assert "pseudogene" in result
