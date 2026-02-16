#!/usr/bin/env python3
"""
Unit tests for scripts/ftp_dump/create_intergenic_file.py

Tests the intergenic sequence file creation functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from ftp_dump.create_intergenic_file import (
    get_chromosome_lengths,
    NUM_TO_LETTER,
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

    def test_multiple_chromosomes(self, mock_db_session):
        """Test with multiple chromosomes."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            (i, i * 100000) for i in range(1, 18)
        ]

        result = get_chromosome_lengths(mock_db_session)

        assert len(result) == 17


class TestNumToLetterMapping:
    """Tests for NUM_TO_LETTER constant."""

    def test_mapping_defined(self):
        """Test that mapping is defined."""
        assert len(NUM_TO_LETTER) > 0

    def test_chromosome_1_to_a(self):
        """Test chromosome 1 maps to A."""
        assert NUM_TO_LETTER[1] == "A"

    def test_chromosome_range(self):
        """Test all standard chromosomes have mappings."""
        for i in range(1, 18):
            assert i in NUM_TO_LETTER

    def test_mitochondria_mapping(self):
        """Test mitochondrial chromosome mapping."""
        # Chromosome 17 is typically mitochondrial
        assert 17 in NUM_TO_LETTER


class TestMainFunction:
    """Tests for the main function."""

    @patch('ftp_dump.create_intergenic_file.SessionLocal')
    @patch('ftp_dump.create_intergenic_file.DATA_DUMP_DIR')
    @patch('ftp_dump.create_intergenic_file.FTP_INTERGENIC_DIR')
    @patch('ftp_dump.create_intergenic_file.LOG_DIR')
    def test_main_basic(
        self, mock_log_dir, mock_ftp_dir, mock_data_dir, mock_session_local, temp_dir
    ):
        """Test basic main function execution."""
        from ftp_dump.create_intergenic_file import main

        mock_log_dir.__truediv__ = lambda self, x: temp_dir / x
        mock_ftp_dir.__truediv__ = lambda self, x: temp_dir / x
        mock_ftp_dir.mkdir = MagicMock()
        mock_data_dir.__truediv__ = lambda self, x: temp_dir / x

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

        # Mock database queries - return empty results
        mock_session.execute.return_value.fetchall.return_value = []

        with patch.object(sys, 'argv', ['prog']):
            with patch('ftp_dump.create_intergenic_file.DATA_DUMP_DIR', temp_dir):
                with patch('ftp_dump.create_intergenic_file.FTP_INTERGENIC_DIR', temp_dir):
                    with patch('ftp_dump.create_intergenic_file.LOG_DIR', temp_dir):
                        result = main()

        assert result == 0

    @patch('ftp_dump.create_intergenic_file.SessionLocal')
    def test_main_handles_exception(self, mock_session_local, temp_dir):
        """Test main handles exceptions."""
        from ftp_dump.create_intergenic_file import main

        mock_session_local.side_effect = Exception("Database error")

        with patch.object(sys, 'argv', ['prog']):
            with patch('ftp_dump.create_intergenic_file.LOG_DIR', temp_dir):
                result = main()

        assert result == 1


class TestEdgeCases:
    """Tests for edge cases."""

    def test_num_to_letter_coverage(self):
        """Test NUM_TO_LETTER covers expected range."""
        # Should have mappings for at least chromosomes 1-16 plus mito
        expected_min = 16
        assert len(NUM_TO_LETTER) >= expected_min

    def test_letter_uniqueness(self):
        """Test all letters in mapping are unique."""
        letters = list(NUM_TO_LETTER.values())
        assert len(letters) == len(set(letters))
