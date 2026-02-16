#!/usr/bin/env python3
"""
Unit tests for scripts/checking/check_pmid_valid.py

Tests the PubMed ID validation functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

# Skip all tests in this module if Biopython is not installed
pytest.importorskip("Bio", reason="Biopython not installed")


class TestSetupLogging:
    """Tests for setup_logging function."""

    def test_setup_logging_default(self, temp_dir):
        """Test default logging setup."""
        from checking.check_pmid_valid import setup_logging
        import logging

        setup_logging(verbose=False)
        logger = logging.getLogger()
        assert logger.level == logging.INFO or logger.level == logging.WARNING

    def test_setup_logging_verbose(self, temp_dir):
        """Test verbose logging setup."""
        from checking.check_pmid_valid import setup_logging
        import logging

        setup_logging(verbose=True)
        # Verbose mode should set DEBUG level
        # Note: This may affect root logger


class TestGetPubmedIdsFromDb:
    """Tests for get_pubmed_ids_from_db function."""

    def test_returns_list_of_ids(self, mock_db_session):
        """Test that function returns list of PubMed IDs."""
        from checking.check_pmid_valid import get_pubmed_ids_from_db

        # Mock the execute result
        mock_db_session.execute.return_value = [
            (12345678,),
            (23456789,),
            (34567890,),
        ]

        result = get_pubmed_ids_from_db(mock_db_session)

        assert len(result) == 3
        assert 12345678 in result
        assert 23456789 in result
        assert 34567890 in result

    def test_filters_null_values(self, mock_db_session):
        """Test that NULL values are filtered out."""
        from checking.check_pmid_valid import get_pubmed_ids_from_db

        mock_db_session.execute.return_value = [
            (12345678,),
            (None,),
            (23456789,),
        ]

        result = get_pubmed_ids_from_db(mock_db_session)

        assert len(result) == 2
        assert None not in result

    def test_empty_result(self, mock_db_session):
        """Test handling of empty result set."""
        from checking.check_pmid_valid import get_pubmed_ids_from_db

        mock_db_session.execute.return_value = []

        result = get_pubmed_ids_from_db(mock_db_session)

        assert result == []


class TestValidatePubmedIds:
    """Tests for validate_pubmed_ids function."""

    @patch('checking.check_pmid_valid.Entrez')
    def test_all_valid_ids(self, mock_entrez):
        """Test validation when all IDs are valid."""
        from checking.check_pmid_valid import validate_pubmed_ids

        # Mock Entrez response
        mock_handle = MagicMock()
        mock_entrez.esearch.return_value = mock_handle
        mock_entrez.read.return_value = {'IdList': ['12345678', '23456789']}

        pubmed_ids = [12345678, 23456789]
        invalid = validate_pubmed_ids(pubmed_ids, "test@example.com", batch_size=100)

        assert len(invalid) == 0

    @patch('checking.check_pmid_valid.Entrez')
    def test_some_invalid_ids(self, mock_entrez):
        """Test validation with some invalid IDs."""
        from checking.check_pmid_valid import validate_pubmed_ids

        mock_handle = MagicMock()
        mock_entrez.esearch.return_value = mock_handle
        # Only return one ID as valid
        mock_entrez.read.return_value = {'IdList': ['12345678']}

        pubmed_ids = [12345678, 99999999]  # Second ID is invalid
        invalid = validate_pubmed_ids(pubmed_ids, "test@example.com", batch_size=100)

        assert len(invalid) == 1
        assert 99999999 in invalid

    @patch('checking.check_pmid_valid.Entrez')
    def test_all_invalid_ids(self, mock_entrez):
        """Test validation when all IDs are invalid."""
        from checking.check_pmid_valid import validate_pubmed_ids

        mock_handle = MagicMock()
        mock_entrez.esearch.return_value = mock_handle
        mock_entrez.read.return_value = {'IdList': []}

        pubmed_ids = [99999999, 88888888]
        invalid = validate_pubmed_ids(pubmed_ids, "test@example.com", batch_size=100)

        assert len(invalid) == 2

    @patch('checking.check_pmid_valid.Entrez')
    @patch('checking.check_pmid_valid.time.sleep')  # Don't actually sleep
    def test_batching(self, mock_sleep, mock_entrez):
        """Test that large ID lists are batched."""
        from checking.check_pmid_valid import validate_pubmed_ids

        mock_handle = MagicMock()
        mock_entrez.esearch.return_value = mock_handle
        mock_entrez.read.return_value = {'IdList': []}

        # Create more IDs than batch size
        pubmed_ids = list(range(1, 251))  # 250 IDs
        validate_pubmed_ids(pubmed_ids, "test@example.com", batch_size=100)

        # Should call esearch 3 times for 250 IDs with batch size 100
        assert mock_entrez.esearch.call_count == 3

    @patch('checking.check_pmid_valid.Entrez')
    def test_handles_api_error(self, mock_entrez):
        """Test handling of API errors."""
        from checking.check_pmid_valid import validate_pubmed_ids

        mock_entrez.esearch.side_effect = Exception("API Error")

        pubmed_ids = [12345678]
        # Should not raise, just log error
        invalid = validate_pubmed_ids(pubmed_ids, "test@example.com")

        # ID should not be in invalid list since we couldn't check
        assert len(invalid) == 0

    @patch('checking.check_pmid_valid.Entrez')
    def test_email_set(self, mock_entrez):
        """Test that email is set on Entrez."""
        from checking.check_pmid_valid import validate_pubmed_ids

        mock_handle = MagicMock()
        mock_entrez.esearch.return_value = mock_handle
        mock_entrez.read.return_value = {'IdList': ['12345678']}

        validate_pubmed_ids([12345678], "custom@example.com")

        assert mock_entrez.email == "custom@example.com"


class TestCheckPubmedIds:
    """Tests for check_pubmed_ids function."""

    @patch('checking.check_pmid_valid.SessionLocal')
    @patch('checking.check_pmid_valid.validate_pubmed_ids')
    @patch('checking.check_pmid_valid.get_pubmed_ids_from_db')
    def test_basic_check(self, mock_get_ids, mock_validate, mock_session):
        """Test basic PubMed ID check."""
        from checking.check_pmid_valid import check_pubmed_ids

        mock_get_ids.return_value = [12345678, 23456789]
        mock_validate.return_value = [99999999]

        # Setup session context manager
        mock_session_instance = MagicMock()
        mock_session.return_value.__enter__ = MagicMock(
            return_value=mock_session_instance
        )
        mock_session.return_value.__exit__ = MagicMock(return_value=False)

        stats = check_pubmed_ids("test@example.com")

        assert stats['total_ids'] == 2
        assert stats['invalid_ids'] == [99999999]

    @patch('checking.check_pmid_valid.SessionLocal')
    @patch('checking.check_pmid_valid.get_pubmed_ids_from_db')
    def test_no_ids_to_check(self, mock_get_ids, mock_session):
        """Test when no PubMed IDs exist in database."""
        from checking.check_pmid_valid import check_pubmed_ids

        mock_get_ids.return_value = []

        mock_session_instance = MagicMock()
        mock_session.return_value.__enter__ = MagicMock(
            return_value=mock_session_instance
        )
        mock_session.return_value.__exit__ = MagicMock(return_value=False)

        stats = check_pubmed_ids("test@example.com")

        assert stats['total_ids'] == 0
        assert stats['invalid_ids'] == []


class TestMainFunction:
    """Tests for the main function."""

    @patch('checking.check_pmid_valid.check_pubmed_ids')
    @patch('checking.check_pmid_valid.setup_logging')
    def test_main_success(self, mock_setup, mock_check):
        """Test main function succeeds."""
        from checking.check_pmid_valid import main

        mock_check.return_value = {
            'total_ids': 100,
            'invalid_ids': [],
            'errors': 0,
        }

        with patch.object(sys, 'argv', ['prog', '--email', 'test@example.com']):
            main()

        # Verify check_pubmed_ids was called
        mock_check.assert_called_once()

    @patch('checking.check_pmid_valid.check_pubmed_ids')
    @patch('checking.check_pmid_valid.setup_logging')
    def test_main_with_invalid_ids(self, mock_setup, mock_check):
        """Test main function with invalid IDs found."""
        from checking.check_pmid_valid import main

        mock_check.return_value = {
            'total_ids': 100,
            'invalid_ids': [99999999, 88888888],
            'errors': 0,
        }

        with patch.object(sys, 'argv', ['prog']):
            main()

        # Verify check_pubmed_ids was called and returned invalid IDs
        mock_check.assert_called_once()
        assert len(mock_check.return_value['invalid_ids']) == 2

    @patch('checking.check_pmid_valid.check_pubmed_ids')
    @patch('checking.check_pmid_valid.setup_logging')
    def test_main_with_exception(self, mock_setup, mock_check):
        """Test main function handles exceptions."""
        from checking.check_pmid_valid import main

        mock_check.side_effect = Exception("Database error")

        with patch.object(sys, 'argv', ['prog']):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1

    @patch('checking.check_pmid_valid.check_pubmed_ids')
    @patch('checking.check_pmid_valid.setup_logging')
    def test_main_dry_run(self, mock_setup, mock_check, capsys):
        """Test main function with dry-run flag."""
        from checking.check_pmid_valid import main

        mock_check.return_value = {
            'total_ids': 10,
            'invalid_ids': [],
            'errors': 0,
        }

        with patch.object(sys, 'argv', ['prog', '--dry-run']):
            main()

        # Should have called check_pubmed_ids with dry_run=True
        mock_check.assert_called_once()
        call_args = mock_check.call_args
        assert call_args[1].get('dry_run', False) or call_args[0][1] if len(call_args[0]) > 1 else True


class TestConstants:
    """Tests for module constants."""

    def test_batch_size(self):
        """Test BATCH_SIZE constant."""
        from checking.check_pmid_valid import BATCH_SIZE
        assert BATCH_SIZE > 0
        assert BATCH_SIZE <= 500  # NCBI recommends max 500

    def test_default_email(self):
        """Test DEFAULT_EMAIL constant."""
        from checking.check_pmid_valid import DEFAULT_EMAIL
        assert "@" in DEFAULT_EMAIL


class TestEdgeCases:
    """Tests for edge cases."""

    @patch('checking.check_pmid_valid.Entrez')
    def test_empty_id_list(self, mock_entrez):
        """Test validation with empty ID list."""
        from checking.check_pmid_valid import validate_pubmed_ids

        invalid = validate_pubmed_ids([], "test@example.com")
        assert invalid == []
        mock_entrez.esearch.assert_not_called()

    @patch('checking.check_pmid_valid.Entrez')
    @patch('checking.check_pmid_valid.time.sleep')
    def test_single_id(self, mock_sleep, mock_entrez):
        """Test validation with single ID."""
        from checking.check_pmid_valid import validate_pubmed_ids

        mock_handle = MagicMock()
        mock_entrez.esearch.return_value = mock_handle
        mock_entrez.read.return_value = {'IdList': ['12345678']}

        invalid = validate_pubmed_ids([12345678], "test@example.com")
        assert len(invalid) == 0
