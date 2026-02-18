#!/usr/bin/env python3
"""
Unit tests for scripts/loading/load_headlines.py

Tests the headline and name description loading functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from loading.load_headlines import (
    cleanup_text,
    get_reference_no,
    insert_ref_links,
    load_headlines,
)


class TestCleanupText:
    """Tests for cleanup_text function."""

    def test_strip_whitespace(self):
        """Test stripping leading/trailing whitespace."""
        assert cleanup_text("  hello world  ") == "hello world"

    def test_remove_leading_quote(self):
        """Test removal of leading double quote."""
        assert cleanup_text('"hello world') == "hello world"

    def test_remove_trailing_quote(self):
        """Test removal of trailing double quote."""
        assert cleanup_text('hello world"') == "hello world"

    def test_remove_both_quotes(self):
        """Test removal of both leading and trailing quotes."""
        assert cleanup_text('"hello world"') == "hello world"

    def test_fix_5_prime_apostrophes(self):
        """Test fixing double 5' apostrophes."""
        assert cleanup_text("5'' end") == "5' end"

    def test_fix_3_prime_apostrophes(self):
        """Test fixing double 3' apostrophes."""
        assert cleanup_text("3'' end") == "3' end"

    def test_fix_both_prime_apostrophes(self):
        """Test fixing both 5' and 3' apostrophes."""
        assert cleanup_text("5'' to 3'' direction") == "5' to 3' direction"

    def test_empty_string(self):
        """Test handling of empty string."""
        assert cleanup_text("") == ""

    def test_none_value(self):
        """Test handling of None value."""
        assert cleanup_text(None) == ""

    def test_normal_text_unchanged(self):
        """Test that normal text without issues is unchanged."""
        assert cleanup_text("Normal text here") == "Normal text here"

    def test_combined_cleanup(self):
        """Test multiple cleanup operations combined."""
        # Note: cleanup_text strips first, then removes quotes
        assert cleanup_text('  "5\'\' end sequence"  ') == "5' end sequence"


class TestGetReferenceNo:
    """Tests for get_reference_no function."""

    def test_project_reference_id(self, mock_db_session):
        """Test getting reference from project ID (e.g., CGD:123)."""
        result = get_reference_no(mock_db_session, "CGD:123", "CGD")
        assert result == 123

    def test_project_reference_id_custom_acronym(self, mock_db_session):
        """Test getting reference with custom project acronym."""
        result = get_reference_no(mock_db_session, "SGD:456", "SGD")
        assert result == 456

    def test_pmid_reference_found(self, mock_db_session):
        """Test getting reference from PMID."""
        mock_ref = MagicMock()
        mock_ref.reference_no = 789
        mock_db_session.query.return_value.filter.return_value.first.return_value = mock_ref

        result = get_reference_no(mock_db_session, "PMID:12345", "CGD")
        assert result == 789

    def test_pmid_reference_not_found(self, mock_db_session):
        """Test handling PMID not found in database."""
        mock_db_session.query.return_value.filter.return_value.first.return_value = None

        result = get_reference_no(mock_db_session, "PMID:99999", "CGD")
        assert result is None

    def test_invalid_reference_format(self, mock_db_session):
        """Test handling of invalid reference format."""
        result = get_reference_no(mock_db_session, "INVALID:REF", "CGD")
        assert result is None

    def test_whitespace_trimmed(self, mock_db_session):
        """Test that whitespace is trimmed from reference string."""
        result = get_reference_no(mock_db_session, "  CGD:100  ", "CGD")
        assert result == 100


class TestInsertRefLinks:
    """Tests for insert_ref_links function."""

    def test_insert_new_ref_link(self, mock_db_session):
        """Test inserting a new ref_link."""
        # Mock no existing ref_link
        mock_db_session.query.return_value.filter.return_value.first.return_value = None

        with patch('loading.load_headlines.get_reference_no', return_value=1000):
            count = insert_ref_links(
                mock_db_session,
                ["CGD:1000"],
                "HEADLINE",
                123,
                "CGD",
                "TEST"
            )

        assert count == 1
        mock_db_session.add.assert_called_once()

    def test_skip_existing_ref_link(self, mock_db_session):
        """Test that existing ref_links are skipped."""
        # Mock existing ref_link
        mock_existing = MagicMock()
        mock_db_session.query.return_value.filter.return_value.first.return_value = mock_existing

        with patch('loading.load_headlines.get_reference_no', return_value=1000):
            count = insert_ref_links(
                mock_db_session,
                ["CGD:1000"],
                "HEADLINE",
                123,
                "CGD",
                "TEST"
            )

        assert count == 0
        mock_db_session.add.assert_not_called()

    def test_skip_empty_ref_strings(self, mock_db_session):
        """Test that empty reference strings are skipped."""
        count = insert_ref_links(
            mock_db_session,
            ["", "  ", ""],
            "HEADLINE",
            123,
            "CGD",
            "TEST"
        )

        assert count == 0

    def test_skip_invalid_references(self, mock_db_session):
        """Test that invalid references are skipped."""
        with patch('loading.load_headlines.get_reference_no', return_value=None):
            count = insert_ref_links(
                mock_db_session,
                ["INVALID:REF"],
                "HEADLINE",
                123,
                "CGD",
                "TEST"
            )

        assert count == 0

    def test_multiple_refs_inserted(self, mock_db_session):
        """Test inserting multiple ref_links."""
        mock_db_session.query.return_value.filter.return_value.first.return_value = None

        def get_ref_side_effect(session, ref_str, acronym):
            if ref_str.strip() == "CGD:1000":
                return 1000
            elif ref_str.strip() == "CGD:2000":
                return 2000
            return None

        with patch('loading.load_headlines.get_reference_no', side_effect=get_ref_side_effect):
            count = insert_ref_links(
                mock_db_session,
                ["CGD:1000", "CGD:2000"],
                "HEADLINE",
                123,
                "CGD",
                "TEST"
            )

        assert count == 2


class TestLoadHeadlines:
    """Tests for load_headlines function."""

    def test_load_basic_data(self, mock_db_session, temp_file):
        """Test loading basic headline data."""
        content = "ORF19.1\tName description\tCGD:100\tHeadline text\tCGD:200\n"
        data_file = temp_file("headlines.txt", content)

        mock_feature = MagicMock()
        mock_feature.feature_no = 123
        mock_db_session.query.return_value.filter.return_value.first.return_value = mock_feature

        with patch('loading.load_headlines.insert_ref_links', return_value=1):
            stats = load_headlines(
                mock_db_session,
                data_file,
                "TEST",
                "CGD",
                dry_run=True
            )

        assert stats['processed'] == 1
        assert stats['features_updated'] == 1
        assert mock_feature.headline == "Headline text"
        assert mock_feature.name_description == "Name description"

    def test_skip_empty_lines(self, mock_db_session, temp_file):
        """Test that empty lines are skipped."""
        content = "ORF19.1\tDesc\tCGD:100\tHeadline\tCGD:200\n\nORF19.2\tDesc2\tCGD:101\tHeadline2\tCGD:201\n"
        data_file = temp_file("headlines.txt", content)

        mock_feature = MagicMock()
        mock_feature.feature_no = 123
        mock_db_session.query.return_value.filter.return_value.first.return_value = mock_feature

        with patch('loading.load_headlines.insert_ref_links', return_value=0):
            stats = load_headlines(
                mock_db_session,
                data_file,
                "TEST",
                dry_run=True
            )

        assert stats['processed'] == 2

    def test_skip_invalid_line_format(self, mock_db_session, temp_file):
        """Test that lines with fewer than 5 columns are skipped."""
        content = "ORF19.1\tDesc\tCGD:100\n"  # Only 3 columns
        data_file = temp_file("headlines.txt", content)

        stats = load_headlines(
            mock_db_session,
            data_file,
            "TEST",
            dry_run=True
        )

        assert stats['processed'] == 0

    def test_feature_not_found(self, mock_db_session, temp_file):
        """Test handling of features not found in database."""
        content = "NONEXISTENT\tDesc\tCGD:100\tHeadline\tCGD:200\n"
        data_file = temp_file("headlines.txt", content)

        mock_db_session.query.return_value.filter.return_value.first.return_value = None

        stats = load_headlines(
            mock_db_session,
            data_file,
            "TEST",
            dry_run=True
        )

        assert stats['processed'] == 1
        assert stats['not_found'] == 1
        assert stats['features_updated'] == 0

    def test_feature_name_uppercase(self, mock_db_session, temp_file):
        """Test that feature names are converted to uppercase."""
        content = "orf19.1\tDesc\tCGD:100\tHeadline\tCGD:200\n"
        data_file = temp_file("headlines.txt", content)

        mock_feature = MagicMock()
        mock_feature.feature_no = 123
        mock_db_session.query.return_value.filter.return_value.first.return_value = mock_feature

        with patch('loading.load_headlines.insert_ref_links', return_value=0):
            load_headlines(
                mock_db_session,
                data_file,
                "TEST",
                dry_run=True
            )

        # Verify query was made with uppercase feature name
        mock_db_session.query.return_value.filter.assert_called()

    def test_text_cleanup_applied(self, mock_db_session, temp_file):
        """Test that cleanup_text is applied to descriptions and headlines."""
        content = 'ORF19.1\t"Name desc"\tCGD:100\t"Headline 5\'\'"\tCGD:200\n'
        data_file = temp_file("headlines.txt", content)

        mock_feature = MagicMock()
        mock_feature.feature_no = 123
        mock_db_session.query.return_value.filter.return_value.first.return_value = mock_feature

        with patch('loading.load_headlines.insert_ref_links', return_value=0):
            load_headlines(
                mock_db_session,
                data_file,
                "TEST",
                dry_run=True
            )

        # Quotes and apostrophes should be cleaned
        assert mock_feature.name_description == "Name desc"
        assert mock_feature.headline == "Headline 5'"

    def test_ref_links_for_both_columns(self, mock_db_session, temp_file):
        """Test that ref_links are created for both columns."""
        content = "ORF19.1\tDesc\tCGD:100|CGD:101\tHeadline\tCGD:200|CGD:201\n"
        data_file = temp_file("headlines.txt", content)

        mock_feature = MagicMock()
        mock_feature.feature_no = 123
        mock_db_session.query.return_value.filter.return_value.first.return_value = mock_feature

        insert_calls = []

        def track_insert_calls(*args, **kwargs):
            insert_calls.append(args)
            return 2

        with patch('loading.load_headlines.insert_ref_links', side_effect=track_insert_calls):
            stats = load_headlines(
                mock_db_session,
                data_file,
                "TEST",
                dry_run=True
            )

        # Should have been called twice (once for NAME_DESCRIPTION, once for HEADLINE)
        assert len(insert_calls) == 2
        assert stats['ref_links_inserted'] == 4

    def test_empty_file(self, mock_db_session, temp_file):
        """Test loading empty file."""
        data_file = temp_file("headlines.txt", "")

        stats = load_headlines(
            mock_db_session,
            data_file,
            "TEST",
            dry_run=True
        )

        assert stats['processed'] == 0


class TestMainFunction:
    """Tests for the main function."""

    def test_main_missing_data_file(self, temp_dir):
        """Test main with missing data file."""
        from loading.load_headlines import main

        with patch.object(sys, 'argv', [
            'prog', str(temp_dir / 'nonexistent.txt')
        ]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1

    @patch('loading.load_headlines.SessionLocal')
    def test_main_dry_run(self, mock_session_local, temp_file):
        """Test main function with dry-run option."""
        from loading.load_headlines import main

        content = "ORF19.1\tDesc\tCGD:100\tHeadline\tCGD:200\n"
        data_file = temp_file("headlines.txt", content)

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.query.return_value.filter.return_value.first.return_value = None

        with patch.object(sys, 'argv', [
            'prog', str(data_file), '--dry-run'
        ]):
            main()

        # Should have called rollback for dry run
        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()

    @patch('loading.load_headlines.SessionLocal')
    def test_main_with_options(self, mock_session_local, temp_file):
        """Test main with various options."""
        from loading.load_headlines import main

        content = "ORF19.1\tDesc\tCGD:100\tHeadline\tCGD:200\n"
        data_file = temp_file("headlines.txt", content)

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.query.return_value.filter.return_value.first.return_value = None

        with patch.object(sys, 'argv', [
            'prog', str(data_file),
            '--created-by', 'TESTUSER',
            '--project-acronym', 'SGD',
            '--verbose'
        ]):
            main()

        mock_session.commit.assert_called_once()


class TestEdgeCases:
    """Tests for edge cases."""

    def test_cleanup_text_multiple_spaces(self):
        """Test that multiple spaces are preserved in cleanup."""
        result = cleanup_text("word1   word2")
        assert result == "word1   word2"

    def test_ref_string_with_spaces(self, mock_db_session):
        """Test reference string with extra spaces."""
        result = get_reference_no(mock_db_session, "  CGD:123  ", "CGD")
        assert result == 123

    def test_pmid_with_leading_zeros(self, mock_db_session):
        """Test PMID with leading zeros."""
        mock_ref = MagicMock()
        mock_ref.reference_no = 999
        mock_db_session.query.return_value.filter.return_value.first.return_value = mock_ref

        result = get_reference_no(mock_db_session, "PMID:00012345", "CGD")
        # Should parse as integer (leading zeros removed)
        assert result == 999

    def test_multiple_pipe_separated_refs(self, mock_db_session, temp_file):
        """Test handling of multiple pipe-separated references."""
        content = "ORF19.1\tDesc\tCGD:100|PMID:123|CGD:200\tHeadline\tCGD:300\n"
        data_file = temp_file("headlines.txt", content)

        mock_feature = MagicMock()
        mock_feature.feature_no = 123
        mock_db_session.query.return_value.filter.return_value.first.return_value = mock_feature

        ref_lists = []

        def capture_refs(session, refs, *args, **kwargs):
            ref_lists.append(refs)
            return len(refs)

        with patch('loading.load_headlines.insert_ref_links', side_effect=capture_refs):
            load_headlines(
                mock_db_session,
                data_file,
                "TEST",
                dry_run=True
            )

        # NAME_DESCRIPTION refs should be split by pipe
        assert len(ref_lists[0]) == 3
        assert "CGD:100" in ref_lists[0]
        assert "PMID:123" in ref_lists[0]
        assert "CGD:200" in ref_lists[0]
