#!/usr/bin/env python3
"""
Unit tests for scripts/cron/check_data.py

Tests the data checking functionality for business rule violations.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from cron.check_data import (
    DataChecker,
    send_report_email,
)


class TestDataCheckerInit:
    """Tests for DataChecker initialization."""

    def test_init(self, mock_db_session):
        """Test DataChecker initialization."""
        checker = DataChecker(mock_db_session)

        assert checker.session == mock_db_session
        assert checker.issues == []

    def test_add_issue(self, mock_db_session):
        """Test adding an issue."""
        checker = DataChecker(mock_db_session)

        checker.add_issue("Test Category", "Test message")

        assert len(checker.issues) == 1
        assert checker.issues[0]["category"] == "Test Category"
        assert checker.issues[0]["message"] == "Test message"

    def test_add_multiple_issues(self, mock_db_session):
        """Test adding multiple issues."""
        checker = DataChecker(mock_db_session)

        checker.add_issue("Cat1", "Msg1")
        checker.add_issue("Cat2", "Msg2")
        checker.add_issue("Cat1", "Msg3")

        assert len(checker.issues) == 3


class TestCheckDuplicateUrlTypes:
    """Tests for check_duplicate_url_types method."""

    def test_no_duplicates(self, mock_db_session):
        """Test with no duplicate URL types."""
        checker = DataChecker(mock_db_session)
        mock_db_session.execute.return_value = iter([])

        count = checker.check_duplicate_url_types()

        assert count == 0
        assert len(checker.issues) == 0

    def test_with_duplicates(self, mock_db_session):
        """Test with duplicate URL types found."""
        checker = DataChecker(mock_db_session)
        mock_db_session.execute.return_value = iter([
            ("GENE1", "GenBank", 3),
            ("GENE2", "UniProt", 2),
        ])

        count = checker.check_duplicate_url_types()

        assert count == 2
        assert len(checker.issues) == 2
        assert "GENE1" in checker.issues[0]["message"]
        assert "GenBank" in checker.issues[0]["message"]


class TestCheckGeneReservations:
    """Tests for check_gene_reservations method."""

    def test_no_invalid_emails(self, mock_db_session):
        """Test with no invalid reservation emails."""
        checker = DataChecker(mock_db_session)
        mock_db_session.execute.return_value = iter([])

        count = checker.check_gene_reservations("SC5314", 1)

        assert count == 0
        assert len(checker.issues) == 0

    def test_with_invalid_emails(self, mock_db_session):
        """Test with invalid reservation emails."""
        checker = DataChecker(mock_db_session)
        mock_db_session.execute.return_value = iter([
            ("RES1", None),
            ("RES2", "invalid_email"),
        ])

        count = checker.check_gene_reservations("SC5314", 1)

        assert count == 2
        assert len(checker.issues) == 2
        assert "Gene Reservation (SC5314)" in checker.issues[0]["category"]

    def test_strain_in_category(self, mock_db_session):
        """Test that strain abbreviation appears in category."""
        checker = DataChecker(mock_db_session)
        mock_db_session.execute.return_value = iter([
            ("RES1", "bad"),
        ])

        checker.check_gene_reservations("WO-1", 2)

        assert "WO-1" in checker.issues[0]["category"]


class TestCheckLocusVsAliasNames:
    """Tests for check_locus_vs_alias_names method."""

    def test_no_conflicts(self, mock_db_session):
        """Test with no locus/alias conflicts."""
        checker = DataChecker(mock_db_session)
        mock_db_session.execute.return_value = iter([])

        count = checker.check_locus_vs_alias_names("SC5314", 1)

        assert count == 0

    def test_with_conflicts(self, mock_db_session):
        """Test with locus/alias conflicts."""
        checker = DataChecker(mock_db_session)
        mock_db_session.execute.return_value = iter([
            ("GENE1", "ACT1", "GENE2"),
            ("GENE3", "TUB1", "GENE4"),
        ])

        count = checker.check_locus_vs_alias_names("SC5314", 1)

        assert count == 2
        assert len(checker.issues) == 2
        assert "GENE1" in checker.issues[0]["message"]
        assert "ACT1" in checker.issues[0]["message"]


class TestCheckPseudogenesWithGo:
    """Tests for check_pseudogenes_with_go method."""

    def test_no_pseudogenes_with_go(self, mock_db_session):
        """Test with no pseudogenes having GO annotations."""
        checker = DataChecker(mock_db_session)
        mock_db_session.execute.return_value = iter([])

        count = checker.check_pseudogenes_with_go("SC5314", 1)

        assert count == 0

    def test_with_pseudogenes_having_go(self, mock_db_session):
        """Test pseudogenes with GO annotations."""
        checker = DataChecker(mock_db_session)
        mock_db_session.execute.return_value = iter([
            ("PSEUDO1", "pseudogene"),
            ("PSEUDO2", "pseudogene"),
        ])

        count = checker.check_pseudogenes_with_go("SC5314", 1)

        assert count == 2
        assert "Pseudogene GO" in checker.issues[0]["category"]
        assert "PSEUDO1" in checker.issues[0]["message"]


class TestCheckHeadlineDescriptions:
    """Tests for check_headline_descriptions method."""

    def test_no_multiple_refs(self, mock_db_session):
        """Test with no features having multiple headline refs."""
        checker = DataChecker(mock_db_session)
        mock_db_session.execute.return_value = iter([])

        count = checker.check_headline_descriptions("SC5314", 1)

        assert count == 0

    def test_with_multiple_refs(self, mock_db_session):
        """Test features with multiple headline refs."""
        checker = DataChecker(mock_db_session)
        mock_db_session.execute.return_value = iter([
            ("GENE1", 3),
            ("GENE2", 2),
        ])

        count = checker.check_headline_descriptions("SC5314", 1)

        assert count == 2
        assert "Headline Refs" in checker.issues[0]["category"]
        assert "3 references" in checker.issues[0]["message"]


class TestGetAllStrains:
    """Tests for get_all_strains method."""

    def test_returns_strains(self, mock_db_session):
        """Test returning strain list."""
        checker = DataChecker(mock_db_session)
        mock_db_session.execute.return_value = iter([
            (1, "SC5314"),
            (2, "WO-1"),
            (3, "CD36"),
        ])

        strains = checker.get_all_strains()

        assert len(strains) == 3
        assert strains[0] == {"organism_no": 1, "organism_abbrev": "SC5314"}
        assert strains[1] == {"organism_no": 2, "organism_abbrev": "WO-1"}

    def test_empty_strains(self, mock_db_session):
        """Test with no strains."""
        checker = DataChecker(mock_db_session)
        mock_db_session.execute.return_value = iter([])

        strains = checker.get_all_strains()

        assert strains == []


class TestRunAllChecks:
    """Tests for run_all_checks method."""

    def test_runs_all_checks(self, mock_db_session):
        """Test that all checks are run."""
        checker = DataChecker(mock_db_session)

        # Mock different results for different queries
        call_count = [0]
        def mock_execute(*args, **kwargs):
            call_count[0] += 1
            result = MagicMock()
            if call_count[0] == 1:
                # get_all_strains - return no strains for simplicity
                result.__iter__ = lambda self: iter([])
            else:
                result.__iter__ = lambda self: iter([])
            return result

        mock_db_session.execute.side_effect = mock_execute

        # Override get_all_strains to return empty
        checker.get_all_strains = MagicMock(return_value=[])

        stats = checker.run_all_checks()

        assert "total_issues" in stats
        assert "checks_run" in stats
        assert stats["checks_run"] >= 1

    def test_counts_issues(self, mock_db_session):
        """Test that issues are counted correctly."""
        checker = DataChecker(mock_db_session)

        # Add some issues
        checker.add_issue("Test", "Issue 1")
        checker.add_issue("Test", "Issue 2")

        # Override all checks to return 0
        checker.check_duplicate_url_types = MagicMock(return_value=0)
        checker.get_all_strains = MagicMock(return_value=[])

        stats = checker.run_all_checks()

        assert stats["total_issues"] == 2


class TestSendReportEmail:
    """Tests for send_report_email function."""

    @patch('cron.check_data.CURATOR_EMAIL', None)
    def test_no_curator_email(self, caplog):
        """Test when CURATOR_EMAIL is not set."""
        issues = [{"category": "Test", "message": "Issue"}]

        send_report_email(issues)

        assert "CURATOR_EMAIL not set" in caplog.text

    @patch('cron.check_data.CURATOR_EMAIL', 'test@example.com')
    @patch('cron.check_data.logger')
    def test_no_issues(self, mock_logger):
        """Test when there are no issues."""
        send_report_email([])

        mock_logger.info.assert_called_with("No issues to report")

    @patch('cron.check_data.CURATOR_EMAIL', 'test@example.com')
    @patch('cron.check_data.logger')
    def test_with_issues(self, mock_logger):
        """Test with issues to report."""
        issues = [
            {"category": "Cat1", "message": "Msg1"},
            {"category": "Cat1", "message": "Msg2"},
            {"category": "Cat2", "message": "Msg3"},
        ]

        send_report_email(issues)

        # Should log that it would send report
        mock_logger.info.assert_called()

    @patch('cron.check_data.CURATOR_EMAIL', 'curator@cgd.org')
    @patch('cron.check_data.logger')
    def test_groups_by_category(self, mock_logger):
        """Test that issues are grouped by category."""
        issues = [
            {"category": "Duplicates", "message": "Dup 1"},
            {"category": "Conflicts", "message": "Conf 1"},
            {"category": "Duplicates", "message": "Dup 2"},
        ]

        send_report_email(issues)

        # Function should complete without error and log
        mock_logger.info.assert_called()


class TestMainFunction:
    """Tests for the main function."""

    @patch('cron.check_data.SessionLocal')
    def test_main_no_issues(self, mock_session_local):
        """Test main with no issues found."""
        from cron.check_data import main

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

        # Mock all database queries to return empty
        mock_session.execute.return_value = iter([])

        with patch('cron.check_data.DataChecker') as mock_checker_class:
            mock_checker = MagicMock()
            mock_checker.issues = []
            mock_checker.run_all_checks.return_value = {"total_issues": 0, "checks_run": 5}
            mock_checker_class.return_value = mock_checker

            result = main()

        assert result == 0

    @patch('cron.check_data.SessionLocal')
    def test_main_with_issues(self, mock_session_local):
        """Test main with issues found."""
        from cron.check_data import main

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

        with patch('cron.check_data.DataChecker') as mock_checker_class:
            mock_checker = MagicMock()
            mock_checker.issues = [{"category": "Test", "message": "Issue"}]
            mock_checker.run_all_checks.return_value = {"total_issues": 1, "checks_run": 5}
            mock_checker_class.return_value = mock_checker

            with patch('cron.check_data.send_report_email'):
                result = main()

        assert result == 1  # Issues found

    @patch('cron.check_data.SessionLocal')
    def test_main_handles_exception(self, mock_session_local, caplog):
        """Test main handles exceptions."""
        from cron.check_data import main

        mock_session_local.side_effect = Exception("Database error")

        result = main()

        assert result == 1
        assert "Error" in caplog.text


class TestEdgeCases:
    """Tests for edge cases."""

    def test_issue_with_special_characters(self, mock_db_session):
        """Test issue messages with special characters."""
        checker = DataChecker(mock_db_session)

        checker.add_issue("Test <Category>", "Message with 'quotes' and \"double\"")

        assert len(checker.issues) == 1
        assert "quotes" in checker.issues[0]["message"]

    def test_empty_message(self, mock_db_session):
        """Test adding issue with empty message."""
        checker = DataChecker(mock_db_session)

        checker.add_issue("Category", "")

        assert len(checker.issues) == 1
        assert checker.issues[0]["message"] == ""

    def test_very_long_message(self, mock_db_session):
        """Test adding issue with very long message."""
        checker = DataChecker(mock_db_session)
        long_message = "x" * 10000

        checker.add_issue("Category", long_message)

        assert len(checker.issues) == 1
        assert len(checker.issues[0]["message"]) == 10000

    def test_unicode_in_messages(self, mock_db_session):
        """Test Unicode characters in issue messages."""
        checker = DataChecker(mock_db_session)

        checker.add_issue("Café", "Message with émojis 🧬 and αβγ")

        assert len(checker.issues) == 1
        assert "🧬" in checker.issues[0]["message"]

    def test_strain_with_special_chars(self, mock_db_session):
        """Test strain abbreviations with special characters."""
        checker = DataChecker(mock_db_session)
        mock_db_session.execute.return_value = iter([
            ("RES1", "bad@email"),
        ])

        count = checker.check_gene_reservations("C. albicans (SC5314)", 1)

        assert count == 1
        assert "C. albicans (SC5314)" in checker.issues[0]["category"]

    def test_run_checks_with_multiple_strains(self, mock_db_session):
        """Test running checks across multiple strains."""
        checker = DataChecker(mock_db_session)

        # Setup mock to return different things for different calls
        checker.check_duplicate_url_types = MagicMock(return_value=1)
        checker.check_gene_reservations = MagicMock(return_value=2)
        checker.check_locus_vs_alias_names = MagicMock(return_value=0)
        checker.check_headline_descriptions = MagicMock(return_value=1)
        checker.check_pseudogenes_with_go = MagicMock(return_value=0)
        checker.get_all_strains = MagicMock(return_value=[
            {"organism_no": 1, "organism_abbrev": "SC5314"},
            {"organism_no": 2, "organism_abbrev": "WO-1"},
        ])

        # Add issues to match the mocked return values
        checker.add_issue("Dup", "Issue 1")
        for _ in range(4):  # 2 strains * 2 issues per strain from gene_reservations
            checker.add_issue("GR", "Gene res issue")
        for _ in range(2):  # 2 strains * 1 issue per strain from headline
            checker.add_issue("HL", "Headline issue")

        stats = checker.run_all_checks()

        # Should run checks for both strains
        assert checker.check_gene_reservations.call_count == 2
        assert checker.check_locus_vs_alias_names.call_count == 2
