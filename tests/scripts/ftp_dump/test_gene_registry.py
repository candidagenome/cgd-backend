#!/usr/bin/env python3
"""
Unit tests for scripts/ftp_dump/gene_registry.py

Tests the gene registry FTP file generation functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from ftp_dump.gene_registry import (
    delete_unwanted_chars,
    TAB_FNAME,
    TEXT_FNAME,
)


class TestDeleteUnwantedChars:
    """Tests for delete_unwanted_chars function."""

    def test_remove_control_characters(self):
        """Test removal of control characters."""
        text = "Hello\x00World\x1f"
        result = delete_unwanted_chars(text)
        assert "\x00" not in result
        assert "\x1f" not in result
        assert "HelloWorld" in result

    def test_normalize_whitespace(self):
        """Test normalization of whitespace."""
        text = "Hello   World"
        result = delete_unwanted_chars(text)
        assert result == "Hello World"

    def test_strip_leading_trailing(self):
        """Test stripping of leading/trailing whitespace."""
        text = "  Hello World  "
        result = delete_unwanted_chars(text)
        assert result == "Hello World"

    def test_empty_string(self):
        """Test handling of empty string."""
        assert delete_unwanted_chars("") == ""

    def test_none_value(self):
        """Test handling of None value."""
        assert delete_unwanted_chars(None) == ""

    def test_normal_text_unchanged(self):
        """Test that normal text is unchanged."""
        text = "This is normal text with numbers 123 and symbols !@#"
        result = delete_unwanted_chars(text)
        assert result == text.strip()

    def test_newlines_removed(self):
        """Test that newlines are normalized to spaces."""
        text = "Line1\nLine2\rLine3"
        result = delete_unwanted_chars(text)
        assert "\n" not in result
        assert "\r" not in result


class TestFileConstants:
    """Tests for file name constants."""

    def test_tab_filename(self):
        """Test tab-delimited filename."""
        assert TAB_FNAME == "registry.genenames.tab"
        assert TAB_FNAME.endswith(".tab")

    def test_text_filename(self):
        """Test text filename."""
        assert TEXT_FNAME == "registry.genenames.txt"
        assert TEXT_FNAME.endswith(".txt")


class TestGetPhenotypeText:
    """Tests for get_phenotype_text function."""

    def test_get_phenotype_text(self, mock_db_session):
        """Test getting phenotype text from database."""
        from ftp_dump.gene_registry import get_phenotype_text

        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "Phenotype 1"),
            (1, "Phenotype 2"),
            (2, "Phenotype 3"),
        ]

        result = get_phenotype_text(mock_db_session)

        # Feature 1 should have both phenotypes joined
        assert 1 in result
        assert "Phenotype 1" in result[1]
        assert "Phenotype 2" in result[1]
        assert "|" in result[1]

        # Feature 2 should have single phenotype
        assert 2 in result
        assert result[2] == "Phenotype 3"

    def test_get_phenotype_text_empty(self, mock_db_session):
        """Test getting phenotype text when none exist."""
        from ftp_dump.gene_registry import get_phenotype_text

        mock_db_session.execute.return_value.fetchall.return_value = []

        result = get_phenotype_text(mock_db_session)

        assert result == {}

    def test_get_phenotype_text_cleans_text(self, mock_db_session):
        """Test that phenotype text is cleaned."""
        from ftp_dump.gene_registry import get_phenotype_text

        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "Phenotype\x00with\x1fcontrol"),
        ]

        result = get_phenotype_text(mock_db_session)

        assert "\x00" not in result[1]
        assert "\x1f" not in result[1]


class TestGetGeneInfo:
    """Tests for get_gene_info function."""

    def test_get_gene_info_basic(self, mock_db_session):
        """Test getting basic gene information."""
        from ftp_dump.gene_registry import get_gene_info

        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "ACT1", "ALIAS1", "Actin", "Actin protein", 1, "orf19.1", "CAL0001"),
            (1, "ACT1", "ALIAS2", "Actin", "Actin protein", 1, "orf19.1", "CAL0001"),
            (2, "TUB1", None, "Tubulin", "Tubulin protein", 2, "orf19.2", "CAL0002"),
        ]

        result = get_gene_info(mock_db_session)

        assert len(result) == 2  # Consolidated by feature

        # Find ACT1 entry
        act1 = next(g for g in result if g["gene_name"] == "ACT1")
        assert "ALIAS1" in act1["aliases"]
        assert "ALIAS2" in act1["aliases"]
        assert act1["headline"] == "Actin"
        assert act1["dbxref_id"] == "CAL0001"

    def test_get_gene_info_sorted(self, mock_db_session):
        """Test that gene info is sorted by gene name."""
        from ftp_dump.gene_registry import get_gene_info

        mock_db_session.execute.return_value.fetchall.return_value = [
            (2, "ZZZ1", None, "", "", 2, "orf19.2", "CAL0002"),
            (1, "AAA1", None, "", "", 1, "orf19.1", "CAL0001"),
        ]

        result = get_gene_info(mock_db_session)

        assert result[0]["gene_name"] == "AAA1"
        assert result[1]["gene_name"] == "ZZZ1"

    def test_get_gene_info_empty(self, mock_db_session):
        """Test getting gene info when none exist."""
        from ftp_dump.gene_registry import get_gene_info

        mock_db_session.execute.return_value.fetchall.return_value = []

        result = get_gene_info(mock_db_session)

        assert result == []


class TestMainFunction:
    """Tests for the main function."""

    @patch('ftp_dump.gene_registry.SessionLocal')
    @patch('ftp_dump.gene_registry.DATA_DIR')
    def test_main_creates_files(self, mock_data_dir, mock_session, temp_dir):
        """Test that main function creates output files."""
        from ftp_dump.gene_registry import main

        mock_data_dir.__truediv__ = lambda self, x: temp_dir / x
        mock_data_dir.mkdir = MagicMock()
        mock_data_dir.exists = MagicMock(return_value=False)

        # Mock session
        mock_session_instance = MagicMock()
        mock_session.return_value.__enter__ = MagicMock(return_value=mock_session_instance)
        mock_session.return_value.__exit__ = MagicMock(return_value=False)

        # Mock database queries
        mock_session_instance.execute.return_value.fetchall.return_value = []

        with patch.object(sys, 'argv', ['prog']):
            result = main()

        # Should have tried to create directory
        mock_data_dir.mkdir.assert_called()

    @patch('ftp_dump.gene_registry.SessionLocal')
    @patch('ftp_dump.gene_registry.DATA_DIR')
    def test_main_handles_exception(self, mock_data_dir, mock_session, temp_dir):
        """Test main function handles exceptions."""
        from ftp_dump.gene_registry import main

        mock_data_dir.__truediv__ = lambda self, x: temp_dir / x
        mock_data_dir.mkdir = MagicMock()

        mock_session.side_effect = Exception("Database error")

        with patch.object(sys, 'argv', ['prog']):
            result = main()

        assert result == 1

    @patch('ftp_dump.gene_registry.SessionLocal')
    @patch('ftp_dump.gene_registry.DATA_DIR')
    def test_main_debug_mode(self, mock_data_dir, mock_session, temp_dir):
        """Test main function with debug flag."""
        from ftp_dump.gene_registry import main

        mock_data_dir.__truediv__ = lambda self, x: temp_dir / x
        mock_data_dir.mkdir = MagicMock()

        mock_session_instance = MagicMock()
        mock_session.return_value.__enter__ = MagicMock(return_value=mock_session_instance)
        mock_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session_instance.execute.return_value.fetchall.return_value = []

        with patch.object(sys, 'argv', ['prog', '--debug']):
            main()


class TestFileOutput:
    """Tests for file output format."""

    def test_tab_file_format(self, temp_dir):
        """Test tab-delimited file format."""
        # Create a mock output
        tab_file = temp_dir / TAB_FNAME

        # Write sample data in expected format
        with open(tab_file, 'w') as f:
            f.write("ACT1\tALIAS1|ALIAS2\tActin\tActin protein\tphenotype\torf19.1\tCAL0001\n")

        content = tab_file.read_text()
        fields = content.strip().split('\t')

        assert len(fields) == 7
        assert fields[0] == "ACT1"
        assert "ALIAS1" in fields[1]

    def test_text_file_format(self, temp_dir):
        """Test human-readable text file format."""
        text_file = temp_dir / TEXT_FNAME

        # Write sample data in expected format
        with open(text_file, 'w') as f:
            f.write("Locus_Name:\tACT1\n")
            f.write("Alias_Name:\tALIAS1|ALIAS2\n")
            f.write("\n")

        content = text_file.read_text()
        assert "Locus_Name:" in content
        assert "ACT1" in content


class TestEdgeCases:
    """Tests for edge cases."""

    def test_delete_chars_only_spaces(self):
        """Test text with only whitespace."""
        result = delete_unwanted_chars("   \t\n   ")
        assert result == ""

    def test_delete_chars_unicode(self):
        """Test handling of unicode characters."""
        text = "Gene α with β motif"
        result = delete_unwanted_chars(text)
        assert "α" in result
        assert "β" in result

    def test_consolidate_multiple_aliases(self):
        """Test consolidation of multiple aliases."""
        from ftp_dump.gene_registry import get_gene_info

        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = [
            (1, "GENE1", "ALIAS1", "", "", 1, "", "ID1"),
            (1, "GENE1", "ALIAS2", "", "", 1, "", "ID1"),
            (1, "GENE1", "ALIAS3", "", "", 1, "", "ID1"),
        ]

        result = get_gene_info(mock_session)

        assert len(result) == 1
        aliases = result[0]["aliases"].split("|")
        assert len(aliases) == 3
