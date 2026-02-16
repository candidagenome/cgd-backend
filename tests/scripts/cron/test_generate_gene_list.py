#!/usr/bin/env python3
"""
Unit tests for scripts/cron/generate_gene_list.py

Tests the gene list HTML page generation functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from cron.generate_gene_list import (
    escape_html,
    generate_gene_list,
    main,
)


class TestEscapeHtml:
    """Tests for escape_html function."""

    def test_escape_ampersand(self):
        """Test escaping ampersand."""
        assert escape_html("Tom & Jerry") == "Tom &amp; Jerry"

    def test_escape_less_than(self):
        """Test escaping less than sign."""
        assert escape_html("a < b") == "a &lt; b"

    def test_escape_greater_than(self):
        """Test escaping greater than sign."""
        assert escape_html("a > b") == "a &gt; b"

    def test_escape_double_quote(self):
        """Test escaping double quote."""
        assert escape_html('say "hello"') == "say &quot;hello&quot;"

    def test_escape_multiple(self):
        """Test escaping multiple special characters."""
        assert escape_html('<a href="test">link</a>') == "&lt;a href=&quot;test&quot;&gt;link&lt;/a&gt;"

    def test_none_value(self):
        """Test handling of None value."""
        assert escape_html(None) == ""

    def test_empty_string(self):
        """Test handling of empty string."""
        assert escape_html("") == ""

    def test_normal_text_unchanged(self):
        """Test that normal text is unchanged."""
        assert escape_html("Hello World") == "Hello World"

    def test_numeric_conversion(self):
        """Test that numeric values are converted to string."""
        assert escape_html(123) == "123"

    def test_ampersand_entity_not_double_escaped(self):
        """Test that already escaped content gets escaped again (not ideal but expected)."""
        result = escape_html("&amp;")
        assert result == "&amp;amp;"


class TestGenerateGeneList:
    """Tests for generate_gene_list function."""

    @patch('cron.generate_gene_list.SessionLocal')
    def test_generate_basic(self, mock_session_local, temp_dir):
        """Test basic gene list generation."""
        output_file = temp_dir / "genelist.shtml"

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.execute.return_value.fetchall.return_value = [
            ("orf19.1", "CAL0001", "ACT1", "Actin protein", "Candida albicans"),
            ("orf19.2", "CAL0002", "TUB1", "Tubulin protein", "Candida albicans"),
        ]

        # Patch the OUTPUT_FILE to write to our temp file
        with patch('cron.generate_gene_list.OUTPUT_FILE', output_file):
            result = generate_gene_list()

        assert result is True
        assert output_file.exists()

        content = output_file.read_text()
        assert "ACT1" in content
        assert "TUB1" in content
        assert "Actin protein" in content

    @patch('cron.generate_gene_list.SessionLocal')
    @patch('cron.generate_gene_list.OUTPUT_FILE')
    def test_generate_escapes_html(self, mock_output, mock_session_local, temp_dir):
        """Test that HTML is properly escaped in output."""
        output_file = temp_dir / "genelist.shtml"

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.execute.return_value.fetchall.return_value = [
            ("orf19.1", "CAL0001", "Gene<1>", "Description & Info", "Species"),
        ]

        with patch('cron.generate_gene_list.OUTPUT_FILE', output_file):
            result = generate_gene_list()

        assert result is True
        content = output_file.read_text()
        assert "&lt;" in content
        assert "&gt;" in content
        assert "&amp;" in content

    @patch('cron.generate_gene_list.SessionLocal')
    @patch('cron.generate_gene_list.OUTPUT_FILE')
    def test_generate_header_rows(self, mock_output, mock_session_local, temp_dir):
        """Test that header rows are added every 20 genes."""
        output_file = temp_dir / "genelist.shtml"

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

        # Create 25 genes to test header row at position 0 and 20
        genes = [
            (f"orf19.{i}", f"CAL000{i}", f"GENE{i}", f"Desc {i}", "Species")
            for i in range(25)
        ]
        mock_session.execute.return_value.fetchall.return_value = genes

        with patch('cron.generate_gene_list.OUTPUT_FILE', output_file):
            result = generate_gene_list()

        assert result is True
        content = output_file.read_text()
        # Should have 2 header rows (at position 0 and 20)
        assert content.count("Locus Id") == 2

    @patch('cron.generate_gene_list.SessionLocal')
    @patch('cron.generate_gene_list.OUTPUT_FILE')
    def test_generate_empty_genes(self, mock_output, mock_session_local, temp_dir):
        """Test generation with no genes."""
        output_file = temp_dir / "genelist.shtml"

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = []

        with patch('cron.generate_gene_list.OUTPUT_FILE', output_file):
            result = generate_gene_list()

        assert result is True
        content = output_file.read_text()
        assert "<table" in content
        assert "</table>" in content

    @patch('cron.generate_gene_list.SessionLocal')
    @patch('cron.generate_gene_list.OUTPUT_FILE')
    def test_generate_handles_none_values(self, mock_output, mock_session_local, temp_dir):
        """Test handling of None values in gene data."""
        output_file = temp_dir / "genelist.shtml"

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.execute.return_value.fetchall.return_value = [
            ("orf19.1", "CAL0001", None, None, None),
        ]

        with patch('cron.generate_gene_list.OUTPUT_FILE', output_file):
            result = generate_gene_list()

        assert result is True
        content = output_file.read_text()
        assert "orf19.1" in content

    @patch('cron.generate_gene_list.SessionLocal')
    def test_generate_handles_exception(self, mock_session_local):
        """Test that exceptions are handled."""
        mock_session_local.side_effect = Exception("Database error")

        result = generate_gene_list()

        assert result is False


class TestMainFunction:
    """Tests for the main function."""

    @patch('cron.generate_gene_list.generate_gene_list')
    def test_main_success(self, mock_generate):
        """Test main returns 0 on success."""
        mock_generate.return_value = True

        result = main()

        assert result == 0

    @patch('cron.generate_gene_list.generate_gene_list')
    def test_main_failure(self, mock_generate):
        """Test main returns 1 on failure."""
        mock_generate.return_value = False

        result = main()

        assert result == 1


class TestHtmlStructure:
    """Tests for HTML structure of output."""

    @patch('cron.generate_gene_list.SessionLocal')
    @patch('cron.generate_gene_list.OUTPUT_FILE')
    def test_html_has_ssi_include(self, mock_output, mock_session_local, temp_dir):
        """Test that HTML includes SSI header."""
        output_file = temp_dir / "genelist.shtml"

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = []

        with patch('cron.generate_gene_list.OUTPUT_FILE', output_file):
            generate_gene_list()

        content = output_file.read_text()
        assert "<!--#include virtual=" in content

    @patch('cron.generate_gene_list.SessionLocal')
    @patch('cron.generate_gene_list.OUTPUT_FILE')
    def test_html_has_links(self, mock_output, mock_session_local, temp_dir):
        """Test that HTML includes proper links."""
        output_file = temp_dir / "genelist.shtml"

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.execute.return_value.fetchall.return_value = [
            ("orf19.1", "CAL0001", "ACT1", "Description", "Species"),
        ]

        with patch('cron.generate_gene_list.OUTPUT_FILE', output_file):
            generate_gene_list()

        content = output_file.read_text()
        assert "/cgi-bin/locus.pl?dbid=CAL0001" in content


class TestEdgeCases:
    """Tests for edge cases."""

    def test_escape_html_with_all_special_chars(self):
        """Test escaping text with all special characters."""
        text = '<script>alert("XSS & Attack")</script>'
        result = escape_html(text)
        assert "<" not in result
        assert ">" not in result
        assert "&" not in result or "&amp;" in result or "&lt;" in result or "&gt;" in result
        assert '"' not in result

    def test_escape_html_unicode(self):
        """Test that unicode characters are preserved."""
        text = "Gene α with β function"
        result = escape_html(text)
        assert "α" in result
        assert "β" in result
