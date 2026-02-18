#!/usr/bin/env python3
"""
Unit tests for scripts/ftp_dump/gp2protein.py

Tests the gp2protein mapping file generation functionality.
"""

import pytest
import hashlib
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from ftp_dump.gp2protein import (
    get_feature_qualifiers,
    get_dbxref_mappings,
    get_annotated_features,
    compute_checksum,
)


class TestGetFeatureQualifiers:
    """Tests for get_feature_qualifiers function."""

    def test_basic_qualifiers(self, mock_db_session):
        """Test getting basic feature qualifiers."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("ORF19.1", "Verified"),
            ("ORF19.2", "Dubious"),
        ]

        result = get_feature_qualifiers(mock_db_session)

        assert result["ORF19.1"] == "Verified"
        assert result["ORF19.2"] == "Dubious"

    def test_empty_qualifiers(self, mock_db_session):
        """Test with no qualifiers."""
        mock_db_session.execute.return_value.fetchall.return_value = []

        result = get_feature_qualifiers(mock_db_session)

        assert result == {}

    def test_case_preservation(self, mock_db_session):
        """Test that feature names are uppercase."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("ORF19.1", "Verified"),  # Already uppercase from SQL
        ]

        result = get_feature_qualifiers(mock_db_session)

        assert "ORF19.1" in result


class TestGetDbxrefMappings:
    """Tests for get_dbxref_mappings function."""

    def test_uniprot_mapping(self, mock_db_session):
        """Test getting UniProt mappings."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("P12345", "UniProt/Swiss-Prot ID", "CAL0001"),
            ("Q67890", "UniProt/TrEMBL ID", "CAL0002"),
        ]

        uniprot_map, refseq_map = get_dbxref_mappings(mock_db_session)

        assert uniprot_map["CAL0001"] == "P12345"
        assert uniprot_map["CAL0002"] == "Q67890"
        assert len(refseq_map) == 0

    def test_refseq_mapping(self, mock_db_session):
        """Test getting RefSeq mappings with version stripping."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("NP_001234.1", "RefSeq protein version ID", "CAL0001"),
            ("NP_005678.2", "RefSeq protein version ID", "CAL0002"),
        ]

        uniprot_map, refseq_map = get_dbxref_mappings(mock_db_session)

        # Version numbers should be stripped
        assert refseq_map["CAL0001"] == "NP_001234"
        assert refseq_map["CAL0002"] == "NP_005678"
        assert len(uniprot_map) == 0

    def test_mixed_mappings(self, mock_db_session):
        """Test getting both UniProt and RefSeq mappings."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("P12345", "UniProt/Swiss-Prot ID", "CAL0001"),
            ("NP_001234.1", "RefSeq protein version ID", "CAL0002"),
        ]

        uniprot_map, refseq_map = get_dbxref_mappings(mock_db_session)

        assert len(uniprot_map) == 1
        assert len(refseq_map) == 1

    def test_empty_mappings(self, mock_db_session):
        """Test with no mappings."""
        mock_db_session.execute.return_value.fetchall.return_value = []

        uniprot_map, refseq_map = get_dbxref_mappings(mock_db_session)

        assert uniprot_map == {}
        assert refseq_map == {}


class TestGetAnnotatedFeatures:
    """Tests for get_annotated_features function."""

    def test_get_annotated_features(self, mock_db_session):
        """Test getting annotated features."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("orf19.1", "CAL0001"),
            ("orf19.2", "CAL0002"),
        ]
        qualifiers = {}

        result = get_annotated_features(mock_db_session, qualifiers)

        assert "CAL0001" in result
        assert "CAL0002" in result

    def test_skip_deleted_features(self, mock_db_session):
        """Test that deleted features are skipped."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("ORF19.1", "CAL0001"),
            ("ORF19.2", "CAL0002"),
        ]
        qualifiers = {
            "ORF19.1": "Verified",
            "ORF19.2": "Deleted",
        }

        result = get_annotated_features(mock_db_session, qualifiers)

        assert "CAL0001" in result
        assert "CAL0002" not in result

    def test_skip_merged_features(self, mock_db_session):
        """Test that merged features are skipped."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("ORF19.1", "CAL0001"),
        ]
        qualifiers = {"ORF19.1": "Merged"}

        result = get_annotated_features(mock_db_session, qualifiers)

        assert "CAL0001" not in result

    def test_skip_dubious_features(self, mock_db_session):
        """Test that dubious features are skipped."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("ORF19.1", "CAL0001"),
        ]
        qualifiers = {"ORF19.1": "Dubious ORF"}

        result = get_annotated_features(mock_db_session, qualifiers)

        assert "CAL0001" not in result

    def test_skip_none_dbxref(self, mock_db_session):
        """Test that features without dbxref_id are skipped."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("orf19.1", None),
            ("orf19.2", "CAL0002"),
        ]
        qualifiers = {}

        result = get_annotated_features(mock_db_session, qualifiers)

        assert "CAL0002" in result
        assert len(result) == 1

    def test_empty_features(self, mock_db_session):
        """Test with no annotated features."""
        mock_db_session.execute.return_value.fetchall.return_value = []
        qualifiers = {}

        result = get_annotated_features(mock_db_session, qualifiers)

        assert result == set()


class TestComputeChecksum:
    """Tests for compute_checksum function."""

    def test_basic_checksum(self, temp_dir):
        """Test computing checksum of file."""
        file_path = temp_dir / "test.txt"
        file_path.write_text("line1\tdata1\nline2\tdata2\n")

        result = compute_checksum(file_path)

        # Should be a valid MD5 hash
        assert len(result) == 32
        assert all(c in '0123456789abcdef' for c in result)

    def test_checksum_ignores_non_data_lines(self, temp_dir):
        """Test that lines without tabs are ignored."""
        file_path = temp_dir / "test.txt"
        file_path.write_text("!Header line\n!Comment\nline1\tdata1\n")

        result = compute_checksum(file_path)

        # Compute expected checksum (only "line1\tdata1\n")
        expected = hashlib.md5("line1\tdata1\n".encode()).hexdigest()
        assert result == expected

    def test_checksum_sorted(self, temp_dir):
        """Test that lines are sorted before checksum."""
        file_path1 = temp_dir / "test1.txt"
        file_path1.write_text("b\t2\na\t1\n")

        file_path2 = temp_dir / "test2.txt"
        file_path2.write_text("a\t1\nb\t2\n")

        result1 = compute_checksum(file_path1)
        result2 = compute_checksum(file_path2)

        # Same content in different order should produce same checksum
        assert result1 == result2

    def test_checksum_empty_file(self, temp_dir):
        """Test checksum of empty file."""
        file_path = temp_dir / "test.txt"
        file_path.write_text("")

        result = compute_checksum(file_path)

        # MD5 of empty string
        assert result == hashlib.md5(b"").hexdigest()

    def test_checksum_only_headers(self, temp_dir):
        """Test checksum of file with only header lines."""
        file_path = temp_dir / "test.txt"
        file_path.write_text("!Header1\n!Header2\n")

        result = compute_checksum(file_path)

        # No data lines, so empty content
        assert result == hashlib.md5(b"").hexdigest()


class TestMainFunction:
    """Tests for the main function."""

    @patch('ftp_dump.gp2protein.SessionLocal')
    @patch('ftp_dump.gp2protein.DATA_DIR')
    @patch('ftp_dump.gp2protein.FTP_DIR')
    def test_main_test_mode(self, mock_ftp_dir, mock_data_dir, mock_session_local, temp_dir):
        """Test main function in test mode."""
        from ftp_dump.gp2protein import main

        mock_data_dir.__truediv__ = lambda self, x: temp_dir / x
        mock_data_dir.mkdir = MagicMock()
        mock_ftp_dir.__truediv__ = lambda self, x: temp_dir / x

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

        # Mock database queries
        mock_session.execute.return_value.fetchall.return_value = []

        with patch.object(sys, 'argv', ['prog', '--test']):
            with patch('ftp_dump.gp2protein.DATA_DIR', temp_dir):
                result = main()

        assert result == 0

    @patch('ftp_dump.gp2protein.SessionLocal')
    def test_main_handles_exception(self, mock_session_local, temp_dir):
        """Test main handles exceptions."""
        from ftp_dump.gp2protein import main

        mock_session_local.side_effect = Exception("Database error")

        with patch.object(sys, 'argv', ['prog', '--test']):
            with patch('ftp_dump.gp2protein.DATA_DIR', temp_dir):
                with patch('ftp_dump.gp2protein.FTP_DIR', temp_dir):
                    result = main()

        assert result == 1


class TestEdgeCases:
    """Tests for edge cases."""

    def test_refseq_without_version(self, mock_db_session):
        """Test RefSeq ID without version number."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("NP_001234", "RefSeq protein version ID", "CAL0001"),
        ]

        uniprot_map, refseq_map = get_dbxref_mappings(mock_db_session)

        # Should work even without version
        assert refseq_map["CAL0001"] == "NP_001234"

    def test_qualifier_case_insensitive(self, mock_db_session):
        """Test that qualifier matching is case-insensitive."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("ORF19.1", "CAL0001"),
        ]
        qualifiers = {"ORF19.1": "DELETED"}  # Uppercase

        result = get_annotated_features(mock_db_session, qualifiers)

        assert "CAL0001" not in result

    def test_feature_name_none(self, mock_db_session):
        """Test handling of None feature names."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            (None, "CAL0001"),
        ]
        qualifiers = {}

        result = get_annotated_features(mock_db_session, qualifiers)

        assert "CAL0001" in result
