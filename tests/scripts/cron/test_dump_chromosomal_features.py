#!/usr/bin/env python3
"""
Unit tests for scripts/cron/dump_chromosomal_features.py

Tests the chromosomal feature data dumping functionality.
"""

import gzip
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from cron.dump_chromosomal_features import (
    get_strain_config,
    get_all_features,
    get_feature_aliases,
    get_feature_qualifier,
    get_secondary_dbxref,
    get_orthologs,
    get_reserved_gene_info,
    archive_old_file,
)


class TestGetStrainConfig:
    """Tests for get_strain_config function."""

    def test_strain_found(self, mock_db_session):
        """Test when strain is found."""
        # First call returns organism info
        mock_db_session.execute.return_value.fetchone.side_effect = [
            (1, "C_albicans_SC5314", "Candida albicans SC5314"),
            ("Assembly22",),  # seq_source
        ]

        result = get_strain_config(mock_db_session, "C_albicans_SC5314")

        assert result is not None
        assert result["organism_no"] == 1
        assert result["organism_abbrev"] == "C_albicans_SC5314"
        assert result["seq_source"] == "Assembly22"

    def test_strain_not_found(self, mock_db_session):
        """Test when strain is not found."""
        mock_db_session.execute.return_value.fetchone.return_value = None

        result = get_strain_config(mock_db_session, "NonexistentStrain")

        assert result is None


class TestGetAllFeatures:
    """Tests for get_all_features function."""

    def test_returns_features(self, mock_db_session):
        """Test returning features."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "orf19.1", "ACT1", "ORF", "CGDID:CAL0001", "Actin",
             datetime(2020, 1, 1), 100, 200, "+", "ChrA"),
            (2, "orf19.2", None, "ORF", "CGDID:CAL0002", None,
             datetime(2020, 1, 2), 300, 400, "-", "ChrA"),
        ]

        result = get_all_features(mock_db_session, "C_albicans_SC5314", "Assembly22")

        assert len(result) == 2
        assert result[0]["feature_name"] == "orf19.1"
        assert result[0]["gene_name"] == "ACT1"
        assert result[1]["feature_name"] == "orf19.2"

    def test_empty_features(self, mock_db_session):
        """Test with no features."""
        mock_db_session.execute.return_value.fetchall.return_value = []

        result = get_all_features(mock_db_session, "C_albicans_SC5314", "Assembly22")

        assert result == []


class TestGetFeatureAliases:
    """Tests for get_feature_aliases function."""

    def test_with_aliases(self, mock_db_session):
        """Test getting aliases."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("ACT",),
            ("ACTIN",),
        ]

        result = get_feature_aliases(mock_db_session, 1)

        assert len(result) == 2
        assert "ACT" in result
        assert "ACTIN" in result

    def test_no_aliases(self, mock_db_session):
        """Test with no aliases."""
        mock_db_session.execute.return_value.fetchall.return_value = []

        result = get_feature_aliases(mock_db_session, 1)

        assert result == []


class TestGetFeatureQualifier:
    """Tests for get_feature_qualifier function."""

    def test_qualifier_found(self, mock_db_session):
        """Test when qualifier is found."""
        mock_db_session.execute.return_value.fetchone.return_value = ("Verified",)

        result = get_feature_qualifier(mock_db_session, 1)

        assert result == "Verified"

    def test_no_qualifier(self, mock_db_session):
        """Test with no qualifier."""
        mock_db_session.execute.return_value.fetchone.return_value = None

        result = get_feature_qualifier(mock_db_session, 1)

        assert result is None


class TestGetSecondaryDbxref:
    """Tests for get_secondary_dbxref function."""

    def test_dbxref_found(self, mock_db_session):
        """Test when secondary dbxref is found."""
        mock_db_session.execute.return_value.fetchone.return_value = ("orf19.1234",)

        result = get_secondary_dbxref(mock_db_session, 1)

        assert result == "orf19.1234"

    def test_no_dbxref(self, mock_db_session):
        """Test with no secondary dbxref."""
        mock_db_session.execute.return_value.fetchone.return_value = None

        result = get_secondary_dbxref(mock_db_session, 1)

        assert result is None


class TestGetOrthologs:
    """Tests for get_orthologs function."""

    def test_with_orthologs(self, mock_db_session):
        """Test getting orthologs."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("ACT1",),
            ("ACT2",),
        ]

        result = get_orthologs(mock_db_session, 1)

        assert len(result) == 2
        assert "ACT1" in result
        assert "ACT2" in result

    def test_no_orthologs(self, mock_db_session):
        """Test with no orthologs."""
        mock_db_session.execute.return_value.fetchall.return_value = []

        result = get_orthologs(mock_db_session, 1)

        assert result == []


class TestGetReservedGeneInfo:
    """Tests for get_reserved_gene_info function."""

    def test_with_reservation(self, mock_db_session):
        """Test with reservation info."""
        mock_db_session.execute.return_value.fetchone.side_effect = [
            ("2020-01-15",),
            ("Y",),
        ]

        date, is_standard = get_reserved_gene_info(mock_db_session, 1)

        assert date == "2020-01-15"
        assert is_standard == "Y"

    def test_no_reservation(self, mock_db_session):
        """Test without reservation info."""
        mock_db_session.execute.return_value.fetchone.return_value = None

        date, is_standard = get_reserved_gene_info(mock_db_session, 1)

        assert date is None
        assert is_standard is None


class TestArchiveOldFile:
    """Tests for archive_old_file function."""

    def test_archives_existing_file(self, temp_dir):
        """Test archiving an existing file."""
        old_file = temp_dir / "test.tab"
        old_file.write_text("old content")

        archive_dir = temp_dir / "archive"

        archive_old_file(old_file, archive_dir)

        assert not old_file.exists()
        assert archive_dir.exists()

        # Check that gzipped file exists
        gz_files = list(archive_dir.glob("*.gz"))
        assert len(gz_files) == 1

    def test_nonexistent_file(self, temp_dir):
        """Test with nonexistent file."""
        old_file = temp_dir / "nonexistent.tab"
        archive_dir = temp_dir / "archive"

        # Should not raise error
        archive_old_file(old_file, archive_dir)

    def test_creates_archive_dir(self, temp_dir):
        """Test that archive directory is created."""
        old_file = temp_dir / "test.tab"
        old_file.write_text("content")

        archive_dir = temp_dir / "nested" / "archive"

        archive_old_file(old_file, archive_dir)

        assert archive_dir.exists()


class TestMainFunction:
    """Tests for the main function."""

    @patch('cron.dump_chromosomal_features.SessionLocal')
    @patch('cron.dump_chromosomal_features.HTML_ROOT_DIR')
    def test_main_success(self, mock_html_dir, mock_session_local, temp_dir):
        """Test main with successful execution."""
        from cron.dump_chromosomal_features import main

        mock_html_dir.__truediv__ = lambda self, x: temp_dir / x

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

        # Mock get_strain_config
        with patch('cron.dump_chromosomal_features.get_strain_config') as mock_config:
            mock_config.return_value = {
                "organism_no": 1,
                "organism_abbrev": "C_albicans_SC5314",
                "organism_name": "Candida albicans SC5314",
                "seq_source": "Assembly22",
            }

            with patch('cron.dump_chromosomal_features.write_chromosomal_features'):
                with patch.object(sys, 'argv', ['prog', 'C_albicans_SC5314', '--output-dir', str(temp_dir), '--no-archive']):
                    result = main()

        assert result == 0

    @patch('cron.dump_chromosomal_features.SessionLocal')
    def test_main_strain_not_found(self, mock_session_local):
        """Test main when strain not found."""
        from cron.dump_chromosomal_features import main

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

        with patch('cron.dump_chromosomal_features.get_strain_config') as mock_config:
            mock_config.return_value = None

            with patch.object(sys, 'argv', ['prog', 'NonexistentStrain']):
                result = main()

        assert result == 1

    @patch('cron.dump_chromosomal_features.SessionLocal')
    def test_main_no_seq_source(self, mock_session_local):
        """Test main when no seq_source found."""
        from cron.dump_chromosomal_features import main

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

        with patch('cron.dump_chromosomal_features.get_strain_config') as mock_config:
            mock_config.return_value = {
                "organism_no": 1,
                "organism_abbrev": "C_albicans_SC5314",
                "organism_name": "Candida albicans SC5314",
                "seq_source": None,
            }

            with patch.object(sys, 'argv', ['prog', 'C_albicans_SC5314']):
                result = main()

        assert result == 1


class TestEdgeCases:
    """Tests for edge cases."""

    def test_feature_with_all_none_values(self, mock_db_session):
        """Test handling feature with all None values."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, None, None, None, None, None, None, None, None, None, None),
        ]

        result = get_all_features(mock_db_session, "C_albicans_SC5314", "Assembly22")

        assert len(result) == 1
        assert result[0]["feature_name"] is None

    def test_archive_preserves_content(self, temp_dir):
        """Test that archived content matches original."""
        old_file = temp_dir / "test.tab"
        original_content = "Line 1\nLine 2\nSpecial: αβγ\n"
        old_file.write_text(original_content)

        archive_dir = temp_dir / "archive"

        archive_old_file(old_file, archive_dir)

        # Read back the archived content
        gz_file = list(archive_dir.glob("*.gz"))[0]
        with gzip.open(gz_file, "rt") as f:
            archived_content = f.read()

        assert archived_content == original_content
