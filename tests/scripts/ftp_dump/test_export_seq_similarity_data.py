#!/usr/bin/env python3
"""
Unit tests for scripts/ftp_dump/export_seq_similarity_data.py

Tests the sequence similarity data export functionality.
"""

import gzip
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from ftp_dump.export_seq_similarity_data import (
    convert_score,
    copy_file_to_archive,
    copy_domain_file_to_ftp,
    create_pdb_homolog_file,
    create_file_from_homolog_table,
    BEST_HITS_METHOD,
    UNIPROT_METHOD,
    UNIPROT_SOURCE,
)


class TestConvertScore:
    """Tests for convert_score function."""

    def test_none_score(self):
        """Test with None score."""
        result = convert_score(None)
        assert result == ""

    def test_regular_score(self):
        """Test with regular decimal score."""
        result = convert_score(0.5)
        assert result == "0.5000"

    def test_small_score_scientific(self):
        """Test small score uses scientific notation."""
        result = convert_score(0.00001)
        assert "e" in result.lower()
        assert result == "1.00e-05"

    def test_boundary_score(self):
        """Test score at boundary (0.0001)."""
        result = convert_score(0.0001)
        assert result == "0.0001"

    def test_just_below_boundary(self):
        """Test score just below boundary."""
        result = convert_score(0.00009)
        assert "e" in result.lower()

    def test_very_small_score(self):
        """Test very small score."""
        result = convert_score(1e-100)
        assert "e" in result.lower()

    def test_zero_score(self):
        """Test zero score."""
        result = convert_score(0.0)
        # Zero is < 0.0001 so gets scientific notation
        assert "e" in result.lower() or result == "0.0000"

    def test_integer_score(self):
        """Test integer score."""
        result = convert_score(1.0)
        assert result == "1.0000"

    def test_large_score(self):
        """Test large score."""
        result = convert_score(1234.5678)
        assert result == "1234.5678"


class TestCopyFileToArchive:
    """Tests for copy_file_to_archive function."""

    def test_archive_with_extension(self, temp_dir):
        """Test archiving file with extension."""
        # Create source file
        source_file = temp_dir / "test.tab"
        source_file.write_text("data content")

        copy_file_to_archive(temp_dir, "test.tab")

        # Check archive directory exists
        archive_dir = temp_dir / "archive"
        assert archive_dir.exists()

        # Check archived file exists with date stamp
        archived_files = list(archive_dir.glob("test.*.tab"))
        assert len(archived_files) == 1

    def test_archive_without_extension(self, temp_dir):
        """Test archiving file without extension."""
        source_file = temp_dir / "testfile"
        source_file.write_text("data")

        copy_file_to_archive(temp_dir, "testfile")

        archive_dir = temp_dir / "archive"
        archived_files = list(archive_dir.glob("testfile.*"))
        assert len(archived_files) == 1

    def test_archive_nonexistent_file(self, temp_dir, caplog):
        """Test archiving nonexistent file."""
        copy_file_to_archive(temp_dir, "nonexistent.tab")

        assert "File not found" in caplog.text

    def test_archive_creates_directory(self, temp_dir):
        """Test that archive directory is created."""
        source_file = temp_dir / "data.txt"
        source_file.write_text("content")

        archive_dir = temp_dir / "archive"
        assert not archive_dir.exists()

        copy_file_to_archive(temp_dir, "data.txt")

        assert archive_dir.exists()


class TestCopyDomainFileToFtp:
    """Tests for copy_domain_file_to_ftp function."""

    def test_successful_copy(self, temp_dir):
        """Test successful domain file copy."""
        # Setup source directory
        domain_dir = temp_dir / "domain"
        domain_dir.mkdir()
        domain_file = domain_dir / "domains.tab"
        domain_file.write_text("domain\tdata\n")

        ftp_dir = temp_dir / "ftp"
        ftp_dir.mkdir()

        result = copy_domain_file_to_ftp(temp_dir, ftp_dir)

        assert result == 0

        # Check file was copied
        ftp_domain_file = ftp_dir / "sequence_similarity" / "domains" / "domains.tab"
        assert ftp_domain_file.exists()
        assert ftp_domain_file.read_text() == "domain\tdata\n"

    def test_missing_source_file(self, temp_dir, caplog):
        """Test with missing source file."""
        ftp_dir = temp_dir / "ftp"
        ftp_dir.mkdir()

        result = copy_domain_file_to_ftp(temp_dir, ftp_dir)

        assert result == 1
        assert "Domain file not found" in caplog.text

    def test_creates_ftp_directory_structure(self, temp_dir):
        """Test FTP directory structure is created."""
        domain_dir = temp_dir / "domain"
        domain_dir.mkdir()
        (domain_dir / "domains.tab").write_text("data")

        ftp_dir = temp_dir / "ftp"

        result = copy_domain_file_to_ftp(temp_dir, ftp_dir)

        assert result == 0
        assert (ftp_dir / "sequence_similarity" / "domains").exists()


class TestCreatePdbHomologFile:
    """Tests for create_pdb_homolog_file function."""

    def test_creates_file(self, temp_dir, mock_db_session):
        """Test PDB homolog file creation."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("SEQ1", 1, 100, 1, 95, 95.5, 0.001, "PDB1", 9606, "Homo sapiens"),
            ("SEQ2", 10, 200, 5, 190, 90.0, 1e-50, "PDB2", 4932, "S. cerevisiae"),
        ]

        result = create_pdb_homolog_file(mock_db_session, temp_dir)

        assert result == 0

        # Check gzipped file was created
        gz_file = temp_dir / "sequence_similarity" / "pdb_homologs" / "pdb_homologs.tab.gz"
        assert gz_file.exists()

        # Verify content
        with gzip.open(gz_file, "rt") as f:
            lines = f.readlines()
            assert len(lines) == 2
            assert "SEQ1" in lines[0]
            assert "PDB1" in lines[0]

    def test_empty_results(self, temp_dir, mock_db_session):
        """Test with no PDB homologs."""
        mock_db_session.execute.return_value.fetchall.return_value = []

        result = create_pdb_homolog_file(mock_db_session, temp_dir)

        assert result == 0

        gz_file = temp_dir / "sequence_similarity" / "pdb_homologs" / "pdb_homologs.tab.gz"
        assert gz_file.exists()

    def test_handles_none_values(self, temp_dir, mock_db_session):
        """Test handling of None values in results."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("SEQ1", None, None, None, None, None, None, "PDB1", None, None),
        ]

        result = create_pdb_homolog_file(mock_db_session, temp_dir)

        assert result == 0


class TestCreateFileFromHomologTable:
    """Tests for create_file_from_homolog_table function."""

    def test_besthits_source(self, temp_dir, mock_db_session):
        """Test best hits file creation."""
        mock_db_session.execute.return_value = iter([
            ("FEAT1", 1, 100, 1, 100, 99.0, 0.5, "HIT1", 9606, "Homo sapiens"),
        ])

        result = create_file_from_homolog_table(mock_db_session, "besthits", temp_dir)

        assert result == 0

        # Best hits are not compressed
        out_file = temp_dir / "sequence_similarity" / "best_hits" / "best_hits.tab"
        assert out_file.exists()

    def test_uniprot_source(self, temp_dir, mock_db_session):
        """Test UniProt PSI-BLAST file creation."""
        mock_db_session.execute.return_value = iter([
            ("FEAT1", 1, 100, 1, 100, 99.0, 1e-50, "P12345", 9606, "Homo sapiens"),
        ])

        result = create_file_from_homolog_table(mock_db_session, "uniprot", temp_dir)

        assert result == 0

        # UniProt files are compressed
        gz_file = temp_dir / "sequence_similarity" / "psi_blast" / "psi_blast.tab.gz"
        assert gz_file.exists()

    def test_unknown_source(self, temp_dir, mock_db_session, caplog):
        """Test with unknown source type."""
        result = create_file_from_homolog_table(mock_db_session, "unknown", temp_dir)

        assert result == 1
        assert "Unknown source" in caplog.text

    def test_empty_results(self, temp_dir, mock_db_session):
        """Test with no results."""
        mock_db_session.execute.return_value = iter([])

        result = create_file_from_homolog_table(mock_db_session, "besthits", temp_dir)

        assert result == 0

    def test_score_conversion_in_output(self, temp_dir, mock_db_session):
        """Test that scores are properly converted in output."""
        mock_db_session.execute.return_value = iter([
            ("FEAT1", 1, 100, 1, 100, 99.0, 0.00001, "HIT1", 9606, "Human"),
        ])

        result = create_file_from_homolog_table(mock_db_session, "besthits", temp_dir)

        assert result == 0

        out_file = temp_dir / "sequence_similarity" / "best_hits" / "best_hits.tab"
        content = out_file.read_text()
        # Small score should be in scientific notation
        assert "e-" in content.lower()


class TestConstants:
    """Tests for module constants."""

    def test_best_hits_method(self):
        """Test BEST_HITS_METHOD constant."""
        assert BEST_HITS_METHOD == "BLASTP"

    def test_uniprot_method(self):
        """Test UNIPROT_METHOD constant."""
        assert UNIPROT_METHOD == "PSI-BLAST"

    def test_uniprot_source(self):
        """Test UNIPROT_SOURCE constant."""
        assert UNIPROT_SOURCE == "UniProt"


class TestMainFunction:
    """Tests for the main function."""

    @patch('ftp_dump.export_seq_similarity_data.copy_domain_file_to_ftp')
    def test_main_domain(self, mock_copy, temp_dir):
        """Test main with domain source."""
        from ftp_dump.export_seq_similarity_data import main

        mock_copy.return_value = 0

        with patch.object(sys, 'argv', ['prog', 'domain']):
            result = main()

        assert result == 0
        mock_copy.assert_called_once()

    @patch('ftp_dump.export_seq_similarity_data.SessionLocal')
    @patch('ftp_dump.export_seq_similarity_data.create_pdb_homolog_file')
    def test_main_pdb(self, mock_create_pdb, mock_session_local):
        """Test main with pdb source."""
        from ftp_dump.export_seq_similarity_data import main

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)
        mock_create_pdb.return_value = 0

        with patch.object(sys, 'argv', ['prog', 'pdb']):
            result = main()

        assert result == 0
        mock_create_pdb.assert_called_once()

    @patch('ftp_dump.export_seq_similarity_data.SessionLocal')
    @patch('ftp_dump.export_seq_similarity_data.create_file_from_homolog_table')
    def test_main_besthits(self, mock_create_file, mock_session_local):
        """Test main with besthits source."""
        from ftp_dump.export_seq_similarity_data import main

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)
        mock_create_file.return_value = 0

        with patch.object(sys, 'argv', ['prog', 'besthits']):
            result = main()

        assert result == 0
        mock_create_file.assert_called_once()

    @patch('ftp_dump.export_seq_similarity_data.SessionLocal')
    def test_main_handles_exception(self, mock_session_local, caplog):
        """Test main handles database exceptions."""
        from ftp_dump.export_seq_similarity_data import main

        mock_session_local.side_effect = Exception("Database error")

        with patch.object(sys, 'argv', ['prog', 'pdb']):
            result = main()

        assert result == 1
        assert "Error" in caplog.text

    def test_main_invalid_source(self):
        """Test main with invalid source."""
        from ftp_dump.export_seq_similarity_data import main

        with patch.object(sys, 'argv', ['prog', 'invalid']):
            with pytest.raises(SystemExit):
                main()


class TestEdgeCases:
    """Tests for edge cases."""

    def test_convert_score_negative(self):
        """Test convert_score with negative value."""
        result = convert_score(-0.5)
        # Negative values may get scientific notation
        assert "-" in result  # Should be negative
        assert "5" in result  # Should contain 5

    def test_copy_file_preserves_content(self, temp_dir):
        """Test that copy preserves file content exactly."""
        original_content = "Line 1\nLine 2\nSpecial chars: \t\n"
        source_file = temp_dir / "data.tab"
        source_file.write_text(original_content)

        copy_file_to_archive(temp_dir, "data.tab")

        archive_dir = temp_dir / "archive"
        archived_files = list(archive_dir.glob("data.*.tab"))
        assert len(archived_files) == 1
        assert archived_files[0].read_text() == original_content

    def test_pdb_file_tab_delimited(self, temp_dir, mock_db_session):
        """Test PDB output is tab-delimited."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("SEQ1", 1, 100, 1, 95, 95.5, 0.5, "PDB1", 9606, "Human"),
        ]

        create_pdb_homolog_file(mock_db_session, temp_dir)

        gz_file = temp_dir / "sequence_similarity" / "pdb_homologs" / "pdb_homologs.tab.gz"
        with gzip.open(gz_file, "rt") as f:
            line = f.readline()
            fields = line.strip().split("\t")
            assert len(fields) == 10  # All 10 fields

    def test_homolog_file_handles_special_characters(self, temp_dir, mock_db_session):
        """Test handling of special characters in identifiers."""
        mock_db_session.execute.return_value = iter([
            ("FEAT:1|test", 1, 100, 1, 100, 99.0, 0.5, "HIT|1:2", 9606, "Homo sapiens (human)"),
        ])

        result = create_file_from_homolog_table(mock_db_session, "besthits", temp_dir)

        assert result == 0
        out_file = temp_dir / "sequence_similarity" / "best_hits" / "best_hits.tab"
        content = out_file.read_text()
        assert "FEAT:1|test" in content
