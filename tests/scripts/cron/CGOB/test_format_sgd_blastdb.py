#!/usr/bin/env python3
"""
Unit tests for scripts/cron/CGOB/format_sgd_blastdb.py

Tests the SGD BLAST database formatting functionality.
"""

import gzip
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent.parent / "scripts"))

from cron.CGOB.format_sgd_blastdb import (
    decompress_if_needed,
    create_blast_database,
    SEQUENCE_SETS,
    SGD_STRAIN,
)


class TestDecompressIfNeeded:
    """Tests for decompress_if_needed function."""

    def test_non_gzipped_file(self, temp_dir):
        """Test with non-gzipped file."""
        fasta_file = temp_dir / "test.fasta"
        fasta_file.write_text(">seq1\nATGC\n")

        result = decompress_if_needed(fasta_file)

        assert result == fasta_file
        assert result.exists()

    def test_gzipped_file_decompression(self, temp_dir):
        """Test decompression of gzipped file."""
        # Create gzipped file
        gz_file = temp_dir / "test.fasta.gz"
        content = b">seq1\nATGC\n"
        with gzip.open(gz_file, "wb") as f:
            f.write(content)

        result = decompress_if_needed(gz_file)

        assert result == temp_dir / "test.fasta"
        assert result.exists()
        assert result.read_text() == ">seq1\nATGC\n"

    def test_already_decompressed(self, temp_dir):
        """Test when uncompressed file already exists."""
        # Create uncompressed file
        fasta_file = temp_dir / "test.fasta"
        fasta_file.write_text(">seq1\nATGC\n")

        # Request with .gz suffix (but gzipped doesn't exist)
        gz_path = temp_dir / "test.fasta.gz"

        result = decompress_if_needed(gz_path)

        assert result == fasta_file

    def test_file_not_found(self, temp_dir):
        """Test when neither gzipped nor uncompressed exists."""
        gz_path = temp_dir / "nonexistent.fasta.gz"

        with pytest.raises(FileNotFoundError):
            decompress_if_needed(gz_path)


class TestCreateBlastDatabase:
    """Tests for create_blast_database function."""

    @patch('cron.CGOB.format_sgd_blastdb.subprocess.run')
    def test_successful_creation(self, mock_run, temp_dir):
        """Test successful BLAST database creation."""
        mock_run.return_value = MagicMock(returncode=0)

        fasta_file = temp_dir / "test.fasta"
        fasta_file.write_text(">seq1\nATGC\n")
        db_name = temp_dir / "test_db"

        result = create_blast_database(fasta_file, db_name, "prot")

        assert "Created BLAST database" in result
        mock_run.assert_called_once()

        # Verify command line arguments
        call_args = mock_run.call_args[0][0]
        assert "-in" in call_args
        assert "-dbtype" in call_args
        assert "prot" in call_args

    @patch('cron.CGOB.format_sgd_blastdb.subprocess.run')
    def test_failed_creation(self, mock_run, temp_dir):
        """Test failed BLAST database creation."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stderr="makeblastdb error"
        )

        fasta_file = temp_dir / "test.fasta"
        fasta_file.write_text(">seq1\nATGC\n")
        db_name = temp_dir / "test_db"

        result = create_blast_database(fasta_file, db_name, "prot")

        assert "ERROR" in result

    @patch('cron.CGOB.format_sgd_blastdb.subprocess.run')
    def test_removes_existing_database(self, mock_run, temp_dir):
        """Test that existing database files are removed."""
        mock_run.return_value = MagicMock(returncode=0)

        fasta_file = temp_dir / "test.fasta"
        fasta_file.write_text(">seq1\nATGC\n")
        db_name = temp_dir / "test_db"

        # Create some fake existing database files
        (temp_dir / "test_db.phr").write_text("old")
        (temp_dir / "test_db.pin").write_text("old")

        result = create_blast_database(fasta_file, db_name, "prot")

        # Old files should be removed
        assert not (temp_dir / "test_db.phr").exists()
        assert not (temp_dir / "test_db.pin").exists()

    @patch('cron.CGOB.format_sgd_blastdb.subprocess.run')
    def test_nucleotide_database(self, mock_run, temp_dir):
        """Test creating nucleotide database."""
        mock_run.return_value = MagicMock(returncode=0)

        fasta_file = temp_dir / "test.fasta"
        fasta_file.write_text(">seq1\nATGC\n")
        db_name = temp_dir / "test_db"

        result = create_blast_database(fasta_file, db_name, "nucl")

        call_args = mock_run.call_args[0][0]
        assert "nucl" in call_args

    def test_exception_handling(self, temp_dir):
        """Test exception handling."""
        fasta_file = temp_dir / "nonexistent.fasta"
        db_name = temp_dir / "test_db"

        with patch('cron.CGOB.format_sgd_blastdb.subprocess.run', side_effect=Exception("Test error")):
            result = create_blast_database(fasta_file, db_name, "prot")

        assert "ERROR" in result


class TestConstants:
    """Tests for module constants."""

    def test_sgd_strain(self):
        """Test SGD strain constant."""
        assert SGD_STRAIN == "S_cerevisiae"

    def test_sequence_sets_defined(self):
        """Test that sequence sets are defined."""
        assert len(SEQUENCE_SETS) > 0

    def test_sequence_sets_structure(self):
        """Test sequence sets have required fields."""
        for seq_set in SEQUENCE_SETS:
            assert "name" in seq_set
            assert "type" in seq_set
            assert "suffix" in seq_set
            assert seq_set["type"] in ["prot", "nucl"]

    def test_sequence_sets_protein(self):
        """Test protein sequence set exists."""
        protein_sets = [s for s in SEQUENCE_SETS if s["type"] == "prot"]
        assert len(protein_sets) > 0

    def test_sequence_sets_nucleotide(self):
        """Test nucleotide sequence sets exist."""
        nucl_sets = [s for s in SEQUENCE_SETS if s["type"] == "nucl"]
        assert len(nucl_sets) > 0


class TestMainFunction:
    """Tests for the main function."""

    def test_main_runs(self):
        """Test that main function can be imported and called structure is correct."""
        from cron.CGOB.format_sgd_blastdb import main
        import inspect

        # Verify main is callable
        assert callable(main)

        # Verify it takes no required arguments (uses argparse)
        sig = inspect.signature(main)
        assert len([p for p in sig.parameters.values()
                   if p.default == inspect.Parameter.empty]) == 0


class TestEdgeCases:
    """Tests for edge cases."""

    def test_decompress_large_file(self, temp_dir):
        """Test decompression of larger file."""
        # Create a larger gzipped file
        gz_file = temp_dir / "large.fasta.gz"
        content = (">seq1\n" + "ATGC" * 1000 + "\n") * 10
        with gzip.open(gz_file, "wt") as f:
            f.write(content)

        result = decompress_if_needed(gz_file)

        assert result.exists()
        assert len(result.read_text()) == len(content)

    @patch('cron.CGOB.format_sgd_blastdb.subprocess.run')
    def test_create_db_with_special_path(self, mock_run, temp_dir):
        """Test database creation with path containing spaces."""
        mock_run.return_value = MagicMock(returncode=0)

        special_dir = temp_dir / "path with spaces"
        special_dir.mkdir()

        fasta_file = special_dir / "test.fasta"
        fasta_file.write_text(">seq1\nATGC\n")
        db_name = special_dir / "test_db"

        result = create_blast_database(fasta_file, db_name, "prot")

        assert "Created BLAST database" in result
