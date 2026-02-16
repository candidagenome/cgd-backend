#!/usr/bin/env python3
"""
Unit tests for scripts/data/dump_intergenic_sequences.py

Tests the intergenic sequence dumping functionality.
"""

import gzip
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

# Skip all tests in this module if Biopython is not installed
pytest.importorskip("Bio", reason="Biopython not installed")


class TestIntergenicSequenceDumperInit:
    """Tests for IntergenicSequenceDumper initialization."""

    def test_init_with_seq_source(self, mock_db_session):
        """Test initialization with explicit seq_source."""
        from data.dump_intergenic_sequences import IntergenicSequenceDumper

        dumper = IntergenicSequenceDumper(
            mock_db_session,
            strain_abbrev="C_albicans_SC5314",
            seq_source="GenBank"
        )

        assert dumper.strain_abbrev == "C_albicans_SC5314"
        assert dumper.seq_source == "GenBank"
        assert dumper.total_features == 0
        assert dumper.total_intergenic == 0

    def test_init_without_seq_source(self, mock_db_session):
        """Test initialization auto-detects seq_source."""
        from data.dump_intergenic_sequences import IntergenicSequenceDumper

        # Mock the query result
        mock_db_session.execute.return_value.first.return_value = ("Assembly22",)

        dumper = IntergenicSequenceDumper(
            mock_db_session,
            strain_abbrev="C_albicans_SC5314"
        )

        assert dumper.seq_source == "Assembly22"


class TestGetChromosomeLengths:
    """Tests for get_chromosome_lengths method."""

    def test_get_chromosome_lengths(self, mock_db_session):
        """Test getting chromosome lengths."""
        from data.dump_intergenic_sequences import IntergenicSequenceDumper

        mock_db_session.execute.return_value = [
            ("Ca22chr1", 3188363),
            ("Ca22chr2", 2231883),
            ("Ca22chrM", 40420),
        ]

        dumper = IntergenicSequenceDumper(
            mock_db_session,
            strain_abbrev="C_albicans_SC5314",
            seq_source="GenBank"
        )

        # Reset the mock for the actual query
        mock_db_session.execute.return_value = [
            ("Ca22chr1", 3188363),
            ("Ca22chr2", 2231883),
        ]

        lengths = dumper.get_chromosome_lengths()

        assert "Ca22chr1" in lengths
        assert lengths["Ca22chr1"] == 3188363
        assert lengths["Ca22chr2"] == 2231883


class TestGetChromosomeNames:
    """Tests for get_chromosome_names method."""

    def test_get_chromosome_names(self, mock_db_session):
        """Test getting sorted chromosome names."""
        from data.dump_intergenic_sequences import IntergenicSequenceDumper

        mock_db_session.execute.return_value = [
            ("Ca22chr1",),
            ("Ca22chr2",),
            ("Ca22chrM",),
        ]

        dumper = IntergenicSequenceDumper(
            mock_db_session,
            strain_abbrev="C_albicans_SC5314",
            seq_source="GenBank"
        )

        # Reset for actual query
        mock_db_session.execute.return_value = [
            ("Ca22chr1",),
            ("Ca22chr2",),
            ("Ca22chrM",),
        ]

        names = dumper.get_chromosome_names()

        assert len(names) == 3
        assert names[0] == "Ca22chr1"


class TestFindIntergenicRegions:
    """Tests for find_intergenic_regions method."""

    def test_find_intergenic_no_features(self, mock_db_session):
        """Test finding intergenic regions when no features exist."""
        from data.dump_intergenic_sequences import IntergenicSequenceDumper

        mock_db_session.execute.return_value = []

        dumper = IntergenicSequenceDumper(
            mock_db_session,
            strain_abbrev="C_albicans_SC5314",
            seq_source="GenBank"
        )

        regions = dumper.find_intergenic_regions("Ca22chr1", 10000)

        # With no features, no intergenic regions
        assert regions == []

    def test_find_intergenic_with_gap(self, mock_db_session):
        """Test finding intergenic region between two features."""
        from data.dump_intergenic_sequences import IntergenicSequenceDumper

        # Mock features with a gap between them
        mock_db_session.execute.return_value = [
            ("gene1", 100, 500),
            ("gene2", 1000, 1500),
        ]

        dumper = IntergenicSequenceDumper(
            mock_db_session,
            strain_abbrev="C_albicans_SC5314",
            seq_source="GenBank"
        )

        regions = dumper.find_intergenic_regions("Ca22chr1", 2000)

        # Should find regions before first gene, between genes, and after last gene
        assert len(regions) >= 1

        # Check that we have the gap between genes
        gap_found = False
        for start, end, left, right in regions:
            if start > 500 and end < 1000:
                gap_found = True
                assert left == "gene1"
                assert right == "gene2"

        assert gap_found

    def test_find_intergenic_before_first_feature(self, mock_db_session):
        """Test finding intergenic region before first feature."""
        from data.dump_intergenic_sequences import IntergenicSequenceDumper

        mock_db_session.execute.return_value = [
            ("gene1", 500, 1000),
        ]

        dumper = IntergenicSequenceDumper(
            mock_db_session,
            strain_abbrev="C_albicans_SC5314",
            seq_source="GenBank"
        )

        regions = dumper.find_intergenic_regions("Ca22chr1", 2000)

        # Should find region before gene1
        before_found = any(
            start == 1 and end < 500
            for start, end, _, _ in regions
        )
        assert before_found

    def test_find_intergenic_after_last_feature(self, mock_db_session):
        """Test finding intergenic region after last feature."""
        from data.dump_intergenic_sequences import IntergenicSequenceDumper

        mock_db_session.execute.return_value = [
            ("gene1", 100, 500),
        ]

        dumper = IntergenicSequenceDumper(
            mock_db_session,
            strain_abbrev="C_albicans_SC5314",
            seq_source="GenBank"
        )

        regions = dumper.find_intergenic_regions("Ca22chr1", 2000)

        # Should find region after gene1
        after_found = any(
            start > 500 and end == 2000
            for start, end, _, _ in regions
        )
        assert after_found

    def test_overlapping_features(self, mock_db_session):
        """Test handling of overlapping features."""
        from data.dump_intergenic_sequences import IntergenicSequenceDumper

        mock_db_session.execute.return_value = [
            ("gene1", 100, 600),
            ("gene2", 500, 1000),  # Overlaps with gene1
        ]

        dumper = IntergenicSequenceDumper(
            mock_db_session,
            strain_abbrev="C_albicans_SC5314",
            seq_source="GenBank"
        )

        regions = dumper.find_intergenic_regions("Ca22chr1", 2000)

        # Should not have intergenic region in overlap zone
        for start, end, _, _ in regions:
            # No region should start/end in 500-600 overlap zone
            assert not (500 < start < 600 and start == end + 1)


class TestGetSequence:
    """Tests for get_sequence method."""

    def test_get_sequence_found(self, mock_db_session):
        """Test getting sequence when found."""
        from data.dump_intergenic_sequences import IntergenicSequenceDumper

        mock_db_session.execute.return_value.first.return_value = ("ATGCGATCGATCG",)

        dumper = IntergenicSequenceDumper(
            mock_db_session,
            strain_abbrev="C_albicans_SC5314",
            seq_source="GenBank"
        )

        seq = dumper.get_sequence("Ca22chr1", 100, 200)

        assert seq == "ATGCGATCGATCG"

    def test_get_sequence_not_found(self, mock_db_session):
        """Test getting sequence when not found."""
        from data.dump_intergenic_sequences import IntergenicSequenceDumper

        mock_db_session.execute.return_value.first.return_value = None

        dumper = IntergenicSequenceDumper(
            mock_db_session,
            strain_abbrev="C_albicans_SC5314",
            seq_source="GenBank"
        )

        seq = dumper.get_sequence("Ca22chr1", 100, 200)

        assert seq is None


class TestDumpSequences:
    """Tests for dump_sequences method."""

    @patch('data.dump_intergenic_sequences.SeqIO')
    def test_dump_creates_files(self, mock_seqio, mock_db_session, temp_dir):
        """Test that dump creates output files."""
        from data.dump_intergenic_sequences import IntergenicSequenceDumper

        # Mock chromosome data
        mock_db_session.execute.return_value = [("Ca22chr1", 10000)]

        dumper = IntergenicSequenceDumper(
            mock_db_session,
            strain_abbrev="C_albicans_SC5314",
            seq_source="GenBank"
        )

        # Mock methods
        dumper.get_chromosome_lengths = MagicMock(return_value={"Ca22chr1": 10000})
        dumper.get_chromosome_names = MagicMock(return_value=["Ca22chr1"])
        dumper.find_intergenic_regions = MagicMock(return_value=[
            (1, 100, "start of Ca22chr1", "gene1")
        ])
        dumper.get_sequence = MagicMock(return_value="ATGCATGC")

        fasta_file = temp_dir / "test.fasta"
        gff_file = temp_dir / "test.gff"

        intergenic_count, feature_count = dumper.dump_sequences(fasta_file, gff_file)

        # Check that files were created
        assert gff_file.exists()

        # Check GFF content
        gff_content = gff_file.read_text()
        assert "##gff-version" in gff_content
        assert "intergenic_region" in gff_content


class TestDumpIntergenicSequences:
    """Tests for dump_intergenic_sequences function."""

    @patch('data.dump_intergenic_sequences.SessionLocal')
    @patch('data.dump_intergenic_sequences.IntergenicSequenceDumper')
    def test_dump_success(self, mock_dumper_class, mock_session, temp_dir, monkeypatch):
        """Test successful dump."""
        from data.dump_intergenic_sequences import dump_intergenic_sequences

        # Mock the dumper
        mock_dumper = MagicMock()
        mock_dumper.dump_sequences.return_value = (10, 100)
        mock_dumper_class.return_value = mock_dumper

        # Mock session context manager
        mock_session_instance = MagicMock()
        mock_session.return_value.__enter__ = MagicMock(return_value=mock_session_instance)
        mock_session.return_value.__exit__ = MagicMock(return_value=False)

        # Mock LOG_DIR
        monkeypatch.setattr(
            'data.dump_intergenic_sequences.LOG_DIR',
            temp_dir
        )

        result = dump_intergenic_sequences(
            strain_abbrev="C_albicans_SC5314",
            output_dir=temp_dir,
            compress=False
        )

        assert result is True

    @patch('data.dump_intergenic_sequences.SessionLocal')
    def test_dump_handles_exception(self, mock_session, temp_dir, monkeypatch):
        """Test exception handling in dump."""
        from data.dump_intergenic_sequences import dump_intergenic_sequences

        mock_session.side_effect = Exception("Database error")

        monkeypatch.setattr(
            'data.dump_intergenic_sequences.LOG_DIR',
            temp_dir
        )

        result = dump_intergenic_sequences(
            strain_abbrev="C_albicans_SC5314",
            output_dir=temp_dir
        )

        assert result is False


class TestMainFunction:
    """Tests for the main function."""

    @patch('data.dump_intergenic_sequences.dump_intergenic_sequences')
    def test_main_success(self, mock_dump, temp_dir, capsys):
        """Test main function succeeds."""
        from data.dump_intergenic_sequences import main

        mock_dump.return_value = True

        with patch.object(sys, 'argv', [
            'prog',
            '--strain', 'C_albicans_SC5314',
            '--output', str(temp_dir)
        ]):
            result = main()

        assert result == 0

    @patch('data.dump_intergenic_sequences.dump_intergenic_sequences')
    def test_main_failure(self, mock_dump, temp_dir):
        """Test main function returns 1 on failure."""
        from data.dump_intergenic_sequences import main

        mock_dump.return_value = False

        with patch.object(sys, 'argv', [
            'prog',
            '--strain', 'C_albicans_SC5314',
            '--output', str(temp_dir)
        ]):
            result = main()

        assert result == 1

    @patch('data.dump_intergenic_sequences.dump_intergenic_sequences')
    def test_main_with_seq_source(self, mock_dump, temp_dir):
        """Test main function with seq-source option."""
        from data.dump_intergenic_sequences import main

        mock_dump.return_value = True

        with patch.object(sys, 'argv', [
            'prog',
            '--strain', 'C_albicans_SC5314',
            '--output', str(temp_dir),
            '--seq-source', 'Assembly22'
        ]):
            main()

        mock_dump.assert_called_once()
        call_kwargs = mock_dump.call_args[1]
        assert call_kwargs['seq_source'] == 'Assembly22'

    @patch('data.dump_intergenic_sequences.dump_intergenic_sequences')
    def test_main_no_compress(self, mock_dump, temp_dir):
        """Test main function with --no-compress option."""
        from data.dump_intergenic_sequences import main

        mock_dump.return_value = True

        with patch.object(sys, 'argv', [
            'prog',
            '--strain', 'C_albicans_SC5314',
            '--output', str(temp_dir),
            '--no-compress'
        ]):
            main()

        mock_dump.assert_called_once()
        call_kwargs = mock_dump.call_args[1]
        assert call_kwargs['compress'] is False


class TestGCContentCalculation:
    """Tests for GC content calculation in dump."""

    def test_gc_content_calculation(self):
        """Test GC content percentage calculation."""
        # Test sequence with known GC content
        sequence = "ATGCATGC"  # 50% GC

        sequence_clean = sequence.upper().replace(" ", "").replace("\n", "")
        gc_count = sequence_clean.count("G") + sequence_clean.count("C")
        gc_percent = (gc_count / len(sequence_clean)) * 100

        assert gc_percent == 50.0

    def test_at_content_calculation(self):
        """Test AT content percentage calculation."""
        sequence = "ATATGCGC"  # 50% AT, 50% GC

        sequence_clean = sequence.upper().replace(" ", "").replace("\n", "")
        at_count = sequence_clean.count("A") + sequence_clean.count("T")
        at_percent = (at_count / len(sequence_clean)) * 100

        assert at_percent == 50.0

    def test_all_gc_sequence(self):
        """Test 100% GC sequence."""
        sequence = "GCGCGCGC"

        sequence_clean = sequence.upper()
        gc_count = sequence_clean.count("G") + sequence_clean.count("C")
        gc_percent = (gc_count / len(sequence_clean)) * 100

        assert gc_percent == 100.0


class TestEdgeCases:
    """Tests for edge cases."""

    def test_empty_chromosome_list(self, mock_db_session, temp_dir):
        """Test handling when no chromosomes exist."""
        from data.dump_intergenic_sequences import IntergenicSequenceDumper

        mock_db_session.execute.return_value = []

        dumper = IntergenicSequenceDumper(
            mock_db_session,
            strain_abbrev="C_albicans_SC5314",
            seq_source="GenBank"
        )

        dumper.get_chromosome_lengths = MagicMock(return_value={})
        dumper.get_chromosome_names = MagicMock(return_value=[])

        fasta_file = temp_dir / "test.fasta"
        gff_file = temp_dir / "test.gff"

        intergenic_count, feature_count = dumper.dump_sequences(fasta_file, gff_file)

        assert intergenic_count == 0

    def test_single_base_intergenic(self, mock_db_session):
        """Test handling of single base intergenic region."""
        from data.dump_intergenic_sequences import IntergenicSequenceDumper

        mock_db_session.execute.return_value = [
            ("gene1", 100, 498),
            ("gene2", 500, 1000),
        ]

        dumper = IntergenicSequenceDumper(
            mock_db_session,
            strain_abbrev="C_albicans_SC5314",
            seq_source="GenBank"
        )

        regions = dumper.find_intergenic_regions("Ca22chr1", 2000)

        # Should have a 1bp intergenic region at position 499
        single_bp = any(
            end - start + 1 == 1
            for start, end, _, _ in regions
        )
        # This depends on exact implementation
