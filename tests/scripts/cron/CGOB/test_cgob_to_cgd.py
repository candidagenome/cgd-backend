#!/usr/bin/env python3
"""
Unit tests for scripts/cron/CGOB/cgob_to_cgd.py

Tests the CGOB to CGD identifier mapping functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent.parent / "scripts"))

from cron.CGOB.cgob_to_cgd import (
    get_strain_from_prefix,
    normalize_id,
    parse_fasta_ids,
    parse_blast_results,
    STRAIN_PREFIXES,
    MIN_IDENTITY,
    MAX_GAPS,
    MAX_MISMATCH,
)


class TestGetStrainFromPrefix:
    """Tests for get_strain_from_prefix function."""

    def test_orf19_prefix(self):
        """Test orf19 prefix recognition."""
        result = get_strain_from_prefix("orf19.123")
        assert result == "C_albicans_SC5314"

    def test_ORF19_uppercase_prefix(self):
        """Test ORF19 uppercase prefix recognition."""
        result = get_strain_from_prefix("ORF19.456")
        assert result == "C_albicans_SC5314"

    def test_cagl_prefix(self):
        """Test CAGL prefix recognition."""
        result = get_strain_from_prefix("CAGL0A00001g")
        assert result == "C_glabrata_CBS138"

    def test_cort_prefix(self):
        """Test CORT prefix recognition."""
        result = get_strain_from_prefix("CORT0A00001g")
        assert result == "C_tropicalis_MYA3404"

    def test_cd36_prefix(self):
        """Test Cd36 prefix recognition."""
        result = get_strain_from_prefix("Cd36_00010")
        assert result == "C_dubliniensis_CD36"

    def test_CD36_uppercase_prefix(self):
        """Test CD36 uppercase prefix recognition."""
        result = get_strain_from_prefix("CD36_00010")
        assert result == "C_dubliniensis_CD36"

    def test_unknown_prefix(self):
        """Test unknown prefix returns None."""
        result = get_strain_from_prefix("UNKNOWN123")
        assert result is None

    def test_empty_string(self):
        """Test empty string returns None."""
        result = get_strain_from_prefix("")
        assert result is None


class TestNormalizeId:
    """Tests for normalize_id function."""

    def test_cort_normalization(self):
        """Test CORT prefix normalization."""
        result = normalize_id("CORT0A00001g")
        assert result == "CORT_0A00001g"

    def test_orf19_case_normalization(self):
        """Test ORF19 to orf19 case normalization."""
        result = normalize_id("ORF19.123")
        assert result == "orf19.123"

    def test_cd36_case_normalization(self):
        """Test CD36 to Cd36 case normalization."""
        result = normalize_id("CD36_00010")
        assert result == "Cd36_00010"

    def test_no_normalization_needed(self):
        """Test ID that doesn't need normalization."""
        result = normalize_id("CAGL0A00001g")
        assert result == "CAGL0A00001g"

    def test_already_normalized(self):
        """Test already normalized ID."""
        result = normalize_id("orf19.123")
        assert result == "orf19.123"


class TestParseFastaIds:
    """Tests for parse_fasta_ids function."""

    def test_parse_basic_fasta(self, temp_file):
        """Test parsing basic FASTA file."""
        fasta_content = """>orf19.1 description
ATGCATGC
>orf19.2 another desc
GCTAGCTA
"""
        fasta_file = temp_file("test.fasta", fasta_content)

        result = parse_fasta_ids(fasta_file)

        assert len(result) == 2
        assert ("orf19.1", "C_albicans_SC5314") in result
        assert ("orf19.2", "C_albicans_SC5314") in result

    def test_parse_mixed_species(self, temp_file):
        """Test parsing FASTA with mixed species."""
        fasta_content = """>orf19.1
ATGC
>CAGL0A00001g
GCTA
>CORT0A00001g
TTTT
"""
        fasta_file = temp_file("test.fasta", fasta_content)

        result = parse_fasta_ids(fasta_file)

        assert len(result) == 3
        strains = [strain for _, strain in result]
        assert "C_albicans_SC5314" in strains
        assert "C_glabrata_CBS138" in strains
        assert "C_tropicalis_MYA3404" in strains

    def test_parse_unknown_prefix_skipped(self, temp_file):
        """Test that unknown prefixes are skipped."""
        fasta_content = """>orf19.1
ATGC
>UNKNOWN123
GCTA
"""
        fasta_file = temp_file("test.fasta", fasta_content)

        result = parse_fasta_ids(fasta_file)

        assert len(result) == 1

    def test_parse_empty_file(self, temp_file):
        """Test parsing empty FASTA file."""
        fasta_file = temp_file("test.fasta", "")

        result = parse_fasta_ids(fasta_file)

        assert len(result) == 0

    def test_parse_multiline_sequence(self, temp_file):
        """Test parsing FASTA with multiline sequences."""
        fasta_content = """>orf19.1 desc
ATGCATGCATGC
ATGCATGCATGC
ATGCATGCATGC
>orf19.2
GCTA
"""
        fasta_file = temp_file("test.fasta", fasta_content)

        result = parse_fasta_ids(fasta_file)

        assert len(result) == 2


class TestParseBlastResults:
    """Tests for parse_blast_results function."""

    def test_parse_basic_results(self, temp_file):
        """Test parsing basic BLAST results."""
        # Format: query hit identity length mismatches gaps qstart qend sstart send evalue score
        blast_content = """query1\thit1\t100.0\t200\t0\t0\t1\t200\t1\t200\t1e-50\t400
query2\thit2\t99.0\t150\t1\t0\t1\t150\t1\t150\t1e-40\t300
"""
        blast_file = temp_file("blast.txt", blast_content)

        result = parse_blast_results(blast_file)

        assert "query1" in result
        assert "query2" in result
        assert result["query1"][0] == "hit1"
        assert result["query1"][1] == 100.0

    def test_filter_low_identity(self, temp_file):
        """Test that low identity hits are filtered."""
        blast_content = """query1\thit1\t95.0\t200\t10\t0\t1\t200\t1\t200\t1e-50\t400
"""
        blast_file = temp_file("blast.txt", blast_content)

        result = parse_blast_results(blast_file)

        # Identity 95% < MIN_IDENTITY (98%)
        assert "query1" not in result

    def test_filter_too_many_gaps(self, temp_file):
        """Test that hits with too many gaps are filtered."""
        blast_content = """query1\thit1\t99.0\t200\t0\t5\t1\t200\t1\t200\t1e-50\t400
"""
        blast_file = temp_file("blast.txt", blast_content)

        result = parse_blast_results(blast_file)

        # Gaps 5 > MAX_GAPS (0)
        assert "query1" not in result

    def test_filter_too_many_mismatches(self, temp_file):
        """Test that hits with too many mismatches are filtered."""
        blast_content = """query1\thit1\t99.0\t200\t10\t0\t1\t200\t1\t200\t1e-50\t400
"""
        blast_file = temp_file("blast.txt", blast_content)

        result = parse_blast_results(blast_file)

        # Mismatches 10 > MAX_MISMATCH (4)
        assert "query1" not in result

    def test_perfect_self_match(self, temp_file):
        """Test perfect self-match is accepted."""
        blast_content = """query1\tquery1\t100.0\t200\t0\t0\t1\t200\t1\t200\t1e-100\t500
"""
        blast_file = temp_file("blast.txt", blast_content)

        result = parse_blast_results(blast_file)

        assert "query1" in result
        assert result["query1"][0] == "query1"

    def test_first_valid_hit_kept(self, temp_file):
        """Test that first valid hit is kept for a query."""
        blast_content = """query1\thit1\t99.0\t200\t2\t0\t1\t200\t1\t200\t1e-40\t300
query1\thit2\t99.5\t200\t1\t0\t1\t200\t1\t200\t1e-50\t400
"""
        blast_file = temp_file("blast.txt", blast_content)

        result = parse_blast_results(blast_file)

        # First valid hit is kept (code doesn't update once query is in best_hits)
        assert result["query1"][0] == "hit1"

    def test_short_lines_skipped(self, temp_file):
        """Test that short lines are skipped."""
        blast_content = """query1\thit1\t100.0
query2\thit2\t100.0\t200\t0\t0\t1\t200\t1\t200\t1e-50\t400
"""
        blast_file = temp_file("blast.txt", blast_content)

        result = parse_blast_results(blast_file)

        assert "query1" not in result
        assert "query2" in result

    def test_empty_file(self, temp_file):
        """Test parsing empty BLAST file."""
        blast_file = temp_file("blast.txt", "")

        result = parse_blast_results(blast_file)

        assert result == {}


class TestConstants:
    """Tests for module constants."""

    def test_strain_prefixes_defined(self):
        """Test that strain prefixes are defined."""
        assert len(STRAIN_PREFIXES) > 0
        assert "orf19" in STRAIN_PREFIXES
        assert "CAGL" in STRAIN_PREFIXES

    def test_min_identity(self):
        """Test MIN_IDENTITY is reasonable."""
        assert MIN_IDENTITY == 98.0
        assert MIN_IDENTITY > 0
        assert MIN_IDENTITY <= 100

    def test_max_gaps(self):
        """Test MAX_GAPS is defined."""
        assert MAX_GAPS == 0

    def test_max_mismatch(self):
        """Test MAX_MISMATCH is defined."""
        assert MAX_MISMATCH == 4


class TestEdgeCases:
    """Tests for edge cases."""

    def test_normalize_multiple_patterns(self):
        """Test ID that could match multiple normalization patterns."""
        # ORF19 takes precedence since it's checked first
        result = normalize_id("ORF19.123")
        assert result.startswith("orf19")

    def test_fasta_with_complex_headers(self, temp_file):
        """Test FASTA with complex header lines."""
        fasta_content = """>orf19.1 gene=ACT1 organism=Candida description="Actin protein"
ATGC
"""
        fasta_file = temp_file("test.fasta", fasta_content)

        result = parse_fasta_ids(fasta_file)

        # Should extract just the ID (first word after >)
        assert len(result) == 1
        assert result[0][0] == "orf19.1"

    def test_blast_with_tied_scores(self, temp_file):
        """Test BLAST results with tied scores."""
        blast_content = """query1\thit1\t99.0\t200\t2\t0\t1\t200\t1\t200\t1e-50\t400
query1\thit2\t99.0\t200\t2\t0\t1\t200\t1\t200\t1e-50\t400
"""
        blast_file = temp_file("blast.txt", blast_content)

        result = parse_blast_results(blast_file)

        # First hit should be kept (same score)
        assert "query1" in result
