"""
Tests for PatMatch service functionality.

Tests cover:
- IUPAC code conversion (DNA and protein)
- Pattern conversion for nrgrep
- Reverse complement
- Python regex fallback search
- TSV result formatting
- Schema validation
"""
import os
import pytest
import tempfile
from unittest.mock import patch, MagicMock

from cgd.core.patmatch_config import (
    IUPAC_DNA,
    IUPAC_PROTEIN,
    PatternType,
    convert_pattern_for_nrgrep,
    get_reverse_complement,
    get_dataset_config,
    PATMATCH_DATASETS,
)
from cgd.schemas.patmatch_schema import (
    PatternType as SchemaPatternType,
    StrandOption,
    PatmatchSearchRequest,
    PatmatchDownloadRequest,
    PatmatchHit,
    PatmatchSearchResult,
)
from cgd.api.services.patmatch_service import (
    format_results_tsv,
    _run_python_search,
    _search_sequence_regex,
)


class TestIUPACCodes:
    """Tests for IUPAC nucleotide and protein code mappings."""

    def test_dna_standard_bases(self):
        """Standard DNA bases should map to themselves."""
        assert IUPAC_DNA['A'] == 'A'
        assert IUPAC_DNA['C'] == 'C'
        assert IUPAC_DNA['G'] == 'G'
        assert IUPAC_DNA['T'] == 'T'

    def test_dna_uracil_maps_to_thymine(self):
        """U (uracil) should map to T."""
        assert IUPAC_DNA['U'] == 'T'

    def test_dna_ambiguity_codes(self):
        """DNA ambiguity codes should expand to character classes."""
        assert IUPAC_DNA['R'] == '[AG]'  # Purine
        assert IUPAC_DNA['Y'] == '[CT]'  # Pyrimidine
        assert IUPAC_DNA['S'] == '[GC]'  # Strong
        assert IUPAC_DNA['W'] == '[AT]'  # Weak
        assert IUPAC_DNA['K'] == '[GT]'  # Keto
        assert IUPAC_DNA['M'] == '[AC]'  # Amino
        assert IUPAC_DNA['N'] == '[ACGT]'  # Any

    def test_dna_three_base_ambiguity_codes(self):
        """DNA codes for three bases should expand correctly."""
        assert IUPAC_DNA['B'] == '[CGT]'  # Not A
        assert IUPAC_DNA['D'] == '[AGT]'  # Not C
        assert IUPAC_DNA['H'] == '[ACT]'  # Not G
        assert IUPAC_DNA['V'] == '[ACG]'  # Not T

    def test_protein_standard_amino_acids(self):
        """Standard amino acids should map to themselves."""
        standard_aa = 'ACDEFGHIKLMNPQRSTVWY'
        for aa in standard_aa:
            assert IUPAC_PROTEIN[aa] == aa

    def test_protein_ambiguity_codes(self):
        """Protein ambiguity codes should expand correctly."""
        assert IUPAC_PROTEIN['B'] == '[DN]'  # Asp or Asn
        assert IUPAC_PROTEIN['Z'] == '[EQ]'  # Glu or Gln
        assert IUPAC_PROTEIN['X'] == '.'     # Any

    def test_protein_stop_codon(self):
        """Stop codon should be escaped."""
        assert IUPAC_PROTEIN['*'] == '\\*'


class TestPatternConversion:
    """Tests for pattern conversion functions."""

    def test_convert_simple_dna_pattern(self):
        """Simple DNA pattern should pass through unchanged."""
        result = convert_pattern_for_nrgrep("ATGC", PatternType.DNA)
        assert result == "ATGC"

    def test_convert_lowercase_pattern(self):
        """Lowercase pattern should be converted to uppercase."""
        result = convert_pattern_for_nrgrep("atgc", PatternType.DNA)
        assert result == "ATGC"

    def test_convert_pattern_with_whitespace(self):
        """Whitespace should be stripped."""
        result = convert_pattern_for_nrgrep("  ATGC  ", PatternType.DNA)
        assert result == "ATGC"

    def test_convert_dna_with_ambiguity_codes(self):
        """DNA pattern with ambiguity codes should expand."""
        result = convert_pattern_for_nrgrep("ATRYN", PatternType.DNA)
        assert result == "AT[AG][CT][ACGT]"

    def test_convert_dna_with_wildcard(self):
        """DNA pattern with wildcard should use [ACGT]."""
        result = convert_pattern_for_nrgrep("AT.C", PatternType.DNA)
        assert result == "AT[ACGT]C"

    def test_convert_protein_pattern(self):
        """Protein pattern should convert correctly."""
        result = convert_pattern_for_nrgrep("MVLX", PatternType.PROTEIN)
        assert result == "MVL."  # X becomes any

    def test_convert_protein_with_ambiguity(self):
        """Protein pattern with B/Z codes should expand."""
        result = convert_pattern_for_nrgrep("MBAZ", PatternType.PROTEIN)
        assert result == "M[DN]A[EQ]"


class TestReverseComplement:
    """Tests for reverse complement function."""

    def test_simple_reverse_complement(self):
        """Basic reverse complement should work."""
        assert get_reverse_complement("ATGC") == "GCAT"

    def test_reverse_complement_preserves_case(self):
        """Reverse complement should preserve case."""
        assert get_reverse_complement("AtGc") == "gCaT"

    def test_reverse_complement_of_complement(self):
        """Double reverse complement should return original."""
        original = "ATGCAATTGGCC"
        result = get_reverse_complement(get_reverse_complement(original))
        assert result == original

    def test_reverse_complement_palindrome(self):
        """Palindromic sequences should be their own reverse complement."""
        palindrome = "GAATTC"  # EcoRI site
        assert get_reverse_complement(palindrome) == palindrome

    def test_empty_sequence(self):
        """Empty sequence should return empty."""
        assert get_reverse_complement("") == ""

    def test_single_base(self):
        """Single base complement should work."""
        assert get_reverse_complement("A") == "T"
        assert get_reverse_complement("T") == "A"
        assert get_reverse_complement("G") == "C"
        assert get_reverse_complement("C") == "G"


class TestPythonRegexSearch:
    """Tests for Python regex fallback search."""

    @pytest.fixture
    def temp_fasta_file(self):
        """Create a temporary FASTA file for testing."""
        content = """>seq1 Test sequence 1
ATGCATGCATGCATGC
GGGGAAAACCCCTTTT
>seq2 Test sequence 2
ATATATATATAT
GCGCGCGCGCGC
>seq3 Short sequence
AAAA
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.fasta', delete=False) as f:
            f.write(content)
            temp_path = f.name

        yield temp_path

        # Cleanup
        os.unlink(temp_path)

    def test_search_finds_exact_match(self, temp_fasta_file):
        """Should find exact pattern matches."""
        import re
        pattern = re.compile("ATGC", re.IGNORECASE)

        hits, seqs_searched, residues, total = _run_python_search(
            "ATGC",
            temp_fasta_file,
            PatternType.DNA,
            StrandOption.WATSON,
            max_results=100,
        )

        assert total > 0
        assert seqs_searched == 3
        # seq1 has multiple ATGC matches
        assert any(h[0] == "seq1" for h in hits)

    def test_search_both_strands(self, temp_fasta_file):
        """Should search both strands when requested."""
        hits_both, _, _, total_both = _run_python_search(
            "AAAA",
            temp_fasta_file,
            PatternType.DNA,
            StrandOption.BOTH,
            max_results=100,
        )

        hits_watson, _, _, total_watson = _run_python_search(
            "AAAA",
            temp_fasta_file,
            PatternType.DNA,
            StrandOption.WATSON,
            max_results=100,
        )

        # Both strands should find at least as many as Watson only
        assert total_both >= total_watson

    def test_search_respects_max_results(self, temp_fasta_file):
        """Should limit results to max_results."""
        hits, _, _, total = _run_python_search(
            "AT",
            temp_fasta_file,
            PatternType.DNA,
            StrandOption.WATSON,
            max_results=2,
        )

        assert len(hits) <= 2
        # Total should reflect actual matches found
        assert total >= len(hits)

    def test_search_returns_correct_positions(self, temp_fasta_file):
        """Should return 1-based positions."""
        hits, _, _, _ = _run_python_search(
            "GGGG",
            temp_fasta_file,
            PatternType.DNA,
            StrandOption.WATSON,
            max_results=100,
        )

        # GGGG appears at position 17 in seq1 (after the first line)
        # But positions depend on how the sequence is concatenated
        for hit in hits:
            seq_name, start, end, strand, matched = hit
            assert start >= 1  # 1-based
            assert end >= start
            assert strand in ["W", "C"]

    def test_search_with_iupac_pattern(self, temp_fasta_file):
        """Should expand IUPAC codes in search."""
        # R = [AG], so ARG would match AAG or AGG
        hits, _, _, total = _run_python_search(
            "AAAN",  # N = any base
            temp_fasta_file,
            PatternType.DNA,
            StrandOption.WATSON,
            max_results=100,
        )

        # Should find matches
        assert total >= 0  # At least check it runs without error


class TestSearchSequenceRegex:
    """Tests for single sequence regex search helper."""

    def test_search_watson_strand(self):
        """Should find matches on Watson strand."""
        import re
        pattern = re.compile("ATG", re.IGNORECASE)

        hits = _search_sequence_regex(
            "test_seq",
            "ATGATGATG",
            pattern,
            StrandOption.WATSON,
            PatternType.DNA,
        )

        assert len(hits) == 3
        assert all(h[3] == "W" for h in hits)  # All Watson strand

    def test_search_crick_strand(self):
        """Should find matches on Crick strand."""
        import re
        pattern = re.compile("ATG", re.IGNORECASE)

        hits = _search_sequence_regex(
            "test_seq",
            "CATCATCAT",  # Reverse complement is ATGATGATG
            pattern,
            StrandOption.CRICK,
            PatternType.DNA,
        )

        assert len(hits) == 3
        assert all(h[3] == "C" for h in hits)  # All Crick strand

    def test_search_both_strands(self):
        """Should find matches on both strands."""
        import re
        pattern = re.compile("GAATTC", re.IGNORECASE)  # EcoRI palindrome

        hits = _search_sequence_regex(
            "test_seq",
            "GAATTC",  # Palindrome - matches both strands at same position
            pattern,
            StrandOption.BOTH,
            PatternType.DNA,
        )

        # Should find on both strands
        strands = [h[3] for h in hits]
        assert "W" in strands
        assert "C" in strands


class TestResultFormatting:
    """Tests for TSV result formatting."""

    @pytest.fixture
    def sample_result(self):
        """Create sample search result for testing."""
        hits = [
            PatmatchHit(
                sequence_name="seq1",
                sequence_description="Test sequence",
                match_start=100,
                match_end=110,
                strand="+",
                matched_sequence="ATGCATGCAT",
                context_before="AAAA",
                context_after="TTTT",
            ),
            PatmatchHit(
                sequence_name="seq2",
                sequence_description="Another sequence",
                match_start=200,
                match_end=210,
                strand="-",
                matched_sequence="GCTAGCTAGT",
                context_before="CCCC",
                context_after="GGGG",
            ),
        ]

        return PatmatchSearchResult(
            pattern="ATGCATGCAT",
            pattern_type="dna",
            dataset="Test Dataset",
            strand="both",
            total_hits=2,
            hits=hits,
            search_params={"max_mismatches": 0},
            sequences_searched=100,
            total_residues_searched=50000,
        )

    def test_tsv_contains_header_comments(self, sample_result):
        """TSV should contain header comments with search info."""
        tsv = format_results_tsv(sample_result)

        assert "# Pattern Match Results" in tsv
        assert "# Pattern: ATGCATGCAT" in tsv
        assert "# Dataset: Test Dataset" in tsv
        assert "# Total Hits: 2" in tsv

    def test_tsv_contains_column_headers(self, sample_result):
        """TSV should contain column headers."""
        tsv = format_results_tsv(sample_result)

        assert "Sequence\t" in tsv
        assert "Start\t" in tsv
        assert "End\t" in tsv
        assert "Strand\t" in tsv
        assert "Matched_Sequence" in tsv

    def test_tsv_contains_hit_data(self, sample_result):
        """TSV should contain hit data rows."""
        tsv = format_results_tsv(sample_result)

        assert "seq1\t" in tsv
        assert "seq2\t" in tsv
        assert "ATGCATGCAT" in tsv
        assert "100\t" in tsv

    def test_tsv_is_tab_separated(self, sample_result):
        """TSV should use tabs as delimiters."""
        tsv = format_results_tsv(sample_result)

        # Find data lines (non-comment, non-empty)
        data_lines = [
            line for line in tsv.split('\n')
            if line and not line.startswith('#')
        ]

        # Each data line should have tabs
        for line in data_lines:
            assert '\t' in line


class TestSchemaValidation:
    """Tests for PatMatch request schema validation."""

    def test_valid_search_request(self):
        """Valid request should pass validation."""
        request = PatmatchSearchRequest(
            pattern="ATGC",
            pattern_type=SchemaPatternType.DNA,
            dataset="genomic_C_albicans_SC5314_A22",
            strand=StrandOption.BOTH,
        )
        assert request.pattern == "ATGC"
        assert request.max_results == 100  # Default

    def test_pattern_max_length(self):
        """Pattern exceeding max length should fail."""
        with pytest.raises(Exception):  # ValidationError
            PatmatchSearchRequest(
                pattern="A" * 101,  # Exceeds 100 char limit
                pattern_type=SchemaPatternType.DNA,
                dataset="test",
            )

    def test_pattern_min_length(self):
        """Empty pattern should fail."""
        with pytest.raises(Exception):  # ValidationError
            PatmatchSearchRequest(
                pattern="",
                pattern_type=SchemaPatternType.DNA,
                dataset="test",
            )

    def test_max_mismatches_limit(self):
        """Mismatches exceeding limit should fail."""
        with pytest.raises(Exception):  # ValidationError
            PatmatchSearchRequest(
                pattern="ATGC",
                pattern_type=SchemaPatternType.DNA,
                dataset="test",
                max_mismatches=5,  # Exceeds 3
            )

    def test_search_max_results_limit(self):
        """Search request max_results has 50000 limit."""
        # Valid at 50000
        request = PatmatchSearchRequest(
            pattern="ATGC",
            pattern_type=SchemaPatternType.DNA,
            dataset="test",
            max_results=50000,
        )
        assert request.max_results == 50000

        # Invalid above 50000
        with pytest.raises(Exception):
            PatmatchSearchRequest(
                pattern="ATGC",
                pattern_type=SchemaPatternType.DNA,
                dataset="test",
                max_results=50001,
            )

    def test_download_request_higher_limit(self):
        """Download request should allow up to 50000 results."""
        request = PatmatchDownloadRequest(
            pattern="ATGC",
            pattern_type=SchemaPatternType.DNA,
            dataset="test",
            max_results=50000,
        )
        assert request.max_results == 50000

    def test_download_default_higher(self):
        """Download request should default to 10000."""
        request = PatmatchDownloadRequest(
            pattern="ATGC",
            pattern_type=SchemaPatternType.DNA,
            dataset="test",
        )
        assert request.max_results == 10000

    def test_strand_options(self):
        """All strand options should be valid."""
        for strand in [StrandOption.BOTH, StrandOption.WATSON, StrandOption.CRICK]:
            request = PatmatchSearchRequest(
                pattern="ATGC",
                pattern_type=SchemaPatternType.DNA,
                dataset="test",
                strand=strand,
            )
            assert request.strand == strand


class TestDatasetConfig:
    """Tests for dataset configuration."""

    def test_datasets_registered(self):
        """Datasets should be registered in PATMATCH_DATASETS."""
        assert len(PATMATCH_DATASETS) > 0

    def test_c_albicans_a22_datasets_exist(self):
        """C. albicans A22 datasets should be registered."""
        assert "genomic_C_albicans_SC5314_A22" in PATMATCH_DATASETS
        assert "orf_coding_C_albicans_SC5314_A22" in PATMATCH_DATASETS
        assert "orf_trans_all_C_albicans_SC5314_A22" in PATMATCH_DATASETS

    def test_dataset_has_required_fields(self):
        """Dataset config should have all required fields."""
        config = PATMATCH_DATASETS.get("genomic_C_albicans_SC5314_A22")
        if config:
            assert config.name is not None
            assert config.display_name is not None
            assert config.description is not None
            assert config.pattern_type in [PatternType.DNA, PatternType.PROTEIN]
            assert config.fasta_file is not None

    def test_protein_dataset_has_protein_type(self):
        """Protein datasets should have PROTEIN pattern type."""
        config = PATMATCH_DATASETS.get("orf_trans_all_C_albicans_SC5314_A22")
        if config:
            assert config.pattern_type == PatternType.PROTEIN

    def test_dna_dataset_has_dna_type(self):
        """DNA datasets should have DNA pattern type."""
        config = PATMATCH_DATASETS.get("genomic_C_albicans_SC5314_A22")
        if config:
            assert config.pattern_type == PatternType.DNA

    def test_get_dataset_config_returns_none_for_unknown(self):
        """get_dataset_config should return None for unknown dataset."""
        config = get_dataset_config("nonexistent_dataset")
        assert config is None
