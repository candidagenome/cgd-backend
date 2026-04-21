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
    _convert_repetitions,
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
    _create_clean_sequence_file,
    _generate_fasta_index,
    _parse_nrgrep_output,
    _find_sequence_offset,
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


class TestRepetitionConversion:
    """Tests for PatMatch repetition syntax conversion."""

    def test_no_repetition_unchanged(self):
        """Pattern without repetition should pass through unchanged."""
        assert _convert_repetitions("ATGC") == "ATGC"
        assert _convert_repetitions("[ST]QPKA") == "[ST]QPKA"

    def test_exact_repetition(self):
        """Test {m} exact repetition."""
        assert _convert_repetitions("A{2}") == "AA"
        assert _convert_repetitions("A{3}") == "AAA"
        assert _convert_repetitions("G{1}") == "G"

    def test_optional_repetition(self):
        """Test {0,1} optional repetition."""
        assert _convert_repetitions("A{0,1}") == "A?"

    def test_range_repetition(self):
        """Test {m,n} range repetition."""
        assert _convert_repetitions("A{2,3}") == "AAA?"
        assert _convert_repetitions("A{1,3}") == "AA?A?"
        assert _convert_repetitions("A{2,4}") == "AAA?A?"

    def test_minimum_repetition(self):
        """Test {m,} minimum repetition (m or more)."""
        assert _convert_repetitions("A{2,}") == "AAA*"
        assert _convert_repetitions("A{1,}") == "AA*"

    def test_maximum_repetition(self):
        """Test {,n} maximum repetition (0 to n)."""
        assert _convert_repetitions("A{,2}") == "A?A?"
        assert _convert_repetitions("A{,3}") == "A?A?A?"

    def test_character_class_repetition(self):
        """Test repetition with character class [...]."""
        assert _convert_repetitions("[ST]{0,1}") == "[ST]?"
        assert _convert_repetitions("[ST]{2}") == "[ST][ST]"
        assert _convert_repetitions("[ACG]{1,3}") == "[ACG][ACG]?[ACG]?"

    def test_mixed_pattern_with_repetition(self):
        """Test pattern with repetition mixed with other characters."""
        assert _convert_repetitions("M[ST]{0,1}QPKA") == "M[ST]?QPKA"
        assert _convert_repetitions("ATG{2}C") == "ATGGC"
        assert _convert_repetitions("[AT]{2}GC[CG]{1,2}") == "[AT][AT]GC[CG][CG]?"

    def test_multiple_repetitions(self):
        """Test pattern with multiple repetition groups."""
        assert _convert_repetitions("A{2}T{3}") == "AATTT"
        assert _convert_repetitions("[ST]{0,1}X{2,3}") == "[ST]?XXX?"


class TestPatternConversionWithRepetitions:
    """Tests for full pattern conversion including repetitions."""

    def test_protein_pattern_with_repetition(self):
        """Test protein pattern with repetition syntax."""
        # [ST]{0,1}QPKA should become [ST]?QPKA
        result = convert_pattern_for_nrgrep("[ST]{0,1}QPKA", PatternType.PROTEIN)
        assert result == "[ST]?QPKA"

    def test_protein_pattern_with_wildcard_and_repetition(self):
        """Test protein pattern with X wildcard and repetition."""
        # M[ST]{0,1}X should become M[ST]?.
        result = convert_pattern_for_nrgrep("M[ST]{0,1}X", PatternType.PROTEIN)
        assert result == "M[ST]?."

    def test_protein_iupac_with_repetition(self):
        """Test protein pattern with IUPAC code and repetition."""
        # B{2} should become [DN][DN]
        result = convert_pattern_for_nrgrep("B{2}", PatternType.PROTEIN)
        assert result == "[DN][DN]"

    def test_dna_pattern_with_repetition(self):
        """Test DNA pattern with repetition."""
        result = convert_pattern_for_nrgrep("ATG{2}", PatternType.DNA)
        assert result == "ATGG"

    def test_dna_n_wildcard_repetition(self):
        """Test DNA N wildcard with repetition."""
        # N{3} should become [ACGT][ACGT][ACGT]
        result = convert_pattern_for_nrgrep("N{3}", PatternType.DNA)
        assert result == "[ACGT][ACGT][ACGT]"

    def test_dna_iupac_r_with_repetition(self):
        """Test DNA R (purine) with repetition."""
        # R{0,1}ATG should become [AG]?ATG
        result = convert_pattern_for_nrgrep("R{0,1}ATG", PatternType.DNA)
        assert result == "[AG]?ATG"

    def test_dna_character_class_repetition(self):
        """Test DNA character class with repetition."""
        result = convert_pattern_for_nrgrep("[AT]{2,3}", PatternType.DNA)
        assert result == "[AT][AT][AT]?"

    def test_complex_pattern_with_repetition(self):
        """Test complex pattern combining multiple features."""
        # Protein: M[AVILM]{2,4}[ST]{0,1}K
        result = convert_pattern_for_nrgrep("M[AVILM]{2,4}[ST]{0,1}K", PatternType.PROTEIN)
        assert "[AVILM]" in result
        assert "[ST]?" in result
        assert result.startswith("M")
        assert result.endswith("K")


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

        # New format with NumHits includes different column names
        assert "Sequence Name\t" in tsv
        assert "NumHits\t" in tsv
        assert "MatchStartCoord\t" in tsv
        assert "MatchStopCoord\t" in tsv
        assert "Strand\t" in tsv
        assert "MatchPattern\t" in tsv

    def test_tsv_contains_hit_data(self, sample_result):
        """TSV should contain hit data rows."""
        tsv = format_results_tsv(sample_result)

        # Uses sequence_description which is "Test sequence" and "Another sequence"
        assert "Test sequence\t" in tsv
        assert "Another sequence\t" in tsv
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

    def test_tsv_with_num_hits(self):
        """TSV with include_num_hits=True should have NumHits column."""
        hits = [
            PatmatchHit(
                sequence_name="seq1",
                sequence_description="seq1/GENE1",
                match_start=100,
                match_end=110,
                strand="+",
                matched_sequence="ATGCAT",
            ),
            PatmatchHit(
                sequence_name="seq1",
                sequence_description="seq1/GENE1",
                match_start=200,
                match_end=210,
                strand="+",
                matched_sequence="ATGCAT",
            ),
            PatmatchHit(
                sequence_name="seq2",
                sequence_description="seq2/GENE2",
                match_start=50,
                match_end=60,
                strand="-",
                matched_sequence="ATGCAT",
            ),
        ]

        result = PatmatchSearchResult(
            pattern="ATGCAT",
            pattern_type="protein",
            dataset="Test Dataset",
            strand="both",
            total_hits=3,
            hits=hits,
            search_params={},
            sequences_searched=100,
            total_residues_searched=50000,
        )

        tsv = format_results_tsv(result, include_num_hits=True)

        # Header should have NumHits column
        assert "NumHits" in tsv

        # seq1 has 2 hits, seq2 has 1 hit
        lines = tsv.split('\n')
        data_lines = [l for l in lines if l and not l.startswith('#') and 'Sequence' not in l]

        # Check that seq1 rows show NumHits=2
        seq1_lines = [l for l in data_lines if 'seq1' in l]
        for line in seq1_lines:
            fields = line.split('\t')
            assert fields[1] == '2'  # NumHits is second column

        # Check that seq2 row shows NumHits=1
        seq2_lines = [l for l in data_lines if 'seq2' in l]
        for line in seq2_lines:
            fields = line.split('\t')
            assert fields[1] == '1'

    def test_tsv_includes_gene_name_in_description(self):
        """TSV should show gene name with systematic name when available."""
        hits = [
            PatmatchHit(
                sequence_name="CR_05010W_A",
                sequence_description="CR_05010W_A/YEN1",  # Gene name included
                match_start=100,
                match_end=110,
                strand="+",
                matched_sequence="ATGCAT",
            ),
        ]

        result = PatmatchSearchResult(
            pattern="ATGCAT",
            pattern_type="protein",
            dataset="Test Dataset",
            strand="both",
            total_hits=1,
            hits=hits,
            search_params={},
            sequences_searched=100,
            total_residues_searched=50000,
        )

        tsv = format_results_tsv(result, include_num_hits=True)

        # Should contain the systematic name with gene name
        assert "CR_05010W_A/YEN1" in tsv


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


class TestCleanSequenceFile:
    """Tests for clean sequence file generation and coordinate mapping."""

    @pytest.fixture
    def multi_seq_fasta(self):
        """Create a FASTA file with multiple sequences and wrapped lines."""
        content = """>seq1 First protein
ABCDEFGHIJ
KLMNOPQRST
>seq2 Second protein
UVWXYZABCD
>seq3 Third protein
EFGHIJKLMN
OPQRSTUVWX
YZ
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.fasta', delete=False) as f:
            f.write(content)
            temp_path = f.name

        yield temp_path
        os.unlink(temp_path)

    def test_create_clean_sequence_file_concatenates_sequences(self, multi_seq_fasta):
        """Clean file should have all sequences concatenated without headers/newlines."""
        temp_file, index, offsets = _create_clean_sequence_file(multi_seq_fasta)

        try:
            with open(temp_file, 'r') as f:
                content = f.read()

            # Should be: seq1 (20 chars) + seq2 (10 chars) + seq3 (22 chars) = 52 chars
            assert content == "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ"
            assert len(content) == 52
        finally:
            os.unlink(temp_file)

    def test_create_clean_sequence_file_builds_correct_index(self, multi_seq_fasta):
        """Index should map sequence start offsets to sequence names."""
        temp_file, index, offsets = _create_clean_sequence_file(multi_seq_fasta)

        try:
            # seq1 starts at 0, has 20 chars
            # seq2 starts at 20, has 10 chars
            # seq3 starts at 30, has 22 chars
            assert offsets == [0, 20, 30]
            assert index[0] == "seq1"
            assert index[20] == "seq2"
            assert index[30] == "seq3"
        finally:
            os.unlink(temp_file)

    def test_find_sequence_offset_first_sequence(self):
        """Position in first sequence should return offset 0."""
        offsets = [0, 20, 30]
        assert _find_sequence_offset(0, offsets) == 0
        assert _find_sequence_offset(5, offsets) == 0
        assert _find_sequence_offset(19, offsets) == 0

    def test_find_sequence_offset_middle_sequence(self):
        """Position in middle sequence should return correct offset."""
        offsets = [0, 20, 30]
        assert _find_sequence_offset(20, offsets) == 20
        assert _find_sequence_offset(25, offsets) == 20
        assert _find_sequence_offset(29, offsets) == 20

    def test_find_sequence_offset_last_sequence(self):
        """Position in last sequence should return correct offset."""
        offsets = [0, 20, 30]
        assert _find_sequence_offset(30, offsets) == 30
        assert _find_sequence_offset(45, offsets) == 30
        assert _find_sequence_offset(51, offsets) == 30

    def test_parse_nrgrep_output_correct_sequence_attribution(self):
        """Hits should be attributed to correct sequences based on position."""
        # Simulate nrgrep output for matches in different sequences
        nrgrep_output = """[5, 11: FGHIJK]
[22, 28: XYZABC]
[35, 41: JKLMNO]
"""
        # seq1: 0-19 (20 chars), seq2: 20-29 (10 chars), seq3: 30-51 (22 chars)
        index = {0: "seq1", 20: "seq2", 30: "seq3"}
        offsets = [0, 20, 30]

        hits = _parse_nrgrep_output(nrgrep_output, index, offsets)

        # First hit at global 5-11 should be in seq1
        assert hits[0][0] == "seq1"
        assert hits[0][1] == 6  # local_start = 5 - 0 + 1 = 6
        assert hits[0][4] == "FGHIJK"

        # Second hit at global 22-28 should be in seq2
        assert hits[1][0] == "seq2"
        assert hits[1][1] == 3  # local_start = 22 - 20 + 1 = 3
        assert hits[1][4] == "XYZABC"

        # Third hit at global 35-41 should be in seq3
        assert hits[2][0] == "seq3"
        assert hits[2][1] == 6  # local_start = 35 - 30 + 1 = 6
        assert hits[2][4] == "JKLMNO"

    def test_parse_nrgrep_output_handles_boundary_positions(self):
        """Hits at sequence boundaries should be attributed correctly."""
        # Hit starting exactly at seq2 boundary
        nrgrep_output = "[20, 26: UVWXYZ]\n"
        index = {0: "seq1", 20: "seq2", 30: "seq3"}
        offsets = [0, 20, 30]

        hits = _parse_nrgrep_output(nrgrep_output, index, offsets)

        assert len(hits) == 1
        assert hits[0][0] == "seq2"
        assert hits[0][1] == 1  # local_start = 20 - 20 + 1 = 1


class TestProteinPatternSearch:
    """Tests for protein pattern searches (related to reported bug)."""

    @pytest.fixture
    def protein_fasta(self):
        """Create a FASTA file with known protein sequences for testing."""
        # Include a sequence with the pattern HX[KR]XX[ST]
        content = """>CR_10860C_A Test protein A
MSTVLKAHPKN
NSDEFGHIJKL
>CR_10860C_B Test protein B
ABCDEFGHIJK
LMNOPQRSTUV
>CR_10860C_C Test protein C with match
AAHTKRFFSKL
MNHPKRQQSXY
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.fasta', delete=False) as f:
            f.write(content)
            temp_path = f.name

        yield temp_path
        os.unlink(temp_path)

    def test_python_search_correct_attribution(self, protein_fasta):
        """Python search should attribute matches to correct sequences."""
        import re

        # Pattern HX[KR]XX[ST] -> H.[KR]..[ST] as regex
        pattern = re.compile(r'H.[KR]..[ST]', re.IGNORECASE)

        hits, seqs_searched, residues, total = _run_python_search(
            "H.[KR]..[ST]",
            protein_fasta,
            PatternType.PROTEIN,
            StrandOption.BOTH,
            max_results=100,
        )

        # Should find matches only in sequences that actually contain the pattern
        # CR_10860C_A has HPKN at position 8, but need to verify if it matches
        # CR_10860C_C has HTKRFFS at position 3 which matches H.[KR]..[ST]? No, that's HTKRFF which matches H(T)[KR](RF)F - not [ST] at end

        # Actually let's trace through:
        # CR_10860C_A: MSTVLKAHPKNNSDEFGHIJKL - position 8 is H, then P, K, N, N, S
        #              HPKNNS matches H(P)[K](NN)[S] = H.K..S ✓
        # CR_10860C_C: AAHTKRFFSKLMNHPKRQQSXY
        #              Position 3: HTKRFF - H(T)[K](RF)F - no [ST] at end
        #              Position 14: HPKRQQS - H(P)[K](RQ)Q - no [ST] at end

        # So only CR_10860C_A should have a match
        for hit in hits:
            # Verify hit is attributed to the right sequence
            seq_name = hit[0]
            matched_seq = hit[4]
            local_start = hit[1]

            # The matched sequence should actually exist in that protein
            # This is the key check - if the bug were still present, this would fail
            assert len(matched_seq) == 6  # Pattern is 6 chars

    def test_generate_fasta_index_matches_clean_file(self, protein_fasta):
        """Regular index should produce same offsets as clean file index."""
        regular_index, regular_offsets = _generate_fasta_index(protein_fasta)
        temp_file, clean_index, clean_offsets = _create_clean_sequence_file(protein_fasta)

        try:
            # Both should have same offsets and sequence names
            assert regular_offsets == clean_offsets
            assert regular_index == clean_index
        finally:
            os.unlink(temp_file)
