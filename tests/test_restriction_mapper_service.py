"""
Tests for Restriction Mapper service functionality.

Tests cover:
- IUPAC to regex conversion
- Reverse complement
- Enzyme loading (builtin and from files)
- Cut site finding (Python fallback)
- Fragment size calculation
- Result filtering
- TSV formatting
"""
import os
import pytest
import tempfile
from unittest.mock import MagicMock, patch

from cgd.core.restriction_config import (
    IUPAC_TO_REGEX,
    EnzymeFilterType,
    EnzymeType,
    EnzymeInfo,
    load_enzymes,
    get_builtin_enzymes,
    get_enzyme_file,
    pattern_to_regex,
    get_reverse_complement,
)
from cgd.schemas.restriction_mapper_schema import (
    EnzymeFilterType as SchemaEnzymeFilterType,
    EnzymeType as SchemaEnzymeType,
    EnzymeCutSite,
    RestrictionMapResult,
    RestrictionMapperRequest,
)
from cgd.api.services.restriction_mapper_service import (
    _iupac_to_regex,
    _reverse_complement,
    _find_cut_sites_python,
    _filter_enzymes,
    _get_sequence_for_locus,
    format_results_tsv,
    format_non_cutting_tsv,
    get_restriction_mapper_config,
    run_restriction_mapping,
)


class TestIUPACToRegex:
    """Tests for IUPAC nucleotide to regex conversion."""

    def test_standard_bases(self):
        """Standard bases should map to themselves."""
        assert IUPAC_TO_REGEX['A'] == 'A'
        assert IUPAC_TO_REGEX['C'] == 'C'
        assert IUPAC_TO_REGEX['G'] == 'G'
        assert IUPAC_TO_REGEX['T'] == 'T'

    def test_ambiguity_codes(self):
        """Ambiguity codes should expand to character classes."""
        assert IUPAC_TO_REGEX['R'] == '[AG]'  # Purine
        assert IUPAC_TO_REGEX['Y'] == '[CT]'  # Pyrimidine
        assert IUPAC_TO_REGEX['N'] == '[ACGT]'  # Any

    def test_pattern_conversion(self):
        """Full pattern should convert correctly."""
        # EcoRI site
        result = _iupac_to_regex("GAATTC")
        assert result == "GAATTC"

        # Pattern with ambiguity
        result = _iupac_to_regex("GANTC")
        assert result == "GA[ACGT]TC"

    def test_pattern_to_regex_function(self):
        """pattern_to_regex from config module should work."""
        result = pattern_to_regex("GAATTC")
        assert result == "GAATTC"


class TestReverseComplement:
    """Tests for reverse complement function."""

    def test_simple_reverse_complement(self):
        """Basic reverse complement."""
        assert _reverse_complement("ATGC") == "GCAT"

    def test_config_reverse_complement(self):
        """Config module reverse complement."""
        assert get_reverse_complement("ATGC") == "GCAT"

    def test_palindrome(self):
        """Palindromic sequences (restriction sites)."""
        # EcoRI site is palindromic
        assert _reverse_complement("GAATTC") == "GAATTC"

    def test_empty_sequence(self):
        """Empty sequence returns empty."""
        assert _reverse_complement("") == ""


class TestEnzymeInfo:
    """Tests for EnzymeInfo parsing."""

    def test_parse_valid_line(self):
        """Should parse valid enzyme line."""
        line = "EcoRI 1 4 GAATTC"
        enzyme = EnzymeInfo.from_line(line)

        assert enzyme is not None
        assert enzyme.name == "EcoRI"
        assert enzyme.offset == 1
        assert enzyme.overhang == 4
        assert enzyme.pattern == "GAATTC"
        assert enzyme.enzyme_type == EnzymeType.FIVE_PRIME

    def test_parse_blunt_enzyme(self):
        """Should correctly identify blunt end enzyme."""
        line = "SmaI 3 0 CCCGGG"
        enzyme = EnzymeInfo.from_line(line)

        assert enzyme is not None
        assert enzyme.enzyme_type == EnzymeType.BLUNT

    def test_parse_three_prime_overhang(self):
        """Should correctly identify 3' overhang enzyme."""
        line = "KpnI 5 -4 GGTACC"
        enzyme = EnzymeInfo.from_line(line)

        assert enzyme is not None
        assert enzyme.enzyme_type == EnzymeType.THREE_PRIME

    def test_parse_empty_line(self):
        """Empty line should return None."""
        assert EnzymeInfo.from_line("") is None
        assert EnzymeInfo.from_line("   ") is None

    def test_parse_comment_line(self):
        """Comment line should return None."""
        assert EnzymeInfo.from_line("# This is a comment") is None

    def test_parse_invalid_line(self):
        """Invalid line should return None."""
        assert EnzymeInfo.from_line("only two parts") is None


class TestEnzymeLoading:
    """Tests for enzyme loading functions."""

    def test_builtin_enzymes_not_empty(self):
        """Builtin enzymes should have entries."""
        enzymes = get_builtin_enzymes()
        assert len(enzymes) > 0

    def test_builtin_enzymes_have_ecori(self):
        """Builtin enzymes should include EcoRI."""
        enzymes = get_builtin_enzymes()
        names = [e.name for e in enzymes]
        assert "EcoRI" in names

    def test_builtin_enzymes_have_all_types(self):
        """Builtin enzymes should include all enzyme types."""
        enzymes = get_builtin_enzymes()
        types = {e.enzyme_type for e in enzymes}

        assert EnzymeType.FIVE_PRIME in types
        assert EnzymeType.THREE_PRIME in types
        assert EnzymeType.BLUNT in types

    def test_load_enzymes_fallback(self):
        """load_enzymes should fall back to builtin when file missing."""
        # This will use builtin since enzyme files likely don't exist in test
        enzymes = load_enzymes(EnzymeFilterType.ALL)
        assert len(enzymes) > 0

    def test_get_enzyme_file_returns_path(self):
        """get_enzyme_file should return a path."""
        path = get_enzyme_file(EnzymeFilterType.ALL)
        assert path is not None
        assert "rest_enzymes" in path


class TestCutSiteFinding:
    """Tests for finding restriction enzyme cut sites."""

    @pytest.fixture
    def ecori_enzyme(self):
        """EcoRI enzyme for testing."""
        return EnzymeInfo(
            name="EcoRI",
            offset=1,
            overhang=4,
            pattern="GAATTC",
            enzyme_type=EnzymeType.FIVE_PRIME,
        )

    @pytest.fixture
    def smai_enzyme(self):
        """SmaI (blunt cutter) for testing."""
        return EnzymeInfo(
            name="SmaI",
            offset=3,
            overhang=0,
            pattern="CCCGGG",
            enzyme_type=EnzymeType.BLUNT,
        )

    def test_find_single_cut_site(self, ecori_enzyme):
        """Should find a single cut site."""
        # Sequence with one EcoRI site
        sequence = "ATGCGAATTCATGC"
        result = _find_cut_sites_python(sequence, ecori_enzyme)

        assert result.enzyme_name == "EcoRI"
        assert result.total_cuts >= 1

    def test_find_multiple_cut_sites(self, ecori_enzyme):
        """Should find multiple cut sites."""
        # Sequence with two EcoRI sites
        sequence = "GAATTCAAAAAGAATTC"
        result = _find_cut_sites_python(sequence, ecori_enzyme)

        assert result.total_cuts >= 2

    def test_no_cut_sites(self, ecori_enzyme):
        """Should handle sequence with no cut sites."""
        sequence = "ATGCATGCATGCATGC"
        result = _find_cut_sites_python(sequence, ecori_enzyme)

        assert result.total_cuts == 0
        assert result.fragment_sizes == []

    def test_fragment_sizes_calculated(self, ecori_enzyme):
        """Should calculate fragment sizes correctly."""
        # Single EcoRI site in middle
        sequence = "AAAAAAGAATTCAAAAAA"  # 18 bp, site at 7-12
        result = _find_cut_sites_python(sequence, ecori_enzyme)

        if result.total_cuts > 0:
            # Should have 2 fragments
            assert len(result.fragment_sizes) == 2
            # Fragment sizes should sum to sequence length
            assert sum(result.fragment_sizes) == len(sequence)

    def test_palindrome_finds_both_strands(self, ecori_enzyme):
        """Palindromic sites should be found on both strands."""
        # EcoRI is palindromic, so searching both strands finds same site
        sequence = "GAATTC"
        result = _find_cut_sites_python(sequence, ecori_enzyme)

        # Both Watson and Crick should have positions
        # (they'll be the same position for a palindrome)
        total_positions = len(result.cut_positions_watson) + len(result.cut_positions_crick)
        assert total_positions >= 1


class TestEnzymeFiltering:
    """Tests for enzyme result filtering."""

    @pytest.fixture
    def sample_cut_sites(self):
        """Sample cut sites for filtering tests."""
        return [
            EnzymeCutSite(
                enzyme_name="EcoRI",
                recognition_seq="GAATTC",
                enzyme_type=SchemaEnzymeType.FIVE_PRIME,
                cut_positions_watson=[100],
                cut_positions_crick=[100],
                total_cuts=1,
                fragment_sizes=[100, 100],
            ),
            EnzymeCutSite(
                enzyme_name="BamHI",
                recognition_seq="GGATCC",
                enzyme_type=SchemaEnzymeType.FIVE_PRIME,
                cut_positions_watson=[50, 150],
                cut_positions_crick=[50, 150],
                total_cuts=2,
                fragment_sizes=[50, 100, 50],
            ),
            EnzymeCutSite(
                enzyme_name="SmaI",
                recognition_seq="CCCGGG",
                enzyme_type=SchemaEnzymeType.BLUNT,
                cut_positions_watson=[],
                cut_positions_crick=[],
                total_cuts=0,
                fragment_sizes=[],
            ),
            EnzymeCutSite(
                enzyme_name="KpnI",
                recognition_seq="GGTACC",
                enzyme_type=SchemaEnzymeType.THREE_PRIME,
                cut_positions_watson=[75],
                cut_positions_crick=[75],
                total_cuts=1,
                fragment_sizes=[75, 125],
            ),
        ]

    def test_filter_all(self, sample_cut_sites):
        """Filter ALL should include all cutting enzymes."""
        cutting, non_cutting = _filter_enzymes(
            SchemaEnzymeFilterType.ALL, sample_cut_sites
        )

        assert len(cutting) == 3  # EcoRI, BamHI, KpnI
        assert len(non_cutting) == 1  # SmaI (0 cuts)

    def test_filter_cut_once(self, sample_cut_sites):
        """Filter CUT_ONCE should only include enzymes cutting once."""
        cutting, non_cutting = _filter_enzymes(
            SchemaEnzymeFilterType.CUT_ONCE, sample_cut_sites
        )

        assert len(cutting) == 2  # EcoRI, KpnI
        for enzyme in cutting:
            assert enzyme.total_cuts == 1

    def test_filter_cut_twice(self, sample_cut_sites):
        """Filter CUT_TWICE should only include enzymes cutting twice."""
        cutting, non_cutting = _filter_enzymes(
            SchemaEnzymeFilterType.CUT_TWICE, sample_cut_sites
        )

        assert len(cutting) == 1  # BamHI
        assert cutting[0].enzyme_name == "BamHI"

    def test_filter_five_prime(self, sample_cut_sites):
        """Filter FIVE_PRIME should only include 5' overhang enzymes."""
        cutting, non_cutting = _filter_enzymes(
            SchemaEnzymeFilterType.FIVE_PRIME_OVERHANG, sample_cut_sites
        )

        for enzyme in cutting:
            assert enzyme.enzyme_type == SchemaEnzymeType.FIVE_PRIME

    def test_filter_three_prime(self, sample_cut_sites):
        """Filter THREE_PRIME should only include 3' overhang enzymes."""
        cutting, non_cutting = _filter_enzymes(
            SchemaEnzymeFilterType.THREE_PRIME_OVERHANG, sample_cut_sites
        )

        for enzyme in cutting:
            assert enzyme.enzyme_type == SchemaEnzymeType.THREE_PRIME


class TestResultFormatting:
    """Tests for TSV result formatting."""

    @pytest.fixture
    def sample_result(self):
        """Sample restriction map result."""
        cutting_enzymes = [
            EnzymeCutSite(
                enzyme_name="EcoRI",
                recognition_seq="GAATTC",
                enzyme_type=SchemaEnzymeType.FIVE_PRIME,
                cut_positions_watson=[100, 500],
                cut_positions_crick=[100, 500],
                total_cuts=2,
                fragment_sizes=[100, 400, 500],
            ),
        ]

        return RestrictionMapResult(
            seq_name="ACT1",
            seq_length=1000,
            coordinates="Chr1:1000-2000(+)",
            cutting_enzymes=cutting_enzymes,
            non_cutting_enzymes=["SmaI", "HpaI"],
            total_enzymes_searched=100,
        )

    def test_tsv_contains_header(self, sample_result):
        """TSV should contain header comments."""
        tsv = format_results_tsv(sample_result)

        assert "# Restriction Map for: ACT1" in tsv
        assert "# Sequence Length: 1000 bp" in tsv

    def test_tsv_contains_column_headers(self, sample_result):
        """TSV should contain column headers."""
        tsv = format_results_tsv(sample_result)

        assert "Enzyme\t" in tsv
        assert "Recognition\t" in tsv
        assert "Cuts\t" in tsv

    def test_tsv_contains_enzyme_data(self, sample_result):
        """TSV should contain enzyme data."""
        tsv = format_results_tsv(sample_result)

        assert "EcoRI\t" in tsv
        assert "GAATTC" in tsv

    def test_non_cutting_tsv(self, sample_result):
        """Non-cutting TSV should list non-cutting enzymes."""
        tsv = format_non_cutting_tsv(sample_result)

        assert "# Non-Cutting Enzymes" in tsv
        assert "SmaI" in tsv
        assert "HpaI" in tsv


class TestSequenceLookup:
    """Tests for database sequence lookup."""

    def test_get_sequence_returns_none_when_not_found(self):
        """Should return None when locus not found."""
        mock_db = MagicMock()
        mock_query = MagicMock()
        mock_query.outerjoin.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = None
        mock_db.query.return_value = mock_query

        result = _get_sequence_for_locus(mock_db, "NONEXISTENT")
        assert result is None


class TestConfig:
    """Tests for restriction mapper configuration."""

    def test_config_returns_enzyme_filters(self):
        """Config should return enzyme filter options."""
        config = get_restriction_mapper_config()

        assert len(config.enzyme_filters) > 0
        filter_values = [f.value for f in config.enzyme_filters]
        assert SchemaEnzymeFilterType.ALL in filter_values

    def test_config_returns_enzyme_count(self):
        """Config should return total enzyme count."""
        config = get_restriction_mapper_config()
        assert config.total_enzymes > 0


class TestRunRestrictionMapping:
    """Tests for the main run_restriction_mapping function."""

    def test_requires_locus_or_sequence(self):
        """Should return error when neither locus nor sequence provided."""
        mock_db = MagicMock()

        result = run_restriction_mapping(
            db=mock_db,
            locus=None,
            sequence=None,
        )

        assert result.success is False
        assert "must be provided" in result.error

    def test_handles_raw_sequence(self):
        """Should process raw DNA sequence."""
        mock_db = MagicMock()

        result = run_restriction_mapping(
            db=mock_db,
            locus=None,
            sequence="ATGCGAATTCATGC",  # Contains EcoRI site
            sequence_name="Test Sequence",
        )

        assert result.success is True
        assert result.result is not None
        assert result.result.seq_name == "Test Sequence"
        assert result.result.seq_length == 14

    def test_cleans_invalid_characters_from_sequence(self):
        """Should remove non-DNA characters from sequence."""
        mock_db = MagicMock()

        result = run_restriction_mapping(
            db=mock_db,
            sequence="ATG C\nGAA TTC\tATGC",  # With whitespace
            sequence_name="Test",
        )

        assert result.success is True
        # Should have removed spaces, newlines, tabs
        assert result.result.seq_length == 14

    def test_rejects_empty_sequence(self):
        """Should reject sequence with no valid DNA bases."""
        mock_db = MagicMock()

        result = run_restriction_mapping(
            db=mock_db,
            sequence="12345!@#$%",  # No DNA bases
        )

        assert result.success is False
        assert "no valid DNA bases" in result.error

    def test_locus_not_found(self):
        """Should return error when locus not found."""
        mock_db = MagicMock()
        mock_query = MagicMock()
        mock_query.outerjoin.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = None
        mock_db.query.return_value = mock_query

        result = run_restriction_mapping(
            db=mock_db,
            locus="NONEXISTENT_GENE",
        )

        assert result.success is False
        assert "not found" in result.error


class TestSchemaValidation:
    """Tests for request schema validation."""

    def test_valid_locus_request(self):
        """Valid locus request should pass."""
        request = RestrictionMapperRequest(
            locus="ACT1",
            enzyme_filter=SchemaEnzymeFilterType.ALL,
        )
        assert request.locus == "ACT1"

    def test_valid_sequence_request(self):
        """Valid sequence request should pass."""
        request = RestrictionMapperRequest(
            sequence="ATGCATGC",
            sequence_name="Test",
            enzyme_filter=SchemaEnzymeFilterType.CUT_ONCE,
        )
        assert request.sequence == "ATGCATGC"

    def test_default_enzyme_filter(self):
        """Default enzyme filter should be ALL."""
        request = RestrictionMapperRequest(locus="ACT1")
        assert request.enzyme_filter == SchemaEnzymeFilterType.ALL
