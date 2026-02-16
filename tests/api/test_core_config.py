"""
Tests for Core Configuration Modules.

Tests cover:
- BLAST configuration utilities
- Restriction enzyme configuration utilities
- Pattern matching configuration utilities
"""
import pytest

from cgd.core.blast_config import (
    extract_organism_tag_from_database,
    build_database_name,
    build_database_names,
    get_database_type_for_dataset,
    BLAST_ORGANISMS,
)
from cgd.core.restriction_config import (
    pattern_to_regex,
    get_reverse_complement as restriction_get_reverse_complement,
    get_builtin_enzymes,
)
from cgd.core.patmatch_config import (
    convert_pattern_for_nrgrep,
    get_reverse_complement as patmatch_get_reverse_complement,
    PatternType,
)


class TestExtractOrganismTagFromDatabase:
    """Tests for extract_organism_tag_from_database."""

    def test_new_naming_convention_genomic(self):
        """Should extract organism from new naming convention."""
        result = extract_organism_tag_from_database("default_genomic_C_albicans_SC5314_A22")
        assert result == "C_albicans_SC5314_A22"

    def test_simple_genomic_prefix(self):
        """Should extract organism from simple genomic prefix."""
        result = extract_organism_tag_from_database("genomic_C_albicans_SC5314_A21")
        assert result == "C_albicans_SC5314_A21"

    def test_coding_prefix(self):
        """Should extract organism from coding prefix."""
        result = extract_organism_tag_from_database("coding_C_glabrata_CBS138")
        assert result == "C_glabrata_CBS138"

    def test_protein_prefix(self):
        """Should extract organism from protein prefix."""
        result = extract_organism_tag_from_database("protein_C_auris_B11221")
        assert result == "C_auris_B11221"

    def test_orf_genomic_prefix(self):
        """Should extract organism from orf_genomic prefix."""
        result = extract_organism_tag_from_database("orf_genomic_C_dubliniensis_CD36")
        assert result == "C_dubliniensis_CD36"

    def test_orf_coding_prefix(self):
        """Should extract organism from orf_coding prefix."""
        result = extract_organism_tag_from_database("orf_coding_C_parapsilosis_CDC317")
        assert result == "C_parapsilosis_CDC317"

    def test_legacy_genome_suffix(self):
        """Should extract organism from legacy naming with _genome suffix."""
        result = extract_organism_tag_from_database("C_albicans_SC5314_A22_genome")
        assert result == "C_albicans_SC5314_A22"

    def test_returns_none_for_unknown(self):
        """Should return None for unrecognized patterns."""
        result = extract_organism_tag_from_database("unknown_pattern")
        assert result is None


class TestBuildDatabaseName:
    """Tests for build_database_name."""

    def test_genome_type(self):
        """Should build genomic database name."""
        result = build_database_name("C_albicans_SC5314_A22", "GENOME")
        assert result == "genomic_C_albicans_SC5314_A22"

    def test_genes_type(self):
        """Should build genes database name."""
        result = build_database_name("C_albicans_SC5314_A22", "GENES")
        assert result == "orf_genomic_C_albicans_SC5314_A22"

    def test_coding_type(self):
        """Should build coding database name."""
        result = build_database_name("C_albicans_SC5314_A22", "CODING")
        assert result == "orf_coding_C_albicans_SC5314_A22"

    def test_protein_type(self):
        """Should build protein database name."""
        result = build_database_name("C_albicans_SC5314_A22", "PROTEIN")
        assert result == "orf_trans_all_C_albicans_SC5314_A22"

    def test_other_type(self):
        """Should build other features database name."""
        result = build_database_name("C_albicans_SC5314_A22", "OTHER")
        assert result == "other_features_genomic_C_albicans_SC5314_A22"

    def test_unknown_type_defaults_to_genomic(self):
        """Should default to genomic for unknown type."""
        result = build_database_name("C_albicans_SC5314_A22", "UNKNOWN")
        assert result == "genomic_C_albicans_SC5314_A22"


class TestBuildDatabaseNames:
    """Tests for build_database_names."""

    def test_multiple_genomes(self):
        """Should build database names for multiple genomes."""
        genomes = ["C_albicans_SC5314_A22", "C_glabrata_CBS138"]
        result = build_database_names(genomes, "GENOME")
        assert result == [
            "genomic_C_albicans_SC5314_A22",
            "genomic_C_glabrata_CBS138",
        ]

    def test_empty_list(self):
        """Should handle empty genome list."""
        result = build_database_names([], "GENOME")
        assert result == []

    def test_single_genome(self):
        """Should handle single genome."""
        result = build_database_names(["C_auris_B11221"], "PROTEIN")
        assert result == ["orf_trans_all_C_auris_B11221"]


class TestGetDatabaseTypeForDataset:
    """Tests for get_database_type_for_dataset."""

    def test_genome_is_nucleotide(self):
        """GENOME should return nucleotide."""
        assert get_database_type_for_dataset("GENOME") == "nucleotide"

    def test_genes_is_nucleotide(self):
        """GENES should return nucleotide."""
        assert get_database_type_for_dataset("GENES") == "nucleotide"

    def test_coding_is_nucleotide(self):
        """CODING should return nucleotide."""
        assert get_database_type_for_dataset("CODING") == "nucleotide"

    def test_protein_is_protein(self):
        """PROTEIN should return protein."""
        assert get_database_type_for_dataset("PROTEIN") == "protein"

    def test_unknown_defaults_to_nucleotide(self):
        """Unknown type should default to nucleotide."""
        assert get_database_type_for_dataset("UNKNOWN") == "nucleotide"


class TestBlastOrganismsConfig:
    """Tests for BLAST_ORGANISMS configuration."""

    def test_c_albicans_present(self):
        """C. albicans SC5314 A22 should be configured."""
        assert "C_albicans_SC5314_A22" in BLAST_ORGANISMS

    def test_c_albicans_has_required_fields(self):
        """C. albicans config should have required fields."""
        config = BLAST_ORGANISMS["C_albicans_SC5314_A22"]
        assert "full_name" in config
        assert "trans_table" in config
        assert "seq_sets" in config

    def test_c_albicans_trans_table_12(self):
        """C. albicans should use translation table 12."""
        config = BLAST_ORGANISMS["C_albicans_SC5314_A22"]
        assert config["trans_table"] == 12

    def test_c_glabrata_trans_table_1(self):
        """C. glabrata should use standard translation table."""
        config = BLAST_ORGANISMS["C_glabrata_CBS138"]
        assert config["trans_table"] == 1


class TestPatternToRegex:
    """Tests for pattern_to_regex (restriction config)."""

    def test_simple_pattern(self):
        """Should convert simple ACGT pattern."""
        result = pattern_to_regex("ACGT")
        assert result == "ACGT"

    def test_r_ambiguity(self):
        """R should convert to [AG]."""
        result = pattern_to_regex("R")
        assert result == "[AG]"

    def test_y_ambiguity(self):
        """Y should convert to [CT]."""
        result = pattern_to_regex("Y")
        assert result == "[CT]"

    def test_n_any_base(self):
        """N should convert to [ACGT]."""
        result = pattern_to_regex("N")
        assert result == "[ACGT]"

    def test_complex_pattern(self):
        """Should convert complex IUPAC pattern."""
        result = pattern_to_regex("GAATTC")
        assert result == "GAATTC"

    def test_pattern_with_ambiguity(self):
        """Should convert pattern with IUPAC codes."""
        result = pattern_to_regex("GANTC")
        assert "N" not in result or "[ACGT]" in result

    def test_lowercase_converted(self):
        """Should handle lowercase input."""
        result = pattern_to_regex("acgt")
        assert result == "ACGT"


class TestRestrictionGetReverseComplement:
    """Tests for get_reverse_complement (restriction config)."""

    def test_simple_sequence(self):
        """Should return reverse complement."""
        result = restriction_get_reverse_complement("ACGT")
        assert result == "ACGT"  # ACGT is self-complementary when reversed

    def test_asymmetric_sequence(self):
        """Should reverse complement asymmetric sequence."""
        result = restriction_get_reverse_complement("AACGT")
        # AACGT -> complement TTGCA -> reverse ACGTT
        assert result == "ACGTT"

    def test_all_a(self):
        """All A should become all T reversed."""
        result = restriction_get_reverse_complement("AAAA")
        assert result == "TTTT"

    def test_lowercase_handling(self):
        """Should handle lowercase input."""
        result = restriction_get_reverse_complement("acgt")
        assert result.upper() == "ACGT"

    def test_iupac_codes(self):
        """Should handle IUPAC ambiguity codes."""
        # R (A/G) complements to Y (T/C)
        result = restriction_get_reverse_complement("R")
        assert result == "Y"

    def test_empty_string(self):
        """Should handle empty string."""
        result = restriction_get_reverse_complement("")
        assert result == ""


class TestConvertPatternForNrgrep:
    """Tests for convert_pattern_for_nrgrep."""

    def test_simple_dna_pattern(self):
        """Should convert simple DNA pattern."""
        result = convert_pattern_for_nrgrep("ACGT", PatternType.DNA)
        assert result == "ACGT"

    def test_dna_ambiguity_n(self):
        """N should expand to character class for DNA."""
        result = convert_pattern_for_nrgrep("N", PatternType.DNA)
        assert "[" in result and "]" in result

    def test_dna_ambiguity_r(self):
        """R should expand to [AG] for DNA."""
        result = convert_pattern_for_nrgrep("R", PatternType.DNA)
        assert result == "[AG]"

    def test_dna_ambiguity_y(self):
        """Y should expand to [CT] for DNA."""
        result = convert_pattern_for_nrgrep("Y", PatternType.DNA)
        assert result == "[CT]"

    def test_protein_pattern(self):
        """Should convert simple protein pattern."""
        result = convert_pattern_for_nrgrep("ACDE", PatternType.PROTEIN)
        assert result == "ACDE"

    def test_protein_ambiguity_x(self):
        """X should expand to . for protein."""
        result = convert_pattern_for_nrgrep("X", PatternType.PROTEIN)
        assert result == "."

    def test_uppercase_conversion(self):
        """Should convert to uppercase."""
        result = convert_pattern_for_nrgrep("acgt", PatternType.DNA)
        assert result == "ACGT"

    def test_strip_whitespace(self):
        """Should strip whitespace."""
        result = convert_pattern_for_nrgrep("  ACGT  ", PatternType.DNA)
        assert result == "ACGT"


class TestPatmatchGetReverseComplement:
    """Tests for get_reverse_complement (patmatch config)."""

    def test_simple_sequence(self):
        """Should return reverse complement."""
        result = patmatch_get_reverse_complement("ACGT")
        assert result == "ACGT"

    def test_asymmetric_sequence(self):
        """Should reverse complement asymmetric sequence."""
        result = patmatch_get_reverse_complement("AACGT")
        # AACGT -> complement TTGCA -> reverse ACGTT
        assert result == "ACGTT"

    def test_lowercase(self):
        """Should handle lowercase."""
        result = patmatch_get_reverse_complement("acgt")
        assert result == "acgt"

    def test_empty_string(self):
        """Should handle empty string."""
        result = patmatch_get_reverse_complement("")
        assert result == ""


class TestBuiltinEnzymes:
    """Tests for get_builtin_enzymes configuration."""

    def test_ecori_present(self):
        """EcoRI should be in builtin enzymes."""
        enzymes = get_builtin_enzymes()
        ecori = next((e for e in enzymes if e.name == "EcoRI"), None)
        assert ecori is not None

    def test_ecori_pattern(self):
        """EcoRI should have correct recognition pattern."""
        enzymes = get_builtin_enzymes()
        ecori = next((e for e in enzymes if e.name == "EcoRI"), None)
        assert ecori.pattern == "GAATTC"

    def test_enzymes_have_required_fields(self):
        """All enzymes should have name and pattern."""
        enzymes = get_builtin_enzymes()
        for enzyme in enzymes[:10]:  # Check first 10
            assert enzyme.name is not None
            assert enzyme.pattern is not None

    def test_hindiii_present(self):
        """HindIII should be in builtin enzymes."""
        enzymes = get_builtin_enzymes()
        hindiii = next((e for e in enzymes if e.name == "HindIII"), None)
        assert hindiii is not None


class TestEdgeCases:
    """Edge case tests."""

    def test_empty_database_name(self):
        """Should handle empty database name."""
        result = extract_organism_tag_from_database("")
        assert result is None

    def test_very_long_pattern(self):
        """Should handle very long patterns."""
        long_pattern = "ACGT" * 100
        result = pattern_to_regex(long_pattern)
        assert len(result) == 400

    def test_mixed_case_organism(self):
        """Should handle organism names with mixed case."""
        result = build_database_name("C_Albicans_SC5314_A22", "GENOME")
        assert "C_Albicans_SC5314_A22" in result
