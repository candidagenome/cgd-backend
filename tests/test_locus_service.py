"""
Tests for Locus Service.

Tests cover:
- Gene name to protein name conversion
- Systematic name to protein name conversion
- GCG sequence formatting
- CUG codon counting
- Codon translation (including table 12)
- Sequence translation
- Allelic variation checking
- Citation formatting
- Domain database inference from accession
"""
import pytest

from cgd.api.services.locus_service import (
    _gene_name_to_protein_name,
    _systematic_name_to_protein_name,
    _format_sequence_gcg,
    _count_cug_codons,
    _translate_codon,
    _translate_sequence,
    _check_allelic_variation,
    _format_citation,
    _infer_domain_db_from_accession,
)


class TestGeneNameToProteinName:
    """Tests for _gene_name_to_protein_name."""

    def test_simple_gene_name(self):
        """Should convert ACT1 to Act1p."""
        assert _gene_name_to_protein_name("ACT1") == "Act1p"

    def test_uppercase_gene_name(self):
        """Should capitalize first letter and lowercase rest."""
        assert _gene_name_to_protein_name("CDC42") == "Cdc42p"

    def test_three_letter_gene(self):
        """Should handle 3-letter gene names."""
        assert _gene_name_to_protein_name("BRG1") == "Brg1p"

    def test_empty_string(self):
        """Should return empty string for empty input."""
        assert _gene_name_to_protein_name("") == ""

    def test_none_handling(self):
        """Should handle None-like values."""
        assert _gene_name_to_protein_name(None) == ""

    def test_lowercase_input(self):
        """Should handle lowercase input."""
        assert _gene_name_to_protein_name("als1") == "Als1p"


class TestSystematicNameToProteinName:
    """Tests for _systematic_name_to_protein_name."""

    def test_candida_pattern_a_allele(self):
        """Should convert C1_13700W_A to C1_13700wp_a."""
        assert _systematic_name_to_protein_name("C1_13700W_A") == "C1_13700wp_a"

    def test_candida_pattern_b_allele(self):
        """Should convert C1_13700W_B to C1_13700wp_b."""
        assert _systematic_name_to_protein_name("C1_13700W_B") == "C1_13700wp_b"

    def test_orf_pattern(self):
        """Should convert orf19.1234 to orf19.1234p."""
        result = _systematic_name_to_protein_name("orf19.1234")
        assert result == "orf19.1234p"

    def test_orf_uppercase(self):
        """Should handle uppercase ORF."""
        result = _systematic_name_to_protein_name("ORF19.5678")
        assert result == "orf19.5678p"

    def test_empty_string(self):
        """Should return empty string for empty input."""
        assert _systematic_name_to_protein_name("") == ""

    def test_none_handling(self):
        """Should handle None."""
        assert _systematic_name_to_protein_name(None) == ""

    def test_default_fallback(self):
        """Should use default lowercase+p for unrecognized patterns."""
        result = _systematic_name_to_protein_name("UNKNOWN123")
        assert result == "unknown123p"


class TestFormatSequenceGCG:
    """Tests for _format_sequence_gcg."""

    def test_basic_formatting(self):
        """Should format sequence in GCG format."""
        seq = "MDDDIAALVD"
        result = _format_sequence_gcg(seq, "ACT1p", 10)
        assert "!!AA_SEQUENCE 1.0" in result
        assert "ACT1p" in result
        assert "Length: 10" in result

    def test_includes_checksum(self):
        """Should include checksum value."""
        seq = "MDDDIAALVD"
        result = _format_sequence_gcg(seq, "Test", 10)
        assert "Check:" in result

    def test_empty_sequence(self):
        """Should return empty string for empty sequence."""
        assert _format_sequence_gcg("", "Test", 0) == ""

    def test_long_sequence_formatting(self):
        """Should format long sequences with line breaks."""
        seq = "A" * 100
        result = _format_sequence_gcg(seq, "Test", 100)
        lines = result.split("\n")
        # Should have header lines plus data lines
        assert len(lines) > 3


class TestCountCugCodons:
    """Tests for _count_cug_codons."""

    def test_single_ctg(self):
        """Should count single CTG codon."""
        # ATG CTG TAA = 1 CTG
        assert _count_cug_codons("ATGCTGTAA") == 1

    def test_multiple_ctg(self):
        """Should count multiple CTG codons."""
        # CTG CTG CTG = 3 CTGs
        assert _count_cug_codons("CTGCTGCTG") == 3

    def test_no_ctg(self):
        """Should return 0 when no CTG present."""
        assert _count_cug_codons("ATGATGATG") == 0

    def test_ctg_out_of_frame(self):
        """Should only count in-frame CTG codons."""
        # A CTG TGA = CTG is in frame
        # ACT GTG A = no in-frame CTG
        assert _count_cug_codons("ACTGTGA") == 0

    def test_case_insensitive(self):
        """Should be case insensitive."""
        assert _count_cug_codons("ctgctgctg") == 3
        assert _count_cug_codons("CtGcTgCtG") == 3

    def test_empty_sequence(self):
        """Should return 0 for empty sequence."""
        assert _count_cug_codons("") == 0

    def test_none_handling(self):
        """Should return 0 for None."""
        assert _count_cug_codons(None) == 0

    def test_short_sequence(self):
        """Should handle sequences shorter than 3 bases."""
        assert _count_cug_codons("CT") == 0


class TestTranslateCodon:
    """Tests for _translate_codon."""

    def test_standard_codons(self):
        """Should translate standard codons."""
        assert _translate_codon("ATG") == "M"  # Methionine (start)
        assert _translate_codon("TTT") == "F"  # Phenylalanine
        assert _translate_codon("TAA") == "*"  # Stop

    def test_ctg_table_12(self):
        """Should translate CTG as Serine in table 12."""
        assert _translate_codon("CTG", use_table_12=True) == "S"

    def test_ctg_standard(self):
        """Should translate CTG as Leucine in standard table."""
        assert _translate_codon("CTG", use_table_12=False) == "L"

    def test_case_insensitive(self):
        """Should be case insensitive."""
        assert _translate_codon("atg") == "M"
        assert _translate_codon("AtG") == "M"

    def test_unknown_codon(self):
        """Should return X for unknown codons."""
        assert _translate_codon("NNN") == "X"
        assert _translate_codon("XXX") == "X"

    def test_all_stop_codons(self):
        """Should recognize all stop codons."""
        assert _translate_codon("TAA") == "*"
        assert _translate_codon("TAG") == "*"
        assert _translate_codon("TGA") == "*"


class TestTranslateSequence:
    """Tests for _translate_sequence."""

    def test_simple_translation(self):
        """Should translate simple sequence."""
        # ATG GCT = Met Ala
        result = _translate_sequence("ATGGCT")
        assert result == "MA"

    def test_table_12_ctg(self):
        """Should use table 12 by default (CTG -> Ser)."""
        # CTG = Ser in table 12
        result = _translate_sequence("CTG", use_table_12=True)
        assert result == "S"

    def test_standard_table_ctg(self):
        """Should use standard table when specified (CTG -> Leu)."""
        result = _translate_sequence("CTG", use_table_12=False)
        assert result == "L"

    def test_stop_codon(self):
        """Should translate stop codon as *."""
        result = _translate_sequence("ATGTAA")
        assert result == "M*"

    def test_ambiguous_bases(self):
        """Should return X for codons with ambiguous bases."""
        result = _translate_sequence("ATGNNN")
        assert result == "MX"

    def test_empty_sequence(self):
        """Should return empty for empty input."""
        assert _translate_sequence("") == ""

    def test_none_handling(self):
        """Should return empty for None."""
        assert _translate_sequence(None) == ""

    def test_incomplete_codon_ignored(self):
        """Should ignore incomplete final codon."""
        # ATG GC = Met + incomplete
        result = _translate_sequence("ATGGC")
        assert result == "M"


class TestCheckAllelicVariation:
    """Tests for _check_allelic_variation."""

    def test_identical_sequences(self):
        """Should detect no variation for identical sequences."""
        result = _check_allelic_variation(
            "ATGATG", "ATGATG", "primary", "allele"
        )
        assert "No allelic variation" in result

    def test_synonymous_variation(self):
        """Should detect synonymous variation."""
        # Both translate to same protein but different DNA
        # GCT and GCC both code for Alanine
        result = _check_allelic_variation(
            "GCT", "GCC", "primary", "allele"
        )
        assert "Synonymous variation" in result

    def test_nonsynonymous_variation(self):
        """Should detect non-synonymous variation."""
        # ATG = Met, GCT = Ala
        result = _check_allelic_variation(
            "ATG", "GCT", "primary", "allele"
        )
        assert "Non-synonymous variation" in result

    def test_ambiguous_sequence(self):
        """Should note ambiguous sequences."""
        result = _check_allelic_variation(
            "ATGNNN", "ATGNNN", "primary", "allele"
        )
        assert "ambiguous" in result.lower()

    def test_none_primary(self):
        """Should return None if primary is None."""
        result = _check_allelic_variation(
            None, "ATGATG", "primary", "allele"
        )
        assert result is None

    def test_none_allele(self):
        """Should return None if allele is None."""
        result = _check_allelic_variation(
            "ATGATG", None, "primary", "allele"
        )
        assert result is None


class TestFormatCitation:
    """Tests for _format_citation."""

    def test_standard_citation(self):
        """Should extract first author and add et al."""
        result = _format_citation("Smith, Jones, Brown (2023) Title")
        assert result == "Smith et al"

    def test_single_word_author(self):
        """Should handle single word author."""
        result = _format_citation("Johnson (2020) Some paper")
        assert result == "Johnson et al"

    def test_empty_citation(self):
        """Should return empty for empty input."""
        assert _format_citation("") == ""

    def test_none_handling(self):
        """Should return empty for None."""
        assert _format_citation(None) == ""

    def test_long_author_name(self):
        """Should handle long author names."""
        # Since the regex extracts first word before space/comma, a long word is treated as author
        long_text = "x" * 50
        result = _format_citation(long_text)
        # The regex matches the whole thing as the first author
        assert "et al" in result


class TestInferDomainDbFromAccession:
    """Tests for _infer_domain_db_from_accession."""

    def test_pfam(self):
        """Should recognize Pfam accessions."""
        name, url = _infer_domain_db_from_accession("PF00001")
        assert name == "Pfam"
        assert "pfam" in url.lower()

    def test_panther(self):
        """Should recognize PANTHER accessions."""
        name, url = _infer_domain_db_from_accession("PTHR10000")
        assert name == "PANTHER"
        assert "panther" in url.lower()

    def test_smart(self):
        """Should recognize SMART accessions."""
        name, url = _infer_domain_db_from_accession("SM00001")
        assert name == "SMART"
        assert "smart" in url.lower()

    def test_superfamily(self):
        """Should recognize SUPERFAMILY accessions."""
        name, url = _infer_domain_db_from_accession("SSF12345")
        assert name == "SUPERFAMILY"

    def test_gene3d(self):
        """Should recognize Gene3D accessions."""
        name, url = _infer_domain_db_from_accession("G3DSA:1.10.10.10")
        assert name == "Gene3D"

    def test_cdd(self):
        """Should recognize CDD accessions."""
        name, url = _infer_domain_db_from_accession("CD00001")
        assert name == "CDD"

    def test_prosite(self):
        """Should recognize ProSite accessions."""
        name, url = _infer_domain_db_from_accession("PS00001")
        assert name == "ProSite"

    def test_prints(self):
        """Should recognize PRINTS accessions."""
        name, url = _infer_domain_db_from_accession("PR00001")
        assert name == "PRINTS"

    def test_tigrfams(self):
        """Should recognize TIGRFAMs accessions."""
        name, url = _infer_domain_db_from_accession("TIGR00001")
        assert name == "TIGRFAMs"

    def test_pirsf(self):
        """Should recognize PIRSF accessions."""
        name, url = _infer_domain_db_from_accession("PIRSF000001")
        assert name == "PIRSF"

    def test_interpro(self):
        """Should recognize InterPro accessions."""
        name, url = _infer_domain_db_from_accession("IPR000001")
        assert name == "InterPro"

    def test_unknown(self):
        """Should return Unknown for unrecognized accessions."""
        name, url = _infer_domain_db_from_accession("UNKNOWN123")
        assert name == "Unknown"
        assert url is None

    def test_case_insensitive(self):
        """Should be case insensitive."""
        name, _ = _infer_domain_db_from_accession("pf00001")
        assert name == "Pfam"


class TestCodonTableCompleteness:
    """Tests to verify codon table coverage."""

    def test_all_standard_amino_acids(self):
        """Should translate codons for all 20 standard amino acids."""
        # Test at least one codon for each amino acid
        codons = {
            'A': 'GCT', 'C': 'TGT', 'D': 'GAT', 'E': 'GAA',
            'F': 'TTT', 'G': 'GGT', 'H': 'CAT', 'I': 'ATT',
            'K': 'AAA', 'L': 'TTA', 'M': 'ATG', 'N': 'AAT',
            'P': 'CCT', 'Q': 'CAA', 'R': 'CGT', 'S': 'TCT',
            'T': 'ACT', 'V': 'GTT', 'W': 'TGG', 'Y': 'TAT',
        }
        for aa, codon in codons.items():
            result = _translate_codon(codon, use_table_12=False)
            assert result == aa, f"Codon {codon} should translate to {aa}"

    def test_table_12_serine_codons(self):
        """Should have CTG as Serine in table 12."""
        serine_codons = ['TCT', 'TCC', 'TCA', 'TCG', 'AGT', 'AGC', 'CTG']
        for codon in serine_codons:
            result = _translate_codon(codon, use_table_12=True)
            assert result == 'S', f"Codon {codon} should translate to S in table 12"


class TestTranslationEdgeCases:
    """Edge case tests for translation functions."""

    def test_lowercase_translation(self):
        """Should handle lowercase sequence."""
        result = _translate_sequence("atggct")
        assert result == "MA"

    def test_mixed_case_translation(self):
        """Should handle mixed case sequence."""
        result = _translate_sequence("AtGgCt")
        assert result == "MA"

    def test_very_long_sequence(self):
        """Should handle very long sequences."""
        # 1000 codons
        long_seq = "ATG" * 1000
        result = _translate_sequence(long_seq)
        assert len(result) == 1000
        assert all(c == 'M' for c in result)

    def test_realistic_candida_gene(self):
        """Should translate realistic Candida gene fragment with CTG."""
        # A fragment that includes CTG codons
        # ATG CTG GCT CTG TAA
        seq = "ATGCTGGCTCTGTAA"
        result = _translate_sequence(seq, use_table_12=True)
        # M S A S * (CTG -> S in table 12)
        assert result == "MSAS*"
