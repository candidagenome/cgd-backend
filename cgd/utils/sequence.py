"""
DNA/protein sequence manipulation utilities.

This module provides functions for common sequence operations
like reverse complement, translation, and subsequence extraction.
"""

from typing import Optional

# Standard genetic code
CODON_TABLE = {
    "TTT": "F", "TTC": "F", "TTA": "L", "TTG": "L",
    "TCT": "S", "TCC": "S", "TCA": "S", "TCG": "S",
    "TAT": "Y", "TAC": "Y", "TAA": "*", "TAG": "*",
    "TGT": "C", "TGC": "C", "TGA": "*", "TGG": "W",
    "CTT": "L", "CTC": "L", "CTA": "L", "CTG": "L",
    "CCT": "P", "CCC": "P", "CCA": "P", "CCG": "P",
    "CAT": "H", "CAC": "H", "CAA": "Q", "CAG": "Q",
    "CGT": "R", "CGC": "R", "CGA": "R", "CGG": "R",
    "ATT": "I", "ATC": "I", "ATA": "I", "ATG": "M",
    "ACT": "T", "ACC": "T", "ACA": "T", "ACG": "T",
    "AAT": "N", "AAC": "N", "AAA": "K", "AAG": "K",
    "AGT": "S", "AGC": "S", "AGA": "R", "AGG": "R",
    "GTT": "V", "GTC": "V", "GTA": "V", "GTG": "V",
    "GCT": "A", "GCC": "A", "GCA": "A", "GCG": "A",
    "GAT": "D", "GAC": "D", "GAA": "E", "GAG": "E",
    "GGT": "G", "GGC": "G", "GGA": "G", "GGG": "G",
}

# DNA complement mapping
COMPLEMENT_MAP = {
    "A": "T", "T": "A", "G": "C", "C": "G",
    "a": "t", "t": "a", "g": "c", "c": "g",
    "N": "N", "n": "n",
    "R": "Y", "Y": "R", "r": "y", "y": "r",
    "M": "K", "K": "M", "m": "k", "k": "m",
    "S": "S", "W": "W", "s": "s", "w": "w",
    "B": "V", "V": "B", "b": "v", "v": "b",
    "D": "H", "H": "D", "d": "h", "h": "d",
}


def reverse_complement(seq: str) -> str:
    """
    Return the reverse complement of a DNA sequence.

    Args:
        seq: DNA sequence string

    Returns:
        Reverse complement sequence

    Example:
        >>> reverse_complement("ATGC")
        "GCAT"
    """
    return "".join(COMPLEMENT_MAP.get(base, base) for base in reversed(seq))


def complement(seq: str) -> str:
    """
    Return the complement of a DNA sequence (without reversing).

    Args:
        seq: DNA sequence string

    Returns:
        Complement sequence
    """
    return "".join(COMPLEMENT_MAP.get(base, base) for base in seq)


def translate_dna(
    dna_seq: str,
    codon_table: Optional[dict[str, str]] = None,
    stop_symbol: str = "*",
    unknown_symbol: str = "X",
) -> str:
    """
    Translate a DNA sequence to protein.

    Args:
        dna_seq: DNA sequence (length should be multiple of 3)
        codon_table: Custom codon table (default: standard genetic code)
        stop_symbol: Character for stop codons
        unknown_symbol: Character for unknown codons

    Returns:
        Protein sequence

    Example:
        >>> translate_dna("ATGGCC")
        "MA"
    """
    if codon_table is None:
        codon_table = CODON_TABLE

    dna_seq = dna_seq.upper()
    protein = []

    for i in range(0, len(dna_seq) - 2, 3):
        codon = dna_seq[i:i + 3]
        amino_acid = codon_table.get(codon, unknown_symbol)
        protein.append(amino_acid)

    return "".join(protein)


def extract_subsequence(
    seq: str,
    start: int,
    stop: int,
    strand: str = "W",
) -> str:
    """
    Extract a subsequence based on coordinates and strand.

    Coordinates are 1-based inclusive (biological convention).
    For Crick strand, returns reverse complement.

    Args:
        seq: Full sequence string
        start: Start position (1-based)
        stop: Stop position (1-based, inclusive)
        strand: "W" or "+" for Watson, "C" or "-" for Crick

    Returns:
        Extracted subsequence (reverse complemented if Crick strand)

    Example:
        >>> extract_subsequence("ATGCGATCG", 2, 5, "W")
        "TGCG"
    """
    # Convert to 0-based indexing
    start_idx = start - 1
    stop_idx = stop

    # Ensure start < stop
    if start_idx > stop_idx:
        start_idx, stop_idx = stop_idx, start_idx + 1

    subseq = seq[start_idx:stop_idx]

    # Reverse complement for Crick strand
    if strand.upper() in ("C", "-"):
        subseq = reverse_complement(subseq)

    return subseq


def validate_dna_sequence(seq: str, allow_ambiguous: bool = True) -> bool:
    """
    Validate that a sequence contains only valid DNA characters.

    Args:
        seq: Sequence to validate
        allow_ambiguous: Allow IUPAC ambiguity codes (default: True)

    Returns:
        True if valid, False otherwise
    """
    if allow_ambiguous:
        valid_chars = set("ATGCNRYMKSWBDHVatgcnrymkswbdhv")
    else:
        valid_chars = set("ATGCNatgcn")

    return all(c in valid_chars for c in seq)


def validate_protein_sequence(seq: str) -> bool:
    """
    Validate that a sequence contains only valid protein characters.

    Args:
        seq: Sequence to validate

    Returns:
        True if valid, False otherwise
    """
    valid_chars = set("ACDEFGHIKLMNPQRSTVWYXZBJUacdefghiklmnpqrstvwyxzbju*")
    return all(c in valid_chars for c in seq)


def gc_content(seq: str) -> float:
    """
    Calculate GC content of a DNA sequence.

    Args:
        seq: DNA sequence

    Returns:
        GC content as a fraction (0.0 to 1.0)
    """
    seq = seq.upper()
    gc_count = seq.count("G") + seq.count("C")
    total = len(seq)
    return gc_count / total if total > 0 else 0.0


def count_bases(seq: str) -> dict[str, int]:
    """
    Count occurrences of each base in a sequence.

    Args:
        seq: DNA sequence

    Returns:
        Dictionary with base counts
    """
    seq = seq.upper()
    return {
        "A": seq.count("A"),
        "T": seq.count("T"),
        "G": seq.count("G"),
        "C": seq.count("C"),
        "N": seq.count("N"),
    }


def split_into_codons(seq: str) -> list[str]:
    """
    Split a sequence into codons.

    Args:
        seq: DNA sequence

    Returns:
        List of codon strings
    """
    return [seq[i:i + 3] for i in range(0, len(seq), 3)]


def get_orf_protein(
    seq: str,
    start: int,
    stop: int,
    strand: str,
    include_stop: bool = False,
) -> str:
    """
    Get the protein sequence for an ORF.

    Args:
        seq: Chromosome/contig sequence
        start: ORF start coordinate (1-based)
        stop: ORF stop coordinate (1-based)
        strand: "W" or "C" for strand
        include_stop: Include stop codon in translation

    Returns:
        Protein sequence
    """
    coding_seq = extract_subsequence(seq, start, stop, strand)
    protein = translate_dna(coding_seq)

    if not include_stop and protein.endswith("*"):
        protein = protein[:-1]

    return protein
