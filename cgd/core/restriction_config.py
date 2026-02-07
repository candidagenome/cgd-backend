"""
Restriction Mapper Configuration - Enzyme files and binary tool settings.
"""
import os
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


# Binary tool path
SCAN_FOR_MATCHES_BINARY = os.environ.get("SCAN_FOR_MATCHES_BINARY", "/data/bin/scan_for_matches")

# Restriction enzyme data directory
RESTRICTION_ENZYMES_DIR = os.environ.get("RESTRICTION_ENZYMES_DIR", "/data/restriction_enzymes")


class EnzymeFilterType(str, Enum):
    """Filter types for restriction enzyme selection."""
    ALL = "all"
    THREE_PRIME_OVERHANG = "3_prime"
    FIVE_PRIME_OVERHANG = "5_prime"
    BLUNT = "blunt"
    CUT_ONCE = "cut_once"
    CUT_TWICE = "cut_twice"
    SIX_BASE = "six_base"


class EnzymeType(str, Enum):
    """Type of enzyme cut."""
    THREE_PRIME = "3_prime"
    FIVE_PRIME = "5_prime"
    BLUNT = "blunt"


@dataclass
class EnzymeInfo:
    """Information about a restriction enzyme."""
    name: str
    offset: int          # Cut position offset from pattern start
    overhang: int        # Overhang length (positive = 5', negative = 3', 0 = blunt)
    pattern: str         # Recognition sequence
    enzyme_type: EnzymeType

    @classmethod
    def from_line(cls, line: str) -> Optional["EnzymeInfo"]:
        """Parse enzyme info from a line in the enzyme file."""
        line = line.strip()
        if not line or line.startswith('#'):
            return None

        parts = line.split()
        if len(parts) < 4:
            return None

        try:
            name = parts[0]
            offset = int(parts[1])
            overhang = int(parts[2])
            pattern = parts[3]

            # Determine enzyme type from overhang
            if overhang == 0:
                enzyme_type = EnzymeType.BLUNT
            elif overhang > 0:
                enzyme_type = EnzymeType.FIVE_PRIME
            else:
                enzyme_type = EnzymeType.THREE_PRIME

            return cls(
                name=name,
                offset=offset,
                overhang=overhang,
                pattern=pattern,
                enzyme_type=enzyme_type,
            )
        except (ValueError, IndexError):
            return None


# Enzyme file paths for different filters
ENZYME_FILES: Dict[EnzymeFilterType, str] = {
    EnzymeFilterType.ALL: os.path.join(RESTRICTION_ENZYMES_DIR, "rest_enzymes"),
    EnzymeFilterType.THREE_PRIME_OVERHANG: os.path.join(RESTRICTION_ENZYMES_DIR, "rest_enzymes.3"),
    EnzymeFilterType.FIVE_PRIME_OVERHANG: os.path.join(RESTRICTION_ENZYMES_DIR, "rest_enzymes.5"),
    EnzymeFilterType.BLUNT: os.path.join(RESTRICTION_ENZYMES_DIR, "rest_enzymes.blunt"),
    EnzymeFilterType.SIX_BASE: os.path.join(RESTRICTION_ENZYMES_DIR, "rest_enzymes.6base"),
    # CUT_ONCE and CUT_TWICE use the ALL file but filter results
    EnzymeFilterType.CUT_ONCE: os.path.join(RESTRICTION_ENZYMES_DIR, "rest_enzymes"),
    EnzymeFilterType.CUT_TWICE: os.path.join(RESTRICTION_ENZYMES_DIR, "rest_enzymes"),
}


def get_enzyme_file(filter_type: EnzymeFilterType) -> str:
    """Get the enzyme file path for a given filter type."""
    return ENZYME_FILES.get(filter_type, ENZYME_FILES[EnzymeFilterType.ALL])


def load_enzymes(filter_type: EnzymeFilterType = EnzymeFilterType.ALL) -> List[EnzymeInfo]:
    """
    Load enzyme information from the appropriate enzyme file.

    Returns list of EnzymeInfo objects. Falls back to builtin enzymes if
    file doesn't exist, can't be read, or contains no valid entries.
    """
    enzyme_file = get_enzyme_file(filter_type)
    enzymes = []

    if not os.path.exists(enzyme_file):
        # Fall back to built-in enzyme list if file doesn't exist
        return get_builtin_enzymes()

    try:
        with open(enzyme_file, 'r') as f:
            for line in f:
                enzyme = EnzymeInfo.from_line(line)
                if enzyme:
                    enzymes.append(enzyme)
    except IOError:
        return get_builtin_enzymes()

    # Fall back to builtin if file was empty or had no valid entries
    if not enzymes:
        return get_builtin_enzymes()

    return enzymes


def get_builtin_enzymes() -> List[EnzymeInfo]:
    """
    Return a built-in list of common restriction enzymes as fallback.
    Used when enzyme files are not available.
    """
    # Common restriction enzymes with format: (name, offset, overhang, pattern)
    builtin = [
        # 6-base cutters with 5' overhang
        ("EcoRI", 1, 4, "GAATTC"),
        ("BamHI", 1, 4, "GGATCC"),
        ("HindIII", 1, 4, "AAGCTT"),
        ("SalI", 1, 4, "GTCGAC"),
        ("XbaI", 1, 4, "TCTAGA"),
        ("XhoI", 1, 4, "CTCGAG"),
        ("NcoI", 1, 4, "CCATGG"),
        ("NdeI", 2, 2, "CATATG"),
        ("BglII", 1, 4, "AGATCT"),
        ("ClaI", 2, 2, "ATCGAT"),

        # 6-base cutters with 3' overhang
        ("SacI", 5, -4, "GAGCTC"),
        ("SphI", 5, -4, "GCATGC"),
        ("KpnI", 5, -4, "GGTACC"),
        ("PstI", 5, -4, "CTGCAG"),
        ("ApaI", 5, -4, "GGGCCC"),

        # 6-base cutters with blunt ends
        ("SmaI", 3, 0, "CCCGGG"),
        ("EcoRV", 3, 0, "GATATC"),
        ("StuI", 3, 0, "AGGCCT"),
        ("HpaI", 3, 0, "GTTAAC"),
        ("NruI", 3, 0, "TCGCGA"),
        ("PvuII", 3, 0, "CAGCTG"),

        # 8-base cutters (rare cutters)
        ("NotI", 2, 4, "GCGGCCGC"),
        ("PacI", 5, -2, "TTAATTAA"),
        ("AscI", 2, 4, "GGCGCGCC"),

        # 4-base cutters (frequent cutters)
        ("MboI", 0, 4, "GATC"),
        ("Sau3AI", 0, 4, "GATC"),
        ("HaeIII", 2, 0, "GGCC"),
        ("AluI", 2, 0, "AGCT"),
        ("RsaI", 2, 0, "GTAC"),
        ("TaqI", 1, 2, "TCGA"),
        ("MspI", 1, 2, "CCGG"),
        ("HpaII", 1, 2, "CCGG"),

        # Additional common enzymes
        ("NheI", 1, 4, "GCTAGC"),
        ("SpeI", 1, 4, "ACTAGT"),
        ("MluI", 1, 4, "ACGCGT"),
        ("BsiWI", 1, 4, "CGTACG"),
        ("AgeI", 1, 4, "ACCGGT"),
        ("BsrGI", 1, 4, "TGTACA"),
        ("PmeI", 4, 0, "GTTTAAAC"),
    ]

    enzymes = []
    for name, offset, overhang, pattern in builtin:
        if overhang == 0:
            enzyme_type = EnzymeType.BLUNT
        elif overhang > 0:
            enzyme_type = EnzymeType.FIVE_PRIME
        else:
            enzyme_type = EnzymeType.THREE_PRIME

        enzymes.append(EnzymeInfo(
            name=name,
            offset=offset,
            overhang=overhang,
            pattern=pattern,
            enzyme_type=enzyme_type,
        ))

    return enzymes


# IUPAC nucleotide codes to regex (for pattern matching)
IUPAC_TO_REGEX = {
    "A": "A", "C": "C", "G": "G", "T": "T", "U": "T",
    "R": "[AG]",      # Purine
    "Y": "[CT]",      # Pyrimidine
    "S": "[GC]",      # Strong
    "W": "[AT]",      # Weak
    "K": "[GT]",      # Keto
    "M": "[AC]",      # Amino
    "B": "[CGT]",     # Not A
    "D": "[AGT]",     # Not C
    "H": "[ACT]",     # Not G
    "V": "[ACG]",     # Not T
    "N": "[ACGT]",    # Any
}


def pattern_to_regex(pattern: str) -> str:
    """Convert IUPAC DNA pattern to regex."""
    regex_parts = []
    for char in pattern.upper():
        if char in IUPAC_TO_REGEX:
            regex_parts.append(IUPAC_TO_REGEX[char])
        else:
            regex_parts.append(char)
    return "".join(regex_parts)


def get_reverse_complement(seq: str) -> str:
    """Return the reverse complement of a DNA sequence."""
    complement_map = str.maketrans(
        "ACGTacgtRYSWKMBDHVNryswkmbdhvn",
        "TGCAtgcaYRSWMKVHDBNyrswmkvhdbn"
    )
    return seq.translate(complement_map)[::-1]
