"""
ID formatting utilities.

This module provides functions for formatting and normalizing
various identifiers used in CGD (GO IDs, chromosome names, etc.).
"""

import re
from typing import Optional

# Roman numeral mappings
ROMAN_TO_ARABIC = {
    "I": 1, "II": 2, "III": 3, "IV": 4, "V": 5,
    "VI": 6, "VII": 7, "VIII": 8, "IX": 9, "X": 10,
    "XI": 11, "XII": 12, "XIII": 13, "XIV": 14, "XV": 15,
    "XVI": 16, "XVII": 17, "XVIII": 18, "XIX": 19, "XX": 20,
}

ARABIC_TO_ROMAN = {v: k for k, v in ROMAN_TO_ARABIC.items()}

# Chromosome name variations
MITO_NAMES = {"mito", "mt", "mitochondrion", "mitochondrial", "chrmt", "chrmito", "17"}


def format_goid(goid: str | int, prefix: bool = True) -> str:
    """
    Format a GO ID with leading zeros.

    Args:
        goid: GO ID (string or integer)
        prefix: Include "GO:" prefix (default: True)

    Returns:
        Formatted GO ID string

    Example:
        >>> format_goid(1234)
        "GO:0001234"
        >>> format_goid("GO:1234", prefix=False)
        "0001234"
    """
    # Remove existing prefix if present
    goid_str = str(goid).replace("GO:", "").strip()

    # Remove leading zeros and re-pad
    try:
        goid_int = int(goid_str)
        padded = f"{goid_int:07d}"
    except ValueError:
        padded = goid_str.zfill(7)

    if prefix:
        return f"GO:{padded}"
    return padded


def parse_goid(goid: str) -> int:
    """
    Parse a GO ID string to integer.

    Args:
        goid: GO ID string (with or without "GO:" prefix)

    Returns:
        Integer GO ID

    Example:
        >>> parse_goid("GO:0001234")
        1234
    """
    goid_str = str(goid).replace("GO:", "").strip()
    return int(goid_str)


def normalize_chromosome_name(name: str) -> str:
    """
    Normalize chromosome name to standard format.

    Converts various chromosome name formats to a standardized form.

    Args:
        name: Chromosome name/number in various formats

    Returns:
        Normalized chromosome name

    Example:
        >>> normalize_chromosome_name("chrI")
        "1"
        >>> normalize_chromosome_name("chromosome 3")
        "3"
        >>> normalize_chromosome_name("Mito")
        "Mito"
    """
    name_lower = str(name).lower().strip()

    # Handle mitochondrial
    if name_lower in MITO_NAMES:
        return "Mito"

    # Handle 2-micron
    if "micron" in name_lower or name_lower == "2u":
        return "2-micron"

    # Remove common prefixes
    cleaned = re.sub(r"^(chr|chromosome)\s*", "", name, flags=re.IGNORECASE)
    cleaned = cleaned.strip()

    # Check if Roman numeral
    cleaned_upper = cleaned.upper()
    if cleaned_upper in ROMAN_TO_ARABIC:
        return str(ROMAN_TO_ARABIC[cleaned_upper])

    # Try to parse as number
    try:
        return str(int(cleaned))
    except ValueError:
        pass

    return cleaned


def chromosome_to_roman(num: int | str) -> str:
    """
    Convert chromosome number to Roman numeral.

    Args:
        num: Chromosome number

    Returns:
        Roman numeral string

    Example:
        >>> chromosome_to_roman(3)
        "III"
    """
    try:
        n = int(num)
        if n == 17:
            return "Mito"
        return ARABIC_TO_ROMAN.get(n, str(n))
    except ValueError:
        return str(num)


def chromosome_to_arabic(roman: str) -> int:
    """
    Convert Roman numeral to chromosome number.

    Args:
        roman: Roman numeral string

    Returns:
        Integer chromosome number
    """
    roman_upper = roman.upper().strip()

    if roman_upper in ("MITO", "MT"):
        return 17

    if roman_upper in ROMAN_TO_ARABIC:
        return ROMAN_TO_ARABIC[roman_upper]

    try:
        return int(roman)
    except ValueError:
        raise ValueError(f"Invalid chromosome: {roman}")


def format_sgdid(sgdid: str, prefix: Optional[str] = None) -> str:
    """
    Format an SGD/CGD ID.

    Args:
        sgdid: The database ID
        prefix: Optional prefix to add (e.g., "CGD", "SGD")

    Returns:
        Formatted ID string
    """
    sgdid = str(sgdid).strip()

    # Remove existing prefix if present
    for known_prefix in ("CGD:", "SGD:", "CAL:"):
        if sgdid.upper().startswith(known_prefix.upper()):
            sgdid = sgdid[len(known_prefix):]
            break

    if prefix:
        return f"{prefix}:{sgdid}"
    return sgdid


def parse_dbxref(dbxref: str) -> tuple[str, str]:
    """
    Parse a database cross-reference.

    Args:
        dbxref: Database cross-reference in "SOURCE:ID" format

    Returns:
        Tuple of (source, id)

    Example:
        >>> parse_dbxref("UniProt:P12345")
        ("UniProt", "P12345")
    """
    if ":" in dbxref:
        parts = dbxref.split(":", 1)
        return (parts[0].strip(), parts[1].strip())
    return ("", dbxref.strip())


def format_dbxref(source: str, identifier: str) -> str:
    """
    Format a database cross-reference.

    Args:
        source: Database source
        identifier: Database identifier

    Returns:
        Formatted dbxref string
    """
    return f"{source}:{identifier}"


def format_pubmed_id(pmid: str | int, prefix: bool = True) -> str:
    """
    Format a PubMed ID.

    Args:
        pmid: PubMed ID
        prefix: Include "PMID:" prefix

    Returns:
        Formatted PubMed ID
    """
    pmid_str = str(pmid).replace("PMID:", "").strip()
    if prefix:
        return f"PMID:{pmid_str}"
    return pmid_str


def is_valid_goid(goid: str) -> bool:
    """
    Check if a GO ID is valid format.

    Args:
        goid: GO ID to validate

    Returns:
        True if valid format
    """
    pattern = r"^(GO:)?\d{1,7}$"
    return bool(re.match(pattern, str(goid).strip()))


def is_valid_feature_name(name: str) -> bool:
    """
    Check if a feature name follows standard naming conventions.

    Standard names are like: YAL001C, YBR002W, etc.

    Args:
        name: Feature name to validate

    Returns:
        True if valid format
    """
    # Standard ORF naming pattern
    pattern = r"^Y[A-P][LR]\d{3}[WC](-[A-Z])?$"
    return bool(re.match(pattern, str(name).upper().strip()))
