"""
PatMatch Configuration - Dataset and binary tool settings.
"""
import os
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum


# Base data directory (Dev: /data, Prod: /data/tools)
CGD_DATA_DIR = os.environ.get("CGD_DATA_DIR", "/data")

# Binary tool paths
NRGREP_BINARY = os.environ.get("NRGREP_BINARY", f"{CGD_DATA_DIR}/bin/nrgrep_coords")
SCAN_FOR_MATCHES_BINARY = os.environ.get("SCAN_FOR_MATCHES_BINARY", f"{CGD_DATA_DIR}/bin/scan_for_matches")

# Data directories
FASTA_FILES_DIR = os.environ.get("FASTA_FILES_DIR", f"{CGD_DATA_DIR}/fasta_files")
RESTRICTION_ENZYMES_DIR = os.environ.get("RESTRICTION_ENZYMES_DIR", f"{CGD_DATA_DIR}/restriction_enzymes")

# Index generation script (for nrgrep)
INDEX_GENERATOR_SCRIPT = os.environ.get(
    "INDEX_GENERATOR_SCRIPT",
    f"{CGD_DATA_DIR}/bin/generate_sequence_index.pl"
)


class PatternType(str, Enum):
    """Type of pattern being searched."""
    DNA = "dna"
    PROTEIN = "protein"


@dataclass
class DatasetConfig:
    """Configuration for a sequence dataset."""
    name: str                    # Internal name (e.g., "genomic_C_albicans_SC5314_A22")
    display_name: str            # Display name for UI
    description: str             # Description text
    pattern_type: PatternType    # DNA or protein
    fasta_file: str              # Path to FASTA file
    organism: str                # Organism name
    assembly: Optional[str]      # Assembly version (e.g., "A22")


# Dataset configurations for all organisms and assemblies
# Maps dataset key to DatasetConfig
PATMATCH_DATASETS: Dict[str, DatasetConfig] = {}


def _register_organism_datasets(
    organism_tag: str,
    organism_name: str,
    assemblies: List[str],
):
    """Register all datasets for an organism."""
    base_dir = os.path.join(FASTA_FILES_DIR, organism_tag)

    for assembly in assemblies:
        assembly_suffix = f"_{assembly}" if assembly else ""
        file_suffix = f"_{organism_tag}{assembly_suffix}"

        # Genomic chromosomes
        key = f"genomic_{organism_tag}{assembly_suffix}"
        PATMATCH_DATASETS[key] = DatasetConfig(
            name=key,
            display_name=f"{organism_name} {assembly} - Chromosomes/Contigs",
            description=f"Complete chromosome sequences ({assembly})",
            pattern_type=PatternType.DNA,
            fasta_file=os.path.join(base_dir, f"genomic{file_suffix}.fasta"),
            organism=organism_name,
            assembly=assembly,
        )

        # ORF genomic (with introns)
        key = f"orf_genomic_{organism_tag}{assembly_suffix}"
        PATMATCH_DATASETS[key] = DatasetConfig(
            name=key,
            display_name=f"{organism_name} {assembly} - ORF Genomic DNA",
            description=f"ORF sequences including introns ({assembly})",
            pattern_type=PatternType.DNA,
            fasta_file=os.path.join(base_dir, f"orf_genomic{file_suffix}.fasta"),
            organism=organism_name,
            assembly=assembly,
        )

        # ORF coding (exons only)
        key = f"orf_coding_{organism_tag}{assembly_suffix}"
        PATMATCH_DATASETS[key] = DatasetConfig(
            name=key,
            display_name=f"{organism_name} {assembly} - ORF Coding DNA",
            description=f"ORF coding sequences, exons only ({assembly})",
            pattern_type=PatternType.DNA,
            fasta_file=os.path.join(base_dir, f"orf_coding{file_suffix}.fasta"),
            organism=organism_name,
            assembly=assembly,
        )

        # ORF genomic with 1kb flanking
        key = f"orf_genomic_1000_{organism_tag}{assembly_suffix}"
        PATMATCH_DATASETS[key] = DatasetConfig(
            name=key,
            display_name=f"{organism_name} {assembly} - ORF Genomic +/- 1kb",
            description=f"ORF sequences with 1kb flanking regions ({assembly})",
            pattern_type=PatternType.DNA,
            fasta_file=os.path.join(base_dir, f"orf_genomic_1000{file_suffix}.fasta"),
            organism=organism_name,
            assembly=assembly,
        )

        # ORF protein translations
        key = f"orf_trans_all_{organism_tag}{assembly_suffix}"
        PATMATCH_DATASETS[key] = DatasetConfig(
            name=key,
            display_name=f"{organism_name} {assembly} - Protein Sequences",
            description=f"Translated ORF proteins ({assembly})",
            pattern_type=PatternType.PROTEIN,
            fasta_file=os.path.join(base_dir, f"orf_trans_all{file_suffix}.fasta"),
            organism=organism_name,
            assembly=assembly,
        )

        # Intergenic/not-feature regions
        key = f"not_feature_{organism_tag}{assembly_suffix}"
        PATMATCH_DATASETS[key] = DatasetConfig(
            name=key,
            display_name=f"{organism_name} {assembly} - Intergenic Regions",
            description=f"Sequences between genes ({assembly})",
            pattern_type=PatternType.DNA,
            fasta_file=os.path.join(base_dir, f"not_feature{file_suffix}.fasta"),
            organism=organism_name,
            assembly=assembly,
        )

        # Other features genomic
        key = f"other_features_genomic_{organism_tag}{assembly_suffix}"
        PATMATCH_DATASETS[key] = DatasetConfig(
            name=key,
            display_name=f"{organism_name} {assembly} - Other Features (genomic)",
            description=f"Non-ORF features genomic sequences ({assembly})",
            pattern_type=PatternType.DNA,
            fasta_file=os.path.join(base_dir, f"other_features_genomic{file_suffix}.fasta"),
            organism=organism_name,
            assembly=assembly,
        )

        # Other features no introns
        key = f"other_features_no_introns_{organism_tag}{assembly_suffix}"
        PATMATCH_DATASETS[key] = DatasetConfig(
            name=key,
            display_name=f"{organism_name} {assembly} - Other Features (spliced)",
            description=f"Non-ORF features, excluding introns ({assembly})",
            pattern_type=PatternType.DNA,
            fasta_file=os.path.join(base_dir, f"other_features_no_introns{file_suffix}.fasta"),
            organism=organism_name,
            assembly=assembly,
        )


# Register C. albicans SC5314 datasets (multiple assemblies)
_register_organism_datasets(
    "C_albicans_SC5314",
    "C. albicans SC5314",
    ["A22", "A21", "A19"]
)

# Register C. glabrata CBS138
_register_organism_datasets(
    "C_glabrata_CBS138",
    "C. glabrata CBS138",
    [""]  # No assembly suffix for C. glabrata
)

# Register C. auris B8441
_register_organism_datasets(
    "C_auris_B8441",
    "C. auris B8441",
    [""]
)

# Register C. dubliniensis CD36
_register_organism_datasets(
    "C_dubliniensis_CD36",
    "C. dubliniensis CD36",
    [""]
)

# Register C. parapsilosis CDC317
_register_organism_datasets(
    "C_parapsilosis_CDC317",
    "C. parapsilosis CDC317",
    [""]
)

# Register C. tropicalis MYA-3404
_register_organism_datasets(
    "C_tropicalis",
    "C. tropicalis MYA-3404",
    [""]
)


def get_available_datasets(pattern_type: Optional[PatternType] = None) -> List[DatasetConfig]:
    """
    Get list of available datasets, optionally filtered by pattern type.
    Only returns datasets whose FASTA files exist.
    """
    datasets = []
    for config in PATMATCH_DATASETS.values():
        # Filter by pattern type if specified
        if pattern_type and config.pattern_type != pattern_type:
            continue

        # Check if FASTA file exists
        if os.path.exists(config.fasta_file):
            datasets.append(config)

    # Sort by organism, then assembly, then name
    datasets.sort(key=lambda d: (d.organism, d.assembly or "", d.name))
    return datasets


def get_dataset_config(dataset_key: str) -> Optional[DatasetConfig]:
    """Get configuration for a specific dataset."""
    return PATMATCH_DATASETS.get(dataset_key)


# IUPAC nucleotide codes for pattern conversion
IUPAC_DNA = {
    'A': 'A', 'C': 'C', 'G': 'G', 'T': 'T', 'U': 'T',
    'R': '[AG]',      # Purine
    'Y': '[CT]',      # Pyrimidine
    'S': '[GC]',      # Strong
    'W': '[AT]',      # Weak
    'K': '[GT]',      # Keto
    'M': '[AC]',      # Amino
    'B': '[CGT]',     # Not A
    'D': '[AGT]',     # Not C
    'H': '[ACT]',     # Not G
    'V': '[ACG]',     # Not T
    'N': '[ACGT]',    # Any
}

# IUPAC protein codes
IUPAC_PROTEIN = {
    'A': 'A', 'C': 'C', 'D': 'D', 'E': 'E', 'F': 'F',
    'G': 'G', 'H': 'H', 'I': 'I', 'K': 'K', 'L': 'L',
    'M': 'M', 'N': 'N', 'P': 'P', 'Q': 'Q', 'R': 'R',
    'S': 'S', 'T': 'T', 'V': 'V', 'W': 'W', 'Y': 'Y',
    'B': '[DN]',      # Aspartic acid or Asparagine
    'Z': '[EQ]',      # Glutamic acid or Glutamine
    'X': '.',         # Any amino acid
    '*': '\\*',       # Stop codon
}


def _convert_repetitions(pattern: str) -> str:
    """
    Convert PatMatch repetition syntax to nrgrep/regex syntax.

    PatMatch syntax:
    - {m}    -> exactly m times
    - {m,n}  -> m to n times
    - {m,}   -> m or more times
    - {,n}   -> 0 to n times

    Examples:
    - A{2}     -> AA
    - A{0,1}   -> A?
    - A{2,3}   -> AAA?
    - A{2,}    -> AAA*
    - [ST]{0,1} -> [ST]?
    """
    import re

    if '{' not in pattern:
        return pattern

    result = []
    i = 0
    while i < len(pattern):
        char = pattern[i]

        # Check if we have a repetition coming up
        if i + 1 < len(pattern) and pattern[i + 1] == '{':
            # Find the pattern to repeat
            if char == ']':
                # Find the opening bracket
                bracket_start = i
                while bracket_start > 0 and pattern[bracket_start] != '[':
                    bracket_start -= 1
                repeat_pattern = pattern[bracket_start:i + 1]
                # Remove the bracket pattern from result (already added chars, excluding ']')
                # The ']' hasn't been added yet since we entered this block first
                result = result[:-(i - bracket_start)]
            elif char == ')':
                # Find the opening parenthesis
                paren_start = i
                depth = 1
                while paren_start > 0 and depth > 0:
                    paren_start -= 1
                    if pattern[paren_start] == ')':
                        depth += 1
                    elif pattern[paren_start] == '(':
                        depth -= 1
                repeat_pattern = pattern[paren_start:i + 1]
                # Remove the paren pattern from result (already added chars, excluding ')')
                result = result[:-(i - paren_start)]
            else:
                # Single character
                repeat_pattern = char
                # Don't add to result yet

            # Parse the repetition info
            i += 2  # Skip past '{'
            rep_end = pattern.find('}', i)
            if rep_end == -1:
                # Malformed, just add the character
                result.append(repeat_pattern)
                continue

            rep_info = pattern[i:rep_end]
            i = rep_end + 1

            # Parse lower and upper bounds
            lower = 0
            upper = 0

            if ',' in rep_info:
                parts = rep_info.split(',')
                if rep_info.startswith(','):
                    # {,n} format
                    lower = 0
                    upper = int(parts[1]) if parts[1] else 0
                elif rep_info.endswith(','):
                    # {m,} format
                    lower = int(parts[0]) if parts[0] else 0
                    upper = -1  # Infinite
                else:
                    # {m,n} format
                    lower = int(parts[0]) if parts[0] else 0
                    upper = int(parts[1]) if parts[1] else 0
            else:
                # {m} format - exact
                lower = int(rep_info)
                upper = lower

            # Build the nrgrep pattern
            # Add the pattern 'lower' times
            for _ in range(lower):
                result.append(repeat_pattern)

            # Add optional repeats
            if upper == -1:
                # Infinite - add pattern with *
                result.append(repeat_pattern + '*')
            elif upper > lower:
                # Add (upper - lower) optional copies
                for _ in range(upper - lower):
                    result.append(repeat_pattern + '?')

        else:
            result.append(char)
            i += 1

    return ''.join(result)


def convert_pattern_for_nrgrep(
    pattern: str,
    pattern_type: PatternType,
    mismatches: int = 0,
    insertions: int = 0,
    deletions: int = 0,
) -> str:
    """
    Convert a user pattern to nrgrep format.

    Supports:
    - IUPAC codes: R, Y, S, W, K, M, B, D, H, V, N (DNA) or B, Z, X (protein)
    - Character classes: [ST], [ACG], etc.
    - Repetition patterns: {m}, {m,n}, {m,}, {,n}
    - Wildcards: X (protein) or N (DNA) for any character

    Examples:
    - [ST]{0,1}QPKA -> [ST]?QPKA (optional S or T)
    - A{2,3}TG -> AAATG or AAA?TG
    """
    pattern = pattern.upper().strip()

    # First, convert repetition patterns to regex
    pattern = _convert_repetitions(pattern)

    iupac_map = IUPAC_DNA if pattern_type == PatternType.DNA else IUPAC_PROTEIN

    # Expand IUPAC codes (but preserve character classes and regex syntax)
    expanded = []
    in_bracket = False
    i = 0
    while i < len(pattern):
        char = pattern[i]

        if char == '[':
            in_bracket = True
            expanded.append(char)
        elif char == ']':
            in_bracket = False
            expanded.append(char)
        elif in_bracket:
            # Inside brackets, keep characters as-is
            expanded.append(char)
        elif char in '?*+()':
            # Regex quantifiers and grouping - keep as-is (check BEFORE IUPAC)
            expanded.append(char)
        elif char == '.':
            # Wildcard
            expanded.append('.' if pattern_type == PatternType.PROTEIN else '[ACGT]')
        elif char in iupac_map:
            expanded.append(iupac_map[char])
        else:
            expanded.append(char)

        i += 1

    nrgrep_pattern = ''.join(expanded)

    # Note: nrgrep_coords handles mismatches via command-line options
    # The pattern itself doesn't need modification for fuzzy matching

    return nrgrep_pattern


def get_reverse_complement(seq: str) -> str:
    """Return the reverse complement of a DNA sequence."""
    complement = str.maketrans('ACGTacgt', 'TGCAtgca')
    return seq.translate(complement)[::-1]
