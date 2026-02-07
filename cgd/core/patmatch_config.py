"""
PatMatch Configuration - Dataset and binary tool settings.
"""
import os
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum


# Binary tool paths
NRGREP_BINARY = os.environ.get("NRGREP_BINARY", "/data/bin/nrgrep_coords")
SCAN_FOR_MATCHES_BINARY = os.environ.get("SCAN_FOR_MATCHES_BINARY", "/data/bin/scan_for_matches")

# Data directories
FASTA_FILES_DIR = os.environ.get("FASTA_FILES_DIR", "/data/fasta_files")
RESTRICTION_ENZYMES_DIR = os.environ.get("RESTRICTION_ENZYMES_DIR", "/data/restriction_enzymes")

# Index generation script (for nrgrep)
INDEX_GENERATOR_SCRIPT = os.environ.get(
    "INDEX_GENERATOR_SCRIPT",
    "/data/bin/generate_sequence_index.pl"
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


def convert_pattern_for_nrgrep(
    pattern: str,
    pattern_type: PatternType,
    mismatches: int = 0,
    insertions: int = 0,
    deletions: int = 0,
) -> str:
    """
    Convert a user pattern to nrgrep format.

    nrgrep supports fuzzy matching with syntax like:
    - Simple pattern: ATCG
    - With mismatches: pattern#k (where k is max errors)

    For IUPAC codes, we expand them to character classes.
    """
    pattern = pattern.upper().strip()
    iupac_map = IUPAC_DNA if pattern_type == PatternType.DNA else IUPAC_PROTEIN

    # Expand IUPAC codes
    expanded = []
    for char in pattern:
        if char in iupac_map:
            expanded.append(iupac_map[char])
        elif char == '.':
            # Wildcard
            expanded.append('.' if pattern_type == PatternType.PROTEIN else '[ACGT]')
        else:
            expanded.append(char)

    nrgrep_pattern = ''.join(expanded)

    # Note: nrgrep_coords handles mismatches via command-line options
    # The pattern itself doesn't need modification for fuzzy matching

    return nrgrep_pattern


def get_reverse_complement(seq: str) -> str:
    """Return the reverse complement of a DNA sequence."""
    complement = str.maketrans('ACGTacgt', 'TGCAtgca')
    return seq.translate(complement)[::-1]
