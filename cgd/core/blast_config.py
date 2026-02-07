"""
BLAST configuration for organisms.

This module provides configuration for organisms available in the BLAST service,
including CGD internal organisms and external organisms loaded from configuration files.
"""
from __future__ import annotations

import os
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

# CGD organisms with BLAST configuration
# Format: tag -> config dict
# Note: Tags must match database file naming (e.g., genomic_C_albicans_SC5314_A22)
BLAST_ORGANISMS: Dict[str, Dict[str, Any]] = {
    # C. albicans SC5314 assemblies
    "C_albicans_SC5314_A22": {
        "full_name": "Candida albicans SC5314 (Assembly 22)",
        "trans_table": 12,  # Alternative yeast nuclear code (CTG clade)
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": "cgd_data/C_albicans_SC5314",
        "is_cgd": True,
        "assembly": "A22",
    },
    "C_albicans_SC5314_A21": {
        "full_name": "Candida albicans SC5314 (Assembly 21)",
        "trans_table": 12,
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": None,  # No JBrowse data for A21
        "is_cgd": True,
        "assembly": "A21",
    },
    "C_albicans_SC5314_A19": {
        "full_name": "Candida albicans SC5314 (Assembly 19)",
        "trans_table": 12,
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": None,  # No JBrowse data for A19
        "is_cgd": True,
        "assembly": "A19",
    },
    # C. albicans WO-1
    "C_albicans_WO-1": {
        "full_name": "Candida albicans WO-1",
        "trans_table": 12,
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": "cgd_data/C_albicans_WO-1",
        "is_cgd": True,
    },
    # C. auris strains
    "C_auris_B11221": {
        "full_name": "Candida auris B11221",
        "trans_table": 12,
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": "cgd_data/C_auris_B11221",
        "is_cgd": True,
    },
    "C_auris_B8441": {
        "full_name": "Candida auris B8441",
        "trans_table": 12,
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": "cgd_data/C_auris_B8441",
        "is_cgd": True,
    },
    # C. dubliniensis
    "C_dubliniensis_CD36": {
        "full_name": "Candida dubliniensis CD36",
        "trans_table": 12,
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": "cgd_data/C_dubliniensis_CD36",
        "is_cgd": True,
    },
    # C. glabrata
    "C_glabrata_CBS138": {
        "full_name": "Candida glabrata CBS138",
        "trans_table": 1,  # Standard genetic code (not CTG clade)
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": "cgd_data/C_glabrata_CBS138",
        "is_cgd": True,
    },
    # C. guilliermondii
    "C_guilliermondii_ATCC_6260": {
        "full_name": "Candida guilliermondii ATCC 6260",
        "trans_table": 12,
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": "cgd_data/C_guilliermondii_ATCC_6260",
        "is_cgd": True,
    },
    # C. lusitaniae strains
    "C_lusitaniae_ATCC_42720": {
        "full_name": "Candida lusitaniae ATCC 42720",
        "trans_table": 12,
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": "cgd_data/C_lusitaniae_ATCC_42720",
        "is_cgd": True,
    },
    "C_lusitaniae_CBS6936": {
        "full_name": "Candida lusitaniae CBS6936",
        "trans_table": 12,
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": "cgd_data/C_lusitaniae_CBS6936",
        "is_cgd": True,
    },
    # C. orthopsilosis
    "C_orthopsilosis_Co_90-125": {
        "full_name": "Candida orthopsilosis Co 90-125",
        "trans_table": 12,
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": "cgd_data/C_orthopsilosis_Co_90-125",
        "is_cgd": True,
    },
    # C. parapsilosis
    "C_parapsilosis_CDC317": {
        "full_name": "Candida parapsilosis CDC317",
        "trans_table": 12,
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": "cgd_data/C_parapsilosis_CDC317",
        "is_cgd": True,
    },
    # C. tropicalis
    "C_tropicalis_MYA-3404": {
        "full_name": "Candida tropicalis MYA-3404",
        "trans_table": 12,
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": "cgd_data/C_tropicalis_MYA-3404",
        "is_cgd": True,
    },
    # D. hansenii
    "D_hansenii_CBS767": {
        "full_name": "Debaryomyces hansenii CBS767",
        "trans_table": 12,
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": "cgd_data/D_hansenii_CBS767",
        "is_cgd": True,
    },
    # L. elongisporus
    "L_elongisporus_NRLL_YB-4239": {
        "full_name": "Lodderomyces elongisporus NRRL YB-4239",
        "trans_table": 12,
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": "cgd_data/L_elongisporus_NRLL_YB-4239",
        "is_cgd": True,
    },
    # S. cerevisiae (external - no CGD JBrowse)
    "S_cerevisiae_S288C": {
        "full_name": "Saccharomyces cerevisiae S288C",
        "trans_table": 1,  # Standard genetic code
        "seq_sets": ["genomic", "gene", "coding", "protein"],
        "jbrowse_data": None,  # External link to SGD
        "is_cgd": False,
    },
}

# Database prefixes for different sequence types (new naming convention)
# Pattern: {prefix}_{organism_tag} e.g., default_genomic_C_albicans_SC5314_A22
DATABASE_PREFIXES = {
    "genomic": "default_genomic_",
    "coding": "default_coding_",
    "protein": "default_protein_",
}

# Legacy database suffixes (old naming convention)
# Pattern: {organism_tag}_{suffix} e.g., C_albicans_SC5314_A22_genome
DATABASE_SUFFIXES = {
    "genomic": "_genome",
    "gene": "_ORFs",
    "coding": "_coding",
    "protein": "_protein",
}

# Database type mapping
DATABASE_TYPES = {
    "genomic": "nucleotide",
    "gene": "nucleotide",
    "coding": "nucleotide",
    "protein": "protein",
}


def load_blast_clade_conf(path: str) -> Dict[str, Dict[str, Any]]:
    """
    Parse blast_clade.conf tab-delimited configuration file.

    The Perl blast_clade.conf format has sections like:
    - TAG_TO_FULL_NAME: organism_tag -> full name
    - TAG_TO_TRANS_TABLE: organism_tag -> translation table number
    - TAG_TO_SEQ_SETS: organism_tag -> comma-separated seq sets
    - TAG_TO_JBROWSE_DATA: organism_tag -> jbrowse data path

    Args:
        path: Path to the configuration file

    Returns:
        Dictionary of organism configurations keyed by tag
    """
    if not os.path.exists(path):
        logger.warning(f"BLAST config file not found: {path}")
        return {}

    organisms: Dict[str, Dict[str, Any]] = {}
    current_section = None

    try:
        with open(path, 'r') as f:
            for line in f:
                line = line.strip()

                # Skip empty lines and comments
                if not line or line.startswith('#'):
                    continue

                # Check for section headers
                if line.startswith('[') and line.endswith(']'):
                    current_section = line[1:-1]
                    continue

                # Parse key-value pairs
                if '\t' in line:
                    parts = line.split('\t', 1)
                    if len(parts) == 2:
                        tag, value = parts[0].strip(), parts[1].strip()

                        # Initialize organism if needed
                        if tag not in organisms:
                            organisms[tag] = {
                                "full_name": tag,
                                "trans_table": 1,
                                "seq_sets": ["genomic", "gene", "coding", "protein"],
                                "jbrowse_data": None,
                                "is_cgd": False,
                            }

                        # Set value based on section
                        if current_section == "TAG_TO_FULL_NAME":
                            organisms[tag]["full_name"] = value
                        elif current_section == "TAG_TO_TRANS_TABLE":
                            try:
                                organisms[tag]["trans_table"] = int(value)
                            except ValueError:
                                pass
                        elif current_section == "TAG_TO_SEQ_SETS":
                            organisms[tag]["seq_sets"] = [
                                s.strip() for s in value.split(',')
                            ]
                        elif current_section == "TAG_TO_JBROWSE_DATA":
                            organisms[tag]["jbrowse_data"] = value

    except Exception as e:
        logger.error(f"Error parsing BLAST config file {path}: {e}")
        return {}

    return organisms


def get_all_blast_organisms(
    external_config_path: Optional[str] = None
) -> Dict[str, Dict[str, Any]]:
    """
    Get combined CGD + external organisms configuration.

    Args:
        external_config_path: Optional path to external blast_clade.conf file

    Returns:
        Dictionary of all organism configurations
    """
    # Start with CGD organisms
    all_organisms = dict(BLAST_ORGANISMS)

    # Load external organisms if config path provided
    if external_config_path:
        external = load_blast_clade_conf(external_config_path)
        # External organisms can override CGD organisms if needed
        all_organisms.update(external)

    return all_organisms


def get_organism_databases(
    tag: str,
    organisms: Optional[Dict[str, Dict[str, Any]]] = None
) -> List[Dict[str, str]]:
    """
    Get available BLAST databases for an organism.

    Args:
        tag: Organism tag
        organisms: Optional organisms dict (uses BLAST_ORGANISMS if not provided)

    Returns:
        List of database info dicts with 'name', 'type', 'display_name'
    """
    if organisms is None:
        organisms = BLAST_ORGANISMS

    config = organisms.get(tag)
    if not config:
        return []

    databases = []
    for seq_set in config.get("seq_sets", []):
        suffix = DATABASE_SUFFIXES.get(seq_set, f"_{seq_set}")
        db_type = DATABASE_TYPES.get(seq_set, "nucleotide")

        databases.append({
            "name": f"{tag}{suffix}",
            "type": db_type,
            "display_name": f"{config['full_name']} - {seq_set.capitalize()}",
            "seq_set": seq_set,
        })

    return databases


def get_organism_for_database(
    database_name: str,
    organisms: Optional[Dict[str, Dict[str, Any]]] = None
) -> Optional[Dict[str, Any]]:
    """
    Get organism configuration for a database name.

    Args:
        database_name: Name of the BLAST database
        organisms: Optional organisms dict (uses BLAST_ORGANISMS if not provided)

    Returns:
        Organism configuration dict or None if not found
    """
    if organisms is None:
        organisms = BLAST_ORGANISMS

    # Extract organism tag from database name
    tag = extract_organism_tag_from_database(database_name)
    if tag and tag in organisms:
        return {"tag": tag, **organisms[tag]}

    # Also check for exact tag match (for all_candida type databases)
    if database_name.startswith("all_"):
        return {
            "tag": "all_candida",
            "full_name": "All Candida Species",
            "trans_table": 12,
            "seq_sets": ["genomic", "gene", "coding", "protein"],
            "jbrowse_data": None,
            "is_cgd": True,
        }

    return None


def extract_organism_tag_from_database(database_name: str) -> Optional[str]:
    """
    Extract organism tag from a database name.

    Supports both naming conventions:
    - New: "default_genomic_C_albicans_SC5314_A22" -> "C_albicans_SC5314_A22"
    - Old: "C_albicans_SC5314_A22_genome" -> "C_albicans_SC5314_A22"
    - Also: "genomic_C_albicans_SC5314_A21" -> "C_albicans_SC5314_A21"

    Args:
        database_name: Name of the BLAST database

    Returns:
        Organism tag like "C_albicans_SC5314_A22" or None
    """
    # New naming convention: prefix_organism_tag
    for prefix in DATABASE_PREFIXES.values():
        if database_name.startswith(prefix):
            return database_name[len(prefix):]

    # Also handle non-default prefix pattern: genomic_C_albicans_SC5314_A21
    # Include all prefixes from DATASET_TYPE_TO_PREFIX
    for seq_type in ["genomic_", "coding_", "protein_", "orf_genomic_", "orf_coding_", "orf_trans_all_", "other_features_genomic_", "other_features_no_introns_"]:
        if database_name.startswith(seq_type):
            return database_name[len(seq_type):]

    # Legacy naming convention: organism_tag_suffix
    for suffix in DATABASE_SUFFIXES.values():
        if database_name.endswith(suffix):
            return database_name[:-len(suffix)]

    return None


# Dataset type to database prefix mapping
# Maps DatasetType enum values to database file prefixes
DATASET_TYPE_TO_PREFIX = {
    "GENOME": "genomic",
    "GENES": "orf_genomic",
    "CODING": "orf_coding",
    "PROTEIN": "orf_trans_all",
    "OTHER": "other_features_genomic",
    "OTHER_SPLICED": "other_features_no_introns",
}

# Dataset type to database type mapping
DATASET_TYPE_TO_DB_TYPE = {
    "GENOME": "nucleotide",
    "GENES": "nucleotide",
    "CODING": "nucleotide",
    "PROTEIN": "protein",
    "OTHER": "nucleotide",
    "OTHER_SPLICED": "nucleotide",
}


def build_database_name(genome_id: str, dataset_type: str) -> str:
    """
    Build a database name from genome ID and dataset type.

    Args:
        genome_id: Genome identifier (e.g., 'C_albicans_SC5314_A22')
        dataset_type: Dataset type (e.g., 'GENOME', 'CODING', 'PROTEIN')

    Returns:
        Database name (e.g., 'genomic_C_albicans_SC5314_A22')
    """
    prefix = DATASET_TYPE_TO_PREFIX.get(dataset_type, "genomic")
    return f"{prefix}_{genome_id}"


def build_database_names(
    genomes: List[str],
    dataset_type: str
) -> List[str]:
    """
    Build a list of database names from genome IDs and dataset type.

    Args:
        genomes: List of genome identifiers
        dataset_type: Dataset type

    Returns:
        List of database names
    """
    return [build_database_name(genome, dataset_type) for genome in genomes]


def get_database_type_for_dataset(dataset_type: str) -> str:
    """
    Get the database type (nucleotide or protein) for a dataset type.

    Args:
        dataset_type: Dataset type (e.g., 'GENOME', 'PROTEIN')

    Returns:
        Database type ('nucleotide' or 'protein')
    """
    return DATASET_TYPE_TO_DB_TYPE.get(dataset_type, "nucleotide")


# BLAST task information for different programs
BLAST_TASKS = {
    "blastn": [
        {
            "name": "megablast",
            "display_name": "megablast",
            "description": "Highly similar sequences (default)",
            "default_for_length": 50,  # Use for queries >= 50 bp
        },
        {
            "name": "dc-megablast",
            "display_name": "dc-megablast",
            "description": "Discontiguous megablast for more divergent sequences",
        },
        {
            "name": "blastn",
            "display_name": "blastn",
            "description": "Traditional blastn (somewhat similar sequences)",
        },
        {
            "name": "blastn-short",
            "display_name": "blastn-short",
            "description": "Short query sequences (<50 bp)",
            "default_for_length": 0,  # Use for queries < 50 bp
        },
    ],
    "blastp": [
        {
            "name": "blastp",
            "display_name": "blastp",
            "description": "Traditional blastp (default)",
            "default_for_length": 30,  # Use for queries >= 30 aa
        },
        {
            "name": "blastp-fast",
            "display_name": "blastp-fast",
            "description": "Faster, less sensitive search",
        },
        {
            "name": "blastp-short",
            "display_name": "blastp-short",
            "description": "Short query sequences (<30 aa)",
            "default_for_length": 0,  # Use for queries < 30 aa
        },
    ],
}

# Genetic code descriptions
GENETIC_CODES = {
    1: {"name": "Standard", "description": "Standard genetic code"},
    2: {"name": "Vertebrate Mitochondrial", "description": "Vertebrate mitochondrial code"},
    3: {"name": "Yeast Mitochondrial", "description": "Yeast mitochondrial code"},
    4: {"name": "Mold Mitochondrial", "description": "Mold, protozoan, and coelenterate mitochondrial code"},
    5: {"name": "Invertebrate Mitochondrial", "description": "Invertebrate mitochondrial code"},
    6: {"name": "Ciliate", "description": "Ciliate, dasycladacean, and hexamita nuclear code"},
    9: {"name": "Echinoderm Mitochondrial", "description": "Echinoderm and flatworm mitochondrial code"},
    10: {"name": "Euplotid", "description": "Euplotid nuclear code"},
    11: {"name": "Bacterial", "description": "Bacterial, archaeal, and plant plastid code"},
    12: {"name": "Yeast Nuclear", "description": "Alternative yeast nuclear code (CTG clade)"},
    13: {"name": "Ascidian Mitochondrial", "description": "Ascidian mitochondrial code"},
    14: {"name": "Flatworm Mitochondrial", "description": "Alternative flatworm mitochondrial code"},
    15: {"name": "Blepharisma", "description": "Blepharisma nuclear code"},
    16: {"name": "Chlorophycean Mitochondrial", "description": "Chlorophycean mitochondrial code"},
    21: {"name": "Trematode Mitochondrial", "description": "Trematode mitochondrial code"},
    22: {"name": "Scenedesmus Mitochondrial", "description": "Scenedesmus obliquus mitochondrial code"},
    23: {"name": "Thraustochytrium Mitochondrial", "description": "Thraustochytrium mitochondrial code"},
}
