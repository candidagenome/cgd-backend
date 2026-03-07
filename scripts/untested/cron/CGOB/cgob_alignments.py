#!/usr/bin/env python3
"""
Create ortholog alignments for homology groups.

This script creates sequence files, multiple sequence alignments, and
phylogenetic trees for ortholog homology groups from CGOB data.

Based on CGOB_alignments.pl.

Usage:
    python cgob_alignments.py
    python cgob_alignments.py --debug
    python cgob_alignments.py --debug --rounds 10

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
"""

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
MUSCLE = os.getenv("MUSCLE", "muscle")
SEMPHY = os.getenv("SEMPHY", "semphy")
BLASTDBCMD = os.getenv("BLASTDBCMD", "blastdbcmd")

# CGOB configuration
CGOB_DATA_DIR = DATA_DIR / "CGOB"
CGOB_BLAST_DIR = CGOB_DATA_DIR / "blastdb"
CGOB_ALIGN_DIR = CGOB_DATA_DIR / "alignments"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# File types for ortholog groups
FILE_TYPES = [
    "_protein.fasta",
    "_coding.fasta",
    "_gene.fasta",
    "_g1000.fasta",
    "_protein_align.fasta",
    "_tree_unrooted.par",
]

# Strain order and prefixes (same as prepare_cgd_orthologs.py)
STRAIN_ORDER = [
    "C_albicans_SC5314",
    "C_dubliniensis_CD36",
    "C_tropicalis_MYA3404",
    "C_parapsilosis_CDC317",
    "L_elongisporus_NRRL_YB4239",
    "C_guilliermondii_ATCC6260",
    "C_lusitaniae_ATCC42720",
    "D_hansenii_CBS767",
    "C_glabrata_CBS138",
    "S_cerevisiae",
]

# Strain prefixes for ID matching
STRAIN_PREFIXES = {
    "orf19": "C_albicans_SC5314",
    "ORF19": "C_albicans_SC5314",
    "CAGL": "C_glabrata_CBS138",
    "CORT": "C_tropicalis_MYA3404",
    "Cd36": "C_dubliniensis_CD36",
    "CD36": "C_dubliniensis_CD36",
    "CPAG": "C_parapsilosis_CDC317",
    "LELG": "L_elongisporus_NRRL_YB4239",
    "PGUG": "C_guilliermondii_ATCC6260",
    "CLUG": "C_lusitaniae_ATCC42720",
    "DEHA": "D_hansenii_CBS767",
    "Y": "S_cerevisiae",
    "S": "S_cerevisiae",
}

# "Alien" strains (not in our database)
ALIEN_STRAINS = {"S_cerevisiae"}

# Sequence types
SEQ_TYPES = ["protein", "coding", "gene", "g1000"]


def get_strain_from_prefix(seq_id: str) -> str | None:
    """Determine strain from sequence ID prefix."""
    for prefix, strain in STRAIN_PREFIXES.items():
        if seq_id.startswith(prefix):
            return strain
    return None


def is_alien_strain(strain: str) -> bool:
    """Check if strain is an alien strain (not in our database)."""
    return strain in ALIEN_STRAINS


def is_db_strain(strain: str) -> bool:
    """Check if strain is a database strain."""
    return strain in STRAIN_ORDER and strain not in ALIEN_STRAINS


def get_blast_db(strain: str, seq_type: str) -> Path:
    """Get path to BLAST database for a strain and sequence type."""
    return CGOB_BLAST_DIR / strain / f"{strain}_{seq_type}"


def get_alignment_dir(dbxref_id: str) -> Path:
    """Get alignment directory for a feature based on its dbxref_id."""
    # Create subdirectory based on last 3 digits of dbxref_id
    subdir = dbxref_id[-3:] if len(dbxref_id) >= 3 else dbxref_id
    return CGOB_ALIGN_DIR / subdir / dbxref_id


def get_sequence_from_blastdb(
    blast_db: Path, seq_id: str
) -> tuple[str, str] | None:
    """
    Get sequence from BLAST database.

    Returns (id, sequence) tuple or None if not found.
    """
    try:
        cmd = [
            BLASTDBCMD,
            "-db", str(blast_db),
            "-entry", seq_id,
            "-outfmt", "%f",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            return None

        # Parse FASTA output
        lines = result.stdout.strip().split("\n")
        if not lines or not lines[0].startswith(">"):
            return None

        header = lines[0][1:].split()[0]
        sequence = "".join(lines[1:])

        return (header, sequence)

    except Exception:
        return None


def collect_dbids(session) -> dict[str, str]:
    """
    Collect feature_name to dbxref_id mappings from database.

    Returns dict mapping feature_name (ORF) to dbxref_id.
    """
    query = text(f"""
        SELECT feature_name, dbxref_id
        FROM {DB_SCHEMA}.feature
        WHERE feature_type IN ('ORF', 'pseudogene')
        AND dbxref_id IS NOT NULL
    """)

    dbid_for_orf: dict[str, str] = {}

    for row in session.execute(query).fetchall():
        feature_name, dbxref_id = row
        if feature_name and dbxref_id:
            dbid_for_orf[feature_name] = str(dbxref_id)

    return dbid_for_orf


def get_default_proteins(session) -> tuple[dict[str, str], dict[str, str]]:
    """
    Get default protein features for C. albicans SC5314.

    Returns:
        default_feat: mapping of feature_name to default feature
        orf_for_allele: mapping of allele to parent ORF
    """
    default_feat: dict[str, str] = {}
    orf_for_allele: dict[str, str] = {}

    # Get default alleles for SC5314
    query = text(f"""
        SELECT f.feature_name, f.parent_feature_name
        FROM {DB_SCHEMA}.feature f
        WHERE f.organism_abbrev = 'C_albicans_SC5314'
        AND f.feature_type IN ('ORF', 'pseudogene', 'allele')
    """)

    for row in session.execute(query).fetchall():
        feature_name, parent = row
        if feature_name:
            default_feat[feature_name] = feature_name
            if parent:
                orf_for_allele[feature_name] = parent

    return default_feat, orf_for_allele


def load_homology_groups(clusters_file: Path) -> list[list[str]]:
    """
    Load homology groups from CGD clusters file.

    Returns list of lists, where each inner list contains ORF IDs.
    """
    groups = []

    if not clusters_file.exists():
        return groups

    with open(clusters_file) as f:
        # Skip header line
        next(f, None)

        for line in f:
            line = line.strip()
            if not line:
                continue

            orfs = []
            for part in line.split("\t"):
                if part and part != "---":
                    orfs.append(part)

            if len(orfs) >= 2:
                groups.append(orfs)

    return groups


def write_sequence_files(
    homology_group: list[str],
    dbxref_id: str,
    directory: Path,
    dbid_for_orf: dict[str, str],
    default_feat_for_orf: dict[str, str],
    log_text: list[str],
) -> int:
    """
    Write sequence files for a homology group.

    Returns count of valid sequences written.
    """
    valid_count = 0

    prot_file = directory / f"{dbxref_id}_protein.fasta"
    cds_file = directory / f"{dbxref_id}_coding.fasta"
    gene_file = directory / f"{dbxref_id}_gene.fasta"
    g1000_file = directory / f"{dbxref_id}_g1000.fasta"

    prot_seqs = []
    cds_seqs = []
    gene_seqs = []
    g1000_seqs = []

    for orf in homology_group:
        strain = get_strain_from_prefix(orf)

        if not strain:
            log_text.append(f"No strain identified for {orf}\n")
            continue

        if is_alien_strain(strain):
            continue

        # Get default feature ID (for C. albicans alleles)
        default_id = default_feat_for_orf.get(orf, orf)

        # Get sequences from BLAST databases
        prot_db = get_blast_db(strain, "protein")
        cds_db = get_blast_db(strain, "coding")
        gene_db = get_blast_db(strain, "gene")
        g1000_db = get_blast_db(strain, "g1000")

        prot_seq = get_sequence_from_blastdb(prot_db, default_id)
        if not prot_seq:
            log_text.append(
                f"   WARNING: Could not get protein sequence for {default_id}\n"
            )
            continue

        cds_seq = get_sequence_from_blastdb(cds_db, default_id)
        if not cds_seq:
            log_text.append(
                f"   WARNING: Could not get coding sequence for {default_id}\n"
            )
            continue

        gene_seq = get_sequence_from_blastdb(gene_db, default_id)
        if not gene_seq:
            log_text.append(
                f"   WARNING: Could not get gene sequence for {default_id}\n"
            )
            continue

        g1000_seq = get_sequence_from_blastdb(g1000_db, default_id)
        if not g1000_seq:
            log_text.append(
                f"   WARNING: Could not get g1000 sequence for {default_id}\n"
            )
            continue

        valid_count += 1

        prot_seqs.append((prot_seq[0], prot_seq[1]))
        cds_seqs.append((cds_seq[0], cds_seq[1]))
        gene_seqs.append((gene_seq[0], gene_seq[1]))
        g1000_seqs.append((g1000_seq[0], g1000_seq[1]))

    # Write sequence files
    if valid_count > 0:
        write_fasta(prot_file, prot_seqs)
        write_fasta(cds_file, cds_seqs)
        write_fasta(gene_file, gene_seqs)
        write_fasta(g1000_file, g1000_seqs)

    return valid_count


def write_fasta(filepath: Path, sequences: list[tuple[str, str]]) -> None:
    """Write sequences to FASTA file."""
    with open(filepath, "w") as f:
        for seq_id, sequence in sequences:
            f.write(f">{seq_id}\n")
            # Write sequence in 60-character lines
            for i in range(0, len(sequence), 60):
                f.write(sequence[i:i + 60] + "\n")


def run_muscle_alignment(input_file: Path, output_file: Path) -> bool:
    """Run MUSCLE multiple sequence alignment."""
    try:
        cmd = [
            MUSCLE,
            "-in", str(input_file),
            "-out", str(output_file),
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode == 0

    except Exception as e:
        logger.error(f"MUSCLE error: {e}")
        return False


def run_semphy_tree(input_file: Path, output_file: Path) -> bool:
    """Run SEMPHY phylogenetic tree construction."""
    try:
        cmd = [
            SEMPHY,
            "-s", str(input_file),
            "-o", str(output_file),
            "-a", "protein",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode == 0

    except Exception as e:
        logger.error(f"SEMPHY error: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create ortholog alignments for homology groups"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=4,
        help="Number of rounds to process in debug mode (default: 4)",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    # Set up log file
    log_file = LOG_DIR / "ortholog_alignment.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    log_text: list[str] = [f"Program {__file__}: Starting {datetime.now()}\n\n"]

    # Load homology groups
    cgd_clusters = CGOB_DATA_DIR / "cgd_clusters.tab"

    if not cgd_clusters.exists():
        error_msg = f"CGD clusters file not found: {cgd_clusters}"
        log_text.append(f"ERROR: {error_msg}\n")
        logger.error(error_msg)
        with open(log_file, "w") as f:
            f.writelines(log_text)
        return 1

    logger.info(f"Loading homology groups from {cgd_clusters}")
    homology_groups = load_homology_groups(cgd_clusters)
    logger.info(f"Loaded {len(homology_groups)} homology groups")

    try:
        with SessionLocal() as session:
            # Collect DBIDs for all features
            logger.info("Collecting feature DBIDs...")
            dbid_for_orf = collect_dbids(session)
            logger.info(f"Found {len(dbid_for_orf)} feature DBIDs")

            # Get default proteins for C. albicans
            logger.info("Getting default proteins for C. albicans...")
            default_feat, orf_for_allele = get_default_proteins(session)

            # Build default feature mapping
            default_feat_for_orf: dict[str, str] = {}
            for default in default_feat:
                orf = orf_for_allele.get(default, default)
                default_feat_for_orf[orf] = default

            logger.info(f"Found {len(default_feat_for_orf)} default features")

    except Exception as e:
        log_text.append(f"ERROR: Database error: {e}\n")
        logger.error(f"Database error: {e}")
        with open(log_file, "w") as f:
            f.writelines(log_text)
        return 1

    # Track which DBIDs have been used
    dbids_used: set[str] = set()

    # Process homology groups
    count = 0
    for hg_idx, homology_group in enumerate(homology_groups):
        ref_dbid = ""
        ref_dir: Path | None = None
        tot_seqs = 0

        for orf in homology_group:
            strain = get_strain_from_prefix(orf)

            if not strain or not is_db_strain(strain):
                continue

            if orf not in dbid_for_orf:
                log_text.append(
                    f"   WARNING: Could not determine DBID for ORF {orf}\n"
                )
                continue

            dbid = dbid_for_orf[orf]
            dbids_used.add(dbid)

            if not ref_dbid:
                ref_dbid = dbid

            # Get alignment directory
            directory = get_alignment_dir(dbid)

            # Create directory if needed
            if not directory.exists():
                directory.mkdir(parents=True, exist_ok=True)

            if ref_dbid == dbid:
                ref_dir = directory

            # Remove existing files
            for file_type in FILE_TYPES:
                filepath = directory / f"{dbid}{file_type}"
                if filepath.exists() or filepath.is_symlink():
                    filepath.unlink()

            if dbid == ref_dbid:
                # Write sequence files for reference feature
                num_seqs = write_sequence_files(
                    homology_group,
                    dbid,
                    directory,
                    dbid_for_orf,
                    default_feat_for_orf,
                    log_text,
                )

                if num_seqs < 1:
                    log_text.append(
                        f"No valid sequences for homology group {hg_idx}\n"
                    )
                    continue
                elif num_seqs < 2:
                    log_text.append(
                        f"Only 1 valid sequence for homology group {hg_idx}: "
                        "no alignments or trees made\n"
                    )
                    continue

                tot_seqs = num_seqs

                # Create protein alignment
                prot_file = directory / f"{dbid}_protein.fasta"
                align_file = directory / f"{dbid}_protein_align.fasta"

                if run_muscle_alignment(prot_file, align_file):
                    if args.debug:
                        logger.debug(f"Created alignment file {align_file}")
                else:
                    log_text.append(
                        f"   WARNING: Failed to create alignment for {dbid}\n"
                    )

                # Create tree (if more than 2 sequences)
                if num_seqs > 2:
                    tree_file = directory / f"{dbid}_tree_unrooted.par"
                    if run_semphy_tree(align_file, tree_file):
                        if args.debug:
                            logger.debug(f"Created tree file {tree_file}")
                    else:
                        log_text.append(
                            f"   WARNING: Failed to create tree for {dbid}\n"
                        )
                else:
                    log_text.append(
                        f"Only {num_seqs} valid sequences for homology group "
                        f"{hg_idx}: no tree made\n"
                    )
            else:
                # Create symlinks to reference files
                if ref_dir:
                    for file_type in FILE_TYPES:
                        target = ref_dir / f"{ref_dbid}{file_type}"
                        link = directory / f"{dbid}{file_type}"

                        if not target.exists():
                            continue

                        try:
                            link.symlink_to(target)
                        except OSError as e:
                            log_text.append(
                                f"Error creating symlink {link} -> {target}: {e}\n"
                            )

        count += 1
        if args.debug:
            logger.debug(f"{tot_seqs} sequences aligned")
            logger.debug(f"Rounds = {count}")
            if count >= args.rounds:
                break

    # Clean up defunct files (not in debug mode)
    if not args.debug:
        for orf, dbid in dbid_for_orf.items():
            if dbid in dbids_used:
                continue

            directory = get_alignment_dir(dbid)
            prot_file = directory / f"{dbid}_protein.fasta"

            if not prot_file.exists():
                continue

            log_text.append(f"Removing defunct files for DBID {dbid}\n")

            for file_type in FILE_TYPES:
                filepath = directory / f"{dbid}{file_type}"
                if filepath.exists() or filepath.is_symlink():
                    filepath.unlink()

    log_text.append(f"\n{__file__} completed successfully {datetime.now()}\n")

    # Write log file
    with open(log_file, "w") as f:
        f.writelines(log_text)

    logger.info(f"Processed {count} homology groups")
    logger.info(f"Log written to {log_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
