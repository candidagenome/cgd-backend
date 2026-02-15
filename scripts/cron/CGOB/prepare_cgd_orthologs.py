#!/usr/bin/env python3
"""
Prepare CGD orthologs file from CGOB clusters.

This script creates a CGD-format ortholog clusters file from CGOB data,
mapping CGOB identifiers to CGD identifiers using a key file.

Based on prepareCGDorthologs.pl.

Usage:
    python prepare_cgd_orthologs.py
    python prepare_cgd_orthologs.py --debug

Environment Variables:
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

# Load environment variables
load_dotenv()

# Configuration from environment
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))

# CGOB configuration
CGOB_DATA_DIR = DATA_DIR / "CGOB"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Strain order for output
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

# Strain full names for header
STRAIN_FULLNAMES = {
    "C_albicans_SC5314": "Candida albicans SC5314",
    "C_dubliniensis_CD36": "Candida dubliniensis CD36",
    "C_tropicalis_MYA3404": "Candida tropicalis MYA-3404",
    "C_parapsilosis_CDC317": "Candida parapsilosis CDC317",
    "L_elongisporus_NRRL_YB4239": "Lodderomyces elongisporus NRRL YB-4239",
    "C_guilliermondii_ATCC6260": "Candida guilliermondii ATCC 6260",
    "C_lusitaniae_ATCC42720": "Candida lusitaniae ATCC 42720",
    "D_hansenii_CBS767": "Debaryomyces hansenii CBS767",
    "C_glabrata_CBS138": "Candida glabrata CBS138",
    "S_cerevisiae": "Saccharomyces cerevisiae S288C",
}

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

# Non-coding feature prefixes to skip
NONCODING_PREFIXES = ["tRNA", "rRNA", "snRNA", "snoRNA"]


def get_strain_from_prefix(seq_id: str) -> str | None:
    """Determine strain from sequence ID prefix."""
    for prefix, strain in STRAIN_PREFIXES.items():
        if seq_id.startswith(prefix):
            return strain
    return None


def is_noncoding(seq_id: str) -> bool:
    """Check if sequence ID represents a non-coding feature."""
    for prefix in NONCODING_PREFIXES:
        if prefix in seq_id:
            return True
    return False


def load_cgob_to_cgd_key(key_file: Path) -> tuple[dict[str, str], dict[str, str]]:
    """
    Load CGOB to CGD key file.

    Returns:
        cgd_id_for_cgob: mapping of CGOB ID to CGD ID
        strain_for_cgob: mapping of CGOB ID to strain
    """
    cgd_id_for_cgob: dict[str, str] = {}
    strain_for_cgob: dict[str, str] = {}

    with open(key_file) as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) >= 3:
                cgob_id = parts[0]
                strain = parts[1]
                cgd_id = parts[2]

                cgd_id_for_cgob[cgob_id] = cgd_id
                strain_for_cgob[cgob_id] = strain

    return cgd_id_for_cgob, strain_for_cgob


def load_cglab_orthologs(cglab_file: Path) -> dict[str, str]:
    """
    Load C. glabrata to S. cerevisiae ortholog mappings.

    Returns dict mapping S. cerevisiae ID to C. glabrata ID.
    """
    cglab_for_scer: dict[str, str] = {}

    if not cglab_file.exists():
        return cglab_for_scer

    with open(cglab_file) as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) >= 2:
                cglab, scer = parts[0], parts[1]
                if cglab and scer:
                    cglab_for_scer[scer] = cglab

    return cglab_for_scer


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Prepare CGD orthologs file from CGOB clusters"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    # Set up log file
    log_file = LOG_DIR / "CGOB_update.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    log_text = f"Program {__file__}: Starting {datetime.now()}\n\n"

    # Input/output files
    cgob_clusters = CGOB_DATA_DIR / "cgob_clusters.tab"
    cgd_clusters = CGOB_DATA_DIR / "cgd_clusters.tab"
    key_file = CGOB_DATA_DIR / "cgob_to_cgd_key.txt"
    cglab_orthologs = CGOB_DATA_DIR / "cglab_orthologs.txt"
    orphans_file = CGOB_DATA_DIR / "cgob_orphans.txt"

    # Check input files
    if not cgob_clusters.exists():
        error_msg = f"CGOB clusters file not found: {cgob_clusters}"
        log_text += f"ERROR: {error_msg}\n"
        logger.error(error_msg)
        with open(log_file, "w") as f:
            f.write(log_text)
        return 1

    if not key_file.exists():
        error_msg = f"Key file not found: {key_file}"
        log_text += f"ERROR: {error_msg}\n"
        logger.error(error_msg)
        with open(log_file, "w") as f:
            f.write(log_text)
        return 1

    # Load key file
    logger.info(f"Loading key file from {key_file}")
    cgd_id_for_cgob, strain_for_cgob = load_cgob_to_cgd_key(key_file)
    logger.info(f"Loaded {len(cgd_id_for_cgob)} CGOB to CGD mappings")

    # Load C. glabrata orthologs
    logger.info(f"Loading C. glabrata orthologs from {cglab_orthologs}")
    cglab_for_scer = load_cglab_orthologs(cglab_orthologs)
    logger.info(f"Loaded {len(cglab_for_scer)} C. glabrata ortholog mappings")

    # Process CGOB clusters
    logger.info(f"Processing CGOB clusters from {cgob_clusters}")
    orphans = []

    with open(cgob_clusters) as f_in, open(cgd_clusters, "w") as f_out:
        # Write header
        header = "\t".join(
            STRAIN_FULLNAMES.get(strain, strain) for strain in STRAIN_ORDER
        )
        f_out.write(header + "\n")

        for line in f_in:
            line = line.strip()
            if not line:
                continue

            homologs = line.split("\t")

            skip = False
            count = 0
            homolog_for_strain: dict[str, str] = {}

            for src_id in homologs:
                # Skip empty or placeholder entries
                if not src_id or src_id == "---" or not src_id.strip():
                    continue

                # Skip non-coding features
                if is_noncoding(src_id):
                    skip = True
                    break

                strain = get_strain_from_prefix(src_id)

                if not strain:
                    log_text += f"\tCould not identify strain for CGOB sequence {src_id}\n"
                    logger.debug(f"Could not identify strain for {src_id}")
                    continue

                # Check if this is an "alien" strain (not in our database)
                if strain not in STRAIN_ORDER:
                    count += 1
                    homolog_for_strain[strain] = src_id
                    continue

                # Map CGOB ID to CGD ID
                cgd_id = cgd_id_for_cgob.get(src_id)
                mapped_strain = strain_for_cgob.get(src_id, strain)

                if not cgd_id:
                    orphans.append(src_id)
                    continue

                count += 1
                homolog_for_strain[mapped_strain] = cgd_id

                # Check for C. glabrata ortholog via S. cerevisiae
                if src_id in cglab_for_scer:
                    cglab_id = cglab_for_scer[src_id]
                    local_cglab = cgd_id_for_cgob.get(cglab_id)
                    cg_strain = strain_for_cgob.get(cglab_id)

                    if local_cglab and cg_strain and cg_strain.startswith("C_glabrata"):
                        count += 1
                        homolog_for_strain[cg_strain] = local_cglab
                    else:
                        orphans.append(cglab_id)

            if skip:
                continue

            # Need at least 2 homologs to write a cluster
            if count < 2:
                continue

            # Write cluster line
            output_parts = []
            for strain in STRAIN_ORDER:
                if strain in homolog_for_strain:
                    output_parts.append(homolog_for_strain[strain])
                else:
                    output_parts.append("---")

            f_out.write("\t".join(output_parts) + "\n")

    log_text += f"\nCreating file {cgd_clusters}\n\n"
    logger.info(f"Wrote CGD clusters to {cgd_clusters}")

    # Write orphans file
    if orphans:
        with open(orphans_file, "w") as f:
            for orphan_id in orphans:
                f.write(f"{orphan_id}\n")

        log_text += "The following CGOB ids could not be found among CGD sequences:\n"
        for orphan_id in orphans:
            log_text += f"{orphan_id}\n"
        log_text += "\n"

        logger.info(f"Wrote {len(orphans)} orphan IDs to {orphans_file}")

    log_text += f"Exiting {__file__}: {datetime.now()}\n\n"

    # Write log file
    with open(log_file, "w") as f:
        f.write(log_text)

    logger.info(f"Log written to {log_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
