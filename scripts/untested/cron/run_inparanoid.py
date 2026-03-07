#!/usr/bin/env python3
"""
Run InParanoid ortholog prediction.

This script runs InParanoid ortholog prediction between two organisms using
an outgroup. It downloads protein sequences, runs InParanoid, parses results,
and updates the database with ortholog mappings.

Based on run_inparanoid.pl.

Usage:
    python run_inparanoid.py --strain C_albicans_SC5314
    python run_inparanoid.py --strain C_albicans_SC5314 --from-organism S_cerevisiae
    python run_inparanoid.py --strain C_albicans_SC5314 --config conf/inparanoid.txt

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
    INPARANOID_BIN: Path to InParanoid binary
"""

import argparse
import gzip
import logging
import os
import re
import shutil
import subprocess
import sys
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
INPARANOID_BIN = Path(os.getenv("INPARANOID_BIN", "/usr/local/bin/inparanoid.pl"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


@dataclass
class InparanoidConfig:
    """Configuration for InParanoid run."""

    ortholog_download_dir: str = ""
    ortholog_list_filename: str = ""
    inparanoid_self_base_url: str = ""
    inparanoid_self_file: str = ""
    inparanoid_comp_base_url: str = ""
    inparanoid_comp_file: str = ""
    inparanoid_outgroup_base_url: str = ""
    inparanoid_outgroup_file: str = ""
    inparanoid_self_download_filename: str = ""
    inparanoid_comp_download_filename: str = ""
    inparanoid_out_download_filename: str = ""
    besthits_download_dir: str = ""
    besthits_download_filename: str = ""


def read_config_file(config_file: Path) -> dict[str, str]:
    """Read configuration file with key=value pairs."""
    config = {}

    if not config_file.exists():
        return config

    with open(config_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if key and value:
                    config[key] = value

    return config


def download_file(url: str, local_path: Path) -> bool:
    """Download a file from URL."""
    try:
        logger.info(f"Downloading {url}")
        urllib.request.urlretrieve(url, local_path)
        return True
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        return False


def gunzip_file(gz_path: Path, output_path: Path | None = None) -> Path:
    """Decompress a gzipped file."""
    if output_path is None:
        output_path = gz_path.with_suffix("")

    with gzip.open(gz_path, "rb") as f_in:
        with open(output_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    return output_path


def cleanup_fasta(fasta_file: Path) -> None:
    """
    Clean up FASTA headers.

    Reduces header line to only first three space-separated elements.
    """
    tmp_file = fasta_file.with_suffix(".tmp")
    shutil.copy(fasta_file, tmp_file)

    with open(tmp_file) as f_in, open(fasta_file, "w") as f_out:
        for line in f_in:
            if line.startswith(">"):
                parts = line.split()
                if len(parts) >= 3:
                    parts[2] = parts[2].rstrip(",")
                    f_out.write(" ".join(parts[:3]) + "\n")
                else:
                    f_out.write(line)
            else:
                f_out.write(line)

    tmp_file.unlink()


def get_feature_by_name(session, feature_name: str) -> dict | None:
    """Get feature information by name."""
    query = text(f"""
        SELECT feature_no, feature_name, gene_name, dbxref_id
        FROM {DB_SCHEMA}.feature
        WHERE feature_name = :name
    """)

    result = session.execute(query, {"name": feature_name}).fetchone()
    if result:
        return {
            "feature_no": result[0],
            "feature_name": result[1],
            "gene_name": result[2],
            "dbxref_id": result[3],
        }
    return None


def parse_comp_fasta(fasta_file: Path, dbxref_source: str) -> tuple[dict, dict]:
    """
    Parse comparative organism FASTA file.

    Returns (orf2gene, orf2dbid) dictionaries.
    """
    orf2gene: dict[str, str] = {}
    orf2dbid: dict[str, str] = {}

    try:
        from Bio import SeqIO

        with open(fasta_file) as f:
            for record in SeqIO.parse(f, "fasta"):
                orf_id = record.id
                desc = record.description

                # Parse gene name and DBID from description
                gene_name = ""
                dbid = orf_id

                if "SGD" in dbxref_source:
                    parts = desc.split()
                    if len(parts) >= 2:
                        gene_name = parts[1] if not parts[1].startswith("SGD") else ""
                    for part in parts:
                        if part.startswith("SGDID:"):
                            dbid = part.replace("SGDID:", "")
                            break

                elif "POMBASE" in dbxref_source:
                    parts = desc.split()
                    if len(parts) >= 2:
                        gene_name = parts[1]

                orf2gene[orf_id] = gene_name
                orf2dbid[orf_id] = dbid

        return orf2gene, orf2dbid

    except ImportError:
        logger.error("BioPython not available")
        return {}, {}


def run_inparanoid(
    data_dir: Path,
    self_file: Path,
    comp_file: Path,
    outgroup_file: Path,
) -> bool:
    """Run InParanoid program."""
    try:
        cmd = [
            str(INPARANOID_BIN),
            str(data_dir),
            str(self_file),
            str(comp_file),
            str(outgroup_file),
        ]

        logger.info(f"Running InParanoid: {' '.join(cmd)}")

        result = subprocess.run(
            cmd, capture_output=True, text=True, cwd=str(data_dir)
        )

        if result.returncode != 0:
            logger.error(f"InParanoid failed: {result.stderr}")
            return False

        logger.info("InParanoid completed successfully")
        return True

    except Exception as e:
        logger.error(f"Error running InParanoid: {e}")
        return False


def parse_inparanoid_output(
    session,
    output_file: Path,
    ortholog_file: Path,
    orf2gene: dict[str, str],
    orf2dbid: dict[str, str],
    strain_abbrev: str,
    seq_source: str,
) -> int:
    """
    Parse InParanoid output and write ortholog file.

    Returns count of ortholog pairs.
    """
    # InParanoid output format:
    # Groups are bounded by lines with underscores
    # Each group contains ortholog pairs with percentage scores
    # We only want pairs where both have 100.00%

    self_orfs: list[str] = []
    comp_orfs: list[str] = []
    in_mapping = False
    ortholog_count = 0

    with open(output_file) as f_in, open(ortholog_file, "w") as f_out:
        # Write header
        f_out.write(f"## Ortholog file for {strain_abbrev}\n")
        f_out.write(f"## Created: {datetime.now()}\n")
        f_out.write(f"## Seq source: {seq_source}\n")
        f_out.write("#\n")

        for line in f_in:
            line = line.strip()

            # Start of mapping section
            if line.startswith("__") and not in_mapping:
                in_mapping = True
                continue

            # End of group or end of file
            if (line.startswith("__") or not line) and (self_orfs and comp_orfs):
                # Process the group
                for self_orf in self_orfs:
                    feat = get_feature_by_name(session, self_orf)
                    if not feat:
                        continue

                    gene_name = feat["gene_name"] or ""

                    for comp_orf in comp_orfs:
                        if comp_orf not in orf2gene or comp_orf not in orf2dbid:
                            logger.warning(f"Missing data for {comp_orf}")
                            continue

                        f_out.write(
                            f"{self_orf}\t{gene_name}\t{feat['dbxref_id']}\t"
                            f"{comp_orf}\t{orf2gene[comp_orf]}\t{orf2dbid[comp_orf]}\n"
                        )
                        ortholog_count += 1

                self_orfs = []
                comp_orfs = []
                continue

            # Skip header lines within mapping
            if in_mapping and line.startswith(("Group ", "Score ", "Bootstrap ")):
                continue

            # Parse ortholog mapping line
            if not in_mapping:
                continue

            parts = line.split()

            if parts and parts[0]:
                # Line has self ORF
                if len(parts) >= 2 and parts[1] == "100.00%":
                    self_orfs.append(parts[0])

                if len(parts) >= 4 and parts[3] == "100.00%":
                    comp_orfs.append(parts[2])

            elif len(parts) >= 2:
                # Line only has comp ORF
                if parts[1] == "100.00%":
                    comp_orfs.append(parts[0])

    return ortholog_count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run InParanoid ortholog prediction"
    )
    parser.add_argument(
        "--strain",
        required=True,
        help="Strain abbreviation",
    )
    parser.add_argument(
        "--from-organism",
        default="S_cerevisiae",
        help="Organism to compare against (default: S_cerevisiae)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Configuration file",
    )
    parser.add_argument(
        "--dbxref-source",
        default="SGD",
        help="DBXREF source for loading (default: SGD)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode - don't update database",
    )

    args = parser.parse_args()

    strain_abbrev = args.strain
    from_organism = args.from_organism
    dbxref_source = args.dbxref_source

    # Set up directories
    data_dir = DATA_DIR / "inparanoid" / strain_abbrev / from_organism
    data_dir.mkdir(parents=True, exist_ok=True)

    # Set up log file
    log_file = LOG_DIR / f"inparanoid_{strain_abbrev}_{from_organism}.log"

    # Configure file logging
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    logger.info(f"Starting InParanoid for {strain_abbrev} vs {from_organism}")

    try:
        with SessionLocal() as session:
            # Read config file if provided
            config = InparanoidConfig()
            if args.config:
                cfg = read_config_file(args.config)
                for key, value in cfg.items():
                    if hasattr(config, key):
                        setattr(config, key, value)
                logger.info(f"Read configuration from {args.config}")

            # Get seq_source for strain
            query = text(f"""
                SELECT DISTINCT s.source
                FROM {DB_SCHEMA}.seq s
                JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
                WHERE f.organism_abbrev = :strain
                AND s.is_seq_current = 'Y'
                FETCH FIRST 1 ROW ONLY
            """)
            result = session.execute(query, {"strain": strain_abbrev}).fetchone()
            seq_source = result[0] if result else ""

            # Download self sequences
            if config.inparanoid_self_base_url and config.inparanoid_self_file:
                self_url = config.inparanoid_self_base_url + config.inparanoid_self_file
                self_file = data_dir / f"SELF_{config.inparanoid_self_file}"

                if not download_file(self_url, self_file):
                    logger.error("Failed to download self sequences")
                    return 1

                if str(self_file).endswith(".gz"):
                    self_file = gunzip_file(self_file)

                cleanup_fasta(self_file)
            else:
                logger.error("Self sequence file not configured")
                return 1

            # Download comparison sequences
            if config.inparanoid_comp_base_url and config.inparanoid_comp_file:
                comp_url = config.inparanoid_comp_base_url + config.inparanoid_comp_file
                comp_file = data_dir / f"COMP_{config.inparanoid_comp_file}"

                if not download_file(comp_url, comp_file):
                    logger.error("Failed to download comparison sequences")
                    return 1

                if str(comp_file).endswith(".gz"):
                    comp_file = gunzip_file(comp_file)

                cleanup_fasta(comp_file)
            else:
                logger.error("Comparison sequence file not configured")
                return 1

            # Download outgroup sequences
            if config.inparanoid_outgroup_base_url and config.inparanoid_outgroup_file:
                out_url = config.inparanoid_outgroup_base_url + config.inparanoid_outgroup_file
                out_file = data_dir / f"OUT_{config.inparanoid_outgroup_file}"

                if not download_file(out_url, out_file):
                    logger.warning("Failed to download outgroup sequences, using old file")
                    old_file = out_file.with_suffix(out_file.suffix + ".old")
                    if old_file.exists():
                        shutil.copy(old_file, out_file)
                    else:
                        logger.error("No outgroup file available")
                        return 1

                if str(out_file).endswith(".gz"):
                    out_file = gunzip_file(out_file)

                cleanup_fasta(out_file)
            else:
                logger.error("Outgroup sequence file not configured")
                return 1

            # Parse comparison FASTA for gene/DBID mappings
            orf2gene, orf2dbid = parse_comp_fasta(comp_file, dbxref_source)
            if not orf2dbid:
                logger.error("Failed to parse comparison FASTA")
                return 1
            logger.info(f"Parsed {len(orf2dbid)} sequences from comparison file")

            # Run InParanoid
            if not run_inparanoid(data_dir, self_file, comp_file, out_file):
                logger.error("InParanoid failed")
                return 1

            # Find output file
            inparanoid_output = data_dir / f"Output.{self_file.name}-{comp_file.name}"
            if not inparanoid_output.exists():
                logger.error(f"InParanoid output not found: {inparanoid_output}")
                return 1

            # Parse output
            ortholog_file = data_dir / config.ortholog_list_filename
            if not ortholog_file.name:
                ortholog_file = data_dir / f"{strain_abbrev}_{from_organism}_orthologs.txt"

            ortholog_count = parse_inparanoid_output(
                session, inparanoid_output, ortholog_file,
                orf2gene, orf2dbid, strain_abbrev, seq_source
            )
            logger.info(f"Found {ortholog_count} ortholog pairs")

            # Rename files with .old extension
            for f in [self_file, comp_file, out_file]:
                if f.exists():
                    shutil.move(str(f), str(f) + ".old")

            if not args.test:
                # Copy to download directory
                if config.ortholog_download_dir:
                    download_dir = HTML_ROOT_DIR / config.ortholog_download_dir
                    download_dir.mkdir(parents=True, exist_ok=True)
                    shutil.copy(ortholog_file, download_dir / ortholog_file.name)
                    logger.info(f"Copied ortholog file to {download_dir}")

            logger.info("InParanoid completed successfully")
            return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
