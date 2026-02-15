#!/usr/bin/env python3
"""
Generate BLAST best hits for features without orthologs.

This script generates best hits between organisms for proteins that don't have
orthologs. It downloads sequences, creates BLAST databases, runs BLAST, and
updates the database with the results.

Based on generateBestHits.pl by Jon Binkley.

Usage:
    python generate_best_hits.py --strain C_albicans_SC5314
    python generate_best_hits.py --strain C_albicans_SC5314 --from-organism S_cerevisiae
    python generate_best_hits.py --strain C_albicans_SC5314 --from-organism S_pombe --config conf/pombe.txt

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
"""

import argparse
import gzip
import logging
import os
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
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")

# BLAST configuration
BLAST_BIN = Path(os.getenv("BLAST_BIN", "/usr/local/blast/bin"))
BLASTP = os.getenv("BLASTP", "blastp")
MAKEBLASTDB = os.getenv("MAKEBLASTDB", "makeblastdb")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Mapping of organism to DBXREF source
SOURCE_FOR_ORGANISM = {
    "S_cerevisiae": "SGD_BEST_HIT",
    "S_pombe": "POMBASE_BEST_HIT",
    "N_crassa": "BROAD_NEUROSPORA_BEST_HIT",
    "C_albicans": "CGD_BEST_HIT",
    "A_nidulans": "AspGD_BEST_HIT",
}

# Homology group settings
HOMOLOGY_GROUP_TYPE = "best hit"
HOMOLOGY_GROUP_METHOD = "BLAST"


@dataclass
class OrganismConfig:
    """Configuration for an organism."""

    organism_abbrev: str
    seq_source: str
    ortholog_download_dir: str
    ortholog_list_filename: str
    inparanoid_self_base_url: str
    inparanoid_self_file: str
    inparanoid_comp_base_url: str
    inparanoid_comp_file: str
    besthits_download_dir: str
    besthits_download_filename: str


def get_organism(session, organism_abbrev: str) -> dict | None:
    """Get organism information from database."""
    query = text(f"""
        SELECT organism_no, organism_abbrev, common_name
        FROM {DB_SCHEMA}.organism
        WHERE organism_abbrev = :abbrev
    """)

    result = session.execute(query, {"abbrev": organism_abbrev}).fetchone()
    if result:
        return {
            "organism_no": result[0],
            "organism_abbrev": result[1],
            "common_name": result[2],
        }
    return None


def get_all_strains(session) -> list[str]:
    """Get all strain abbreviations from database."""
    query = text(f"""
        SELECT organism_abbrev
        FROM {DB_SCHEMA}.organism
    """)

    return [row[0] for row in session.execute(query).fetchall()]


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


def get_feature_by_dbxref(session, dbxref_id: str) -> dict | None:
    """Get feature information by dbxref_id."""
    query = text(f"""
        SELECT feature_no, feature_name, gene_name, dbxref_id
        FROM {DB_SCHEMA}.feature
        WHERE dbxref_id = :dbxref
    """)

    result = session.execute(query, {"dbxref": dbxref_id}).fetchone()
    if result:
        return {
            "feature_no": result[0],
            "feature_name": result[1],
            "gene_name": result[2],
            "dbxref_id": result[3],
        }
    return None


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


def read_ortholog_file(ortholog_file: Path) -> set[str]:
    """
    Read ortholog file and return set of feature names that have orthologs.

    The ortholog file is a 3-column file with the feature name in the first column.
    """
    features_with_orthologs = set()

    with open(ortholog_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split("\t")
            if parts[0]:
                features_with_orthologs.add(parts[0])

    return features_with_orthologs


def create_orphan_file(
    self_seq_file: Path,
    orphan_file: Path,
    features_with_orthologs: set[str],
) -> int:
    """
    Create a file of sequences that don't have orthologs.

    Returns count of orphan sequences.
    """
    try:
        from Bio import SeqIO

        orphan_count = 0
        with open(self_seq_file) as f_in, open(orphan_file, "w") as f_out:
            for record in SeqIO.parse(f_in, "fasta"):
                if record.id not in features_with_orthologs:
                    SeqIO.write(record, f_out, "fasta")
                    orphan_count += 1

        return orphan_count

    except ImportError:
        logger.error("BioPython not available")
        return 0


def create_blast_database(fasta_file: Path, db_type: str = "prot") -> bool:
    """Create a BLAST database from a FASTA file."""
    try:
        cmd = [
            MAKEBLASTDB,
            "-in", str(fasta_file),
            "-dbtype", db_type,
            "-parse_seqids",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"makeblastdb failed: {result.stderr}")
            return False

        logger.info(f"Created BLAST database: {fasta_file}")
        return True

    except Exception as e:
        logger.error(f"Error creating BLAST database: {e}")
        return False


def run_blast(
    database: Path,
    query_file: Path,
    output_file: Path,
    evalue: float = 1e-5,
    num_threads: int = 4,
) -> bool:
    """Run BLASTP."""
    try:
        cmd = [
            BLASTP,
            "-db", str(database),
            "-query", str(query_file),
            "-outfmt", "6",  # tabular format
            "-out", str(output_file),
            "-num_threads", str(num_threads),
            "-evalue", str(evalue),
            "-seg", "yes",  # Low complexity filtering
            "-matrix", "BLOSUM80",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"BLAST failed: {result.stderr}")
            return False

        logger.info(f"BLAST completed, results in {output_file}")
        return True

    except Exception as e:
        logger.error(f"Error running BLAST: {e}")
        return False


def parse_comp_fasta(fasta_file: Path, dbxref_source: str) -> tuple[dict, dict]:
    """
    Parse comparative organism FASTA file to get ORF to gene and DBID mappings.

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
                # Format varies by source
                gene_name = ""
                dbid = orf_id

                if "SGD" in dbxref_source:
                    # SGD format: >YAL001C TFC3 SGDID:S000000001
                    parts = desc.split()
                    if len(parts) >= 2:
                        gene_name = parts[1] if not parts[1].startswith("SGD") else ""
                    for part in parts:
                        if part.startswith("SGDID:"):
                            dbid = part.replace("SGDID:", "")
                            break

                elif "POMBASE" in dbxref_source:
                    # PomBase format varies
                    parts = desc.split()
                    if len(parts) >= 2:
                        gene_name = parts[1]

                orf2gene[orf_id] = gene_name
                orf2dbid[orf_id] = dbid

        return orf2gene, orf2dbid

    except ImportError:
        logger.error("BioPython not available")
        return {}, {}


def parse_blast_results(
    session,
    blast_file: Path,
    comp_fasta: Path,
    output_file: Path,
    dbxref_source: str,
    strain_abbrev: str,
    seq_source: str,
) -> int:
    """
    Parse BLAST results and output best hits.

    Returns count of best hits.
    """
    # Parse comparative FASTA for gene/DBID mappings
    orf2gene, orf2dbid = parse_comp_fasta(comp_fasta, dbxref_source)
    logger.info(f"Collected {len(orf2dbid)} DBIDs from {comp_fasta}")

    seen: set[str] = set()
    best_hits_count = 0

    with open(blast_file) as f_in, open(output_file, "w") as f_out:
        # Write header
        f_out.write(f"## Best hits file for {strain_abbrev}\n")
        f_out.write(f"## Created: {datetime.now()}\n")
        f_out.write(f"## Seq source: {seq_source}\n")
        f_out.write("#\n")

        for line in f_in:
            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) < 12:
                continue

            query = parts[0]
            target = parts[1]

            # Skip if already seen (keep only best hit)
            if query in seen:
                continue

            # Get DBID for target
            if target not in orf2dbid:
                logger.warning(f"No DBID found for BLAST hit: {target}")
                continue

            # Get query feature info
            feat = get_feature_by_name(session, query)
            if not feat:
                logger.warning(f"Can't map {query} to a feature")
                continue

            query_gene = feat["gene_name"] or ""
            query_dbid = feat["dbxref_id"] or ""

            target_gene = orf2gene.get(target, "")
            target_dbid = orf2dbid[target]

            f_out.write(
                f"{query}\t{query_gene}\t{query_dbid}\t"
                f"{target}\t{target_gene}\t{target_dbid}\n"
            )

            seen.add(query)
            best_hits_count += 1

    return best_hits_count


def delete_previous_best_hits(
    session,
    strain_abbrev1: str,
    strain_abbrev2: str,
    homology_group_type: str,
    method: str,
) -> int:
    """Delete previous best hit mappings between two strains."""
    query = text(f"""
        DELETE FROM {DB_SCHEMA}.homology_group
        WHERE homology_group_type = :group_type
        AND method = :method
        AND homology_group_no IN (
            SELECT DISTINCT fh1.homology_group_no
            FROM {DB_SCHEMA}.feat_homology fh1
            JOIN {DB_SCHEMA}.feat_homology fh2
                ON fh1.homology_group_no = fh2.homology_group_no
            JOIN {DB_SCHEMA}.feature f1 ON fh1.feature_no = f1.feature_no
            JOIN {DB_SCHEMA}.feature f2 ON fh2.feature_no = f2.feature_no
            JOIN {DB_SCHEMA}.organism o1 ON f1.organism_no = o1.organism_no
            JOIN {DB_SCHEMA}.organism o2 ON f2.organism_no = o2.organism_no
            WHERE o1.organism_abbrev = :strain1
            AND o2.organism_abbrev = :strain2
        )
    """)

    result = session.execute(
        query,
        {
            "group_type": homology_group_type,
            "method": method,
            "strain1": strain_abbrev1,
            "strain2": strain_abbrev2,
        },
    )
    session.commit()

    return result.rowcount


def insert_homology_group(session, group_type: str, method: str, created_by: str) -> int:
    """Insert a new homology group record."""
    query = text(f"""
        INSERT INTO {DB_SCHEMA}.homology_group
        (homology_group_type, method, created_by, date_created)
        VALUES (:group_type, :method, :created_by, CURRENT_TIMESTAMP)
        RETURNING homology_group_no
    """)

    result = session.execute(
        query,
        {"group_type": group_type, "method": method, "created_by": created_by},
    )
    session.commit()

    return result.fetchone()[0]


def insert_feat_homology(
    session, homology_group_no: int, feature_no: int, created_by: str
) -> None:
    """Insert a feat_homology record."""
    query = text(f"""
        INSERT INTO {DB_SCHEMA}.feat_homology
        (feature_no, homology_group_no, created_by, date_created)
        VALUES (:feature_no, :hg_no, :created_by, CURRENT_TIMESTAMP)
    """)

    session.execute(
        query,
        {
            "feature_no": feature_no,
            "hg_no": homology_group_no,
            "created_by": created_by,
        },
    )


def load_best_hits_to_database(
    session,
    best_hits_file: Path,
    strain_abbrev: str,
    from_organism_abbrev: str,
    common_name: str,
    admin_user: str,
) -> int:
    """Load best hits to homology_group and feat_homology tables."""
    group_type = f"{HOMOLOGY_GROUP_TYPE} for {common_name}"

    # Delete previous mappings
    deleted = delete_previous_best_hits(
        session,
        strain_abbrev,
        from_organism_abbrev,
        group_type,
        HOMOLOGY_GROUP_METHOD,
    )
    logger.info(f"Deleted {deleted} previous best hit mappings")

    # Load new mappings
    success = 0

    with open(best_hits_file) as f:
        for line in f:
            if line.startswith("#"):
                continue

            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) < 6:
                continue

            orf1, gene1, dbid1, orf2, gene2, dbid2 = parts[:6]

            # Get feature objects
            feat1 = get_feature_by_name(session, orf1)
            if not feat1:
                logger.warning(f"Can't map {orf1} to a feature")
                continue

            feat2 = get_feature_by_dbxref(session, dbid2)
            if not feat2:
                logger.warning(f"Can't map {gene2} ({dbid2}) to a feature")
                continue

            # Insert homology group and features
            hg_no = insert_homology_group(
                session, group_type, HOMOLOGY_GROUP_METHOD, admin_user
            )
            insert_feat_homology(session, hg_no, feat1["feature_no"], admin_user)
            insert_feat_homology(session, hg_no, feat2["feature_no"], admin_user)

            session.commit()
            success += 1

    return success


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


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate BLAST best hits for features without orthologs"
    )
    parser.add_argument(
        "--strain",
        required=True,
        help="Abbreviation for organism to generate best hits",
    )
    parser.add_argument(
        "--from-organism",
        default="S_cerevisiae",
        help="Abbreviation for organism to get best hits from (default: S_cerevisiae)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Configuration file with file paths",
    )
    parser.add_argument(
        "--dbxref-source",
        default=None,
        help="DBXREF source for loading (default: based on from-organism)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode - don't update database",
    )

    args = parser.parse_args()

    strain_abbrev = args.strain
    from_organism_abbrev = args.from_organism

    # Set up data directory
    data_dir = DATA_DIR / "best_hits" / strain_abbrev / from_organism_abbrev
    data_dir.mkdir(parents=True, exist_ok=True)

    # Set up log file
    log_file = LOG_DIR / f"generateBestHits_{strain_abbrev}_{from_organism_abbrev}.log"

    # Configure file logging
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    logger.info(f"Starting best hits generation for {strain_abbrev} from {from_organism_abbrev}")

    try:
        with SessionLocal() as session:
            # Validate strain
            strain_obj = get_organism(session, strain_abbrev)
            if not strain_obj:
                logger.error(f"No organism found for: {strain_abbrev}")
                return 1

            # Determine dbxref_source
            dbxref_source = args.dbxref_source
            if not dbxref_source:
                # Check if from_organism is in our database
                all_strains = get_all_strains(session)
                if from_organism_abbrev in all_strains:
                    dbxref_source = PROJECT_ACRONYM
                elif from_organism_abbrev in SOURCE_FOR_ORGANISM:
                    dbxref_source = SOURCE_FOR_ORGANISM[from_organism_abbrev]
                else:
                    logger.error(
                        f"Unknown from_organism: {from_organism_abbrev}. "
                        f"Please specify --dbxref-source"
                    )
                    return 1

            logger.info(f"Using DBXREF source: {dbxref_source}")

            # Read config file if provided
            config = {}
            if args.config:
                config = read_config_file(args.config)
                logger.info(f"Read {len(config)} settings from config file")

            # Get ortholog file path
            ortholog_dir = config.get(
                "orthologDownloadDir",
                f"download/orthologs/{strain_abbrev}/",
            )
            ortholog_file = config.get(
                "orthologListFileName",
                f"{strain_abbrev}_{from_organism_abbrev}_orthologs.txt",
            )
            ortholog_path = HTML_ROOT_DIR / ortholog_dir / ortholog_file

            if not ortholog_path.exists():
                logger.error(f"Ortholog file not found: {ortholog_path}")
                return 1

            # Read orthologs
            logger.info(f"Reading ortholog file: {ortholog_path}")
            features_with_orthologs = read_ortholog_file(ortholog_path)
            logger.info(f"Found {len(features_with_orthologs)} features with orthologs")

            # Get self sequences file
            self_seq_url = config.get("inparanoidSelfBaseURL", "")
            self_seq_file_name = config.get("inparanoidSelfFile", "")

            if self_seq_url and self_seq_file_name:
                self_seq_file = data_dir / self_seq_file_name
                if not download_file(self_seq_url + self_seq_file_name, self_seq_file):
                    logger.error("Failed to download self sequences")
                    return 1

                # Decompress if needed
                if str(self_seq_file).endswith(".gz"):
                    self_seq_file = gunzip_file(self_seq_file)
            else:
                logger.error("Self sequence file not configured")
                return 1

            # Get comparative sequences file
            comp_seq_url = config.get("inparanoidCompBaseURL", "")
            comp_seq_file_name = config.get("inparanoidCompFile", "")

            if comp_seq_url and comp_seq_file_name:
                comp_seq_file = data_dir / comp_seq_file_name
                if not download_file(comp_seq_url + comp_seq_file_name, comp_seq_file):
                    logger.error("Failed to download comparative sequences")
                    return 1

                # Decompress if needed
                if str(comp_seq_file).endswith(".gz"):
                    comp_seq_file = gunzip_file(comp_seq_file)
            else:
                logger.error("Comparative sequence file not configured")
                return 1

            # Create orphan file
            logger.info("Creating file of orphan sequences")
            orphan_file = data_dir / "orphaned.fasta"
            orphan_count = create_orphan_file(
                self_seq_file, orphan_file, features_with_orthologs
            )
            logger.info(f"Created orphan file with {orphan_count} sequences")

            # Create BLAST database
            logger.info("Creating BLAST database")
            if not create_blast_database(comp_seq_file):
                logger.error("Failed to create BLAST database")
                return 1

            # Run BLAST
            logger.info("Running BLAST")
            blast_output = data_dir / "blast.out"
            if not run_blast(comp_seq_file, orphan_file, blast_output):
                logger.error("BLAST failed")
                return 1

            # Parse BLAST results
            logger.info("Parsing BLAST results")
            best_hits_file = data_dir / f"{strain_abbrev}_{from_organism_abbrev}_best_hits.txt"

            # Get seq_source for strain
            seq_source_query = text(f"""
                SELECT DISTINCT s.source
                FROM {DB_SCHEMA}.seq s
                JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
                WHERE f.organism_abbrev = :strain
                AND s.is_seq_current = 'Y'
                FETCH FIRST 1 ROW ONLY
            """)
            result = session.execute(
                seq_source_query, {"strain": strain_abbrev}
            ).fetchone()
            seq_source = result[0] if result else ""

            best_hits_count = parse_blast_results(
                session,
                blast_output,
                comp_seq_file,
                best_hits_file,
                dbxref_source,
                strain_abbrev,
                seq_source,
            )
            logger.info(f"Found {best_hits_count} best hits")

            # Update database
            if not args.test:
                if dbxref_source == PROJECT_ACRONYM:
                    # Load to homology_group and feat_homology tables
                    logger.info("Loading best hits to database")
                    loaded = load_best_hits_to_database(
                        session,
                        best_hits_file,
                        strain_abbrev,
                        from_organism_abbrev,
                        strain_obj["common_name"],
                        "ADMIN",
                    )
                    logger.info(f"Loaded {loaded} best hits to database")
                else:
                    logger.info(
                        f"Best hits from external source ({dbxref_source}) "
                        f"should be loaded via loadOrthologs script"
                    )
            else:
                logger.info("Test mode - database not updated")

            # Copy to download directory
            besthits_dir = config.get(
                "besthitsDownloadDir",
                f"download/best_hits/{strain_abbrev}/",
            )
            besthits_filename = config.get(
                "besthitsDownloadFileName",
                f"{strain_abbrev}_{from_organism_abbrev}_best_hits.txt",
            )
            download_path = HTML_ROOT_DIR / besthits_dir
            download_path.mkdir(parents=True, exist_ok=True)
            shutil.copy(best_hits_file, download_path / besthits_filename)
            logger.info(f"Copied best hits file to {download_path / besthits_filename}")

            logger.info("Best hits generation completed successfully")
            return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
