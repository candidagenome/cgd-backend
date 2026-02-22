#!/usr/bin/env python3
"""
Reciprocal BLAST analysis against external databases.

This script performs reciprocal best hit BLAST analysis to identify homologs
between CGD features and external databases (RGD, MGD, dictyBase).

Based on reciprocalBlast.pl by CGD team.

Usage:
    python reciprocal_blast.py --strain C_albicans_SC5314 --target RGD --user admin
    python reciprocal_blast.py --strain C_albicans_SC5314 --target MGD --user admin

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
from datetime import datetime
from pathlib import Path
from urllib.request import urlretrieve

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
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))

# External database sources
SOURCE_URLS = {
    "RGD": "ftp://rgd.mcw.edu/pub/RGD_genome_annotations/sequence_files/Rattus-norvegicus_RGD_version_protein-reps.fa",
    "MGD": "ftp://ftp.informatics.jax.org/pub/sequence_dbs/seq_dbs.current/uniprotmus.Z",
    "dictyBase": "",  # Must be downloaded manually
}

# URL templates for external links
URL_TEMPLATES = {
    "dictyBase": "http://dictybase.org/gene/_SUBSTITUTE_THIS_",
    "RGD": "https://rgd.mcw.edu/rgdweb/report/gene/main.html?id=_SUBSTITUTE_THIS_",
    "MGD": "http://www.informatics.jax.org/marker/_SUBSTITUTE_THIS_",
}

# BLAST executables
MAKEBLASTDB = os.getenv("MAKEBLASTDB", "makeblastdb")
BLASTP = os.getenv("BLASTP", "blastp")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def download_target_sequences(target: str, output_file: Path) -> bool:
    """Download protein sequences for the target database."""
    source_url = SOURCE_URLS.get(target)

    if not source_url:
        # Must be downloaded manually
        if output_file.exists():
            logger.info(f"Using existing file: {output_file}")
            return True
        logger.error(f"No source URL for {target} and file doesn't exist")
        return False

    # Check if backup exists and is recent (< 7 days old)
    backup_file = output_file.with_suffix(".backup.fasta")
    if backup_file.exists():
        age_days = (datetime.now() - datetime.fromtimestamp(
            backup_file.stat().st_mtime
        )).days
        if age_days <= 7:
            logger.info(
                f"Using backup file ({age_days} days old): {backup_file}"
            )
            shutil.copy(backup_file, output_file)
            return True

    try:
        logger.info(f"Downloading {source_url}")

        # Determine if compressed
        is_gz = source_url.endswith(".gz")
        is_z = source_url.endswith(".Z")

        if is_gz:
            temp_file = output_file.with_suffix(".fasta.gz")
        elif is_z:
            temp_file = output_file.with_suffix(".fasta.Z")
        else:
            temp_file = output_file

        urlretrieve(source_url, temp_file)

        # Decompress if needed
        if is_gz:
            with gzip.open(temp_file, "rb") as f_in:
                with open(output_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            temp_file.unlink()
        elif is_z:
            # Use uncompress for .Z files
            result = subprocess.run(
                ["uncompress", "-f", str(temp_file)],
                capture_output=True,
            )
            if result.returncode != 0:
                logger.error(f"Error uncompressing: {result.stderr}")
                return False
            # Rename to output file
            decompressed = temp_file.with_suffix("")
            shutil.move(decompressed, output_file)

        # Create backup
        shutil.copy(output_file, backup_file)
        logger.info(f"Downloaded and backed up to {backup_file}")

        return True

    except Exception as e:
        logger.error(f"Error downloading: {e}")
        if backup_file.exists():
            logger.info("Using backup file")
            shutil.copy(backup_file, output_file)
            return True
        return False


def filter_sequences(input_file: Path, target: str) -> Path:
    """Filter and reformat sequences for the target database."""
    output_file = input_file.with_suffix(".filtered.fasta")

    seen_ids: set[str] = set()
    records: list[tuple[str, str, str]] = []  # (id, description, sequence)

    current_id = None
    current_desc = None
    current_seq = []

    with open(input_file) as f:
        for line in f:
            if line.startswith(">"):
                # Save previous record
                if current_id and current_id not in seen_ids:
                    records.append((current_id, current_desc, "".join(current_seq)))
                    seen_ids.add(current_id)

                # Parse header
                header = line[1:].strip()
                current_seq = []

                if target == "MGD":
                    # Format: >sp|id|name description
                    parts = header.split("|")
                    if len(parts) < 3 or parts[0] != "sp":
                        current_id = None
                        continue

                    db_id = parts[1]
                    rest = parts[2] if len(parts) > 2 else ""
                    name_desc = rest.split(" ", 1)
                    name = name_desc[0]
                    desc = name_desc[1] if len(name_desc) > 1 else ""

                    # Only mouse sequences
                    if "OS=Mus musculus" not in desc:
                        current_id = None
                        continue

                    # Extract gene name
                    gene = ""
                    if "GN=" in desc:
                        gene = desc.split("GN=")[1].split()[0]

                    current_id = db_id
                    current_desc = f"{name} {gene.upper()}" if gene else name

                elif target == "dictyBase":
                    # Format: >id|description
                    parts = header.split("|", 1)
                    current_id = parts[0]
                    desc = parts[1] if len(parts) > 1 else ""

                    # Extract gene name
                    gene = ""
                    if "Protein gene: " in desc:
                        gene = desc.split("Protein gene: ")[1].split()[0]

                    current_desc = f"{desc.split()[0]} {gene}" if gene else desc.split()[0]

                elif target == "RGD":
                    # Format: >id|name|db|gene|description
                    parts = header.split("|")
                    if len(parts) >= 4:
                        rgd_id = parts[0]
                        name = parts[1]
                        gene = parts[3] if len(parts) > 3 else ""

                        current_id = f"Rat_{rgd_id}"
                        current_desc = f"{name} {gene}" if gene else name
                    else:
                        current_id = parts[0]
                        current_desc = parts[1] if len(parts) > 1 else ""

                else:
                    current_id = header.split()[0]
                    current_desc = " ".join(header.split()[1:])

            else:
                if current_id:
                    current_seq.append(line.strip())

    # Save last record
    if current_id and current_id not in seen_ids:
        records.append((current_id, current_desc, "".join(current_seq)))

    # Write filtered file
    with open(output_file, "w") as f:
        for seq_id, desc, seq in records:
            f.write(f">{seq_id} {desc}\n")
            # Write sequence in 60-char lines
            for i in range(0, len(seq), 60):
                f.write(seq[i:i+60] + "\n")

    logger.info(f"Filtered {len(records)} sequences to {output_file}")
    return output_file


def create_blast_db(fasta_file: Path, db_name: Path) -> bool:
    """Create a BLAST database from a FASTA file."""
    try:
        cmd = [
            MAKEBLASTDB,
            "-in", str(fasta_file),
            "-dbtype", "prot",
            "-out", str(db_name),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"makeblastdb failed: {result.stderr}")
            return False

        logger.info(f"Created BLAST database: {db_name}")
        return True

    except Exception as e:
        logger.error(f"Error creating BLAST database: {e}")
        return False


def run_blast(
    query_file: Path,
    db_path: Path,
    output_file: Path,
    num_hits: int = 5,
) -> bool:
    """Run BLAST and save output."""
    try:
        cmd = [
            BLASTP,
            "-query", str(query_file),
            "-db", str(db_path),
            "-out", str(output_file),
            "-outfmt", "6",  # Tabular output
            "-max_target_seqs", str(num_hits),
            "-evalue", "1e-5",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"BLAST failed: {result.stderr}")
            return False

        logger.info(f"BLAST completed: {output_file}")
        return True

    except Exception as e:
        logger.error(f"Error running BLAST: {e}")
        return False


def parse_blast_output(output_file: Path) -> dict[str, list[str]]:
    """Parse BLAST tabular output and return top hits."""
    hits: dict[str, list[str]] = {}

    with open(output_file) as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 2:
                continue

            query = parts[0]
            subject = parts[1]

            if query not in hits:
                hits[query] = []
            hits[query].append(subject)

    return hits


def get_strain_proteins(
    session, strain_abbrev: str
) -> tuple[Path, dict[str, str]]:
    """Get protein sequences for a strain and return FASTA file path and ID mapping."""
    # Get seq_source for strain
    seq_query = text(f"""
        SELECT DISTINCT s.source
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feat_location fl ON s.seq_no = fl.root_seq_no
        JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
        WHERE s.is_seq_current = 'Y'
        AND f.organism_abbrev = :strain
        FETCH FIRST 1 ROW ONLY
    """)
    seq_result = session.execute(seq_query, {"strain": strain_abbrev}).fetchone()
    seq_source = seq_result[0] if seq_result else None

    if not seq_source:
        raise ValueError(f"No seq_source found for {strain_abbrev}")

    # Get proteins
    query = text(f"""
        SELECT f.feature_name, ps.residues
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.protein_seq ps ON f.feature_no = ps.feature_no
        WHERE f.organism_abbrev = :strain
        AND f.feature_type = 'ORF'
        AND ps.is_seq_current = 'Y'
    """)

    proteins: dict[str, str] = {}
    for row in session.execute(query, {"strain": strain_abbrev}).fetchall():
        proteins[row[0]] = row[1]

    # Write to FASTA file
    data_dir = DATA_DIR / "homology" / "reciprocalBlast"
    data_dir.mkdir(parents=True, exist_ok=True)

    fasta_file = data_dir / f"{strain_abbrev}_proteins.fasta"
    with open(fasta_file, "w") as f:
        for feature_name, seq in proteins.items():
            f.write(f">{feature_name}\n")
            for i in range(0, len(seq), 60):
                f.write(seq[i:i+60] + "\n")

    logger.info(f"Wrote {len(proteins)} proteins to {fasta_file}")
    return fasta_file, proteins


def find_reciprocal_best_hits(
    db_hits: dict[str, list[str]],
    target_hits: dict[str, list[str]],
) -> list[tuple[str, str]]:
    """Find reciprocal best hits between two BLAST results."""
    reciprocal_hits: list[tuple[str, str]] = []

    for db_seq, target_seqs in db_hits.items():
        if not target_seqs:
            continue

        top_target = target_seqs[0]

        # Check if target's top hit is the original db_seq
        if top_target in target_hits:
            target_top_hits = target_hits[top_target]
            if target_top_hits and db_seq in target_top_hits:
                reciprocal_hits.append((db_seq, top_target))

    return reciprocal_hits


def update_database(
    session,
    strain_abbrev: str,
    target: str,
    hits: list[tuple[str, str]],
    user: str,
) -> int:
    """Update database with reciprocal best hits."""
    # Delete existing entries for this target
    delete_query = text(f"""
        DELETE FROM {DB_SCHEMA}.dbxref_feat
        WHERE dbxref_no IN (
            SELECT d.dbxref_no
            FROM {DB_SCHEMA}.dbxref d
            WHERE d.source = :target
            AND d.dbxref_type = 'Gene ID'
        )
        AND feature_no IN (
            SELECT f.feature_no
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.organism o ON (
                f.organism_no = o.organism_no
                AND o.organism_abbrev = :strain
            )
            WHERE f.feature_type = 'ORF'
        )
    """)
    session.execute(delete_query, {"target": target, "strain": strain_abbrev})

    # Get or create URL
    url_template = URL_TEMPLATES.get(target, "")

    url_query = text(f"""
        SELECT url_no FROM {DB_SCHEMA}.url WHERE url = :url
    """)
    url_result = session.execute(url_query, {"url": url_template}).fetchone()

    if url_result:
        url_no = url_result[0]
    else:
        insert_url = text(f"""
            INSERT INTO {DB_SCHEMA}.url (url, created_by)
            VALUES (:url, :user)
        """)
        session.execute(insert_url, {"url": url_template, "user": user.upper()})

        url_result = session.execute(url_query, {"url": url_template}).fetchone()
        url_no = url_result[0]

    # Insert hits
    count = 0
    for feature_name, target_id in hits:
        # Clean up target ID
        clean_id = target_id.replace("Rat_", "")

        # Get feature_no
        feat_query = text(f"""
            SELECT feature_no FROM {DB_SCHEMA}.feature
            WHERE feature_name = :name
        """)
        feat_result = session.execute(feat_query, {"name": feature_name}).fetchone()

        if not feat_result:
            continue

        feature_no = feat_result[0]

        # Get or create dbxref
        dbxref_query = text(f"""
            SELECT dbxref_no FROM {DB_SCHEMA}.dbxref
            WHERE dbxref_id = :id AND source = :source AND dbxref_type = 'Gene ID'
        """)
        dbxref_result = session.execute(
            dbxref_query, {"id": clean_id, "source": target}
        ).fetchone()

        if dbxref_result:
            dbxref_no = dbxref_result[0]
        else:
            insert_dbxref = text(f"""
                INSERT INTO {DB_SCHEMA}.dbxref
                (dbxref_id, source, dbxref_type, created_by)
                VALUES (:id, :source, 'Gene ID', :user)
            """)
            session.execute(
                insert_dbxref,
                {"id": clean_id, "source": target, "user": user.upper()},
            )
            dbxref_result = session.execute(
                dbxref_query, {"id": clean_id, "source": target}
            ).fetchone()
            dbxref_no = dbxref_result[0]

        # Check if dbxref_feat exists
        check_df = text(f"""
            SELECT 1 FROM {DB_SCHEMA}.dbxref_feat
            WHERE dbxref_no = :dx AND feature_no = :fn
        """)
        if not session.execute(
            check_df, {"dx": dbxref_no, "fn": feature_no}
        ).fetchone():
            insert_df = text(f"""
                INSERT INTO {DB_SCHEMA}.dbxref_feat (dbxref_no, feature_no)
                VALUES (:dx, :fn)
            """)
            session.execute(insert_df, {"dx": dbxref_no, "fn": feature_no})

        # Check if dbxref_url exists
        check_du = text(f"""
            SELECT 1 FROM {DB_SCHEMA}.dbxref_url
            WHERE dbxref_no = :dx AND url_no = :un
        """)
        if not session.execute(check_du, {"dx": dbxref_no, "un": url_no}).fetchone():
            insert_du = text(f"""
                INSERT INTO {DB_SCHEMA}.dbxref_url (dbxref_no, url_no)
                VALUES (:dx, :un)
            """)
            session.execute(insert_du, {"dx": dbxref_no, "un": url_no})

        count += 1

    return count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Reciprocal BLAST analysis against external databases"
    )
    parser.add_argument(
        "--strain",
        required=True,
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--target",
        required=True,
        choices=["RGD", "MGD", "dictyBase"],
        help="Target database",
    )
    parser.add_argument(
        "--user",
        required=True,
        help="Database user for created_by field",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Actually update the database (default: dry run)",
    )

    args = parser.parse_args()

    data_dir = DATA_DIR / "homology" / "reciprocalBlast"
    data_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Running reciprocal BLAST: {args.strain} vs {args.target}"
    )

    try:
        # Download target sequences
        target_fasta = data_dir / f"{args.target}_proteins.fasta"
        if not download_target_sequences(args.target, target_fasta):
            return 1

        # Filter target sequences
        filtered_target = filter_sequences(target_fasta, args.target)

        with SessionLocal() as session:
            # Get strain proteins
            strain_fasta, proteins = get_strain_proteins(session, args.strain)

            # Create BLAST databases
            strain_db = strain_fasta.with_suffix("")
            target_db = filtered_target.with_suffix("")

            if not create_blast_db(strain_fasta, strain_db):
                return 1
            if not create_blast_db(filtered_target, target_db):
                return 1

            # Run forward BLAST (strain vs target)
            forward_output = data_dir / f"{args.strain}_vs_{args.target}.blast"
            logger.info("Running forward BLAST...")
            if not run_blast(strain_fasta, target_db, forward_output):
                return 1

            forward_hits = parse_blast_output(forward_output)
            logger.info(f"Forward BLAST: {len(forward_hits)} queries with hits")

            # Extract target sequences that were hit
            hit_targets: set[str] = set()
            for targets in forward_hits.values():
                hit_targets.update(targets)

            # Write hit targets to file for reverse BLAST
            target_queries = data_dir / f"{args.target}_queries.fasta"
            with open(filtered_target) as f_in, open(target_queries, "w") as f_out:
                write_seq = False
                for line in f_in:
                    if line.startswith(">"):
                        seq_id = line[1:].split()[0]
                        write_seq = seq_id in hit_targets
                    if write_seq:
                        f_out.write(line)

            # Run reverse BLAST (target hits vs strain)
            reverse_output = data_dir / f"{args.target}_vs_{args.strain}.blast"
            logger.info("Running reverse BLAST...")
            if not run_blast(target_queries, strain_db, reverse_output):
                return 1

            reverse_hits = parse_blast_output(reverse_output)
            logger.info(f"Reverse BLAST: {len(reverse_hits)} queries with hits")

            # Find reciprocal best hits
            reciprocal_hits = find_reciprocal_best_hits(forward_hits, reverse_hits)
            logger.info(f"Found {len(reciprocal_hits)} reciprocal best hits")

            # Update database
            if args.update:
                count = update_database(
                    session, args.strain, args.target, reciprocal_hits, args.user
                )
                session.commit()
                logger.info(f"Updated database with {count} hits")
            else:
                logger.info("Dry run - no database changes made")
                for feat, target in reciprocal_hits[:10]:
                    logger.info(f"  {feat} <-> {target}")
                if len(reciprocal_hits) > 10:
                    logger.info(f"  ... and {len(reciprocal_hits) - 10} more")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
