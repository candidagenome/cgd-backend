#!/usr/bin/env python3
"""
FTP data dump - generate chromosomal feature files.

This script generates chromosomal feature data dump files for FTP download.
It creates versioned files with genome version information.

Based on ftp_datadump.pl by Stan Dong.

Usage:
    python ftp_datadump.py --strain C_albicans_SC5314 --output-dir download/chromosomal_feature_files/C_albicans_SC5314/
    python ftp_datadump.py --strain A_nidulans_FGSC_A4 --output-dir download/chromosomal_feature_files/A_nidulans_FGSC_A4/

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    HTML_ROOT_DIR: Root directory for HTML files
    LOG_DIR: Directory for log files
    TMP_DIR: Directory for temporary files
"""

import argparse
import gzip
import logging
import os
import re
import shutil
import sys
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
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Maximum retry attempts for file operations
MAX_ATTEMPTS = 3


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


def get_seq_source(session, strain_abbrev: str) -> str | None:
    """Get default seq_source for strain."""
    query = text(f"""
        SELECT DISTINCT s.source
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
        WHERE f.organism_abbrev = :strain
        AND s.is_seq_current = 'Y'
        FETCH FIRST 1 ROW ONLY
    """)

    result = session.execute(query, {"strain": strain_abbrev}).fetchone()
    return result[0] if result else None


def get_genome_version(session, strain_abbrev: str, seq_source: str) -> str | None:
    """Get current genome version from database."""
    query = text(f"""
        SELECT s.genome_version
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
        WHERE f.organism_abbrev = :strain
        AND s.source = :source
        AND s.is_seq_current = 'Y'
        FETCH FIRST 1 ROW ONLY
    """)

    result = session.execute(
        query, {"strain": strain_abbrev, "source": seq_source}
    ).fetchone()
    return result[0] if result else None


def get_latest_versioned_file(directory: Path, stub: str) -> Path | None:
    """Find the latest versioned file matching a pattern."""
    pattern = f"*version_*{stub}"

    files = list(directory.glob(pattern))
    if not files:
        return None

    # Sort by modification time (newest first)
    files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return files[0]


def get_versioned_filename(
    directory: Path, strain_abbrev: str, genome_version: str, stub: str
) -> Path:
    """Generate a versioned filename."""
    return directory / f"{strain_abbrev}_version_{genome_version}_{stub}"


def get_chromosomal_features(
    session, strain_abbrev: str, seq_source: str
) -> list[dict]:
    """Get chromosomal features from database."""
    query = text(f"""
        SELECT f.feature_name, f.gene_name, f.feature_type,
               f.headline, f.orf_classification,
               l.start_coord, l.stop_coord, l.strand,
               p.feature_name as chromosome
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location l ON f.feature_no = l.feature_no
        JOIN {DB_SCHEMA}.seq s ON l.root_seq_no = s.seq_no
        LEFT JOIN {DB_SCHEMA}.feat_relationship fr ON f.feature_no = fr.child_feature_no
        LEFT JOIN {DB_SCHEMA}.feature p ON fr.parent_feature_no = p.feature_no
        WHERE f.organism_abbrev = :strain
        AND s.source = :source
        AND s.is_seq_current = 'Y'
        AND l.is_loc_current = 'Y'
        ORDER BY f.feature_name
    """)

    features = []
    for row in session.execute(
        query, {"strain": strain_abbrev, "source": seq_source}
    ).fetchall():
        features.append({
            "feature_name": row[0],
            "gene_name": row[1],
            "feature_type": row[2],
            "headline": row[3],
            "orf_classification": row[4],
            "start": row[5],
            "stop": row[6],
            "strand": row[7],
            "chromosome": row[8],
        })

    return features


def write_chromosomal_features(
    output_file: Path,
    features: list[dict],
    strain_abbrev: str,
    genome_version: str,
) -> int:
    """Write chromosomal features to a tab-delimited file."""
    count = 0

    with open(output_file, "w") as f:
        # Write header
        f.write(f"## Chromosomal features for {strain_abbrev}\n")
        f.write(f"## Genome version: {genome_version}\n")
        f.write(f"## Created: {datetime.now()}\n")
        f.write("#\n")
        f.write(
            "# Feature_name\tGene_name\tFeature_type\tChromosome\t"
            "Start\tStop\tStrand\tORF_classification\tHeadline\n"
        )

        for feat in features:
            f.write(
                f"{feat['feature_name']}\t"
                f"{feat['gene_name'] or ''}\t"
                f"{feat['feature_type']}\t"
                f"{feat['chromosome'] or ''}\t"
                f"{feat['start']}\t"
                f"{feat['stop']}\t"
                f"{feat['strand']}\t"
                f"{feat['orf_classification'] or ''}\t"
                f"{feat['headline'] or ''}\n"
            )
            count += 1

    return count


def move_to_archive(file_path: Path, archive_dir: Path) -> Path | None:
    """Move a file to the archive directory and gzip it."""
    if not file_path.exists():
        return None

    archive_dir.mkdir(parents=True, exist_ok=True)

    archive_file = archive_dir / file_path.name

    # If archive already exists (gzipped), just delete the original
    if (archive_file.with_suffix(archive_file.suffix + ".gz")).exists():
        file_path.unlink()
        return None

    # Move to archive
    shutil.move(str(file_path), str(archive_file))

    # Gzip the archived file
    with open(archive_file, "rb") as f_in:
        with gzip.open(str(archive_file) + ".gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Remove uncompressed archive
    archive_file.unlink()

    return archive_file.with_suffix(archive_file.suffix + ".gz")


def create_stable_symlink(versioned_file: Path, stable_name: str) -> bool:
    """Create a stable symlink (without version) pointing to versioned file."""
    stable_link = versioned_file.parent / stable_name

    # Remove existing symlink if present
    if stable_link.is_symlink():
        stable_link.unlink()
    elif stable_link.exists():
        stable_link.unlink()

    # Create new symlink
    try:
        stable_link.symlink_to(versioned_file.name)
        return True
    except Exception as e:
        logger.error(f"Error creating symlink: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate chromosomal feature data dump files"
    )
    parser.add_argument(
        "--strain",
        required=True,
        help="Strain abbreviation",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Output directory (relative to HTML_ROOT_DIR)",
    )

    args = parser.parse_args()

    strain_abbrev = args.strain
    output_dir = HTML_ROOT_DIR / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    archive_dir = output_dir / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    # Set up log file
    log_file = LOG_DIR / f"ftp_data_dump_{strain_abbrev}.log"

    # Configure file logging
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    logger.info("*" * 50)
    logger.info(f"Starting FTP data dump for {strain_abbrev}")

    try:
        with SessionLocal() as session:
            # Validate strain
            strain_obj = get_organism(session, strain_abbrev)
            if not strain_obj:
                logger.error(f"No organism found for: {strain_abbrev}")
                return 1

            # Get seq_source
            seq_source = get_seq_source(session, strain_abbrev)
            if not seq_source:
                logger.error(f"No seq_source found for: {strain_abbrev}")
                return 1

            logger.info(f"Using seq_source: {seq_source}")

            # Get genome version
            genome_version = get_genome_version(session, strain_abbrev, seq_source)
            if not genome_version:
                logger.error(f"No genome version found for: {strain_abbrev}")
                return 1

            logger.info(f"Genome version: {genome_version}")

            # Check for existing file
            file_stub = "chromosomal_feature.tab"
            curr_file = get_latest_versioned_file(output_dir, file_stub)

            if curr_file:
                logger.info(f"Latest existing file: {curr_file}")

            # Generate new filename
            new_file = get_versioned_filename(
                output_dir, strain_abbrev, genome_version, file_stub
            )
            logger.info(f"New file: {new_file}")

            # Check if file already exists for this version
            if new_file.exists() and "version_" in str(new_file):
                logger.info(
                    f"Chromosomal feature file ({new_file}) already exists "
                    f"for latest genome version. Quitting."
                )
                return 0

            # Generate data with retries
            tmp_file = TMP_DIR / f"{new_file.name}.tmp"
            success = False

            for attempt in range(MAX_ATTEMPTS):
                try:
                    # Get chromosomal features
                    features = get_chromosomal_features(
                        session, strain_abbrev, seq_source
                    )
                    logger.info(f"Retrieved {len(features)} features")

                    # Write to temp file
                    count = write_chromosomal_features(
                        tmp_file, features, strain_abbrev, genome_version
                    )
                    logger.info(f"Wrote {count} features to {tmp_file}")

                    success = True
                    break

                except Exception as e:
                    logger.error(f"Error generating file (attempt {attempt + 1}): {e}")
                    if attempt < MAX_ATTEMPTS - 1:
                        logger.info(f"Will retry {MAX_ATTEMPTS - attempt - 1} more time(s)")
                        import time
                        time.sleep(5)

            if not success:
                logger.error("Failed to generate file after all attempts")
                return 1

            # Archive old file
            if curr_file and curr_file.exists():
                archived = move_to_archive(curr_file, archive_dir)
                if archived:
                    logger.info(f"Archived old file to {archived}")

            # Move temp file to destination
            shutil.move(str(tmp_file), str(new_file))
            logger.info(f"Moved temp file to {new_file}")

            # Create stable symlink
            stable_name = f"{strain_abbrev}_chromosomal_feature.tab"
            if create_stable_symlink(new_file, stable_name):
                logger.info(f"Created stable symlink: {stable_name}")

            logger.info("FTP data dump completed successfully")
            return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
