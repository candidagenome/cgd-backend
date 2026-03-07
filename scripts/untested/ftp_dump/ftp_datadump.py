#!/usr/bin/env python3
"""
FTP data dump coordinator script.

This script invokes a series of data dump methods to create data dump files
for the FTP site. It generates various tab-delimited files containing
chromosomal features, literature curation data, and protein information.

Based on ftp_datadump.pl.

Usage:
    python ftp_datadump.py
    python ftp_datadump.py --debug
    python ftp_datadump.py --help

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    FTP_DIR: FTP directory for output files
    LOG_DIR: Directory for log files
    PROJECT_ACRONYM: Project acronym (e.g., CGD, SGD)

Output Files:
    chromosomal_feature/SGD_features.tab
    chromosomal_feature/dbxref.tab
    chromosomal_feature/annotation_change.tab
    chromosomal_feature/clone.tab
    chromosomal_feature/chromosome_length.tab
    literature_curation/go_terms.tab
    literature_curation/phenotypes.tab
    literature_curation/gene_literature.tab
    literature_curation/go_slim_mapping.tab
    literature_curation/go_protein_complex_slim.tab
    literature_curation/interactions.tab
    protein_info/protein_properties.tab
"""

import argparse
import gzip
import logging
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
FTP_DIR = Path(os.getenv("FTP_DIR", "/var/ftp/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")

# Maximum retry attempts for NFS issues
MAX_ATTEMPTS = 3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class DataDumper:
    """Class to handle various data dump operations."""

    def __init__(self, session):
        """Initialize with database session."""
        self.session = session

    def get_sgd_features(self, output_file: Path) -> None:
        """Generate SGD features file."""
        # Import and use the dedicated script's logic
        from scripts.ftp_dump.recreate_sgd_features import (
            get_sgd_features, write_features_file
        )
        features = get_sgd_features(self.session)
        write_features_file(features, output_file)
        logger.info(f"Generated {output_file}")

    def get_external_id(self, output_file: Path) -> None:
        """Generate external ID (dbxref) file."""
        query = text(f"""
            SELECT f.feature_name, d.dbxref_id, d.dbxref_type, d.source
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.dbxref_feat df ON f.feature_no = df.feature_no
            JOIN {DB_SCHEMA}.dbxref d ON df.dbxref_no = d.dbxref_no
            ORDER BY f.feature_name, d.source, d.dbxref_type
        """)

        with open(output_file, "w") as fh:
            for row in self.session.execute(query).fetchall():
                feat_name, dbxref_id, dbxref_type, source = row
                line = f"{feat_name}\t{dbxref_id}\t{dbxref_type}\t{source}\n"
                fh.write(line)

        logger.info(f"Generated {output_file}")

    def get_go_term(self, output_file: Path) -> None:
        """Generate GO terms file."""
        query = text(f"""
            SELECT DISTINCT f.feature_name, f.gene_name, g.go_term,
                   g.goid, ga.go_evidence, ga.annotation_type
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.go_annotation ga ON f.feature_no = ga.feature_no
            JOIN {DB_SCHEMA}.go g ON ga.go_no = g.go_no
            ORDER BY f.feature_name, g.go_aspect, g.go_term
        """)

        with open(output_file, "w") as fh:
            for row in self.session.execute(query).fetchall():
                (feat_name, gene_name, go_term, goid,
                 go_evidence, annotation_type) = row
                # Format GOID with leading zeros
                goid_fmt = f"GO:{goid:07d}" if goid else ""
                line = f"{feat_name}\t{gene_name or ''}\t{go_term}\t{goid_fmt}\t{go_evidence or ''}\t{annotation_type or ''}\n"
                fh.write(line)

        logger.info(f"Generated {output_file}")

    def get_phenotypes(self, output_file: Path) -> None:
        """Generate phenotypes file."""
        query = text(f"""
            SELECT f.feature_name, f.gene_name, p.phenotype,
                   fp.phenotype_type, fp.experiment_type
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_pheno fp ON f.feature_no = fp.feature_no
            JOIN {DB_SCHEMA}.phenotype p ON fp.phenotype_no = p.phenotype_no
            ORDER BY f.feature_name, p.phenotype
        """)

        with open(output_file, "w") as fh:
            for row in self.session.execute(query).fetchall():
                (feat_name, gene_name, phenotype,
                 pheno_type, experiment_type) = row
                line = f"{feat_name}\t{gene_name or ''}\t{phenotype or ''}\t{pheno_type or ''}\t{experiment_type or ''}\n"
                fh.write(line)

        logger.info(f"Generated {output_file}")

    def get_annotation_change(self, output_file: Path) -> None:
        """Generate annotation change file."""
        query = text(f"""
            SELECT f.feature_name, f.gene_name, ac.change_type,
                   ac.old_value, ac.new_value, ac.date_changed
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.annotation_change ac ON f.feature_no = ac.feature_no
            ORDER BY ac.date_changed DESC, f.feature_name
        """)

        with open(output_file, "w") as fh:
            for row in self.session.execute(query).fetchall():
                (feat_name, gene_name, change_type,
                 old_value, new_value, date_changed) = row
                date_str = date_changed.strftime("%Y-%m-%d") if date_changed else ""
                line = f"{feat_name}\t{gene_name or ''}\t{change_type or ''}\t{old_value or ''}\t{new_value or ''}\t{date_str}\n"
                fh.write(line)

        logger.info(f"Generated {output_file}")

    def get_clone(self, output_file: Path) -> None:
        """Generate clone file."""
        query = text(f"""
            SELECT f.feature_name, c.clone_name, c.clone_type,
                   c.vector, c.source
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_clone fc ON f.feature_no = fc.feature_no
            JOIN {DB_SCHEMA}.clone c ON fc.clone_no = c.clone_no
            ORDER BY f.feature_name, c.clone_name
        """)

        with open(output_file, "w") as fh:
            for row in self.session.execute(query).fetchall():
                (feat_name, clone_name, clone_type,
                 vector, source) = row
                line = f"{feat_name}\t{clone_name or ''}\t{clone_type or ''}\t{vector or ''}\t{source or ''}\n"
                fh.write(line)

        logger.info(f"Generated {output_file}")

    def get_gene_reference(self, output_file: Path) -> None:
        """Generate gene literature file."""
        query = text(f"""
            SELECT f.feature_name, f.gene_name, r.pubmed,
                   r.citation, rl.topic
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.reflink rl ON f.feature_no = rl.primary_key
            JOIN {DB_SCHEMA}.reference r ON rl.reference_no = r.reference_no
            WHERE rl.tab_name = 'FEATURE'
            ORDER BY f.feature_name, r.pubmed
        """)

        with open(output_file, "w") as fh:
            for row in self.session.execute(query).fetchall():
                (feat_name, gene_name, pubmed,
                 citation, topic) = row
                line = f"{feat_name}\t{gene_name or ''}\t{pubmed or ''}\t{citation or ''}\t{topic or ''}\n"
                fh.write(line)

        logger.info(f"Generated {output_file}")

    def get_chromosome_length(self, output_file: Path) -> None:
        """Generate chromosome length file."""
        query = text(f"""
            SELECT f.feature_name, fl.stop_coord
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
            WHERE f.feature_type = 'chromosome'
            ORDER BY f.feature_name
        """)

        with open(output_file, "w") as fh:
            for row in self.session.execute(query).fetchall():
                feat_name, length = row
                line = f"{feat_name}\t{length or ''}\n"
                fh.write(line)

        logger.info(f"Generated {output_file}")

    def get_protein_info(self, output_file: Path) -> None:
        """Generate protein properties file."""
        query = text(f"""
            SELECT f.feature_name, f.gene_name,
                   pp.molecular_weight, pp.pi, pp.protein_length
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.protein_info pp ON f.feature_no = pp.feature_no
            ORDER BY f.feature_name
        """)

        with open(output_file, "w") as fh:
            for row in self.session.execute(query).fetchall():
                (feat_name, gene_name, mw, pi, length) = row
                line = f"{feat_name}\t{gene_name or ''}\t{mw or ''}\t{pi or ''}\t{length or ''}\n"
                fh.write(line)

        logger.info(f"Generated {output_file}")

    def get_go_slim_mapping(self, output_file: Path) -> None:
        """Generate GO slim mapping file."""
        query = text(f"""
            SELECT f.feature_name, f.gene_name, gs.slim_name,
                   g.goid, g.go_term, g.go_aspect
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.go_annotation ga ON f.feature_no = ga.feature_no
            JOIN {DB_SCHEMA}.go g ON ga.go_no = g.go_no
            JOIN {DB_SCHEMA}.go_slim gs ON g.go_no = gs.go_no
            ORDER BY f.feature_name, gs.slim_name
        """)

        with open(output_file, "w") as fh:
            for row in self.session.execute(query).fetchall():
                (feat_name, gene_name, slim_name,
                 goid, go_term, go_aspect) = row
                goid_fmt = f"GO:{goid:07d}" if goid else ""
                line = f"{feat_name}\t{gene_name or ''}\t{slim_name or ''}\t{goid_fmt}\t{go_term or ''}\t{go_aspect or ''}\n"
                fh.write(line)

        logger.info(f"Generated {output_file}")

    def get_go_protein_complex_slim(self, output_file: Path) -> None:
        """Generate GO protein complex slim file."""
        query = text(f"""
            SELECT f.feature_name, f.gene_name, pc.complex_name,
                   g.goid, g.go_term
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_complex fc ON f.feature_no = fc.feature_no
            JOIN {DB_SCHEMA}.protein_complex pc ON fc.complex_no = pc.complex_no
            LEFT JOIN {DB_SCHEMA}.go g ON pc.go_no = g.go_no
            ORDER BY f.feature_name, pc.complex_name
        """)

        with open(output_file, "w") as fh:
            for row in self.session.execute(query).fetchall():
                (feat_name, gene_name, complex_name,
                 goid, go_term) = row
                goid_fmt = f"GO:{goid:07d}" if goid else ""
                line = f"{feat_name}\t{gene_name or ''}\t{complex_name or ''}\t{goid_fmt}\t{go_term or ''}\n"
                fh.write(line)

        logger.info(f"Generated {output_file}")

    def get_interaction(self, output_file: Path) -> None:
        """Generate interactions file."""
        query = text(f"""
            SELECT f1.feature_name as bait, f2.feature_name as hit,
                   i.interaction_type, i.experiment_type,
                   r.pubmed, i.source
            FROM {DB_SCHEMA}.interaction i
            JOIN {DB_SCHEMA}.feature f1 ON i.bait_feature_no = f1.feature_no
            JOIN {DB_SCHEMA}.feature f2 ON i.hit_feature_no = f2.feature_no
            LEFT JOIN {DB_SCHEMA}.reference r ON i.reference_no = r.reference_no
            ORDER BY f1.feature_name, f2.feature_name
        """)

        with open(output_file, "w") as fh:
            for row in self.session.execute(query).fetchall():
                (bait, hit, interaction_type,
                 experiment_type, pubmed, source) = row
                line = f"{bait}\t{hit}\t{interaction_type or ''}\t{experiment_type or ''}\t{pubmed or ''}\t{source or ''}\n"
                fh.write(line)

        logger.info(f"Generated {output_file}")


def archive_data(datafile: Path) -> None:
    """Archive data file at the first run of the month."""
    if not datafile.exists():
        return

    now = datetime.now()
    day = now.day

    # Archive at first run of month (day < 8)
    if day >= 8:
        return

    date_str = now.strftime("%Y%m")

    archive_dir = datafile.parent / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    archive_file = archive_dir / f"{datafile.name}.{date_str}"

    try:
        shutil.copy(str(datafile), str(archive_file))
        # Gzip the archive
        with open(archive_file, "rb") as f_in:
            with gzip.open(f"{archive_file}.gz", "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        archive_file.unlink()  # Remove uncompressed version
        logger.info(f"{datafile} archived successfully")
    except Exception as e:
        logger.error(f"{datafile} was not archived correctly: {e}")


def archive_weekly(datafile: Path) -> None:
    """Archive data file weekly (with full date)."""
    if not datafile.exists():
        return

    now = datetime.now()
    date_str = now.strftime("%Y%m%d")

    archive_dir = datafile.parent / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    archive_file = archive_dir / f"{datafile.name}.{date_str}"

    try:
        shutil.copy(str(datafile), str(archive_file))
        # Gzip the archive
        with open(archive_file, "rb") as f_in:
            with gzip.open(f"{archive_file}.gz", "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        archive_file.unlink()  # Remove uncompressed version
        logger.info(f"{datafile} archived successfully")
    except Exception as e:
        logger.error(f"{datafile} was not archived correctly: {e}")


def retrieve_data(dumper: DataDumper, method_name: str, output_file: Path) -> None:
    """
    Retrieve data with retry logic for NFS issues.
    """
    attempts = MAX_ATTEMPTS
    method = getattr(dumper, method_name)

    while attempts > 0:
        try:
            method(output_file)
            archive_data(output_file)
            return

        except Exception as e:
            attempts -= 1
            logger.error(f"Problem generating {output_file}: {e}")

            if attempts > 0:
                logger.info(f"Will retry {attempts} more time(s)")
                time.sleep(5)

    logger.error(f"Failed to generate {output_file} after {MAX_ATTEMPTS} attempts")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="FTP data dump coordinator"
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
    log_file = LOG_DIR / "ftp_data_dump.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    logger.info("*" * 50)
    logger.info(datetime.now().isoformat())

    # Base output directory
    data_dump_dir = FTP_DIR / "data_download"

    # Create necessary directories
    for subdir in ["chromosomal_feature", "literature_curation", "protein_info"]:
        (data_dump_dir / subdir).mkdir(parents=True, exist_ok=True)

    try:
        with SessionLocal() as session:
            dumper = DataDumper(session)

            # Generate data files
            files_to_generate = [
                ("get_sgd_features", data_dump_dir / "chromosomal_feature" / f"{PROJECT_ACRONYM}_features.tab"),
                ("get_external_id", data_dump_dir / "chromosomal_feature" / "dbxref.tab"),
                ("get_go_term", data_dump_dir / "literature_curation" / "go_terms.tab"),
                ("get_phenotypes", data_dump_dir / "literature_curation" / "phenotypes.tab"),
                ("get_annotation_change", data_dump_dir / "chromosomal_feature" / "annotation_change.tab"),
                ("get_clone", data_dump_dir / "chromosomal_feature" / "clone.tab"),
                ("get_gene_reference", data_dump_dir / "literature_curation" / "gene_literature.tab"),
                ("get_chromosome_length", data_dump_dir / "chromosomal_feature" / "chromosome_length.tab"),
                ("get_protein_info", data_dump_dir / "protein_info" / "protein_properties.tab"),
                ("get_go_slim_mapping", data_dump_dir / "literature_curation" / "go_slim_mapping.tab"),
                ("get_go_protein_complex_slim", data_dump_dir / "literature_curation" / "go_protein_complex_slim.tab"),
                ("get_interaction", data_dump_dir / "literature_curation" / "interactions.tab"),
            ]

            for method_name, output_file in files_to_generate:
                logger.info(f"Generating {output_file.name}...")
                retrieve_data(dumper, method_name, output_file)
                logger.info(datetime.now().isoformat())

            # Archive GFF files (created by other scripts)
            gff_files = [
                data_dump_dir / "chromosomal_feature" / "saccharomyces_cerevisiae.gff",
                data_dump_dir / "chromosomal_feature" / "scerevisiae_regulatory.gff",
                data_dump_dir / "chromosomal_feature" / "scerevisiae_clonedata.gff",
            ]

            for gff_file in gff_files:
                if gff_file.exists():
                    archive_weekly(gff_file)

            # Archive GO files
            go_files = [
                data_dump_dir / "literature_curation" / "orf_geneontology.all.tab",
                data_dump_dir / "literature_curation" / "orf_geneontology.tab",
            ]

            for go_file in go_files:
                if go_file.exists():
                    archive_data(go_file)

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    logger.info("Finished executing ftp_datadump.py")
    logger.info(datetime.now().isoformat())

    return 0


if __name__ == "__main__":
    sys.exit(main())
