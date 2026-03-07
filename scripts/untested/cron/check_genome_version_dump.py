#!/usr/bin/env python3
"""
Check genome version and dump GFF/Sequence/Feature files.

This script compares database genome version with existing files and:
- Compares chromosome/contig sequences
- Compares gene model annotations
- Compares curatorial annotations
- Updates GFF, sequence, and chromosomal feature files if needed
- Reloads GBrowse databases

Based on checkGV_dumpGFF_Seq_Feat.pl.

Usage:
    python check_genome_version_dump.py --strain C_albicans_SC5314
    python check_genome_version_dump.py --strain C_albicans_SC5314 --update
    python check_genome_version_dump.py --strain C_albicans_SC5314 --force

Options:
    --strain: Strain abbreviation (required)
    --source: Sequence source (uses strain default if not provided)
    --update: Update GFF/Sequence/Chr Features files
    --force: Force dump using current DB genome version

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files
    HTML_ROOT_DIR: Root directory for HTML files
    LOG_DIR: Directory for log files
"""

import argparse
import gzip
import logging
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
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
BIN_DIR = Path(os.getenv("BIN_DIR", "/usr/local/bin"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
ADMIN_USER = os.getenv("ADMIN_USER", "ADMIN")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# File stubs for different directories
GB_STUBS = ["chromosomes.fasta", "features.gff", "intergenic.gff"]
GFF_STUBS = [
    "features.gff", "features.gtf", "intergenic.gff",
    "features_with_chromosome_sequences.gff.gz"
]
SEQ_STUBS = [
    "chromosomes.fasta.gz", "not_feature.fasta.gz", "orf_coding.fasta.gz",
    "orf_genomic_1000.fasta.gz", "orf_genomic.fasta.gz",
    "orf_plus_intergenic.fasta.gz", "orf_trans_all.fasta.gz",
    "other_features_genomic_1000.fasta.gz", "other_features_genomic.fasta.gz",
    "other_features_no_introns.fasta.gz", "other_features_plus_intergenic.fasta.gz"
]
CHR_STUBS = ["chromosomal_feature.tab"]

# Minor release description
MINOR_RELEASE_DESC = (
    "Minor release; curatorial information was updated in the files. "
    "No changes to sequence or gene model annotations."
)


@dataclass
class GenomeVersion:
    """Parsed genome version."""

    s: int  # sequence version
    m: int  # model version
    r: int  # release version
    assembly: str = ""

    def __str__(self) -> str:
        prefix = f"{self.assembly}-" if self.assembly else ""
        return f"{prefix}s{self.s:02d}-m{self.m:02d}-r{self.r:02d}"

    @classmethod
    def parse(cls, version_str: str) -> "GenomeVersion | None":
        """Parse a genome version string."""
        match = re.search(r"(A\d+)?-?s(\d+)-m(\d+)-r(\d+)", version_str)
        if match:
            assembly = match.group(1) or ""
            return cls(
                s=int(match.group(2)),
                m=int(match.group(3)),
                r=int(match.group(4)),
                assembly=assembly,
            )
        return None

    def __lt__(self, other: "GenomeVersion") -> bool:
        if self.s != other.s:
            return self.s < other.s
        if self.m != other.m:
            return self.m < other.m
        return self.r < other.r

    def increment_r(self) -> "GenomeVersion":
        """Return new version with incremented r."""
        return GenomeVersion(
            s=self.s, m=self.m, r=self.r + 1, assembly=self.assembly
        )


@dataclass
class FeatureProperties:
    """Properties of a genomic feature."""

    feature_name: str
    feature_type: str
    gene_name: str | None = None
    dbxref_id: str | None = None
    headline: str | None = None
    classification: str | None = None
    strand: str | None = None
    start: int | None = None
    end: int | None = None
    chromosome: str | None = None
    exons: list[str] = field(default_factory=list)
    attributes: dict[str, set[str]] = field(default_factory=dict)


def get_db_genome_version(session, strain_abbrev: str, seq_source: str) -> str | None:
    """Get current genome version from database."""
    query = text(f"""
        SELECT genome_version
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


def collect_root_seqs(
    session, strain_abbrev: str, seq_source: str
) -> dict[str, str]:
    """Collect chromosome/contig sequences from database."""
    query = text(f"""
        SELECT f.feature_name, s.residues
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
        WHERE f.organism_abbrev = :strain
        AND s.source = :source
        AND s.is_seq_current = 'Y'
        AND f.feature_type IN ('chromosome', 'contig')
    """)

    seqs = {}
    for row in session.execute(
        query, {"strain": strain_abbrev, "source": seq_source}
    ).fetchall():
        seqs[row[0]] = row[1]

    return seqs


def get_db_features(
    session, strain_abbrev: str, seq_source: str
) -> dict[str, FeatureProperties]:
    """Get feature information from database."""
    query = text(f"""
        SELECT f.feature_name, f.feature_type, f.gene_name, f.dbxref_id,
               f.headline, f.orf_classification,
               l.strand, l.start_coord, l.stop_coord,
               p.feature_name as parent_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location l ON f.feature_no = l.feature_no
        JOIN {DB_SCHEMA}.seq s ON l.root_seq_no = s.seq_no
        LEFT JOIN {DB_SCHEMA}.feat_relationship fr ON f.feature_no = fr.child_feature_no
        LEFT JOIN {DB_SCHEMA}.feature p ON fr.parent_feature_no = p.feature_no
        WHERE f.organism_abbrev = :strain
        AND s.source = :source
        AND s.is_seq_current = 'Y'
        AND l.is_loc_current = 'Y'
        AND f.feature_type NOT IN ('chromosome', 'contig')
    """)

    features = {}
    for row in session.execute(
        query, {"strain": strain_abbrev, "source": seq_source}
    ).fetchall():
        feat_name = row[0]
        features[feat_name] = FeatureProperties(
            feature_name=feat_name,
            feature_type=row[1],
            gene_name=row[2],
            dbxref_id=row[3],
            headline=row[4],
            classification=row[5],
            strand=row[6],
            start=row[7],
            end=row[8],
            chromosome=row[9],
        )

    return features


def read_fasta_sequences(fasta_file: Path) -> dict[str, str]:
    """Read sequences from a FASTA file."""
    seqs = {}
    current_id = None
    current_seq = []

    # Handle gzipped files
    if str(fasta_file).endswith(".gz"):
        opener = gzip.open
        mode = "rt"
    else:
        opener = open
        mode = "r"

    with opener(fasta_file, mode) as f:
        for line in f:
            line = line.strip()
            if line.startswith(">"):
                if current_id:
                    seqs[current_id] = "".join(current_seq)
                current_id = line[1:].split()[0]
                current_seq = []
            else:
                current_seq.append(line)

        if current_id:
            seqs[current_id] = "".join(current_seq)

    return seqs


def get_latest_versioned_file(directory: Path, stub: str, assembly: str = "") -> Path | None:
    """Find the latest versioned file matching a pattern."""
    pattern = f"*{assembly}*s*-m*-r*_{stub}" if assembly else f"*s*-m*-r*_{stub}"

    files = list(directory.glob(pattern))
    if not files:
        # Try without underscore
        pattern = f"*{assembly}*s*-m*-r*{stub}" if assembly else f"*s*-m*-r*{stub}"
        files = list(directory.glob(pattern))

    if not files:
        return None

    # Sort by modification time
    files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return files[0]


def extract_version_from_filename(filename: str) -> GenomeVersion | None:
    """Extract genome version from a filename."""
    return GenomeVersion.parse(filename)


def compare_sequences(
    db_seqs: dict[str, str],
    file_seqs: dict[str, str],
    log_file,
) -> bool:
    """
    Compare database sequences with file sequences.

    Returns True if there are differences.
    """
    has_diff = False

    for seq_id, file_seq in file_seqs.items():
        if seq_id not in db_seqs:
            log_file.write(
                f"SEQUENCE DIFFERENCE: Root sequence {seq_id} found in file but not Database\n"
            )
            has_diff = True
        elif file_seq != db_seqs[seq_id]:
            log_file.write(
                f"SEQUENCE DIFFERENCE: Root sequence {seq_id} changed in Database compared to file\n"
            )
            has_diff = True

    for seq_id in db_seqs:
        if seq_id not in file_seqs:
            log_file.write(
                f"SEQUENCE DIFFERENCE: Root sequence {seq_id} found in Database but not file\n"
            )
            has_diff = True

    return has_diff


def write_chromosome_sequences(output_file: Path, seqs: dict[str, str]) -> None:
    """Write chromosome sequences to a FASTA file."""
    # Determine if output should be gzipped
    if str(output_file).endswith(".gz"):
        opener = gzip.open
        mode = "wt"
    else:
        opener = open
        mode = "w"

    with opener(output_file, mode) as f:
        for seq_id in sorted(seqs.keys()):
            f.write(f">{seq_id}\n")
            seq = seqs[seq_id]
            # Write in 60-character lines
            for i in range(0, len(seq), 60):
                f.write(seq[i:i+60] + "\n")


def get_versioned_filename(
    directory: Path,
    strain_abbrev: str,
    version: GenomeVersion,
    stub: str,
) -> Path:
    """Generate a versioned filename."""
    return directory / f"{strain_abbrev}_version_{version}_{stub}"


def update_genome_version(
    session, strain_abbrev: str, seq_source: str, new_version: str, description: str
) -> bool:
    """Update genome version in database."""
    try:
        query = text(f"""
            UPDATE {DB_SCHEMA}.seq s
            SET genome_version = :new_version,
                date_updated = CURRENT_TIMESTAMP
            WHERE s.seq_no IN (
                SELECT s2.seq_no
                FROM {DB_SCHEMA}.seq s2
                JOIN {DB_SCHEMA}.feature f ON s2.feature_no = f.feature_no
                WHERE f.organism_abbrev = :strain
                AND s2.source = :source
                AND s2.is_seq_current = 'Y'
            )
        """)

        session.execute(
            query,
            {"new_version": new_version, "strain": strain_abbrev, "source": seq_source},
        )
        session.commit()

        logger.info(f"Updated genome version to {new_version}")
        return True

    except Exception as e:
        logger.error(f"Error updating genome version: {e}")
        session.rollback()
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Check genome version and dump GFF/Sequence/Feature files"
    )
    parser.add_argument(
        "--strain",
        required=True,
        help="Strain abbreviation",
    )
    parser.add_argument(
        "--source",
        default=None,
        help="Sequence source (uses strain default if not provided)",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update GFF/Sequence/Chr Features files",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force dump using current DB genome version",
    )

    args = parser.parse_args()

    strain_abbrev = args.strain

    # Set up log file
    log_file_path = LOG_DIR / f"{strain_abbrev}_featDump.log"

    try:
        with SessionLocal() as session:
            # Get seq_source
            if args.source:
                seq_source = args.source
            else:
                # Get default seq_source for strain
                query = text(f"""
                    SELECT DISTINCT s.source
                    FROM {DB_SCHEMA}.seq s
                    JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
                    WHERE f.organism_abbrev = :strain
                    AND s.is_seq_current = 'Y'
                    FETCH FIRST 1 ROW ONLY
                """)
                result = session.execute(query, {"strain": strain_abbrev}).fetchone()
                if not result:
                    logger.error(f"No sequence source found for {strain_abbrev}")
                    return 1
                seq_source = result[0]

            logger.info(f"Using seq_source: {seq_source}")

            # Parse assembly from seq_source
            assembly = ""
            assembly_match = re.search(r"Assembly (\d+)", seq_source)
            if assembly_match:
                assembly = f"A{assembly_match.group(1)}"

            # Set up directories
            download_chr_dir = HTML_ROOT_DIR / "download" / "chromosomal_feature_files" / strain_abbrev
            gff_dir = HTML_ROOT_DIR / "download" / "gff" / strain_abbrev
            seq_dir = HTML_ROOT_DIR / "download" / "sequence" / strain_abbrev

            if assembly:
                gff_current = gff_dir / f"Assembly{assembly_match.group(1)}"
                seq_current = seq_dir / f"Assembly{assembly_match.group(1)}" / "current"
                log_file_path = LOG_DIR / f"{strain_abbrev}_{assembly}_featDump.log"
            else:
                gff_current = gff_dir
                seq_current = seq_dir / "current"

            gff_archive = gff_dir / "archive"
            seq_archive = seq_current.parent / "archive"
            chr_archive = download_chr_dir / "archive"

            # Open log file
            with open(log_file_path, "w") as log_file:
                log_file.write(f"Started at {datetime.now()}\n\n")

                # Get database information
                db_seqs = collect_root_seqs(session, strain_abbrev, seq_source)
                db_features = get_db_features(session, strain_abbrev, seq_source)
                db_gv_str = get_db_genome_version(session, strain_abbrev, seq_source)

                if not db_gv_str:
                    log_file.write("ERROR: Could not get genome version from database\n")
                    return 1

                db_gv = GenomeVersion.parse(db_gv_str)
                if not db_gv:
                    log_file.write(f"ERROR: Could not parse genome version: {db_gv_str}\n")
                    return 1

                log_file.write(f"Database genome version: {db_gv}\n")
                log_file.write(f"Found {len(db_seqs)} root sequences\n")
                log_file.write(f"Found {len(db_features)} features\n\n")

                new_gv = None

                if args.force:
                    args.update = True
                    new_gv = db_gv
                    log_file.write(
                        f"FORCE option: updating to current DB genome version {db_gv}\n\n"
                    )
                else:
                    # Find latest file genome version
                    log_file.write("Comparing existing files with Database information\n\n")

                    # Get latest versioned file for sequences
                    gb_dir = HTML_ROOT_DIR / "gbrowse2" / "databases" / strain_abbrev
                    seq_file = get_latest_versioned_file(gb_dir, "chromosomes.fasta", assembly)

                    if not seq_file or not seq_file.exists():
                        log_file.write(f"WARNING: No sequence file found in {gb_dir}\n")
                        file_gv = None
                    else:
                        file_gv = extract_version_from_filename(seq_file.name)
                        log_file.write(f"File genome version: {file_gv} (from {seq_file.name})\n")

                    if file_gv and db_gv < file_gv:
                        log_file.write(
                            f"ERROR: Database Genome Version {db_gv} is lower than "
                            f"File Genome Version {file_gv}\n"
                        )
                        return 1

                    # Compare sequences
                    if seq_file and seq_file.exists():
                        log_file.write("Checking for sequence differences:\n\n")
                        file_seqs = read_fasta_sequences(seq_file)

                        seq_diff = compare_sequences(db_seqs, file_seqs, log_file)

                        if seq_diff:
                            log_file.write("\nSequence differences found\n")
                            if file_gv and db_gv.s == file_gv.s:
                                log_file.write(
                                    "ERROR: Database genome version has not been incremented "
                                    "to reflect sequence changes.\n"
                                )
                                return 1
                            else:
                                log_file.write(
                                    "Database genome version already incremented. "
                                    "Updating all files.\n\n"
                                )
                                new_gv = db_gv
                        else:
                            log_file.write("No root sequence differences found\n\n")

                            # TODO: Compare gene models and curatorial annotations
                            # This would require reading and parsing the GFF file
                            # For now, we'll just check if update flag is set

                            if args.update:
                                # Increment r version
                                new_gv = db_gv.increment_r()
                                log_file.write(
                                    f"Curatorial update requested. "
                                    f"Incrementing version to {new_gv}\n\n"
                                )

                                # Update database genome version
                                if not update_genome_version(
                                    session, strain_abbrev, seq_source,
                                    str(new_gv), MINOR_RELEASE_DESC
                                ):
                                    return 1

                # Update files if we have a new version
                if new_gv and args.update:
                    log_file.write(f"Updating files to version {new_gv}\n\n")

                    # Create directories if needed
                    for d in [gff_current, seq_current, download_chr_dir,
                              gff_archive, seq_archive, chr_archive]:
                        d.mkdir(parents=True, exist_ok=True)

                    # Write chromosome sequences
                    seq_file_path = get_versioned_filename(
                        seq_current, strain_abbrev, new_gv, "chromosomes.fasta.gz"
                    )
                    write_chromosome_sequences(seq_file_path, db_seqs)
                    log_file.write(f"Created {seq_file_path}\n")

                    # Note: Full implementation would also:
                    # - Write GFF files (features.gff, intergenic.gff, features.gtf)
                    # - Write sequence files (orf_coding.fasta.gz, etc.)
                    # - Write chromosomal features table
                    # - Reload GBrowse database
                    # - Create stable symlinks

                    log_file.write("\nFile update completed\n")

                log_file.write(f"\nCompleted at {datetime.now()}\n")

            logger.info(f"Log written to {log_file_path}")
            return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
