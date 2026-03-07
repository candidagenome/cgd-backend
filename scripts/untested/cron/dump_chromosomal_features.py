#!/usr/bin/env python3
"""
Dump chromosomal feature data to a tab-delimited file.

This script generates the chromosomal_feature.tab file containing
detailed information about all chromosomal features for a strain.

Based on ftp_datadump.pl by CGD team.

Usage:
    python dump_chromosomal_features.py <strain_abbrev>
    python dump_chromosomal_features.py C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    HTML_ROOT_DIR: Root directory for download files
    LOG_DIR: Directory for log files
"""

import argparse
import gzip
import logging
import os
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
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
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


def get_strain_config(session, strain_abbrev: str) -> dict | None:
    """Get strain configuration from database."""
    query = text(f"""
        SELECT o.organism_no, o.organism_abbrev, o.organism_name
        FROM {DB_SCHEMA}.organism o
        WHERE o.organism_abbrev = :strain_abbrev
    """)
    result = session.execute(query, {"strain_abbrev": strain_abbrev}).fetchone()
    if not result:
        return None

    # Get seq_source
    seq_query = text(f"""
        SELECT DISTINCT s.source
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feat_location fl ON s.seq_no = fl.root_seq_no
        JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
        WHERE s.is_seq_current = 'Y'
        AND f.organism_abbrev = :strain_abbrev
        FETCH FIRST 1 ROW ONLY
    """)
    seq_result = session.execute(seq_query, {"strain_abbrev": strain_abbrev}).fetchone()

    return {
        "organism_no": result[0],
        "organism_abbrev": result[1],
        "organism_name": result[2],
        "seq_source": seq_result[0] if seq_result else None,
    }


def get_all_features(session, strain_abbrev: str, seq_source: str) -> list[dict]:
    """Get all features for a strain with their details."""
    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.gene_name, f.feature_type,
               f.dbxref_id, f.headline, f.date_created,
               fl.min_coord, fl.max_coord, fl.strand,
               chr.feature_name as chromosome
        FROM {DB_SCHEMA}.feature f
        LEFT JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
            AND fl.is_loc_current = 'Y'
        LEFT JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
            AND s.is_seq_current = 'Y'
            AND s.source = :seq_source
        LEFT JOIN {DB_SCHEMA}.feature chr ON s.feature_no = chr.feature_no
        WHERE f.organism_abbrev = :strain_abbrev
        AND f.feature_type NOT IN ('chromosome', 'contig')
        ORDER BY chr.feature_name, fl.min_coord, f.feature_name
    """)

    features = []
    for row in session.execute(
        query, {"strain_abbrev": strain_abbrev, "seq_source": seq_source}
    ).fetchall():
        features.append({
            "feature_no": row[0],
            "feature_name": row[1],
            "gene_name": row[2],
            "feature_type": row[3],
            "dbxref_id": row[4],
            "headline": row[5],
            "date_created": row[6],
            "start": row[7],
            "end": row[8],
            "strand": row[9],
            "chromosome": row[10],
        })

    return features


def get_feature_aliases(session, feature_no: int) -> list[str]:
    """Get aliases for a feature."""
    query = text(f"""
        SELECT a.alias_name
        FROM {DB_SCHEMA}.alias a
        JOIN {DB_SCHEMA}.feat_alias fa ON a.alias_no = fa.alias_no
        WHERE fa.feature_no = :feature_no
    """)
    return [row[0] for row in session.execute(query, {"feature_no": feature_no}).fetchall()]


def get_feature_qualifier(session, feature_no: int) -> str | None:
    """Get feature qualifier (e.g., Deleted, Merged)."""
    query = text(f"""
        SELECT fp.property_value
        FROM {DB_SCHEMA}.feat_property fp
        WHERE fp.feature_no = :feature_no
        AND fp.property_type = 'feature_qualifier'
    """)
    result = session.execute(query, {"feature_no": feature_no}).fetchone()
    return result[0] if result else None


def get_secondary_dbxref(session, feature_no: int) -> str | None:
    """Get secondary database cross-reference ID."""
    query = text(f"""
        SELECT fp.property_value
        FROM {DB_SCHEMA}.feat_property fp
        WHERE fp.feature_no = :feature_no
        AND fp.property_type = 'secondary_dbxref_id'
    """)
    result = session.execute(query, {"feature_no": feature_no}).fetchone()
    return result[0] if result else None


def get_orthologs(session, feature_no: int) -> list[str]:
    """Get S. cerevisiae ortholog names for a feature."""
    query = text(f"""
        SELECT f2.feature_name
        FROM {DB_SCHEMA}.feat_relationship fr
        JOIN {DB_SCHEMA}.feature f2 ON fr.child_feature_no = f2.feature_no
        WHERE fr.parent_feature_no = :feature_no
        AND fr.relationship_type = 'ortholog'
        AND f2.organism_abbrev = 'S_cerevisiae'
    """)
    return [row[0] for row in session.execute(query, {"feature_no": feature_no}).fetchall()]


def get_reserved_gene_info(session, feature_no: int) -> tuple[str | None, str | None]:
    """Get gene name reservation info."""
    # Get reservation date
    date_query = text(f"""
        SELECT fp.property_value
        FROM {DB_SCHEMA}.feat_property fp
        WHERE fp.feature_no = :feature_no
        AND fp.property_type = 'gene_name_reservation_date'
    """)
    date_result = session.execute(date_query, {"feature_no": feature_no}).fetchone()
    reservation_date = date_result[0] if date_result else None

    # Check if reserved name is now standard
    std_query = text(f"""
        SELECT fp.property_value
        FROM {DB_SCHEMA}.feat_property fp
        WHERE fp.feature_no = :feature_no
        AND fp.property_type = 'reserved_name_is_standard'
    """)
    std_result = session.execute(std_query, {"feature_no": feature_no}).fetchone()
    is_standard = std_result[0] if std_result else None

    return reservation_date, is_standard


def write_chromosomal_features(
    session, strain_abbrev: str, seq_source: str, output_file: Path
):
    """Write chromosomal features to a tab-delimited file."""
    features = get_all_features(session, strain_abbrev, seq_source)
    logger.info(f"Found {len(features)} features")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(output_file, "w") as f:
        # Header
        f.write(f"! Generated by {PROJECT_ACRONYM} on {timestamp}\n")
        f.write(f"! Strain: {strain_abbrev}\n")
        f.write("!\n")
        f.write("! Columns:\n")
        f.write("!  1. Feature name (primary)\n")
        f.write("!  2. Standard gene name (locus name)\n")
        f.write("!  3. Aliases (| separated)\n")
        f.write("!  4. Feature type\n")
        f.write("!  5. Chromosome\n")
        f.write("!  6. Start coordinate\n")
        f.write("!  7. Stop coordinate\n")
        f.write("!  8. Strand\n")
        f.write("!  9. Primary DBID\n")
        f.write("! 10. Secondary DBID\n")
        f.write("! 11. Description (headline)\n")
        f.write("! 12. Date created\n")
        f.write("! 13. Feature qualifier\n")
        f.write("! 14. Gene name reservation date\n")
        f.write("! 15. Reserved name is standard (Y/N)\n")
        f.write("! 16. S. cerevisiae ortholog(s) (| separated)\n")
        f.write("!\n")

        for feat in features:
            feature_no = feat["feature_no"]

            # Get additional info
            aliases = get_feature_aliases(session, feature_no)
            qualifier = get_feature_qualifier(session, feature_no)
            secondary_dbxref = get_secondary_dbxref(session, feature_no)
            reservation_date, is_standard = get_reserved_gene_info(session, feature_no)
            orthologs = get_orthologs(session, feature_no)

            # Format fields
            feature_name = feat["feature_name"] or ""
            gene_name = feat["gene_name"] or ""
            aliases_str = "|".join(aliases) if aliases else ""
            feature_type = feat["feature_type"] or ""
            chromosome = feat["chromosome"] or ""
            start = str(feat["start"]) if feat["start"] else ""
            end = str(feat["end"]) if feat["end"] else ""
            strand = feat["strand"] or ""
            dbxref_id = feat["dbxref_id"] or ""
            secondary_dbxref = secondary_dbxref or ""
            headline = (feat["headline"] or "").replace("\t", " ").replace("\n", " ")
            date_created = feat["date_created"].strftime("%Y-%m-%d") if feat["date_created"] else ""
            qualifier = qualifier or ""
            reservation_date = reservation_date or ""
            is_standard = is_standard or ""
            orthologs_str = "|".join(orthologs) if orthologs else ""

            # Write line
            fields = [
                feature_name,
                gene_name,
                aliases_str,
                feature_type,
                chromosome,
                start,
                end,
                strand,
                dbxref_id,
                secondary_dbxref,
                headline,
                date_created,
                qualifier,
                reservation_date,
                is_standard,
                orthologs_str,
            ]

            f.write("\t".join(fields) + "\n")


def archive_old_file(current_file: Path, archive_dir: Path):
    """Move old file to archive directory with date suffix."""
    if not current_file.exists():
        return

    archive_dir.mkdir(parents=True, exist_ok=True)

    # Add date suffix
    date_suffix = datetime.now().strftime("%Y%m")
    archive_name = f"{current_file.name}.{date_suffix}"
    archive_path = archive_dir / archive_name

    # Move and compress
    shutil.move(current_file, archive_path)
    with open(archive_path, "rb") as f_in:
        with gzip.open(f"{archive_path}.gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    archive_path.unlink()

    logger.info(f"Archived to {archive_path}.gz")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump chromosomal feature data to a tab-delimited file"
    )
    parser.add_argument(
        "strain_abbrev",
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for files",
    )
    parser.add_argument(
        "--no-archive",
        action="store_true",
        help="Don't archive old file",
    )

    args = parser.parse_args()
    strain_abbrev = args.strain_abbrev

    logger.info(f"Dumping chromosomal features for {strain_abbrev}")

    try:
        with SessionLocal() as session:
            # Get strain config
            config = get_strain_config(session, strain_abbrev)
            if not config:
                logger.error(f"Strain not found: {strain_abbrev}")
                return 1

            seq_source = config["seq_source"]
            if not seq_source:
                logger.error(f"No seq_source found for {strain_abbrev}")
                return 1

            logger.info(f"Seq source: {seq_source}")

            # Determine output directory
            if args.output_dir:
                output_dir = args.output_dir
            else:
                output_dir = (
                    HTML_ROOT_DIR / "download" / "chromosomal_feature_files" /
                    strain_abbrev
                )

            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / "chromosomal_feature.tab"

            # Archive old file if exists
            if not args.no_archive and output_file.exists():
                archive_dir = output_dir / "archive"
                archive_old_file(output_file, archive_dir)

            # Write new file
            write_chromosomal_features(session, strain_abbrev, seq_source, output_file)
            logger.info(f"Chromosomal features written to {output_file}")

            # Create current symlink
            current_link = output_dir / "current"
            if current_link.exists() or current_link.is_symlink():
                current_link.unlink()
            current_link.symlink_to(output_file.name)
            logger.info(f"Created symlink: {current_link} -> {output_file.name}")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
