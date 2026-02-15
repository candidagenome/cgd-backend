#!/usr/bin/env python3
"""
Generate GPI (Gene Product Information) files for GO annotations.

This script creates GPI 2.0 format files containing gene product information
for use with GO (Gene Ontology) annotation pipelines. It queries the database
for features (ORFs, ncRNAs, pseudogenes, etc.) and outputs standardized
gene product information.

Based on makeGPI.pl by CGD team.

Usage:
    python make_gpi.py <strain_abbrev>
    python make_gpi.py C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
    HTML_ROOT_DIR: Root directory for HTML/download files
    LOG_DIR: Directory for log files
"""

import argparse
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
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
PROJECT_URL = os.getenv("PROJECT_URL", "http://www.candidagenome.org")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# SO (Sequence Ontology) codes for feature types
CODE_FOR_TYPE = {
    "ORF": "SO:0001217",
    "ncRNA": "SO:0001263",
    "rRNA": "SO:0001263",
    "snRNA": "SO:0001263",
    "snoRNA": "SO:0001263",
    "tRNA": "SO:0001263",
    "pseudogene": "SO:0000336",
}


def get_date() -> str:
    """Get current date in YYYY-MM-DD format."""
    return datetime.now().strftime("%Y-%m-%d")


def get_current_genome_version(session, seq_source: str) -> str | None:
    """Get the current genome version for a sequence source."""
    query = text(f"""
        SELECT genome_version
        FROM   {DB_SCHEMA}.genome_version
        WHERE  is_ver_current = 'Y'
        AND    genome_version_no in (
            SELECT distinct genome_version_no
            FROM   {DB_SCHEMA}.seq
            WHERE  is_seq_current = 'Y'
            AND    source = :seq_source
        )
    """)
    result = session.execute(query, {"seq_source": seq_source}).fetchone()
    return result[0] if result else None


def get_strain_config(session, strain_abbrev: str) -> dict | None:
    """Get strain configuration from organism table."""
    # Get strain info from organism table
    query = text(f"""
        SELECT o.taxon_id, o.organism_name
        FROM {DB_SCHEMA}.organism o
        WHERE o.organism_abbrev = :strain_abbrev
    """)
    result = session.execute(query, {"strain_abbrev": strain_abbrev}).fetchone()
    if not result:
        return None

    # For seq_source, we typically use organism name with specific formatting
    # This may need adjustment based on your actual data
    taxon_id = result[0]
    organism_name = result[1]

    # Try to get seq_source - typically follows pattern like "C. albicans SC5314 Assembly 22"
    seq_query = text(f"""
        SELECT DISTINCT s.source
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feat_location fl ON s.seq_no = fl.root_seq_no
        JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
        WHERE s.is_seq_current = 'Y'
        AND f.organism_abbrev = :strain_abbrev
        AND ROWNUM = 1
    """)
    seq_result = session.execute(seq_query, {"strain_abbrev": strain_abbrev}).fetchone()

    return {
        "taxon_id": taxon_id,
        "organism_name": organism_name,
        "seq_source": seq_result[0] if seq_result else None,
    }


def get_features(session, seq_source: str):
    """Get all features for a given sequence source."""
    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.dbxref_id, f.feature_type,
               f.gene_name, f.headline
        FROM   {DB_SCHEMA}.feature f
        JOIN   {DB_SCHEMA}.feat_location fl
               ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN   {DB_SCHEMA}.seq s
               ON (fl.root_seq_no = s.seq_no AND s.is_seq_current = 'Y'
                   AND s.source = :seq_source)
        JOIN   {DB_SCHEMA}.genome_version gv
               ON (s.genome_version_no = gv.genome_version_no
                   AND gv.is_ver_current = 'Y')
        WHERE  f.feature_type in ('ORF', 'ncRNA', 'rRNA', 'snRNA', 'snoRNA',
                                   'tRNA', 'pseudogene')
    """)
    return session.execute(query, {"seq_source": seq_source}).fetchall()


def get_aliases(session, feature_no: int) -> list[str]:
    """Get aliases for a feature."""
    query = text(f"""
        SELECT a.alias_name
        FROM   {DB_SCHEMA}.alias a
        JOIN   {DB_SCHEMA}.feat_alias fa
               ON (fa.alias_no = a.alias_no AND fa.feature_no = :feature_no)
    """)
    results = session.execute(query, {"feature_no": feature_no}).fetchall()
    return [row[0] for row in results if row[0]]


def get_uniprot_ids(session, feature_no: int) -> list[str]:
    """Get UniProt IDs for a feature."""
    query = text(f"""
        SELECT d.dbxref_id
        FROM   {DB_SCHEMA}.dbxref d
        JOIN   {DB_SCHEMA}.dbxref_feat df
               ON (d.dbxref_no = df.dbxref_no AND df.feature_no = :feature_no)
        WHERE  d.dbxref_type in ('SwissProt', 'UniProtKB')
    """)
    results = session.execute(query, {"feature_no": feature_no}).fetchall()
    return [row[0] for row in results if row[0]]


def clean_description(headline: str | None) -> str:
    """Clean and format the description from headline."""
    if not headline:
        return ""

    desc = headline
    # Extract first part before semicolon if present
    if ";" in desc:
        desc = desc.split(";")[0]

    # Remove HTML italic tags
    desc = desc.replace("<i>", "").replace("</i>", "")

    return desc


def generate_gpi_file(
    session,
    strain_abbrev: str,
    output_file: Path,
    seq_source: str,
    taxon_id: int,
    genome_version: str,
) -> int:
    """
    Generate GPI file for a strain.

    Returns the number of features written.
    """
    date_str = get_date()

    with open(output_file, "w") as gpi:
        # Write GPI 2.0 header
        gpi.write("!gpi-version: 2.0\n")
        gpi.write(f"!generated-by: {PROJECT_ACRONYM}\n")
        gpi.write(f"!date-generated: {date_str}\n")
        gpi.write(f"!URL: {PROJECT_URL}\n")
        gpi.write(f"!Project-release: {seq_source} genome version {genome_version}\n")

        features = get_features(session, seq_source)
        count = 0

        for row in features:
            feature_no, feature_name, dbxref_id, feature_type, gene_name, headline = row

            if not feature_no:
                continue

            # Format DBID
            dbid = f"{PROJECT_ACRONYM}:{dbxref_id}"

            # Get SO code for feature type
            so_code = CODE_FOR_TYPE.get(feature_type, "")

            # Format taxon
            taxon = f"NCBITaxon:{taxon_id}"

            # Clean description
            desc = clean_description(headline)

            # Build name list (gene name + aliases)
            names = []
            if gene_name:
                names.append(gene_name)
            aliases = get_aliases(session, feature_no)
            names.extend(aliases)
            name_list = " | ".join(names)

            # Get UniProt IDs for ORFs
            up_list = ""
            if feature_type == "ORF":
                uniprot_ids = get_uniprot_ids(session, feature_no)
                up_list = " | ".join(f"UniProtKB:{up}" for up in uniprot_ids)

            # Write GPI line (tab-separated)
            # GPI 2.0 columns:
            # 1. DB_Object_ID
            # 2. DB_Object_Symbol
            # 3. DB_Object_Name
            # 4. DB_Object_Synonym(s)
            # 5. DB_Object_Type
            # 6. Taxon
            # 7. Parent_Object_ID (empty)
            # 8. DB_Xref(s)
            # 9. Gene_Product_Properties (empty)
            # 10. Annotation_Target_Set (empty)
            gpi.write(
                f"{dbid}\t{feature_name}\t{desc}\t{name_list}\t{so_code}\t"
                f"{taxon}\t\t{dbid}\t\t{up_list}\t\n"
            )
            count += 1

    return count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate GPI (Gene Product Information) files for GO annotations"
    )
    parser.add_argument(
        "strain_abbrev",
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for GPI file (default: HTML_ROOT_DIR/download/go/)",
    )

    args = parser.parse_args()

    strain_abbrev = args.strain_abbrev
    output_dir = args.output_dir or HTML_ROOT_DIR / "download" / "go"
    archive_dir = output_dir / "archive"

    logger.info(f"Generating GPI file for {strain_abbrev}")

    # Ensure directories exist
    output_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)

    gpi_file = output_dir / f"{strain_abbrev}.gpi"

    with SessionLocal() as session:
        # Get strain configuration
        strain_config = get_strain_config(session, strain_abbrev)
        if not strain_config:
            logger.error(f"Strain {strain_abbrev} not found in database")
            return 1

        if not strain_config["seq_source"]:
            logger.error(f"No sequence source found for strain {strain_abbrev}")
            return 1

        seq_source = strain_config["seq_source"]
        taxon_id = strain_config["taxon_id"]

        # Get genome version
        genome_version = get_current_genome_version(session, seq_source)
        if not genome_version:
            logger.error(f"No current genome version found for {seq_source}")
            return 1

        logger.info(f"Sequence source: {seq_source}")
        logger.info(f"Taxon ID: {taxon_id}")
        logger.info(f"Genome version: {genome_version}")

        # Archive existing file if it exists
        if gpi_file.exists():
            date_tag = get_date().replace("-", "")
            archive_file = archive_dir / f"{strain_abbrev}_{date_tag}.gpi"
            logger.info(f"Archiving existing file to {archive_file}")
            shutil.move(str(gpi_file), str(archive_file))

        # Generate new GPI file
        count = generate_gpi_file(
            session,
            strain_abbrev,
            gpi_file,
            seq_source,
            taxon_id,
            genome_version,
        )

        logger.info(f"Generated {gpi_file} with {count} features")

    return 0


if __name__ == "__main__":
    sys.exit(main())
