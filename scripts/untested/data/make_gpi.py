#!/usr/bin/env python3
"""
Generate GPI (Gene Product Information) file for GO.

This script creates a GPI 2.0 format file containing gene product information
for the Gene Ontology Consortium. The file includes:
- Gene/feature identifiers
- Gene names and aliases
- Feature types (with SO codes)
- UniProt cross-references
- Gene descriptions

Usage:
    python make_gpi.py --strain C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    HTML_ROOT_DIR: Root directory for HTML files
    PROJECT_ACRONYM: Project acronym (e.g., CGD)
"""

import argparse
import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import TextIO

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")

# SO (Sequence Ontology) codes for feature types
FEATURE_TYPE_SO_CODES = {
    "ORF": "SO:0001217",
    "ncRNA": "SO:0001263",
    "rRNA": "SO:0001263",
    "snRNA": "SO:0001263",
    "snoRNA": "SO:0001263",
    "tRNA": "SO:0001263",
    "pseudogene": "SO:0000336",
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class GPIGenerator:
    """Generate GPI files for Gene Ontology."""

    def __init__(self, session, strain_abbrev: str):
        self.session = session
        self.strain_abbrev = strain_abbrev
        self.taxon_id = None
        self.seq_source = None
        self.genome_version = None

    def get_strain_info(self) -> bool:
        """Get strain information from database."""
        query = text(f"""
            SELECT o.taxon_id
            FROM {DB_SCHEMA}.organism o
            WHERE o.organism_abbrev = :strain_abbrev
        """)

        result = self.session.execute(
            query, {"strain_abbrev": self.strain_abbrev}
        ).first()

        if result:
            self.taxon_id = result[0]
            return True
        return False

    def get_seq_source(self) -> str | None:
        """Get sequence source for the strain."""
        query = text(f"""
            SELECT DISTINCT fl.seq_source
            FROM {DB_SCHEMA}.feat_location fl
            JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_abbrev = :strain_abbrev
            AND fl.is_loc_current = 'Y'
        """)

        result = self.session.execute(
            query, {"strain_abbrev": self.strain_abbrev}
        ).first()

        if result:
            self.seq_source = result[0]
            return self.seq_source
        return None

    def get_genome_version(self) -> str | None:
        """Get current genome version for the seq_source."""
        query = text(f"""
            SELECT gv.genome_version
            FROM {DB_SCHEMA}.genome_version gv
            WHERE gv.is_ver_current = 'Y'
            AND gv.genome_version_no IN (
                SELECT DISTINCT s.genome_version_no
                FROM {DB_SCHEMA}.seq s
                WHERE s.is_seq_current = 'Y'
                AND s.source = :seq_source
            )
        """)

        result = self.session.execute(
            query, {"seq_source": self.seq_source}
        ).first()

        if result:
            self.genome_version = result[0]
            return self.genome_version
        return None

    def get_features(self) -> list[dict]:
        """Get features for GPI file."""
        query = text(f"""
            SELECT f.feature_no, f.feature_name, f.dbxref_id, f.feature_type,
                   f.gene_name, f.headline
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
                AND fl.is_loc_current = 'Y'
            JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
                AND s.is_seq_current = 'Y'
                AND s.source = :seq_source
            JOIN {DB_SCHEMA}.genome_version gv ON s.genome_version_no = gv.genome_version_no
                AND gv.is_ver_current = 'Y'
            WHERE f.feature_type IN ('ORF', 'ncRNA', 'rRNA', 'snRNA', 'snoRNA', 'tRNA', 'pseudogene')
            ORDER BY f.feature_name
        """)

        result = self.session.execute(query, {"seq_source": self.seq_source})

        features = []
        for row in result:
            features.append({
                "feature_no": row[0],
                "feature_name": row[1],
                "dbxref_id": row[2],
                "feature_type": row[3],
                "gene_name": row[4],
                "headline": row[5],
            })

        return features

    def get_aliases(self, feature_no: int) -> list[str]:
        """Get aliases for a feature."""
        query = text(f"""
            SELECT a.alias_name
            FROM {DB_SCHEMA}.alias a
            JOIN {DB_SCHEMA}.feat_alias fa ON fa.alias_no = a.alias_no
            WHERE fa.feature_no = :feature_no
        """)

        result = self.session.execute(query, {"feature_no": feature_no})
        return [row[0] for row in result if row[0]]

    def get_uniprot_ids(self, feature_no: int) -> list[str]:
        """Get UniProt cross-references for a feature."""
        query = text(f"""
            SELECT d.dbxref_id
            FROM {DB_SCHEMA}.dbxref d
            JOIN {DB_SCHEMA}.dbxref_feat df ON d.dbxref_no = df.dbxref_no
            WHERE df.feature_no = :feature_no
            AND d.dbxref_type IN ('SwissProt', 'UniProtKB')
        """)

        result = self.session.execute(query, {"feature_no": feature_no})
        return [row[0] for row in result if row[0]]

    def clean_description(self, headline: str | None) -> str:
        """Clean headline for use as description."""
        if not headline:
            return ""

        # Extract first part before semicolon
        if ";" in headline:
            desc = headline.split(";")[0].strip()
        else:
            desc = headline

        # Remove HTML tags
        import re
        desc = re.sub(r"<[^>]+>", "", desc)

        return desc

    def write_header(self, f: TextIO) -> None:
        """Write GPI file header."""
        date_str = datetime.now().strftime("%Y-%m-%d")

        f.write("!gpi-version: 2.0\n")
        f.write(f"!generated-by: {PROJECT_ACRONYM}\n")
        f.write(f"!date-generated: {date_str}\n")
        f.write(f"!URL: http://www.candidagenome.org\n")
        f.write(f"!Project-release: {self.seq_source} genome version {self.genome_version}\n")

    def write_feature(self, f: TextIO, feature: dict) -> None:
        """Write a single feature to GPI file."""
        dbid = f"{PROJECT_ACRONYM}:{feature['dbxref_id']}"
        feature_name = feature["feature_name"]
        feature_type = feature["feature_type"]

        # Get SO code
        so_code = FEATURE_TYPE_SO_CODES.get(feature_type, "")

        # Get taxon
        taxon = f"NCBITaxon:{self.taxon_id}"

        # Get description
        description = self.clean_description(feature["headline"])

        # Build name list (gene name + aliases)
        names = []
        if feature["gene_name"]:
            names.append(feature["gene_name"])

        aliases = self.get_aliases(feature["feature_no"])
        names.extend(aliases)

        name_list = " | ".join(names) if names else ""

        # Get UniProt IDs (only for ORFs)
        uniprot_list = ""
        if feature_type == "ORF":
            uniprot_ids = self.get_uniprot_ids(feature["feature_no"])
            if uniprot_ids:
                uniprot_list = " | ".join(f"UniProtKB:{up}" for up in uniprot_ids)

        # Write GPI row
        # Format: DB_Object_ID, DB_Object_Symbol, DB_Object_Name, DB_Object_Synonym,
        #         DB_Object_Type, DB_Object_Taxon, Encoded_by, Parent_Protein,
        #         Protein_Containing_Complex_Members, DB_Xref(s), Gene_Product_Properties
        row = [
            dbid,                # DB_Object_ID
            feature_name,        # DB_Object_Symbol
            description,         # DB_Object_Name
            name_list,           # DB_Object_Synonym
            so_code,             # DB_Object_Type
            taxon,               # DB_Object_Taxon
            "",                  # Encoded_by
            dbid,                # Parent_Protein
            "",                  # Protein_Containing_Complex_Members
            uniprot_list,        # DB_Xref(s)
            "",                  # Gene_Product_Properties
        ]

        f.write("\t".join(row) + "\n")

    def generate_gpi(self, output_file: Path) -> int:
        """
        Generate GPI file.

        Args:
            output_file: Path to output file

        Returns:
            Number of features written
        """
        if not self.get_strain_info():
            raise ValueError(f"Strain {self.strain_abbrev} not found")

        if not self.get_seq_source():
            raise ValueError(f"No sequence source found for {self.strain_abbrev}")

        self.get_genome_version()

        features = self.get_features()
        logger.info(f"Found {len(features)} features")

        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w") as f:
            self.write_header(f)

            for feature in features:
                self.write_feature(f, feature)

        logger.info(f"Wrote {len(features)} features to {output_file}")
        return len(features)


def make_gpi(strain_abbrev: str) -> bool:
    """
    Main function to generate GPI file.

    Args:
        strain_abbrev: Strain abbreviation

    Returns:
        True on success, False on failure
    """
    logger.info(f"Generating GPI file for {strain_abbrev}")

    try:
        with SessionLocal() as session:
            generator = GPIGenerator(session, strain_abbrev)

            # Output directories
            download_dir = HTML_ROOT_DIR / "download" / "go"
            archive_dir = download_dir / "archive"
            archive_dir.mkdir(parents=True, exist_ok=True)

            output_file = download_dir / f"{strain_abbrev}.gpi"

            # Archive existing file
            if output_file.exists():
                date_tag = datetime.now().strftime("%Y%m%d")
                archive_file = archive_dir / f"{strain_abbrev}_{date_tag}.gpi"
                shutil.move(output_file, archive_file)
                logger.info(f"Archived existing file to {archive_file}")

            # Generate new file
            count = generator.generate_gpi(output_file)

            logger.info(f"GPI generation complete: {count} features")
            return True

    except Exception as e:
        logger.exception(f"Error generating GPI file: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate GPI file for Gene Ontology"
    )
    parser.add_argument(
        "--strain",
        required=True,
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )

    args = parser.parse_args()

    success = make_gpi(args.strain)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
