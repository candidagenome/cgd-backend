#!/usr/bin/env python3
"""
Dump GO annotations to GAF (Gene Association File) format.

This script dumps Gene Ontology annotations from the database into
the gene_association.{project} flat file in GAF 2.0 format.

Based on dumpAnnotation.pl by Shuai Weng (Sept. 2000)
Updated for CGD by Prachi Shah (Nov 2007)
Updated for MULTI by Jon Binkley (Feb 2011)

Usage:
    python dump_go_annotation.py

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    HTML_ROOT_DIR: Root directory for HTML files
    TMP_DIR: Temporary directory
    LOG_DIR: Log directory
    PROJECT_ACRONYM: Project acronym (e.g., CGD)
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

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
PROJECT_NAME = os.getenv("PROJECT_NAME", "cgd")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def zero_pad_goid(goid: str | int) -> str:
    """Zero-pad a GO ID to 7 digits."""
    goid_str = str(goid)
    return goid_str.zfill(7)


class GOAnnotationDumper:
    """Dump GO annotations to GAF format."""

    def __init__(self, session):
        self.session = session

        # Data structures
        self.organism_nos: list[int] = []
        self.org_no_to_seq_source: dict[int, str] = {}
        self.org_no_to_taxon_id: dict[int, int] = {}
        self.org_no_to_gene_names: dict[int, list[str]] = {}

        self.feat_no_to_feat_name: dict[int, str] = {}
        self.feat_name_to_dbid: dict[str, str] = {}
        self.gene_name_to_feat_name: dict[int, dict[str, str]] = {}
        self.feat_name_to_annot: dict[str, str] = {}
        self.feat_name_to_alias_string: dict[str, str] = {}

        self.go_id_to_aspect: dict[str, str] = {}
        self.go_no_to_id: dict[int, str] = {}
        self.supports: dict[int, str] = {}

        # DB code to GO code mapping
        self.db_code_to_go_code: dict[str, str] = {}

    def get_tax_info(self) -> None:
        """Get taxonomic information for all species."""
        logger.info("Getting taxonomic information...")

        # Get all species
        query = text(f"""
            SELECT o.organism_no, o.organism_abbrev, o.taxon_id
            FROM {DB_SCHEMA}.organism o
            WHERE o.tax_rank = 'species'
        """)
        result = self.session.execute(query)

        for row in result:
            species_no, species_abbrev, species_taxon_id = row

            # Get default strain for this species
            strain_query = text(f"""
                SELECT o.organism_no, o.taxon_id
                FROM {DB_SCHEMA}.organism o
                WHERE o.organism_abbrev = :abbrev
            """)

            # Find the strain - for now, use the species itself
            # In the original Perl, this used config to find defaultStrainAbbrev
            strain_no = species_no
            strain_taxon_id = species_taxon_id

            self.organism_nos.append(strain_no)
            self.org_no_to_taxon_id[strain_no] = strain_taxon_id

            # Get seq_source for this organism
            seq_query = text(f"""
                SELECT DISTINCT fl.seq_source
                FROM {DB_SCHEMA}.feat_location fl
                JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
                WHERE f.organism_no = :org_no
                AND fl.is_loc_current = 'Y'
            """)
            seq_result = self.session.execute(
                seq_query, {"org_no": strain_no}
            ).first()

            if seq_result:
                self.org_no_to_seq_source[strain_no] = seq_result[0]

        logger.info(f"Found {len(self.organism_nos)} organisms")

    def get_feature_info(self) -> None:
        """Get feature information for all organisms."""
        logger.info("Getting feature information...")

        get_feat_query = text(f"""
            SELECT f.feature_no, f.feature_name, f.gene_name, f.dbxref_id
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
                AND fl.is_loc_current = 'Y'
            JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
                AND s.is_seq_current = 'Y'
                AND s.source = :seq_source
            JOIN {DB_SCHEMA}.genome_version gv ON s.genome_version_no = gv.genome_version_no
                AND gv.is_ver_current = 'Y'
            WHERE f.feature_type IN (
                SELECT col_value
                FROM {DB_SCHEMA}.web_metadata
                WHERE tab_name = 'FEATURE'
                AND col_name = 'FEATURE_TYPE'
                AND application_name = 'Chromosomal Feature Search'
            )
            AND f.feature_no NOT IN (
                SELECT DISTINCT fp.feature_no
                FROM {DB_SCHEMA}.feat_property fp
                WHERE property_type = 'feature_qualifier'
                AND (property_value LIKE 'Deleted%' OR property_value = 'Dubious')
            )
        """)

        get_alias_query = text(f"""
            SELECT a.alias_name
            FROM {DB_SCHEMA}.alias a
            JOIN {DB_SCHEMA}.feat_alias fa ON a.alias_no = fa.alias_no
            WHERE fa.feature_no = :feature_no
        """)

        for organism_no in self.organism_nos:
            seq_source = self.org_no_to_seq_source.get(organism_no)
            if not seq_source:
                continue

            self.org_no_to_gene_names[organism_no] = []
            self.gene_name_to_feat_name[organism_no] = {}

            result = self.session.execute(
                get_feat_query, {"seq_source": seq_source}
            )

            for row in result:
                feat_no, feat_name, gene_name, dbid = row

                self.feat_no_to_feat_name[feat_no] = feat_name
                self.feat_name_to_dbid[feat_name] = dbid

                display_name = gene_name or feat_name
                self.org_no_to_gene_names[organism_no].append(display_name)
                self.gene_name_to_feat_name[organism_no][display_name] = feat_name

                # Get aliases
                alias_result = self.session.execute(
                    get_alias_query, {"feature_no": feat_no}
                )
                aliases = [r[0] for r in alias_result if r[0]]
                if aliases:
                    self.feat_name_to_alias_string[feat_name] = "|".join(aliases)

        total_features = sum(
            len(names) for names in self.org_no_to_gene_names.values()
        )
        logger.info(f"Found {total_features} features")

    def load_db_code_mapping(self) -> None:
        """Load database code to GO code mapping."""
        mapping_file = DATA_DIR / "GO_DB_code_mapping"
        if mapping_file.exists():
            with open(mapping_file) as f:
                for line in f:
                    parts = line.strip().split("\t")
                    if len(parts) >= 3:
                        go_code, db_code, db_type = parts[:3]
                        key = f"{db_code.upper()}:{db_type.upper()}"
                        self.db_code_to_go_code[key] = go_code

    def get_go_info(self) -> None:
        """Get GO annotation information."""
        logger.info("Getting GO information...")

        # Get GO aspects
        go_aspect_query = text(f"""
            SELECT go_no, goid, go_aspect
            FROM {DB_SCHEMA}.go
        """)
        result = self.session.execute(go_aspect_query)
        for row in result:
            go_no, goid, aspect = row
            self.go_id_to_aspect[goid] = aspect
            self.go_no_to_id[go_no] = goid

        # Get GO references
        go_ref_query = text(f"""
            SELECT gr.go_ref_no, r.dbxref_id, r.pubmed, gr.go_annotation_no,
                   gr.date_created
            FROM {DB_SCHEMA}.go_ref gr
            JOIN {DB_SCHEMA}.reference r ON gr.reference_no = r.reference_no
        """)
        result = self.session.execute(go_ref_query)
        annot_rows = result.fetchall()

        # Get GO annotations
        go_annot_query = text(f"""
            SELECT go_no, feature_no, go_evidence, source
            FROM {DB_SCHEMA}.go_annotation
            WHERE go_annotation_no = :anno_no
        """)

        # Get GO qualifiers
        has_qual_query = text(f"""
            SELECT has_qualifier
            FROM {DB_SCHEMA}.go_ref
            WHERE go_ref_no = :ref_no
        """)

        qual_query = text(f"""
            SELECT qualifier
            FROM {DB_SCHEMA}.go_qualifier
            WHERE go_ref_no = :ref_no
        """)

        for row in annot_rows:
            go_ref_no, ref_no, pubmed, go_anno_no, date_created = row

            # Format date
            if date_created:
                date_str = date_created.strftime("%Y%m%d") if hasattr(
                    date_created, 'strftime'
                ) else str(date_created).replace("-", "")
            else:
                date_str = ""

            # Get annotation details
            anno_result = self.session.execute(
                go_annot_query, {"anno_no": go_anno_no}
            ).first()
            if not anno_result:
                continue

            go_no, feat_no, go_ev_code, source = anno_result
            goid = self.go_no_to_id.get(go_no)
            if not goid:
                continue

            # Check for NOT qualifier
            is_not = ""
            has_qual_result = self.session.execute(
                has_qual_query, {"ref_no": go_ref_no}
            ).first()
            if has_qual_result and has_qual_result[0] and has_qual_result[0].upper() == 'Y':
                qual_result = self.session.execute(
                    qual_query, {"ref_no": go_ref_no}
                ).first()
                if qual_result:
                    is_not = qual_result[0] or ""

            feat_name = self.feat_no_to_feat_name.get(feat_no)
            if not feat_name:
                continue

            annot_str = f"{goid}::{go_ref_no}::{ref_no}::{pubmed or ''}::{go_ev_code}::{is_not}::{date_str}::{source or ''}"

            if feat_name in self.feat_name_to_annot:
                self.feat_name_to_annot[feat_name] += f"\t{annot_str}"
            else:
                self.feat_name_to_annot[feat_name] = annot_str

        # Get support evidence (with annotation)
        self.load_db_code_mapping()

        dbcode_query = text(f"""
            SELECT gr.go_ref_no, d.source, d.dbxref_type, d.dbxref_id
            FROM {DB_SCHEMA}.go_ref gr
            JOIN {DB_SCHEMA}.goref_dbxref gd ON gd.go_ref_no = gr.go_ref_no
            JOIN {DB_SCHEMA}.dbxref d ON gd.dbxref_no = d.dbxref_no
            ORDER BY gr.go_ref_no, d.source
        """)

        result = self.session.execute(dbcode_query)
        for row in result:
            go_ref_no, source, dbxref_type, dbxref_id = row

            key = f"{source.upper() if source else ''}:{dbxref_type.upper() if dbxref_type else ''}"
            go_code = self.db_code_to_go_code.get(key, source or "")

            # Format GO ID with padding
            if go_code == "GO":
                dbxref_id = zero_pad_goid(dbxref_id)

            support_str = f"{go_code}:{dbxref_id}"

            if go_ref_no in self.supports:
                self.supports[go_ref_no] += f"|{support_str}"
            else:
                self.supports[go_ref_no] = support_str

        logger.info(f"Found {len(self.feat_name_to_annot)} annotated features")

    def get_gaf_header(self, filename: str) -> str:
        """Generate GAF file header."""
        date_str = datetime.now().strftime("%Y-%m-%d")
        header = []
        header.append("!gaf-version: 2.0")
        header.append(f"!{PROJECT_ACRONYM} Gene Ontology Annotation File")
        header.append(f"!Date generated: {date_str}")
        header.append(f"!Generated by: {PROJECT_ACRONYM}")
        header.append(f"!Filename: {filename}")
        header.append("!")
        return "\n".join(header) + "\n"

    def write_file(self, output_file: Path) -> int:
        """Write GAF file."""
        logger.info(f"Writing GAF file to {output_file}")

        output_file.parent.mkdir(parents=True, exist_ok=True)
        count = {}
        loop_count = 0

        with open(output_file, "w") as f:
            # Write header
            f.write(self.get_gaf_header(output_file.name))

            for org_no in self.organism_nos:
                taxon_id = self.org_no_to_taxon_id.get(org_no)
                if not taxon_id:
                    continue

                gene_names = self.org_no_to_gene_names.get(org_no, [])

                for gene_name in sorted(gene_names):
                    feat_name = self.gene_name_to_feat_name.get(
                        org_no, {}
                    ).get(gene_name)
                    if not feat_name:
                        continue

                    annots = self.feat_name_to_annot.get(feat_name, "")
                    if not annots:
                        continue

                    dbid = self.feat_name_to_dbid.get(feat_name)
                    if not dbid:
                        continue

                    for annot in annots.split("\t"):
                        if not annot:
                            continue

                        count_key = f"{annot}:{feat_name}"
                        if count_key in count:
                            continue
                        count[count_key] = True

                        parts = annot.split("::")
                        if len(parts) < 8:
                            continue

                        (goid, go_ref_no, ref_no, pubmed, go_ev_code,
                         is_not, date_created, source) = parts[:8]

                        go_aspect = self.go_id_to_aspect.get(goid, "")
                        goid = zero_pad_goid(goid)

                        alias_str = self.feat_name_to_alias_string.get(
                            feat_name, ""
                        )
                        if alias_str:
                            alias_str = f"{feat_name}|{alias_str}"
                        else:
                            alias_str = feat_name

                        ref_no = ref_no.replace(" ", "")
                        pubmed_str = f"|PMID:{pubmed}" if pubmed else ""

                        support = self.supports.get(int(go_ref_no), "")

                        # GAF 2.0 format
                        row = [
                            PROJECT_ACRONYM,      # DB
                            dbid,                 # DB_Object_ID
                            gene_name,            # DB_Object_Symbol
                            is_not,               # Qualifier
                            f"GO:{goid}",         # GO_ID
                            f"{PROJECT_ACRONYM}_REF:{ref_no}{pubmed_str}",  # DB:Reference
                            go_ev_code,           # Evidence Code
                            support,              # With/From
                            go_aspect,            # Aspect
                            "",                   # DB_Object_Name
                            alias_str,            # DB_Object_Synonym
                            "gene_product",       # DB_Object_Type
                            f"taxon:{taxon_id}",  # Taxon
                            date_created,         # Date
                            source or PROJECT_ACRONYM,  # Assigned_By
                            "",                   # Annotation_Extension (GAF 2.0)
                            "",                   # Gene_Product_Form_ID (GAF 2.0)
                        ]

                        f.write("\t".join(row) + "\n")
                        loop_count += 1

        logger.info(f"Wrote {loop_count} records to {output_file}")
        return loop_count

    def run(self, output_file: Path) -> int:
        """Run the full annotation dump."""
        self.get_tax_info()
        self.get_feature_info()
        self.get_go_info()
        return self.write_file(output_file)


def dump_go_annotation() -> bool:
    """
    Main function to dump GO annotations.

    Returns:
        True on success, False on failure
    """
    logger.info("Starting GO annotation dump...")

    try:
        # Set up paths
        data_file = f"gene_association.{PROJECT_ACRONYM.lower()}"
        output_file = TMP_DIR / data_file
        public_dir = HTML_ROOT_DIR / "download" / "go"
        archive_dir = public_dir / "archive"

        # Create directories
        public_dir.mkdir(parents=True, exist_ok=True)
        archive_dir.mkdir(parents=True, exist_ok=True)

        with SessionLocal() as session:
            dumper = GOAnnotationDumper(session)
            count = dumper.run(output_file)

        if count == 0:
            logger.warning("No annotations written")
            return False

        # Gzip the output file
        gzip_file = Path(str(output_file) + ".gz")
        with open(output_file, "rb") as f_in:
            with gzip.open(gzip_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Archive old file if exists
        public_file = public_dir / f"{data_file}.gz"
        if public_file.exists():
            date_tag = datetime.now().strftime("%Y%m%d")
            archive_file = archive_dir / f"{data_file}_{date_tag}.gz"
            shutil.move(public_file, archive_file)
            logger.info(f"Archived old file to {archive_file}")

        # Copy new file to public directory
        shutil.copy(gzip_file, public_file)
        logger.info(f"Copied new file to {public_file}")

        # Clean up temp files
        output_file.unlink(missing_ok=True)
        gzip_file.unlink(missing_ok=True)

        logger.info("GO annotation dump complete")
        return True

    except Exception as e:
        logger.exception(f"Error dumping GO annotations: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump GO annotations to GAF format"
    )
    parser.parse_args()

    success = dump_go_annotation()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
