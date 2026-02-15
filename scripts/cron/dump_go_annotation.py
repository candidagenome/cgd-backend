#!/usr/bin/env python3
"""
Dump GO annotations to gene_association (GAF) file format.

This script exports GO (Gene Ontology) annotations from the database to the
standard GAF 2.0 (Gene Association File) format for submission to the GO
Consortium and for public download.

Based on dumpAnnotation.pl by Shuai Weng (Sept 2000).
Updated for MULTI by Jon Binkley (Feb 2011).

Usage:
    python dump_go_annotation.py
    python dump_go_annotation.py --output-dir /path/to/output

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    HTML_ROOT_DIR: Root directory for public download files
    TMP_DIR: Temporary directory
    LOG_DIR: Directory for log files
    CURATOR_EMAIL: Email for notifications
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
PROJECT_NAME = os.getenv("PROJECT_NAME", "CGD")
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL", "")

# Project-specific reference IDs for orthology and domain-based annotations
ORTHOLOGY_REF = {
    "AspGD": "ASPL0000000005",
    "CGD": "CAL0121033",
}
DOMAIN_REF = {
    "AspGD": "ASPL0000166200",
    "CGD": "CAL0142013",
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def send_error_email(subject: str, message: str) -> None:
    """Send error notification email."""
    if not CURATOR_EMAIL:
        logger.warning("CURATOR_EMAIL not set, skipping email notification")
        return
    logger.error(f"Email notification: {subject}")
    logger.error(f"Message: {message}")


def zero_pad_goid(goid: str) -> str:
    """Pad GO ID to 7 digits with leading zeros."""
    goid = str(goid).replace("GO:", "")
    return goid.zfill(7)


class GoAnnotationDumper:
    """Dump GO annotations to GAF format."""

    def __init__(self, session):
        self.session = session

        # Data caches
        self.organism_nos: list[int] = []
        self.org_no_to_seq_source: dict[int, str] = {}
        self.org_no_to_taxon_id: dict[int, int] = {}

        self.feat_no_to_feat_name: dict[int, str] = {}
        self.feat_name_to_dbid: dict[str, str] = {}
        self.gene_name_to_feat_name: dict[int, dict[str, str]] = {}  # org_no -> gene_name -> feat_name
        self.org_no_to_gene_names: dict[int, list[str]] = {}
        self.feat_name_to_alias_string: dict[str, str] = {}

        self.go_id_to_aspect: dict[str, str] = {}
        self.go_no_to_go_id: dict[int, str] = {}
        self.feat_name_to_annotations: dict[str, list[dict]] = {}
        self.supports: dict[int, str] = {}

    def get_tax_info(self) -> None:
        """Get taxonomic information for all species."""
        logger.info("Getting taxonomic information...")

        # Get all species
        species_query = text(f"""
            SELECT organism_no, organism_abbrev, taxon_id
            FROM {DB_SCHEMA}.organism
            WHERE tax_rank = 'Species'
        """)
        species_rows = self.session.execute(species_query).fetchall()

        for species_row in species_rows:
            species_abbrev = species_row[1]

            # Get default strain for this species
            strain_query = text(f"""
                SELECT o.organism_no, o.taxon_id
                FROM {DB_SCHEMA}.organism o
                WHERE o.parent_organism_no = :species_no
                AND o.tax_rank = 'Strain'
                ORDER BY o.organism_no
                FETCH FIRST 1 ROW ONLY
            """)
            strain_row = self.session.execute(
                strain_query, {"species_no": species_row[0]}
            ).fetchone()

            if strain_row:
                org_no = strain_row[0]
                self.organism_nos.append(org_no)
                self.org_no_to_taxon_id[org_no] = strain_row[1]

                # Get seq_source for this strain
                seq_query = text(f"""
                    SELECT DISTINCT s.source
                    FROM {DB_SCHEMA}.seq s
                    JOIN {DB_SCHEMA}.feat_location fl ON s.seq_no = fl.root_seq_no
                    JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
                    WHERE s.is_seq_current = 'Y'
                    AND f.organism_no = :org_no
                    FETCH FIRST 1 ROW ONLY
                """)
                seq_row = self.session.execute(seq_query, {"org_no": org_no}).fetchone()
                if seq_row:
                    self.org_no_to_seq_source[org_no] = seq_row[0]

        logger.info(f"Found {len(self.organism_nos)} organisms")

    def get_feat_info(self) -> None:
        """Get feature information for all organisms."""
        logger.info("Getting feature information...")

        feat_query = text(f"""
            SELECT f.feature_no, f.feature_name, f.gene_name, f.dbxref_id
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_location fl
                ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
            JOIN {DB_SCHEMA}.seq s
                ON (fl.root_seq_no = s.seq_no AND s.is_seq_current = 'Y' AND s.source = :seq_source)
            JOIN {DB_SCHEMA}.genome_version gv
                ON (s.genome_version_no = gv.genome_version_no AND gv.is_ver_current = 'Y')
            WHERE f.feature_type IN (
                SELECT col_value
                FROM {DB_SCHEMA}.web_metadata
                WHERE tab_name = 'FEATURE'
                AND col_name = 'FEATURE_TYPE'
                AND application_name = 'Chromosomal Feature Search')
            AND f.feature_no NOT IN (
                SELECT DISTINCT fp.feature_no
                FROM {DB_SCHEMA}.feat_property fp
                WHERE property_type = 'feature_qualifier'
                AND (property_value LIKE 'Deleted%' OR property_value = 'Dubious'))
        """)

        alias_query = text(f"""
            SELECT a.alias_name
            FROM {DB_SCHEMA}.alias a
            JOIN {DB_SCHEMA}.feat_alias fa ON (a.alias_no = fa.alias_no AND fa.feature_no = :feat_no)
        """)

        for org_no in self.organism_nos:
            seq_source = self.org_no_to_seq_source.get(org_no)
            if not seq_source:
                continue

            self.gene_name_to_feat_name[org_no] = {}
            self.org_no_to_gene_names[org_no] = []

            feat_rows = self.session.execute(feat_query, {"seq_source": seq_source}).fetchall()

            for feat_no, feat_name, gene_name, dbxref_id in feat_rows:
                self.feat_no_to_feat_name[feat_no] = feat_name
                self.feat_name_to_dbid[feat_name] = dbxref_id

                gene = gene_name or feat_name
                self.org_no_to_gene_names[org_no].append(gene)
                self.gene_name_to_feat_name[org_no][gene] = feat_name

                # Get aliases
                alias_rows = self.session.execute(alias_query, {"feat_no": feat_no}).fetchall()
                aliases = [row[0] for row in alias_rows if row[0]]
                if aliases:
                    self.feat_name_to_alias_string[feat_name] = "|".join(aliases)

        logger.info(f"Found {len(self.feat_no_to_feat_name)} features")

    def get_go_info(self) -> None:
        """Get GO annotation information."""
        logger.info("Getting GO annotation information...")

        # Get GO aspects
        aspect_query = text(f"""
            SELECT go_no, goid, go_aspect
            FROM {DB_SCHEMA}.go
        """)
        for go_no, goid, go_aspect in self.session.execute(aspect_query).fetchall():
            self.go_id_to_aspect[goid] = go_aspect
            self.go_no_to_go_id[go_no] = goid

        # Get GO references
        ref_query = text(f"""
            SELECT gr.go_ref_no, r.dbxref_id, r.pubmed, gr.go_annotation_no, gr.date_created
            FROM {DB_SCHEMA}.go_ref gr
            JOIN {DB_SCHEMA}.reference r ON gr.reference_no = r.reference_no
        """)

        annot_query = text(f"""
            SELECT go_no, feature_no, go_evidence, source
            FROM {DB_SCHEMA}.go_annotation
            WHERE go_annotation_no = :annot_no
        """)

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

        ref_rows = self.session.execute(ref_query).fetchall()

        for go_ref_no, ref_no, pubmed, go_annot_no, date_created in ref_rows:
            date_str = date_created.strftime("%Y%m%d") if date_created else ""

            annot_row = self.session.execute(
                annot_query, {"annot_no": go_annot_no}
            ).fetchone()

            if not annot_row:
                continue

            go_no, feat_no, go_ev_code, source = annot_row
            goid = self.go_no_to_go_id.get(go_no, "")

            # Check for NOT qualifier
            has_qual_row = self.session.execute(
                has_qual_query, {"ref_no": go_ref_no}
            ).fetchone()
            is_not = ""
            if has_qual_row and has_qual_row[0] and has_qual_row[0].upper() == "Y":
                qual_row = self.session.execute(qual_query, {"ref_no": go_ref_no}).fetchone()
                if qual_row:
                    is_not = qual_row[0] or ""

            feat_name = self.feat_no_to_feat_name.get(feat_no)
            if not feat_name:
                continue  # Skip dubious ORFs

            if feat_name not in self.feat_name_to_annotations:
                self.feat_name_to_annotations[feat_name] = []

            self.feat_name_to_annotations[feat_name].append({
                "goid": goid,
                "go_ref_no": go_ref_no,
                "ref_no": ref_no,
                "pubmed": pubmed,
                "go_ev_code": go_ev_code,
                "is_not": is_not,
                "date_created": date_str,
                "source": source,
            })

        # Get support/with information
        support_query = text(f"""
            SELECT gr.go_ref_no, d.source, d.dbxref_type, d.dbxref_id
            FROM {DB_SCHEMA}.go_ref gr
            JOIN {DB_SCHEMA}.goref_dbxref gd ON gd.go_ref_no = gr.go_ref_no
            JOIN {DB_SCHEMA}.dbxref d ON gd.dbxref_no = d.dbxref_no
            ORDER BY gr.go_ref_no, d.source
        """)

        for go_ref_no, source, dbxref_type, dbxref_id in self.session.execute(support_query).fetchall():
            # Format the support string
            go_code = source  # Default to source as code
            if go_code == "GO":
                dbxref_id = zero_pad_goid(dbxref_id)

            support_str = f"{go_code}:{dbxref_id}"

            if go_ref_no in self.supports:
                self.supports[go_ref_no] += f"|{support_str}"
            else:
                self.supports[go_ref_no] = support_str

        logger.info(f"Found annotations for {len(self.feat_name_to_annotations)} features")

    def get_gaf_header(self) -> str:
        """Generate GAF file header."""
        header_lines = [
            f"!gaf-version: 2.0",
            f"!Project: {PROJECT_ACRONYM}",
            f"!Date: {datetime.now().strftime('%Y-%m-%d')}",
            f"!Generated by: dump_go_annotation.py",
            "",
        ]
        return "\n".join(header_lines)

    def write_gaf_file(self, output_file: Path) -> int:
        """
        Write annotations to GAF file.

        Returns the number of records written.
        """
        logger.info(f"Writing GAF file: {output_file}")

        count = 0
        seen = set()

        with open(output_file, "w") as f:
            # Write header
            f.write(self.get_gaf_header())

            for org_no in self.organism_nos:
                taxon_id = self.org_no_to_taxon_id.get(org_no)
                if not taxon_id:
                    continue

                gene_names = self.org_no_to_gene_names.get(org_no, [])

                for gene_name in sorted(gene_names):
                    feat_name = self.gene_name_to_feat_name.get(org_no, {}).get(gene_name)
                    if not feat_name:
                        continue

                    dbid = self.feat_name_to_dbid.get(feat_name)
                    if not dbid:
                        continue

                    annotations = self.feat_name_to_annotations.get(feat_name, [])

                    for annot in annotations:
                        # Skip duplicates
                        key = f"{annot['goid']}:{annot['go_ref_no']}:{feat_name}"
                        if key in seen:
                            continue
                        seen.add(key)

                        goid = zero_pad_goid(annot["goid"])
                        go_aspect = self.go_id_to_aspect.get(annot["goid"], "")

                        # Build alias string
                        alias_str = self.feat_name_to_alias_string.get(feat_name, "")
                        if alias_str:
                            alias_str = f"{feat_name}|{alias_str}"
                        else:
                            alias_str = feat_name

                        # Build reference string
                        ref_str = f"{PROJECT_ACRONYM}_REF:{annot['ref_no']}"
                        if annot["pubmed"]:
                            ref_str += f"|PMID:{annot['pubmed']}"

                        # Get support/with info
                        support = self.supports.get(annot["go_ref_no"], "")

                        # GAF 2.0 format (17 columns)
                        columns = [
                            PROJECT_ACRONYM,          # 1. DB
                            dbid,                     # 2. DB Object ID
                            gene_name,                # 3. DB Object Symbol
                            annot["is_not"],          # 4. Qualifier
                            f"GO:{goid}",             # 5. GO ID
                            ref_str,                  # 6. DB:Reference
                            annot["go_ev_code"],      # 7. Evidence Code
                            support,                  # 8. With/From
                            go_aspect,                # 9. Aspect
                            "",                       # 10. DB Object Name
                            alias_str,                # 11. DB Object Synonym
                            "gene_product",           # 12. DB Object Type
                            f"taxon:{taxon_id}",      # 13. Taxon
                            annot["date_created"],    # 14. Date
                            annot["source"],          # 15. Assigned By
                            "",                       # 16. Annotation Extension
                            "",                       # 17. Gene Product Form ID
                        ]

                        f.write("\t".join(columns) + "\n")
                        count += 1

        logger.info(f"Wrote {count} annotation records")
        return count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump GO annotations to gene_association (GAF) file format"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=HTML_ROOT_DIR / "download" / "go",
        help="Output directory for GAF file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write to temp directory only, don't update public files",
    )

    args = parser.parse_args()

    # Set up file logging
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "dumpAnnotation.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Starting GO annotation dump at {datetime.now()}")

    data_file = f"gene_association.{PROJECT_ACRONYM.lower()}"

    try:
        with SessionLocal() as session:
            dumper = GoAnnotationDumper(session)

            # Gather data
            dumper.get_tax_info()
            dumper.get_feat_info()
            dumper.get_go_info()

            # Write to temp file first
            TMP_DIR.mkdir(parents=True, exist_ok=True)
            temp_file = TMP_DIR / data_file
            count = dumper.write_gaf_file(temp_file)

            if args.dry_run:
                logger.info(f"DRY RUN - output written to {temp_file}")
                return 0

            # Move to public directory
            args.output_dir.mkdir(parents=True, exist_ok=True)
            public_file = args.output_dir / data_file

            # Archive old file if it exists
            if public_file.exists():
                archive_dir = args.output_dir / "archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                date_str = datetime.now().strftime("%Y%m%d")
                archive_file = archive_dir / f"{data_file}.{date_str}"
                shutil.copy2(public_file, archive_file)
                logger.info(f"Archived old file to {archive_file}")

            # Copy new file
            shutil.copy2(temp_file, public_file)
            logger.info(f"Copied to {public_file}")

            # Create gzipped version
            gz_file = public_file.with_suffix(public_file.suffix + ".gz")
            with open(public_file, "rb") as f_in:
                with gzip.open(gz_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            logger.info(f"Created gzipped file: {gz_file}")

            # Clean up temp file
            temp_file.unlink()

        logger.info(f"Completed at {datetime.now()}")
        return 0

    except Exception as e:
        error_msg = f"Error dumping GO annotations: {e}"
        logger.error(error_msg)
        send_error_email("Error in dumpAnnotation", error_msg)
        return 1

    finally:
        logger.removeHandler(file_handler)
        file_handler.close()


if __name__ == "__main__":
    sys.exit(main())
