#!/usr/bin/env python3
"""
Dump GO Slim annotations to GAF format.

This script dumps GO Slim annotations from the database into the
GOslim_gene_association.{project} flat file.

Based on dumpGOSlimAnnotation.pl by Prachi Shah (Apr 2008)
Updated for MULTI by Jon Binkley (Feb 2011)

Usage:
    python dump_go_slim_annotation.py

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


class GOSlimAnnotationDumper:
    """Dump GO Slim annotations to GAF format."""

    def __init__(self, session, go_slim_set: str | None = None):
        self.session = session
        self.go_slim_set = go_slim_set

        # Data structures
        self.organism_nos: list[int] = []
        self.org_no_to_seq_source: dict[int, str] = {}
        self.org_no_to_taxon_id: dict[int, int] = {}
        self.org_no_to_gene_names: dict[int, list[str]] = {}
        self.genus_names: set[str] = set()

        self.feat_no_to_feat_name: dict[int, str] = {}
        self.feat_name_to_dbid: dict[str, str] = {}
        self.gene_name_to_feat_name: dict[int, dict[str, str]] = {}
        self.feat_name_to_annot: dict[str, str] = {}
        self.feat_name_to_alias_string: dict[str, str] = {}

        self.go_id_to_aspect: dict[str, str] = {}
        self.go_no_to_id: dict[int, str] = {}
        self.go_id_to_no: dict[str, int] = {}
        self.supports: dict[int, str] = {}

        # GO Slim mapping
        self.inuse_go_id_to_slim_parent: dict[str, str] = {}
        self.slim_goid_to_aspect: dict[str, str] = {}
        self.slim_go_nos_for_child_go_no: dict[int, set[int]] = {}
        self.goid_for_slim_go_no: dict[int, str] = {}

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

            # Get genus name
            genus_query = text(f"""
                SELECT p.organism_name
                FROM {DB_SCHEMA}.organism o
                JOIN {DB_SCHEMA}.organism p ON o.parent_no = p.organism_no
                WHERE o.organism_no = :species_no
                AND p.tax_rank = 'genus'
            """)
            genus_result = self.session.execute(
                genus_query, {"species_no": species_no}
            ).first()
            if genus_result:
                self.genus_names.add(genus_result[0])

            # For now, use species as the organism
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

        # Determine GO Slim set name from genus
        if not self.go_slim_set and self.genus_names:
            # Use first genus name
            genus = next(iter(self.genus_names))
            self.go_slim_set = f"{genus} GO-Slim"

        logger.info(f"Found {len(self.organism_nos)} organisms")
        logger.info(f"Using GO Slim set: {self.go_slim_set}")

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
            self.go_id_to_no[goid] = go_no

        # Get GO references
        go_ref_query = text(f"""
            SELECT gr.go_ref_no, r.dbxref_id, r.pubmed, gr.go_annotation_no,
                   gr.date_created
            FROM {DB_SCHEMA}.go_ref gr
            JOIN {DB_SCHEMA}.reference r ON gr.reference_no = r.reference_no
        """)
        result = self.session.execute(go_ref_query)
        annot_rows = result.fetchall()

        # Get NOT qualifiers
        not_query = text(f"""
            SELECT go_ref_no
            FROM {DB_SCHEMA}.go_qualifier
            WHERE qualifier = 'NOT'
        """)
        not_result = self.session.execute(not_query)
        is_not_set = {r[0] for r in not_result}

        # Get GO annotations
        go_annot_query = text(f"""
            SELECT go_no, feature_no, go_evidence, source
            FROM {DB_SCHEMA}.go_annotation
            WHERE go_annotation_no = :anno_no
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

            # Mark this GO ID as in use
            self.inuse_go_id_to_slim_parent[goid] = "NA"

            # Check for NOT qualifier
            qualifier = "NOT" if go_ref_no in is_not_set else ""

            feat_name = self.feat_no_to_feat_name.get(feat_no)
            if not feat_name:
                continue

            annot_str = f"{goid}::{go_ref_no}::{ref_no}::{pubmed or ''}::{go_ev_code}::{qualifier}::{date_str}::{source or ''}"

            if feat_name in self.feat_name_to_annot:
                self.feat_name_to_annot[feat_name] += f"\t{annot_str}"
            else:
                self.feat_name_to_annot[feat_name] = annot_str

        # Load DB code mapping and get support evidence
        self._load_support_evidence()

        logger.info(f"Found {len(self.feat_name_to_annot)} annotated features")

    def _load_support_evidence(self) -> None:
        """Load support evidence for GO annotations."""
        # Load DB code mapping
        db_code_to_go_code: dict[str, str] = {}
        mapping_file = DATA_DIR / "GO_DB_code_mapping"
        if mapping_file.exists():
            with open(mapping_file) as f:
                for line in f:
                    parts = line.strip().split("\t")
                    if len(parts) >= 2:
                        go_code, db_code = parts[:2]
                        db_code_to_go_code[db_code.upper()] = go_code

        dbcode_query = text(f"""
            SELECT gr.go_ref_no, d.source, d.dbxref_id
            FROM {DB_SCHEMA}.go_ref gr
            JOIN {DB_SCHEMA}.goref_dbxref gd ON gd.go_ref_no = gr.go_ref_no
            JOIN {DB_SCHEMA}.dbxref d ON gd.dbxref_no = d.dbxref_no
            ORDER BY gr.go_ref_no, d.source
        """)

        result = self.session.execute(dbcode_query)
        for row in result:
            go_ref_no, source, dbxref_id = row

            go_code = db_code_to_go_code.get(source.upper() if source else "", source or "")

            # Format GO ID with padding
            if go_code == "GO":
                dbxref_id = zero_pad_goid(dbxref_id)

            support_str = f"{go_code}:{dbxref_id}"

            if go_ref_no in self.supports:
                self.supports[go_ref_no] += f"|{support_str}"
            else:
                self.supports[go_ref_no] = support_str

    def populate_slim_term_hashes(self) -> None:
        """Get GO Slim term information."""
        logger.info("Getting GO Slim term information...")

        # Get GO set info
        go_set_query = text(f"""
            SELECT gs.set_name, g.goid, g.go_term, g.go_aspect
            FROM {DB_SCHEMA}.go_set gs
            JOIN {DB_SCHEMA}.go_set_member gsm ON gs.go_set_no = gsm.go_set_no
            JOIN {DB_SCHEMA}.go g ON gsm.go_no = g.go_no
        """)

        result = self.session.execute(go_set_query)
        for row in result:
            set_name, goid, go_term, go_aspect = row

            # Check if this is our GO Slim set (match by genus)
            for genus in self.genus_names:
                if genus.lower() in set_name.lower():
                    self.slim_goid_to_aspect[goid] = go_aspect
                    break

        logger.info(f"Found {len(self.slim_goid_to_aspect)} GO Slim terms")

    def populate_ancestor_paths(self) -> None:
        """Get ancestor paths to map GO IDs to Slim terms."""
        logger.info("Mapping GO IDs to Slim terms...")

        if not self.go_slim_set:
            logger.warning("No GO Slim set specified, skipping mapping")
            return

        # Get slim mapping from go_path table
        # This maps child GO terms to their slim parent terms
        slim_query = text(f"""
            SELECT gs.set_name, gsm.go_no AS slim_go_no, g.goid AS slim_goid,
                   gp.child_go_no
            FROM {DB_SCHEMA}.go_set gs
            JOIN {DB_SCHEMA}.go_set_member gsm ON gs.go_set_no = gsm.go_set_no
            JOIN {DB_SCHEMA}.go g ON gsm.go_no = g.go_no
            JOIN {DB_SCHEMA}.go_path gp ON gp.parent_go_no = gsm.go_no
            WHERE gs.set_name = :set_name
        """)

        result = self.session.execute(slim_query, {"set_name": self.go_slim_set})
        for row in result:
            set_name, slim_go_no, slim_goid, child_go_no = row
            self.goid_for_slim_go_no[slim_go_no] = slim_goid

            if child_go_no not in self.slim_go_nos_for_child_go_no:
                self.slim_go_nos_for_child_go_no[child_go_no] = set()
            self.slim_go_nos_for_child_go_no[child_go_no].add(slim_go_no)

        # Map in-use GO IDs to their slim parents
        for go_id in self.inuse_go_id_to_slim_parent:
            go_no = self.go_id_to_no.get(go_id)
            if not go_no:
                continue

            slim_go_nos = self.slim_go_nos_for_child_go_no.get(go_no, set())

            slim_ids = []
            for slim_go_no in slim_go_nos:
                slim_goid = self.goid_for_slim_go_no.get(slim_go_no)
                if slim_goid:
                    slim_ids.append(slim_goid)

            if slim_ids:
                self.inuse_go_id_to_slim_parent[go_id] = "|" + "|".join(slim_ids) + "|"

        mapped_count = sum(
            1 for v in self.inuse_go_id_to_slim_parent.values() if v != "NA"
        )
        logger.info(f"Mapped {mapped_count} GO IDs to Slim terms")

    def get_gaf_header(self, filename: str) -> str:
        """Generate GAF file header."""
        date_str = datetime.now().strftime("%Y-%m-%d")
        header = []
        header.append("!gpi-version: 2.0")
        header.append(f"!{PROJECT_ACRONYM} GO Slim Annotation File")
        header.append(f"!Date generated: {date_str}")
        header.append(f"!Generated by: {PROJECT_ACRONYM}")
        header.append(f"!Filename: {filename}")
        header.append("!")
        return "\n".join(header) + "\n"

    def write_file(self, output_file: Path) -> int:
        """Write GO Slim GAF file."""
        logger.info(f"Writing GO Slim file to {output_file}")

        output_file.parent.mkdir(parents=True, exist_ok=True)
        count: dict[str, bool] = {}
        results: dict[str, bool] = {}

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

                    # Get slim IDs for this GO ID
                    slim_ids = self.inuse_go_id_to_slim_parent.get(goid, "NA")
                    if slim_ids == "NA" or not slim_ids:
                        logger.warning(f"No slim term for goid={goid}")
                        continue

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

                    # Write one row per slim ID
                    for slim_id in slim_ids.split("|"):
                        if not slim_id:
                            continue

                        slim_id = zero_pad_goid(slim_id)

                        row = "\t".join([
                            PROJECT_ACRONYM,      # DB
                            dbid,                 # DB_Object_ID
                            gene_name,            # DB_Object_Symbol
                            is_not,               # Qualifier
                            f"GO:{slim_id}",      # GO_ID (Slim)
                            f"{PROJECT_ACRONYM}_REF:{ref_no}{pubmed_str}",  # Reference
                            go_ev_code,           # Evidence Code
                            support,              # With/From
                            go_aspect,            # Aspect
                            "",                   # DB_Object_Name
                            alias_str,            # DB_Object_Synonym
                            "gene",               # DB_Object_Type
                            f"taxon:{taxon_id}",  # Taxon
                            date_created,         # Date
                            source or PROJECT_ACRONYM,  # Assigned_By
                        ])

                        results[row] = True

        # Write unique results to file
        with open(output_file, "w") as f:
            f.write(self.get_gaf_header(output_file.name))
            for row in results:
                f.write(row + "\n")

        loop_count = len(results)
        logger.info(f"Wrote {loop_count} records to {output_file}")
        return loop_count

    def run(self, output_file: Path) -> int:
        """Run the full annotation dump."""
        self.get_tax_info()
        self.get_feature_info()
        self.get_go_info()
        self.populate_slim_term_hashes()
        self.populate_ancestor_paths()
        return self.write_file(output_file)


def dump_go_slim_annotation() -> bool:
    """
    Main function to dump GO Slim annotations.

    Returns:
        True on success, False on failure
    """
    logger.info("Starting GO Slim annotation dump...")

    try:
        # Set up paths
        data_file = f"GOslim_gene_association.{PROJECT_ACRONYM.lower()}"
        output_file = TMP_DIR / data_file
        public_dir = HTML_ROOT_DIR / "download" / "go" / "go_slim"
        archive_dir = public_dir / "archive"

        # Create directories
        public_dir.mkdir(parents=True, exist_ok=True)
        archive_dir.mkdir(parents=True, exist_ok=True)

        with SessionLocal() as session:
            dumper = GOSlimAnnotationDumper(session)
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

        logger.info("GO Slim annotation dump complete")
        return True

    except Exception as e:
        logger.exception(f"Error dumping GO Slim annotations: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump GO Slim annotations to GAF format"
    )
    parser.parse_args()

    success = dump_go_slim_annotation()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
