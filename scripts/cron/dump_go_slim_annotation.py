#!/usr/bin/env python3
"""
Dump GO Slim annotations to a gene association file (GAF format).

This script exports GO Slim annotations (mapped from detailed GO terms to
GO Slim terms) for genes in a format similar to gene_association files.
GO Slim provides a broad overview of gene functions using a reduced set
of high-level GO terms.

Based on dumpGOSlimAnnotation.pl by Prachi Shah (Apr 2008).

Usage:
    python dump_go_slim_annotation.py
    python dump_go_slim_annotation.py --output GOslim_gene_association.cgd

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
DEFAULT_GENUS = os.getenv("DEFAULT_GENUS", "Candida")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def get_default_strain_info(session) -> list[dict]:
    """Get default/reference strain info for each species."""
    # Get all species and their default strains
    query = text(f"""
        SELECT o.organism_no, o.organism_abbrev, o.taxon_id
        FROM {DB_SCHEMA}.organism o
        WHERE o.organism_type = 'strain'
    """)

    strains = []
    for row in session.execute(query).fetchall():
        strains.append({
            "organism_no": row[0],
            "organism_abbrev": row[1],
            "taxon_id": row[2],
        })

    return strains


def get_species_taxon_for_strain(session, strain_organism_no: int) -> int | None:
    """Get species-level taxon ID for a strain."""
    query = text(f"""
        SELECT p.taxon_id
        FROM {DB_SCHEMA}.organism s
        JOIN {DB_SCHEMA}.organism p ON s.parent_organism_no = p.organism_no
        WHERE s.organism_no = :organism_no
        AND p.organism_type = 'species'
    """)
    result = session.execute(query, {"organism_no": strain_organism_no}).fetchone()
    return result[0] if result else None


def get_seq_source(session, strain_abbrev: str) -> str | None:
    """Get sequence source for a strain."""
    query = text(f"""
        SELECT DISTINCT s.source
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feat_location fl ON s.seq_no = fl.root_seq_no
        JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
        WHERE s.is_seq_current = 'Y'
        AND f.organism_abbrev = :strain_abbrev
        FETCH FIRST 1 ROW ONLY
    """)
    result = session.execute(query, {"strain_abbrev": strain_abbrev}).fetchone()
    return result[0] if result else None


def get_features_for_strain(session, seq_source: str) -> list[dict]:
    """Get non-dubious, non-deleted features for a strain."""
    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.gene_name, f.dbxref_id, f.organism_no
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN {DB_SCHEMA}.seq s ON (fl.root_seq_no = s.seq_no AND s.is_seq_current = 'Y' AND s.source = :seq_source)
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

    features = []
    for row in session.execute(query, {"seq_source": seq_source}).fetchall():
        features.append({
            "feature_no": row[0],
            "feature_name": row[1],
            "gene_name": row[2] or row[1],
            "dbxref_id": row[3],
            "organism_no": row[4],
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


def get_go_info(session) -> tuple[dict, dict, dict]:
    """
    Get GO term information.

    Returns:
        Tuple of (goid_to_aspect, gono_to_goid, goid_to_gono)
    """
    query = text(f"""
        SELECT go_no, goid, go_aspect
        FROM {DB_SCHEMA}.go
    """)

    goid_to_aspect = {}
    gono_to_goid = {}
    goid_to_gono = {}

    for row in session.execute(query).fetchall():
        go_no, goid, go_aspect = row
        goid_to_aspect[goid] = go_aspect
        gono_to_goid[go_no] = goid
        goid_to_gono[goid] = go_no

    return goid_to_aspect, gono_to_goid, goid_to_gono


def get_go_annotations(session, feature_nos: set[int]) -> dict[int, list[dict]]:
    """Get GO annotations for features."""
    if not feature_nos:
        return {}

    # Get all GO annotations with reference info
    query = text(f"""
        SELECT ga.feature_no, ga.go_no, ga.go_evidence, ga.source, ga.go_annotation_no,
               gr.go_ref_no, r.dbxref_id as ref_dbxref, r.pubmed, gr.date_created
        FROM {DB_SCHEMA}.go_annotation ga
        JOIN {DB_SCHEMA}.go_ref gr ON ga.go_annotation_no = gr.go_annotation_no
        JOIN {DB_SCHEMA}.reference r ON gr.reference_no = r.reference_no
    """)

    # Get NOT qualifiers
    not_query = text(f"""
        SELECT go_ref_no
        FROM {DB_SCHEMA}.go_qualifier
        WHERE qualifier = 'NOT'
    """)
    not_refs = {row[0] for row in session.execute(not_query).fetchall()}

    annotations: dict[int, list[dict]] = {}
    for row in session.execute(query).fetchall():
        feature_no = row[0]
        if feature_no not in feature_nos:
            continue

        go_no = row[1]
        go_evidence = row[2]
        source = row[3]
        go_ref_no = row[5]
        ref_dbxref = row[6]
        pubmed = row[7]
        date_created = row[8]

        is_not = go_ref_no in not_refs

        if feature_no not in annotations:
            annotations[feature_no] = []

        annotations[feature_no].append({
            "go_no": go_no,
            "go_evidence": go_evidence,
            "source": source,
            "go_ref_no": go_ref_no,
            "ref_dbxref": ref_dbxref,
            "pubmed": pubmed,
            "date_created": date_created.strftime("%Y%m%d") if date_created else "",
            "is_not": is_not,
        })

    return annotations


def get_goref_supports(session) -> dict[int, str]:
    """Get support evidence (with annotations) for GO refs."""
    query = text(f"""
        SELECT gr.go_ref_no, d.source, d.dbxref_id
        FROM {DB_SCHEMA}.go_ref gr
        JOIN {DB_SCHEMA}.goref_dbxref gd ON gr.go_ref_no = gd.go_ref_no
        JOIN {DB_SCHEMA}.dbxref d ON gd.dbxref_no = d.dbxref_no
        ORDER BY gr.go_ref_no, d.source
    """)

    # Load DB code mapping
    db_code_map = {
        "GO": "GO",
        "INTERPRO": "InterPro",
        "UNIPROTKB": "UniProtKB",
        # Add more mappings as needed
    }

    supports: dict[int, list[str]] = {}
    for row in session.execute(query).fetchall():
        go_ref_no = row[0]
        source = row[1]
        dbxref_id = row[2]

        # Map source to GO code
        go_code = db_code_map.get(source.upper(), source)

        # Format GO IDs properly
        if go_code == "GO":
            dbxref_id = str(dbxref_id).zfill(7)

        entry = f"{go_code}:{dbxref_id}"

        if go_ref_no not in supports:
            supports[go_ref_no] = []
        supports[go_ref_no].append(entry)

    return {k: "|".join(v) for k, v in supports.items()}


def get_go_slim_terms(session, genus: str) -> dict[int, str]:
    """
    Get GO Slim terms for a genus.

    Returns dict mapping goid -> aspect for slim terms
    """
    query = text(f"""
        SELECT gs.goid, g.go_aspect
        FROM {DB_SCHEMA}.go_set gs
        JOIN {DB_SCHEMA}.go g ON gs.go_no = g.go_no
        WHERE gs.set_name LIKE :set_pattern
    """)

    slim_terms = {}
    for row in session.execute(query, {"set_pattern": f"%{genus}%GO-Slim%"}).fetchall():
        goid, aspect = row
        slim_terms[goid] = aspect

    return slim_terms


def get_go_slim_mapping(session, genus: str) -> dict[int, set[int]]:
    """
    Get mapping from GO terms to their GO Slim parent terms.

    Returns dict mapping child_go_no -> set of slim_go_nos
    """
    # Get the GO set mapping using go_path
    query = text(f"""
        SELECT gp.child_go_no, gs.go_no as slim_go_no
        FROM {DB_SCHEMA}.go_path gp
        JOIN {DB_SCHEMA}.go_set gs ON gp.parent_go_no = gs.go_no
        WHERE gs.set_name LIKE :set_pattern
    """)

    mapping: dict[int, set[int]] = {}
    for row in session.execute(query, {"set_pattern": f"%{genus}%GO-Slim%"}).fetchall():
        child_go_no = row[0]
        slim_go_no = row[1]

        if child_go_no not in mapping:
            mapping[child_go_no] = set()
        mapping[child_go_no].add(slim_go_no)

    return mapping


def format_goid(goid: int | str) -> str:
    """Format GO ID with proper padding."""
    goid_str = str(goid)
    return "GO:" + goid_str.zfill(7)


def generate_gaf_header() -> str:
    """Generate GAF 2.0 header."""
    lines = [
        f"!gaf-version: 2.0",
        f"!Project: {PROJECT_ACRONYM}",
        f"!Date: {datetime.now().strftime('%Y-%m-%d')}",
        f"!GO Slim annotation file",
        f"!",
    ]
    return "\n".join(lines) + "\n"


def dump_go_slim_annotations(
    session,
    output_file: Path,
    genus: str = DEFAULT_GENUS,
) -> int:
    """
    Dump GO Slim annotations to file.

    Returns number of annotation rows written.
    """
    logger.info("Getting GO information")
    goid_to_aspect, gono_to_goid, goid_to_gono = get_go_info(session)

    logger.info(f"Getting GO Slim terms for {genus}")
    slim_terms = get_go_slim_terms(session, genus)
    logger.info(f"Found {len(slim_terms)} GO Slim terms")

    logger.info("Getting GO Slim mapping")
    slim_mapping = get_go_slim_mapping(session, genus)
    logger.info(f"Found mappings for {len(slim_mapping)} GO terms")

    logger.info("Getting strain information")
    strains = get_default_strain_info(session)
    logger.info(f"Found {len(strains)} strains")

    logger.info("Getting GO reference supports")
    supports = get_goref_supports(session)

    # Collect all features across strains
    all_features: list[dict] = []
    strain_taxon_map: dict[int, int] = {}

    for strain in strains:
        seq_source = get_seq_source(session, strain["organism_abbrev"])
        if not seq_source:
            logger.warning(f"No seq_source for {strain['organism_abbrev']}")
            continue

        # Get species taxon ID
        species_taxon = get_species_taxon_for_strain(session, strain["organism_no"])
        if species_taxon:
            strain_taxon_map[strain["organism_no"]] = species_taxon
        else:
            strain_taxon_map[strain["organism_no"]] = strain["taxon_id"]

        features = get_features_for_strain(session, seq_source)
        all_features.extend(features)
        logger.info(f"Found {len(features)} features for {strain['organism_abbrev']}")

    # Get annotations for all features
    feature_nos = {f["feature_no"] for f in all_features}
    logger.info("Getting GO annotations")
    annotations = get_go_annotations(session, feature_nos)
    logger.info(f"Found annotations for {len(annotations)} features")

    # Build feature lookup
    feature_lookup = {f["feature_no"]: f for f in all_features}

    # Get aliases for features with annotations
    logger.info("Getting feature aliases")
    feature_aliases: dict[int, list[str]] = {}
    for feature_no in annotations.keys():
        feature_aliases[feature_no] = get_feature_aliases(session, feature_no)

    # Write output
    results = set()  # Use set for unique rows

    for feature_no, annot_list in annotations.items():
        if feature_no not in feature_lookup:
            continue

        feat = feature_lookup[feature_no]
        dbxref_id = feat["dbxref_id"]
        if not dbxref_id:
            continue

        gene_name = feat["gene_name"]
        taxon_id = strain_taxon_map.get(feat["organism_no"], 0)

        # Build alias string
        aliases = feature_aliases.get(feature_no, [])
        alias_str = feat["feature_name"]
        if aliases:
            alias_str += "|" + "|".join(aliases)

        for annot in annot_list:
            go_no = annot["go_no"]
            goid = gono_to_goid.get(go_no)
            if not goid:
                continue

            go_aspect = goid_to_aspect.get(goid, "")

            # Find GO Slim parent terms
            slim_gonos = slim_mapping.get(go_no, set())
            if not slim_gonos:
                # Term itself might be a slim term
                if goid in slim_terms:
                    slim_gonos = {go_no}
                else:
                    continue

            qualifier = "NOT" if annot["is_not"] else ""
            ref_str = f"{PROJECT_ACRONYM}_REF:{annot['ref_dbxref']}"
            if annot["pubmed"]:
                ref_str += f"|PMID:{annot['pubmed']}"

            support_str = supports.get(annot["go_ref_no"], "")

            for slim_go_no in slim_gonos:
                slim_goid = gono_to_goid.get(slim_go_no)
                if not slim_goid:
                    continue

                row = "\t".join([
                    PROJECT_ACRONYM,
                    str(dbxref_id),
                    gene_name,
                    qualifier,
                    format_goid(slim_goid),
                    ref_str,
                    annot["go_evidence"],
                    support_str,
                    go_aspect,
                    "",  # gene product name
                    alias_str,
                    "gene",
                    f"taxon:{taxon_id}",
                    annot["date_created"],
                    annot["source"],
                ])
                results.add(row)

    # Write to file
    with open(output_file, "w") as f:
        f.write(generate_gaf_header())
        for row in sorted(results):
            f.write(row + "\n")

    return len(results)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump GO Slim annotations to gene association file"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=None,
        help="Output file path (default: GOslim_gene_association.<acronym>)",
    )
    parser.add_argument(
        "--genus",
        default=DEFAULT_GENUS,
        help=f"Genus for GO Slim set (default: {DEFAULT_GENUS})",
    )
    parser.add_argument(
        "--gzip", "-z",
        action="store_true",
        help="Gzip the output file",
    )

    args = parser.parse_args()

    # Determine output file
    if args.output:
        output_file = args.output
    else:
        output_file = Path(f"GOslim_gene_association.{PROJECT_ACRONYM.lower()}")

    # Set up logging to file
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "dump_go_slim_annotation.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info("Dumping GO Slim annotations")
    logger.info(f"Output file: {output_file}")
    logger.info(f"Genus: {args.genus}")

    try:
        with SessionLocal() as session:
            count = dump_go_slim_annotations(
                session,
                output_file,
                args.genus,
            )

            logger.info(f"Wrote {count} annotation rows to {output_file}")

            if args.gzip:
                # Compress the file
                with open(output_file, "rb") as f_in:
                    with gzip.open(str(output_file) + ".gz", "wb") as f_out:
                        f_out.writelines(f_in)
                output_file.unlink()
                logger.info(f"Compressed to {output_file}.gz")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        return 1

    finally:
        logger.removeHandler(file_handler)
        file_handler.close()


if __name__ == "__main__":
    sys.exit(main())
