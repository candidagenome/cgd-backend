#!/usr/bin/env python3
"""
Dump GO annotations to gene_association file (GAF 2.0 format).

This script generates the gene_association.{cgd|aspgd} file containing
GO annotations for all features in the database.

Based on dumpAnnotation.pl by Shuai Weng (Sept 2000), updated for CGD by
Prachi Shah (Nov 2007) and MULTI by Jon Binkley (Feb 2011).

Usage:
    python dump_gene_association.py
    python dump_gene_association.py --output-dir /var/www/html/download/go

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
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))

# Reference IDs for orthology and domain-based annotations
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


@dataclass
class FeatureInfo:
    """Information about a feature."""
    feature_no: int
    feature_name: str
    gene_name: str | None
    dbxref_id: str
    aliases: list[str] = field(default_factory=list)


@dataclass
class GOAnnotation:
    """A GO annotation for a feature."""
    feature_name: str
    goid: int
    go_ref_no: int
    ref_dbxref_id: str
    pubmed: str | None
    go_evidence: str
    qualifier: str
    date_created: str
    source: str


def get_gaf_header(
    session, filename: str, organism_nos: list[int]
) -> str:
    """Generate GAF 2.0 header."""
    lines = []
    lines.append("!gaf-version: 2.0")
    lines.append(f"!generated-by: {PROJECT_ACRONYM}")
    lines.append(f"!date-generated: {datetime.now().strftime('%Y-%m-%d')}")
    lines.append(f"!file: {filename}")

    # Get organism info
    for org_no in organism_nos:
        query = text(f"""
            SELECT organism_name, taxon_id
            FROM {DB_SCHEMA}.organism
            WHERE organism_no = :org_no
        """)
        result = session.execute(query, {"org_no": org_no}).fetchone()
        if result:
            lines.append(f"!taxon: {result[0]} (taxon:{result[1]})")

    lines.append("!")
    return "\n".join(lines) + "\n"


def get_species_and_strains(session) -> list[tuple[int, str, int]]:
    """
    Get reference strains for each species.

    Returns list of (organism_no, seq_source, taxon_id) tuples.
    """
    # Get species-level organisms
    species_query = text(f"""
        SELECT organism_no, organism_abbrev, taxon_id
        FROM {DB_SCHEMA}.organism
        WHERE parent_organism_no IS NULL
        OR parent_organism_no = organism_no
    """)

    strains = []

    for row in session.execute(species_query).fetchall():
        species_no = row[0]
        species_abbrev = row[1]
        taxon_id = row[2]

        # Get default strain for this species
        strain_query = text(f"""
            SELECT o.organism_no, o.organism_abbrev
            FROM {DB_SCHEMA}.organism o
            WHERE o.parent_organism_no = :species_no
            OR o.organism_no = :species_no
            ORDER BY o.organism_no
            FETCH FIRST 1 ROW ONLY
        """)
        strain_result = session.execute(
            strain_query, {"species_no": species_no}
        ).fetchone()

        if strain_result:
            strain_no = strain_result[0]
            strain_abbrev = strain_result[1]

            # Get seq_source for strain
            seq_query = text(f"""
                SELECT DISTINCT s.source
                FROM {DB_SCHEMA}.seq s
                JOIN {DB_SCHEMA}.feat_location fl ON s.seq_no = fl.root_seq_no
                JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
                WHERE s.is_seq_current = 'Y'
                AND f.organism_abbrev = :strain
                FETCH FIRST 1 ROW ONLY
            """)
            seq_result = session.execute(
                seq_query, {"strain": strain_abbrev}
            ).fetchone()

            seq_source = seq_result[0] if seq_result else None

            if seq_source:
                # Handle NCBI taxon ID changes
                if "A_fumigatus" in species_abbrev:
                    taxon_id = 746128

                strains.append((strain_no, seq_source, taxon_id))

    return strains


def get_features_for_organism(
    session, seq_source: str
) -> dict[int, FeatureInfo]:
    """Get all features with their info for an organism."""
    features: dict[int, FeatureInfo] = {}

    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.gene_name, f.dbxref_id
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON (
            f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y'
        )
        JOIN {DB_SCHEMA}.seq s ON (
            fl.root_seq_no = s.seq_no
            AND s.is_seq_current = 'Y'
            AND s.source = :seq_source
        )
        JOIN {DB_SCHEMA}.genome_version gv ON (
            s.genome_version_no = gv.genome_version_no
            AND gv.is_ver_current = 'Y'
        )
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

    for row in session.execute(query, {"seq_source": seq_source}).fetchall():
        feat = FeatureInfo(
            feature_no=row[0],
            feature_name=row[1],
            gene_name=row[2],
            dbxref_id=row[3],
        )
        features[feat.feature_no] = feat

    # Get aliases for features
    alias_query = text(f"""
        SELECT fa.feature_no, a.alias_name
        FROM {DB_SCHEMA}.alias a
        JOIN {DB_SCHEMA}.feat_alias fa ON a.alias_no = fa.alias_no
        WHERE fa.feature_no IN :feat_nos
    """)

    if features:
        feat_nos = tuple(features.keys())
        for row in session.execute(alias_query, {"feat_nos": feat_nos}).fetchall():
            if row[0] in features:
                features[row[0]].aliases.append(row[1])

    return features


def get_go_info(session) -> tuple[dict[int, str], dict[int, int]]:
    """Get GO term info."""
    go_aspect: dict[int, str] = {}
    go_no_to_id: dict[int, int] = {}

    query = text(f"""
        SELECT go_no, goid, go_aspect
        FROM {DB_SCHEMA}.go
    """)

    for row in session.execute(query).fetchall():
        go_no_to_id[row[0]] = row[1]
        go_aspect[row[1]] = row[2]

    return go_aspect, go_no_to_id


def get_go_annotations(
    session,
    features: dict[int, FeatureInfo],
    go_no_to_id: dict[int, int],
) -> dict[str, list[GOAnnotation]]:
    """Get GO annotations for features."""
    annotations: dict[str, list[GOAnnotation]] = {}

    # Get GO refs
    go_ref_query = text(f"""
        SELECT gr.go_ref_no, r.dbxref_id, r.pubmed, gr.go_annotation_no, gr.date_created
        FROM {DB_SCHEMA}.go_ref gr
        JOIN {DB_SCHEMA}.reference r ON gr.reference_no = r.reference_no
    """)

    go_refs = {}
    for row in session.execute(go_ref_query).fetchall():
        go_refs[row[0]] = {
            "ref_dbxref_id": row[1],
            "pubmed": row[2],
            "go_annotation_no": row[3],
            "date_created": row[4].strftime("%Y%m%d") if row[4] else "",
        }

    # Get GO annotations
    go_annot_query = text(f"""
        SELECT go_annotation_no, go_no, feature_no, go_evidence, source
        FROM {DB_SCHEMA}.go_annotation
    """)

    go_annots = {}
    for row in session.execute(go_annot_query).fetchall():
        go_annots[row[0]] = {
            "go_no": row[1],
            "feature_no": row[2],
            "go_evidence": row[3],
            "source": row[4],
        }

    # Get qualifiers
    has_qual_query = text(f"""
        SELECT go_ref_no, has_qualifier
        FROM {DB_SCHEMA}.go_ref
    """)
    has_qualifier = {
        row[0]: row[1] for row in session.execute(has_qual_query).fetchall()
    }

    qual_query = text(f"""
        SELECT go_ref_no, qualifier
        FROM {DB_SCHEMA}.go_qualifier
    """)
    qualifiers = {
        row[0]: row[1] for row in session.execute(qual_query).fetchall()
    }

    # Build annotations
    feat_no_to_name = {f.feature_no: f.feature_name for f in features.values()}

    for go_ref_no, ref_info in go_refs.items():
        ga_no = ref_info["go_annotation_no"]
        if ga_no not in go_annots:
            continue

        annot_info = go_annots[ga_no]
        feat_no = annot_info["feature_no"]

        if feat_no not in feat_no_to_name:
            continue

        feat_name = feat_no_to_name[feat_no]
        goid = go_no_to_id.get(annot_info["go_no"])

        if not goid:
            continue

        # Get qualifier
        qualifier = ""
        if has_qualifier.get(go_ref_no, "N") == "Y":
            qualifier = qualifiers.get(go_ref_no, "")

        annot = GOAnnotation(
            feature_name=feat_name,
            goid=goid,
            go_ref_no=go_ref_no,
            ref_dbxref_id=ref_info["ref_dbxref_id"],
            pubmed=ref_info["pubmed"],
            go_evidence=annot_info["go_evidence"],
            qualifier=qualifier,
            date_created=ref_info["date_created"],
            source=annot_info["source"],
        )

        if feat_name not in annotations:
            annotations[feat_name] = []
        annotations[feat_name].append(annot)

    return annotations


def get_support_info(session) -> dict[int, str]:
    """Get supporting evidence (WITH field) for GO refs."""
    supports: dict[int, str] = {}

    # Read DB code mapping
    db_code_file = DATA_DIR / "GO_DB_code_mapping"
    db_code_map: dict[str, str] = {}

    if db_code_file.exists():
        with open(db_code_file) as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) >= 3:
                    go_code, db_code, db_type = parts[0], parts[1], parts[2]
                    key = f"{db_code.upper()}:{db_type.upper()}"
                    db_code_map[key] = go_code

    # Get dbxref info for go_refs
    query = text(f"""
        SELECT gr.go_ref_no, d.source, d.dbxref_type, d.dbxref_id
        FROM {DB_SCHEMA}.go_ref gr
        JOIN {DB_SCHEMA}.goref_dbxref gd ON gd.go_ref_no = gr.go_ref_no
        JOIN {DB_SCHEMA}.dbxref d ON gd.dbxref_no = d.dbxref_no
        ORDER BY gr.go_ref_no, d.source
    """)

    for row in session.execute(query).fetchall():
        go_ref_no = row[0]
        source = row[1]
        dbxref_type = row[2]
        dbxref_id = row[3]

        # Map to GO code
        key = f"{source.upper()}:{dbxref_type.upper()}"
        go_code = db_code_map.get(key, source)

        # Format GO IDs
        if go_code == "GO":
            dbxref_id = str(dbxref_id).zfill(7)

        support = f"{go_code}:{dbxref_id}"

        if go_ref_no in supports:
            supports[go_ref_no] += f"|{support}"
        else:
            supports[go_ref_no] = support

    return supports


def zero_pad_goid(goid: int) -> str:
    """Zero-pad a GO ID to 7 digits."""
    return str(goid).zfill(7)


def write_gaf_file(
    session,
    output_file: Path,
    features: dict[int, FeatureInfo],
    annotations: dict[str, list[GOAnnotation]],
    go_aspect: dict[int, str],
    supports: dict[int, str],
    organism_nos: list[int],
    taxon_ids: dict[int, int],
) -> int:
    """Write the GAF file and return record count."""
    filename = output_file.name

    with open(output_file, "w") as f:
        # Write header
        f.write(get_gaf_header(session, filename, organism_nos))

        count = 0
        seen: set[str] = set()

        # Create lookup maps
        feat_name_to_info: dict[str, FeatureInfo] = {
            info.feature_name: info for info in features.values()
        }
        feat_name_to_org: dict[str, int] = {}

        for org_no in organism_nos:
            for feat in features.values():
                feat_name_to_org[feat.feature_name] = org_no

        # Write annotations
        for feat_name in sorted(feat_name_to_info.keys()):
            feat_info = feat_name_to_info[feat_name]

            if not feat_info.dbxref_id:
                continue

            if feat_name not in annotations:
                continue

            org_no = feat_name_to_org.get(feat_name)
            taxon_id = taxon_ids.get(org_no, 4932)

            gene_name = feat_info.gene_name or feat_info.feature_name

            # Build alias string
            alias_list = [feat_info.feature_name] + feat_info.aliases
            alias_str = "|".join(alias_list)

            for annot in annotations[feat_name]:
                # Skip duplicates
                key = f"{annot.goid}:{annot.feature_name}:{annot.go_ref_no}"
                if key in seen:
                    continue
                seen.add(key)

                aspect = go_aspect.get(annot.goid, "")
                goid_str = "GO:" + zero_pad_goid(annot.goid)

                # Build reference string
                ref_str = f"{PROJECT_ACRONYM}_REF:{annot.ref_dbxref_id}"
                if annot.pubmed:
                    ref_str += f"|PMID:{annot.pubmed}"

                support = supports.get(annot.go_ref_no, "")

                # GAF 2.0 format
                fields = [
                    PROJECT_ACRONYM,            # 1. DB
                    feat_info.dbxref_id,        # 2. DB Object ID
                    gene_name,                  # 3. DB Object Symbol
                    annot.qualifier,            # 4. Qualifier
                    goid_str,                   # 5. GO ID
                    ref_str,                    # 6. DB:Reference
                    annot.go_evidence,          # 7. Evidence Code
                    support,                    # 8. With/From
                    aspect,                     # 9. Aspect
                    "",                         # 10. DB Object Name
                    alias_str,                  # 11. DB Object Synonym
                    "gene_product",             # 12. DB Object Type
                    f"taxon:{taxon_id}",        # 13. Taxon
                    annot.date_created,         # 14. Date
                    annot.source,               # 15. Assigned By
                    "",                         # 16. Annotation Extension
                    "",                         # 17. Gene Product Form ID
                ]

                f.write("\t".join(fields) + "\n")
                count += 1

        return count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump GO annotations to gene_association file"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for the file",
    )

    args = parser.parse_args()

    output_dir = args.output_dir or (HTML_ROOT_DIR / "download" / "go")
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"gene_association.{PROJECT_ACRONYM.lower()}"
    output_file = TMP_DIR / filename

    logger.info("Dumping GO annotations to gene_association file")

    try:
        with SessionLocal() as session:
            # Get strains and their seq sources
            strains = get_species_and_strains(session)
            logger.info(f"Found {len(strains)} strains to process")

            # Collect all features
            all_features: dict[int, FeatureInfo] = {}
            organism_nos: list[int] = []
            taxon_ids: dict[int, int] = {}

            for org_no, seq_source, taxon_id in strains:
                organism_nos.append(org_no)
                taxon_ids[org_no] = taxon_id

                features = get_features_for_organism(session, seq_source)
                all_features.update(features)
                logger.info(
                    f"Found {len(features)} features for organism {org_no}"
                )

            # Get GO info
            go_aspect, go_no_to_id = get_go_info(session)
            logger.info(f"Found {len(go_aspect)} GO terms")

            # Get annotations
            annotations = get_go_annotations(
                session, all_features, go_no_to_id
            )
            logger.info(
                f"Found annotations for {len(annotations)} features"
            )

            # Get support info
            supports = get_support_info(session)
            logger.info(f"Found {len(supports)} support entries")

            # Write file
            count = write_gaf_file(
                session,
                output_file,
                all_features,
                annotations,
                go_aspect,
                supports,
                organism_nos,
                taxon_ids,
            )
            logger.info(f"Wrote {count} records to {output_file}")

            # Compress and move to output directory
            gz_file = output_file.with_suffix(output_file.suffix + ".gz")
            with open(output_file, "rb") as f_in:
                with gzip.open(gz_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Archive old file and copy new
            final_file = output_dir / f"{filename}.gz"
            if final_file.exists():
                archive_dir = output_dir / "archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                date_str = datetime.now().strftime("%Y%m%d")
                archive_file = archive_dir / f"{filename}.{date_str}.gz"
                shutil.move(final_file, archive_file)
                logger.info(f"Archived old file to {archive_file}")

            shutil.copy(gz_file, final_file)
            logger.info(f"Copied new file to {final_file}")

            # Cleanup
            output_file.unlink(missing_ok=True)
            gz_file.unlink(missing_ok=True)

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
