#!/usr/bin/env python3
"""
Transfer GO annotations between species via orthology.

This script transfers GO annotations from source species (e.g., S. cerevisiae)
to a target species based on ortholog mappings. It applies various filtering
rules to ensure quality:
- Only transfer annotations with certain evidence codes (IDA, IPI, IGI, IMP)
- Remove redundant annotations (more general terms when specific ones exist)
- Respect existing curated annotations
- Handle NOT qualifier conflicts
- Apply taxon constraint filtering

Based on transferGo.pl by CGD team.

Usage:
    python transfer_go.py <strain_abbrev> <config_file>
    python transfer_go.py C_albicans_SC5314 go_transfer_config.tsv

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
"""

import argparse
import gzip
import logging
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from urllib.request import urlretrieve

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
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/var/www/cgd"))

# Evidence codes to transfer
TRANSFER_EVIDENCE_CODES = {"IDA", "IPI", "IGI", "IMP"}

# Evidence codes that block transfer (existing manual annotations)
BLOCKING_EVIDENCE_CODES = {"IC", "IDA", "IEP", "IGI", "IMP", "IPI", "ISS", "NAS", "TAS"}

# GO IDs to avoid transferring
GOIDS_TO_AVOID = {
    "GO:0005515",  # protein binding
    "GO:0005488",  # binding
    "GO:0033903",
    "GO:0004437",
    "GO:0004428",
}

# GO IDs to replace (obsolete -> replacement)
GOIDS_TO_REPLACE = {
    "GO:0000267": "GO:0005575",
    "GO:0005626": "GO:0005575",
    "GO:0005625": "GO:0005575",
    "GO:0001950": "GO:0005886",
    "GO:0005624": "GO:0016020",
    "GO:0000299": "GO:0016021",
    "GO:0000300": "GO:0019898",
    "GO:0042598": "GO:0031982",
    "GO:0005792": "GO:0043231",
    "GO:0051825": "GO:0044406",
}

# Feature types to avoid for GO transfer
FEATURE_TYPES_TO_AVOID = {"pseudogene"}

# References to avoid (project-specific)
REFERENCES_TO_AVOID = {
    "CGD": "CAL0121033",
    "AspGD": "ASPL0000000005",
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


@dataclass
class GOAnnotation:
    """A GO annotation."""
    db: str
    db_object_id: str
    symbol: str
    qualifier: str
    go_id: str
    reference: str
    evidence: str
    with_from: str
    aspect: str
    name: str
    synonym: str
    obj_type: str
    taxon: str
    date: str
    assigned_by: str
    raw_line: str = ""


@dataclass
class TransferConfig:
    """Configuration for a source species."""
    organism_abbrev: str
    gene_assoc_ext: str
    ortho_dir: str
    ortho_file: str
    chrfeat_url: str
    chrfeat_file: str
    taxon_id: str | None = None


@dataclass
class TransferStats:
    """Statistics for the transfer process."""
    redundant_with_existing: int = 0
    redundant_with_others: int = 0
    conflicts_not_annotation: int = 0
    failed_taxon_triggers: int = 0
    num_transferred: int = 0
    genes_with_transfers: int = 0


def get_organism_taxon_id(session, strain_abbrev: str) -> str | None:
    """Get the species-level taxon ID for an organism."""
    query = text(f"""
        SELECT o2.taxon_id
        FROM {DB_SCHEMA}.organism o1
        JOIN {DB_SCHEMA}.organism o2 ON o1.species_no = o2.organism_no
        WHERE o1.organism_abbrev = :strain_abbrev
    """)
    result = session.execute(query, {"strain_abbrev": strain_abbrev}).fetchone()
    return str(result[0]) if result else None


def get_feature_dbxref_mapping(session, strain_abbrev: str) -> dict[str, str]:
    """Map feature names to database cross-reference IDs."""
    query = text(f"""
        SELECT f.feature_name, f.dbxref_id
        FROM {DB_SCHEMA}.feature f
        WHERE f.organism_abbrev = :strain_abbrev
        AND f.feature_type NOT IN :avoid_types
    """)

    mapping = {}
    for row in session.execute(
        query, {"strain_abbrev": strain_abbrev, "avoid_types": tuple(FEATURE_TYPES_TO_AVOID)}
    ).fetchall():
        mapping[row[0]] = row[1]

    return mapping


def download_gene_association_file(source_file: str, local_path: Path) -> Path:
    """Download a gene association file from GO website."""
    url = f"https://current.geneontology.org/annotations/{source_file}"
    gz_path = local_path.with_suffix(local_path.suffix + ".gz")

    logger.info(f"Downloading {url}")
    urlretrieve(url, gz_path)

    # Decompress
    with gzip.open(gz_path, "rb") as f_in:
        with open(local_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    gz_path.unlink()
    return local_path


def parse_gaf_line(line: str) -> GOAnnotation | None:
    """Parse a GAF (Gene Association Format) line."""
    if line.startswith("!") or not line.strip():
        return None

    parts = line.strip().split("\t")
    if len(parts) < 15:
        return None

    return GOAnnotation(
        db=parts[0],
        db_object_id=parts[1],
        symbol=parts[2],
        qualifier=parts[3] or "none",
        go_id=parts[4],
        reference=parts[5],
        evidence=parts[6],
        with_from=parts[7],
        aspect=parts[8],
        name=parts[9],
        synonym=parts[10],
        obj_type=parts[11],
        taxon=parts[12].replace("taxon:", ""),
        date=parts[13],
        assigned_by=parts[14] if len(parts) > 14 else "",
        raw_line=line.strip(),
    )


def read_associations(
    gaf_file: Path,
    filter_taxon: str | None = None,
    accept_evidence: set[str] | None = None,
    avoid_reference: str | None = None,
    allow_not: bool = False,
) -> dict[str, dict[str, dict[str, dict[str, str]]]]:
    """
    Read GO associations from a GAF file.

    Returns nested dict: aspect -> dbid -> goid -> qualifier -> raw_line
    """
    associations: dict[str, dict[str, dict[str, dict[str, str]]]] = {}

    with open(gaf_file) as f:
        for line in f:
            annot = parse_gaf_line(line)
            if not annot:
                continue

            # Filter by taxon
            if filter_taxon and annot.taxon != filter_taxon:
                continue

            # Filter by NOT qualifier
            if annot.qualifier == "NOT" and not allow_not:
                continue
            if annot.qualifier != "NOT" and allow_not:
                continue

            # Filter by evidence code
            if accept_evidence and annot.evidence not in accept_evidence:
                continue

            # Filter by reference
            if avoid_reference and avoid_reference in annot.reference:
                continue

            aspect = annot.aspect
            dbid = annot.db_object_id
            goid = annot.go_id
            qualifier = annot.qualifier

            if aspect not in associations:
                associations[aspect] = {}
            if dbid not in associations[aspect]:
                associations[aspect][dbid] = {}
            if goid not in associations[aspect][dbid]:
                associations[aspect][dbid][goid] = {}

            associations[aspect][dbid][goid][qualifier] = annot.raw_line

    return associations


def read_ortholog_file(
    ortho_file: Path,
    feat_to_dbid: dict[str, str],
    dbids_to_avoid: set[str],
) -> dict[str, list[str]]:
    """
    Read ortholog mapping file.

    Returns dict mapping source dbid -> list of target dbids.
    """
    mapping: dict[str, list[str]] = {}

    with open(ortho_file) as f:
        for line in f:
            if line.startswith("#"):
                continue

            parts = line.strip().split("\t")
            if len(parts) < 6:
                continue

            target_feat = parts[0]  # Target species feature name
            source_dbid = parts[5]  # Source species DBID

            if not target_feat or not source_dbid:
                continue

            # Skip if target feature not in mapping
            target_dbid = feat_to_dbid.get(target_feat)
            if not target_dbid:
                continue

            # Skip if source DBID should be avoided (e.g., pseudogenes)
            if source_dbid in dbids_to_avoid:
                continue

            if source_dbid not in mapping:
                mapping[source_dbid] = []
            mapping[source_dbid].append(target_dbid)

    return mapping


def determine_candidate_transfers(
    source_annotations: dict[str, dict[str, dict[str, dict[str, str]]]],
    ortholog_mapping: dict[str, list[str]],
) -> dict[str, dict[str, dict[str, dict[str, set[str]]]]]:
    """
    Determine candidate GO annotations to transfer based on orthology.

    Returns nested dict: aspect -> target_dbid -> goid -> qualifier -> set of source_dbids
    """
    candidates: dict[str, dict[str, dict[str, dict[str, set[str]]]]] = {}

    for aspect, dbid_dict in source_annotations.items():
        for source_dbid, goid_dict in dbid_dict.items():
            # Skip if no ortholog mapping
            target_dbids = ortholog_mapping.get(source_dbid, [])
            if not target_dbids:
                continue

            for goid, qual_dict in goid_dict.items():
                # Skip GO IDs to avoid
                if goid in GOIDS_TO_AVOID:
                    continue

                # Replace obsolete GO IDs
                if goid in GOIDS_TO_REPLACE:
                    goid = GOIDS_TO_REPLACE[goid]

                for qualifier in qual_dict:
                    for target_dbid in target_dbids:
                        if aspect not in candidates:
                            candidates[aspect] = {}
                        if target_dbid not in candidates[aspect]:
                            candidates[aspect][target_dbid] = {}
                        if goid not in candidates[aspect][target_dbid]:
                            candidates[aspect][target_dbid][goid] = {}
                        if qualifier not in candidates[aspect][target_dbid][goid]:
                            candidates[aspect][target_dbid][goid][qualifier] = set()

                        candidates[aspect][target_dbid][goid][qualifier].add(source_dbid)

    return candidates


def is_ancestor_of(go_parents: dict[str, set[str]], ancestor_id: str, descendant_id: str) -> bool:
    """
    Check if ancestor_id is an ancestor of descendant_id in GO hierarchy.

    Uses pre-computed parent relationships.
    """
    if ancestor_id == descendant_id:
        return False

    visited = set()
    to_check = [descendant_id]

    while to_check:
        current = to_check.pop()
        if current in visited:
            continue
        visited.add(current)

        parents = go_parents.get(current, set())
        if ancestor_id in parents:
            return True
        to_check.extend(parents)

    return False


def load_go_hierarchy(go_obo_file: Path) -> dict[str, set[str]]:
    """
    Load GO parent-child relationships from OBO file.

    Returns dict mapping GO ID -> set of parent GO IDs.
    """
    parents: dict[str, set[str]] = {}
    current_id = None

    with open(go_obo_file) as f:
        for line in f:
            line = line.strip()

            if line == "[Term]":
                current_id = None
            elif line.startswith("id: GO:"):
                current_id = line.split(": ", 1)[1]
                if current_id not in parents:
                    parents[current_id] = set()
            elif line.startswith("is_a: GO:") and current_id:
                parent_id = line.split(": ", 1)[1].split(" !")[0]
                parents[current_id].add(parent_id)
            elif line.startswith("relationship: part_of GO:") and current_id:
                parent_id = line.split("part_of ", 1)[1].split(" !")[0]
                parents[current_id].add(parent_id)

    return parents


def remove_redundant_annotations(
    candidates: dict[str, dict[str, dict[str, dict[str, set[str]]]]],
    existing_annotations: dict[str, dict[str, dict[str, dict[str, str]]]],
    go_parents: dict[str, set[str]],
    stats: TransferStats,
):
    """
    Remove annotations that are redundant with existing curated annotations
    or with other candidate annotations.
    """
    for aspect in list(candidates.keys()):
        for dbid in list(candidates.get(aspect, {}).keys()):
            to_delete: set[tuple[str, str]] = set()

            candidate_goids = list(candidates[aspect][dbid].keys())
            existing_goids = list(existing_annotations.get(aspect, {}).get(dbid, {}).keys())

            for goid in candidate_goids:
                # Check against existing curated annotations
                for existing_goid in existing_goids:
                    if goid == existing_goid or is_ancestor_of(go_parents, goid, existing_goid):
                        # Candidate is same as or more general than existing
                        for qualifier in existing_annotations[aspect][dbid][existing_goid]:
                            if qualifier in candidates[aspect][dbid].get(goid, {}):
                                to_delete.add((goid, qualifier))
                                stats.redundant_with_existing += 1

                # Check against other candidate annotations
                for other_goid in candidate_goids:
                    if goid == other_goid:
                        continue
                    if is_ancestor_of(go_parents, goid, other_goid):
                        # This candidate is more general than another
                        for qualifier in candidates[aspect][dbid].get(other_goid, {}):
                            if qualifier in candidates[aspect][dbid].get(goid, {}):
                                to_delete.add((goid, qualifier))
                                stats.redundant_with_others += 1

            # Delete marked annotations
            for goid, qualifier in to_delete:
                if goid in candidates[aspect][dbid]:
                    candidates[aspect][dbid][goid].pop(qualifier, None)
                    if not candidates[aspect][dbid][goid]:
                        del candidates[aspect][dbid][goid]


def remove_not_conflicts(
    candidates: dict[str, dict[str, dict[str, dict[str, set[str]]]]],
    not_annotations: dict[str, dict[str, dict[str, dict[str, str]]]],
    go_parents: dict[str, set[str]],
    stats: TransferStats,
):
    """
    Remove candidate transfers that conflict with existing NOT annotations.
    """
    for aspect in list(candidates.keys()):
        for dbid in list(candidates.get(aspect, {}).keys()):
            to_delete: set[tuple[str, str]] = set()

            candidate_goids = list(candidates[aspect][dbid].keys())
            not_goids = list(not_annotations.get(aspect, {}).get(dbid, {}).keys())

            for goid in candidate_goids:
                for not_goid in not_goids:
                    # If candidate equals NOT term or NOT term is ancestor
                    if goid == not_goid or is_ancestor_of(go_parents, not_goid, goid):
                        for qualifier in candidates[aspect][dbid].get(goid, {}):
                            to_delete.add((goid, qualifier))
                            stats.conflicts_not_annotation += 1

            # Delete marked annotations
            for goid, qualifier in to_delete:
                if goid in candidates[aspect][dbid]:
                    candidates[aspect][dbid][goid].pop(qualifier, None)
                    if not candidates[aspect][dbid][goid]:
                        del candidates[aspect][dbid][goid]


def write_transfer_file(
    candidates: dict[str, dict[str, dict[str, dict[str, set[str]]]]],
    output_file: Path,
    stats: TransferStats,
):
    """Write the annotations to be transferred to a file."""
    genes_seen = set()

    with open(output_file, "w") as f:
        f.write("\t".join(["ASPECT", "DBID", "GOID", "QUALIFIER", "SUPPORT_SOURCE"]) + "\n")

        for aspect in sorted(candidates.keys()):
            for dbid in sorted(candidates[aspect].keys()):
                genes_seen.add(dbid)

                for goid in sorted(candidates[aspect][dbid].keys()):
                    for qualifier in sorted(candidates[aspect][dbid][goid].keys()):
                        source_dbids = candidates[aspect][dbid][goid][qualifier]
                        f.write(f"{aspect}\t{dbid}\t{goid}\t{qualifier}\t"
                                f"{','.join(sorted(source_dbids))}\n")
                        stats.num_transferred += 1

    stats.genes_with_transfers = len(genes_seen)


def read_config_file(config_file: Path) -> list[TransferConfig]:
    """Read the transfer configuration file."""
    configs = []

    with open(config_file) as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue

            parts = line.strip().split("\t")
            if len(parts) < 6:
                continue

            config = TransferConfig(
                organism_abbrev=parts[0],
                gene_assoc_ext=parts[1],
                ortho_dir=parts[2],
                ortho_file=parts[3],
                chrfeat_url=parts[4] if len(parts) > 4 else "",
                chrfeat_file=parts[5] if len(parts) > 5 else "",
                taxon_id=parts[6] if len(parts) > 6 else None,
            )
            configs.append(config)

    return configs


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Transfer GO annotations between species via orthology"
    )
    parser.add_argument(
        "strain_abbrev",
        help="Target strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "config_file",
        type=Path,
        help="Tab-delimited config file listing source species",
    )
    parser.add_argument(
        "--go-obo",
        type=Path,
        default=None,
        help="Path to GO OBO file (will download if not provided)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for results",
    )

    args = parser.parse_args()
    strain_abbrev = args.strain_abbrev

    # Set up directories
    work_dir = args.output_dir or (DATA_DIR / "ortholog_GOtransfer")
    work_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Transferring GO annotations to {strain_abbrev}")

    try:
        with SessionLocal() as session:
            # Get target organism taxon ID
            target_taxon = get_organism_taxon_id(session, strain_abbrev)
            logger.info(f"Target taxon ID: {target_taxon}")

            # Map target feature names to DBIDs
            logger.info("Mapping feature names to DBIDs")
            feat_to_dbid = get_feature_dbxref_mapping(session, strain_abbrev)
            logger.info(f"Found {len(feat_to_dbid)} features")

            # Read config file
            configs = read_config_file(args.config_file)
            logger.info(f"Found {len(configs)} source species in config")

            # Load GO hierarchy
            go_obo_file = args.go_obo
            if not go_obo_file or not go_obo_file.exists():
                go_obo_file = work_dir / "gene_ontology.obo"
                if not go_obo_file.exists():
                    logger.info("Downloading GO OBO file")
                    urlretrieve(
                        "http://purl.obolibrary.org/obo/go/go-basic.obo",
                        go_obo_file
                    )

            logger.info("Loading GO hierarchy")
            go_parents = load_go_hierarchy(go_obo_file)
            logger.info(f"Loaded {len(go_parents)} GO terms")

            # Collect annotations from all source species
            all_source_annotations: dict[str, dict[str, dict[str, dict[str, str]]]] = {}
            all_ortholog_mappings: dict[str, list[str]] = {}
            dbids_to_avoid: set[str] = set()

            for config in configs:
                logger.info(f"Processing source: {config.organism_abbrev}")

                # Download gene association file
                gaf_file = work_dir / f"gene_association.{config.gene_assoc_ext.lower()}"
                if not gaf_file.exists():
                    source_file = f"{config.gene_assoc_ext.lower()}.gaf.gz"
                    download_gene_association_file(source_file, gaf_file)

                # Read source annotations
                source_annots = read_associations(
                    gaf_file,
                    filter_taxon=config.taxon_id,
                    accept_evidence=TRANSFER_EVIDENCE_CODES,
                )

                # Merge into combined annotations
                for aspect, dbid_dict in source_annots.items():
                    if aspect not in all_source_annotations:
                        all_source_annotations[aspect] = {}
                    for dbid, goid_dict in dbid_dict.items():
                        if dbid not in all_source_annotations[aspect]:
                            all_source_annotations[aspect][dbid] = {}
                        for goid, qual_dict in goid_dict.items():
                            if goid not in all_source_annotations[aspect][dbid]:
                                all_source_annotations[aspect][dbid][goid] = {}
                            all_source_annotations[aspect][dbid][goid].update(qual_dict)

                # Read ortholog mapping
                ortho_file = Path(config.ortho_dir) / config.ortho_file
                if ortho_file.exists():
                    ortho_mapping = read_ortholog_file(ortho_file, feat_to_dbid, dbids_to_avoid)
                    for source_dbid, target_dbids in ortho_mapping.items():
                        if source_dbid not in all_ortholog_mappings:
                            all_ortholog_mappings[source_dbid] = []
                        all_ortholog_mappings[source_dbid].extend(target_dbids)
                else:
                    logger.warning(f"Ortholog file not found: {ortho_file}")

            # Determine candidate transfers
            logger.info("Determining candidate annotations to transfer")
            candidates = determine_candidate_transfers(
                all_source_annotations, all_ortholog_mappings
            )

            # Count initial candidates
            initial_count = sum(
                len(qual_dict)
                for aspect_dict in candidates.values()
                for dbid_dict in aspect_dict.values()
                for qual_dict in dbid_dict.values()
            )
            logger.info(f"Initial candidates: {initial_count}")

            # Read existing target annotations
            logger.info("Reading existing target annotations")
            target_gaf = HTML_ROOT_DIR / "download" / "go" / f"gene_association.{PROJECT_ACRONYM.lower()}.gz"
            if target_gaf.exists():
                # Decompress temporarily
                temp_gaf = work_dir / f"gene_association.{PROJECT_ACRONYM.lower()}"
                with gzip.open(target_gaf, "rb") as f_in:
                    with open(temp_gaf, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

                avoid_ref = REFERENCES_TO_AVOID.get(PROJECT_ACRONYM)

                existing_annotations = read_associations(
                    temp_gaf,
                    filter_taxon=target_taxon,
                    accept_evidence=BLOCKING_EVIDENCE_CODES,
                    avoid_reference=avoid_ref,
                )

                not_annotations = read_associations(
                    temp_gaf,
                    filter_taxon=target_taxon,
                    accept_evidence={"EXP", "IDA", "IEP", "IGI", "IMP", "IPI"},
                    allow_not=True,
                )

                temp_gaf.unlink()
            else:
                logger.warning("Target GAF file not found")
                existing_annotations = {}
                not_annotations = {}

            # Filter candidates
            stats = TransferStats()

            logger.info("Removing redundant annotations")
            remove_redundant_annotations(candidates, existing_annotations, go_parents, stats)

            logger.info("Removing NOT conflicts")
            remove_not_conflicts(candidates, not_annotations, go_parents, stats)

            # Write output
            output_file = work_dir / f"newAnnotations_{strain_abbrev}.txt"
            write_transfer_file(candidates, output_file, stats)

            # Log statistics
            logger.info(f"\nTransfer Statistics:")
            logger.info(f"  Redundant with existing: {stats.redundant_with_existing}")
            logger.info(f"  Redundant with others: {stats.redundant_with_others}")
            logger.info(f"  Conflicts with NOT: {stats.conflicts_not_annotation}")
            logger.info(f"  Failed taxon triggers: {stats.failed_taxon_triggers}")
            logger.info(f"  Genes getting transfers: {stats.genes_with_transfers}")
            logger.info(f"  Total annotations to transfer: {stats.num_transferred}")
            logger.info(f"\nOutput written to: {output_file}")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
