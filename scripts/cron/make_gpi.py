#!/usr/bin/env python3
from __future__ import annotations

"""
Generate GPI (Gene Product Information) files for GO annotation.

This script generates GPI 2.0 format files for submission to the GO Consortium.
Each feature is written as an 11-column, tab-separated row: DB_Object_ID,
DB_Object_Symbol, DB_Object_Name, DB_Object_Synonym(s), DB_Object_Type,
DB_Object_Taxon, Encoded_by, Parent_Object_ID, Protein_Containing_Complex_Members,
DB_Xref(s), and Gene_Product_Properties.

The column conventions mirror SGD's dump_gpi.py so both MODs submit comparable
files: protein-coding features use the DB_Object_Type PR:000000001, the standard
gene name is the symbol (systematic name and aliases become synonyms), DB_Xrefs
carry UniProtKB and RefSeq protein accessions, and Gene_Product_Properties
records db_subset, go_annotation_complete (latest manual review date), and
uniprot_proteome. Two columns are intentionally left empty: Encoded_by, and
Protein_Containing_Complex_Members (CGD does not curate protein complexes).

Output files are gzipped (.gpi.gz), the format GO Central requires for ingest.

For diploid strains (C. albicans SC5314), two files are generated:
- {strain}.gpi.gz - A alleles only (backwards compatible)
- {strain}_with_B_alleles.gpi.gz - Both A and B alleles

Validation checks before copying to final location:
---------------------------------------------------------------------------
- Writes to temp directory first
- Validates minimum feature count (100+ per strain)
- Checks feature count change < 10% vs existing file
- Only copies to final location if validation passes
- Sends Slack notifications on success or failure

Based on makeGPI.pl by Shuai Weng

Usage:
    python make_gpi.py C_albicans_SC5314
    python make_gpi.py --all

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DOWNLOAD_DIR: Directory for output files
    SLACK_WEBHOOK_URL: Slack webhook URL for notifications
    ENV_STATE: Environment state (dev/prod) for Slack labels
"""

import argparse
import gzip
import json
import logging
import os
import shutil
import sys
import tempfile
import urllib.request
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Project root directory (cgd-backend/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load environment variables BEFORE importing cgd modules (settings validation)
load_dotenv(PROJECT_ROOT / ".env")

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", str(PROJECT_ROOT / "data")))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")

# Output directories
GPI_DIR = DOWNLOAD_DIR / "go"
ARCHIVE_DIR = GPI_DIR / "archive"

# Slack webhook for notifications
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
ENV_STATE = os.getenv("ENV_STATE", "dev")

# Validation thresholds
MIN_FEATURES = 100  # Absolute minimum features expected per strain
MAX_FEATURE_CHANGE_PERCENT = 10.0  # Maximum allowed change from previous file

# DB_Object_Type codes (GPI column 5).
#
# These identify the *entity* being described, matching SGD's dump_gpi.py so
# both MODs submit comparable files to the GO Consortium:
#   - protein-coding features (ORF and its B alleles) are protein entities,
#     PR:000000001
#   - RNA-coding features use the generic ncRNA-gene subtype SO:0001263
#   - pseudogenes keep the SO pseudogene term (they encode no gene product)
TYPE_TO_OBJECT_TYPE = {
    "ORF": "PR:000000001",
    "allele": "PR:000000001",  # B alleles are protein entities, same as ORF
    "ncRNA": "SO:0001263",
    "rRNA": "SO:0001263",
    "snRNA": "SO:0001263",
    "snoRNA": "SO:0001263",
    "tRNA": "SO:0001263",
    "pseudogene": "SO:0000336",
}

# Feature types that are protein-coding (get UniProt/RefSeq xrefs and the
# Swiss-Prot db_subset property).
PROTEIN_FEATURE_TYPES = ("ORF", "allele")

# UniProt dbxref selection. CGD stores EBI-loaded accessions under three
# dbxref_type values: reviewed 'SwissProt', unreviewed 'TrEMBL', and a generic
# 'UniProtKB'. All three are accepted (matching the accessions the original
# script emitted); only 'SwissProt' counts as reviewed for the Swiss-Prot
# db_subset property.
UNIPROT_SOURCE = "EBI"
SWISSPROT_TYPE = "SwissProt"
UNIPROT_TYPES = ("SwissProt", "TrEMBL", "UniProtKB")

# RefSeq protein accessions stored against features (matches ftp_dump/gp2protein).
# CGD does not currently load these, so this yields nothing until it does.
REFSEQ_PROTEIN_TYPE = "RefSeq protein version ID"

# Strain configurations
STRAIN_CONFIGS = {
    "C_albicans_SC5314": {
        "seq_source": "C. albicans SC5314 Assembly 22",
        "taxon_id": "237561",
        "has_b_alleles": True,  # Diploid genome with A and B alleles
    },
    "C_auris_B8441": {
        "seq_source": "C. auris B8441",
        "taxon_id": "498019",
    },
    "C_dubliniensis_CD36": {
        "seq_source": "C. dubliniensis CD36",
        "taxon_id": "573826",
    },
    "C_glabrata_CBS138": {
        "seq_source": "C. glabrata CBS138",
        "taxon_id": "284593",
    },
    "C_parapsilosis_CDC317": {
        "seq_source": "C. parapsilosis CDC317",
        "taxon_id": "578454",
    },
    "C_tropicalis": {
        "seq_source": "C. tropicalis MYA-3404",
        "taxon_id": "294747",
    },
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def send_slack_message(message: str, is_error: bool = False) -> None:
    """Send a message to Slack webhook."""
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set, skipping notification")
        return

    emoji = ":x:" if is_error else ":white_check_mark:"
    env_prefix = f"[{ENV_STATE.upper()}] " if ENV_STATE != "prod" else ""

    payload = {
        "text": f"{emoji} {env_prefix}GPI Generation: {message}"
    }

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status != 200:
                logger.warning(f"Slack notification failed: {response.status}")
    except Exception as e:
        logger.warning(f"Failed to send Slack notification: {e}")


def count_features_in_file(file_path: Path) -> int:
    """Count non-header lines (features) in a GPI file (plain or gzipped)."""
    if not file_path.exists():
        return 0

    opener = gzip.open if file_path.suffix == ".gz" else open
    count = 0
    with opener(file_path, "rt") as f:
        for line in f:
            if not line.startswith("!"):
                count += 1
    return count


def validate_output_file(
    new_file: Path,
    existing_file: Path | None = None,
    strain_abbrev: str = "",
) -> tuple[bool, str]:
    """
    Validate the generated GPI file.

    Returns:
        Tuple of (is_valid, message)
    """
    # Check file exists and has content
    if not new_file.exists():
        return False, f"Output file does not exist: {new_file}"

    new_count = count_features_in_file(new_file)

    # Check minimum features
    if new_count < MIN_FEATURES:
        return False, (
            f"Too few features for {strain_abbrev}: {new_count} "
            f"(minimum: {MIN_FEATURES})"
        )

    # Check against existing file if present
    if existing_file and existing_file.exists():
        existing_count = count_features_in_file(existing_file)
        if existing_count > 0:
            change_pct = abs(new_count - existing_count) / existing_count * 100
            if change_pct > MAX_FEATURE_CHANGE_PERCENT:
                return False, (
                    f"Feature count changed too much for {strain_abbrev}: "
                    f"{existing_count} -> {new_count} ({change_pct:.1f}% "
                    f"change, max: {MAX_FEATURE_CHANGE_PERCENT}%)"
                )

    return True, f"Validation passed for {strain_abbrev}: {new_count} features"


def get_genome_version(session, seq_source: str) -> str | None:
    """Get current genome version for the sequence source.

    A source can map to more than one current genome_version when the
    organelle genome is versioned separately (e.g. C. auris B8441 has both
    the nuclear 's02-m01-r12' and the mitochondrial 'mito-s01-m01-r01').
    Prefer the nuclear/primary assembly version and fall back to a 'mito%'
    version only if that is all there is.
    """
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
        ORDER BY CASE WHEN LOWER(gv.genome_version) LIKE 'mito%' THEN 1 ELSE 0 END,
                 gv.genome_version
        FETCH FIRST 1 ROW ONLY
    """)

    result = session.execute(query, {"seq_source": seq_source}).fetchone()
    return result[0] if result else None


def get_features(session, seq_source: str, include_alleles: bool = False) -> list[dict]:
    """Get features for GPI output.

    Args:
        session: Database session
        seq_source: Sequence source string
        include_alleles: If True, include B allele features (for diploid genomes)
    """
    # Base feature types
    feature_types = ['ORF', 'ncRNA', 'rRNA', 'snRNA', 'snoRNA', 'tRNA', 'pseudogene']
    if include_alleles:
        feature_types.append('allele')

    feature_types_str = ", ".join(f"'{ft}'" for ft in feature_types)

    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.dbxref_id, f.feature_type,
               f.gene_name, f.headline
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN {DB_SCHEMA}.seq s ON (fl.root_seq_no = s.seq_no AND s.is_seq_current = 'Y' AND s.source = :seq_source)
        JOIN {DB_SCHEMA}.genome_version gv ON (s.genome_version_no = gv.genome_version_no AND gv.is_ver_current = 'Y')
        WHERE f.feature_type IN ({feature_types_str})
    """)

    result = session.execute(query, {"seq_source": seq_source})
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


def get_aliases(session, feature_no: int) -> list[str]:
    """Get aliases for a feature."""
    query = text(f"""
        SELECT a.alias_name
        FROM {DB_SCHEMA}.alias a
        JOIN {DB_SCHEMA}.feat_alias fa ON (fa.alias_no = a.alias_no AND fa.feature_no = :feature_no)
    """)

    result = session.execute(query, {"feature_no": feature_no})
    return [row[0] for row in result if row[0]]


def get_uniprot_id(session, feature_no: int) -> tuple[str | None, bool]:
    """Get the best UniProt accession for a feature.

    Returns a (accession, is_swissprot) tuple. A reviewed Swiss-Prot accession
    is preferred over the unreviewed TrEMBL/UniProtKB ones; ties are broken by
    the lowest accession for determinism. Returns (None, False) when the feature
    has no EBI-loaded UniProt cross-reference.
    """
    types_clause = ", ".join(f"'{t}'" for t in UNIPROT_TYPES)
    query = text(f"""
        SELECT d.dbxref_type, d.dbxref_id
        FROM {DB_SCHEMA}.dbxref d
        JOIN {DB_SCHEMA}.dbxref_feat df ON (d.dbxref_no = df.dbxref_no AND df.feature_no = :feature_no)
        WHERE d.source = :src
          AND d.dbxref_type IN ({types_clause})
    """)

    # best = (rank, accession); rank 0 = reviewed Swiss-Prot (preferred), 1 = other.
    best: tuple[int, str] | None = None
    result = session.execute(
        query, {"feature_no": feature_no, "src": UNIPROT_SOURCE},
    )
    for dbxref_type, accession in result:
        if not accession:
            continue
        rank = 0 if dbxref_type == SWISSPROT_TYPE else 1
        candidate = (rank, accession)
        if best is None or candidate < best:
            best = candidate

    if best is None:
        return None, False
    return best[1], best[0] == 0


def get_refseq_protein_ids(session, feature_no: int) -> list[str]:
    """Get RefSeq protein version accessions for a feature."""
    query = text(f"""
        SELECT d.dbxref_id
        FROM {DB_SCHEMA}.dbxref d
        JOIN {DB_SCHEMA}.dbxref_feat df ON (d.dbxref_no = df.dbxref_no AND df.feature_no = :feature_no)
        WHERE d.dbxref_type = :refseq_type
    """)

    result = session.execute(
        query, {"feature_no": feature_no, "refseq_type": REFSEQ_PROTEIN_TYPE}
    )
    return sorted({row[0] for row in result if row[0]})


def get_go_annotation_complete_date(session, feature_no: int) -> str:
    """Return the most recent manual GO annotation review date (YYYYMMDD).

    Mirrors SGD's go_annotation_complete: the date of the latest manually
    curated (non-computational) GO annotation for the feature. Returns an empty
    string when the feature has no manual GO annotations.
    """
    query = text(f"""
        SELECT MAX(ga.date_last_reviewed)
        FROM {DB_SCHEMA}.go_annotation ga
        WHERE ga.feature_no = :feature_no
          AND ga.annotation_type != 'computational'
    """)

    row = session.execute(query, {"feature_no": feature_no}).fetchone()
    if not row or not row[0]:
        return ""
    return row[0].strftime("%Y%m%d")


def sanitize_field(value: str | None) -> str:
    """Make a value safe for a tab-delimited, newline-terminated GPI row.

    Collapses any embedded whitespace (tabs, carriage returns, newlines, runs
    of spaces) to single spaces so a stray newline in the source data cannot
    split one feature's record across multiple lines.
    """
    if not value:
        return ""
    return " ".join(value.split())


def clean_description(headline: str | None) -> str:
    """Clean headline for GPI description field."""
    if not headline:
        return ""

    # Extract first part before semicolon
    desc = headline
    if ";" in headline:
        desc = headline.split(";")[0]

    # Remove HTML tags
    desc = desc.replace("<i>", "").replace("</i>", "")
    desc = desc.replace("<sub>", "").replace("</sub>", "")
    desc = desc.replace("<sup>", "").replace("</sup>", "")

    # Collapse embedded newlines/tabs so the description stays on one column.
    return sanitize_field(desc)


def generate_gpi(
    strain_abbrev: str,
    output_dir: Path | None = None,
    include_alleles: bool = False,
    file_suffix: str = "",
) -> bool:
    """
    Generate GPI file for a strain.

    Args:
        strain_abbrev: Strain abbreviation (e.g., C_albicans_SC5314)
        output_dir: Output directory (default: DOWNLOAD_DIR/go)
        include_alleles: If True, include B allele features
        file_suffix: Optional suffix to add to filename (e.g., "_with_B_alleles")

    Returns:
        True on success, False on failure
    """
    if strain_abbrev not in STRAIN_CONFIGS:
        logger.error(f"Unknown strain: {strain_abbrev}")
        return False

    config = STRAIN_CONFIGS[strain_abbrev]
    seq_source = config["seq_source"]
    taxon_id = config["taxon_id"]

    if output_dir is None:
        output_dir = GPI_DIR

    output_dir.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    # GO Central ingests these gzipped, so the published file is .gpi.gz.
    gpi_filename = f"{strain_abbrev}{file_suffix}.gpi.gz"
    final_file = output_dir / gpi_filename
    today_tag = datetime.now().strftime("%Y%m%d")

    # Create temp directory for safe generation
    temp_dir = Path(tempfile.mkdtemp(prefix="gpi_"))
    temp_file = temp_dir / gpi_filename

    try:
        with SessionLocal() as session:
            # Get genome version
            genome_version = get_genome_version(session, seq_source)
            if not genome_version:
                logger.warning(f"No genome version found for {seq_source}")
                genome_version = "unknown"

            # Get features
            features = get_features(session, seq_source, include_alleles=include_alleles)
            allele_note = " (with B alleles)" if include_alleles else ""
            logger.info(f"Found {len(features)} features for {strain_abbrev}{allele_note}")

            # Write GPI file to temp location (gzipped)
            with gzip.open(temp_file, "wt") as f:
                # Write header
                f.write("!gpi-version: 2.0\n")
                f.write(f"!generated-by: {PROJECT_ACRONYM}\n")
                f.write(f"!date-generated: {today_tag}\n")
                f.write("!URL: http://www.candidagenome.org\n")
                f.write("!Contact Email: candida@lists.stanford.edu\n")
                f.write("!Funding: NIDCR at US NIH, grant number R01-DE015873\n")
                f.write(f"!Project-release: {seq_source} genome version {genome_version}\n")
                f.write("!\n")

                # Write feature lines
                for feat in features:
                    feature_no = feat["feature_no"]
                    feature_name = feat["feature_name"] or ""
                    dbxref_id = f"{PROJECT_ACRONYM}:{feat['dbxref_id']}"
                    feature_type = feat["feature_type"]
                    gene_name = feat["gene_name"] or ""
                    headline = feat["headline"]
                    is_protein = feature_type in PROTEIN_FEATURE_TYPES

                    # col5: DB_Object_Type (protein PR: / RNA or pseudogene SO:)
                    object_type = TYPE_TO_OBJECT_TYPE.get(feature_type, "")

                    # col6: taxon
                    taxon = f"NCBITaxon:{taxon_id}"

                    # col3: gene product description
                    description = clean_description(headline)

                    # col2/col4: symbol is the standard gene name (falling back to
                    # the systematic name); the systematic name plus aliases go in
                    # the synonym list. Mirrors SGD's display_name / systematic_name
                    # split.
                    symbol = sanitize_field(gene_name or feature_name)
                    synonyms = [feature_name] if feature_name else []
                    for alias in get_aliases(session, feature_no):
                        if alias not in synonyms:
                            synonyms.append(alias)
                    synonym_list = "|".join(sanitize_field(s) for s in synonyms)

                    # col10: DB_Xref(s) — UniProt (protein features) then RefSeq
                    uniprot_acc, is_swissprot = (
                        get_uniprot_id(session, feature_no) if is_protein else (None, False)
                    )
                    dbxrefs = []
                    if uniprot_acc:
                        dbxrefs.append(f"UniProtKB:{uniprot_acc}")
                    if is_protein:
                        dbxrefs.extend(
                            f"RefSeq:{acc}" for acc in get_refseq_protein_ids(session, feature_no)
                        )
                    dbxref_list = "|".join(dbxrefs)

                    # col11: Gene_Product_Properties
                    props = []
                    if is_swissprot:
                        props.append("db_subset=Swiss-Prot")
                    props.append(
                        f"go_annotation_complete={get_go_annotation_complete_date(session, feature_no)}"
                    )
                    if uniprot_acc:
                        props.append(f"uniprot_proteome=UniProtKB:{uniprot_acc}")
                    property_list = "|".join(props)

                    # Write GPI 2.0 line (tab-separated, 11 columns):
                    #  1 DB_Object_ID          7 Encoded_by (empty)
                    #  2 DB_Object_Symbol      8 Parent_Object_ID
                    #  3 DB_Object_Name        9 Protein_Containing_Complex_Members
                    #  4 DB_Object_Synonym(s)    (empty; CGD does not curate complexes)
                    #  5 DB_Object_Type       10 DB_Xref(s)
                    #  6 DB_Object_Taxon      11 Gene_Product_Properties
                    f.write(
                        f"{dbxref_id}\t{symbol}\t{description}\t{synonym_list}\t"
                        f"{object_type}\t{taxon}\t\t{dbxref_id}\t\t{dbxref_list}\t"
                        f"{property_list}\n"
                    )

            logger.info(f"Generated temp file: {temp_file}")

            # Validate the generated file
            is_valid, validation_msg = validate_output_file(
                temp_file,
                final_file if final_file.exists() else None,
                strain_abbrev,
            )

            if not is_valid:
                error_msg = f"Validation failed: {validation_msg}"
                logger.error(error_msg)
                send_slack_message(error_msg, is_error=True)
                return False

            # Archive existing file before replacing
            if final_file.exists():
                archive_file = ARCHIVE_DIR / f"{strain_abbrev}{file_suffix}_{today_tag}.gpi.gz"
                try:
                    shutil.copy(str(final_file), str(archive_file))
                    logger.info(f"Archived {final_file} to {archive_file}")
                except Exception as e:
                    logger.warning(f"Could not archive {final_file}: {e}")

            # Copy validated file to final location
            shutil.copy(str(temp_file), str(final_file))
            logger.info(f"Copied validated file to {final_file}")

            print(f"Generated: {final_file} ({len(features)} features)")

            # Send success notification
            send_slack_message(
                f"Successfully generated {gpi_filename} with "
                f"{len(features)} features"
            )

            return True

    except Exception as e:
        logger.exception(f"Error generating GPI for {strain_abbrev}: {e}")
        send_slack_message(
            f"Error generating GPI for {strain_abbrev}: {e}",
            is_error=True
        )
        return False

    finally:
        # Clean up temp directory
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate GPI files for GO annotation"
    )
    parser.add_argument(
        "strain",
        nargs="?",
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate GPI files for all strains",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory",
    )

    args = parser.parse_args()

    if args.all:
        strains = list(STRAIN_CONFIGS.keys())
    elif args.strain:
        strains = [args.strain]
    else:
        parser.print_help()
        return 1

    success = True
    for strain in strains:
        # Generate standard GPI file (A alleles only for diploids)
        if not generate_gpi(strain, args.output_dir):
            success = False

        # For strains with B alleles, also generate a version with all alleles
        config = STRAIN_CONFIGS.get(strain, {})
        if config.get("has_b_alleles"):
            if not generate_gpi(
                strain,
                args.output_dir,
                include_alleles=True,
                file_suffix="_with_B_alleles",
            ):
                success = False

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
