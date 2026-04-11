#!/usr/bin/env python3
from __future__ import annotations

"""
Dump chromosomal feature data to tab-delimited files.

This script generates the following files for a strain:
1. <strain>_version_<version>_chromosomal_feature.tab - Feature details
2. ORF19_Assembly22_mapping.tab - Maps ORF19 IDs to Assembly22 IDs (C_albicans only)
3. <strain>_haplotype_variations.tab - Haplotype A vs B differences (if applicable)
4. README - Documentation file

Safety Checks:
---------------------------------------------------------------------------
Before copying files to the final location, the script validates:
1. Minimum feature count (at least 10,000 features expected)
2. ORF features must be present
3. Feature count change must be < 10% compared to existing file
4. Expected feature types (ORF, tRNA, snoRNA, etc.)

Files are written to a temp directory first, then validated, and only
copied to the final location if validation passes. Slack notifications
are sent on success or failure.

Use --skip-validation to bypass these checks (not recommended for production).

Output Files:
---------------------------------------------------------------------------
1. CHROMOSOMAL FEATURE FILE (<strain>_version_<version>_chromosomal_feature.tab)
   Columns:
   1.  Feature name (primary systematic name)
   2.  Standard gene name (locus name)
   3.  Aliases (| separated)
   4.  Feature type|Qualifier (e.g., "ORF|Verified")
   5.  Chromosome
   6.  Start coordinate
   7.  Stop coordinate
   8.  Strand (W/C)
   9.  Primary CGDID
   10. Secondary CGDID
   11. Description
   12. Date created
   13. Sequence coordinate version date
   14. (blank)
   15. (blank)
   16. Gene name reservation date
   17. Reserved name is standard (Y/N)
   18. S. cerevisiae ortholog(s) (| separated)

2. ORF MAPPING FILE (ORF19_Assembly22_mapping.tab)
   Maps old Assembly 19 ORF IDs to Assembly 22 IDs.
   Columns: ORF19_ID, ASSEMBLY22_ID, GENE_NAME

3. HAPLOTYPE VARIATIONS FILE (<strain>_haplotype_variations.tab)
   Lists differences between haplotypes A and B.
   Columns: Hap A Chr, Hap A Up, Hap A Down, Hap B Chr, Hap B Up, Hap B Down,
            Change Type, Hap A Seq, Hap B Seq, Affected Features
---------------------------------------------------------------------------

Based on ftp_datadump.pl by CGD team.

Usage:
    python dump_chromosomal_features.py <strain_abbrev>
    python dump_chromosomal_features.py C_albicans_SC5314
    python dump_chromosomal_features.py C_albicans_SC5314 --output-dir /tmp/test

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    DOWNLOAD_DIR: Directory for output files
    LOG_DIR: Directory for log files
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

# Add parent directories to path
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", str(PROJECT_ROOT / "data")))
LOG_DIR = Path(os.getenv("LOG_DIR", str(PROJECT_ROOT / "logs")))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
ENV_STATE = os.getenv("ENV_STATE", "dev")

# Validation thresholds
MIN_FEATURES = 10000  # Minimum expected features for C. albicans
MAX_FEATURE_CHANGE_PERCENT = 10.0  # Max % change from existing file
EXPECTED_FEATURE_TYPES = {
    "ORF", "tRNA", "snoRNA", "snRNA", "rRNA", "ncRNA",
    "long_terminal_repeat", "repeat_region", "retrotransposon",
    "pseudogene", "centromere", "blocked_reading_frame", "multigene locus"
}

# Configure logging to stderr so stdout can be used for summary output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger(__name__)


def send_slack_message(message: str, is_error: bool = False) -> bool:
    """Send a message to Slack."""
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set, skipping Slack notification")
        return False

    env_label = "PROD" if ENV_STATE in ("prod", "production") else "DEV"
    emoji = ":x:" if is_error else ":white_check_mark:"

    slack_message = {
        "text": f"{emoji} *Chromosomal Features Dump ({env_label})*\n{message}"
    }

    try:
        data = json.dumps(slack_message).encode("utf-8")
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.status == 200
    except Exception as e:
        logger.error(f"Failed to send Slack message: {e}")
        return False


def count_lines_and_types(filepath: Path) -> tuple[int, dict[str, int]]:
    """Count lines and feature types in a chromosomal features file."""
    line_count = 0
    feature_types = {}

    with open(filepath, "r") as f:
        for line in f:
            if line.startswith("!"):
                continue
            line_count += 1
            parts = line.strip().split("\t")
            if len(parts) >= 4:
                # Feature type is in column 4, format: "ORF|Verified" or just "ORF"
                ft = parts[3].split("|")[0] if parts[3] else "unknown"
                feature_types[ft] = feature_types.get(ft, 0) + 1

    return line_count, feature_types


def validate_output_file(
    new_file: Path,
    existing_file: Path | None,
    strain_abbrev: str,
) -> tuple[bool, str | None]:
    """
    Validate the newly generated file before copying to final location.

    Returns:
        (is_valid, error_message) - error_message is None if valid
    """
    # Check file exists and is not empty
    if not new_file.exists():
        return False, f"Output file not created: {new_file}"

    if new_file.stat().st_size == 0:
        return False, "Output file is empty"

    # Count lines and feature types
    new_count, new_types = count_lines_and_types(new_file)

    # Check minimum features
    if new_count < MIN_FEATURES:
        return False, (
            f"Too few features: {new_count} (minimum: {MIN_FEATURES}). "
            "This may indicate a database or query issue."
        )

    # Check for expected feature types
    if "ORF" not in new_types:
        return False, "No ORF features found in output"

    # Check for unexpected feature types
    unexpected = set(new_types.keys()) - EXPECTED_FEATURE_TYPES
    if unexpected:
        logger.warning(f"Unexpected feature types found: {unexpected}")

    # Compare with existing file if it exists
    if existing_file and existing_file.exists():
        existing_count, existing_types = count_lines_and_types(existing_file)

        # Check for large changes
        if existing_count > 0:
            change_percent = abs(new_count - existing_count) / existing_count * 100
            if change_percent > MAX_FEATURE_CHANGE_PERCENT:
                return False, (
                    f"Feature count changed by {change_percent:.1f}% "
                    f"(from {existing_count} to {new_count}). "
                    f"Maximum allowed: {MAX_FEATURE_CHANGE_PERCENT}%. "
                    "Please verify this is expected."
                )

        # Check ORF count specifically
        existing_orfs = existing_types.get("ORF", 0)
        new_orfs = new_types.get("ORF", 0)
        if existing_orfs > 0:
            orf_change = abs(new_orfs - existing_orfs) / existing_orfs * 100
            if orf_change > MAX_FEATURE_CHANGE_PERCENT:
                return False, (
                    f"ORF count changed by {orf_change:.1f}% "
                    f"(from {existing_orfs} to {new_orfs}). "
                    "Please verify this is expected."
                )

    logger.info(f"Validation passed: {new_count} features, {len(new_types)} types")
    return True, None


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

    organism_no = result[0]

    # Get seq_source
    seq_query = text(f"""
        SELECT DISTINCT s.source
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feat_location fl ON s.seq_no = fl.root_seq_no
        JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
        WHERE s.is_seq_current = 'Y'
        AND f.organism_no = :organism_no
        ORDER BY s.source DESC
        FETCH FIRST 1 ROW ONLY
    """)
    seq_result = session.execute(seq_query, {"organism_no": organism_no}).fetchone()

    return {
        "organism_no": result[0],
        "organism_abbrev": result[1],
        "organism_name": result[2],
        "seq_source": seq_result[0] if seq_result else None,
    }


def get_all_features(session, organism_no: int, seq_source: str) -> list[dict]:
    """Get all features for a strain with their details.

    Includes:
    - Biological features (ORF, tRNA, snoRNA, etc.) on Assembly 22 chromosomes
    - B-haplotype alleles (stored as 'allele' type but shown as ORF in output)

    Excludes: CDS, intron, noncoding_exon, adjustment, gap, telomeric_repeat,
    and legacy orf19.* features.

    For diploid organisms like C. albicans, the _A features are primary ORFs,
    and the _B features are stored as alleles but represent the B haplotype
    version of the ORF.
    """
    features = []

    # Part 1: Get all biological features (not alleles)
    # These are the primary features including _A haplotype ORFs
    query_main = text(f"""
        SELECT f.feature_no, f.feature_name, f.gene_name, f.feature_type,
               f.dbxref_id, f.headline, f.date_created,
               fl.start_coord, fl.stop_coord, fl.strand,
               chr.feature_name as chromosome
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
            AND fl.is_loc_current = 'Y'
        JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
            AND s.is_seq_current = 'Y'
            AND s.source = :seq_source
        JOIN {DB_SCHEMA}.feature chr ON s.feature_no = chr.feature_no
        WHERE f.organism_no = :organism_no
        AND f.feature_type NOT IN (
            'chromosome', 'contig', 'CDS', 'allele', 'intron',
            'noncoding_exon', 'adjustment', 'gap', 'telomeric_repeat'
        )
        AND f.feature_name NOT LIKE 'orf19.%'
        ORDER BY chr.feature_name, fl.start_coord, f.feature_name
    """)

    for row in session.execute(
        query_main, {"organism_no": organism_no, "seq_source": seq_source}
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

    # Part 2: Get B-haplotype alleles (feature_name ending in _B)
    # These are the B haplotype versions stored as alleles
    # We get the feature_type and headline from the _A parent feature
    query_alleles = text(f"""
        SELECT b.feature_no, b.feature_name, b.gene_name,
               a.feature_type as parent_feature_type,
               b.dbxref_id,
               COALESCE(b.headline, a.headline) as headline,
               b.date_created,
               fl.start_coord, fl.stop_coord, fl.strand,
               chr.feature_name as chromosome
        FROM {DB_SCHEMA}.feature b
        JOIN {DB_SCHEMA}.feat_location fl ON b.feature_no = fl.feature_no
            AND fl.is_loc_current = 'Y'
        JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
            AND s.is_seq_current = 'Y'
            AND s.source = :seq_source
        JOIN {DB_SCHEMA}.feature chr ON s.feature_no = chr.feature_no
        JOIN {DB_SCHEMA}.feature a ON a.organism_no = b.organism_no
            AND REPLACE(b.feature_name, '_B', '_A') = a.feature_name
            AND a.feature_type NOT IN ('allele', 'CDS', 'intron', 'noncoding_exon')
        WHERE b.organism_no = :organism_no
        AND b.feature_type = 'allele'
        AND b.feature_name LIKE '%\\_B' ESCAPE '\\'
        ORDER BY chr.feature_name, fl.start_coord, b.feature_name
    """)

    for row in session.execute(
        query_alleles, {"organism_no": organism_no, "seq_source": seq_source}
    ).fetchall():
        features.append({
            "feature_no": row[0],
            "feature_name": row[1],
            "gene_name": row[2],
            "feature_type": row[3],  # Parent's feature type (ORF, tRNA, etc.)
            "dbxref_id": row[4],
            "headline": row[5],
            "date_created": row[6],
            "start": row[7],
            "end": row[8],
            "strand": row[9],
            "chromosome": row[10],
        })

    # Part 3: Get features without locations (e.g., multigene locus)
    # These are included in the output with empty location fields
    query_no_loc = text(f"""
        SELECT f.feature_no, f.feature_name, f.gene_name, f.feature_type,
               f.dbxref_id, f.headline, f.date_created
        FROM {DB_SCHEMA}.feature f
        WHERE f.organism_no = :organism_no
        AND f.feature_type IN ('multigene locus')
        AND f.feature_name NOT LIKE 'orf19.%'
        AND NOT EXISTS (
            SELECT 1 FROM {DB_SCHEMA}.feat_location fl
            WHERE fl.feature_no = f.feature_no
            AND fl.is_loc_current = 'Y'
        )
        ORDER BY f.feature_name
    """)

    for row in session.execute(
        query_no_loc, {"organism_no": organism_no}
    ).fetchall():
        features.append({
            "feature_no": row[0],
            "feature_name": row[1],
            "gene_name": row[2],
            "feature_type": row[3],
            "dbxref_id": row[4],
            "headline": row[5],
            "date_created": row[6],
            "start": None,
            "end": None,
            "strand": None,
            "chromosome": None,
        })

    # Sort combined results by chromosome, start position, feature name
    # Features without location (chromosome=None) will be sorted at the beginning
    features.sort(key=lambda x: (x["chromosome"] or "", x["start"] or 0, x["feature_name"] or ""))

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
        JOIN {DB_SCHEMA}.organism o ON f2.organism_no = o.organism_no
        WHERE fr.parent_feature_no = :feature_no
        AND fr.relationship_type = 'ortholog'
        AND o.organism_abbrev = 'S_cerevisiae'
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


def get_genome_version(session, organism_no: int, seq_source: str) -> str | None:
    """Get genome version string for an organism and seq_source."""
    query = text(f"""
        SELECT gv.genome_version
        FROM {DB_SCHEMA}.genome_version gv
        JOIN {DB_SCHEMA}.seq s ON s.genome_version_no = gv.genome_version_no
        WHERE gv.organism_no = :organism_no
        AND s.source = :seq_source
        AND s.is_seq_current = 'Y'
        AND gv.is_ver_current = 'Y'
        FETCH FIRST 1 ROW ONLY
    """)
    result = session.execute(
        query, {"organism_no": organism_no, "seq_source": seq_source}
    ).fetchone()
    return result[0] if result else None


def get_orf19_to_a22_mapping(session, organism_no: int) -> list[tuple[str, str, str]]:
    """
    Get ORF19 to Assembly22 ID mapping for C. albicans.

    Returns list of (orf19_id, assembly22_id, gene_name) tuples.
    """
    # Get features with their ORF19 aliases (aliases starting with "orf19.")
    query = text(f"""
        SELECT a.alias_name, f.feature_name, f.gene_name
        FROM {DB_SCHEMA}.alias a
        JOIN {DB_SCHEMA}.feat_alias fa ON a.alias_no = fa.alias_no
        JOIN {DB_SCHEMA}.feature f ON fa.feature_no = f.feature_no
        WHERE f.organism_no = :organism_no
        AND (a.alias_name LIKE 'orf19.%'
             OR a.alias_name LIKE 'CEN%'
             OR a.alias_name LIKE 'Ca3-%'
             OR a.alias_name LIKE 'HOK-%'
             OR a.alias_name LIKE 'MRS-%'
             OR a.alias_name LIKE 'RB2-%'
             OR a.alias_name LIKE 'ITS%'
             OR a.alias_name LIKE 'Caalf%'
             OR a.alias_name LIKE 'CD%'
             OR a.alias_name LIKE 'LSU-%')
        AND f.feature_name LIKE 'C%\\_%\\_A'  ESCAPE '\\'
        ORDER BY a.alias_name
    """)

    mappings = []
    seen = set()
    for row in session.execute(query, {"organism_no": organism_no}).fetchall():
        orf19_id = row[0]
        a22_id = row[1]
        gene_name = row[2] or ""

        # Avoid duplicates
        if orf19_id not in seen:
            mappings.append((orf19_id, a22_id, gene_name))
            seen.add(orf19_id)

    return mappings


def get_haplotype_variations(session, organism_no: int) -> list[dict]:
    """
    Get haplotype variation data between haplotype A and B chromosomes.

    Returns list of variation records.

    NOTE: This query currently uses seq_change_archive table which provides
    partial data (~5,000 records). The production file has ~64,000 records,
    suggesting the original data source is different (possibly derived from
    direct sequence comparison of A vs B haplotype chromosomes).
    TODO: Investigate the correct data source for complete haplotype variations.
    """
    # Using seq_change_archive table - may not be the complete source
    query = text(f"""
        SELECT
            chr_a.feature_name as hap_a_chr,
            sca.change_start_coord as hap_a_up,
            sca.change_stop_coord as hap_a_down,
            chr_b.feature_name as hap_b_chr,
            sca.change_start_coord as hap_b_up,
            sca.change_stop_coord as hap_b_down,
            sca.seq_change_type as change_type,
            '' as hap_a_seq,
            '' as hap_b_seq,
            '' as affected_features
        FROM {DB_SCHEMA}.seq_change_archive sca
        JOIN {DB_SCHEMA}.seq s ON sca.seq_no = s.seq_no
        JOIN {DB_SCHEMA}.feature chr_a ON s.feature_no = chr_a.feature_no
        LEFT JOIN {DB_SCHEMA}.feature chr_b ON chr_b.organism_no = chr_a.organism_no
            AND chr_b.feature_type = 'chromosome'
            AND REPLACE(chr_b.feature_name, 'B_', 'A_') = chr_a.feature_name
        WHERE chr_a.organism_no = :organism_no
        AND chr_a.feature_name LIKE '%A\\_%' ESCAPE '\\'
        ORDER BY chr_a.feature_name, sca.change_start_coord
    """)

    variations = []
    try:
        for row in session.execute(query, {"organism_no": organism_no}).fetchall():
            variations.append({
                "hap_a_chr": row[0] or "",
                "hap_a_up": row[1] or "",
                "hap_a_down": row[2] or "",
                "hap_b_chr": row[3] or "",
                "hap_b_up": row[4] or "",
                "hap_b_down": row[5] or "",
                "change_type": row[6] or "",
                "hap_a_seq": row[7] or "",
                "hap_b_seq": row[8] or "",
                "affected_features": row[9] or "",
            })
    except Exception as e:
        logger.warning(f"Could not fetch haplotype variations: {e}")

    return variations


def write_chromosomal_features(
    session, organism_no: int, strain_abbrev: str, organism_name: str,
    seq_source: str, genome_version: str, output_file: Path
) -> int:
    """Write chromosomal features to a tab-delimited file. Returns feature count."""
    features = get_all_features(session, organism_no, seq_source)
    logger.info(f"Found {len(features)} features")

    timestamp = datetime.now().strftime("%a %b %d %H:%M:%S %Y")

    with open(output_file, "w") as f:
        # Header matching production format
        f.write(f"! File name: {output_file.name}\n")
        f.write(f"! Organism: {organism_name}\n")
        f.write(f"! Genome version: {genome_version}\n")
        f.write(f"! Date created: {timestamp}\n")
        f.write(f"! Created by: The Candida Genome Database (http://www.candidagenome.org/)\n")
        f.write("! Contact Email: candida-curator AT lists DOT stanford DOT edu\n")
        f.write("! Funding: NIDCR at US NIH, grant number 1-R01-DE015873-01\n")
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

            # Combine feature_type with qualifier (e.g., "ORF|Verified")
            if qualifier:
                feature_type_qualifier = f"{feature_type}|{qualifier}"
            else:
                feature_type_qualifier = feature_type

            chromosome = feat["chromosome"] or ""
            start = str(feat["start"]) if feat["start"] else ""
            end = str(feat["end"]) if feat["end"] else ""
            strand = feat["strand"] or ""
            dbxref_id = feat["dbxref_id"] or ""
            secondary_dbxref_str = secondary_dbxref or ""
            headline = (feat["headline"] or "").replace("\t", " ").replace("\n", " ")
            date_created = feat["date_created"].strftime("%Y-%m-%d") if feat["date_created"] else ""
            # Seq coord version date - use same as date_created for now
            seq_version_date = date_created
            reservation_date = reservation_date or ""
            is_standard = is_standard or "N"
            orthologs_str = "|".join(orthologs) if orthologs else ""

            # Write line with 18 columns matching production format
            # Cols: name, gene, aliases, type|qual, chr, start, stop, strand,
            #       primary_id, secondary_id, desc, date_created, seq_version_date,
            #       blank, blank, reservation_date, is_standard, orthologs
            fields = [
                feature_name,
                gene_name,
                aliases_str,
                feature_type_qualifier,
                chromosome,
                start,
                end,
                strand,
                dbxref_id,
                secondary_dbxref_str,
                headline,
                date_created,
                seq_version_date,
                "",  # blank column 14
                "",  # blank column 15
                reservation_date,
                is_standard,
                orthologs_str,
            ]

            f.write("\t".join(fields) + "\n")

    return len(features)


def write_orf19_mapping(
    session, organism_no: int, output_file: Path
) -> int:
    """Write ORF19 to Assembly22 mapping file. Returns mapping count."""
    mappings = get_orf19_to_a22_mapping(session, organism_no)
    logger.info(f"Found {len(mappings)} ORF19 mappings")

    with open(output_file, "w") as f:
        # Header
        f.write("ORF19_ID\tASSEMBLY22_ID\tGENE_NAME\n")

        for orf19_id, a22_id, gene_name in mappings:
            f.write(f"{orf19_id}\t{a22_id}\t{gene_name}\n")

    return len(mappings)


def write_haplotype_variations(
    session, organism_no: int, output_file: Path
) -> int:
    """Write haplotype variations file. Returns variation count."""
    variations = get_haplotype_variations(session, organism_no)
    logger.info(f"Found {len(variations)} haplotype variations")

    with open(output_file, "w") as f:
        # Header
        f.write("Hap A Chromosome\tHap A Upstream Coord\tHap A Downstream Coord\t")
        f.write("Hap B Chromosome\tHap B Upstream Coord\tHap B Downstream Coord\t")
        f.write("Change Type\tHap A Seq\tHap B Seq\tAffected Features\n")

        for var in variations:
            fields = [
                str(var["hap_a_chr"]),
                str(var["hap_a_up"]),
                str(var["hap_a_down"]),
                str(var["hap_b_chr"]),
                str(var["hap_b_up"]),
                str(var["hap_b_down"]),
                str(var["change_type"]),
                str(var["hap_a_seq"]),
                str(var["hap_b_seq"]),
                str(var["affected_features"]),
            ]
            f.write("\t".join(fields) + "\n")

    return len(variations)


def write_readme(strain_abbrev: str, output_file: Path) -> None:
    """Write README file for the chromosomal features directory."""
    content = f"""The files in this directory contain information about chromosomal
features for {strain_abbrev.replace('_', ' ')}.

The notation "version_sXX-mYY-rZZ" in the filename indicates the genome version
to which data in the file corresponds. Detailed explanation about the genome
version notation can be found at: http://www.candidagenome.org/help/SequenceHelp.shtml#versions

The file with "current" in its names is provided as a stable filename for
automated downloads. It is identical to (technically, symbolic links to) the
corresponding versioned file.

Note: If you are using these files in conjunction with the corresponding assembly, note that both may change
periodically, so be sure to download both the sequence and chromosomal features files at the same time.

Columns within chromosomal_features.tab:

1.  Feature name (mandatory); this is the primary systematic name, if available
2.  Gene name (locus name)
3.  Aliases (multiples separated by |)
4.  Feature type
5.  Chromosome
6.  Start Coordinate
7.  Stop Coordinate
8.  Strand
9.  Primary CGDID
10. Secondary CGDID (if any)
11. Description
12. Date Created
13. Sequence Coordinate Version Date (if any)
14. Blank
15. Blank
16. Date of gene name reservation (if any).
17. Has the reserved gene name become the standard name? (Y/N)
18. Name of S. cerevisiae ortholog(s) (multiples separated by |)

Note: Genes that have been deleted from the current reference annotation
set are not included in this file.


ORF19_Assembly22_mapping.tab

This file maps Assembly 19 ORF identifiers to Assembly 22 identifiers.

The file has the following columns:

1. ORF19_ID - The Assembly 19 identifier
2. ASSEMBLY22_ID - The Assembly 22 identifier
3. GENE_NAME - The standard gene name (if any)


URL: www.candidagenome.org
Contact: http://www.candidagenome.org/cgi-bin/suggestion
Funding: NIDCR at US NIH, grant number 1-R01-DE015873-01
"""
    with open(output_file, "w") as f:
        f.write(content)


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
        description="Dump chromosomal feature data to tab-delimited files"
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
        help="Don't archive old files",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip validation checks (use with caution)",
    )

    args = parser.parse_args()
    strain_abbrev = args.strain_abbrev

    logger.info(f"Dumping chromosomal features for {strain_abbrev}")

    try:
        with SessionLocal() as session:
            # Get strain config
            config = get_strain_config(session, strain_abbrev)
            if not config:
                error_msg = f"Strain not found: {strain_abbrev}"
                logger.error(error_msg)
                send_slack_message(error_msg, is_error=True)
                return 1

            organism_no = config["organism_no"]
            organism_name = config["organism_name"]
            seq_source = config["seq_source"]

            if not seq_source:
                error_msg = f"No seq_source found for {strain_abbrev}"
                logger.error(error_msg)
                send_slack_message(error_msg, is_error=True)
                return 1

            logger.info(f"Seq source: {seq_source}")

            # Get genome version
            genome_version = get_genome_version(session, organism_no, seq_source)
            if not genome_version:
                logger.warning("No genome version found, using 'unknown'")
                genome_version = "unknown"

            logger.info(f"Genome version: {genome_version}")

            # Determine final output directory
            if args.output_dir:
                final_output_dir = args.output_dir
            else:
                final_output_dir = (
                    DOWNLOAD_DIR / "chromosomal_feature_files" / strain_abbrev
                )

            final_output_dir.mkdir(parents=True, exist_ok=True)
            archive_dir = final_output_dir / "archive"

            # File names
            versioned_filename = f"{strain_abbrev}_version_{genome_version}_chromosomal_feature.tab"
            final_output_file = final_output_dir / versioned_filename

            # Create temp directory for initial output
            with tempfile.TemporaryDirectory(prefix="cgd_dump_") as temp_dir:
                temp_path = Path(temp_dir)
                temp_output_file = temp_path / versioned_filename

                # 1. Write chromosomal features to temp directory
                logger.info("Writing chromosomal features to temp directory...")
                count = write_chromosomal_features(
                    session, organism_no, strain_abbrev, organism_name,
                    seq_source, genome_version, temp_output_file
                )
                logger.info(f"Generated {count} features in temp file")

                # 2. Validate output before copying
                if not args.skip_validation:
                    logger.info("Validating output file...")
                    is_valid, error_msg = validate_output_file(
                        temp_output_file,
                        final_output_file if final_output_file.exists() else None,
                        strain_abbrev,
                    )

                    if not is_valid:
                        logger.error(f"Validation failed: {error_msg}")
                        send_slack_message(
                            f"*Validation failed for {strain_abbrev}*\n{error_msg}\n\n"
                            "Files were NOT copied to the final location.",
                            is_error=True
                        )
                        return 1
                else:
                    logger.warning("Validation skipped (--skip-validation flag)")

                # 3. Archive old file and copy new file to final location
                if not args.no_archive and final_output_file.exists():
                    archive_old_file(final_output_file, archive_dir)

                shutil.copy2(temp_output_file, final_output_file)
                logger.info(f"Chromosomal features written to {final_output_file}")

                # 4. Write ORF19 to Assembly22 mapping (only for C_albicans_SC5314)
                orf_mapping_count = 0
                if strain_abbrev == "C_albicans_SC5314":
                    orf_mapping_file = final_output_dir / "ORF19_Assembly22_mapping.tab"
                    if not args.no_archive and orf_mapping_file.exists():
                        archive_old_file(orf_mapping_file, archive_dir)
                    orf_mapping_count = write_orf19_mapping(session, organism_no, orf_mapping_file)
                    logger.info(f"ORF mapping written to {orf_mapping_file}")

                # 5. Write haplotype variations (if data available)
                haplotype_count = 0
                haplotype_file = final_output_dir / f"{strain_abbrev}_haplotype_variations.tab"
                if not args.no_archive and haplotype_file.exists():
                    archive_old_file(haplotype_file, archive_dir)
                haplotype_count = write_haplotype_variations(session, organism_no, haplotype_file)
                if haplotype_count > 0:
                    logger.info(f"Haplotype variations written to {haplotype_file}")
                else:
                    # Remove empty file
                    if haplotype_file.exists():
                        haplotype_file.unlink()
                    logger.info("No haplotype variation data found")

                # 6. Write README
                readme_file = final_output_dir / "README"
                write_readme(strain_abbrev, readme_file)
                logger.info(f"README written to {readme_file}")

            # Build summary
            summary_parts = [f"{count} features in {versioned_filename}"]
            if orf_mapping_count > 0:
                summary_parts.append(f"{orf_mapping_count} ORF mappings")
            if haplotype_count > 0:
                summary_parts.append(f"{haplotype_count} haplotype variations")

            summary = f"*{strain_abbrev}*: " + " | ".join(summary_parts)

            # Print summary to stdout
            print(summary)

            # Send success Slack notification
            send_slack_message(
                f"{summary}\n\n"
                f"Output directory: `{final_output_dir}`",
                is_error=False
            )

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        error_trace = traceback.format_exc()
        logger.error(error_trace)
        strain_info = strain_abbrev if 'strain_abbrev' in dir() else "unknown"
        send_slack_message(
            f"*Unexpected error during dump for {strain_info}*\n"
            f"```{str(e)[:500]}```",
            is_error=True
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
