#!/usr/bin/env python3
"""
Perform various quality checks on ORF sequences.

This script periodically checks ORF sequences for common issues:
- Internal stop codons
- Missing end stop codon
- Non-ATG start codon
- Multiple consecutive stop codons
- Partial terminal codons (not multiples of 3)
- New/deleted genes since a reference version

Based on variousChecksOnOrfSeqs.pl by Prachi Shah (Oct 2006).

Usage:
    python various_checks_on_orf_seqs.py <strain_abbrev>
    python various_checks_on_orf_seqs.py C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    HTML_ROOT_DIR: Root directory for download files
    LOG_DIR: Directory for log files
    CURATOR_EMAIL: Email for notifications
"""

import argparse
import logging
import os
import re
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
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL", "curator@candidagenome.org")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


@dataclass
class OrfCheckResults:
    """Results of ORF sequence checks."""
    internal_stops: list = field(default_factory=list)
    no_end_stop: list = field(default_factory=list)
    no_start: list = field(default_factory=list)
    multiple_stops: list = field(default_factory=list)
    partial_terminal_codon: list = field(default_factory=list)
    new_since_reference: list = field(default_factory=list)
    deleted_since_reference: list = field(default_factory=list)


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

    # Get seq_source
    seq_query = text(f"""
        SELECT DISTINCT s.source
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feat_location fl ON s.seq_no = fl.root_seq_no
        JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
        WHERE s.is_seq_current = 'Y'
        AND f.organism_abbrev = :strain_abbrev
        FETCH FIRST 1 ROW ONLY
    """)
    seq_result = session.execute(seq_query, {"strain_abbrev": strain_abbrev}).fetchone()

    return {
        "organism_no": result[0],
        "organism_abbrev": result[1],
        "organism_name": result[2],
        "seq_source": seq_result[0] if seq_result else None,
    }


def get_all_orfs(session, strain_abbrev: str) -> list[dict]:
    """Get all ORF features for a strain."""
    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.feature_type, f.gene_name
        FROM {DB_SCHEMA}.feature f
        WHERE f.organism_abbrev = :strain_abbrev
        AND (f.feature_type = 'ORF' OR f.feature_type LIKE '%RNA%' OR f.feature_type LIKE '%gene%')
        ORDER BY f.feature_name
    """)

    orfs = []
    for row in session.execute(query, {"strain_abbrev": strain_abbrev}).fetchall():
        orfs.append({
            "feature_no": row[0],
            "feature_name": row[1],
            "feature_type": row[2],
            "gene_name": row[3],
        })

    return orfs


def is_feature_deleted(session, feature_no: int) -> bool:
    """Check if a feature is marked as deleted."""
    query = text(f"""
        SELECT 1 FROM {DB_SCHEMA}.feat_property
        WHERE feature_no = :feature_no
        AND property_value LIKE 'Deleted%'
    """)
    result = session.execute(query, {"feature_no": feature_no}).fetchone()
    return result is not None


def get_feature_location(session, feature_no: int, seq_source: str) -> dict | None:
    """Get location information for a feature."""
    query = text(f"""
        SELECT fl.min_coord, fl.max_coord, fl.strand
        FROM {DB_SCHEMA}.feat_location fl
        JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
        WHERE fl.feature_no = :feature_no
        AND fl.is_loc_current = 'Y'
        AND s.is_seq_current = 'Y'
        AND s.source = :seq_source
    """)
    result = session.execute(
        query, {"feature_no": feature_no, "seq_source": seq_source}
    ).fetchone()

    if not result:
        return None

    return {
        "min_coord": result[0],
        "max_coord": result[1],
        "strand": result[2],
    }


def get_coding_sequence(session, feature_no: int) -> str | None:
    """Get the coding (spliced) nucleotide sequence for a feature."""
    query = text(f"""
        SELECT s.residues
        FROM {DB_SCHEMA}.seq s
        WHERE s.feature_no = :feature_no
        AND s.seq_type = 'coding'
        AND s.is_seq_current = 'Y'
    """)
    result = session.execute(query, {"feature_no": feature_no}).fetchone()
    return result[0] if result else None


def get_genomic_sequence(session, feature_no: int) -> str | None:
    """Get the genomic (unspliced) nucleotide sequence for a feature."""
    query = text(f"""
        SELECT s.residues
        FROM {DB_SCHEMA}.seq s
        WHERE s.feature_no = :feature_no
        AND s.seq_type = 'genomic'
        AND s.is_seq_current = 'Y'
    """)
    result = session.execute(query, {"feature_no": feature_no}).fetchone()
    return result[0] if result else None


def get_protein_sequence(session, feature_no: int) -> str | None:
    """Get the protein (translated) sequence for a feature."""
    query = text(f"""
        SELECT s.residues
        FROM {DB_SCHEMA}.seq s
        WHERE s.feature_no = :feature_no
        AND s.seq_type = 'protein'
        AND s.is_seq_current = 'Y'
    """)
    result = session.execute(query, {"feature_no": feature_no}).fetchone()
    return result[0] if result else None


def load_reference_snapshot(reference_file: Path) -> dict[str, str]:
    """Load reference snapshot file for comparison."""
    snapshot = {}
    if not reference_file.exists():
        logger.warning(f"Reference snapshot file not found: {reference_file}")
        return snapshot

    with open(reference_file) as f:
        for line in f:
            if line.startswith("!"):
                continue
            parts = line.strip().split("\t")
            if len(parts) >= 4:
                feature_name = parts[0]
                feature_type = parts[3]
                snapshot[feature_name] = feature_type

    return snapshot


def check_internal_stops(protein_seq: str) -> bool:
    """Check if protein sequence has internal stop codons."""
    if not protein_seq:
        return False
    # Remove terminal stop codon and check for internal ones
    truncated = protein_seq.rstrip("*")
    return "*" in truncated


def check_no_end_stop(protein_seq: str) -> bool:
    """Check if protein sequence is missing end stop codon."""
    if not protein_seq:
        return False
    return not protein_seq.endswith("*")


def check_no_start(coding_seq: str) -> bool:
    """Check if coding sequence doesn't start with ATG."""
    if not coding_seq:
        return False
    return not coding_seq.upper().startswith("ATG")


def check_multiple_stops(protein_seq: str) -> bool:
    """Check if protein sequence has multiple consecutive stops at end."""
    if not protein_seq:
        return False
    # Two or more stops at the end
    return bool(re.search(r"\*{2,}$", protein_seq))


def check_partial_terminal_codon(coding_seq: str) -> bool:
    """Check if coding sequence length is not a multiple of 3."""
    if not coding_seq:
        return False
    return len(coding_seq) % 3 != 0


def run_checks(session, strain_abbrev: str, seq_source: str,
               reference_snapshot: dict[str, str]) -> OrfCheckResults:
    """Run all sequence checks on ORFs."""
    results = OrfCheckResults()
    seen = set()

    orfs = get_all_orfs(session, strain_abbrev)
    logger.info(f"Checking {len(orfs)} ORFs")

    for i, orf in enumerate(orfs):
        feature_no = orf["feature_no"]
        feature_name = orf["feature_name"]
        feature_type = orf["feature_type"]

        if feature_name in seen:
            continue
        seen.add(feature_name)

        if (i + 1) % 500 == 0:
            logger.info(f"Processed {i + 1}/{len(orfs)} ORFs")

        is_deleted = is_feature_deleted(session, feature_no)
        location = get_feature_location(session, feature_no, seq_source)

        # Check new/deleted for all features
        if feature_name not in reference_snapshot:
            results.new_since_reference.append({
                "feature_name": feature_name,
                "feature_type": feature_type,
            })
        elif is_deleted and reference_snapshot.get(feature_name, "").lower() != "deleted":
            results.deleted_since_reference.append({
                "feature_name": feature_name,
                "feature_type": feature_type,
            })

        # Skip remaining checks if no location, deleted, or not an ORF
        if not location or is_deleted or feature_type != "ORF":
            continue

        # Get sequences
        coding_seq = get_coding_sequence(session, feature_no)
        genomic_seq = get_genomic_sequence(session, feature_no)
        protein_seq = get_protein_sequence(session, feature_no)

        if not coding_seq or not protein_seq:
            continue

        # Run sequence checks
        if check_internal_stops(protein_seq):
            results.internal_stops.append({
                "feature_name": feature_name,
                "genomic_seq": genomic_seq,
                "coding_seq": coding_seq,
                "protein_seq": protein_seq,
            })

        if check_no_end_stop(protein_seq):
            results.no_end_stop.append({
                "feature_name": feature_name,
                "genomic_seq": genomic_seq,
                "coding_seq": coding_seq,
                "protein_seq": protein_seq,
            })

        if check_no_start(coding_seq):
            results.no_start.append({
                "feature_name": feature_name,
                "coding_seq": coding_seq,
            })

        if check_multiple_stops(protein_seq):
            results.multiple_stops.append({
                "feature_name": feature_name,
                "genomic_seq": genomic_seq,
                "coding_seq": coding_seq,
                "protein_seq": protein_seq,
            })

        if check_partial_terminal_codon(coding_seq):
            results.partial_terminal_codon.append({
                "feature_name": feature_name,
                "coding_seq": coding_seq,
                "protein_seq": protein_seq,
            })

    return results


def write_report(results: OrfCheckResults, output_dir: Path, strain_abbrev: str):
    """Write check results to report files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Internal stops
    with open(output_dir / f"{strain_abbrev}_orfs_internal_stop.txt", "w") as f:
        f.write(f"ORFs with internal stop codons - Generated {timestamp}\n\n")
        for orf in results.internal_stops:
            f.write(f"############## {orf['feature_name']} ##############\n\n")
            if orf.get("genomic_seq"):
                f.write(f">{orf['feature_name']}_genomic\n{orf['genomic_seq']}\n\n")
            if orf.get("coding_seq"):
                f.write(f">{orf['feature_name']}_coding\n{orf['coding_seq']}\n\n")
            if orf.get("protein_seq"):
                f.write(f">{orf['feature_name']}_protein\n{orf['protein_seq']}\n\n")

    # No end stop
    with open(output_dir / f"{strain_abbrev}_orfs_no_end_stop.txt", "w") as f:
        f.write(f"ORFs without end stop codon - Generated {timestamp}\n\n")
        for orf in results.no_end_stop:
            f.write(f"############## {orf['feature_name']} ##############\n\n")
            if orf.get("coding_seq"):
                f.write(f">{orf['feature_name']}_coding\n{orf['coding_seq']}\n\n")
            if orf.get("protein_seq"):
                f.write(f">{orf['feature_name']}_protein\n{orf['protein_seq']}\n\n")

    # No start
    with open(output_dir / f"{strain_abbrev}_orfs_no_start.txt", "w") as f:
        f.write(f"ORFs with non-ATG start codon - Generated {timestamp}\n\n")
        for orf in results.no_start:
            f.write(f">{orf['feature_name']}\n{orf['coding_seq']}\n\n")

    # Multiple stops
    with open(output_dir / f"{strain_abbrev}_orfs_multiple_stops.txt", "w") as f:
        f.write(f"ORFs with multiple consecutive stop codons - Generated {timestamp}\n\n")
        for orf in results.multiple_stops:
            f.write(f"############## {orf['feature_name']} ##############\n\n")
            if orf.get("coding_seq"):
                f.write(f">{orf['feature_name']}_coding\n{orf['coding_seq']}\n\n")
            if orf.get("protein_seq"):
                f.write(f">{orf['feature_name']}_protein\n{orf['protein_seq']}\n\n")

    # Partial terminal codon
    with open(output_dir / f"{strain_abbrev}_orfs_partial_terminal.txt", "w") as f:
        f.write(f"ORFs with partial terminal codon - Generated {timestamp}\n\n")
        for orf in results.partial_terminal_codon:
            f.write(f"############## {orf['feature_name']} ##############\n\n")
            if orf.get("coding_seq"):
                f.write(f">{orf['feature_name']}_coding\n{orf['coding_seq']}\n\n")
            if orf.get("protein_seq"):
                f.write(f">{orf['feature_name']}_protein\n{orf['protein_seq']}\n\n")

    # New genes (public report)
    public_dir = HTML_ROOT_DIR / "download" / "CurrentNotes"
    public_dir.mkdir(parents=True, exist_ok=True)

    with open(public_dir / f"{strain_abbrev}_new_genes.txt", "w") as f:
        f.write(f"New genes since reference version - Generated {timestamp}\n\n")
        for orf in results.new_since_reference:
            f.write(f"{orf['feature_name']}\t{orf['feature_type']}\n")

    # Deleted genes (public report)
    with open(public_dir / f"{strain_abbrev}_deleted_genes.txt", "w") as f:
        f.write(f"Deleted genes since reference version - Generated {timestamp}\n\n")
        for orf in results.deleted_since_reference:
            f.write(f"{orf['feature_name']}\t{orf['feature_type']}\n")


def generate_summary(results: OrfCheckResults) -> str:
    """Generate summary of check results."""
    lines = [
        "ORF Sequence Checks Summary",
        "===========================",
        "",
        "PUBLIC REPORTS:",
        f"  - New genes: {len(results.new_since_reference)}",
        f"  - Deleted genes: {len(results.deleted_since_reference)}",
        "",
        "PRIVATE REPORTS:",
        f"  - ORFs with internal stop(s): {len(results.internal_stops)}",
        f"  - ORFs without end stop codon: {len(results.no_end_stop)}",
        f"  - ORFs with non-ATG start: {len(results.no_start)}",
        f"  - ORFs with multiple stops: {len(results.multiple_stops)}",
        f"  - ORFs with partial terminal codon: {len(results.partial_terminal_codon)}",
    ]
    return "\n".join(lines)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Perform various quality checks on ORF sequences"
    )
    parser.add_argument(
        "strain_abbrev",
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--reference-file",
        type=Path,
        default=None,
        help="Reference chromosomal features file for new/deleted comparison",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for reports",
    )

    args = parser.parse_args()
    strain_abbrev = args.strain_abbrev

    logger.info(f"Running ORF sequence checks for {strain_abbrev}")

    try:
        with SessionLocal() as session:
            # Get strain config
            config = get_strain_config(session, strain_abbrev)
            if not config:
                logger.error(f"Strain not found: {strain_abbrev}")
                return 1

            seq_source = config["seq_source"]
            if not seq_source:
                logger.error(f"No seq_source found for {strain_abbrev}")
                return 1

            logger.info(f"Seq source: {seq_source}")

            # Load reference snapshot if provided
            reference_snapshot: dict[str, str] = {}
            if args.reference_file:
                reference_snapshot = load_reference_snapshot(args.reference_file)
                logger.info(f"Loaded {len(reference_snapshot)} features from reference")

            # Run checks
            results = run_checks(session, strain_abbrev, seq_source, reference_snapshot)

            # Determine output directory
            output_dir = args.output_dir or (DATA_DIR / "orf_checks" / strain_abbrev)

            # Write reports
            write_report(results, output_dir, strain_abbrev)
            logger.info(f"Reports written to {output_dir}")

            # Print summary
            summary = generate_summary(results)
            logger.info("\n" + summary)

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
