#!/usr/bin/env python3
from __future__ import annotations

"""
Dump sequence files in FASTA format for a strain.

This script generates various FASTA sequence files matching the legacy Perl output:
- Chromosome sequences
- ORF genomic sequences (with optional flanking regions)
- ORF coding sequences (introns removed)
- ORF protein translations
- ORF plus intergenic (extended to adjacent features)
- Other feature sequences (ncRNA, tRNA, etc.)
- Intergenic (not_feature) sequences
- Default/representative sequences (one per gene for diploids)

Output directory structure: {DOWNLOAD_DIR}/sequence/{strain}/Assembly{N}/current/
Filename format: {strain}_version_{genome_version}_{type}.fasta.gz

Validation checks before copying to final location:
---------------------------------------------------------------------------
- Writes to temp directory first
- Validates minimum feature count per file type
- Checks feature count change < 10% vs existing file
- Only copies to final location if validation passes
- Sends Slack notifications on success or failure

Based on dumpSequence.pl by CGD team.

Usage:
    python dump_sequence.py <strain_abbrev> [seq_source]
    python dump_sequence.py C_albicans_SC5314
    python dump_sequence.py C_albicans_SC5314 "C. albicans SC5314 Assembly 22"
    python dump_sequence.py --all

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    DOWNLOAD_DIR: Directory for output files
    SLACK_WEBHOOK_URL: Slack webhook URL for notifications
    ENV_STATE: Environment state (dev/prod) for Slack labels
"""

import argparse
import gzip
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import urllib.request
from collections import defaultdict
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

# Slack webhook for notifications
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
ENV_STATE = os.getenv("ENV_STATE", "dev")

# Validation thresholds
MIN_FEATURES = {
    "chromosomes": 5,
    "orf_coding": 100,
    "orf_genomic": 100,
    "orf_genomic_1000": 100,
    "orf_trans_all": 100,
    "orf_plus_intergenic": 100,
    "other_features_genomic": 10,
    "other_features_genomic_1000": 10,
    "other_features_no_introns": 10,
    "other_features_plus_intergenic": 10,
    "not_feature": 50,
    "default_coding": 100,
    "default_genomic": 100,
    "default_protein": 100,
    # Haploid files (Assembly 19 only)
    "orf_coding_haploid": 100,
    "orf_genomic_haploid": 100,
    "orf_genomic_1000_haploid": 100,
    "orf_trans_all_haploid": 100,
    "orf_plus_intergenic_haploid": 100,
    "other_features_genomic_haploid": 10,
    "other_features_genomic_1000_haploid": 10,
    "other_features_no_introns_haploid": 10,
    "other_features_plus_intergenic_haploid": 10,
}
MAX_FEATURE_CHANGE_PERCENT = 10.0

# Configure logging to stderr so stdout can be used for summary output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger(__name__)


def send_slack_message(message: str, is_error: bool = False) -> None:
    """Send a message to Slack webhook."""
    if not SLACK_WEBHOOK_URL:
        return

    emoji = ":x:" if is_error else ":white_check_mark:"
    env_prefix = f"[{ENV_STATE.upper()}] " if ENV_STATE != "prod" else ""

    payload = {"text": f"{emoji} {env_prefix}Sequence Dump: {message}"}

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status != 200:
                logger.warning(f"Slack notification failed: {response.status}")
    except Exception as e:
        logger.warning(f"Failed to send Slack notification: {e}")


def count_sequences_in_file(file_path: Path) -> int:
    """Count sequences in a FASTA file (gzipped or not)."""
    if not file_path.exists():
        return 0

    count = 0
    if file_path.suffix == ".gz":
        with gzip.open(file_path, "rt") as f:
            for line in f:
                if line.startswith(">"):
                    count += 1
    else:
        with open(file_path) as f:
            for line in f:
                if line.startswith(">"):
                    count += 1
    return count


def validate_output_file(
    new_file: Path,
    existing_file: Path | None,
    file_type: str,
    strain_abbrev: str,
) -> tuple[bool, str]:
    """Validate the generated FASTA file."""
    if not new_file.exists():
        return False, f"Output file does not exist: {new_file}"

    new_count = count_sequences_in_file(new_file)
    min_count = MIN_FEATURES.get(file_type, 10)

    if new_count < min_count:
        return False, (
            f"Too few sequences for {strain_abbrev} {file_type}: {new_count} "
            f"(minimum: {min_count})"
        )

    if existing_file and existing_file.exists():
        existing_count = count_sequences_in_file(existing_file)
        if existing_count > 0:
            change_pct = abs(new_count - existing_count) / existing_count * 100
            if change_pct > MAX_FEATURE_CHANGE_PERCENT:
                return False, (
                    f"Sequence count changed too much for {strain_abbrev} {file_type}: "
                    f"{existing_count} -> {new_count} ({change_pct:.1f}% change)"
                )

    return True, f"Validation passed: {new_count} sequences"


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

    return {
        "organism_no": result[0],
        "organism_abbrev": result[1],
        "organism_name": result[2],
    }


def get_seq_source(session, organism_no: int) -> str | None:
    """Get sequence source for an organism."""
    query = text(f"""
        SELECT DISTINCT s.source
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feat_location fl ON s.seq_no = fl.root_seq_no
        JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
        WHERE s.is_seq_current = 'Y'
        AND f.organism_no = :organism_no
        ORDER BY s.source DESC
        FETCH FIRST 1 ROW ONLY
    """)
    result = session.execute(query, {"organism_no": organism_no}).fetchone()
    return result[0] if result else None


def get_genome_version(session, seq_source: str) -> str | None:
    """Get current genome version for the sequence source."""
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
    result = session.execute(query, {"seq_source": seq_source}).fetchone()
    return result[0] if result else None


def get_assembly_number(genome_version: str) -> str | None:
    """Extract assembly number from genome version (e.g., 'A22-xxx' -> '22')."""
    if genome_version and genome_version.startswith("A"):
        match = re.match(r"A(\d+)", genome_version)
        if match:
            return match.group(1)
    return None


def get_chromosomes(session, seq_source: str) -> list[dict]:
    """Get chromosome/contig sequences for a sequence source."""
    query = text(f"""
        SELECT f.feature_name, s.residues, LENGTH(s.residues) as seq_len
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.seq s ON (f.feature_no = s.feature_no
            AND s.source = :seq_source AND s.is_seq_current = 'Y')
        WHERE f.feature_type IN ('chromosome', 'contig')
        ORDER BY f.feature_name
    """)

    chromosomes = []
    for row in session.execute(query, {"seq_source": seq_source}).fetchall():
        chromosomes.append({
            "name": row[0],
            "sequence": row[1],
            "length": row[2],
        })

    return chromosomes


def get_features_with_locations(
    session, organism_no: int, seq_source: str
) -> list[dict]:
    """Get all features with their location information."""
    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.gene_name, f.dbxref_id,
               f.feature_type, fp.property_value as feature_qualifier, f.headline,
               fl.start_coord, fl.stop_coord, fl.strand,
               root_feat.feature_name as chr_name,
               (SELECT LENGTH(s2.residues) FROM {DB_SCHEMA}.seq s2
                WHERE s2.seq_no = fl.root_seq_no) as chr_length
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN {DB_SCHEMA}.seq s ON (fl.root_seq_no = s.seq_no AND s.is_seq_current = 'Y' AND s.source = :seq_source)
        JOIN {DB_SCHEMA}.feature root_feat ON s.feature_no = root_feat.feature_no
        LEFT JOIN {DB_SCHEMA}.feat_property fp ON (f.feature_no = fp.feature_no AND fp.property_type = 'feature_qualifier')
        WHERE f.organism_no = :organism_no
        AND f.feature_type NOT IN ('chromosome', 'contig')
        ORDER BY root_feat.feature_name, fl.start_coord
    """)

    features = []
    for row in session.execute(
        query, {"organism_no": organism_no, "seq_source": seq_source}
    ).fetchall():
        feature_qualifier = row[5] or ""

        # Skip deleted features
        if "Deleted" in feature_qualifier:
            continue

        start = row[7]
        stop = row[8]
        if start > stop:
            start, stop = stop, start

        features.append({
            "feature_no": row[0],
            "feature_name": row[1],
            "gene_name": row[2],
            "dbxref_id": row[3],
            "feature_type": row[4],
            "feature_qualifier": feature_qualifier,
            "headline": row[6],
            "start_coord": start,
            "stop_coord": stop,
            "strand": row[9],
            "chr_name": row[10],
            "chr_length": row[11],
        })

    return features


def get_chromosome_sequence(session, chr_name: str, seq_source: str) -> str | None:
    """Get chromosome sequence."""
    query = text(f"""
        SELECT s.residues
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
        WHERE f.feature_name = :chr_name
        AND s.source = :seq_source
        AND s.is_seq_current = 'Y'
    """)
    result = session.execute(
        query, {"chr_name": chr_name, "seq_source": seq_source}
    ).fetchone()
    return result[0] if result else None


def get_cds_subfeatures(session, feature_no: int, seq_source: str) -> list[tuple]:
    """Get CDS subfeatures for a feature."""
    query = text(f"""
        SELECT fl.start_coord, fl.stop_coord
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_relationship fr ON fr.child_feature_no = f.feature_no
        JOIN {DB_SCHEMA}.feat_location fl ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN {DB_SCHEMA}.seq s ON (fl.seq_no = s.seq_no AND s.is_seq_current = 'Y' AND s.source = :seq_source)
        WHERE fr.parent_feature_no = :feature_no
        AND fr.rank = 2
        AND f.feature_type = 'CDS'
        ORDER BY fl.start_coord
    """)

    subfeatures = []
    for row in session.execute(
        query, {"feature_no": feature_no, "seq_source": seq_source}
    ).fetchall():
        start, end = row
        if start > end:
            start, end = end, start
        subfeatures.append((start, end))

    return subfeatures


def reverse_complement(seq: str) -> str:
    """Return reverse complement of a DNA sequence."""
    complement = {
        "A": "T", "T": "A", "G": "C", "C": "G",
        "a": "t", "t": "a", "g": "c", "c": "g",
        "N": "N", "n": "n",
    }
    return "".join(complement.get(base, base) for base in reversed(seq))


def translate_sequence(dna_seq: str, trans_table: int = 12) -> str:
    """Translate DNA sequence to protein using genetic code table."""
    codon_table = {
        "TTT": "F", "TTC": "F", "TTA": "L", "TTG": "L",
        "TCT": "S", "TCC": "S", "TCA": "S", "TCG": "S",
        "TAT": "Y", "TAC": "Y", "TAA": "*", "TAG": "*",
        "TGT": "C", "TGC": "C", "TGA": "*", "TGG": "W",
        "CTT": "L", "CTC": "L", "CTA": "L", "CTG": "S" if trans_table == 12 else "L",
        "CCT": "P", "CCC": "P", "CCA": "P", "CCG": "P",
        "CAT": "H", "CAC": "H", "CAA": "Q", "CAG": "Q",
        "CGT": "R", "CGC": "R", "CGA": "R", "CGG": "R",
        "ATT": "I", "ATC": "I", "ATA": "I", "ATG": "M",
        "ACT": "T", "ACC": "T", "ACA": "T", "ACG": "T",
        "AAT": "N", "AAC": "N", "AAA": "K", "AAG": "K",
        "AGT": "S", "AGC": "S", "AGA": "R", "AGG": "R",
        "GTT": "V", "GTC": "V", "GTA": "V", "GTG": "V",
        "GCT": "A", "GCC": "A", "GCA": "A", "GCG": "A",
        "GAT": "D", "GAC": "D", "GAA": "E", "GAG": "E",
        "GGT": "G", "GGC": "G", "GGA": "G", "GGG": "G",
    }

    protein = []
    seq = dna_seq.upper()
    for i in range(0, len(seq) - 2, 3):
        codon = seq[i:i + 3]
        aa = codon_table.get(codon, "X")
        if aa == "*":
            break
        protein.append(aa)

    return "".join(protein)


def format_fasta(header: str, sequence: str, line_length: int = 60) -> str:
    """Format sequence as FASTA."""
    lines = [f">{header}"]
    for i in range(0, len(sequence), line_length):
        lines.append(sequence[i:i + line_length])
    return "\n".join(lines) + "\n"


# Feature types to include in "other_features" files
OTHER_FEATURE_TYPES = {
    "tRNA", "snoRNA", "snRNA", "ncRNA", "rRNA",
    "long_terminal_repeat", "retrotransposon",
    "blocked_reading_frame", "pseudogene",
    "repeat_region", "centromere",
}


def is_orf(feature_type: str) -> bool:
    """Check if feature type is an ORF (not including alleles - those are handled separately)."""
    return feature_type.upper() == "ORF"


def is_other_feature(feature_type: str) -> bool:
    """Check if feature type is an 'other feature' for sequence dumps."""
    return feature_type in OTHER_FEATURE_TYPES


def get_allele_parent_types(session, organism_no: int, seq_source: str) -> dict[int, str]:
    """Get parent feature types for alleles."""
    query = text(f"""
        SELECT child.feature_no, parent.feature_type
        FROM {DB_SCHEMA}.feat_relationship fr
        JOIN {DB_SCHEMA}.feature child ON fr.child_feature_no = child.feature_no
        JOIN {DB_SCHEMA}.feature parent ON fr.parent_feature_no = parent.feature_no
        JOIN {DB_SCHEMA}.feat_location fl ON (child.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN {DB_SCHEMA}.seq s ON (fl.root_seq_no = s.seq_no AND s.is_seq_current = 'Y' AND s.source = :seq_source)
        WHERE child.organism_no = :organism_no
        AND child.feature_type = 'allele'
        AND parent.feature_type != 'chromosome'
    """)
    result = {}
    for row in session.execute(query, {"organism_no": organism_no, "seq_source": seq_source}).fetchall():
        # Cast to int to ensure consistent type for dictionary lookup
        result[int(row[0])] = row[1]
    return result


def is_default_feature(feature_name: str) -> bool:
    """Check if feature is the default/representative (A allele for diploids)."""
    # A allele ends with _A, or no suffix for haploids
    return feature_name.endswith("_A") or not re.search(r"_[AB]$", feature_name)


def is_assembly_19(seq_source: str) -> bool:
    """Check if the sequence source is Assembly 19."""
    return "Assembly 19" in seq_source


def is_allele_feature(feature: dict) -> bool:
    """Check if a feature is an allele."""
    return feature["feature_type"] == "allele"


def build_feature_header(
    feat: dict,
    coords_desc: str = "",
    extra_desc: str = "",
) -> str:
    """Build FASTA header for a feature."""
    parts = [feat["feature_name"]]

    # Gene name
    if feat["gene_name"] and feat["gene_name"] != feat["feature_name"]:
        parts.append(feat["gene_name"])

    # CGDID
    if feat["dbxref_id"]:
        parts.append(f"{PROJECT_ACRONYM}ID:{feat['dbxref_id']}")

    # Coordinates
    if coords_desc:
        parts.append(coords_desc)

    # Extra description (e.g., "Exon(s) only sequence")
    if extra_desc:
        parts.append(extra_desc)

    # ORF classification
    match = re.search(r"(Verified|Uncharacterized|Dubious)", feat["feature_qualifier"])
    if match:
        parts.append(f"{match.group(1)} ORF;")

    # Headline
    if feat["headline"]:
        parts.append(feat["headline"])

    return " ".join(parts)


def get_coords_desc(feat: dict, upstream: int = 0, downstream: int = 0) -> str:
    """Build coordinates description string."""
    strand_char = "W" if feat["strand"] in ("W", "+") else "C"
    start = feat["start_coord"]
    stop = feat["stop_coord"]

    if upstream or downstream:
        if strand_char == "W":
            adj_start = max(1, start - upstream)
            adj_stop = min(feat["chr_length"], stop + downstream)
        else:
            adj_start = max(1, start - downstream)
            adj_stop = min(feat["chr_length"], stop + upstream)
        length = adj_stop - adj_start + 1
        return (
            f"COORDS:{feat['chr_name']}:{adj_start}-{adj_stop}{strand_char} "
            f"with {upstream} bases upstream and {downstream} bases downstream "
            f"({length} nucleotides)"
        )
    else:
        length = stop - start + 1
        return f"COORDS:{feat['chr_name']}:{start}-{stop}{strand_char} ({length} nucleotides)"


def extract_genomic_sequence(
    chr_seq: str,
    start: int,
    stop: int,
    strand: str,
    upstream: int = 0,
    downstream: int = 0,
) -> str:
    """Extract genomic sequence with optional flanking regions."""
    chr_length = len(chr_seq)

    if strand in ("W", "+"):
        adj_start = max(0, start - 1 - upstream)
        adj_stop = min(chr_length, stop + downstream)
    else:
        adj_start = max(0, start - 1 - downstream)
        adj_stop = min(chr_length, stop + upstream)

    sequence = chr_seq[adj_start:adj_stop]

    if strand in ("C", "-"):
        sequence = reverse_complement(sequence)

    return sequence


def extract_coding_sequence(
    chr_seq: str,
    subfeatures: list[tuple],
    strand: str,
) -> str:
    """Extract coding sequence (introns removed)."""
    if not subfeatures:
        return ""

    parts = []
    for sf_start, sf_end in subfeatures:
        part = chr_seq[sf_start - 1:sf_end]
        parts.append(part)

    coding_seq = "".join(parts)

    if strand in ("C", "-"):
        coding_seq = reverse_complement(coding_seq)

    return coding_seq


def dump_sequences(
    session,
    strain_abbrev: str,
    seq_source: str,
    output_dir: Path,
    genome_version: str,
) -> dict[str, int]:
    """
    Dump all sequence files for a strain.

    Returns dict of {file_type: count}.
    """
    config = get_strain_config(session, strain_abbrev)
    if not config:
        raise ValueError(f"Strain not found: {strain_abbrev}")

    organism_no = config["organism_no"]

    # Get all features and chromosomes
    logger.info("Fetching features...")
    features = get_features_with_locations(session, organism_no, seq_source)
    logger.info(f"Found {len(features)} features")

    logger.info("Fetching chromosomes...")
    chromosomes = get_chromosomes(session, seq_source)
    logger.info(f"Found {len(chromosomes)} chromosomes")

    # Build chromosome sequence cache
    chr_seqs = {c["name"]: c["sequence"] for c in chromosomes}

    # Batch fetch CDS subfeatures for all ORFs
    logger.info("Fetching CDS subfeatures...")
    cds_cache = {}
    for feat in features:
        if is_orf(feat["feature_type"]):
            cds_cache[feat["feature_no"]] = get_cds_subfeatures(
                session, feat["feature_no"], seq_source
            )

    # Get parent types for alleles (to include alleles of other_features)
    logger.info("Fetching allele parent types...")
    allele_parent_types = get_allele_parent_types(session, organism_no, seq_source)
    logger.info(f"Found {len(allele_parent_types)} allele parent types")

    # Separate ORFs and other features
    # ORFs include: feature_type ORF, or allele whose parent is ORF
    # Other features include: direct other_feature types, or allele whose parent is other_feature
    orfs = []
    other_features = []
    for f in features:
        if is_orf(f["feature_type"]):
            # Direct ORF
            orfs.append(f)
        elif f["feature_type"] == "allele":
            # Allele - check parent type (cast to int for consistent lookup)
            parent_type = allele_parent_types.get(int(f["feature_no"]))
            if parent_type == "ORF":
                orfs.append(f)
            elif parent_type and is_other_feature(parent_type):
                other_features.append(f)
        elif is_other_feature(f["feature_type"]):
            # Direct other feature
            other_features.append(f)
    logger.info(f"ORFs: {len(orfs)}, Other features: {len(other_features)}")

    # Build features by chromosome for intergenic calculation
    features_by_chr = defaultdict(list)
    for feat in features:
        features_by_chr[feat["chr_name"]].append(feat)

    # Sort by position
    for chr_name in features_by_chr:
        features_by_chr[chr_name].sort(key=lambda f: f["start_coord"])

    counts = {}
    file_prefix = f"{strain_abbrev}_version_{genome_version}"

    # 1. Chromosomes
    logger.info("Writing chromosomes...")
    with open(output_dir / f"{file_prefix}_chromosomes.fasta", "w") as f:
        for chrom in chromosomes:
            if chrom["sequence"]:
                header = f"{chrom['name']} ({chrom['length']} nucleotides)"
                f.write(format_fasta(header, chrom["sequence"]))
    counts["chromosomes"] = len(chromosomes)

    # 2. ORF genomic
    logger.info("Writing ORF genomic...")
    with open(output_dir / f"{file_prefix}_orf_genomic.fasta", "w") as f:
        for feat in orfs:
            chr_seq = chr_seqs.get(feat["chr_name"])
            if not chr_seq:
                continue
            seq = extract_genomic_sequence(
                chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"]
            )
            if seq:
                coords = get_coords_desc(feat)
                header = build_feature_header(feat, coords)
                f.write(format_fasta(header, seq))
    counts["orf_genomic"] = len(orfs)

    # 3. ORF genomic 1000
    logger.info("Writing ORF genomic 1000...")
    with open(output_dir / f"{file_prefix}_orf_genomic_1000.fasta", "w") as f:
        for feat in orfs:
            chr_seq = chr_seqs.get(feat["chr_name"])
            if not chr_seq:
                continue
            seq = extract_genomic_sequence(
                chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"],
                upstream=1000, downstream=1000
            )
            if seq:
                coords = get_coords_desc(feat, upstream=1000, downstream=1000)
                header = build_feature_header(feat, coords)
                f.write(format_fasta(header, seq))
    counts["orf_genomic_1000"] = len(orfs)

    # 4. ORF coding
    logger.info("Writing ORF coding...")
    with open(output_dir / f"{file_prefix}_orf_coding.fasta", "w") as f:
        for feat in orfs:
            chr_seq = chr_seqs.get(feat["chr_name"])
            if not chr_seq:
                continue
            subfeatures = cds_cache.get(feat["feature_no"], [])
            if subfeatures:
                seq = extract_coding_sequence(chr_seq, subfeatures, feat["strand"])
            else:
                seq = extract_genomic_sequence(
                    chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"]
                )
            if seq:
                coords = get_coords_desc(feat)
                extra = f"Exon(s) only sequence ({len(seq)} nucleotides)"
                header = build_feature_header(feat, coords, extra)
                f.write(format_fasta(header, seq))
    counts["orf_coding"] = len(orfs)

    # 5. ORF translations
    logger.info("Writing ORF translations...")
    with open(output_dir / f"{file_prefix}_orf_trans_all.fasta", "w") as f:
        for feat in orfs:
            chr_seq = chr_seqs.get(feat["chr_name"])
            if not chr_seq:
                continue
            subfeatures = cds_cache.get(feat["feature_no"], [])
            if subfeatures:
                coding_seq = extract_coding_sequence(chr_seq, subfeatures, feat["strand"])
            else:
                coding_seq = extract_genomic_sequence(
                    chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"]
                )
            if coding_seq:
                protein = translate_sequence(coding_seq)
                if protein:
                    coords = get_coords_desc(feat)
                    extra = f"translated using codon table 12 ({len(protein)} amino acids)"
                    header = build_feature_header(feat, coords, extra)
                    f.write(format_fasta(header, protein))
    counts["orf_trans_all"] = len(orfs)

    # 6. ORF plus intergenic (extend to adjacent features)
    logger.info("Writing ORF plus intergenic...")
    with open(output_dir / f"{file_prefix}_orf_plus_intergenic.fasta", "w") as f:
        for feat in orfs:
            chr_seq = chr_seqs.get(feat["chr_name"])
            if not chr_seq:
                continue

            # Find adjacent features
            chr_feats = features_by_chr[feat["chr_name"]]
            idx = next((i for i, cf in enumerate(chr_feats) if cf["feature_no"] == feat["feature_no"]), -1)

            # Calculate upstream/downstream to adjacent features
            if idx > 0:
                prev_feat = chr_feats[idx - 1]
                upstream = (feat["start_coord"] - prev_feat["stop_coord"]) // 2
            else:
                upstream = feat["start_coord"] - 1

            if idx < len(chr_feats) - 1:
                next_feat = chr_feats[idx + 1]
                downstream = (next_feat["start_coord"] - feat["stop_coord"]) // 2
            else:
                downstream = len(chr_seq) - feat["stop_coord"]

            upstream = max(0, min(upstream, feat["start_coord"] - 1))
            downstream = max(0, min(downstream, len(chr_seq) - feat["stop_coord"]))

            seq = extract_genomic_sequence(
                chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"],
                upstream=upstream, downstream=downstream
            )
            if seq:
                coords = get_coords_desc(feat, upstream=upstream, downstream=downstream)
                header = build_feature_header(feat, coords)
                f.write(format_fasta(header, seq))
    counts["orf_plus_intergenic"] = len(orfs)

    # 7. Other features genomic
    logger.info("Writing other features genomic...")
    with open(output_dir / f"{file_prefix}_other_features_genomic.fasta", "w") as f:
        for feat in other_features:
            chr_seq = chr_seqs.get(feat["chr_name"])
            if not chr_seq:
                continue
            seq = extract_genomic_sequence(
                chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"]
            )
            if seq:
                coords = get_coords_desc(feat)
                header = build_feature_header(feat, coords)
                f.write(format_fasta(header, seq))
    counts["other_features_genomic"] = len(other_features)

    # 8. Other features genomic 1000
    logger.info("Writing other features genomic 1000...")
    with open(output_dir / f"{file_prefix}_other_features_genomic_1000.fasta", "w") as f:
        for feat in other_features:
            chr_seq = chr_seqs.get(feat["chr_name"])
            if not chr_seq:
                continue
            seq = extract_genomic_sequence(
                chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"],
                upstream=1000, downstream=1000
            )
            if seq:
                coords = get_coords_desc(feat, upstream=1000, downstream=1000)
                header = build_feature_header(feat, coords)
                f.write(format_fasta(header, seq))
    counts["other_features_genomic_1000"] = len(other_features)

    # 9. Other features no introns
    logger.info("Writing other features no introns...")
    with open(output_dir / f"{file_prefix}_other_features_no_introns.fasta", "w") as f:
        for feat in other_features:
            chr_seq = chr_seqs.get(feat["chr_name"])
            if not chr_seq:
                continue
            subfeatures = get_cds_subfeatures(session, feat["feature_no"], seq_source)
            if subfeatures:
                seq = extract_coding_sequence(chr_seq, subfeatures, feat["strand"])
            else:
                seq = extract_genomic_sequence(
                    chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"]
                )
            if seq:
                coords = get_coords_desc(feat)
                extra = f"Exon(s) only sequence ({len(seq)} nucleotides)"
                header = build_feature_header(feat, coords, extra)
                f.write(format_fasta(header, seq))
    counts["other_features_no_introns"] = len(other_features)

    # 10. Other features plus intergenic
    logger.info("Writing other features plus intergenic...")
    with open(output_dir / f"{file_prefix}_other_features_plus_intergenic.fasta", "w") as f:
        for feat in other_features:
            chr_seq = chr_seqs.get(feat["chr_name"])
            if not chr_seq:
                continue

            chr_feats = features_by_chr[feat["chr_name"]]
            idx = next((i for i, cf in enumerate(chr_feats) if cf["feature_no"] == feat["feature_no"]), -1)

            if idx > 0:
                prev_feat = chr_feats[idx - 1]
                upstream = (feat["start_coord"] - prev_feat["stop_coord"]) // 2
            else:
                upstream = feat["start_coord"] - 1

            if idx < len(chr_feats) - 1:
                next_feat = chr_feats[idx + 1]
                downstream = (next_feat["start_coord"] - feat["stop_coord"]) // 2
            else:
                downstream = len(chr_seq) - feat["stop_coord"]

            upstream = max(0, min(upstream, feat["start_coord"] - 1))
            downstream = max(0, min(downstream, len(chr_seq) - feat["stop_coord"]))

            seq = extract_genomic_sequence(
                chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"],
                upstream=upstream, downstream=downstream
            )
            if seq:
                coords = get_coords_desc(feat, upstream=upstream, downstream=downstream)
                header = build_feature_header(feat, coords)
                f.write(format_fasta(header, seq))
    counts["other_features_plus_intergenic"] = len(other_features)

    # 11. Not feature (intergenic)
    logger.info("Writing intergenic sequences...")
    intergenic_count = 0
    with open(output_dir / f"{file_prefix}_not_feature.fasta", "w") as f:
        for chrom in chromosomes:
            chr_seq = chrom["sequence"]
            if not chr_seq:
                continue

            chr_feats = features_by_chr.get(chrom["name"], [])
            prev_end = 0

            for feat in chr_feats:
                if feat["start_coord"] > prev_end + 1:
                    # Intergenic region
                    start = prev_end + 1
                    end = feat["start_coord"] - 1
                    seq = chr_seq[start - 1:end]

                    if seq:
                        if prev_end == 0:
                            desc = f"between start of {chrom['name']} and {feat['feature_name']}"
                        else:
                            prev_feat_name = chr_feats[chr_feats.index(feat) - 1]["feature_name"] if chr_feats.index(feat) > 0 else "start"
                            desc = f"between {prev_feat_name} and {feat['feature_name']}"

                        header = f"{chrom['name']}:{start}-{end} {desc}"
                        f.write(format_fasta(header, seq))
                        intergenic_count += 1

                prev_end = max(prev_end, feat["stop_coord"])

            # After last feature
            if prev_end < len(chr_seq):
                start = prev_end + 1
                end = len(chr_seq)
                seq = chr_seq[start - 1:end]
                if seq and chr_feats:
                    desc = f"between {chr_feats[-1]['feature_name']} and end of {chrom['name']}"
                    header = f"{chrom['name']}:{start}-{end} {desc}"
                    f.write(format_fasta(header, seq))
                    intergenic_count += 1

    counts["not_feature"] = intergenic_count

    # 12-14. Default sequences (representative, A allele only)
    default_orfs = [f for f in orfs if is_default_feature(f["feature_name"])]
    logger.info(f"Writing default sequences ({len(default_orfs)} features)...")

    # Default coding
    with open(output_dir / f"{file_prefix}_default_coding.fasta", "w") as f:
        for feat in default_orfs:
            chr_seq = chr_seqs.get(feat["chr_name"])
            if not chr_seq:
                continue
            subfeatures = cds_cache.get(feat["feature_no"], [])
            if subfeatures:
                seq = extract_coding_sequence(chr_seq, subfeatures, feat["strand"])
            else:
                seq = extract_genomic_sequence(
                    chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"]
                )
            if seq:
                f.write(format_fasta(feat["feature_name"], seq))
    counts["default_coding"] = len(default_orfs)

    # Default genomic
    with open(output_dir / f"{file_prefix}_default_genomic.fasta", "w") as f:
        for feat in default_orfs:
            chr_seq = chr_seqs.get(feat["chr_name"])
            if not chr_seq:
                continue
            seq = extract_genomic_sequence(
                chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"]
            )
            if seq:
                f.write(format_fasta(feat["feature_name"], seq))
    counts["default_genomic"] = len(default_orfs)

    # Default protein
    with open(output_dir / f"{file_prefix}_default_protein.fasta", "w") as f:
        for feat in default_orfs:
            chr_seq = chr_seqs.get(feat["chr_name"])
            if not chr_seq:
                continue
            subfeatures = cds_cache.get(feat["feature_no"], [])
            if subfeatures:
                coding_seq = extract_coding_sequence(chr_seq, subfeatures, feat["strand"])
            else:
                coding_seq = extract_genomic_sequence(
                    chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"]
                )
            if coding_seq:
                protein = translate_sequence(coding_seq)
                if protein:
                    f.write(format_fasta(feat["feature_name"], protein))
    counts["default_protein"] = len(default_orfs)

    # 15-23. Haploid files for Assembly 19 (exclude alleles)
    if is_assembly_19(seq_source):
        logger.info("Generating haploid files for Assembly 19...")

        # Filter out allele features for haploid files
        haploid_orfs = [f for f in orfs if not is_allele_feature(f)]
        haploid_other = [f for f in other_features if not is_allele_feature(f)]
        logger.info(f"Haploid ORFs: {len(haploid_orfs)}, Haploid other: {len(haploid_other)}")

        # ORF genomic haploid
        logger.info("Writing ORF genomic haploid...")
        with open(output_dir / f"{file_prefix}_orf_genomic_haploid.fasta", "w") as f:
            for feat in haploid_orfs:
                chr_seq = chr_seqs.get(feat["chr_name"])
                if not chr_seq:
                    continue
                seq = extract_genomic_sequence(
                    chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"]
                )
                if seq:
                    coords = get_coords_desc(feat)
                    header = build_feature_header(feat, coords)
                    f.write(format_fasta(header, seq))
        counts["orf_genomic_haploid"] = len(haploid_orfs)

        # ORF genomic 1000 haploid
        logger.info("Writing ORF genomic 1000 haploid...")
        with open(output_dir / f"{file_prefix}_orf_genomic_1000_haploid.fasta", "w") as f:
            for feat in haploid_orfs:
                chr_seq = chr_seqs.get(feat["chr_name"])
                if not chr_seq:
                    continue
                seq = extract_genomic_sequence(
                    chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"],
                    upstream=1000, downstream=1000
                )
                if seq:
                    coords = get_coords_desc(feat, upstream=1000, downstream=1000)
                    header = build_feature_header(feat, coords)
                    f.write(format_fasta(header, seq))
        counts["orf_genomic_1000_haploid"] = len(haploid_orfs)

        # ORF coding haploid
        logger.info("Writing ORF coding haploid...")
        with open(output_dir / f"{file_prefix}_orf_coding_haploid.fasta", "w") as f:
            for feat in haploid_orfs:
                chr_seq = chr_seqs.get(feat["chr_name"])
                if not chr_seq:
                    continue
                subfeatures = cds_cache.get(feat["feature_no"], [])
                if subfeatures:
                    seq = extract_coding_sequence(chr_seq, subfeatures, feat["strand"])
                else:
                    seq = extract_genomic_sequence(
                        chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"]
                    )
                if seq:
                    coords = get_coords_desc(feat)
                    extra = f"Exon(s) only sequence ({len(seq)} nucleotides)"
                    header = build_feature_header(feat, coords, extra)
                    f.write(format_fasta(header, seq))
        counts["orf_coding_haploid"] = len(haploid_orfs)

        # ORF translations haploid
        logger.info("Writing ORF translations haploid...")
        with open(output_dir / f"{file_prefix}_orf_trans_all_haploid.fasta", "w") as f:
            for feat in haploid_orfs:
                chr_seq = chr_seqs.get(feat["chr_name"])
                if not chr_seq:
                    continue
                subfeatures = cds_cache.get(feat["feature_no"], [])
                if subfeatures:
                    coding_seq = extract_coding_sequence(chr_seq, subfeatures, feat["strand"])
                else:
                    coding_seq = extract_genomic_sequence(
                        chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"]
                    )
                if coding_seq:
                    protein = translate_sequence(coding_seq)
                    if protein:
                        coords = get_coords_desc(feat)
                        extra = f"translated using codon table 12 ({len(protein)} amino acids)"
                        header = build_feature_header(feat, coords, extra)
                        f.write(format_fasta(header, protein))
        counts["orf_trans_all_haploid"] = len(haploid_orfs)

        # ORF plus intergenic haploid
        logger.info("Writing ORF plus intergenic haploid...")
        with open(output_dir / f"{file_prefix}_orf_plus_intergenic_haploid.fasta", "w") as f:
            for feat in haploid_orfs:
                chr_seq = chr_seqs.get(feat["chr_name"])
                if not chr_seq:
                    continue
                chr_feats = features_by_chr[feat["chr_name"]]
                idx = next((i for i, cf in enumerate(chr_feats) if cf["feature_no"] == feat["feature_no"]), -1)
                if idx > 0:
                    prev_feat = chr_feats[idx - 1]
                    upstream = (feat["start_coord"] - prev_feat["stop_coord"]) // 2
                else:
                    upstream = feat["start_coord"] - 1
                if idx < len(chr_feats) - 1:
                    next_feat = chr_feats[idx + 1]
                    downstream = (next_feat["start_coord"] - feat["stop_coord"]) // 2
                else:
                    downstream = len(chr_seq) - feat["stop_coord"]
                upstream = max(0, min(upstream, feat["start_coord"] - 1))
                downstream = max(0, min(downstream, len(chr_seq) - feat["stop_coord"]))
                seq = extract_genomic_sequence(
                    chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"],
                    upstream=upstream, downstream=downstream
                )
                if seq:
                    coords = get_coords_desc(feat, upstream=upstream, downstream=downstream)
                    header = build_feature_header(feat, coords)
                    f.write(format_fasta(header, seq))
        counts["orf_plus_intergenic_haploid"] = len(haploid_orfs)

        # Other features genomic haploid
        logger.info("Writing other features genomic haploid...")
        with open(output_dir / f"{file_prefix}_other_features_genomic_haploid.fasta", "w") as f:
            for feat in haploid_other:
                chr_seq = chr_seqs.get(feat["chr_name"])
                if not chr_seq:
                    continue
                seq = extract_genomic_sequence(
                    chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"]
                )
                if seq:
                    coords = get_coords_desc(feat)
                    header = build_feature_header(feat, coords)
                    f.write(format_fasta(header, seq))
        counts["other_features_genomic_haploid"] = len(haploid_other)

        # Other features genomic 1000 haploid
        logger.info("Writing other features genomic 1000 haploid...")
        with open(output_dir / f"{file_prefix}_other_features_genomic_1000_haploid.fasta", "w") as f:
            for feat in haploid_other:
                chr_seq = chr_seqs.get(feat["chr_name"])
                if not chr_seq:
                    continue
                seq = extract_genomic_sequence(
                    chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"],
                    upstream=1000, downstream=1000
                )
                if seq:
                    coords = get_coords_desc(feat, upstream=1000, downstream=1000)
                    header = build_feature_header(feat, coords)
                    f.write(format_fasta(header, seq))
        counts["other_features_genomic_1000_haploid"] = len(haploid_other)

        # Other features no introns haploid
        logger.info("Writing other features no introns haploid...")
        with open(output_dir / f"{file_prefix}_other_features_no_introns_haploid.fasta", "w") as f:
            for feat in haploid_other:
                chr_seq = chr_seqs.get(feat["chr_name"])
                if not chr_seq:
                    continue
                subfeatures = get_cds_subfeatures(session, feat["feature_no"], seq_source)
                if subfeatures:
                    seq = extract_coding_sequence(chr_seq, subfeatures, feat["strand"])
                else:
                    seq = extract_genomic_sequence(
                        chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"]
                    )
                if seq:
                    coords = get_coords_desc(feat)
                    extra = f"Exon(s) only sequence ({len(seq)} nucleotides)"
                    header = build_feature_header(feat, coords, extra)
                    f.write(format_fasta(header, seq))
        counts["other_features_no_introns_haploid"] = len(haploid_other)

        # Other features plus intergenic haploid
        logger.info("Writing other features plus intergenic haploid...")
        with open(output_dir / f"{file_prefix}_other_features_plus_intergenic_haploid.fasta", "w") as f:
            for feat in haploid_other:
                chr_seq = chr_seqs.get(feat["chr_name"])
                if not chr_seq:
                    continue
                chr_feats = features_by_chr[feat["chr_name"]]
                idx = next((i for i, cf in enumerate(chr_feats) if cf["feature_no"] == feat["feature_no"]), -1)
                if idx > 0:
                    prev_feat = chr_feats[idx - 1]
                    upstream = (feat["start_coord"] - prev_feat["stop_coord"]) // 2
                else:
                    upstream = feat["start_coord"] - 1
                if idx < len(chr_feats) - 1:
                    next_feat = chr_feats[idx + 1]
                    downstream = (next_feat["start_coord"] - feat["stop_coord"]) // 2
                else:
                    downstream = len(chr_seq) - feat["stop_coord"]
                upstream = max(0, min(upstream, feat["start_coord"] - 1))
                downstream = max(0, min(downstream, len(chr_seq) - feat["stop_coord"]))
                seq = extract_genomic_sequence(
                    chr_seq, feat["start_coord"], feat["stop_coord"], feat["strand"],
                    upstream=upstream, downstream=downstream
                )
                if seq:
                    coords = get_coords_desc(feat, upstream=upstream, downstream=downstream)
                    header = build_feature_header(feat, coords)
                    f.write(format_fasta(header, seq))
        counts["other_features_plus_intergenic_haploid"] = len(haploid_other)

    return counts


def gzip_file(file_path: Path) -> None:
    """Gzip a file and remove the original."""
    with open(file_path, "rb") as f_in:
        with gzip.open(f"{file_path}.gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    file_path.unlink()


def generate_sequences(strain_abbrev: str, seq_source: str | None = None) -> bool:
    """
    Generate sequence files for a strain with validation and safety checks.

    Returns True on success, False on failure.
    """
    temp_dir = Path(tempfile.mkdtemp(prefix="seq_"))

    try:
        with SessionLocal() as session:
            # Get strain config
            config = get_strain_config(session, strain_abbrev)
            if not config:
                error_msg = f"Strain {strain_abbrev} not found"
                logger.error(error_msg)
                send_slack_message(error_msg, is_error=True)
                return False

            # Get or detect seq_source
            if not seq_source:
                seq_source = get_seq_source(session, config["organism_no"])
                if not seq_source:
                    error_msg = f"No seq_source found for {strain_abbrev}"
                    logger.error(error_msg)
                    send_slack_message(error_msg, is_error=True)
                    return False

            logger.info(f"Seq source: {seq_source}")

            # Get genome version
            genome_version = get_genome_version(session, seq_source)
            if not genome_version:
                error_msg = f"No genome version found for {seq_source}"
                logger.error(error_msg)
                send_slack_message(error_msg, is_error=True)
                return False

            logger.info(f"Genome version: {genome_version}")

            # Determine output directory
            assembly_num = get_assembly_number(genome_version)
            if assembly_num:
                final_dir = DOWNLOAD_DIR / "sequence" / strain_abbrev / f"Assembly{assembly_num}" / "current"
            else:
                final_dir = DOWNLOAD_DIR / "sequence" / strain_abbrev / "current"

            final_dir.mkdir(parents=True, exist_ok=True)
            archive_dir = final_dir.parent / "archive"
            archive_dir.mkdir(parents=True, exist_ok=True)

            logger.info(f"Output directory: {final_dir}")

            # Generate sequences to temp directory
            counts = dump_sequences(
                session, strain_abbrev, seq_source, temp_dir, genome_version
            )

            # Validate and gzip each file
            file_prefix = f"{strain_abbrev}_version_{genome_version}"
            today_tag = datetime.now().strftime("%Y%m%d")
            all_valid = True

            for file_type, count in counts.items():
                temp_file = temp_dir / f"{file_prefix}_{file_type}.fasta"
                if not temp_file.exists():
                    continue

                # Gzip
                gzip_file(temp_file)
                temp_gz = temp_dir / f"{file_prefix}_{file_type}.fasta.gz"

                # Validate
                final_gz = final_dir / f"{file_prefix}_{file_type}.fasta.gz"
                is_valid, msg = validate_output_file(
                    temp_gz, final_gz if final_gz.exists() else None,
                    file_type, strain_abbrev
                )

                if not is_valid:
                    logger.error(f"Validation failed for {file_type}: {msg}")
                    all_valid = False
                    continue

                logger.info(f"{file_type}: {msg}")

                # Archive existing
                if final_gz.exists():
                    archive_file = archive_dir / f"{file_prefix}_{file_type}_{today_tag}.fasta.gz"
                    try:
                        shutil.copy(str(final_gz), str(archive_file))
                    except Exception as e:
                        logger.warning(f"Could not archive {final_gz}: {e}")

                # Copy new file
                shutil.copy(str(temp_gz), str(final_gz))

            if not all_valid:
                send_slack_message(
                    f"Some files failed validation for {strain_abbrev}",
                    is_error=True
                )
                return False

            # Print summary
            print(f"*{strain_abbrev}* ({seq_source}):")
            for file_type, count in sorted(counts.items()):
                print(f"  {file_type}: {count}")

            send_slack_message(
                f"Successfully generated sequence files for {strain_abbrev}"
            )
            return True

    except Exception as e:
        logger.exception(f"Error generating sequences for {strain_abbrev}: {e}")
        send_slack_message(
            f"Error generating sequences for {strain_abbrev}: {e}",
            is_error=True
        )
        return False

    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump sequence files in FASTA format"
    )
    parser.add_argument(
        "strain_abbrev",
        nargs="?",
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "seq_source",
        nargs="?",
        default=None,
        help="Sequence source (optional, auto-detected if not provided)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate sequences for all strains",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory (overrides default location)",
    )

    args = parser.parse_args()

    if args.all:
        strains = [
            ("C_albicans_SC5314", "C. albicans SC5314 Assembly 22"),
            ("C_albicans_SC5314", "C. albicans SC5314 Assembly 21"),
            ("C_albicans_SC5314", "C. albicans SC5314 Assembly 19"),
            ("C_auris_B8441", None),
            ("C_dubliniensis_CD36", None),
            ("C_glabrata_CBS138", None),
            ("C_parapsilosis_CDC317", None),
            ("C_tropicalis", None),
        ]
        success = True
        for strain, seq_source in strains:
            if not generate_sequences(strain, seq_source):
                success = False
        return 0 if success else 1

    elif args.strain_abbrev:
        if generate_sequences(args.strain_abbrev, args.seq_source):
            return 0
        return 1

    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
