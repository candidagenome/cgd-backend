#!/usr/bin/env python3
"""
Dump sequence files in FASTA format for a strain.

This script generates various FASTA sequence files:
- Chromosome sequences
- ORF genomic sequences (with optional flanking regions)
- ORF coding sequences (introns removed)
- ORF protein translations
- Other feature sequences (ncRNA, tRNA, etc.)

Based on dumpSequence.pl by CGD team.

Usage:
    python dump_sequence.py <strain_abbrev> [seq_source]
    python dump_sequence.py C_albicans_SC5314
    python dump_sequence.py C_albicans_SC5314 --output-dir ./sequences

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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


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


def get_chromosomes(session, seq_source: str) -> list[dict]:
    """Get chromosome/contig names for a sequence source."""
    query = text(f"""
        SELECT f.feature_name, s.residues
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
        })

    return chromosomes


def get_features(session, strain_abbrev: str, seq_source: str) -> list[dict]:
    """Get features with their sequences and location info."""
    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.gene_name, f.dbxref_id,
               f.feature_type, f.feature_qualifier, f.headline,
               fl.min_coord, fl.max_coord, fl.strand,
               s.seq_name as root_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN {DB_SCHEMA}.seq s ON (fl.root_seq_no = s.seq_no AND s.is_seq_current = 'Y' AND s.source = :seq_source)
        WHERE f.organism_abbrev = :strain_abbrev
        AND f.feature_type NOT IN ('chromosome', 'contig')
        ORDER BY f.feature_name
    """)

    features = []
    for row in session.execute(
        query, {"strain_abbrev": strain_abbrev, "seq_source": seq_source}
    ).fetchall():
        feature_qualifier = row[5] or ""

        # Skip deleted features
        if "Deleted" in feature_qualifier:
            continue

        features.append({
            "feature_no": row[0],
            "feature_name": row[1],
            "gene_name": row[2],
            "dbxref_id": row[3],
            "feature_type": row[4],
            "feature_qualifier": feature_qualifier,
            "headline": row[6],
            "min_coord": row[7],
            "max_coord": row[8],
            "strand": row[9],
            "root_name": row[10],
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


def get_feature_sequence(
    session,
    feature_no: int,
    seq_source: str,
    upstream: int = 0,
    downstream: int = 0,
) -> tuple[str | None, str]:
    """
    Get genomic sequence for a feature.

    Returns (sequence, location_description)
    """
    # Get feature location
    loc_query = text(f"""
        SELECT fl.min_coord, fl.max_coord, fl.strand, s.seq_no, s.residues as chr_seq
        FROM {DB_SCHEMA}.feat_location fl
        JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
        WHERE fl.feature_no = :feature_no
        AND fl.is_loc_current = 'Y'
        AND s.is_seq_current = 'Y'
        AND s.source = :seq_source
    """)
    loc_result = session.execute(
        loc_query, {"feature_no": feature_no, "seq_source": seq_source}
    ).fetchone()

    if not loc_result:
        return None, ""

    min_coord, max_coord, strand, seq_no, chr_seq = loc_result

    if not chr_seq:
        return None, ""

    # Calculate coordinates with flanking
    start = min_coord - 1  # 0-based
    end = max_coord

    # Adjust for flanking regions based on strand
    if strand == "W" or strand == "+":
        start = max(0, start - upstream)
        end = min(len(chr_seq), end + downstream)
    else:
        start = max(0, start - downstream)
        end = min(len(chr_seq), end + upstream)

    sequence = chr_seq[start:end]

    # Reverse complement if on C strand
    if strand == "C" or strand == "-":
        sequence = reverse_complement(sequence)

    # Build location description
    loc_desc = ""
    if upstream or downstream:
        loc_desc = f"with {upstream} bases upstream and {downstream} bases downstream"

    return sequence, loc_desc


def get_feature_coding_sequence(session, feature_no: int, seq_source: str) -> str | None:
    """Get coding sequence (introns removed) for a feature."""
    # Get subfeatures (CDS)
    sf_query = text(f"""
        SELECT sf.relative_coord_start, sf.relative_coord_end
        FROM {DB_SCHEMA}.subfeature sf
        WHERE sf.feature_no = :feature_no
        AND sf.subfeature_type = 'CDS'
        ORDER BY sf.relative_coord_start
    """)

    subfeatures = []
    for row in session.execute(sf_query, {"feature_no": feature_no}).fetchall():
        start, end = row
        if start > end:
            start, end = end, start
        subfeatures.append((start, end))

    if not subfeatures:
        # No subfeatures, return full genomic sequence
        seq, _ = get_feature_sequence(session, feature_no, seq_source)
        return seq

    # Get chromosome sequence and feature location
    loc_query = text(f"""
        SELECT fl.min_coord, fl.strand, s.residues as chr_seq
        FROM {DB_SCHEMA}.feat_location fl
        JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
        WHERE fl.feature_no = :feature_no
        AND fl.is_loc_current = 'Y'
        AND s.is_seq_current = 'Y'
        AND s.source = :seq_source
    """)
    loc_result = session.execute(
        loc_query, {"feature_no": feature_no, "seq_source": seq_source}
    ).fetchone()

    if not loc_result:
        return None

    min_coord, strand, chr_seq = loc_result

    if not chr_seq:
        return None

    # Extract and concatenate CDS sequences
    coding_parts = []
    for sf_start, sf_end in subfeatures:
        # Subfeature coordinates are absolute chromosome coordinates
        part = chr_seq[sf_start - 1:sf_end]
        coding_parts.append(part)

    coding_seq = "".join(coding_parts)

    # Reverse complement if on C strand
    if strand == "C" or strand == "-":
        coding_seq = reverse_complement(coding_seq)

    return coding_seq


def reverse_complement(seq: str) -> str:
    """Return reverse complement of a DNA sequence."""
    complement = {"A": "T", "T": "A", "G": "C", "C": "G",
                  "a": "t", "t": "a", "g": "c", "c": "g",
                  "N": "N", "n": "n"}
    return "".join(complement.get(base, base) for base in reversed(seq))


def translate_sequence(dna_seq: str, trans_table: int = 12) -> str:
    """
    Translate DNA sequence to protein.

    Uses genetic code table (default: 12 = alternative yeast nuclear code).
    """
    # Standard genetic code with CTG -> Ser for table 12
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
        if aa == "*":  # Stop codon
            break
        protein.append(aa)

    return "".join(protein)


def format_fasta(seq_id: str, description: str, sequence: str, line_length: int = 60) -> str:
    """Format sequence as FASTA."""
    lines = [f">{seq_id} {description}"]
    for i in range(0, len(sequence), line_length):
        lines.append(sequence[i:i + line_length])
    return "\n".join(lines) + "\n"


def is_coding_feature(feature_type: str) -> bool:
    """Check if feature type is protein-coding."""
    return feature_type.upper() == "ORF"


def dump_chromosomes(
    session,
    seq_source: str,
    output_file: Path,
) -> int:
    """Dump chromosome sequences to file."""
    chromosomes = get_chromosomes(session, seq_source)

    with open(output_file, "w") as f:
        for chrom in chromosomes:
            if chrom["sequence"]:
                fasta = format_fasta(chrom["name"], "", chrom["sequence"])
                f.write(fasta)

    return len(chromosomes)


def dump_feature_sequences(
    session,
    strain_abbrev: str,
    seq_source: str,
    output_file: Path,
    coding_only: bool = True,
    upstream: int = 0,
    downstream: int = 0,
    coding_seq: bool = False,
    translate: bool = False,
    trans_table: int = 12,
) -> int:
    """
    Dump feature sequences to file.

    Args:
        coding_only: If True, only dump ORFs; if False, only non-coding features
        upstream: Bases upstream to include
        downstream: Bases downstream to include
        coding_seq: If True, remove introns
        translate: If True, translate to protein
    """
    features = get_features(session, strain_abbrev, seq_source)

    count = 0
    with open(output_file, "w") as f:
        for feat in features:
            is_coding = is_coding_feature(feat["feature_type"])

            # Filter by coding/non-coding
            if coding_only and not is_coding:
                continue
            if not coding_only and is_coding:
                continue

            # Skip alleles for certain dumps
            if feat["feature_type"] == "allele":
                continue

            # Get sequence based on options
            if translate or coding_seq:
                sequence = get_feature_coding_sequence(session, feat["feature_no"], seq_source)
                if translate and sequence:
                    sequence = translate_sequence(sequence, trans_table)
            else:
                sequence, loc_desc = get_feature_sequence(
                    session, feat["feature_no"], seq_source, upstream, downstream
                )

            if not sequence:
                continue

            # Build description
            name = feat["gene_name"] or feat["feature_name"]
            desc_parts = [name, f"{PROJECT_ACRONYM}ID:{feat['dbxref_id']}"]

            if loc_desc:
                desc_parts.append(loc_desc)

            # Add ORF classification if available
            import re
            match = re.search(r"(Verified|Uncharacterized|Dubious)", feat["feature_qualifier"])
            if match:
                desc_parts.append(f"{match.group(1)} ORF")

            # Add headline if available
            if feat["headline"]:
                desc_parts.append(feat["headline"])

            description = " ".join(desc_parts)

            fasta = format_fasta(feat["feature_name"], description, sequence)
            f.write(fasta)
            count += 1

    return count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump sequence files in FASTA format"
    )
    parser.add_argument(
        "strain_abbrev",
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "seq_source",
        nargs="?",
        default=None,
        help="Sequence source (optional, auto-detected if not provided)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory (default: current directory)",
    )
    parser.add_argument(
        "--type",
        choices=["all", "chromosomes", "orf_genomic", "orf_genomic_1000",
                 "orf_coding", "orf_trans", "other_features"],
        default="all",
        help="Type of sequences to dump (default: all)",
    )
    parser.add_argument(
        "--gzip", "-z",
        action="store_true",
        help="Gzip the output files",
    )

    args = parser.parse_args()

    strain_abbrev = args.strain_abbrev

    # Set up output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = Path(".")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Set up logging to file
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"dump_sequence_{strain_abbrev}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Dumping sequences for {strain_abbrev}")

    try:
        with SessionLocal() as session:
            # Get strain config
            config = get_strain_config(session, strain_abbrev)
            if not config:
                logger.error(f"Strain not found: {strain_abbrev}")
                return 1

            # Get or detect seq_source
            seq_source = args.seq_source
            if not seq_source:
                seq_source = get_seq_source(session, strain_abbrev)
                if not seq_source:
                    logger.error(f"No seq_source found for {strain_abbrev}")
                    return 1

            logger.info(f"Seq source: {seq_source}")

            def write_file(filename: str, count: int):
                logger.info(f"Wrote {count} sequences to {filename}")
                if args.gzip:
                    with open(output_dir / filename, "rb") as f_in:
                        with gzip.open(output_dir / f"{filename}.gz", "wb") as f_out:
                            f_out.writelines(f_in)
                    (output_dir / filename).unlink()

            # Dump requested sequence types
            if args.type in ("all", "chromosomes"):
                count = dump_chromosomes(
                    session, seq_source,
                    output_dir / f"{strain_abbrev}_chromosomes.fasta"
                )
                write_file(f"{strain_abbrev}_chromosomes.fasta", count)

            if args.type in ("all", "orf_genomic"):
                count = dump_feature_sequences(
                    session, strain_abbrev, seq_source,
                    output_dir / "orf_genomic.fasta",
                    coding_only=True
                )
                write_file("orf_genomic.fasta", count)

            if args.type in ("all", "orf_genomic_1000"):
                count = dump_feature_sequences(
                    session, strain_abbrev, seq_source,
                    output_dir / "orf_genomic_1000.fasta",
                    coding_only=True, upstream=1000, downstream=1000
                )
                write_file("orf_genomic_1000.fasta", count)

            if args.type in ("all", "orf_coding"):
                count = dump_feature_sequences(
                    session, strain_abbrev, seq_source,
                    output_dir / "orf_coding.fasta",
                    coding_only=True, coding_seq=True
                )
                write_file("orf_coding.fasta", count)

            if args.type in ("all", "orf_trans"):
                count = dump_feature_sequences(
                    session, strain_abbrev, seq_source,
                    output_dir / "orf_trans_all.fasta",
                    coding_only=True, translate=True
                )
                write_file("orf_trans_all.fasta", count)

            if args.type in ("all", "other_features"):
                count = dump_feature_sequences(
                    session, strain_abbrev, seq_source,
                    output_dir / "other_features_genomic.fasta",
                    coding_only=False
                )
                write_file("other_features_genomic.fasta", count)

            logger.info("Done")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    finally:
        logger.removeHandler(file_handler)
        file_handler.close()


if __name__ == "__main__":
    sys.exit(main())
