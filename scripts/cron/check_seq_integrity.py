#!/usr/bin/env python3
"""
Check sequence integrity in the database.

This script validates that sequences stored in the database match sequences
computed from chromosome coordinates. It performs various checks:
- Sequence length matches coordinate differences
- Stored sequences match computed sequences
- Subfeature adjacency rules are followed
- No gaps or overlaps between subfeatures

Based on checkSeqIntegrity.pl by CGD team.

Usage:
    python check_seq_integrity.py
    python check_seq_integrity.py --seq-source "C. albicans SC5314 Assembly 22"

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    HTML_ROOT_DIR: Root directory for reports
    LOG_DIR: Directory for log files
"""

import argparse
import logging
import os
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
PROJECT_URL = os.getenv("PROJECT_URL", "http://www.candidagenome.org")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


@dataclass
class IntegrityStats:
    """Statistics for sequence integrity check."""
    total_features: int = 0
    gene_features: int = 0
    deleted_features: int = 0
    no_computed_seq: int = 0
    no_seq: int = 0
    length_mismatch: int = 0
    seq_mismatch: int = 0
    no_protein_seq: int = 0
    protein_length_mismatch: int = 0
    protein_seq_mismatch: int = 0
    invalid_adjacent: int = 0
    wrong_strands: int = 0
    overlaps: int = 0
    gaps: int = 0
    invalid_first: int = 0
    invalid_last: int = 0
    coord_diff_error: int = 0
    errors: list = field(default_factory=list)


def reverse_complement(seq: str) -> str:
    """Return reverse complement of a DNA sequence."""
    complement = {"A": "T", "T": "A", "G": "C", "C": "G",
                  "a": "t", "t": "a", "g": "c", "c": "g",
                  "N": "N", "n": "n"}
    return "".join(complement.get(base, base) for base in reversed(seq))


def get_strains(session) -> list[dict]:
    """Get all strains from the database."""
    query = text(f"""
        SELECT o.organism_no, o.organism_abbrev
        FROM {DB_SCHEMA}.organism o
        WHERE o.organism_type = 'strain'
    """)
    return [{"organism_no": row[0], "organism_abbrev": row[1]}
            for row in session.execute(query).fetchall()]


def get_seq_sources_for_strain(session, organism_no: int) -> list[str]:
    """Get all seq sources for a strain."""
    query = text(f"""
        SELECT DISTINCT s.source
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feat_location fl ON s.seq_no = fl.root_seq_no
        JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
        WHERE f.organism_no = :organism_no
        AND s.is_seq_current = 'Y'
    """)
    return [row[0] for row in session.execute(query, {"organism_no": organism_no}).fetchall()]


def is_feature_deleted(session, feature_no: int) -> bool:
    """Check if a feature is marked as deleted."""
    query = text(f"""
        SELECT 1 FROM {DB_SCHEMA}.feat_property
        WHERE feature_no = :feature_no
        AND property_value LIKE 'Deleted%'
    """)
    result = session.execute(query, {"feature_no": feature_no}).fetchone()
    return result is not None


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


def get_features_with_sequences(
    session,
    organism_no: int,
    seq_source: str,
    seq_type: str = "genomic"
) -> list[dict]:
    """Get features with their stored sequences."""
    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.feature_type,
               fl.start_coord, fl.stop_coord, fl.strand,
               s.residues, s.seq_length,
               rf.feature_name as root_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN {DB_SCHEMA}.seq s ON (fl.seq_no = s.seq_no AND s.is_seq_current = 'Y')
        JOIN {DB_SCHEMA}.seq rs ON (fl.root_seq_no = rs.seq_no AND rs.is_seq_current = 'Y')
        JOIN {DB_SCHEMA}.feature rf ON rs.feature_no = rf.feature_no
        WHERE f.organism_no = :organism_no
        AND s.source = :seq_source
        AND s.seq_type = :seq_type
        AND f.feature_type NOT IN ('chromosome', 'contig')
    """)

    features = []
    for row in session.execute(
        query, {"organism_no": organism_no, "seq_source": seq_source, "seq_type": seq_type}
    ).fetchall():
        features.append({
            "feature_no": row[0],
            "feature_name": row[1],
            "feature_type": row[2],
            "start_coord": row[3],
            "stop_coord": row[4],
            "strand": row[5],
            "residues": row[6],
            "seq_length": row[7],
            "root_name": row[8],
        })

    return features


def get_subfeatures(session, feature_no: int, seq_source: str) -> list[dict]:
    """Get subfeatures for a parent feature."""
    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.feature_type,
               fl.start_coord, fl.stop_coord, fl.strand
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_relationship fr ON (f.feature_no = fr.child_feature_no
            AND fr.relationship_type = 'part of' AND fr.rank = 2)
        JOIN {DB_SCHEMA}.feat_location fl ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN {DB_SCHEMA}.seq s ON (fl.seq_no = s.seq_no AND s.is_seq_current = 'Y')
        WHERE fr.parent_feature_no = :feature_no
        AND s.source = :seq_source
        ORDER BY fl.start_coord
    """)

    subfeatures = []
    for row in session.execute(
        query, {"feature_no": feature_no, "seq_source": seq_source}
    ).fetchall():
        subfeatures.append({
            "feature_no": row[0],
            "feature_name": row[1],
            "feature_type": row[2],
            "start_coord": row[3],
            "stop_coord": row[4],
            "strand": row[5],
        })

    return subfeatures


def compute_sequence_from_coords(
    chr_sequence: str,
    start: int,
    stop: int,
    strand: str
) -> str:
    """Compute sequence from chromosome coordinates."""
    # Ensure start < stop for extraction
    min_coord = min(start, stop)
    max_coord = max(start, stop)

    # Extract sequence (1-based to 0-based)
    seq = chr_sequence[min_coord - 1:max_coord]

    # Reverse complement if on C strand
    if strand == "C" or strand == "-":
        seq = reverse_complement(seq)

    return seq


def check_single_feature(
    session,
    feature: dict,
    chr_sequences: dict[str, str],
    stats: IntegrityStats,
    seq_type: str = "genomic"
) -> bool:
    """
    Check a single feature's sequence integrity.

    Returns True if further checking (subfeatures) should proceed.
    """
    stats.total_features += 1

    feature_name = feature["feature_name"]
    feature_type = feature["feature_type"]

    # Check if deleted
    if is_feature_deleted(session, feature["feature_no"]):
        stats.deleted_features += 1
        return False

    # Check sequence exists
    if not feature["residues"]:
        if seq_type == "protein":
            stats.no_protein_seq += 1
        else:
            stats.no_seq += 1
        stats.errors.append({
            "feature_name": feature_name,
            "feature_type": feature_type,
            "message": f"No {seq_type} sequence stored",
            "tag": 5 if seq_type != "protein" else 8,
        })
        return False

    # Check coordinate difference matches sequence length
    start = feature["start_coord"]
    stop = feature["stop_coord"]
    stored_len = len(feature["residues"])

    if start > stop:
        coord_diff = start - stop + 1
    else:
        coord_diff = stop - start + 1

    if stored_len != coord_diff:
        stats.coord_diff_error += 1
        stats.errors.append({
            "feature_name": feature_name,
            "feature_type": feature_type,
            "message": f"Coord diff ({coord_diff}) != sequence length ({stored_len})",
            "tag": 17,
        })

    # Get chromosome sequence
    root_name = feature["root_name"]
    chr_seq = chr_sequences.get(root_name)

    if not chr_seq:
        stats.no_computed_seq += 1
        stats.errors.append({
            "feature_name": feature_name,
            "feature_type": feature_type,
            "message": f"Cannot get chromosome sequence for {root_name}",
            "tag": 4,
        })
        return False

    # Compute sequence from coordinates
    try:
        computed_seq = compute_sequence_from_coords(
            chr_seq, start, stop, feature["strand"]
        )
    except Exception as e:
        stats.no_computed_seq += 1
        stats.errors.append({
            "feature_name": feature_name,
            "feature_type": feature_type,
            "message": f"Error computing sequence: {e}",
            "tag": 4,
        })
        return False

    # Compare lengths
    if len(computed_seq) != stored_len:
        if seq_type == "protein":
            stats.protein_length_mismatch += 1
            tag = 9
        else:
            stats.length_mismatch += 1
            tag = 6
        stats.errors.append({
            "feature_name": feature_name,
            "feature_type": feature_type,
            "message": f"Length mismatch: stored={stored_len}, computed={len(computed_seq)}",
            "tag": tag,
        })
        return False

    # Compare sequences
    if computed_seq.upper() != feature["residues"].upper():
        if seq_type == "protein":
            stats.protein_seq_mismatch += 1
            tag = 10
        else:
            stats.seq_mismatch += 1
            tag = 7
        stats.errors.append({
            "feature_name": feature_name,
            "feature_type": feature_type,
            "message": "Sequences do not match",
            "tag": tag,
        })

    return True


def check_subfeature_adjacency(
    feature_name: str,
    feature_type: str,
    subfeatures: list[dict],
    stats: IntegrityStats
):
    """Check that subfeatures are adjacent and follow valid patterns."""
    if len(subfeatures) < 2:
        return

    # Valid adjacent feature types
    valid_after_cds = {"intron", "gap", "adjustment", "five_prime_UTR", "three_prime_UTR",
                       "three_prime_UTR_intron", "five_prime_UTR_intron"}
    valid_after_intron = {"CDS", "noncoding_exon"}

    for i in range(len(subfeatures) - 1):
        sf1 = subfeatures[i]
        sf2 = subfeatures[i + 1]

        fname1 = sf1["feature_name"]
        fname2 = sf2["feature_name"]
        ftype1 = sf1["feature_type"]
        ftype2 = sf2["feature_type"]

        # Check strands match
        if sf1["strand"] != sf2["strand"]:
            stats.wrong_strands += 1
            stats.errors.append({
                "feature_name": feature_name,
                "feature_type": feature_type,
                "message": f"Adjacent subfeatures on different strands: {fname1} and {fname2}",
                "tag": 12,
            })
            continue

        # Check for gaps or overlaps
        strand = sf1["strand"]
        if strand == "W" or strand == "+":
            stop1 = sf1["stop_coord"]
            start2 = sf2["start_coord"]
            if stop1 + 1 < start2:
                stats.gaps += 1
                stats.errors.append({
                    "feature_name": feature_name,
                    "feature_type": feature_type,
                    "message": f"Gap between subfeatures: {fname1} and {fname2}",
                    "tag": 14,
                })
            elif stop1 + 1 > start2:
                stats.overlaps += 1
                stats.errors.append({
                    "feature_name": feature_name,
                    "feature_type": feature_type,
                    "message": f"Overlap between subfeatures: {fname1} and {fname2}",
                    "tag": 13,
                })
        else:
            start1 = sf1["start_coord"]
            stop2 = sf2["stop_coord"]
            if start1 + 1 < stop2:
                stats.gaps += 1
                stats.errors.append({
                    "feature_name": feature_name,
                    "feature_type": feature_type,
                    "message": f"Gap between subfeatures: {fname1} and {fname2}",
                    "tag": 14,
                })
            elif start1 + 1 > stop2:
                stats.overlaps += 1
                stats.errors.append({
                    "feature_name": feature_name,
                    "feature_type": feature_type,
                    "message": f"Overlap between subfeatures: {fname1} and {fname2}",
                    "tag": 13,
                })

        # Check valid adjacent types
        if ftype1 in ("intron", "gap", "adjustment"):
            if ftype2 not in valid_after_intron:
                stats.invalid_adjacent += 1
                stats.errors.append({
                    "feature_name": feature_name,
                    "feature_type": feature_type,
                    "message": f"Invalid adjacent subfeatures: {fname1} ({ftype1}) and {fname2} ({ftype2})",
                    "tag": 11,
                })
        elif ftype1 in ("CDS", "noncoding_exon"):
            if ftype2 not in valid_after_cds:
                stats.invalid_adjacent += 1
                stats.errors.append({
                    "feature_name": feature_name,
                    "feature_type": feature_type,
                    "message": f"Invalid adjacent subfeatures: {fname1} ({ftype1}) and {fname2} ({ftype2})",
                    "tag": 11,
                })

    # Check first and last subfeatures
    if subfeatures:
        first = subfeatures[0]
        last = subfeatures[-1]

        valid_first = {"CDS", "noncoding_exon", "five_prime_UTR"}
        valid_last = {"CDS", "noncoding_exon", "three_prime_UTR"}

        if first["strand"] == "C":
            valid_first, valid_last = valid_last, valid_first

        if first["feature_type"] not in valid_first:
            stats.invalid_first += 1
            stats.errors.append({
                "feature_name": feature_name,
                "feature_type": feature_type,
                "message": f"Invalid first subfeature: {first['feature_name']} ({first['feature_type']})",
                "tag": 15,
            })

        if last["feature_type"] not in valid_last:
            stats.invalid_last += 1
            stats.errors.append({
                "feature_name": feature_name,
                "feature_type": feature_type,
                "message": f"Invalid last subfeature: {last['feature_name']} ({last['feature_type']})",
                "tag": 16,
            })


def generate_html_report(stats: IntegrityStats, output_file: Path):
    """Generate HTML report of sequence integrity check."""
    with open(output_file, "w") as f:
        f.write(f"""<!DOCTYPE html>
<html>
<head>
    <title>Sequence Integrity Check Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        tr:nth-child(even) {{ background-color: #f2f2f2; }}
        .summary {{ margin-bottom: 20px; }}
    </style>
</head>
<body>
    <h1>{PROJECT_ACRONYM} Sequence Integrity Check</h1>
    <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

    <div class="summary">
        <h2>Summary</h2>
        <table>
            <tr><th>Tag</th><th>Count</th><th>Description</th></tr>
            <tr><td>1</td><td>{stats.total_features}</td><td>Total features examined</td></tr>
            <tr><td>2</td><td>{stats.gene_features}</td><td>Gene-level features examined</td></tr>
            <tr><td>3</td><td>{stats.deleted_features}</td><td>Deleted features (skipped)</td></tr>
            <tr><td>4</td><td>{stats.no_computed_seq}</td><td>Could not compute sequence</td></tr>
            <tr><td>5</td><td>{stats.no_seq}</td><td>No sequence stored</td></tr>
            <tr><td>6</td><td>{stats.length_mismatch}</td><td>Nucleotide length mismatch</td></tr>
            <tr><td>7</td><td>{stats.seq_mismatch}</td><td>Nucleotide sequence mismatch</td></tr>
            <tr><td>8</td><td>{stats.no_protein_seq}</td><td>No protein sequence</td></tr>
            <tr><td>9</td><td>{stats.protein_length_mismatch}</td><td>Protein length mismatch</td></tr>
            <tr><td>10</td><td>{stats.protein_seq_mismatch}</td><td>Protein sequence mismatch</td></tr>
            <tr><td>11</td><td>{stats.invalid_adjacent}</td><td>Invalid adjacent subfeatures</td></tr>
            <tr><td>12</td><td>{stats.wrong_strands}</td><td>Adjacent on different strands</td></tr>
            <tr><td>13</td><td>{stats.overlaps}</td><td>Overlapping subfeatures</td></tr>
            <tr><td>14</td><td>{stats.gaps}</td><td>Gaps between subfeatures</td></tr>
            <tr><td>15</td><td>{stats.invalid_first}</td><td>Invalid first subfeature</td></tr>
            <tr><td>16</td><td>{stats.invalid_last}</td><td>Invalid last subfeature</td></tr>
            <tr><td>17</td><td>{stats.coord_diff_error}</td><td>Coordinate difference error</td></tr>
        </table>
        <p><strong>Total errors: {len(stats.errors)}</strong></p>
    </div>

    <h2>Details</h2>
    <table>
        <tr><th>Feature Name</th><th>Type</th><th>Issue</th><th>Tag</th></tr>
""")

        for i, error in enumerate(stats.errors):
            bg = "#f2f2f2" if i % 2 == 0 else "#ffffff"
            f.write(f"""        <tr style="background-color: {bg}">
            <td><a href="{PROJECT_URL}/cgi-bin/locus.pl?locus={error['feature_name']}">{error['feature_name']}</a></td>
            <td>{error['feature_type']}</td>
            <td>{error['message']}</td>
            <td>{error['tag']}</td>
        </tr>
""")

        f.write("""    </table>
</body>
</html>
""")


def check_sequence_integrity(
    session,
    seq_source: str | None = None,
) -> IntegrityStats:
    """Run sequence integrity check."""
    stats = IntegrityStats()

    strains = get_strains(session)
    logger.info(f"Found {len(strains)} strains")

    for strain in strains:
        organism_no = strain["organism_no"]
        strain_abbrev = strain["organism_abbrev"]

        # Get seq sources for this strain
        if seq_source:
            seq_sources = [seq_source]
        else:
            seq_sources = get_seq_sources_for_strain(session, organism_no)

        for src in seq_sources:
            logger.info(f"Checking {strain_abbrev} - {src}")

            # Get and cache chromosome sequences
            chr_query = text(f"""
                SELECT f.feature_name, s.residues
                FROM {DB_SCHEMA}.feature f
                JOIN {DB_SCHEMA}.seq s ON f.feature_no = s.feature_no
                WHERE f.feature_type IN ('chromosome', 'contig')
                AND s.source = :seq_source
                AND s.is_seq_current = 'Y'
            """)
            chr_sequences = {}
            for row in session.execute(chr_query, {"seq_source": src}).fetchall():
                chr_sequences[row[0]] = row[1]

            # Check genomic sequences
            features = get_features_with_sequences(session, organism_no, src, "genomic")
            logger.info(f"  Found {len(features)} genomic features")

            for feat in features:
                stats.gene_features += 1
                check_further = check_single_feature(session, feat, chr_sequences, stats, "genomic")

                if check_further:
                    # Check subfeatures
                    subfeatures = get_subfeatures(session, feat["feature_no"], src)
                    if subfeatures:
                        check_subfeature_adjacency(
                            feat["feature_name"],
                            feat["feature_type"],
                            subfeatures,
                            stats
                        )

            # Check protein sequences
            protein_features = get_features_with_sequences(session, organism_no, src, "protein")
            logger.info(f"  Found {len(protein_features)} protein features")

            for feat in protein_features:
                check_single_feature(session, feat, chr_sequences, stats, "protein")

    return stats


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Check sequence integrity in the database"
    )
    parser.add_argument(
        "--seq-source",
        default=None,
        help="Specific sequence source to check (default: all)",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=None,
        help="Output HTML report file",
    )

    args = parser.parse_args()

    # Determine output file
    if args.output:
        output_file = args.output
    else:
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
        reports_dir = HTML_ROOT_DIR / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        output_file = reports_dir / f"seqchk-{timestamp}.html"

    logger.info("Starting sequence integrity check")

    try:
        with SessionLocal() as session:
            stats = check_sequence_integrity(session, args.seq_source)

            logger.info(f"Total features checked: {stats.total_features}")
            logger.info(f"Total errors found: {len(stats.errors)}")

            # Generate HTML report
            generate_html_report(stats, output_file)
            logger.info(f"Report written to {output_file}")

        return 0 if len(stats.errors) == 0 else 1

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
