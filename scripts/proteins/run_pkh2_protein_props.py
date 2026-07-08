#!/usr/bin/env python3
"""
Update protein property data for a single locus (default: PKH2).

This is the targeted counterpart to ``protein_prop_update.py`` (which processes
an entire strain). It pulls the current protein and coding sequences for one
locus directly from the database -- resolved by gene name (e.g. PKH2) or
systematic feature name -- writes them to single-record FASTA files, and reuses
the shared ``ProteinPropertyUpdater`` to recalculate the protein_info /
protein_detail records for just that locus.

Use it after a feature merge (e.g. PKH1 / C1_12400C merged into PKH2) to refresh
the surviving locus without re-running the whole-strain update.

Codon-usage note:
    CAI / codon bias (CBI) / FOP are relative to a strain-wide reference set and
    cannot be reproduced from a single coding sequence. By default this script
    PRESERVES the existing codon-usage values already in the database and only
    recomputes the sequence-derived properties (MW, pI, length, GRAVY,
    aromaticity, amino-acid composition, instability index, extinction
    coefficients, aliphatic index). Pass ``--coding-ref`` with the strain's
    verified-CDS FASTA to recompute codon usage against a proper reference.

Usage:
    python run_pkh2_protein_props.py --created-by DBUSER
    python run_pkh2_protein_props.py --gene PKH2 --strain-abbrev C_albicans_SC5314 \
        --created-by DBUSER --dry-run
    python run_pkh2_protein_props.py --gene PKH2 --created-by DBUSER \
        --coding-ref /path/to/SC5314_verified_orf_coding.fasta
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Resolve imports: the generic protein modules live in scripts/untested/proteins/;
# the `cgd` package lives at the repo root.
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "untested" / "proteins"))
sys.path.insert(0, str(REPO_ROOT))

from cgd.db.engine import SessionLocal  # noqa: E402
from protein_prop_update import (  # noqa: E402
    CODONW_PATH,
    DB_SCHEMA,
    LOG_DIR,
    ProteinPropertyUpdater,
    setup_logging,
)

load_dotenv()

logger = logging.getLogger(__name__)

# seq_type coded values vary in the DB; match case-insensitively.
PROTEIN_SEQ_TYPES = ("protein",)
CODING_SEQ_TYPES = ("coding", "cds")


def resolve_feature(session, gene: str, strain_abbrev: str):
    """Resolve a gene name or systematic feature name to (feature_no, feature_name, gene_name).

    Excludes old Assembly-19/21 features (orf19.* / orf21.*) so a gene name
    resolves to the current Assembly-22 feature -- the same rule the locus page
    uses. Raises if the name is still ambiguous.
    """
    rows = session.execute(
        text(f"""
            SELECT f.feature_no, f.feature_name, f.gene_name
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE (UPPER(f.gene_name) = UPPER(:gene)
                   OR UPPER(f.feature_name) = UPPER(:gene))
              AND o.organism_abbrev = :strain
              AND f.feature_type = 'ORF'
              AND UPPER(f.feature_name) NOT LIKE 'ORF19.%'
              AND UPPER(f.feature_name) NOT LIKE 'ORF21.%'
        """),
        {"gene": gene, "strain": strain_abbrev},
    ).fetchall()

    if not rows:
        raise ValueError(
            f"No current-assembly ORF feature found for '{gene}' in {strain_abbrev}"
        )
    if len(rows) > 1:
        cands = ", ".join(f"{r[1]} (feature_no={r[0]})" for r in rows)
        raise ValueError(
            f"'{gene}' is ambiguous in {strain_abbrev}: {cands}. "
            "Pass the systematic feature name via --gene."
        )
    return rows[0][0], rows[0][1], rows[0][2]


def fetch_current_seq(session, feature_no: int, seq_types) -> str | None:
    """Fetch the most recent current sequence of the given type(s) for a feature."""
    placeholders = ", ".join(f":t{i}" for i in range(len(seq_types)))
    params = {f"t{i}": t for i, t in enumerate(seq_types)}
    params["fno"] = feature_no

    row = session.execute(
        text(f"""
            SELECT residues
            FROM {DB_SCHEMA}.seq
            WHERE feature_no = :fno
              AND is_seq_current = 'Y'
              AND LOWER(seq_type) IN ({placeholders})
            ORDER BY seq_version DESC
        """),
        params,
    ).fetchone()
    return row[0] if row else None


def fetch_existing_codon_usage(session, feature_no: int):
    """Return existing (cai, codon_bias, fop_score) from protein_info, or (None, None, None)."""
    row = session.execute(
        text(f"""
            SELECT cai, codon_bias, fop_score
            FROM {DB_SCHEMA}.protein_info
            WHERE feature_no = :fno
        """),
        {"fno": feature_no},
    ).fetchone()
    if not row:
        return None, None, None
    return row[0], row[1], row[2]


def write_fasta(path: Path, header: str, residues: str) -> None:
    """Write a single-record FASTA file (60 residues per line)."""
    with open(path, "w") as fh:
        fh.write(f">{header}\n")
        for i in range(0, len(residues), 60):
            fh.write(residues[i:i + 60] + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Update protein property data for a single locus (default: PKH2)"
    )
    parser.add_argument(
        "--gene",
        default="PKH2",
        help="Gene name or systematic feature name (default: PKH2)",
    )
    parser.add_argument(
        "--strain-abbrev",
        default="C_albicans_SC5314",
        help="Organism abbreviation (default: C_albicans_SC5314)",
    )
    parser.add_argument(
        "--created-by",
        required=True,
        help="Username for audit trail",
    )
    parser.add_argument(
        "--coding-ref",
        type=Path,
        help=(
            "Strain verified-CDS FASTA used as the codonW reference to recompute "
            "CAI/CBI/FOP. If omitted, existing codon-usage values are preserved."
        ),
    )
    parser.add_argument(
        "--codonw-path",
        default=CODONW_PATH,
        help=f"Path to codonW executable (default: {CODONW_PATH})",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        help="Working directory for intermediate files",
    )
    parser.add_argument("--log-file", type=Path, help="Log file path")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't commit changes to the database",
    )
    args = parser.parse_args()

    log_file = args.log_file
    if not log_file:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_file = LOG_DIR / f"{args.gene}_Properties_update.log"
    setup_logging(args.verbose, log_file)

    logger.info(f"Started at {datetime.now()}")
    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    data_dir = args.data_dir or Path(tempfile.mkdtemp(prefix=f"{args.gene}_props_"))
    data_dir.mkdir(parents=True, exist_ok=True)

    with SessionLocal() as session:
        feature_no, feature_name, gene_name = resolve_feature(
            session, args.gene, args.strain_abbrev
        )
        logger.info(
            f"Resolved '{args.gene}' -> feature_no={feature_no}, "
            f"feature_name={feature_name}, gene_name={gene_name}"
        )

        protein_seq = fetch_current_seq(session, feature_no, PROTEIN_SEQ_TYPES)
        if not protein_seq:
            raise ValueError(f"No current protein sequence found for {feature_name}")

        # ProteinPropertyUpdater matches FASTA record ids against the systematic
        # feature_name, so that MUST be the header (not the gene name).
        protein_file = data_dir / f"{feature_name}_protein.fasta"
        write_fasta(protein_file, feature_name, protein_seq.rstrip("*"))

        updater = ProteinPropertyUpdater(
            session=session,
            strain_abbrev=args.strain_abbrev,
            created_by=args.created_by,
            coding_seq_file=args.coding_ref,
            protein_seq_file=protein_file,
            data_dir=data_dir,
            codonw_path=args.codonw_path,
        )

        # Drive the pipeline step-by-step so we can control codon-usage handling.
        updater.get_organism_no()
        updater.identify_mito_orfs()
        updater.get_all_orfs()
        updater.get_verified_orfs()
        updater.get_default_proteins()
        updater.get_existing_protein_info()
        updater.process_protein_sequences()

        if args.coding_ref:
            logger.info(f"Recomputing codon usage against reference {args.coding_ref}")
            updater.process_coding_sequences()
        else:
            # Preserve existing codon-usage values (CAI/CBI/FOP are relative to a
            # strain-wide reference and cannot be recomputed from one sequence).
            cai, cbi, fop = fetch_existing_codon_usage(session, feature_no)
            props = updater.type_vals_for_orf.get(feature_name)
            if props is not None:
                props.cai = cai
                props.codon_bias = cbi
                props.fop_score = fop
                logger.info(
                    f"Preserving existing codon usage: CAI={cai}, CBI={cbi}, FOP={fop}"
                )

        updater.load_properties_to_db(args.dry_run)
        stats = updater.stats

    logger.info("=" * 50)
    logger.info(f"Locus: {gene_name or feature_name} ({feature_name})")
    logger.info(f"  Proteins processed: {stats['proteins_processed']}")
    logger.info(f"  Protein info inserted: {stats['protein_info_inserted']}")
    logger.info(f"  Protein info updated: {stats['protein_info_updated']}")
    logger.info(f"  Protein detail inserted: {stats['protein_detail_inserted']}")
    logger.info(f"  Protein detail updated: {stats['protein_detail_updated']}")
    logger.info(f"  Errors: {stats['errors']}")
    logger.info("=" * 50)
    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
