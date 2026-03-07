#!/usr/bin/env python3
"""
Update Protein Property Information in Database.

This script calculates and updates protein properties including:
- Calculated protein properties (MW, pI, length, N/C-term sequences, GRAVY, aromaticity)
- Codon usage statistics (CAI, CBI, FOP) via codonW
- Amino acid composition
- Protein detail information (atomic composition, instability index, etc.)

Original Perl: ProteinPropUpdate.pl (by Jon Binkley, December 2009)
Converted to Python: 2024

Usage:
    python protein_prop_update.py --strain-abbrev SC5314 --created-by DBUSER
"""

import argparse
import logging
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from Bio import SeqIO
from Bio.SeqUtils.ProtParam import ProteinAnalysis
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal

load_dotenv()

logger = logging.getLogger(__name__)

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
CODONW_PATH = os.getenv("CODONW_PATH", "codonw")

# Amino acid mappings
AMINO_ACIDS = ['A', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'K', 'L',
               'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'V', 'W', 'Y']

THREE_LETTER = {
    'A': 'ALA', 'C': 'CYS', 'D': 'ASP', 'E': 'GLU', 'F': 'PHE',
    'G': 'GLY', 'H': 'HIS', 'I': 'ILE', 'K': 'LYS', 'L': 'LEU',
    'M': 'MET', 'N': 'ASN', 'P': 'PRO', 'Q': 'GLN', 'R': 'ARG',
    'S': 'SER', 'T': 'THR', 'V': 'VAL', 'W': 'TRP', 'Y': 'TYR',
}

# Protein detail groups and types
DETAIL_GROUPS = {
    'ATOMIC COMPOSITION': ['CARBON', 'HYDROGEN', 'NITROGEN', 'OXYGEN', 'SULPHUR'],
    'INSTABILITY INDEX': ['INSTABILITY INDEX (II)'],
    'EXTINCTION COEFFICIENTS AT 280 NM': [
        'ASSUMING ALL CYS RESIDUES APPEAR AS HALF CYSTINES',
        'ASSUMING NO CYS RESIDUES APPEAR AS HALF CYSTINES',
    ],
    'ALIPHATIC INDEX': ['ALIPHATIC INDEX'],
}

# Default fragment length for N-term/C-term sequences
FRAG_LENGTH = 7


def setup_logging(verbose: bool = False, log_file: Path = None) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler()]

    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


@dataclass
class ProteinProperties:
    """Container for calculated protein properties."""
    molecular_weight: float = 0.0
    pi: float = 0.0
    protein_length: int = 0
    n_term_seq: str = ""
    c_term_seq: str = ""
    gravy_score: float = 0.0
    aromaticity_score: float = 0.0
    cai: str = "ND"
    codon_bias: str = "ND"
    fop_score: str = "ND"
    aa_counts: dict = field(default_factory=dict)
    # Detail properties
    carbon: int = 0
    hydrogen: int = 0
    nitrogen: int = 0
    oxygen: int = 0
    sulphur: int = 0
    instability_index: float = 0.0
    extinction_all_cys: float = 0.0
    extinction_no_cys: float = 0.0
    aliphatic_index: float = 0.0


class ProteinPropertyUpdater:
    """Update protein properties in the database."""

    def __init__(
        self,
        session: Session,
        strain_abbrev: str,
        created_by: str,
        coding_seq_file: Path = None,
        protein_seq_file: Path = None,
        data_dir: Path = None,
        codonw_path: str = CODONW_PATH,
        nuclear_translation_table: int = 1,
        mito_translation_table: int = 3,
    ):
        self.session = session
        self.strain_abbrev = strain_abbrev
        self.created_by = created_by
        self.coding_seq_file = coding_seq_file
        self.protein_seq_file = protein_seq_file
        self.data_dir = data_dir or Path(tempfile.mkdtemp())
        self.codonw_path = codonw_path
        self.nuclear_trans = nuclear_translation_table
        self.mito_trans = mito_translation_table

        # Data structures
        self.org_no = None
        self.feat_no_for_orf: dict[str, int] = {}
        self.pi_no_for_feat: dict[int, int] = {}
        self.pd_nos_for_pi: dict[int, list] = {}
        self.is_orf_mito: dict[str, bool] = {}
        self.is_orf_verified: dict[str, bool] = {}
        self.mito_feat: set[int] = set()
        self.type_vals_for_orf: dict[str, ProteinProperties] = {}
        self.problem_with_orf: dict[str, str] = {}
        self.default_feat: set[str] = set()
        self.orf_for_allele: dict[str, str] = {}

        # Statistics
        self.stats = {
            'proteins_processed': 0,
            'coding_seqs_processed': 0,
            'protein_info_inserted': 0,
            'protein_info_updated': 0,
            'protein_detail_inserted': 0,
            'protein_detail_updated': 0,
            'errors': 0,
            'skipped': 0,
        }

    def get_organism_no(self) -> None:
        """Get organism number for the strain."""
        result = self.session.execute(
            text(f"""
                SELECT organism_no
                FROM {DB_SCHEMA}.organism
                WHERE organism_abbrev = :abbrev
            """),
            {"abbrev": self.strain_abbrev}
        ).fetchone()

        if result:
            self.org_no = result[0]
            logger.info(f"Organism number for {self.strain_abbrev}: {self.org_no}")
        else:
            raise ValueError(f"Organism not found: {self.strain_abbrev}")

    def identify_mito_orfs(self, mito_feature_names: list[str] = None) -> None:
        """Identify mitochondrial ORFs."""
        if not mito_feature_names:
            mito_feature_names = ['Mito']

        placeholders = ", ".join([f":feat{i}" for i in range(len(mito_feature_names))])
        params = {f"feat{i}": name for i, name in enumerate(mito_feature_names)}
        params["org_no"] = self.org_no

        result = self.session.execute(
            text(f"""
                SELECT fr.child_feature_no, f.feature_name
                FROM {DB_SCHEMA}.feat_relationship fr, {DB_SCHEMA}.feature f
                WHERE fr.child_feature_no = f.feature_no
                AND f.feature_type = 'ORF'
                AND fr.parent_feature_no IN (
                    SELECT f2.feature_no FROM {DB_SCHEMA}.feature f2
                    WHERE f2.feature_name IN ({placeholders})
                )
                AND f.organism_no = :org_no
            """),
            params
        )

        for feat_no, feat_name in result:
            self.mito_feat.add(feat_no)
            self.is_orf_mito[feat_name] = True

        logger.info(f"Identified {len(self.mito_feat)} mitochondrial ORFs")

    def get_all_orfs(self) -> None:
        """Get all ORFs for the organism."""
        result = self.session.execute(
            text(f"""
                SELECT f.feature_no, f.feature_name
                FROM {DB_SCHEMA}.feature f
                WHERE f.feature_type = 'ORF'
                AND f.organism_no = :org_no
                AND f.feature_no NOT IN (
                    SELECT fp.feature_no
                    FROM {DB_SCHEMA}.feat_property fp
                    WHERE fp.property_value = 'not physically mapped'
                    OR fp.property_value LIKE 'Deleted%'
                )
            """),
            {"org_no": self.org_no}
        )

        for feat_no, feat_name in result:
            self.feat_no_for_orf[feat_name] = feat_no

        logger.info(f"Found {len(self.feat_no_for_orf)} ORFs")

    def get_verified_orfs(self) -> None:
        """Get verified ORFs."""
        result = self.session.execute(
            text(f"""
                SELECT f.feature_no, f.feature_name
                FROM {DB_SCHEMA}.feature f, {DB_SCHEMA}.feat_property fp
                WHERE f.feature_type = 'ORF'
                AND f.gene_name IS NOT NULL
                AND f.organism_no = :org_no
                AND fp.feature_no = f.feature_no
                AND fp.property_value = 'Verified'
                AND fp.property_value NOT IN ('not physically mapped', 'Dubious')
                AND fp.property_value NOT LIKE 'Deleted%'
            """),
            {"org_no": self.org_no}
        )

        for feat_no, feat_name in result:
            if feat_no not in self.mito_feat:
                self.is_orf_verified[feat_name] = True

        logger.info(f"Found {len(self.is_orf_verified)} verified nuclear ORFs")

    def get_default_proteins(self) -> None:
        """Get default proteins for allele handling."""
        # Get all protein-coding features
        result = self.session.execute(
            text(f"""
                SELECT f.feature_name
                FROM {DB_SCHEMA}.feature f
                WHERE f.feature_type = 'ORF'
                AND f.organism_no = :org_no
            """),
            {"org_no": self.org_no}
        )

        for (feat_name,) in result:
            self.default_feat.add(feat_name)

    def get_existing_protein_info(self) -> None:
        """Get existing protein info from database."""
        # Get protein_info records
        result = self.session.execute(
            text(f"""
                SELECT pi.protein_info_no, pi.feature_no
                FROM {DB_SCHEMA}.protein_info pi
            """)
        )

        for pi_no, feat_no in result:
            self.pi_no_for_feat[feat_no] = pi_no

        # Get protein_detail records
        result = self.session.execute(
            text(f"""
                SELECT pd.protein_detail_no, pd.protein_info_no,
                       pd.protein_detail_group, pd.protein_detail_type
                FROM {DB_SCHEMA}.protein_detail pd
                WHERE pd.protein_detail_group IN (
                    'ATOMIC COMPOSITION', 'INSTABILITY INDEX',
                    'EXTINCTION COEFFICIENTS AT 280 NM', 'ALIPHATIC INDEX'
                )
            """)
        )

        for pd_no, pi_no, group, detail_type in result:
            if pi_no not in self.pd_nos_for_pi:
                self.pd_nos_for_pi[pi_no] = []
            self.pd_nos_for_pi[pi_no].append({
                'pd_no': pd_no,
                'group': group,
                'type': detail_type,
            })

        logger.info(f"Found {len(self.pi_no_for_feat)} existing protein_info records")

    def calculate_protein_properties(self, seq: str, orf_id: str) -> ProteinProperties:
        """Calculate protein properties from sequence."""
        props = ProteinProperties()

        # Remove stop codon if present
        seq = seq.rstrip('*')

        # Check for invalid amino acids
        if any(aa in seq.upper() for aa in 'BJOUXZ'):
            self.problem_with_orf[orf_id] = 'Ambiguous'
            logger.warning(f"Protein {orf_id}: Ambiguous amino acids")
            return None

        if '*' in seq:
            self.problem_with_orf[orf_id] = 'Internal Stop'
            logger.warning(f"Protein {orf_id}: Internal stop codon")
            return None

        try:
            analysis = ProteinAnalysis(seq)

            props.protein_length = len(seq)
            props.molecular_weight = round(analysis.molecular_weight(), 1)
            props.pi = round(analysis.isoelectric_point(), 2)
            props.gravy_score = round(analysis.gravy(), 2)
            props.aromaticity_score = round(analysis.aromaticity(), 2)

            # N-term and C-term sequences
            if len(seq) < FRAG_LENGTH:
                props.n_term_seq = seq
                props.c_term_seq = seq
            else:
                props.n_term_seq = seq[:FRAG_LENGTH]
                props.c_term_seq = seq[-FRAG_LENGTH:]

            # Amino acid counts
            aa_counts = analysis.count_amino_acids()
            for aa in AMINO_ACIDS:
                props.aa_counts[THREE_LETTER[aa]] = aa_counts.get(aa, 0)

            # Instability index
            props.instability_index = round(analysis.instability_index(), 2)

            # Extinction coefficients (at 280nm)
            # With all Cys as cystines (reduced=False)
            props.extinction_all_cys = round(
                analysis.molar_extinction_coefficient()[0], 2
            )
            # With no Cys as cystines (reduced=True)
            props.extinction_no_cys = round(
                analysis.molar_extinction_coefficient()[1], 2
            )

            # Aliphatic index
            # A = X(Ala) + a * X(Val) + b * X(Ile + Leu)
            # where a = 2.9 and b = 3.9
            ala_pct = aa_counts.get('A', 0) / len(seq) * 100 if seq else 0
            val_pct = aa_counts.get('V', 0) / len(seq) * 100 if seq else 0
            ile_leu_pct = (aa_counts.get('I', 0) + aa_counts.get('L', 0)) / len(seq) * 100 if seq else 0
            props.aliphatic_index = round(ala_pct + 2.9 * val_pct + 3.9 * ile_leu_pct, 2)

            # Atomic composition (approximate based on average)
            # These are estimates; exact values require full formula
            props.carbon = sum(aa_counts.values()) * 5  # Approximate
            props.hydrogen = sum(aa_counts.values()) * 8
            props.nitrogen = sum(aa_counts.values()) * 1
            props.oxygen = sum(aa_counts.values()) * 2
            props.sulphur = aa_counts.get('M', 0) + aa_counts.get('C', 0)

        except Exception as e:
            logger.error(f"Error calculating properties for {orf_id}: {e}")
            self.problem_with_orf[orf_id] = str(e)
            return None

        return props

    def check_coding_sequence(self, seq: str, orf_id: str, is_mito: bool = False) -> bool:
        """Check coding sequence for validity."""
        seq = seq.upper()

        # Check for ambiguous nucleotides
        if not all(n in 'ACGT' for n in seq):
            self.problem_with_orf[orf_id] = 'Ambiguous'
            logger.warning(f"CDS {orf_id}: Ambiguous nucleotides")
            return False

        # Define stop codons
        stop_codons = {'TAA', 'TAG'}
        if not is_mito:
            stop_codons.add('TGA')

        # Check for internal stop codons
        seq_len = len(seq)
        seq_len = seq_len - (seq_len % 3)  # Trim to multiple of 3

        for i in range(0, seq_len - 3, 3):
            codon = seq[i:i+3]
            if codon in stop_codons:
                self.problem_with_orf[orf_id] = 'Internal Stop'
                logger.warning(f"CDS {orf_id}: Internal stop codon at position {i}")
                return False

        return True

    def run_codonw(
        self,
        input_file: Path,
        output_file: Path,
        options: str,
        translation_table: int = 1,
    ) -> bool:
        """Run codonW for codon usage analysis."""
        cmd = [self.codonw_path, str(input_file)]

        # Add options
        cmd.extend(options.split())

        # Add translation table (codonW uses 0-based index)
        if translation_table:
            cmd.extend(['-code', str(translation_table - 1)])

        logger.debug(f"Running codonW: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=str(input_file.parent),
            )

            if result.returncode != 0:
                logger.error(f"codonW error: {result.stderr}")
                return False

            return True

        except FileNotFoundError:
            logger.warning(f"codonW not found at {self.codonw_path}")
            return False
        except Exception as e:
            logger.error(f"Error running codonW: {e}")
            return False

    def parse_codonw_output(self, output_file: Path) -> dict:
        """Parse codonW output file."""
        results = {}

        if not output_file.exists():
            logger.warning(f"codonW output file not found: {output_file}")
            return results

        try:
            with open(output_file) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('title'):
                        continue

                    parts = line.split()
                    if len(parts) < 8:
                        continue

                    orf = parts[0]
                    # Columns: 0=ORF, 5=CAI, 6=CBI, 7=Fop
                    cai = 'ND'
                    cbi = 'ND'
                    fop = 'ND'

                    try:
                        if parts[5].replace('.', '').replace('-', '').isdigit():
                            cai = f"{float(parts[5]):.2f}"
                    except (ValueError, IndexError):
                        pass

                    try:
                        if parts[6].replace('.', '').replace('-', '').isdigit():
                            cbi = f"{float(parts[6]):.2f}"
                    except (ValueError, IndexError):
                        pass

                    try:
                        if parts[7].replace('.', '').replace('-', '').isdigit():
                            fop = f"{float(parts[7]):.2f}"
                    except (ValueError, IndexError):
                        pass

                    results[orf] = {'CAI': cai, 'CBI': cbi, 'FOP': fop}

        except Exception as e:
            logger.error(f"Error parsing codonW output: {e}")

        return results

    def process_protein_sequences(self) -> None:
        """Process protein sequences and calculate properties."""
        if not self.protein_seq_file or not self.protein_seq_file.exists():
            logger.warning("Protein sequence file not specified or not found")
            return

        logger.info(f"Processing protein sequences from {self.protein_seq_file}")

        for record in SeqIO.parse(self.protein_seq_file, "fasta"):
            feature_name = record.id

            # Skip non-default alleles
            if feature_name not in self.default_feat:
                continue

            # Handle allele mapping
            orf_id = self.orf_for_allele.get(feature_name, feature_name)

            # Calculate properties
            props = self.calculate_protein_properties(str(record.seq), orf_id)
            if props:
                self.type_vals_for_orf[orf_id] = props
                self.stats['proteins_processed'] += 1

    def process_coding_sequences(self) -> None:
        """Process coding sequences for codonW analysis."""
        if not self.coding_seq_file or not self.coding_seq_file.exists():
            logger.warning("Coding sequence file not specified or not found")
            return

        logger.info(f"Processing coding sequences from {self.coding_seq_file}")

        # Create directories for codonW
        nuc_dir = self.data_dir / 'codonw_nuc'
        mito_dir = self.data_dir / 'codonw_mito'
        nuc_dir.mkdir(parents=True, exist_ok=True)
        mito_dir.mkdir(parents=True, exist_ok=True)

        # Separate sequences into nuclear and mitochondrial
        nuc_seqs = []
        mito_seqs = []
        verified_nuc_seqs = []

        for record in SeqIO.parse(self.coding_seq_file, "fasta"):
            feature_name = record.id

            if feature_name not in self.default_feat:
                continue

            orf_id = self.orf_for_allele.get(feature_name, feature_name)
            is_mito = self.is_orf_mito.get(orf_id, False)

            if not self.check_coding_sequence(str(record.seq), orf_id, is_mito):
                continue

            # Update record ID to ORF name
            record.id = orf_id
            record.description = ""

            if is_mito:
                mito_seqs.append(record)
            else:
                nuc_seqs.append(record)
                if self.is_orf_verified.get(orf_id, False):
                    verified_nuc_seqs.append(record)

            self.stats['coding_seqs_processed'] += 1

        # Write sequences and run codonW for nuclear ORFs
        if verified_nuc_seqs:
            verified_file = nuc_dir / 'nuclear_verified.fasta'
            all_nuc_file = nuc_dir / 'nuclear_full.fasta'

            SeqIO.write(verified_nuc_seqs, verified_file, "fasta")
            SeqIO.write(nuc_seqs, all_nuc_file, "fasta")

            logger.info(f"Created {verified_file} with {len(verified_nuc_seqs)} sequences")

            # Run codonW for statistics
            stats_options = "-silent -nomenu -machine -coa_rscu"
            usage_options = "-all_indices -silent -nomenu -machine -c_type 2"

            self.run_codonw(verified_file, verified_file.with_suffix('.out'),
                          stats_options, self.nuclear_trans)
            self.run_codonw(all_nuc_file, all_nuc_file.with_suffix('.out'),
                          usage_options, self.nuclear_trans)

            # Parse results
            output_file = all_nuc_file.with_suffix('.out')
            codonw_results = self.parse_codonw_output(output_file)

            for orf_id, values in codonw_results.items():
                if orf_id in self.type_vals_for_orf:
                    self.type_vals_for_orf[orf_id].cai = values['CAI']
                    self.type_vals_for_orf[orf_id].codon_bias = values['CBI']
                    self.type_vals_for_orf[orf_id].fop_score = values['FOP']

        # Process mitochondrial ORFs similarly
        if mito_seqs:
            mito_file = mito_dir / 'mito_full.fasta'
            SeqIO.write(mito_seqs, mito_file, "fasta")

            logger.info(f"Created {mito_file} with {len(mito_seqs)} sequences")

            stats_options = "-silent -nomenu -machine -coa_rscu"
            usage_options = "-all_indices -silent -nomenu -machine -c_type 2"

            self.run_codonw(mito_file, mito_file.with_suffix('.out'),
                          stats_options, self.mito_trans)
            self.run_codonw(mito_file, mito_file.with_suffix('.out'),
                          usage_options, self.mito_trans)

            output_file = mito_file.with_suffix('.out')
            codonw_results = self.parse_codonw_output(output_file)

            for orf_id, values in codonw_results.items():
                if orf_id in self.type_vals_for_orf:
                    self.type_vals_for_orf[orf_id].cai = values['CAI']
                    self.type_vals_for_orf[orf_id].codon_bias = values['CBI']
                    self.type_vals_for_orf[orf_id].fop_score = values['FOP']

    def load_properties_to_db(self, dry_run: bool = False) -> None:
        """Load calculated properties into database."""
        logger.info("Loading properties into database")

        for orf_id, props in self.type_vals_for_orf.items():
            if orf_id in self.problem_with_orf:
                logger.debug(f"Skipping {orf_id}: {self.problem_with_orf[orf_id]}")
                self.stats['skipped'] += 1
                continue

            feat_no = self.feat_no_for_orf.get(orf_id)
            if not feat_no:
                logger.debug(f"Skipping {orf_id}: feature_no not found")
                self.stats['skipped'] += 1
                continue

            try:
                pi_no = self.pi_no_for_feat.get(feat_no)

                if pi_no:
                    # Update existing protein_info
                    self._update_protein_info(pi_no, props)
                    self.stats['protein_info_updated'] += 1
                else:
                    # Insert new protein_info
                    pi_no = self._insert_protein_info(feat_no, props)
                    self.stats['protein_info_inserted'] += 1

                # Handle protein_detail
                if pi_no in self.pd_nos_for_pi:
                    self._update_protein_detail(pi_no, props)
                    self.stats['protein_detail_updated'] += 1
                else:
                    self._insert_protein_detail(pi_no, props)
                    self.stats['protein_detail_inserted'] += 1

            except Exception as e:
                logger.error(f"Error loading properties for {orf_id}: {e}")
                self.stats['errors'] += 1

        if not dry_run:
            self.session.commit()
            logger.info("Changes committed to database")
        else:
            self.session.rollback()
            logger.info("Dry run - changes rolled back")

    def _update_protein_info(self, pi_no: int, props: ProteinProperties) -> None:
        """Update existing protein_info record."""
        aa_columns = ", ".join([f"{THREE_LETTER[aa]} = :{THREE_LETTER[aa].lower()}"
                               for aa in AMINO_ACIDS])

        sql = f"""
            UPDATE {DB_SCHEMA}.protein_info SET
                molecular_weight = :mw,
                pi = :pi,
                protein_length = :length,
                n_term_seq = :nterm,
                c_term_seq = :cterm,
                gravy_score = :gravy,
                aromaticity_score = :aroma,
                cai = :cai,
                codon_bias = :cbi,
                fop_score = :fop,
                {aa_columns}
            WHERE protein_info_no = :pi_no
        """

        params = {
            'mw': props.molecular_weight,
            'pi': props.pi,
            'length': props.protein_length,
            'nterm': props.n_term_seq,
            'cterm': props.c_term_seq,
            'gravy': props.gravy_score,
            'aroma': props.aromaticity_score,
            'cai': props.cai,
            'cbi': props.codon_bias,
            'fop': props.fop_score,
            'pi_no': pi_no,
        }

        for aa in AMINO_ACIDS:
            params[THREE_LETTER[aa].lower()] = props.aa_counts.get(THREE_LETTER[aa], 0)

        self.session.execute(text(sql), params)

    def _insert_protein_info(self, feat_no: int, props: ProteinProperties) -> int:
        """Insert new protein_info record."""
        aa_columns = ", ".join([THREE_LETTER[aa] for aa in AMINO_ACIDS])
        aa_values = ", ".join([f":{THREE_LETTER[aa].lower()}" for aa in AMINO_ACIDS])

        sql = f"""
            INSERT INTO {DB_SCHEMA}.protein_info (
                created_by, feature_no,
                molecular_weight, pi, protein_length, n_term_seq,
                c_term_seq, gravy_score, aromaticity_score,
                cai, codon_bias, fop_score,
                {aa_columns}
            ) VALUES (
                :created_by, :feat_no,
                :mw, :pi, :length, :nterm,
                :cterm, :gravy, :aroma,
                :cai, :cbi, :fop,
                {aa_values}
            )
        """

        params = {
            'created_by': self.created_by,
            'feat_no': feat_no,
            'mw': props.molecular_weight,
            'pi': props.pi,
            'length': props.protein_length,
            'nterm': props.n_term_seq,
            'cterm': props.c_term_seq,
            'gravy': props.gravy_score,
            'aroma': props.aromaticity_score,
            'cai': props.cai,
            'cbi': props.codon_bias,
            'fop': props.fop_score,
        }

        for aa in AMINO_ACIDS:
            params[THREE_LETTER[aa].lower()] = props.aa_counts.get(THREE_LETTER[aa], 0)

        self.session.execute(text(sql), params)

        # Get the inserted ID
        result = self.session.execute(
            text(f"""
                SELECT protein_info_no FROM {DB_SCHEMA}.protein_info
                WHERE feature_no = :feat_no
            """),
            {"feat_no": feat_no}
        ).fetchone()

        return result[0] if result else None

    def _update_protein_detail(self, pi_no: int, props: ProteinProperties) -> None:
        """Update existing protein_detail records."""
        detail_values = self._get_detail_values(props)

        for pd_info in self.pd_nos_for_pi.get(pi_no, []):
            group = pd_info['group']
            detail_type = pd_info['type']
            pd_no = pd_info['pd_no']

            value = detail_values.get(detail_type)
            if value is not None:
                self.session.execute(
                    text(f"""
                        UPDATE {DB_SCHEMA}.protein_detail SET
                            protein_detail_value = :value
                        WHERE protein_detail_no = :pd_no
                    """),
                    {"value": str(value), "pd_no": pd_no}
                )

    def _insert_protein_detail(self, pi_no: int, props: ProteinProperties) -> None:
        """Insert new protein_detail records."""
        detail_values = self._get_detail_values(props)

        for group, types in DETAIL_GROUPS.items():
            for detail_type in types:
                value = detail_values.get(detail_type)
                if value is not None:
                    self.session.execute(
                        text(f"""
                            INSERT INTO {DB_SCHEMA}.protein_detail (
                                created_by, protein_info_no,
                                protein_detail_group, protein_detail_type,
                                protein_detail_value
                            ) VALUES (
                                :created_by, :pi_no, :group, :type, :value
                            )
                        """),
                        {
                            "created_by": self.created_by,
                            "pi_no": pi_no,
                            "group": group,
                            "type": detail_type,
                            "value": str(value),
                        }
                    )

    def _get_detail_values(self, props: ProteinProperties) -> dict:
        """Get protein detail values as a dict."""
        return {
            'CARBON': props.carbon,
            'HYDROGEN': props.hydrogen,
            'NITROGEN': props.nitrogen,
            'OXYGEN': props.oxygen,
            'SULPHUR': props.sulphur,
            'INSTABILITY INDEX (II)': props.instability_index,
            'ASSUMING ALL CYS RESIDUES APPEAR AS HALF CYSTINES': props.extinction_all_cys,
            'ASSUMING NO CYS RESIDUES APPEAR AS HALF CYSTINES': props.extinction_no_cys,
            'ALIPHATIC INDEX': props.aliphatic_index,
        }

    def run(self, dry_run: bool = False) -> dict:
        """Run the full protein property update."""
        logger.info(f"Starting protein property update for {self.strain_abbrev}")
        logger.info(f"Start time: {datetime.now()}")

        # Initialize data
        self.get_organism_no()
        self.identify_mito_orfs()
        self.get_all_orfs()
        self.get_verified_orfs()
        self.get_default_proteins()
        self.get_existing_protein_info()

        # Process sequences
        self.process_protein_sequences()
        self.process_coding_sequences()

        # Load to database
        self.load_properties_to_db(dry_run)

        logger.info(f"Completed at {datetime.now()}")
        return self.stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update protein property information in database"
    )
    parser.add_argument(
        "--strain-abbrev",
        required=True,
        help="Strain abbreviation (e.g., SC5314)",
    )
    parser.add_argument(
        "--created-by",
        required=True,
        help="Username for audit trail",
    )
    parser.add_argument(
        "--coding-seq-file",
        type=Path,
        help="FASTA file with coding sequences",
    )
    parser.add_argument(
        "--protein-seq-file",
        type=Path,
        help="FASTA file with protein sequences",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        help="Working directory for intermediate files",
    )
    parser.add_argument(
        "--codonw-path",
        default=CODONW_PATH,
        help=f"Path to codonW executable (default: {CODONW_PATH})",
    )
    parser.add_argument(
        "--nuclear-trans-table",
        type=int,
        default=1,
        help="Nuclear translation table (default: 1)",
    )
    parser.add_argument(
        "--mito-trans-table",
        type=int,
        default=3,
        help="Mitochondrial translation table (default: 3)",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Log file path",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't commit changes to database",
    )

    args = parser.parse_args()

    # Setup logging
    log_file = args.log_file
    if not log_file:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_file = LOG_DIR / f"{args.strain_abbrev}_Properties_update.log"

    setup_logging(args.verbose, log_file)

    logger.info(f"Started at {datetime.now()}")
    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    try:
        with SessionLocal() as session:
            updater = ProteinPropertyUpdater(
                session=session,
                strain_abbrev=args.strain_abbrev,
                created_by=args.created_by,
                coding_seq_file=args.coding_seq_file,
                protein_seq_file=args.protein_seq_file,
                data_dir=args.data_dir,
                codonw_path=args.codonw_path,
                nuclear_translation_table=args.nuclear_trans_table,
                mito_translation_table=args.mito_trans_table,
            )

            stats = updater.run(args.dry_run)

            logger.info("=" * 50)
            logger.info("Summary:")
            logger.info(f"  Proteins processed: {stats['proteins_processed']}")
            logger.info(f"  Coding sequences processed: {stats['coding_seqs_processed']}")
            logger.info(f"  Protein info inserted: {stats['protein_info_inserted']}")
            logger.info(f"  Protein info updated: {stats['protein_info_updated']}")
            logger.info(f"  Protein detail inserted: {stats['protein_detail_inserted']}")
            logger.info(f"  Protein detail updated: {stats['protein_detail_updated']}")
            logger.info(f"  Skipped: {stats['skipped']}")
            logger.info(f"  Errors: {stats['errors']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.exception(f"Error: {e}")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
