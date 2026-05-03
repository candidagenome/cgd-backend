#!/usr/bin/env python3
"""
Fetch CDS sequences for CRISPR test genes and generate fixture data.

This script:
1. Fetches the coding sequence for each test gene from the database
2. Outputs the first 500bp (for 5' region CRISPR targeting)
3. Generates a FASTA file for batch submission to CRISPOR
4. Creates a JSON fixture file for updating the test

Usage:
    # From cgd-backend directory:
    source .venv/bin/activate
    python scripts/fetch_crispr_test_fixtures.py

    # Or run on dev server with database access:
    ssh cgd-backend-dev
    cd work/cgd-backend
    source .venv/bin/activate
    python scripts/fetch_crispr_test_fixtures.py

After running:
1. Submit the generated FASTA file to CRISPOR (https://crispor.tefor.net/)
2. Download the results for each gene
3. Update the fixture file with expected guide sequences
4. Copy the fixture data to tests/api/test_crispr_service.py
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import func
from sqlalchemy.orm import Session

from cgd.db.engine import SessionLocal
from cgd.models.models import Feature, Seq, Organism

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Output directory
OUTPUT_DIR = Path(__file__).parent.parent / "tests" / "api" / "fixtures"

# Test genes - same list as in test_crispr_service.py
TEST_GENES = [
    # Virulence/Adhesion genes
    {"gene_name": "ALS1", "feature_name": "C1_13700C_A", "description": "Agglutinin-like sequence protein"},
    {"gene_name": "ALS3", "feature_name": "C6_01030W_A", "description": "Agglutinin-like protein"},
    {"gene_name": "HWP1", "feature_name": "C1_06250C_A", "description": "Hyphal wall protein 1"},
    {"gene_name": "ECE1", "feature_name": "C3_05610W_A", "description": "Candidalysin precursor"},
    # Secreted aspartyl proteases
    {"gene_name": "SAP1", "feature_name": "C6_02460W_A", "description": "Secreted aspartyl protease 1"},
    {"gene_name": "SAP2", "feature_name": "C6_02480W_A", "description": "Secreted aspartyl protease 2"},
    # Transcription factors
    {"gene_name": "EFG1", "feature_name": "CR_07890W_A", "description": "bHLH transcription factor"},
    {"gene_name": "CPH1", "feature_name": "C4_03540C_A", "description": "Transcription factor for mating/filamentation"},
    {"gene_name": "WOR1", "feature_name": "C1_11000C_A", "description": "Master regulator of white-opaque switching"},
    {"gene_name": "BCR1", "feature_name": "C3_04800W_A", "description": "Biofilm transcription factor"},
    # Signaling pathway genes
    {"gene_name": "HOG1", "feature_name": "C1_05270W_A", "description": "MAP kinase, stress response"},
    {"gene_name": "RAS1", "feature_name": "C2_05700W_A", "description": "Ras-family GTPase"},
    {"gene_name": "CDC42", "feature_name": "C5_02460C_A", "description": "Rho-type GTPase"},
    {"gene_name": "CEK1", "feature_name": "C2_00410C_A", "description": "MAP kinase"},
    # Housekeeping genes
    {"gene_name": "ACT1", "feature_name": "C1_06310W_A", "description": "Actin"},
    {"gene_name": "TUB1", "feature_name": "CR_02550C_A", "description": "Alpha-tubulin"},
    # Cell wall genes
    {"gene_name": "PHR1", "feature_name": "C5_01020C_A", "description": "pH-responsive glycosidase"},
    {"gene_name": "CHT2", "feature_name": "C1_04170C_A", "description": "Chitinase"},
    # Drug resistance
    {"gene_name": "CDR1", "feature_name": "C3_02280C_A", "description": "ABC transporter, azole resistance"},
    {"gene_name": "ERG11", "feature_name": "C5_00660C_A", "description": "Lanosterol 14-alpha-demethylase"},
]

# Organism for C. albicans SC5314
ORGANISM_ABBREV = "C_albicans_SC5314"


def get_gene_sequence(
    db: Session,
    gene_name: str,
    feature_name: str,
) -> Optional[str]:
    """
    Fetch the coding sequence for a gene.

    Args:
        db: Database session
        gene_name: Standard gene name (e.g., "HOG1")
        feature_name: Systematic ORF name (e.g., "C1_05270W_A")

    Returns:
        Coding sequence string or None if not found
    """
    # Try to find by gene name first
    query_upper = gene_name.strip().upper()

    feature = (
        db.query(Feature)
        .join(Organism, Feature.organism_no == Organism.organism_no)
        .filter(
            Organism.organism_abbrev == ORGANISM_ABBREV,
            func.upper(Feature.gene_name) == query_upper
        )
        .first()
    )

    # Fall back to feature name
    if not feature:
        feature = (
            db.query(Feature)
            .join(Organism, Feature.organism_no == Organism.organism_no)
            .filter(
                Organism.organism_abbrev == ORGANISM_ABBREV,
                func.upper(Feature.feature_name) == feature_name.upper()
            )
            .first()
        )

    if not feature:
        logger.warning(f"Feature not found: {gene_name} / {feature_name}")
        return None

    # Get coding sequence
    seq_record = (
        db.query(Seq)
        .filter(
            Seq.feature_no == feature.feature_no,
            Seq.seq_type == "coding",
            Seq.is_seq_current == "Y"
        )
        .first()
    )

    if not seq_record:
        # Fall back to genomic
        seq_record = (
            db.query(Seq)
            .filter(
                Seq.feature_no == feature.feature_no,
                Seq.seq_type == "genomic",
                Seq.is_seq_current == "Y"
            )
            .first()
        )

    if not seq_record:
        logger.warning(f"Sequence not found for: {gene_name}")
        return None

    return seq_record.residues.upper()


def main():
    """Fetch sequences and generate fixture files."""
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Connect to database
    logger.info("Connecting to database...")
    db = SessionLocal()

    try:
        results = []
        fasta_lines = []

        for gene in TEST_GENES:
            gene_name = gene["gene_name"]
            feature_name = gene["feature_name"]

            logger.info(f"Fetching sequence for {gene_name}...")

            sequence = get_gene_sequence(db, gene_name, feature_name)

            if sequence:
                # Get first 500bp for 5' region
                cds_first_500bp = sequence[:500]

                results.append({
                    "gene_name": gene_name,
                    "feature_name": feature_name,
                    "description": gene["description"],
                    "cds_length": len(sequence),
                    "cds_first_500bp": cds_first_500bp,
                    "expected_guides_5prime": [],  # To be filled from CRISPOR
                })

                # Add to FASTA
                fasta_lines.append(f">{gene_name}")
                # Split sequence into 60-char lines
                for i in range(0, len(cds_first_500bp), 60):
                    fasta_lines.append(cds_first_500bp[i:i+60])

                logger.info(f"  Found: {len(sequence)} bp total, using first 500bp")
            else:
                results.append({
                    "gene_name": gene_name,
                    "feature_name": feature_name,
                    "description": gene["description"],
                    "cds_length": 0,
                    "cds_first_500bp": "",
                    "expected_guides_5prime": [],
                    "error": "Sequence not found",
                })
                logger.warning(f"  NOT FOUND")

        # Write JSON fixture
        json_file = OUTPUT_DIR / "crispr_test_genes.json"
        with open(json_file, "w") as f:
            json.dump(results, f, indent=2)
        logger.info(f"Wrote JSON fixture: {json_file}")

        # Write FASTA file for CRISPOR submission
        fasta_file = OUTPUT_DIR / "crispr_test_sequences.fasta"
        with open(fasta_file, "w") as f:
            f.write("\n".join(fasta_lines))
        logger.info(f"Wrote FASTA file: {fasta_file}")

        # Print summary
        print("\n" + "="*60)
        print("CRISPR Test Fixture Generation Complete")
        print("="*60)
        print(f"\nGenerated files:")
        print(f"  1. {json_file}")
        print(f"  2. {fasta_file}")
        print(f"\nGenes processed: {len(results)}")
        print(f"Sequences found: {sum(1 for r in results if r['cds_first_500bp'])}")

        print("\n" + "-"*60)
        print("NEXT STEPS:")
        print("-"*60)
        print("""
1. Go to https://crispor.tefor.net/

2. For each gene in the FASTA file:
   a. Paste the sequence
   b. Select "NGG (SpCas9)" as PAM
   c. Select "Other" genome (paste sequence is used)
   d. Click "Submit"
   e. Download results or copy top 10 guide sequences

3. Update the JSON fixture file with expected guides:
   - Add guide sequences to "expected_guides_5prime" array
   - Each guide should be a 20bp sequence (without PAM)

4. Run the update script to copy fixtures to test file:
   python scripts/update_crispr_test_fixtures.py

Alternative - CRISPOR Batch Mode:
   - Submit the FASTA file directly to CRISPOR
   - Download all results at once
   - Parse and update fixtures automatically
""")

        # Also print a quick reference of sequences for manual CRISPOR submission
        print("\n" + "-"*60)
        print("QUICK REFERENCE - First 100bp of each gene:")
        print("-"*60)
        for r in results:
            if r["cds_first_500bp"]:
                print(f"\n{r['gene_name']}:")
                print(f"  {r['cds_first_500bp'][:100]}...")

    finally:
        db.close()


if __name__ == "__main__":
    main()
