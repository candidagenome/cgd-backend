#!/usr/bin/env python3
"""
Tests to verify C. tropicalis MYA-3404 data was loaded correctly.

Run with pytest:
    pytest scripts/C_tropicalis/test_data_loaded.py -v

Or run directly:
    python scripts/C_tropicalis/test_data_loaded.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
ORGANISM_NAME = "Candida tropicalis MYA-3404"


@pytest.fixture(scope="module")
def session():
    """Database session fixture."""
    with SessionLocal() as sess:
        yield sess


class TestOrganism:
    """Tests for organism and genome version."""

    def test_organism_exists(self, session):
        """Verify C. tropicalis organism entry exists."""
        query = text(f"""
            SELECT organism_no, organism_name, organism_abbrev, taxon_id
            FROM {DB_SCHEMA}.organism
            WHERE organism_name = :name
        """)
        result = session.execute(query, {"name": ORGANISM_NAME}).first()

        assert result is not None, f"Organism '{ORGANISM_NAME}' not found"
        assert result[1] == ORGANISM_NAME
        assert result[2] == "C_tropicalis"
        assert result[3] == 294747

    def test_genome_version_exists(self, session):
        """Verify genome version exists for C. tropicalis."""
        query = text(f"""
            SELECT gv.genome_version_no, gv.is_ver_current
            FROM {DB_SCHEMA}.genome_version gv
            JOIN {DB_SCHEMA}.organism o ON gv.organism_no = o.organism_no
            WHERE o.organism_name = :name
        """)
        result = session.execute(query, {"name": ORGANISM_NAME}).first()

        assert result is not None, "Genome version not found"
        assert result[1] == "Y", "Genome version should be current"


class TestFeatures:
    """Tests for gene features."""

    def test_feature_count(self, session):
        """Verify expected number of features loaded."""
        query = text(f"""
            SELECT COUNT(*)
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_name = :name
            AND f.feature_type = 'ORF'
        """)
        result = session.execute(query, {"name": ORGANISM_NAME}).first()

        assert result[0] >= 6000, f"Expected ~6678 ORF features, got {result[0]}"

    def test_sample_feature_exists(self, session):
        """Verify a sample feature exists with correct attributes."""
        query = text(f"""
            SELECT f.feature_name, f.feature_type, f.source
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_name = :name
            AND f.feature_type = 'ORF'
            AND ROWNUM = 1
        """)
        result = session.execute(query, {"name": ORGANISM_NAME}).first()

        assert result is not None, "No ORF features found"
        assert result[1] == "ORF"
        assert "tropicalis" in result[2].lower() or "ctrop" in result[2].lower()


class TestSequences:
    """Tests for protein and genomic sequences."""

    def test_protein_sequence_count(self, session):
        """Verify protein sequences were loaded."""
        query = text(f"""
            SELECT COUNT(*)
            FROM {DB_SCHEMA}.seq s
            JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_name = :name
            AND s.seq_type = 'Protein'
            AND s.is_seq_current = 'Y'
        """)
        result = session.execute(query, {"name": ORGANISM_NAME}).first()

        assert result[0] >= 6000, f"Expected ~6218 protein sequences, got {result[0]}"

    def test_genomic_sequence_count(self, session):
        """Verify genomic DNA sequences were loaded."""
        query = text(f"""
            SELECT COUNT(*)
            FROM {DB_SCHEMA}.seq s
            JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_name = :name
            AND s.seq_type = 'Genomic DNA'
            AND s.is_seq_current = 'Y'
            AND f.feature_type = 'ORF'
        """)
        result = session.execute(query, {"name": ORGANISM_NAME}).first()

        assert result[0] >= 6000, f"Expected ~6678 genomic sequences, got {result[0]}"

    def test_chromosome_sequences(self, session):
        """Verify chromosome/scaffold sequences were loaded."""
        query = text(f"""
            SELECT COUNT(*)
            FROM {DB_SCHEMA}.seq s
            JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_name = :name
            AND f.feature_type = 'contig'
            AND s.is_seq_current = 'Y'
        """)
        result = session.execute(query, {"name": ORGANISM_NAME}).first()

        assert result[0] == 7, f"Expected 7 chromosome sequences, got {result[0]}"


class TestCoordinates:
    """Tests for feature coordinates."""

    def test_feat_location_count(self, session):
        """Verify feature locations were loaded."""
        query = text(f"""
            SELECT COUNT(*)
            FROM {DB_SCHEMA}.feat_location fl
            JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_name = :name
            AND f.feature_type = 'ORF'
            AND fl.is_loc_current = 'Y'
        """)
        result = session.execute(query, {"name": ORGANISM_NAME}).first()

        assert result[0] >= 6000, f"Expected ~6678 feat_location entries, got {result[0]}"

    def test_location_has_coordinates(self, session):
        """Verify locations have valid coordinates."""
        query = text(f"""
            SELECT fl.start_coord, fl.stop_coord, fl.strand
            FROM {DB_SCHEMA}.feat_location fl
            JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_name = :name
            AND f.feature_type = 'ORF'
            AND fl.is_loc_current = 'Y'
            AND ROWNUM = 1
        """)
        result = session.execute(query, {"name": ORGANISM_NAME}).first()

        assert result is not None, "No feature locations found"
        assert result[0] > 0, "start_coord should be positive"
        assert result[1] > 0, "stop_coord should be positive"
        assert result[2] in ('W', 'C'), f"Invalid strand: {result[2]}"


class TestCDSAndIntrons:
    """Tests for CDS and intron features."""

    def test_cds_feature_count(self, session):
        """Verify CDS features were loaded."""
        query = text(f"""
            SELECT COUNT(*)
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_name = :name
            AND f.feature_type = 'CDS'
        """)
        result = session.execute(query, {"name": ORGANISM_NAME}).first()

        assert result[0] >= 6000, f"Expected ~6655 CDS features, got {result[0]}"

    def test_intron_feature_count(self, session):
        """Verify intron features were loaded."""
        query = text(f"""
            SELECT COUNT(*)
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_name = :name
            AND f.feature_type = 'intron'
        """)
        result = session.execute(query, {"name": ORGANISM_NAME}).first()

        assert result[0] >= 100, f"Expected ~163 intron features, got {result[0]}"

    def test_cds_has_relationship(self, session):
        """Verify CDS features are linked to parent genes."""
        query = text(f"""
            SELECT COUNT(*)
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            JOIN {DB_SCHEMA}.feat_relationship fr ON fr.child_feature_no = f.feature_no
            WHERE o.organism_name = :name
            AND f.feature_type = 'CDS'
            AND fr.relationship_type = 'part of'
        """)
        result = session.execute(query, {"name": ORGANISM_NAME}).first()

        assert result[0] >= 6000, f"Expected CDS with relationships, got {result[0]}"


class TestOrthologs:
    """Tests for ortholog relationships."""

    def test_ortholog_count(self, session):
        """Verify orthologs were loaded."""
        query = text(f"""
            SELECT COUNT(*)
            FROM {DB_SCHEMA}.feat_homology fh
            JOIN {DB_SCHEMA}.feature f ON fh.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_name = :name
        """)
        result = session.execute(query, {"name": ORGANISM_NAME}).first()

        assert result[0] >= 20000, f"Expected ~23167 ortholog entries, got {result[0]}"


class TestProteinDomains:
    """Tests for protein domain annotations."""

    def test_domain_count(self, session):
        """Verify protein domains were loaded."""
        query = text(f"""
            SELECT COUNT(*)
            FROM {DB_SCHEMA}.dbxref_feat df
            JOIN {DB_SCHEMA}.feature f ON df.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_name = :name
        """)
        result = session.execute(query, {"name": ORGANISM_NAME}).first()

        assert result[0] >= 20000, f"Expected ~23712 domain annotations, got {result[0]}"


class TestGOAnnotations:
    """Tests for GO annotations."""

    def test_go_annotation_count(self, session):
        """Verify GO annotations were loaded."""
        query = text(f"""
            SELECT COUNT(*)
            FROM {DB_SCHEMA}.go_annotation ga
            JOIN {DB_SCHEMA}.feature f ON ga.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_name = :name
        """)
        result = session.execute(query, {"name": ORGANISM_NAME}).first()

        assert result[0] >= 15000, f"Expected ~20308 GO annotations, got {result[0]}"

    def test_go_annotation_has_evidence(self, session):
        """Verify GO annotations have IEA evidence code."""
        query = text(f"""
            SELECT ga.go_evidence, COUNT(*)
            FROM {DB_SCHEMA}.go_annotation ga
            JOIN {DB_SCHEMA}.feature f ON ga.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_name = :name
            GROUP BY ga.go_evidence
        """)
        results = session.execute(query, {"name": ORGANISM_NAME}).fetchall()

        evidence_codes = {row[0] for row in results}
        assert "IEA" in evidence_codes, "Expected IEA evidence code"


class TestDescriptions:
    """Tests for gene descriptions."""

    def test_description_count(self, session):
        """Verify gene descriptions were loaded."""
        query = text(f"""
            SELECT COUNT(*)
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_name = :name
            AND f.feature_type = 'ORF'
            AND f.headline IS NOT NULL
        """)
        result = session.execute(query, {"name": ORGANISM_NAME}).first()

        assert result[0] >= 5000, f"Expected ~5246 descriptions, got {result[0]}"


def run_tests():
    """Run all tests and print summary."""
    import sys

    session = SessionLocal()

    tests = [
        ("Organism exists", TestOrganism().test_organism_exists),
        ("Genome version exists", TestOrganism().test_genome_version_exists),
        ("Feature count", TestFeatures().test_feature_count),
        ("Sample feature", TestFeatures().test_sample_feature_exists),
        ("Protein sequences", TestSequences().test_protein_sequence_count),
        ("Genomic sequences", TestSequences().test_genomic_sequence_count),
        ("Chromosome sequences", TestSequences().test_chromosome_sequences),
        ("Feature locations", TestCoordinates().test_feat_location_count),
        ("Location coordinates", TestCoordinates().test_location_has_coordinates),
        ("CDS features", TestCDSAndIntrons().test_cds_feature_count),
        ("Intron features", TestCDSAndIntrons().test_intron_feature_count),
        ("CDS relationships", TestCDSAndIntrons().test_cds_has_relationship),
        ("Orthologs", TestOrthologs().test_ortholog_count),
        ("Protein domains", TestProteinDomains().test_domain_count),
        ("GO annotations", TestGOAnnotations().test_go_annotation_count),
        ("GO evidence", TestGOAnnotations().test_go_annotation_has_evidence),
        ("Descriptions", TestDescriptions().test_description_count),
    ]

    passed = 0
    failed = 0

    print("=" * 60)
    print("C. tropicalis Data Verification Tests")
    print("=" * 60)

    for name, test_func in tests:
        try:
            test_func(session)
            print(f"  PASS: {name}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL: {name} - {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR: {name} - {e}")
            failed += 1

    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    session.close()
    return failed == 0


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
