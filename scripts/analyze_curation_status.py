#!/usr/bin/env python3
"""
Analyze curation status distribution in the database.

This script provides insights into the "Not Yet Curated" vs "High Priority"
paper buckets to help plan a migration strategy.

Usage:
    python scripts/analyze_curation_status.py

Environment Variables:
    DATABASE_URL: Database connection URL
"""

import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import func, text, distinct, and_

# Project root directory (cgd-backend/)
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Load environment variables BEFORE importing cgd modules
load_dotenv(PROJECT_ROOT / ".env")

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal
from cgd.models.models import Reference, RefProperty


def analyze_curation_status():
    """Analyze the distribution and characteristics of curation statuses."""

    with SessionLocal() as session:
        print("=" * 70)
        print("CURATION STATUS ANALYSIS")
        print("=" * 70)
        print()

        # 1. Overall counts by curation status
        print("1. OVERALL COUNTS BY CURATION STATUS (REF_PROPERTY table)")
        print("-" * 50)

        status_counts = (
            session.query(
                RefProperty.property_value,
                func.count(distinct(RefProperty.reference_no)).label("ref_count"),
            )
            .filter(RefProperty.property_type == "curation_status")
            .group_by(RefProperty.property_value)
            .order_by(func.count(distinct(RefProperty.reference_no)).desc())
            .all()
        )

        total_with_status = 0
        for status, count in status_counts:
            print(f"  {status}: {count:,} papers")
            total_with_status += count
        print(f"  TOTAL with curation_status: {total_with_status:,}")
        print()

        # 2. Papers by publication year for "Not yet curated"
        print("2. 'NOT YET CURATED' PAPERS BY PUBLICATION YEAR")
        print("-" * 50)

        nyc_by_year = (
            session.query(
                Reference.year,
                func.count(Reference.reference_no).label("count"),
            )
            .join(RefProperty, Reference.reference_no == RefProperty.reference_no)
            .filter(RefProperty.property_type == "curation_status")
            .filter(RefProperty.property_value == "Not yet curated")
            .group_by(Reference.year)
            .order_by(Reference.year.desc())
            .all()
        )

        old_papers = 0  # Pre-2020
        recent_papers = 0  # 2020+
        for year, count in nyc_by_year:
            year_val = year if year else 0
            marker = ""
            if year_val and year_val >= 2020:
                recent_papers += count
                marker = " <-- recent"
            elif year_val:
                old_papers += count
            print(f"  {year}: {count:,}{marker}")

        print()
        print(f"  Summary: {old_papers:,} old papers (pre-2020), {recent_papers:,} recent (2020+)")
        print()

        # 3. Papers by publication year for "High Priority"
        print("3. 'HIGH PRIORITY' PAPERS BY PUBLICATION YEAR")
        print("-" * 50)

        hp_by_year = (
            session.query(
                Reference.year,
                func.count(Reference.reference_no).label("count"),
            )
            .join(RefProperty, Reference.reference_no == RefProperty.reference_no)
            .filter(RefProperty.property_type == "curation_status")
            .filter(RefProperty.property_value == "High Priority")
            .group_by(Reference.year)
            .order_by(Reference.year.desc())
            .all()
        )

        for year, count in hp_by_year:
            print(f"  {year}: {count:,}")
        print()

        # 4. Date when papers were added (date_created in REF_PROPERTY)
        print("4. 'NOT YET CURATED' - WHEN WERE THEY ADDED TO THE SYSTEM?")
        print("-" * 50)

        nyc_by_add_year = (
            session.query(
                func.extract("year", RefProperty.date_created).label("add_year"),
                func.count(RefProperty.ref_property_no).label("count"),
            )
            .filter(RefProperty.property_type == "curation_status")
            .filter(RefProperty.property_value == "Not yet curated")
            .group_by(func.extract("year", RefProperty.date_created))
            .order_by(func.extract("year", RefProperty.date_created).desc())
            .all()
        )

        for add_year, count in nyc_by_add_year:
            print(f"  Added in {int(add_year) if add_year else 'Unknown'}: {count:,}")
        print()

        # 5. Papers linked to genes vs not
        print("5. 'NOT YET CURATED' - LINKED TO GENES?")
        print("-" * 50)

        # Count NYC papers that have ANY RefpropFeat links
        nyc_with_genes = session.execute(text("""
            SELECT COUNT(DISTINCT rp.reference_no)
            FROM MULTI.ref_property rp
            WHERE rp.property_type = 'curation_status'
            AND rp.property_value = 'Not yet curated'
            AND rp.ref_property_no IN (
                SELECT ref_property_no FROM MULTI.refprop_feat
            )
        """)).scalar()

        nyc_without_genes = session.execute(text("""
            SELECT COUNT(DISTINCT rp.reference_no)
            FROM MULTI.ref_property rp
            WHERE rp.property_type = 'curation_status'
            AND rp.property_value = 'Not yet curated'
            AND rp.ref_property_no NOT IN (
                SELECT ref_property_no FROM MULTI.refprop_feat
            )
        """)).scalar()

        print(f"  With gene links: {nyc_with_genes:,}")
        print(f"  Without gene links: {nyc_without_genes:,}")
        print()

        # 6. Papers in ref_bad (discarded)
        print("6. DISCARDED PAPERS (REF_BAD table)")
        print("-" * 50)

        ref_bad_count = session.execute(text("""
            SELECT COUNT(*) FROM MULTI.ref_bad
        """)).scalar()

        print(f"  Total discarded: {ref_bad_count:,}")
        print()

        # 7. Sample of oldest "Not yet curated" papers
        print("7. SAMPLE OF OLDEST 'NOT YET CURATED' PAPERS")
        print("-" * 50)

        oldest_nyc = (
            session.query(
                Reference.reference_no,
                Reference.pubmed,
                Reference.year,
                Reference.citation,
                RefProperty.date_created,
            )
            .join(RefProperty, Reference.reference_no == RefProperty.reference_no)
            .filter(RefProperty.property_type == "curation_status")
            .filter(RefProperty.property_value == "Not yet curated")
            .filter(Reference.year.isnot(None))
            .order_by(Reference.year)
            .limit(10)
            .all()
        )

        for ref_no, pubmed, year, citation, date_added in oldest_nyc:
            short_citation = (citation[:60] + "...") if citation and len(citation) > 60 else citation
            print(f"  Year {year}: PMID {pubmed}")
            print(f"    Added: {date_added}")
            print(f"    {short_citation}")
            print()

        # 8. Check for papers with literature_topic annotations
        print("8. 'NOT YET CURATED' PAPERS WITH LITERATURE_TOPIC ANNOTATIONS")
        print("-" * 50)
        print("   (These are 'Partially Curated' - have topic but still marked NYC)")

        partially_curated = session.execute(text("""
            SELECT COUNT(DISTINCT rp1.reference_no)
            FROM MULTI.ref_property rp1
            WHERE rp1.property_type = 'curation_status'
            AND rp1.property_value = 'Not yet curated'
            AND rp1.reference_no IN (
                SELECT reference_no FROM MULTI.ref_property
                WHERE property_type = 'literature_topic'
            )
        """)).scalar()

        print(f"  Partially curated: {partially_curated:,}")
        print()

        # 9. Total references in database
        print("9. TOTAL REFERENCES IN DATABASE")
        print("-" * 50)

        total_refs = session.query(func.count(Reference.reference_no)).scalar()
        refs_with_pubmed = session.query(func.count(Reference.reference_no)).filter(
            Reference.pubmed.isnot(None)
        ).scalar()

        print(f"  Total references: {total_refs:,}")
        print(f"  With PubMed ID: {refs_with_pubmed:,}")
        print()

        print("=" * 70)
        print("ANALYSIS COMPLETE")
        print("=" * 70)


if __name__ == "__main__":
    analyze_curation_status()
