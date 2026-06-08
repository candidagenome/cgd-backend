#!/usr/bin/env python3
"""
Add BioGRID Experiment Types to CV Terms
=========================================
Adds the required BioGRID experiment types to the cv_term table (cv_no=7)
so the interaction loader can run successfully.

Usage:
    python add_cv_terms.py [--dry-run]
"""

import os
import sys
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from sqlalchemy import text
from cgd.db.engine import SessionLocal

# BioGRID experiment types used in C. albicans data
BIOGRID_EXPERIMENT_TYPES = [
    'Affinity Capture-MS',
    'Affinity Capture-Western',
    'Biochemical Activity',
    'Co-crystal Structure',
    'Co-localization',
    'Co-purification',
    'Dosage Growth Defect',
    'Dosage Rescue',
    'FRET',
    'Far Western',
    'Negative Genetic',
    'PCA',
    'Phenotypic Enhancement',
    'Phenotypic Suppression',
    'Positive Genetic',
    'Protein-peptide',
    'Reconstituted Complex',
    'Synthetic Growth Defect',
    'Synthetic Haploinsufficiency',
    'Synthetic Lethality',
    'Synthetic Rescue',
    'Two-hybrid',
]

# CV number for experiment_type
CV_NO_EXPERIMENT_TYPE = 7

# User ID for created_by (must exist in dbuser table)
CREATED_BY = 'SHUAI'


def main():
    parser = argparse.ArgumentParser(description='Add BioGRID experiment types to cv_term table')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be added without making changes')
    args = parser.parse_args()

    db = SessionLocal()

    try:
        # Get existing terms
        result = db.execute(text(f'SELECT term_name FROM MULTI.cv_term WHERE cv_no = {CV_NO_EXPERIMENT_TYPE}'))
        existing = {row[0] for row in result}
        print(f"Existing experiment_type terms: {len(existing)}")

        # Get max cv_term_no
        result = db.execute(text('SELECT MAX(cv_term_no) FROM MULTI.cv_term'))
        max_no = result.scalar() or 0
        print(f"Current max cv_term_no: {max_no}")

        # Add missing terms
        added = 0
        skipped = 0
        for term in BIOGRID_EXPERIMENT_TYPES:
            if term in existing:
                print(f"  [SKIP] {term} (already exists)")
                skipped += 1
            else:
                max_no += 1
                if args.dry_run:
                    print(f"  [DRY-RUN] Would add: {term} (cv_term_no={max_no})")
                else:
                    db.execute(text('''
                        INSERT INTO MULTI.cv_term (cv_term_no, cv_no, term_name, date_created, created_by)
                        VALUES (:cv_term_no, :cv_no, :term_name, SYSDATE, :created_by)
                    '''), {
                        'cv_term_no': max_no,
                        'cv_no': CV_NO_EXPERIMENT_TYPE,
                        'term_name': term,
                        'created_by': CREATED_BY
                    })
                    print(f"  [ADDED] {term} (cv_term_no={max_no})")
                added += 1

        if not args.dry_run and added > 0:
            db.commit()
            print(f"\nCommitted {added} new experiment types")
        else:
            print(f"\n{'Would add' if args.dry_run else 'Added'}: {added}, Skipped: {skipped}")

    finally:
        db.close()


if __name__ == '__main__':
    main()
