#!/usr/bin/env python3
"""
Fix Oracle Sequences

Syncs Oracle sequences with the actual max values in their corresponding tables.
This fixes issues where sequences return values that already exist in the table.

Usage:
    python scripts/fix_oracle_sequences.py [--dry-run]

Options:
    --dry-run    Show what would be done without making changes
"""

import argparse
import os
import sys

from sqlalchemy import create_engine, text


# Sequences to fix: (sequence_name, table_name, column_name)
SEQUENCES_TO_FIX = [
    ('go_annotation_seq', 'go_annotation', 'go_annotation_no'),
    ('go_ref_seq', 'go_ref', 'go_ref_no'),
]


def get_database_url():
    """Get database URL from environment or use default."""
    # Check multiple possible environment variable names
    url = os.environ.get('DATABASE_URL') or os.environ.get('CGD_DATABASE_URL')
    if not url:
        # Try to load from .env file
        env_file = os.path.join(os.path.dirname(__file__), '..', '.env')
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('DATABASE_URL=') or line.startswith('CGD_DATABASE_URL='):
                        url = line.split('=', 1)[1].strip().strip('"\'')
                        break

    if not url:
        print("ERROR: DATABASE_URL not set. Set it in environment or .env file.")
        sys.exit(1)

    return url


def fix_sequence(conn, seq_name, table_name, column_name, dry_run=False):
    """Fix a single sequence by syncing it with the table's max value."""
    schema = 'MULTI'
    full_seq_name = f"{schema}.{seq_name}"
    full_table_name = f"{schema}.{table_name}"

    print(f"\n{'='*60}")
    print(f"Fixing sequence: {full_seq_name}")
    print(f"  Table: {full_table_name}")
    print(f"  Column: {column_name}")

    # Get max value from table
    result = conn.execute(text(f"SELECT MAX({column_name}) FROM {full_table_name}"))
    max_value = result.scalar() or 0
    print(f"  Max value in table: {max_value}")

    # Get current sequence value (CURRVAL requires NEXTVAL to be called first in session)
    # So we'll just get NEXTVAL and that tells us the current state
    try:
        result = conn.execute(text(f"SELECT {full_seq_name}.NEXTVAL FROM DUAL"))
        curr_seq_value = result.scalar()
        print(f"  Current sequence NEXTVAL: {curr_seq_value}")
    except Exception as e:
        print(f"  ERROR getting sequence value: {e}")
        print(f"  Sequence may not exist. Skipping.")
        return False

    # Calculate if we need to advance
    if curr_seq_value > max_value:
        print(f"  Sequence is already ahead of max value. No fix needed.")
        return True

    # Calculate gap - we need sequence to be at least max_value + 1
    gap = max_value - curr_seq_value + 1
    print(f"  Gap to fix: {gap}")

    if dry_run:
        print(f"  [DRY RUN] Would advance sequence by {gap}")
        return True

    # Fix the sequence by incrementing
    print(f"  Advancing sequence...")

    # Set increment to the gap
    conn.execute(text(f"ALTER SEQUENCE {full_seq_name} INCREMENT BY {gap}"))

    # Get next value (this advances by the gap)
    result = conn.execute(text(f"SELECT {full_seq_name}.NEXTVAL FROM DUAL"))
    new_value = result.scalar()
    print(f"  New sequence value: {new_value}")

    # Reset increment back to 1
    conn.execute(text(f"ALTER SEQUENCE {full_seq_name} INCREMENT BY 1"))

    # Verify
    result = conn.execute(text(f"SELECT {full_seq_name}.NEXTVAL FROM DUAL"))
    final_value = result.scalar()
    print(f"  Final sequence value (after increment by 1): {final_value}")

    if final_value > max_value:
        print(f"  SUCCESS: Sequence is now ahead of max table value")
        return True
    else:
        print(f"  WARNING: Sequence may still be behind. Manual intervention needed.")
        return False


def main():
    parser = argparse.ArgumentParser(description='Fix Oracle sequences')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without making changes')
    args = parser.parse_args()

    print("Oracle Sequence Fixer")
    print("=" * 60)

    if args.dry_run:
        print("DRY RUN MODE - No changes will be made")

    db_url = get_database_url()
    print(f"Connecting to database...")

    engine = create_engine(db_url)

    success_count = 0
    fail_count = 0

    with engine.connect() as conn:
        for seq_name, table_name, column_name in SEQUENCES_TO_FIX:
            try:
                if fix_sequence(conn, seq_name, table_name, column_name, args.dry_run):
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                print(f"  ERROR: {e}")
                fail_count += 1

        if not args.dry_run:
            conn.commit()
            print("\n" + "=" * 60)
            print("Changes committed.")

    print("\n" + "=" * 60)
    print(f"Summary: {success_count} succeeded, {fail_count} failed")

    return 0 if fail_count == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
