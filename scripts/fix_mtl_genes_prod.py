#!/usr/bin/env python3
"""
Fix MTL locus gene aliases on prod to match dev.

Changes:
1. Remove C5_01745W_A alias from C5_01745W_B
2. Add PAPALPHA alias to C5_01745W_B
3. Add alpha1 alias to C5_01755C_B (MTLALPHA1)
4. Add alpha2 alias to C5_01785W_B (MTLALPHA2)

Usage:
    python fix_mtl_genes_prod.py --dry-run   # Show what would be changed
    python fix_mtl_genes_prod.py --execute   # Actually make the changes
"""

import sys
import os
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cgd.db.engine import SessionLocal
from cgd.models.models import Feature, FeatAlias, Alias
from sqlalchemy import func


def get_next_id(db, model, id_column):
    """Get the next available ID for a table."""
    max_id = db.query(func.max(getattr(model, id_column))).scalar()
    return (max_id or 0) + 1


def show_current_state(db):
    """Show the current state of the MTL genes."""
    print("=" * 60)
    print("CURRENT STATE")
    print("=" * 60)

    genes_to_check = ["C5_01745W_B", "C5_01755C_B", "C5_01785W_B"]

    for gene_name in genes_to_check:
        feat = db.query(Feature).filter(Feature.feature_name == gene_name).first()
        if feat:
            aliases = db.query(Alias).join(FeatAlias).filter(
                FeatAlias.feature_no == feat.feature_no
            ).all()
            print(f"\n{gene_name}:")
            print(f"  gene_name: {feat.gene_name}")
            print(f"  aliases: {[a.alias_name for a in aliases]}")

    # Check C5_01745W_A alias
    alias_a = db.query(Alias).filter(Alias.alias_name == 'C5_01745W_A').first()
    if alias_a:
        feat_alias = db.query(FeatAlias).filter(FeatAlias.alias_no == alias_a.alias_no).first()
        if feat_alias:
            linked = db.query(Feature).filter(Feature.feature_no == feat_alias.feature_no).first()
            print(f"\nC5_01745W_A alias linked to: {linked.feature_name if linked else 'unknown'}")
        else:
            print("\nC5_01745W_A alias exists but not linked to any feature")
    else:
        print("\nC5_01745W_A alias does not exist")

    print()


def execute_changes(db):
    """Execute the database changes."""
    print("=" * 60)
    print("EXECUTING CHANGES")
    print("=" * 60)

    # Step 1: Remove C5_01745W_A alias from C5_01745W_B
    print("\n1. Removing C5_01745W_A alias from C5_01745W_B...")
    feat_b = db.query(Feature).filter(Feature.feature_name == 'C5_01745W_B').first()
    alias_a = db.query(Alias).filter(Alias.alias_name == 'C5_01745W_A').first()

    if feat_b and alias_a:
        feat_alias = db.query(FeatAlias).filter(
            FeatAlias.feature_no == feat_b.feature_no,
            FeatAlias.alias_no == alias_a.alias_no
        ).first()
        if feat_alias:
            db.delete(feat_alias)
            print("   Removed C5_01745W_A alias link from C5_01745W_B")
        else:
            print("   C5_01745W_A alias not linked to C5_01745W_B (already removed?)")
    else:
        print("   Could not find feature or alias")

    # Step 2: Add PAPALPHA alias to C5_01745W_B (to match dev)
    print("\n2. Adding PAPALPHA alias to C5_01745W_B...")
    papalpha_alias = db.query(Alias).filter(Alias.alias_name == 'PAPALPHA').first()

    if not papalpha_alias:
        next_alias_no = get_next_id(db, Alias, 'alias_no')
        papalpha_alias = Alias(
            alias_no=next_alias_no,
            alias_name='PAPALPHA',
            alias_type='Uniform',
            date_created=datetime.now(),
            created_by='SHUAI'
        )
        db.add(papalpha_alias)
        db.flush()
        print(f"   Created new alias PAPALPHA (alias_no: {papalpha_alias.alias_no})")

    # Check if already linked to C5_01745W_B
    existing_link = db.query(FeatAlias).filter(
        FeatAlias.feature_no == feat_b.feature_no,
        FeatAlias.alias_no == papalpha_alias.alias_no
    ).first()

    if not existing_link:
        next_feat_alias_no = get_next_id(db, FeatAlias, 'feat_alias_no')
        new_link = FeatAlias(
            feat_alias_no=next_feat_alias_no,
            feature_no=feat_b.feature_no,
            alias_no=papalpha_alias.alias_no
        )
        db.add(new_link)
        print("   Linked PAPALPHA to C5_01745W_B")
    else:
        print("   PAPALPHA already linked to C5_01745W_B")

    # Step 3: Add alpha1 alias to C5_01755C_B (MTLALPHA1)
    print("\n3. Adding alpha1 alias to C5_01755C_B (MTLALPHA1)...")
    feat_mtlalpha1 = db.query(Feature).filter(Feature.feature_name == 'C5_01755C_B').first()
    alpha1_alias = db.query(Alias).filter(Alias.alias_name == 'alpha1').first()

    if not alpha1_alias:
        next_alias_no = get_next_id(db, Alias, 'alias_no')
        alpha1_alias = Alias(
            alias_no=next_alias_no,
            alias_name='alpha1',
            alias_type='Uniform',
            date_created=datetime.now(),
            created_by='SHUAI'
        )
        db.add(alpha1_alias)
        db.flush()
        print(f"   Created new alias alpha1 (alias_no: {alpha1_alias.alias_no})")

    if feat_mtlalpha1:
        existing_link = db.query(FeatAlias).filter(
            FeatAlias.feature_no == feat_mtlalpha1.feature_no,
            FeatAlias.alias_no == alpha1_alias.alias_no
        ).first()

        if not existing_link:
            next_feat_alias_no = get_next_id(db, FeatAlias, 'feat_alias_no')
            new_link = FeatAlias(
                feat_alias_no=next_feat_alias_no,
                feature_no=feat_mtlalpha1.feature_no,
                alias_no=alpha1_alias.alias_no
            )
            db.add(new_link)
            print("   Linked alpha1 to C5_01755C_B")
        else:
            print("   alpha1 already linked to C5_01755C_B")
    else:
        print("   ERROR: C5_01755C_B not found!")

    # Step 4: Add alpha2 alias to C5_01785W_B (MTLALPHA2)
    print("\n4. Adding alpha2 alias to C5_01785W_B (MTLALPHA2)...")
    feat_mtlalpha2 = db.query(Feature).filter(Feature.feature_name == 'C5_01785W_B').first()
    alpha2_alias = db.query(Alias).filter(Alias.alias_name == 'alpha2').first()

    if not alpha2_alias:
        next_alias_no = get_next_id(db, Alias, 'alias_no')
        alpha2_alias = Alias(
            alias_no=next_alias_no,
            alias_name='alpha2',
            alias_type='Uniform',
            date_created=datetime.now(),
            created_by='SHUAI'
        )
        db.add(alpha2_alias)
        db.flush()
        print(f"   Created new alias alpha2 (alias_no: {alpha2_alias.alias_no})")

    if feat_mtlalpha2:
        existing_link = db.query(FeatAlias).filter(
            FeatAlias.feature_no == feat_mtlalpha2.feature_no,
            FeatAlias.alias_no == alpha2_alias.alias_no
        ).first()

        if not existing_link:
            next_feat_alias_no = get_next_id(db, FeatAlias, 'feat_alias_no')
            new_link = FeatAlias(
                feat_alias_no=next_feat_alias_no,
                feature_no=feat_mtlalpha2.feature_no,
                alias_no=alpha2_alias.alias_no
            )
            db.add(new_link)
            print("   Linked alpha2 to C5_01785W_B")
        else:
            print("   alpha2 already linked to C5_01785W_B")
    else:
        print("   ERROR: C5_01785W_B not found!")

    return True


def verify_changes(db):
    """Verify the changes were made correctly."""
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)
    show_current_state(db)


def main():
    parser = argparse.ArgumentParser(description='Fix MTL locus aliases on prod')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show current state without making changes')
    parser.add_argument('--execute', action='store_true',
                       help='Execute the changes')
    args = parser.parse_args()

    if not args.dry_run and not args.execute:
        print("Please specify --dry-run or --execute")
        print(__doc__)
        return

    db = SessionLocal()

    try:
        show_current_state(db)

        if args.dry_run:
            print("=" * 60)
            print("PROPOSED CHANGES")
            print("=" * 60)
            print("""
1. Remove C5_01745W_A alias from C5_01745W_B
2. Add PAPALPHA alias to C5_01745W_B
3. Add alpha1 alias to C5_01755C_B (MTLALPHA1)
4. Add alpha2 alias to C5_01785W_B (MTLALPHA2)
""")
            print("No changes made (dry run)")

        elif args.execute:
            success = execute_changes(db)

            if success:
                verify_changes(db)

                response = input("\nCommit changes to database? (yes/no): ")
                if response.lower() == 'yes':
                    db.commit()
                    print("\nChanges committed successfully!")
                else:
                    db.rollback()
                    print("\nChanges rolled back.")
            else:
                db.rollback()
                print("\nChanges rolled back due to errors.")

    finally:
        db.close()


if __name__ == '__main__':
    main()
