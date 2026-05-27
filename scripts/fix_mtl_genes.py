#!/usr/bin/env python3
"""
Fix MTL locus gene naming issue.

Problem:
- C5_01745W_A doesn't exist as a separate feature (only as an alias of C5_01745W_B)
- C5_01745W_B has gene_name PAPALPHA with PAP1 as an alias
- User wants:
  - C5_01745W_A = MTLA (aliases: PAP1, a1)
  - C5_01745W_B = MTLALPHA (aliases: PAPALPHA, alpha1, alpha2)

This script will:
1. Create C5_01745W_A as a new feature
2. Update C5_01745W_B gene_name to MTLALPHA
3. Fix aliases for both features

Usage:
    python fix_mtl_genes.py --dry-run   # Show what would be changed
    python fix_mtl_genes.py --execute   # Actually make the changes
"""

import sys
import os
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cgd.db.engine import SessionLocal
from cgd.models.models import Feature, FeatAlias, Alias, FeatLocation
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

    # Get C5_01745W_B
    feat_b = db.query(Feature).filter(Feature.feature_name == 'C5_01745W_B').first()
    if feat_b:
        print(f"\nC5_01745W_B:")
        print(f"  feature_no: {feat_b.feature_no}")
        print(f"  gene_name: {feat_b.gene_name}")
        print(f"  dbxref_id: {feat_b.dbxref_id}")
        print(f"  headline: {feat_b.headline}")

        # Get aliases
        aliases = db.query(Alias).join(FeatAlias).filter(
            FeatAlias.feature_no == feat_b.feature_no
        ).all()
        print(f"  Aliases: {[a.alias_name for a in aliases]}")

        # Get location
        loc = db.query(FeatLocation).filter(
            FeatLocation.feature_no == feat_b.feature_no,
            FeatLocation.is_loc_current == 'Y'
        ).first()
        if loc:
            print(f"  Location: {loc.start_coord}-{loc.stop_coord} ({loc.strand})")
            print(f"  root_seq_no (chromosome): {loc.root_seq_no}")
    else:
        print("\nC5_01745W_B: NOT FOUND")

    # Check if C5_01745W_A exists as a feature
    feat_a = db.query(Feature).filter(Feature.feature_name == 'C5_01745W_A').first()
    print(f"\nC5_01745W_A as separate feature: {'EXISTS' if feat_a else 'DOES NOT EXIST'}")

    # Check if C5_01745W_A exists as an alias
    alias_a = db.query(Alias).filter(Alias.alias_name == 'C5_01745W_A').first()
    if alias_a:
        feat_alias = db.query(FeatAlias).filter(FeatAlias.alias_no == alias_a.alias_no).first()
        if feat_alias:
            linked_feat = db.query(Feature).filter(
                Feature.feature_no == feat_alias.feature_no
            ).first()
            print(f"  C5_01745W_A exists as alias of: {linked_feat.feature_name if linked_feat else 'unknown'}")

    print()


def show_proposed_changes():
    """Show what changes will be made."""
    print("=" * 60)
    print("PROPOSED CHANGES")
    print("=" * 60)
    print("""
1. UPDATE C5_01745W_B:
   - Change gene_name from 'PAPALPHA' to 'MTLALPHA'
   - Remove aliases: PAP1, C5_01745W_A
   - Add aliases: alpha1, alpha2 (keep PAPALPHA)

2. CREATE C5_01745W_A (new feature):
   - feature_name: C5_01745W_A
   - gene_name: MTLA
   - feature_type: ORF
   - Aliases: PAP1, a1
   - Location: Same coordinates as C5_01745W_B but on chromosome A

NOTE: The curator needs to verify:
- The correct chromosome A coordinates for C5_01745W_A
- Whether additional data (CDS subfeatures, etc.) needs to be created
""")


def execute_changes(db):
    """Execute the database changes."""
    print("=" * 60)
    print("EXECUTING CHANGES")
    print("=" * 60)

    # Get C5_01745W_B
    feat_b = db.query(Feature).filter(Feature.feature_name == 'C5_01745W_B').first()
    if not feat_b:
        print("ERROR: C5_01745W_B not found!")
        return False

    # Step 1: Update C5_01745W_B gene_name
    print(f"\n1. Updating C5_01745W_B gene_name from '{feat_b.gene_name}' to 'MTLALPHA'...")
    old_gene_name = feat_b.gene_name
    feat_b.gene_name = 'MTLALPHA'

    # Step 2: Remove PAP1 and C5_01745W_A aliases from C5_01745W_B
    print("\n2. Removing aliases PAP1 and C5_01745W_A from C5_01745W_B...")

    aliases_to_remove = ['PAP1', 'C5_01745W_A']
    for alias_name in aliases_to_remove:
        alias = db.query(Alias).filter(Alias.alias_name == alias_name).first()
        if alias:
            feat_alias = db.query(FeatAlias).filter(
                FeatAlias.feature_no == feat_b.feature_no,
                FeatAlias.alias_no == alias.alias_no
            ).first()
            if feat_alias:
                print(f"   Removing alias link: {alias_name}")
                db.delete(feat_alias)

    # Step 3: Add alpha1, alpha2 aliases to C5_01745W_B
    print("\n3. Adding aliases alpha1, alpha2 to C5_01745W_B...")

    aliases_to_add = ['alpha1', 'alpha2']
    for alias_name in aliases_to_add:
        # Check if alias exists
        alias = db.query(Alias).filter(Alias.alias_name == alias_name).first()
        if not alias:
            # Create new alias
            next_alias_no = get_next_id(db, Alias, 'alias_no')
            alias = Alias(
                alias_no=next_alias_no,
                alias_name=alias_name,
                alias_type='Uniform',
                date_created=datetime.now(),
                created_by='SHUAI'
            )
            db.add(alias)
            db.flush()
            print(f"   Created new alias: {alias_name} (alias_no: {alias.alias_no})")

        # Check if already linked
        existing_link = db.query(FeatAlias).filter(
            FeatAlias.feature_no == feat_b.feature_no,
            FeatAlias.alias_no == alias.alias_no
        ).first()

        if not existing_link:
            next_feat_alias_no = get_next_id(db, FeatAlias, 'feat_alias_no')
            feat_alias = FeatAlias(
                feat_alias_no=next_feat_alias_no,
                feature_no=feat_b.feature_no,
                alias_no=alias.alias_no
            )
            db.add(feat_alias)
            print(f"   Linked alias {alias_name} to C5_01745W_B")

    # Step 4: Create C5_01745W_A feature
    print("\n4. Creating C5_01745W_A feature...")

    # Check if it already exists
    existing_feat_a = db.query(Feature).filter(Feature.feature_name == 'C5_01745W_A').first()
    if existing_feat_a:
        print(f"   C5_01745W_A already exists (feature_no: {existing_feat_a.feature_no})")
        feat_a = existing_feat_a
    else:
        next_feature_no = get_next_id(db, Feature, 'feature_no')

        # Generate a new dbxref_id
        # Format: CAL followed by 10 digits
        max_dbxref = db.query(func.max(Feature.dbxref_id)).filter(
            Feature.dbxref_id.like('CAL%')
        ).scalar()
        if max_dbxref:
            next_dbxref_num = int(max_dbxref[3:]) + 1
            new_dbxref_id = f'CAL{next_dbxref_num:010d}'
        else:
            new_dbxref_id = 'CAL0000999999'  # Fallback

        feat_a = Feature(
            feature_no=next_feature_no,
            organism_no=feat_b.organism_no,
            feature_name='C5_01745W_A',
            dbxref_id=new_dbxref_id,
            feature_type='ORF',
            source='CGD',
            gene_name='MTLA',
            headline='Poly(A) polymerase of the MTLa mating-type-like locus',
            date_created=datetime.now(),
            created_by='SHUAI'
        )
        db.add(feat_a)
        db.flush()
        print(f"   Created C5_01745W_A (feature_no: {feat_a.feature_no}, dbxref_id: {new_dbxref_id})")

    # Step 5: Add aliases PAP1, a1 to C5_01745W_A
    print("\n5. Adding aliases PAP1, a1 to C5_01745W_A...")

    aliases_for_a = ['PAP1', 'a1']
    for alias_name in aliases_for_a:
        alias = db.query(Alias).filter(Alias.alias_name == alias_name).first()
        if not alias:
            next_alias_no = get_next_id(db, Alias, 'alias_no')
            alias = Alias(
                alias_no=next_alias_no,
                alias_name=alias_name,
                alias_type='Uniform',
                date_created=datetime.now(),
                created_by='SHUAI'
            )
            db.add(alias)
            db.flush()
            print(f"   Created new alias: {alias_name} (alias_no: {alias.alias_no})")

        existing_link = db.query(FeatAlias).filter(
            FeatAlias.feature_no == feat_a.feature_no,
            FeatAlias.alias_no == alias.alias_no
        ).first()

        if not existing_link:
            next_feat_alias_no = get_next_id(db, FeatAlias, 'feat_alias_no')
            feat_alias = FeatAlias(
                feat_alias_no=next_feat_alias_no,
                feature_no=feat_a.feature_no,
                alias_no=alias.alias_no
            )
            db.add(feat_alias)
            print(f"   Linked alias {alias_name} to C5_01745W_A")

    # Step 6: Create location for C5_01745W_A
    print("\n6. Creating location for C5_01745W_A...")

    # Get location of C5_01745W_B
    loc_b = db.query(FeatLocation).filter(
        FeatLocation.feature_no == feat_b.feature_no,
        FeatLocation.is_loc_current == 'Y'
    ).first()

    if loc_b:
        # Find chromosome A root_seq_no
        # B9J08_005374_136927 is chr B, B9J08_003089_136894 is chr A
        chr_a_root = db.query(Feature).filter(
            Feature.feature_name == 'B9J08_003089_136894'
        ).first()

        if chr_a_root:
            # Check if location already exists
            existing_loc = db.query(FeatLocation).filter(
                FeatLocation.feature_no == feat_a.feature_no,
                FeatLocation.is_loc_current == 'Y'
            ).first()

            if not existing_loc:
                next_loc_no = get_next_id(db, FeatLocation, 'feat_location_no')
                new_loc = FeatLocation(
                    feat_location_no=next_loc_no,
                    feature_no=feat_a.feature_no,
                    root_seq_no=chr_a_root.feature_no,
                    seq_no=loc_b.seq_no,  # Use same seq_no for now
                    start_coord=loc_b.start_coord,
                    stop_coord=loc_b.stop_coord,
                    strand=loc_b.strand,
                    coord_version=datetime.now(),
                    is_loc_current='Y',
                    date_created=datetime.now(),
                    created_by='SHUAI'
                )
                db.add(new_loc)
                print(f"   Created location on chr A: {loc_b.start_coord}-{loc_b.stop_coord}")
            else:
                print(f"   Location already exists")
        else:
            print("   WARNING: Could not find chromosome A root sequence!")
            print("   You may need to manually add the location for C5_01745W_A")

    return True


def verify_changes(db):
    """Verify the changes were made correctly."""
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    # Check C5_01745W_A
    feat_a = db.query(Feature).filter(Feature.feature_name == 'C5_01745W_A').first()
    if feat_a:
        aliases = db.query(Alias).join(FeatAlias).filter(
            FeatAlias.feature_no == feat_a.feature_no
        ).all()
        print(f"\nC5_01745W_A:")
        print(f"  gene_name: {feat_a.gene_name}")
        print(f"  aliases: {[a.alias_name for a in aliases]}")
    else:
        print("\nC5_01745W_A: NOT FOUND - creation failed!")

    # Check C5_01745W_B
    feat_b = db.query(Feature).filter(Feature.feature_name == 'C5_01745W_B').first()
    if feat_b:
        aliases = db.query(Alias).join(FeatAlias).filter(
            FeatAlias.feature_no == feat_b.feature_no
        ).all()
        print(f"\nC5_01745W_B:")
        print(f"  gene_name: {feat_b.gene_name}")
        print(f"  aliases: {[a.alias_name for a in aliases]}")


def main():
    parser = argparse.ArgumentParser(description='Fix MTL locus gene naming')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be changed without making changes')
    parser.add_argument('--execute', action='store_true',
                       help='Actually execute the changes')
    args = parser.parse_args()

    if not args.dry_run and not args.execute:
        print("Please specify --dry-run or --execute")
        print(__doc__)
        return

    db = SessionLocal()

    try:
        show_current_state(db)

        if args.dry_run:
            show_proposed_changes()
            print("\nNo changes made (dry run)")

        elif args.execute:
            show_proposed_changes()

            response = input("\nProceed with changes? (yes/no): ")
            if response.lower() != 'yes':
                print("Aborted.")
                return

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
