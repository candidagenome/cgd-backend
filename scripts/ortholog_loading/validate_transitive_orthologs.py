#!/usr/bin/env python3
"""
Validate transitive orthologs and identify C. glabrata genes to add to CGOB groups.

This script analyzes the transitive orthologs file and determines:
1. Which C. glabrata genes already have CGOB orthologs
2. Which need to be added to existing CGOB groups
3. Which would need new groups (no existing CGOB group found)

Usage:
    python validate_transitive_orthologs.py [--output report.tsv]
"""

import argparse
import logging
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

from dotenv import dotenv_values
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default paths
TRANSITIVE_FILE = '/data/cgob/cglabrata_transitive_orthologs.tsv'
ENV_FILE = '/home/ec2-user/work/cgd-backend/.env'


@dataclass
class OrthologMapping:
    """Represents a transitive ortholog mapping."""
    cg_gene: str
    cg_feature_no: Optional[int]
    species: str
    ortholog_gene: str
    ortholog_feature_no: Optional[int]
    existing_hg_no: Optional[int]
    status: str  # 'already_in_group', 'add_to_group', 'no_group_found', 'feature_not_found'


def load_transitive_orthologs(filepath: str) -> dict:
    """Load transitive orthologs from TSV file."""
    orthologs = defaultdict(list)
    with open(filepath) as f:
        next(f)  # Skip header
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 3:
                cg_gene, species, ortholog = parts[:3]
                orthologs[cg_gene].append((species, ortholog))
    return orthologs


def validate_orthologs(engine, orthologs: dict) -> list[OrthologMapping]:
    """Validate orthologs against database and identify actions needed."""
    results = []

    with engine.connect() as conn:
        # Build lookup caches
        logger.info("Building feature lookup caches...")

        # Cache C. glabrata feature_no by feature_name
        cg_features = {}
        result = conn.execute(text('''
            SELECT feature_no, feature_name
            FROM MULTI.feature
            WHERE feature_name LIKE 'CAGL%'
        '''))
        for row in result:
            cg_features[row[1]] = row[0]
        logger.info(f"  Cached {len(cg_features)} C. glabrata features")

        # Cache all feature_no by feature_name (for direct lookup)
        all_features = {}
        result = conn.execute(text('''
            SELECT feature_no, feature_name FROM MULTI.feature
        '''))
        for row in result:
            all_features[row[1]] = row[0]
            all_features[row[1].upper()] = row[0]  # Case-insensitive
        logger.info(f"  Cached {len(all_features)} total features")

        # Cache orf19.XXXX aliases to feature_no
        orf19_to_feature = {}
        result = conn.execute(text('''
            SELECT fa.feature_no, a.alias_name
            FROM MULTI.feat_alias fa
            JOIN MULTI.alias a ON fa.alias_no = a.alias_no
            WHERE a.alias_name LIKE 'orf19.%'
        '''))
        for row in result:
            orf19_to_feature[row[1]] = row[0]
        logger.info(f"  Cached {len(orf19_to_feature)} orf19 aliases")

        # Cache existing C. glabrata CGOB memberships
        cg_in_cgob = defaultdict(set)  # cg_feature_no -> set of hg_no
        result = conn.execute(text('''
            SELECT fh.feature_no, fh.homology_group_no
            FROM MULTI.feat_homology fh
            JOIN MULTI.feature f ON fh.feature_no = f.feature_no
            JOIN MULTI.homology_group hg ON fh.homology_group_no = hg.homology_group_no
            WHERE f.feature_name LIKE 'CAGL%' AND hg.method = 'CGOB'
        '''))
        for row in result:
            cg_in_cgob[row[0]].add(row[1])
        logger.info(f"  Cached {len(cg_in_cgob)} C. glabrata CGOB memberships")

        # Cache feature_no -> CGOB homology groups for Candida species
        candida_cgob = defaultdict(set)  # feature_no -> set of hg_no
        result = conn.execute(text('''
            SELECT fh.feature_no, fh.homology_group_no
            FROM MULTI.feat_homology fh
            JOIN MULTI.homology_group hg ON fh.homology_group_no = hg.homology_group_no
            WHERE hg.method = 'CGOB'
        '''))
        for row in result:
            candida_cgob[row[0]].add(row[1])
        logger.info(f"  Cached {len(candida_cgob)} Candida CGOB memberships")

        # Process each ortholog mapping
        logger.info("Validating ortholog mappings...")
        processed = 0
        for cg_gene, orth_list in orthologs.items():
            # Look up C. glabrata feature
            cg_feature_no = cg_features.get(cg_gene)

            for species, ortholog in orth_list:
                mapping = OrthologMapping(
                    cg_gene=cg_gene,
                    cg_feature_no=cg_feature_no,
                    species=species,
                    ortholog_gene=ortholog,
                    ortholog_feature_no=None,
                    existing_hg_no=None,
                    status='unknown'
                )

                if not cg_feature_no:
                    mapping.status = 'cg_feature_not_found'
                    results.append(mapping)
                    continue

                # Look up ortholog feature
                orth_feature_no = None

                # Try direct lookup
                if ortholog in all_features:
                    orth_feature_no = all_features[ortholog]
                elif ortholog.upper() in all_features:
                    orth_feature_no = all_features[ortholog.upper()]
                # Try orf19 alias lookup for C. albicans
                elif ortholog.startswith('orf19.') and ortholog in orf19_to_feature:
                    orth_feature_no = orf19_to_feature[ortholog]

                if not orth_feature_no:
                    mapping.status = 'ortholog_feature_not_found'
                    results.append(mapping)
                    continue

                mapping.ortholog_feature_no = orth_feature_no

                # Find CGOB homology groups containing this ortholog
                orth_hg_nos = candida_cgob.get(orth_feature_no, set())

                if not orth_hg_nos:
                    mapping.status = 'no_cgob_group_for_ortholog'
                    results.append(mapping)
                    continue

                # Check if C. glabrata is already in any of these groups
                cg_hg_nos = cg_in_cgob.get(cg_feature_no, set())
                common_groups = orth_hg_nos & cg_hg_nos

                if common_groups:
                    mapping.existing_hg_no = list(common_groups)[0]
                    mapping.status = 'already_in_group'
                else:
                    # C. glabrata needs to be added to one of the ortholog's groups
                    mapping.existing_hg_no = list(orth_hg_nos)[0]
                    mapping.status = 'add_to_group'

                results.append(mapping)

            processed += 1
            if processed % 1000 == 0:
                logger.info(f"  Processed {processed}/{len(orthologs)} genes")

        logger.info(f"  Completed validation of {len(results)} mappings")

    return results


def generate_report(results: list[OrthologMapping], output_file: str):
    """Generate validation report."""
    # Count by status
    status_counts = defaultdict(int)
    for r in results:
        status_counts[r.status] += 1

    print("\n=== Validation Summary ===")
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")

    # Count unique C. glabrata genes by status
    genes_by_status = defaultdict(set)
    for r in results:
        genes_by_status[r.status].add(r.cg_gene)

    print("\n=== Unique C. glabrata Genes by Status ===")
    for status, genes in sorted(genes_by_status.items()):
        print(f"  {status}: {len(genes)} genes")

    # Genes that need to be added
    to_add = [r for r in results if r.status == 'add_to_group']
    unique_additions = set((r.cg_feature_no, r.existing_hg_no) for r in to_add)
    print(f"\n=== Actions Required ===")
    print(f"  FeatHomology records to create: {len(unique_additions)}")

    # Write detailed report
    with open(output_file, 'w') as f:
        f.write("cg_gene\tcg_feature_no\tspecies\tortholog_gene\tortholog_feature_no\texisting_hg_no\tstatus\n")
        for r in results:
            f.write(f"{r.cg_gene}\t{r.cg_feature_no}\t{r.species}\t{r.ortholog_gene}\t{r.ortholog_feature_no}\t{r.existing_hg_no}\t{r.status}\n")

    print(f"\nDetailed report written to: {output_file}")

    # Write additions file (for loading script)
    additions_file = output_file.replace('.tsv', '_additions.tsv')
    with open(additions_file, 'w') as f:
        f.write("cg_feature_no\thomology_group_no\tcg_gene\n")
        seen = set()
        for r in to_add:
            key = (r.cg_feature_no, r.existing_hg_no)
            if key not in seen:
                seen.add(key)
                f.write(f"{r.cg_feature_no}\t{r.existing_hg_no}\t{r.cg_gene}\n")

    print(f"Additions file written to: {additions_file}")

    return to_add


def main():
    parser = argparse.ArgumentParser(description='Validate transitive orthologs')
    parser.add_argument('--input', default=TRANSITIVE_FILE, help='Input transitive orthologs file')
    parser.add_argument('--output', default='/data/cgob/validation_report.tsv', help='Output report file')
    parser.add_argument('--env', default=ENV_FILE, help='Path to .env file')
    args = parser.parse_args()

    # Load environment
    config = dotenv_values(args.env)
    db_url = config.get('DATABASE_URL')
    if not db_url:
        logger.error(f"DATABASE_URL not found in {args.env}")
        sys.exit(1)

    engine = create_engine(db_url)

    # Load transitive orthologs
    logger.info(f"Loading transitive orthologs from {args.input}")
    orthologs = load_transitive_orthologs(args.input)
    logger.info(f"Loaded {len(orthologs)} C. glabrata genes")

    # Validate
    results = validate_orthologs(engine, orthologs)

    # Generate report
    generate_report(results, args.output)


if __name__ == '__main__':
    main()
