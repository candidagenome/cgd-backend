#!/usr/bin/env python3
"""
Generate ortholog-based descriptions for C. tropicalis genes.

This script creates descriptions in the format:
"Ortholog of C. albicans [GENE_NAME] ([systematic_name]), which [description]"

For genes without a gene name:
"Ortholog of C. albicans [systematic_name], which [description]"

Usage:
    python generate_ortholog_descriptions.py \
        --orthologs reciprocal_best_hits.txt \
        --output ctrop_descriptions.tsv
"""

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Optional
from urllib.request import urlopen

# CGD download URL for C. albicans chromosomal features
CALBICANS_FEATURES_URL = (
    "https://www.candidagenome.org/download/chromosomal_feature_files/"
    "C_albicans_SC5314/C_albicans_SC5314_A22_current_chromosomal_feature.tab"
)


def download_calbicans_annotations(cache_file: Path) -> dict[str, dict]:
    """
    Download and parse C. albicans gene annotations from CGD.

    Returns dict of {systematic_name: {gene_name, description, feature_type, cgd_id}}
    """
    # Check cache first
    if cache_file.exists():
        print(f"Using cached C. albicans annotations: {cache_file}")
        content = cache_file.read_text()
    else:
        print(f"Downloading C. albicans annotations from CGD...")
        with urlopen(CALBICANS_FEATURES_URL) as response:
            content = response.read().decode('utf-8')
        cache_file.write_text(content)
        print(f"Cached to: {cache_file}")

    annotations = {}

    for line in content.split('\n'):
        # Skip comments and empty lines
        if line.startswith('!') or not line.strip():
            continue

        fields = line.split('\t')
        if len(fields) < 10:
            continue

        systematic_name = fields[0].strip()
        gene_name = fields[1].strip() if len(fields) > 1 else ''
        aliases = fields[2].strip() if len(fields) > 2 else ''
        feature_type = fields[3].strip() if len(fields) > 3 else ''
        cgd_id = fields[8].strip() if len(fields) > 8 else ''
        # Description is at column 10 (index 10), after CGD ID and an empty column
        description = fields[10].strip() if len(fields) > 10 else ''

        # Only include ORF features (genes)
        if not feature_type.startswith('ORF'):
            continue

        annotations[systematic_name] = {
            'gene_name': gene_name,
            'aliases': aliases,
            'feature_type': feature_type,
            'cgd_id': cgd_id,
            'description': description,
        }

        # Also index by aliases (orf19. names)
        if aliases:
            for alias in aliases.split('|'):
                alias = alias.strip()
                if alias and alias not in annotations:
                    annotations[alias] = annotations[systematic_name]

    print(f"Loaded {len(annotations)} C. albicans annotations")
    return annotations


def load_orthologs(ortholog_file: Path) -> dict[str, str]:
    """
    Load ortholog mappings from reciprocal best hits file.

    Returns dict of {ctrop_id: calb_id}
    """
    orthologs = {}

    with open(ortholog_file, 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        header = next(reader)  # Skip header

        for row in reader:
            if len(row) >= 2:
                ctrop_id = row[0].strip()
                calb_id = row[1].strip()
                orthologs[ctrop_id] = calb_id

    print(f"Loaded {len(orthologs)} ortholog pairs")
    return orthologs


def clean_description(desc: str) -> str:
    """Clean and format description for use in ortholog context."""
    if not desc:
        return ''

    # Remove HTML tags
    desc = re.sub(r'<[^>]+>', '', desc)

    # Remove leading "Putative" or "Predicted" (we're already saying it's an ortholog)
    desc = re.sub(r'^(Putative|Predicted)\s+', '', desc, flags=re.IGNORECASE)

    # Lowercase first letter if not an acronym
    if desc and not desc[0:2].isupper():
        desc = desc[0].lower() + desc[1:]

    return desc


def generate_description(
    calb_systematic: str,
    calb_annotation: dict,
) -> str:
    """
    Generate ortholog-based description.

    Format: "Ortholog of C. albicans GENE (systematic), which description"
    """
    gene_name = calb_annotation.get('gene_name', '')
    description = calb_annotation.get('description', '')

    # Clean the description
    clean_desc = clean_description(description)

    # Build the ortholog reference
    if gene_name:
        ortholog_ref = f"C. albicans {gene_name} ({calb_systematic})"
    else:
        ortholog_ref = f"C. albicans {calb_systematic}"

    # Build the full description
    if clean_desc:
        # Check if description already starts with a verb or noun phrase
        if clean_desc[0].islower():
            full_desc = f"Ortholog of {ortholog_ref}; {clean_desc}"
        else:
            full_desc = f"Ortholog of {ortholog_ref}; {clean_desc}"
    else:
        full_desc = f"Ortholog of {ortholog_ref}"

    return full_desc


def main():
    parser = argparse.ArgumentParser(
        description='Generate ortholog-based descriptions for C. tropicalis genes'
    )
    parser.add_argument(
        '--orthologs', '-o',
        default='orthologs/reciprocal_best_hits.txt',
        help='Ortholog mapping file (default: orthologs/reciprocal_best_hits.txt)'
    )
    parser.add_argument(
        '--cache', '-c',
        default='C_albicans_annotations.tab',
        help='Cache file for C. albicans annotations (default: C_albicans_annotations.tab)'
    )
    parser.add_argument(
        '--output', '-O',
        default='ctrop_ortholog_descriptions.tsv',
        help='Output file (default: ctrop_ortholog_descriptions.tsv)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Print verbose output'
    )

    args = parser.parse_args()

    # Load data
    calb_annotations = download_calbicans_annotations(Path(args.cache))
    orthologs = load_orthologs(Path(args.orthologs))

    # Generate descriptions
    results = []
    no_annotation = 0
    with_gene_name = 0
    without_gene_name = 0

    for ctrop_id, calb_id in orthologs.items():
        # Try to find C. albicans annotation
        calb_annotation = calb_annotations.get(calb_id)

        if not calb_annotation:
            # Try without _A/_B suffix
            calb_base = re.sub(r'_[AB]$', '', calb_id)
            calb_annotation = calb_annotations.get(calb_base)

        if not calb_annotation:
            no_annotation += 1
            if args.verbose:
                print(f"No annotation found for: {calb_id}")
            continue

        # Generate description
        description = generate_description(calb_id, calb_annotation)

        if calb_annotation.get('gene_name'):
            with_gene_name += 1
        else:
            without_gene_name += 1

        results.append({
            'ctrop_protein_id': ctrop_id,
            'calb_systematic': calb_id,
            'calb_gene_name': calb_annotation.get('gene_name', ''),
            'calb_feature_type': calb_annotation.get('feature_type', ''),
            'generated_description': description,
            'original_calb_description': calb_annotation.get('description', ''),
        })

    # Write output
    output_path = Path(args.output)
    with open(output_path, 'w', newline='') as f:
        fieldnames = [
            'ctrop_protein_id',
            'calb_systematic',
            'calb_gene_name',
            'calb_feature_type',
            'generated_description',
            'original_calb_description',
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t')
        writer.writeheader()
        writer.writerows(results)

    # Summary
    print(f"\n{'='*60}")
    print("Summary")
    print('='*60)
    print(f"Total orthologs processed: {len(orthologs)}")
    print(f"Descriptions generated: {len(results)}")
    print(f"  - With C. albicans gene name: {with_gene_name}")
    print(f"  - Without gene name: {without_gene_name}")
    print(f"No C. albicans annotation found: {no_annotation}")
    print(f"\nOutput written to: {output_path}")

    # Show some examples
    print(f"\n{'='*60}")
    print("Example descriptions:")
    print('='*60)
    for i, r in enumerate(results[:10]):
        print(f"\n{r['ctrop_protein_id']}:")
        print(f"  {r['generated_description']}")


if __name__ == '__main__':
    main()
