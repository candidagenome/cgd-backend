#!/usr/bin/env python3
"""
Automatically collect CRISPOR guides for benchmark genes.

This script:
1. Submits each gene's 5' sequence to CRISPOR web interface
2. Downloads the TSV results
3. Parses and saves the guide data

Usage:
    python collect_crispor_auto.py [--genes GENE1,GENE2,...] [--delay 5]
"""

import argparse
import json
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# CRISPOR settings
CRISPOR_URL = "https://crispor.gi.ucsc.edu/crispor.py"
ORGANISM_CODE = "GCF_000182965.3"  # Candida albicans SC5314
PAM = "NGG"


def load_test_genes() -> List[dict]:
    """Load test genes from fixture file."""
    fixture_path = Path(__file__).parent.parent.parent / "tests/api/fixtures/crispr_test_genes.json"
    with open(fixture_path) as f:
        return json.load(f)


def submit_to_crispor(sequence: str, gene_name: str = "") -> Optional[str]:
    """Submit sequence to CRISPOR and return batch ID."""
    data = {
        'name': gene_name,
        'seq': sequence,
        'org': ORGANISM_CODE,
        'pam': PAM,
        'submit': 'SUBMIT',
    }

    encoded_data = urllib.parse.urlencode(data).encode('utf-8')
    ctx = ssl.create_default_context()

    req = urllib.request.Request(CRISPOR_URL, data=encoded_data, method='POST')
    req.add_header('User-Agent', 'Mozilla/5.0 (CGD-Benchmark/1.0)')
    req.add_header('Content-Type', 'application/x-www-form-urlencoded')
    req.add_header('Origin', 'https://crispor.gi.ucsc.edu')
    req.add_header('Referer', CRISPOR_URL)

    try:
        with urllib.request.urlopen(req, timeout=120, context=ctx) as response:
            html = response.read().decode('utf-8', errors='ignore')

            # Extract batch ID from response
            batch_match = re.search(r'batchId=([a-zA-Z0-9]+)', html)
            if batch_match:
                return batch_match.group(1)

            return None

    except Exception as e:
        print(f"    Error submitting: {e}")
        return None


def download_crispor_tsv(batch_id: str, max_retries: int = 5, retry_delay: float = 3.0) -> Optional[str]:
    """Download TSV results for a batch with retry logic."""
    tsv_url = f"{CRISPOR_URL}?batchId={batch_id}&download=guides"
    ctx = ssl.create_default_context()

    for attempt in range(max_retries):
        req = urllib.request.Request(tsv_url)
        req.add_header('User-Agent', 'Mozilla/5.0 (CGD-Benchmark/1.0)')

        try:
            with urllib.request.urlopen(req, timeout=60, context=ctx) as response:
                content = response.read().decode('utf-8', errors='ignore')
                # Check if we got valid TSV content
                if 'guideId' in content or 'targetSeq' in content:
                    return content
                else:
                    print(f"    Attempt {attempt+1}: Got response but no guide data, retrying...")

        except urllib.error.HTTPError as e:
            if e.code == 500 and attempt < max_retries - 1:
                print(f"    Attempt {attempt+1}: Server processing, waiting {retry_delay}s...")
                time.sleep(retry_delay)
                continue
            else:
                print(f"    Error downloading TSV: {e}")
                return None
        except Exception as e:
            print(f"    Error downloading TSV: {e}")
            return None

        time.sleep(retry_delay)

    return None


def parse_crispor_tsv(tsv_content: str) -> List[dict]:
    """Parse CRISPOR TSV output to extract guides."""
    guides = []
    lines = tsv_content.strip().split('\n')

    if len(lines) < 2:
        return guides

    # Parse header
    header_line = lines[0].lstrip('#')
    header = [h.strip() for h in header_line.split('\t')]

    # Create column index mapping
    col_idx = {name.lower(): i for i, name in enumerate(header)}

    # Parse data rows
    for line in lines[1:]:
        parts = line.split('\t')
        if len(parts) < 4:
            continue

        try:
            # Extract guide sequence (23bp = 20bp guide + 3bp PAM)
            target_seq = parts[col_idx.get('targetseq', 1)]
            if len(target_seq) >= 23:
                sequence = target_seq[:20]
                pam = target_seq[20:23]
            else:
                sequence = target_seq
                pam = ''

            # Extract position and strand from guideId (e.g., "29rev", "39forw")
            guide_id = parts[col_idx.get('guideid', col_idx.get('#guideid', 0))]
            pos_match = re.match(r'(\d+)(rev|forw)', guide_id)
            if pos_match:
                position = int(pos_match.group(1))
                strand = '-' if pos_match.group(2) == 'rev' else '+'
            else:
                position = 0
                strand = '+'

            # Extract scores
            mit_score = float(parts[col_idx.get('mitspecscore', 2)]) if col_idx.get('mitspecscore') else 0
            cfd_score = float(parts[col_idx.get('cfdspecscore', 3)]) if col_idx.get('cfdspecscore') else 0
            offtarget_count = int(parts[col_idx.get('offtargetcount', 4)]) if col_idx.get('offtargetcount') else 0

            # Doench score
            doench_score = 0
            for key in ["doench '16-score", "doench16-score", "doenchscore"]:
                if key in col_idx:
                    try:
                        doench_score = float(parts[col_idx[key]])
                    except (ValueError, IndexError):
                        pass
                    break

            guides.append({
                'sequence': sequence,
                'pam': pam,
                'position': position,
                'strand': strand,
                'mit_specificity_score': mit_score,
                'cfd_score': cfd_score,
                'doench_efficiency_score': doench_score,
                'offtarget_count': offtarget_count,
            })

        except (ValueError, IndexError, KeyError) as e:
            continue

    # Sort by MIT specificity score (descending), then by position
    guides.sort(key=lambda x: (-x['mit_specificity_score'], x['position']))

    return guides


def collect_gene_guides(gene: dict, delay: float = 3.0) -> Tuple[str, List[dict]]:
    """Collect CRISPOR guides for a single gene."""
    gene_name = gene['gene_name']
    sequence = gene['cds_first_500bp']

    # Submit to CRISPOR
    print(f"    Submitting {len(sequence)}bp sequence...")
    batch_id = submit_to_crispor(sequence, gene_name)

    if not batch_id:
        print(f"    Failed to get batch ID")
        return gene_name, []

    print(f"    Got batch ID: {batch_id}")

    # Wait for processing
    time.sleep(delay)

    # Download TSV results
    print(f"    Downloading results...")
    tsv_content = download_crispor_tsv(batch_id)

    if not tsv_content:
        print(f"    Failed to download TSV")
        return gene_name, []

    # Parse results
    guides = parse_crispor_tsv(tsv_content)
    print(f"    Found {len(guides)} guides")

    return gene_name, guides[:10]  # Return top 10


def save_results(results: Dict[str, List[dict]], output_path: str):
    """Save results in the benchmark fixture format."""
    fixture_path = Path(__file__).parent.parent.parent / "tests/api/fixtures/crispr_test_genes.json"
    with open(fixture_path) as f:
        genes = json.load(f)

    output = []
    for gene in genes:
        gene_name = gene['gene_name']
        crispor_guides = results.get(gene_name, [])

        output.append({
            'gene_name': gene_name,
            'feature_name': gene['feature_name'],
            'crispor_top_guides': [
                {
                    'rank': i + 1,
                    'sequence': g.get('sequence', ''),
                    'position': g.get('position', 0),
                    'strand': g.get('strand', '+'),
                    'mit_specificity_score': g.get('mit_specificity_score', 0),
                    'cfd_score': g.get('cfd_score', 0),
                    'doench_efficiency_score': g.get('doench_efficiency_score', 0),
                    'offtarget_count': g.get('offtarget_count', 0),
                }
                for i, g in enumerate(crispor_guides)
            ],
            'notes': f'Collected via CRISPOR web (batch)' if crispor_guides else 'No guides found'
        })

    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nResults saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Collect CRISPOR guides automatically')
    parser.add_argument('--genes', type=str, default=None,
                        help='Comma-separated list of gene names (default: all 20)')
    parser.add_argument('--delay', type=float, default=5.0,
                        help='Delay between requests in seconds (default: 5)')
    parser.add_argument('--output', type=str, default=None,
                        help='Output file path')

    args = parser.parse_args()

    # Load genes
    all_genes = load_test_genes()

    if args.genes:
        gene_names = set(g.strip() for g in args.genes.split(','))
        genes = [g for g in all_genes if g['gene_name'] in gene_names]
        if not genes:
            print(f"Error: No matching genes found for: {args.genes}")
            print(f"Available genes: {[g['gene_name'] for g in all_genes]}")
            sys.exit(1)
    else:
        genes = all_genes

    print("=" * 70)
    print("CRISPOR Automatic Guide Collection")
    print("=" * 70)
    print(f"Organism: Candida albicans SC5314 ({ORGANISM_CODE})")
    print(f"PAM: {PAM}")
    print(f"Genes to process: {len(genes)}")
    print(f"Delay between requests: {args.delay}s")
    print("=" * 70)

    results = {}
    successful = 0
    failed = 0

    for i, gene in enumerate(genes):
        gene_name = gene['gene_name']
        print(f"\n[{i+1}/{len(genes)}] {gene_name}")

        try:
            name, guides = collect_gene_guides(gene, delay=args.delay)
            results[name] = guides

            if guides:
                successful += 1
                # Show top 3 guides
                print(f"    Top 3 guides:")
                for g in guides[:3]:
                    print(f"      {g['sequence']} MIT:{g['mit_specificity_score']:.0f} Doench:{g['doench_efficiency_score']:.0f}")
            else:
                failed += 1

        except Exception as e:
            print(f"    ERROR: {e}")
            results[gene_name] = []
            failed += 1

        # Rate limiting
        if i < len(genes) - 1:
            print(f"    Waiting {args.delay}s...")
            time.sleep(args.delay)

    # Save results
    output_path = args.output or str(Path(__file__).parent / "crispor_results.json")
    save_results(results, output_path)

    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    total_guides = sum(len(v) for v in results.values())
    print(f"Genes processed: {len(genes)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total guides collected: {total_guides}")


if __name__ == '__main__':
    main()
