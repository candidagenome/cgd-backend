#!/usr/bin/env python3
"""
Automatically collect CRISPOR guides for all 20 benchmark genes.

This script supports two modes:
1. CLI mode: Uses local CRISPOR installation (faster, more reliable)
2. Web mode: Uses CRISPOR web API via HTTP requests (fallback)

Usage:
    python collect_crispor_guides.py [--mode cli|web] [--genes GENE1,GENE2,...]
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlencode

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    import requests
except ImportError:
    requests = None


def load_test_genes() -> List[dict]:
    """Load test genes from fixture file."""
    fixture_path = Path(__file__).parent.parent.parent / "tests/api/fixtures/crispr_test_genes.json"
    with open(fixture_path) as f:
        return json.load(f)


def run_crispor_cli(sequence: str, gene_name: str, crispor_dir: str) -> Optional[List[dict]]:
    """Run CRISPOR using command-line tool."""
    crispor_script = os.path.join(crispor_dir, "run_crispor.sh")

    if not os.path.exists(crispor_script):
        print(f"  CRISPOR CLI not found at {crispor_script}")
        return None

    # Create temp input file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.fa', delete=False) as f:
        f.write(f">{gene_name}\n{sequence}\n")
        input_file = f.name

    # Create temp output file
    output_file = tempfile.mktemp(suffix='.tsv')

    try:
        # Run CRISPOR
        cmd = [crispor_script, "candAlb", input_file, output_file]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

        if result.returncode != 0:
            print(f"  CRISPOR error: {result.stderr[:200]}")
            return None

        # Parse output
        if not os.path.exists(output_file):
            print(f"  No output file generated")
            return None

        return parse_crispor_tsv(output_file)

    except subprocess.TimeoutExpired:
        print(f"  CRISPOR timed out")
        return None
    except Exception as e:
        print(f"  Error running CRISPOR: {e}")
        return None
    finally:
        # Cleanup
        if os.path.exists(input_file):
            os.unlink(input_file)
        if os.path.exists(output_file):
            os.unlink(output_file)


def parse_crispor_tsv(filepath: str) -> List[dict]:
    """Parse CRISPOR TSV output file."""
    guides = []
    with open(filepath) as f:
        header = None
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            parts = line.split('\t')
            if header is None:
                header = parts
                continue

            if len(parts) < 6:
                continue

            try:
                # Extract guide info
                # Typical columns: guideId, targetSeq, mitSpecScore, offtargetCount, ...
                guide_data = dict(zip(header, parts))

                # Extract the 20bp guide sequence (without PAM)
                target_seq = guide_data.get('targetSeq', '')
                if len(target_seq) >= 20:
                    sequence = target_seq[:20]  # First 20bp is guide, last 3 is PAM
                else:
                    sequence = target_seq

                guides.append({
                    'sequence': sequence,
                    'position': int(guide_data.get('start', 0)),
                    'strand': guide_data.get('strand', '+'),
                    'mit_specificity_score': float(guide_data.get('mitSpecScore', 0)),
                    'doench_efficiency_score': float(guide_data.get('doenchScore', 0)),
                    'cfd_score': float(guide_data.get('cfdSpecScore', 0)),
                    'offtargets_total': int(guide_data.get('offtargetCount', 0)),
                })
            except (ValueError, KeyError) as e:
                continue

    # Sort by MIT specificity score (descending)
    guides.sort(key=lambda x: -x['mit_specificity_score'])
    return guides


def run_crispor_web(sequence: str, gene_name: str) -> Optional[List[dict]]:
    """Run CRISPOR using web interface (fallback method)."""
    if requests is None:
        print("  requests library not installed, cannot use web mode")
        return None

    # CRISPOR web submission
    url = "http://crispor.gi.ucsc.edu/crispor.py"

    # Submit sequence
    data = {
        'seq': sequence,
        'org': 'candAlb',  # C. albicans genome code
        'pam': 'NGG',
        'submit': 'SUBMIT',
    }

    try:
        print(f"  Submitting to CRISPOR web...")
        response = requests.post(url, data=data, timeout=60, allow_redirects=True)

        if response.status_code != 200:
            print(f"  HTTP error: {response.status_code}")
            return None

        # Check if we got redirected to results page
        if 'batchId' in response.url:
            # Parse results from the page
            return parse_crispor_html(response.text)
        else:
            # May need to wait and poll for results
            print(f"  Results page: {response.url}")
            return parse_crispor_html(response.text)

    except requests.Timeout:
        print(f"  Web request timed out")
        return None
    except Exception as e:
        print(f"  Web request error: {e}")
        return None


def parse_crispor_html(html: str) -> List[dict]:
    """Parse CRISPOR HTML results page to extract guides."""
    guides = []

    # Look for guide table rows
    # Pattern: position, sequence+PAM, MIT score, CFD score, Doench score, etc.
    # This is a simplified parser - may need adjustment based on actual HTML structure

    # Try to find guide rows in the HTML
    # CRISPOR uses a table with class "guideTable" or similar

    # Simple regex to find guide sequences (20bp + 3bp PAM)
    pattern = r'([ACGT]{20})\s+(AGG|TGG|CGG|GGG)'
    matches = re.findall(pattern, html)

    for i, (seq, pam) in enumerate(matches[:50]):  # Top 50
        guides.append({
            'rank': i + 1,
            'sequence': seq,
            'pam': pam,
            'mit_specificity_score': 0,  # Would need more parsing
            'doench_efficiency_score': 0,
        })

    return guides


def collect_all_guides(genes: List[dict], mode: str = 'cli', crispor_dir: str = None) -> Dict[str, List[dict]]:
    """Collect CRISPOR guides for all genes."""
    if crispor_dir is None:
        crispor_dir = os.path.expanduser("~/tools/crispor")

    results = {}

    for i, gene in enumerate(genes):
        gene_name = gene['gene_name']
        sequence = gene['cds_first_500bp']

        print(f"\n[{i+1}/{len(genes)}] Processing {gene_name}...")

        if mode == 'cli':
            guides = run_crispor_cli(sequence, gene_name, crispor_dir)
        else:
            guides = run_crispor_web(sequence, gene_name)
            time.sleep(2)  # Rate limiting for web mode

        if guides:
            results[gene_name] = guides[:10]  # Top 10 guides
            print(f"  Found {len(guides)} guides, keeping top 10")
        else:
            results[gene_name] = []
            print(f"  No guides found")

    return results


def save_results(results: Dict[str, List[dict]], output_path: str):
    """Save results in the benchmark fixture format."""
    # Load existing fixture structure
    fixture_path = Path(__file__).parent.parent.parent / "tests/api/fixtures/crispr_test_genes.json"
    with open(fixture_path) as f:
        genes = json.load(f)

    # Build output structure
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
                    'doench_efficiency_score': g.get('doench_efficiency_score', 0),
                    'offtargets_0mm': 0,
                    'offtargets_1mm': 0,
                    'offtargets_2mm': 0,
                    'offtargets_3mm': 0,
                }
                for i, g in enumerate(crispor_guides)
            ],
            'notes': f"Collected via CRISPOR {'CLI' if crispor_guides else 'N/A'}"
        })

    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nResults saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Collect CRISPOR guides for benchmark genes')
    parser.add_argument('--mode', choices=['cli', 'web'], default='cli',
                        help='Collection mode: cli (local installation) or web (HTTP requests)')
    parser.add_argument('--genes', type=str, default=None,
                        help='Comma-separated list of gene names to process (default: all)')
    parser.add_argument('--crispor-dir', type=str, default=None,
                        help='CRISPOR installation directory')
    parser.add_argument('--output', type=str, default=None,
                        help='Output file path')

    args = parser.parse_args()

    # Load genes
    all_genes = load_test_genes()

    if args.genes:
        gene_names = set(args.genes.split(','))
        genes = [g for g in all_genes if g['gene_name'] in gene_names]
    else:
        genes = all_genes

    print(f"CRISPOR Guide Collection")
    print(f"========================")
    print(f"Mode: {args.mode}")
    print(f"Genes: {len(genes)}")

    # Collect guides
    results = collect_all_guides(genes, mode=args.mode, crispor_dir=args.crispor_dir)

    # Save results
    output_path = args.output or str(Path(__file__).parent / "crispor_results.json")
    save_results(results, output_path)

    # Print summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    total_guides = sum(len(v) for v in results.values())
    genes_with_guides = sum(1 for v in results.values() if v)
    print(f"Genes processed: {len(genes)}")
    print(f"Genes with guides: {genes_with_guides}")
    print(f"Total guides collected: {total_guides}")


if __name__ == '__main__':
    main()
