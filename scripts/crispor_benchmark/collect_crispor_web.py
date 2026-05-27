#!/usr/bin/env python3
"""
Collect CRISPOR guides via web interface.

This script submits sequences to CRISPOR's web interface and parses the results.
It handles the batch submission and result parsing automatically.

Usage:
    python collect_crispor_web.py [--genes GENE1,GENE2,...] [--delay 5]
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode, urljoin
import urllib.request
import urllib.error

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


CRISPOR_URL = "http://crispor.gi.ucsc.edu/crispor.py"
GENOME_CODE = "GCF_000182965.3"  # Candida albicans SC5314


def load_test_genes() -> List[dict]:
    """Load test genes from fixture file."""
    fixture_path = Path(__file__).parent.parent.parent / "tests/api/fixtures/crispr_test_genes.json"
    with open(fixture_path) as f:
        return json.load(f)


def submit_sequence(sequence: str, gene_name: str) -> Optional[str]:
    """Submit sequence to CRISPOR and get the batch ID or results URL."""
    data = {
        'seq': sequence,
        'org': GENOME_CODE,
        'pam': 'NGG',
        'submit': 'SUBMIT',
    }

    encoded_data = urlencode(data).encode('utf-8')

    try:
        req = urllib.request.Request(CRISPOR_URL, data=encoded_data)
        req.add_header('User-Agent', 'Mozilla/5.0 (CGD Benchmark Script)')
        req.add_header('Content-Type', 'application/x-www-form-urlencoded')

        with urllib.request.urlopen(req, timeout=120) as response:
            # Get the final URL after redirects
            final_url = response.geturl()
            html = response.read().decode('utf-8', errors='ignore')

            # Extract batch ID from URL
            if 'batchId=' in final_url:
                return final_url

            # Look for batch ID in the page
            batch_match = re.search(r'batchId=([a-zA-Z0-9]+)', html)
            if batch_match:
                batch_id = batch_match.group(1)
                return f"{CRISPOR_URL}?batchId={batch_id}"

            # If we got results directly, return the HTML
            if 'Predicted guide sequences' in html or 'guideSeq' in html:
                return html

            return None

    except urllib.error.URLError as e:
        print(f"  URL Error: {e}")
        return None
    except Exception as e:
        print(f"  Error: {e}")
        return None


def fetch_results(url_or_html: str) -> Optional[str]:
    """Fetch results page HTML."""
    if url_or_html.startswith('http'):
        try:
            req = urllib.request.Request(url_or_html)
            req.add_header('User-Agent', 'Mozilla/5.0 (CGD Benchmark Script)')

            with urllib.request.urlopen(req, timeout=60) as response:
                return response.read().decode('utf-8', errors='ignore')
        except Exception as e:
            print(f"  Fetch error: {e}")
            return None
    else:
        return url_or_html


def parse_crispor_results(html: str) -> List[dict]:
    """Parse CRISPOR HTML results to extract guide information."""
    guides = []

    # CRISPOR HTML structure has guide info in table rows
    # Pattern to match guide sequences with their scores
    # Format in table: position/strand, guideSeq+PAM, MIT score, CFD score, Doench scores...

    # First, try to find the guide table
    # Look for rows containing guide data

    # Pattern for guide rows (simplified)
    # The format is typically: SEQUENCE + PAM (like "ATACACTGAGCTAAATCCCG TGG")
    row_pattern = r'<tr[^>]*>.*?<td[^>]*>(\d+)\s*/\s*(fw|rev)</td>.*?<td[^>]*>([ACGT]{20,23})\s+([ACGT]{3})</td>.*?<td[^>]*>(\d+)</td>.*?<td[^>]*>(\d+)</td>.*?<td[^>]*>(\d+)</td>'

    matches = re.findall(row_pattern, html, re.DOTALL | re.IGNORECASE)

    if matches:
        for match in matches[:50]:
            position, strand, guide_seq, pam, mit_score, cfd_score, doench_score = match
            # Guide sequence might include PAM, extract just the 20bp
            seq = guide_seq[:20] if len(guide_seq) > 20 else guide_seq

            guides.append({
                'sequence': seq,
                'position': int(position),
                'strand': '+' if strand.lower() == 'fw' else '-',
                'mit_specificity_score': float(mit_score),
                'cfd_score': float(cfd_score),
                'doench_efficiency_score': float(doench_score),
            })
    else:
        # Fallback: simpler pattern matching
        # Look for 20bp sequences followed by PAM
        simple_pattern = r'([ACGT]{20})\s+([ACGT]GG)'
        simple_matches = re.findall(simple_pattern, html)

        # Also try to extract MIT scores
        score_pattern = r'([ACGT]{20})[^0-9]*(\d{1,3})\s*</td>'
        score_matches = re.findall(score_pattern, html)
        score_dict = {m[0]: int(m[1]) for m in score_matches}

        seen = set()
        for seq, pam in simple_matches:
            if seq not in seen:
                seen.add(seq)
                guides.append({
                    'sequence': seq,
                    'position': 0,
                    'strand': '+',
                    'mit_specificity_score': score_dict.get(seq, 0),
                    'doench_efficiency_score': 0,
                })

    # Sort by MIT specificity score (descending)
    guides.sort(key=lambda x: -x.get('mit_specificity_score', 0))

    return guides


def download_tsv_results(batch_url: str) -> Optional[str]:
    """Try to download TSV format results which are easier to parse."""
    # CRISPOR provides a TSV download link
    # Try to construct the download URL

    if 'batchId=' in batch_url:
        batch_id = re.search(r'batchId=([a-zA-Z0-9]+)', batch_url).group(1)
        tsv_url = f"{CRISPOR_URL}?batchId={batch_id}&download=guides"

        try:
            req = urllib.request.Request(tsv_url)
            req.add_header('User-Agent', 'Mozilla/5.0 (CGD Benchmark Script)')

            with urllib.request.urlopen(req, timeout=60) as response:
                content_type = response.headers.get('Content-Type', '')
                content = response.read().decode('utf-8', errors='ignore')

                # Check if we got TSV data
                if 'guideId' in content or '\t' in content.split('\n')[0]:
                    return content

        except Exception:
            pass

    return None


def parse_crispor_tsv(tsv_content: str) -> List[dict]:
    """Parse CRISPOR TSV output."""
    guides = []
    lines = tsv_content.strip().split('\n')

    if not lines:
        return guides

    # Find header line
    header = None
    for i, line in enumerate(lines):
        if 'guideId' in line.lower() or 'targetseq' in line.lower() or 'mitspecscore' in line.lower():
            header = line.lower().split('\t')
            lines = lines[i+1:]
            break

    if header is None:
        # Try first line as header
        header = lines[0].lower().split('\t')
        lines = lines[1:]

    for line in lines:
        parts = line.split('\t')
        if len(parts) < 3:
            continue

        try:
            data = dict(zip(header, parts))

            # Extract sequence - handle various column names
            seq = ''
            for key in ['targetseq', 'guideseq', 'sequence', 'guide']:
                if key in data:
                    seq = data[key][:20] if len(data.get(key, '')) >= 20 else data.get(key, '')
                    break

            if not seq or len(seq) != 20:
                continue

            # Extract scores
            mit_score = 0
            for key in ['mitspecscore', 'mit', 'specificity']:
                if key in data:
                    try:
                        mit_score = float(data[key])
                    except:
                        pass
                    break

            doench_score = 0
            for key in ['doench', 'doench16', 'efficiency', 'doenchscore']:
                if key in data:
                    try:
                        doench_score = float(data[key])
                    except:
                        pass
                    break

            guides.append({
                'sequence': seq,
                'position': int(data.get('start', data.get('position', 0)) or 0),
                'strand': data.get('strand', '+'),
                'mit_specificity_score': mit_score,
                'doench_efficiency_score': doench_score,
            })

        except Exception as e:
            continue

    guides.sort(key=lambda x: -x.get('mit_specificity_score', 0))
    return guides


def collect_gene_guides(gene: dict, delay: float = 3.0) -> Tuple[str, List[dict]]:
    """Collect CRISPOR guides for a single gene."""
    gene_name = gene['gene_name']
    sequence = gene['cds_first_500bp']

    print(f"  Submitting {gene_name} ({len(sequence)}bp)...")

    # Submit sequence
    result = submit_sequence(sequence, gene_name)

    if not result:
        print(f"  Failed to submit sequence")
        return gene_name, []

    time.sleep(delay)  # Wait for processing

    # Try to get TSV results first
    if result.startswith('http'):
        tsv_content = download_tsv_results(result)
        if tsv_content:
            guides = parse_crispor_tsv(tsv_content)
            if guides:
                print(f"  Parsed {len(guides)} guides from TSV")
                return gene_name, guides[:10]

    # Fall back to HTML parsing
    html = fetch_results(result)
    if html:
        guides = parse_crispor_results(html)
        print(f"  Parsed {len(guides)} guides from HTML")
        return gene_name, guides[:10]

    print(f"  No guides found")
    return gene_name, []


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
                    'doench_efficiency_score': g.get('doench_efficiency_score', 0),
                    'offtargets_0mm': 0,
                    'offtargets_1mm': 0,
                    'offtargets_2mm': 0,
                    'offtargets_3mm': 0,
                }
                for i, g in enumerate(crispor_guides)
            ],
            'notes': 'Collected via CRISPOR web interface'
        })

    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nResults saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Collect CRISPOR guides via web interface')
    parser.add_argument('--genes', type=str, default=None,
                        help='Comma-separated list of gene names (default: all)')
    parser.add_argument('--delay', type=float, default=5.0,
                        help='Delay between requests in seconds (default: 5)')
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

    print("=" * 60)
    print("CRISPOR Web Guide Collection")
    print("=" * 60)
    print(f"Genes to process: {len(genes)}")
    print(f"Delay between requests: {args.delay}s")
    print()

    results = {}
    for i, gene in enumerate(genes):
        print(f"[{i+1}/{len(genes)}]", end="")
        gene_name, guides = collect_gene_guides(gene, delay=args.delay)
        results[gene_name] = guides

        if i < len(genes) - 1:
            time.sleep(args.delay)  # Rate limiting

    # Save results
    output_path = args.output or str(Path(__file__).parent / "crispor_results.json")
    save_results(results, output_path)

    # Print summary
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    total_guides = sum(len(v) for v in results.values())
    genes_with_guides = sum(1 for v in results.values() if v)
    print(f"Genes processed: {len(genes)}")
    print(f"Genes with guides: {genes_with_guides}")
    print(f"Total guides collected: {total_guides}")


if __name__ == '__main__':
    main()
