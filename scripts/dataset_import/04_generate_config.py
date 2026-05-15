#!/usr/bin/env python3
"""
04_generate_config.py - Generate expression_service.py study configurations

This script generates Python dict configurations for new expression studies
that can be added to cgd/api/services/expression_service.py.

Usage:
    python 04_generate_config.py --species C_auris_B8441 --study Wang_2024 \
        --pmid 39455573 --ncbi PRJNA1086003 --bigwig-dir /data/HTS/C_auris_B8441/bam/Wang_2024

    python 04_generate_config.py --from-metadata study_metadata.json

Output:
    Prints Python dict configuration to stdout, ready to paste into EXPRESSION_STUDIES.
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Optional


def get_category_from_description(description: str) -> str:
    """Auto-categorize study based on description keywords."""
    desc_lower = description.lower() if description else ""

    if any(kw in desc_lower for kw in ["drug", "resistance", "antifungal", "fluconazole", "amphotericin", "caspofungin", "azole"]):
        return "Drug Resistance"
    elif any(kw in desc_lower for kw in ["biofilm", "adhesion", "aggregation"]):
        return "Biofilm"
    elif any(kw in desc_lower for kw in ["virulence", "pathogen", "host", "infection"]):
        return "Virulence"
    elif any(kw in desc_lower for kw in ["stress", "oxidative", "heat", "ph"]):
        return "Stress Response"
    elif any(kw in desc_lower for kw in ["morphology", "hyphal", "yeast", "filament"]):
        return "Morphogenesis"
    elif any(kw in desc_lower for kw in ["transcription", "regulator", "mutant"]):
        return "Gene Regulation"
    else:
        return "Gene Expression"


def scan_bigwig_files(bigwig_dir: str) -> list[str]:
    """Scan directory for BigWig files and extract condition names."""
    conditions = []
    bigwig_path = Path(bigwig_dir)

    if not bigwig_path.exists():
        print(f"Warning: BigWig directory not found: {bigwig_dir}", file=sys.stderr)
        return conditions

    for bw_file in bigwig_path.glob("*.bw"):
        # Extract condition name from filename
        # Common patterns: SRR123456.bw, control_rep1.bw, treatment_rep2.bw
        name = bw_file.stem
        conditions.append(name)

    return sorted(conditions)


def infer_control_condition(conditions: list[str]) -> Optional[str]:
    """Try to identify the control condition from condition names."""
    control_keywords = ["control", "ctrl", "wt", "wildtype", "wild_type", "untreated", "mock", "dmso"]

    for cond in conditions:
        cond_lower = cond.lower()
        for kw in control_keywords:
            if kw in cond_lower:
                return cond

    # If no obvious control, return first condition
    return conditions[0] if conditions else None


def generate_config(
    species: str,
    study_name: str,
    pmid: Optional[str] = None,
    ncbi_id: Optional[str] = None,
    category: Optional[str] = None,
    description: Optional[str] = None,
    bigwig_dir: Optional[str] = None,
    conditions: Optional[list[str]] = None,
    control: Optional[str] = None,
    path_style: str = "new",
) -> str:
    """Generate Python dict configuration for a study."""

    # Auto-detect conditions from BigWig directory
    if bigwig_dir and not conditions:
        conditions = scan_bigwig_files(bigwig_dir)

    # Auto-detect category
    if not category and description:
        category = get_category_from_description(description)
    elif not category:
        category = "Gene Expression"

    # Auto-detect control
    if conditions and not control:
        control = infer_control_condition(conditions)

    # Build conditions dict
    conditions_dict = {}
    if conditions:
        for cond in conditions:
            # Try to infer bucket (control vs experimental)
            cond_lower = cond.lower()
            if control and cond == control:
                bucket = "control"
            elif any(kw in cond_lower for kw in ["control", "ctrl", "wt", "untreated"]):
                bucket = "control"
            else:
                bucket = "experimental"

            # Generate label from condition name
            label = cond.replace("_", " ").replace("-", " ").title()

            conditions_dict[cond] = {
                "label": label,
                "bucket": bucket,
            }

    # Generate Python dict as string
    config = f'''
    "{study_name}": {{
        "category": "{category}",'''

    if pmid:
        config += f'''
        "pmid": "{pmid}",'''
    else:
        config += '''
        "pmid": None,'''

    if ncbi_id:
        config += f'''
        "ncbi_id": "{ncbi_id}",'''

    config += f'''
        "path_style": "{path_style}",'''

    if control:
        config += f'''
        "control": "{control}",'''

    config += '''
        "conditions": {'''

    for cond_name, cond_data in conditions_dict.items():
        config += f'''
            "{cond_name}": {{"label": "{cond_data['label']}", "bucket": "{cond_data['bucket']}"}},'''

    config += '''
        }
    },'''

    return config


def main():
    parser = argparse.ArgumentParser(
        description="Generate expression_service.py study configurations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate config for a new study
  python 04_generate_config.py --species C_auris_B8441 --study Wang_2024 \\
      --pmid 39455573 --ncbi PRJNA1086003 \\
      --bigwig-dir /data/HTS/C_auris_B8441/bam/Wang_2024

  # Generate config from metadata file
  python 04_generate_config.py --from-metadata study_info.json

  # Specify conditions manually
  python 04_generate_config.py --species C_auris_B8441 --study Test \\
      --conditions control,treatment_1h,treatment_6h --control control
        """,
    )

    parser.add_argument("--species", help="Species ID (e.g., C_auris_B8441)")
    parser.add_argument("--study", help="Study name (e.g., Wang_2024)")
    parser.add_argument("--pmid", help="PubMed ID")
    parser.add_argument("--ncbi", help="NCBI BioProject/GEO ID")
    parser.add_argument("--category", help="Study category (auto-detected if not provided)")
    parser.add_argument("--description", help="Study description (used for auto-categorization)")
    parser.add_argument("--bigwig-dir", help="Directory containing BigWig files")
    parser.add_argument("--conditions", help="Comma-separated list of condition names")
    parser.add_argument("--control", help="Control condition name")
    parser.add_argument("--path-style", default="new", help="Path style: old, new, lohse, direct")
    parser.add_argument("--from-metadata", help="JSON file with study metadata")

    args = parser.parse_args()

    # Load from metadata file if provided
    if args.from_metadata:
        with open(args.from_metadata) as f:
            metadata = json.load(f)
        args.species = metadata.get("species", args.species)
        args.study = metadata.get("study", args.study)
        args.pmid = metadata.get("pmid", args.pmid)
        args.ncbi = metadata.get("ncbi_id", args.ncbi)
        args.category = metadata.get("category", args.category)
        args.description = metadata.get("description", args.description)
        args.bigwig_dir = metadata.get("bigwig_dir", args.bigwig_dir)
        args.conditions = metadata.get("conditions", args.conditions)
        args.control = metadata.get("control", args.control)

    # Validate required arguments
    if not args.study:
        parser.error("--study is required")

    # Parse conditions if provided as string
    conditions = None
    if args.conditions:
        if isinstance(args.conditions, str):
            conditions = [c.strip() for c in args.conditions.split(",")]
        else:
            conditions = args.conditions

    # Generate and print config
    config = generate_config(
        species=args.species or "SPECIES_ID",
        study_name=args.study,
        pmid=args.pmid,
        ncbi_id=args.ncbi,
        category=args.category,
        description=args.description,
        bigwig_dir=args.bigwig_dir,
        conditions=conditions,
        control=args.control,
        path_style=args.path_style,
    )

    print("# Add this to EXPRESSION_STUDIES in cgd/api/services/expression_service.py")
    print(f"# Species: {args.species or 'SPECIES_ID'}")
    print(config)


if __name__ == "__main__":
    main()
