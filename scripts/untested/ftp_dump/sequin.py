#!/usr/bin/env python3
"""
Create NCBI Sequin table files.

This script creates feature information files for NCBI genome section in
sequin table format (.tbl files), one file per chromosome.

Based on sequin.pl.

Usage:
    python sequin.py
    python sequin.py --debug
    python sequin.py --help

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Data directory for output files
    PROJECT_ACRONYM: Project acronym (e.g., CGD, SGD)
    SEQ_SOURCE: Sequence source (e.g., Stanford)

Output Files:
    data/sequin/chr01.tbl through chr16.tbl, chrmt.tbl
"""

import argparse
import html
import logging
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
SEQ_SOURCE = os.getenv("SEQ_SOURCE", "Stanford")

# Output directory
SEQUIN_DIR = DATA_DIR / "sequin"

# Application name for web_metadata
APPLICATION_NAME = "Sequin"

# Maximum chromosome number
MAX_CHRNUM = 17

# Feature type mappings to sequin types
TYPE_TO_SHOW = {
    "ORF": "gene",
    "ARS": "rep_origin",
    "CDS": "CDS",
    "ARS consensus sequence": "rep_origin",
    "telomeric_repeat": "repeat_region",
    "X_element_combinatorial_repeats": "repeat_region",
    "X_element_core_sequence": "repeat_region",
    "Y'_element": "repeat_region",
    "telomere": "repeat_region",
    "centromere": "misc_feature",
    "CDEI": "misc_feature",
    "CDEII": "misc_feature",
    "CDEIII": "misc_feature",
    "long_terminal_repeat": "LTR",
    "retrotransposon": "repeat_region",
    "transposable_element_gene": "CDS",
    "pseudogene": "gene",
    "tRNA": "gene",
    "noncoding_exon": "tRNA",
}

# Repeat type descriptions
RPT_TO_SHOW = {
    "telomeric_repeat": "Telomeric Repeat",
    "X_element_combinatorial_repeats": "X element Combinatorial Repeats",
    "X_element_core_sequence": "X element Core sequence",
    "Y'_element": "Y' element",
    "telomere": "Telomeric Region",
    "retrotransposon": "Transposon",
}

# GO aspect mapping
GO_ASPECT = {
    "P": "go_process",
    "C": "go_component",
    "F": "go_function",
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def delete_html_tag(text_str: str | None) -> str:
    """Remove HTML tags from text."""
    if not text_str:
        return ""
    # Remove HTML tags
    clean = re.sub(r"<[^>]+>", "", text_str)
    return html.unescape(clean)


def format_goid(goid: int) -> str:
    """Format GO ID with leading zeros."""
    return f"GO:{goid:07d}"


def get_ncbi_accessions(session) -> dict[int, str]:
    """Get NCBI accession numbers for chromosomes."""
    query = text(f"""
        SELECT f.feature_name, d.dbxref_id
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.dbxref_feat df ON f.feature_no = df.feature_no
        JOIN {DB_SCHEMA}.dbxref d ON df.dbxref_no = d.dbxref_no
        WHERE f.feature_type = 'chromosome'
        AND d.source = 'NCBI'
    """)

    accessions = {}
    for row in session.execute(query).fetchall():
        chr_name, acc = row
        try:
            chr_num = int(chr_name)
            accessions[chr_num] = acc
        except ValueError:
            pass

    return accessions


def get_feature_qualifiers(session) -> dict[str, str]:
    """Get feature qualifiers for all features."""
    query = text(f"""
        SELECT UPPER(f.feature_name), fp.property_value
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_property fp ON f.feature_no = fp.feature_no
        WHERE fp.property_type = 'Feature Qualifier'
    """)

    qualifiers = {}
    for row in session.execute(query).fetchall():
        qualifiers[row[0]] = row[1]

    return qualifiers


def get_non_standard_genes(session) -> set[str]:
    """Get non-standardized gene names."""
    query = text(f"""
        SELECT UPPER(f.gene_name)
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.gene_reservation gr ON f.feature_no = gr.feature_no
        WHERE gr.date_standardized IS NULL
    """)

    non_std = set()
    for row in session.execute(query).fetchall():
        if row[0]:
            non_std.add(row[0])

    return non_std


def get_aliases(session) -> dict[str, list[str]]:
    """Get alias names for all features."""
    query = text(f"""
        SELECT UPPER(f.feature_name), a.alias_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_alias fa ON f.feature_no = fa.feature_no
        JOIN {DB_SCHEMA}.alias a ON fa.alias_no = a.alias_no
        WHERE a.alias_type = 'Uniform'
    """)

    aliases: dict[str, list[str]] = {}
    for row in session.execute(query).fetchall():
        feat_name, alias = row
        if feat_name not in aliases:
            aliases[feat_name] = []
        aliases[feat_name].append(alias)

    return aliases


def get_ec_numbers(session) -> dict[str, str]:
    """Get EC numbers for features."""
    query = text(f"""
        SELECT UPPER(f.feature_name), d.dbxref_id
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.dbxref_feat df ON f.feature_no = df.feature_no
        JOIN {DB_SCHEMA}.dbxref d ON df.dbxref_no = d.dbxref_no
        WHERE d.dbxref_type = 'EC number'
    """)

    ec_numbers = {}
    for row in session.execute(query).fetchall():
        ec_numbers[row[0]] = row[1]

    return ec_numbers


def get_gene_products(session) -> dict[str, str]:
    """Get gene products for features."""
    query = text(f"""
        SELECT UPPER(f.feature_name), gp.gene_product
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_gp fg ON f.feature_no = fg.feature_no
        JOIN {DB_SCHEMA}.gene_product gp ON fg.gene_product_no = gp.gene_product_no
    """)

    products: dict[str, list[str]] = {}
    for row in session.execute(query).fetchall():
        feat_name, gp = row
        if feat_name not in products:
            products[feat_name] = []
        products[feat_name].append(gp)

    # Join with semicolons
    return {k: "; ".join(v) for k, v in products.items()}


def get_go_annotations(session) -> dict[str, str]:
    """Get GO annotations for features."""
    query = text(f"""
        SELECT UPPER(f.feature_name), g.goid, g.go_term, g.go_aspect,
               ga.go_evidence, r.pubmed
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.go_annotation ga ON f.feature_no = ga.feature_no
        JOIN {DB_SCHEMA}.go g ON ga.go_no = g.go_no
        JOIN {DB_SCHEMA}.go_ref gr ON ga.go_annotation_no = gr.go_annotation_no
        JOIN {DB_SCHEMA}.reference r ON gr.reference_no = r.reference_no
        ORDER BY f.feature_name, g.go_aspect, g.go_term, r.pubmed, ga.go_evidence
    """)

    # Group by feature, aspect, term, goid, pubmed - combine evidences
    go_data: dict[str, dict[str, list[str]]] = {}

    for row in session.execute(query).fetchall():
        feat_name, goid, term, aspect, evidence, pubmed = row

        if feat_name not in go_data:
            go_data[feat_name] = {}

        key = f"{aspect}\t{term}\t{goid}\t{pubmed}"
        if key not in go_data[feat_name]:
            go_data[feat_name][key] = []

        if evidence and evidence not in go_data[feat_name][key]:
            go_data[feat_name][key].append(evidence)

    # Format as output lines
    go_lines: dict[str, str] = {}
    for feat_name, keys in go_data.items():
        lines = []
        for key, evidences in sorted(keys.items()):
            aspect_code, term, goid, pubmed = key.split("\t")
            aspect = GO_ASPECT.get(aspect_code, aspect_code)
            goid_fmt = format_goid(int(goid)) if goid else ""
            evidence_str = ",".join(evidences)
            lines.append(
                f"\t\t\t{aspect}\t{term}|{goid_fmt}|{pubmed or ''}|{evidence_str}"
            )

        go_lines[feat_name] = "\n".join(lines) + "\n" if lines else ""

    return go_lines


def get_subfeature_info(session) -> tuple[dict[str, str], dict[str, str]]:
    """
    Get subfeature (exon) information for features.

    Returns tuple of (exons_dict, mutations_dict).
    """
    query = text(f"""
        SELECT UPPER(p.feature_name), sf.subfeature_type,
               fl.start_coord, fl.stop_coord, fl.strand
        FROM {DB_SCHEMA}.feature p
        JOIN {DB_SCHEMA}.feat_relationship fr ON p.feature_no = fr.parent_feature_no
        JOIN {DB_SCHEMA}.feature sf ON fr.child_feature_no = sf.feature_no
        JOIN {DB_SCHEMA}.feat_location fl ON sf.feature_no = fl.feature_no
        WHERE sf.feature_type IN ('CDS', 'intron', 'noncoding_exon', 'tRNA')
        ORDER BY p.feature_name, fl.start_coord
    """)

    # Get feature strand info
    strand_query = text(f"""
        SELECT UPPER(f.feature_name), fl.strand
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
    """)

    strands = {}
    for row in session.execute(strand_query).fetchall():
        strands[row[0]] = row[1]

    # Process subfeatures
    exons: dict[str, list[tuple]] = {}
    for row in session.execute(query).fetchall():
        feat_name, sf_type, start, stop, strand = row

        if sf_type in ("intron", "plus_1_translational_frameshift"):
            continue

        if feat_name not in exons:
            exons[feat_name] = []

        exons[feat_name].append((start, stop, sf_type))

    # Format exon lines
    exon_lines: dict[str, str] = {}
    for feat_name, coords in exons.items():
        # Sort by start coord (descending for Crick strand)
        feat_strand = strands.get(feat_name, "W")
        if feat_strand == "C":
            coords.sort(key=lambda x: x[0], reverse=True)
        else:
            coords.sort(key=lambda x: x[0])

        lines = []
        first = True
        for start, stop, sf_type in coords:
            show_type = TYPE_TO_SHOW.get(sf_type, sf_type)
            if first:
                lines.append(f"{start}\t{stop}\t{show_type}")
                first = False
            else:
                lines.append(f"{start}\t{stop}")

        exon_lines[feat_name] = "\n".join(lines) + "\n" if lines else ""

    # Hard-coded mutations (from original Perl)
    mutations = {
        "YER109C": "\t\t\ttransl_except\t(pos:377187..377185,aa:Trp)\n",
        "YOR031W": "\t\t\ttransl_except\t(pos:389237..389239,aa:Glu)\n",
    }

    return exon_lines, mutations


def get_feature_data(session) -> list[dict]:
    """
    Get main feature data for sequin output.

    Returns list of feature dictionaries ordered by chromosome and coordinates.
    """
    # Get feature types to include from web_metadata
    type_query = text(f"""
        SELECT col_value
        FROM {DB_SCHEMA}.web_metadata
        WHERE application_name = :app_name
        AND tab_name = 'FEATURE'
        AND col_name = 'FEATURE_TYPE'
    """)

    feature_types = [
        row[0] for row in session.execute(
            type_query, {"app_name": APPLICATION_NAME}
        ).fetchall()
    ]

    if not feature_types:
        # Default feature types if not in web_metadata
        feature_types = list(TYPE_TO_SHOW.keys())

    # Get features
    query = text(f"""
        SELECT p.feature_name as chrnum, f.feature_name, f.gene_name,
               fl.start_coord, fl.stop_coord, f.dbxref_id,
               f.headline, f.feature_type
        FROM {DB_SCHEMA}.feature p
        JOIN {DB_SCHEMA}.feat_relationship fr ON p.feature_no = fr.parent_feature_no
        JOIN {DB_SCHEMA}.feature f ON fr.child_feature_no = f.feature_no
        JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
        JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
        WHERE p.feature_type = 'chromosome'
        AND fr.rank = 1
        AND s.source = :seq_source
        ORDER BY p.feature_name, fl.start_coord, fl.stop_coord
    """)

    features = []
    for row in session.execute(query, {"seq_source": SEQ_SOURCE}).fetchall():
        (chrnum, feat_name, gene_name, start, stop,
         sgdid, headline, feat_type) = row

        try:
            chr_num = int(chrnum)
        except (ValueError, TypeError):
            continue

        features.append({
            "chrnum": chr_num,
            "feat_name": feat_name.upper() if feat_name else "",
            "gene_name": gene_name.upper() if gene_name else "",
            "start": start,
            "stop": stop,
            "sgdid": sgdid or "",
            "headline": headline or "",
            "feat_type": feat_type or "",
        })

    return features


def write_sequin_files(
    features: list[dict],
    qualifiers: dict[str, str],
    non_std_genes: set[str],
    aliases: dict[str, list[str]],
    ec_numbers: dict[str, str],
    gene_products: dict[str, str],
    go_annotations: dict[str, str],
    exons: dict[str, str],
    mutations: dict[str, str],
    ncbi_accessions: dict[int, str],
) -> None:
    """Write sequin table files."""
    # Create output directory
    SEQUIN_DIR.mkdir(parents=True, exist_ok=True)

    # Open output files
    files: dict[int, any] = {}
    for i in range(1, MAX_CHRNUM + 1):
        if i < 10:
            chr_str = f"0{i}"
        elif i < 17:
            chr_str = str(i)
        else:
            chr_str = "mt"

        outfile = SEQUIN_DIR / f"chr{chr_str}.tbl"
        fh = open(outfile, "w")

        # Write header
        acc = ncbi_accessions.get(i, "")
        fh.write(f">Feature ref|{acc}|\n")

        files[i] = fh

    # Track pseudogenes
    pseudo_features = set()
    pseudo_gene_lists: dict[str, list[str]] = {}
    pseudo_sgdid_lists: dict[str, list[str]] = {}
    pseudo_printed: set[str] = set()

    # First pass: identify pseudogenes
    for feat in features:
        feat_name = feat["feat_name"]
        gene_name = feat["gene_name"]
        feat_type = feat["feat_type"]

        qualifier = qualifiers.get(feat_name, "")

        if feat_type == "pseudogene" or "pseudogene" in qualifier.lower():
            pseudo_features.add(feat_name)

            if gene_name and gene_name != feat_name:
                if gene_name not in pseudo_gene_lists:
                    pseudo_gene_lists[gene_name] = []
                    pseudo_sgdid_lists[gene_name] = []

                pseudo_gene_lists[gene_name].append(feat_name)
                pseudo_sgdid_lists[gene_name].append(feat["sgdid"])

    # Second pass: write features
    for feat in features:
        chrnum = feat["chrnum"]
        feat_name = feat["feat_name"]
        gene_name = feat["gene_name"]
        start = feat["start"]
        stop = feat["stop"]
        sgdid = feat["sgdid"]
        headline = feat["headline"]
        feat_type = feat["feat_type"]

        if chrnum not in files:
            continue

        fh = files[chrnum]

        # Skip deleted/merged/dubious
        qualifier = qualifiers.get(feat_name, "")
        if any(x in qualifier.lower() for x in ["deleted", "merged", "dubious"]):
            continue

        # Handle non-standard gene names
        if gene_name in non_std_genes:
            gene_name = ""

        # For non-LTR/transposon features, use feature_name if no gene_name
        if not gene_name and feat_type not in ("long_terminal_repeat", "retrotransposon"):
            gene_name = feat_name

        # Clean up headline
        headline = headline.replace("Hypothetical protein", "hypothetical protein")

        # Get display type
        type_to_show = TYPE_TO_SHOW.get(feat_type, feat_type)

        # Is this a pseudogene?
        is_pseudo = feat_name in pseudo_features

        # Write feature
        if is_pseudo:
            if gene_name == feat_name:
                # Write pseudogene with its own name
                fh.write(f"{start}\t{stop}\t{type_to_show}\n")

                # Aliases
                if feat_name in aliases:
                    for alias in aliases[feat_name]:
                        fh.write(f"\t\t\tgene_syn\t{alias}\n")

                fh.write(f"\t\t\tlocus_tag\t{feat_name}\n")

            elif gene_name not in pseudo_printed:
                # First occurrence of this gene name in pseudogenes
                pseudo_printed.add(gene_name)
                fh.write(f"{start}\t{stop}\t{type_to_show}\n")
                fh.write(f"\t\t\t{type_to_show}\t{gene_name}\n")

                # Aliases
                if feat_name in aliases:
                    for alias in aliases[feat_name]:
                        fh.write(f"\t\t\tgene_syn\t{alias}\n")

                # Locus tags for all pseudogene members
                for member in pseudo_gene_lists.get(gene_name, []):
                    fh.write(f"\t\t\tlocus_tag\t{member}\n")

            else:
                continue  # Skip subsequent pseudogene members

        else:
            # Regular feature
            if feat_type == "transposable_element_gene":
                fh.write(f"{start}\t{stop}\tgene\n")
                fh.write(f"\t\t\tlocus_tag\t{gene_name}\n")
            else:
                fh.write(f"{start}\t{stop}\t{type_to_show}\n")

            if feat_type in ("ORF", "tRNA", "noncoding_exon"):
                tag_name = feat_name if gene_name == feat_name else feat_name

                if gene_name != feat_name:
                    fh.write(f"\t\t\t{type_to_show}\t{gene_name}\n")

                # Aliases
                if feat_name in aliases:
                    for alias in aliases[feat_name]:
                        fh.write(f"\t\t\tgene_syn\t{alias}\n")

                fh.write(f"\t\t\tlocus_tag\t{tag_name}\n")

            elif feat_type == "centromere":
                fh.write(f"\t\t\tnote\tCEN{chrnum}\n")

            elif feat_type.startswith("CDE"):
                fh.write(f"\t\t\tnote\t{feat_name} of CEN{chrnum}\n")

            else:
                if type_to_show == "repeat_region":
                    rpt_desc = RPT_TO_SHOW.get(feat_type, feat_type)
                    fh.write(f"\t\t\trpt_family\t{rpt_desc}\n")

                    if gene_name:
                        fh.write(f"\t\t\tgene\t{gene_name}\n")

                    if headline:
                        if feat_type == "retrotransposon":
                            headline = f"Transposon {feat_name}; {headline}"
                        fh.write(f"\t\t\tnote\t{headline}\n")

                elif type_to_show == "rep_origin":
                    fh.write(f"\t\t\tnote\t{gene_name}\n")

                elif gene_name and feat_type != "transposable_element_gene":
                    fh.write(f"\t\t\tproduct\t{gene_name}\n")

        # Write exons and mutations for ORF/tRNA/noncoding_exon
        if feat_type in ("ORF", "tRNA", "noncoding_exon", "transposable_element_gene") and not is_pseudo:
            # Exon info
            if feat_name in exons:
                fh.write(exons[feat_name])

            # Mutation info
            if feat_name in mutations:
                fh.write(mutations[feat_name])

            # Product(s)
            if feat_type in ("tRNA", "noncoding_exon"):
                fh.write(f"\t\t\tproduct\t{feat_name}\n")
                if feat_name in ec_numbers:
                    fh.write(f"\t\t\tEC_number\t{ec_numbers[feat_name]}\n")
            else:
                # ORF
                product = gene_products.get(feat_name) or headline
                if product:
                    product = delete_html_tag(product)
                    fh.write(f"\t\t\tproduct\t{product}\n")

                # Protein name from gene name (e.g., GDH3 -> Gdh3p)
                if gene_name and gene_name[0].isupper():
                    protein_name = gene_name[0].upper() + gene_name[1:].lower() + "p"
                    fh.write(f"\t\t\tproduct\t{protein_name}\n")

                if feat_name in ec_numbers:
                    fh.write(f"\t\t\tEC_number\t{ec_numbers[feat_name]}\n")

        # Note (for non-ORF features with different gene/feat names)
        note = None
        if feat_type not in ("ORF",) and gene_name != feat_name:
            note = feat_name

        if not is_pseudo and note and feat_type not in ("tRNA", "noncoding_exon"):
            fh.write(f"\t\t\tnote\t{note}\n")

        # Evidence
        if gene_name != feat_name or feat_type == "centromere":
            evidence = "experimental"
        else:
            evidence = "not_experimental"

        fh.write(f"\t\t\tevidence\t{evidence}\n")

        # Pseudo tag
        if is_pseudo:
            fh.write("\t\t\tpseudo\n")
            if note:
                fh.write(f"\t\t\tnote\t{note}\n")

        # GO annotations
        if feat_name in go_annotations and not is_pseudo:
            fh.write(go_annotations[feat_name])

        # dbxref
        if gene_name in pseudo_sgdid_lists:
            for psid in pseudo_sgdid_lists[gene_name]:
                fh.write(f"\t\t\tdb_xref\t{PROJECT_ACRONYM}:{psid}\n")
        else:
            fh.write(f"\t\t\tdb_xref\t{PROJECT_ACRONYM}:{sgdid}\n")

    # Close all files
    for fh in files.values():
        fh.close()

    logger.info(f"Created {len(files)} sequin table files in {SEQUIN_DIR}")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create NCBI Sequin table files"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    try:
        with SessionLocal() as session:
            # Load reference data
            logger.info("Loading NCBI accessions...")
            ncbi_accessions = get_ncbi_accessions(session)

            logger.info("Loading feature qualifiers...")
            qualifiers = get_feature_qualifiers(session)

            logger.info("Loading non-standard gene names...")
            non_std_genes = get_non_standard_genes(session)

            logger.info("Loading aliases...")
            aliases = get_aliases(session)

            logger.info("Loading EC numbers...")
            ec_numbers = get_ec_numbers(session)

            logger.info("Loading gene products...")
            gene_products = get_gene_products(session)

            logger.info("Loading GO annotations...")
            go_annotations = get_go_annotations(session)

            logger.info("Loading subfeature info...")
            exons, mutations = get_subfeature_info(session)

            logger.info("Loading feature data...")
            features = get_feature_data(session)
            logger.info(f"Found {len(features)} features")

            # Write output files
            logger.info("Writing sequin table files...")
            write_sequin_files(
                features,
                qualifiers,
                non_std_genes,
                aliases,
                ec_numbers,
                gene_products,
                go_annotations,
                exons,
                mutations,
                ncbi_accessions,
            )

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
