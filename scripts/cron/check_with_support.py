#!/usr/bin/env python3
"""
Check GO annotations with supporting evidence for mismatches.

This script generates a report for curators indicating genes that have a
partner (WITH support) in another species but the GO terms don't match
in the external gene association file.

Based on checkWithSupport.pl by CGD team.

Usage:
    python check_with_support.py
    python check_with_support.py --output-dir /var/www/html/reports

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    HTML_ROOT_DIR: Root directory for reports
    DATA_DIR: Directory for downloaded files
"""

import argparse
import gzip
import logging
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.request import urlretrieve

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
PROJECT_URL = os.getenv("PROJECT_URL", "http://www.candidagenome.org")

# Gene association file sources
GAF_SOURCES = {
    "aspgd": "https://current.geneontology.org/annotations/aspgd.gaf.gz",
    "cgd": "https://current.geneontology.org/annotations/cgd.gaf.gz",
    "fb": "https://current.geneontology.org/annotations/fb.gaf.gz",
    "rgd": "https://current.geneontology.org/annotations/rgd.gaf.gz",
    "sgd": "https://current.geneontology.org/annotations/sgd.gaf.gz",
    "mgi": "https://current.geneontology.org/annotations/mgi.gaf.gz",
    "pombase": "https://current.geneontology.org/annotations/pombase.gaf.gz",
    "ebi": "https://current.geneontology.org/annotations/goa_uniprot_all_noiea.gaf.gz",
    "uniprotkb": "https://current.geneontology.org/annotations/goa_uniprot_all_noiea.gaf.gz",
}

# Evidence codes to check
CHECK_EVIDENCE_CODES = {"IPI", "IGI", "ISS", "ISM", "ISO", "ISA"}

# Sources to ignore
IGNORE_SOURCES = {"refseq", "genbank", "cgsc"}

# Source to URL mapping for external links
SOURCE_URLS = {
    "aspgd": "https://fungidb.org/fungidb/app/record/gene/",
    "cgd": "https://www.candidagenome.org/locus/",
    "pombase": "https://www.pombase.org/gene/",
    "sgd": "https://www.yeastgenome.org/locus/",
    "ebi": "https://www.ebi.ac.uk/QuickGO/GSearch?query=",
}

# Source to species name mapping
SOURCE_SPECIES = {
    "sgd": "S. cerevisiae",
    "pombase": "S. pombe",
    "aspgd": "A. nidulans",
    "cgd": "C. albicans",
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


@dataclass
class GOAnnotationWithSupport:
    """A GO annotation with supporting evidence from another species."""
    feature_no: int
    feature_name: str
    gene_name: str | None
    feature_dbxref: str
    go_annotation_no: int
    go_evidence: str
    source: str
    dbxref_id: str
    pubmed: str | None
    goid: str
    go_aspect: str
    go_term: str
    organism_abbrev: str
    reference_no: int


def get_go_annotations_with_support(session) -> list[GOAnnotationWithSupport]:
    """Get GO annotations that have supporting evidence from external sources."""
    query = text(f"""
        SELECT DISTINCT f.feature_no, f.feature_name, f.gene_name, f.dbxref_id,
               g.go_annotation_no, g.go_evidence,
               d.source, d.dbxref_id as support_dbxref,
               r.pubmed, go.goid, go.go_aspect, go.go_term,
               o.organism_abbrev, r.reference_no
        FROM {DB_SCHEMA}.go_annotation g
        JOIN {DB_SCHEMA}.go_ref gr ON g.go_annotation_no = gr.go_annotation_no
        JOIN {DB_SCHEMA}.goref_dbxref gd ON gr.go_ref_no = gd.go_ref_no
        JOIN {DB_SCHEMA}.dbxref d ON gd.dbxref_no = d.dbxref_no
        JOIN {DB_SCHEMA}.reference r ON gr.reference_no = r.reference_no
        JOIN {DB_SCHEMA}.go go ON g.go_no = go.go_no
        JOIN {DB_SCHEMA}.feature f ON g.feature_no = f.feature_no
        JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
        WHERE gr.has_supporting_evidence = 'Y'
        AND g.go_evidence IN :evidence_codes
        ORDER BY d.source
    """)

    annotations = []
    for row in session.execute(
        query, {"evidence_codes": tuple(CHECK_EVIDENCE_CODES)}
    ).fetchall():
        goid = f"GO:{row[9]:07d}" if isinstance(row[9], int) else row[9]
        annotations.append(GOAnnotationWithSupport(
            feature_no=row[0],
            feature_name=row[1],
            gene_name=row[2],
            feature_dbxref=row[3],
            go_annotation_no=row[4],
            go_evidence=row[5],
            source=row[6],
            dbxref_id=row[7],
            pubmed=row[8],
            goid=goid,
            go_aspect=row[10],
            go_term=row[11],
            organism_abbrev=row[12],
            reference_no=row[13],
        ))

    return annotations


def get_go_term(session, goid: str) -> str | None:
    """Get GO term name for a GO ID."""
    # Extract numeric part
    if goid.startswith("GO:"):
        goid_num = int(goid[3:])
    else:
        goid_num = int(goid)

    query = text(f"""
        SELECT go_term FROM {DB_SCHEMA}.go WHERE goid = :goid
    """)
    result = session.execute(query, {"goid": goid_num}).fetchone()
    return result[0] if result else None


def get_reference_citation(session, reference_no: int) -> str:
    """Get formatted citation for a reference."""
    query = text(f"""
        SELECT r.citation, r.pubmed
        FROM {DB_SCHEMA}.reference r
        WHERE r.reference_no = :reference_no
    """)
    result = session.execute(query, {"reference_no": reference_no}).fetchone()
    if result:
        citation = result[0] or ""
        pubmed = result[1]
        if pubmed:
            citation += f" (PMID: {pubmed})"
        return citation
    return f"Reference {reference_no}"


def download_gene_association(source: str, output_dir: Path) -> Path | None:
    """Download gene association file for a source."""
    source_lower = source.lower()

    # Handle source name variations
    if source_lower == "genedb_spombe":
        source_lower = "pombase"

    url = GAF_SOURCES.get(source_lower)
    if not url:
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    local_gz = output_dir / f"gene_association.{source_lower}.gz"
    local_file = output_dir / f"gene_association.{source_lower}"

    try:
        logger.info(f"Downloading {url}")
        urlretrieve(url, local_gz)

        # Decompress
        with gzip.open(local_gz, "rb") as f_in:
            with open(local_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        local_gz.unlink()
        return local_file

    except Exception as e:
        logger.error(f"Error downloading {source}: {e}")
        return None


def read_gene_association(
    gaf_file: Path,
) -> dict[str, dict[str, dict[str, str]]]:
    """
    Read gene association file into nested dict.

    Returns: dbxref_id -> aspect -> goid -> line
    """
    associations: dict[str, dict[str, dict[str, str]]] = {}

    with open(gaf_file) as f:
        for line in f:
            if line.startswith("!") or not line.strip():
                continue

            parts = line.strip().split("\t")
            if len(parts) < 15:
                continue

            dbxref_id = parts[1]
            qualifier = parts[3]
            goid = parts[4]
            aspect = parts[8]

            # Skip NOT annotations
            if qualifier == "NOT":
                continue

            if dbxref_id not in associations:
                associations[dbxref_id] = {}
            if aspect not in associations[dbxref_id]:
                associations[dbxref_id][aspect] = {}

            associations[dbxref_id][aspect][goid] = line

    return associations


def format_species_name(source: str) -> str:
    """Format source into a user-readable species name."""
    return SOURCE_SPECIES.get(source.lower(), source)


def generate_html_report(
    session,
    annotations: list[GOAnnotationWithSupport],
    output_file: Path,
):
    """Generate HTML report of mismatched GO annotations."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Cache for downloaded gene association files
    gaf_cache: dict[str, dict[str, dict[str, dict[str, str]]]] = {}
    download_dir = DATA_DIR / "go_check"

    mismatches = []
    count = 0

    for annot in annotations:
        source = annot.source.lower()

        # Skip ignored sources
        if source in IGNORE_SOURCES:
            continue

        # Download/cache gene association file
        if source not in gaf_cache:
            gaf_file = download_gene_association(annot.source, download_dir)
            if gaf_file:
                gaf_cache[source] = read_gene_association(gaf_file)
            else:
                gaf_cache[source] = {}

        source_assoc = gaf_cache.get(source, {})

        # Check if GO ID matches
        dbxref_assoc = source_assoc.get(annot.dbxref_id, {})
        aspect_assoc = dbxref_assoc.get(annot.go_aspect, {})

        if annot.goid in aspect_assoc:
            # Match found - skip
            continue

        # No match - collect mismatch info
        mismatch_terms = []
        for goid in aspect_assoc:
            term = get_go_term(session, goid)
            mismatch_terms.append((goid, term or "Unknown"))

        mismatches.append({
            "annotation": annot,
            "mismatch_terms": mismatch_terms,
            "not_found": len(aspect_assoc) == 0,
        })
        count += 1

    # Generate HTML
    with open(output_file, "w") as f:
        f.write(f"""<!DOCTYPE html>
<html>
<head>
    <title>GO WITH Support Mismatch Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        tr:nth-child(even) {{ background-color: #f2f2f2; }}
        .mismatch-table {{ width: 100%; border: 1px solid #ccc; }}
    </style>
</head>
<body>
    <h1>{PROJECT_ACRONYM} Gene Partner Mismatch Report</h1>
    <p>Generated: {timestamp}</p>
    <p>This report shows GO annotations where the supporting evidence (WITH field)
    references a gene in another species, but the GO term doesn't match what's
    in that species' gene association file.</p>

    <table>
        <tr>
            <th>Feature Name</th>
            <th>GO ID</th>
            <th>GO Term</th>
            <th>Aspect</th>
            <th>Evidence</th>
            <th>Citation</th>
            <th>Source</th>
            <th>Dbxref</th>
            <th>Reference Terms</th>
        </tr>
""")

        for i, mismatch in enumerate(mismatches):
            annot = mismatch["annotation"]
            bg_color = "#cccccc" if i % 2 else "#ffffff"

            # Format feature name with link
            name = annot.gene_name or annot.feature_name
            if annot.gene_name:
                name = f"{annot.gene_name}/{annot.feature_name}"
            name += f" ({annot.organism_abbrev})"
            feature_link = f'<a href="{PROJECT_URL}/locus/{annot.feature_name}">{name}</a>'

            # Format citation
            citation = get_reference_citation(session, annot.reference_no)

            # Format external link
            source_url = SOURCE_URLS.get(annot.source.lower(), "")
            species = format_species_name(annot.source)
            if source_url:
                xref_link = f'<a href="{source_url}{annot.dbxref_id}">{annot.dbxref_id}</a> (<i>{species}</i>)'
            else:
                xref_link = f"{annot.dbxref_id} (<i>{species}</i>)"

            # Format mismatch terms
            if mismatch["not_found"]:
                ref_terms = "not found in gene_association file"
            elif mismatch["mismatch_terms"]:
                ref_terms = '<table class="mismatch-table">'
                for goid, term in mismatch["mismatch_terms"]:
                    ref_terms += f"<tr><td width='30%'>{goid}</td><td>{term}</td></tr>"
                ref_terms += "</table>"
            else:
                ref_terms = ""

            f.write(f"""        <tr style="background-color: {bg_color}">
            <td>{feature_link}</td>
            <td>{annot.goid}</td>
            <td>{annot.go_term}</td>
            <td>{annot.go_aspect}</td>
            <td>{annot.go_evidence}</td>
            <td>{citation}</td>
            <td>{annot.source}</td>
            <td>{xref_link}</td>
            <td>{ref_terms}</td>
        </tr>
""")

        f.write(f"""    </table>
    <p>{count} rows</p>
</body>
</html>
""")

    return count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Check GO annotations with supporting evidence for mismatches"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for reports",
    )

    args = parser.parse_args()

    output_dir = args.output_dir or (HTML_ROOT_DIR / "reports")
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    output_file = output_dir / f"checkWith-{timestamp}.html"

    logger.info("Checking GO annotations with supporting evidence")

    try:
        with SessionLocal() as session:
            # Get annotations with supporting evidence
            annotations = get_go_annotations_with_support(session)
            logger.info(f"Found {len(annotations)} annotations with supporting evidence")

            # Generate report
            count = generate_html_report(session, annotations, output_file)
            logger.info(f"Report written to {output_file}")
            logger.info(f"Found {count} mismatches")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
