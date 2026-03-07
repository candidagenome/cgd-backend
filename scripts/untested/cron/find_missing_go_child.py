#!/usr/bin/env python3
"""
Find GO annotations that could be updated to more specific terms.

This script identifies manually curated GO annotations where more specific
child terms have been added to the GO ontology since the annotation was
last reviewed. This helps curators identify annotations that may need
to be updated.

Based on findMissingGoChild.pl by CGD team.

Usage:
    python find_missing_go_child.py
    python find_missing_go_child.py --output go_updates_needed.html

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    HTML_ROOT_DIR: Root directory for reports
    CURATOR_EMAIL: Email for notifications
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

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
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
PROJECT_URL = os.getenv("PROJECT_URL", "http://www.candidagenome.org")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


@dataclass
class GoAnnotationCandidate:
    """A GO annotation that may need updating."""
    feature_no: int
    feature_name: str
    go_annotation_no: int
    go_no: int
    goid: str
    go_term: str
    go_evidence: str
    date_last_reviewed: datetime | None
    reference_nos: list[int]
    newer_child_terms: list[dict]


def format_goid(goid: int | str) -> str:
    """Format GO ID with proper padding."""
    return f"GO:{str(goid).zfill(7)}"


def get_manual_go_annotations(session) -> list[dict]:
    """Get all manually curated GO annotations."""
    # Exclude root GO terms (biological_process, molecular_function, cellular_component)
    root_go_nos = [24318, 32814, 39472]  # These are typical root GO term numbers

    query = text(f"""
        SELECT f.feature_no, f.feature_name, ga.go_annotation_no, ga.go_evidence,
               g.go_no, g.goid, g.go_term, ga.date_last_reviewed
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.go_annotation ga ON f.feature_no = ga.feature_no
        JOIN {DB_SCHEMA}.go g ON ga.go_no = g.go_no
        WHERE ga.annotation_type = 'manually curated'
        AND g.go_no NOT IN :root_go_nos
        ORDER BY f.feature_no
    """)

    annotations = []
    for row in session.execute(query, {"root_go_nos": tuple(root_go_nos)}).fetchall():
        annotations.append({
            "feature_no": row[0],
            "feature_name": row[1],
            "go_annotation_no": row[2],
            "go_evidence": row[3],
            "go_no": row[4],
            "goid": row[5],
            "go_term": row[6],
            "date_last_reviewed": row[7],
        })

    return annotations


def is_feature_deleted(session, feature_no: int) -> bool:
    """Check if a feature is marked as deleted."""
    query = text(f"""
        SELECT 1 FROM {DB_SCHEMA}.feat_property
        WHERE feature_no = :feature_no
        AND property_value LIKE 'Deleted%'
    """)
    result = session.execute(query, {"feature_no": feature_no}).fetchone()
    return result is not None


def get_newer_child_terms(
    session,
    go_no: int,
    go_annotation_no: int,
    date_last_reviewed: datetime | None
) -> list[dict]:
    """
    Find child GO terms that were created after the annotation was last reviewed.

    Returns list of child terms with their information.
    """
    if not date_last_reviewed:
        return []

    query = text(f"""
        SELECT DISTINCT gp.child_go_no, g.goid, g.go_term, g.date_created,
                        gp.ancestor_path, gp.generation
        FROM {DB_SCHEMA}.go_path gp
        JOIN {DB_SCHEMA}.go g ON gp.child_go_no = g.go_no
        WHERE gp.ancestor_go_no = :go_no
        AND g.date_created > :date_last_reviewed
        ORDER BY gp.ancestor_path
    """)

    children = []
    for row in session.execute(
        query, {"go_no": go_no, "date_last_reviewed": date_last_reviewed}
    ).fetchall():
        children.append({
            "go_no": row[0],
            "goid": row[1],
            "go_term": row[2],
            "date_created": row[3],
            "path": row[4],
            "generation": row[5],
        })

    return children


def get_references_for_annotation(session, go_annotation_no: int) -> list[int]:
    """Get reference numbers for a GO annotation."""
    query = text(f"""
        SELECT reference_no
        FROM {DB_SCHEMA}.go_ref
        WHERE go_annotation_no = :go_annotation_no
    """)
    return [row[0] for row in session.execute(
        query, {"go_annotation_no": go_annotation_no}
    ).fetchall()]


def get_reference_citation(session, reference_no: int) -> str:
    """Get a citation string for a reference."""
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


def find_annotations_needing_update(session) -> list[GoAnnotationCandidate]:
    """Find GO annotations that may need to be updated with more specific terms."""
    logger.info("Getting manual GO annotations")
    annotations = get_manual_go_annotations(session)
    logger.info(f"Found {len(annotations)} manual annotations")

    candidates = []
    processed = 0

    for annot in annotations:
        processed += 1
        if processed % 100 == 0:
            logger.info(f"Processed {processed}/{len(annotations)} annotations")

        # Skip deleted features
        if is_feature_deleted(session, annot["feature_no"]):
            continue

        # Find newer child terms
        newer_children = get_newer_child_terms(
            session,
            annot["go_no"],
            annot["go_annotation_no"],
            annot["date_last_reviewed"]
        )

        if newer_children:
            # Get references
            ref_nos = get_references_for_annotation(session, annot["go_annotation_no"])

            candidates.append(GoAnnotationCandidate(
                feature_no=annot["feature_no"],
                feature_name=annot["feature_name"],
                go_annotation_no=annot["go_annotation_no"],
                go_no=annot["go_no"],
                goid=annot["goid"],
                go_term=annot["go_term"],
                go_evidence=annot["go_evidence"],
                date_last_reviewed=annot["date_last_reviewed"],
                reference_nos=ref_nos,
                newer_child_terms=newer_children,
            ))

    return candidates


def generate_html_report(
    session,
    candidates: list[GoAnnotationCandidate],
    output_file: Path,
    sort_by: str = "feature"
):
    """Generate HTML report of annotations needing update."""

    # Sort candidates
    if sort_by == "reference":
        # Group by first reference
        def sort_key(c):
            return (c.reference_nos[0] if c.reference_nos else 0, c.feature_no, c.go_no)
    else:
        # Sort by feature
        def sort_key(c):
            return (c.feature_no, c.goid, c.go_annotation_no)

    candidates_sorted = sorted(candidates, key=sort_key)

    with open(output_file, "w") as f:
        f.write(f"""<!DOCTYPE html>
<html>
<head>
    <title>GO Updates Needed Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        tr:nth-child(even) {{ background-color: #f2f2f2; }}
        .toggle-btn {{ cursor: pointer; color: blue; text-decoration: underline; }}
        .child-terms {{ display: none; margin-left: 20px; }}
        .child-terms.show {{ display: block; }}
        .indent-0 {{ font-weight: bold; }}
        .indent-1 {{ margin-left: 10px; }}
        .indent-2 {{ margin-left: 20px; }}
        .indent-3 {{ margin-left: 30px; }}
    </style>
    <script>
        function toggleChildren(id) {{
            var elem = document.getElementById(id);
            elem.classList.toggle('show');
        }}
    </script>
</head>
<body>
    <h1>{PROJECT_ACRONYM} GO Updates Needed</h1>
    <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    <p>Found {len(candidates)} annotations that could potentially be updated with more specific GO terms.</p>

    <p>These annotations have been assigned to a GO term for which more specific child terms
    have been added to the ontology since the annotation was last reviewed.</p>

    <table>
        <tr>
            <th>Feature</th>
            <th>GO ID</th>
            <th>GO Term</th>
            <th>Evidence</th>
            <th>Last Reviewed</th>
            <th>Newer Child Terms</th>
            <th>References</th>
        </tr>
""")

        for i, cand in enumerate(candidates_sorted):
            bg = "#f2f2f2" if i % 2 == 0 else "#ffffff"
            child_id = f"children_{i}"

            # Format references
            refs_html = ""
            for ref_no in cand.reference_nos[:3]:  # Limit to 3 refs
                citation = get_reference_citation(session, ref_no)
                refs_html += f"<div>{citation}</div>"
            if len(cand.reference_nos) > 3:
                refs_html += f"<div>... and {len(cand.reference_nos) - 3} more</div>"

            # Format child terms
            children_html = f'<div id="{child_id}" class="child-terms"><ul>'
            for child in cand.newer_child_terms[:20]:  # Limit display
                indent = min(child["generation"], 3)
                children_html += f'<li class="indent-{indent}">{format_goid(child["goid"])} - {child["go_term"]}</li>'
            if len(cand.newer_child_terms) > 20:
                children_html += f'<li>... and {len(cand.newer_child_terms) - 20} more</li>'
            children_html += '</ul></div>'

            date_str = cand.date_last_reviewed.strftime("%Y-%m-%d") if cand.date_last_reviewed else "N/A"

            f.write(f"""        <tr style="background-color: {bg}">
            <td><a href="{PROJECT_URL}/locus/{cand.feature_name}">{cand.feature_name}</a></td>
            <td>{format_goid(cand.goid)}</td>
            <td>{cand.go_term}</td>
            <td>{cand.go_evidence}</td>
            <td>{date_str}</td>
            <td>
                <span class="toggle-btn" onclick="toggleChildren('{child_id}')">
                    {len(cand.newer_child_terms)} terms (click to expand)
                </span>
                {children_html}
            </td>
            <td>{refs_html}</td>
        </tr>
""")

        f.write("""    </table>
</body>
</html>
""")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Find GO annotations that could be updated to more specific terms"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=None,
        help="Output HTML report file",
    )
    parser.add_argument(
        "--sort-by",
        choices=["feature", "reference"],
        default="feature",
        help="Sort results by feature or reference (default: feature)",
    )

    args = parser.parse_args()

    # Determine output file
    if args.output:
        output_file = args.output
    else:
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
        reports_dir = HTML_ROOT_DIR / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        output_file = reports_dir / f"go_updates_needed-{timestamp}.html"

    logger.info("Finding GO annotations that may need updating")

    try:
        with SessionLocal() as session:
            candidates = find_annotations_needing_update(session)

            logger.info(f"Found {len(candidates)} annotations that could be more specific")

            # Generate HTML report
            generate_html_report(session, candidates, output_file, args.sort_by)
            logger.info(f"Report written to {output_file}")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
