"""
Curator Report Service - Canned database statistics for grant reporting.

Implements the SQL the curators previously ran by hand in sqlplus
(phenotype/GO annotation counts, curated-reference counts) as parameterized,
read-only reports. Every report takes an optional "since" date so the same
report answers both the all-time and the "since last grant period" questions.
"""

import logging
import re
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Known GO bulk-source references (from the curators' notebook). The report
# also accepts any other reference_no.
GO_SOURCE_REFERENCES = [
    {"reference_no": 49605, "label": "Orthology pipeline (reference_no 49605)"},
    {"reference_no": 58947, "label": "Protein domains pipeline (reference_no 58947)"},
]


class CuratorReportError(Exception):
    """Raised for invalid report ids or parameters."""


def _validate_since(since: Optional[str]) -> Optional[str]:
    if since in (None, ""):
        return None
    if not DATE_RE.match(since):
        raise CuratorReportError("since must be formatted YYYY-MM-DD")
    return since


class CuratorReportService:
    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Report registry
    # ------------------------------------------------------------------

    def get_definitions(self) -> list[dict]:
        """Report definitions used by the frontend to render param forms."""
        organisms = self._organism_options()
        return [
            {
                "id": "phenotype_annotations",
                "label": "Phenotype annotations by organism",
                "description": (
                    "Features with phenotype annotations and total phenotype "
                    "annotations per organism. Set a date to count only "
                    "annotations created since then."
                ),
                "params": [
                    {"name": "since", "label": "Created on or after", "type": "date", "required": False},
                ],
            },
            {
                "id": "go_annotations",
                "label": "GO annotations by organism",
                "description": (
                    "GO annotations (excluding ND placeholders) and annotated "
                    "genes (excluding orf19.* assembly-19 names) per organism. "
                    "Filter by annotation type, GO aspect, and/or date."
                ),
                "params": [
                    {
                        "name": "annotation_type", "label": "Annotation type", "type": "select",
                        "required": False,
                        "options": [
                            {"value": "", "label": "All types"},
                            {"value": "manually curated", "label": "Manually curated"},
                            {"value": "computational", "label": "Computational"},
                        ],
                    },
                    {
                        "name": "aspect", "label": "GO aspect", "type": "select", "required": False,
                        "options": [
                            {"value": "", "label": "All aspects"},
                            {"value": "P", "label": "Biological Process (P)"},
                            {"value": "F", "label": "Molecular Function (F)"},
                            {"value": "C", "label": "Cellular Component (C)"},
                        ],
                    },
                    {"name": "since", "label": "Created on or after", "type": "date", "required": False},
                ],
            },
            {
                "id": "go_by_source_reference",
                "label": "GO annotations from a bulk source (orthology, domains)",
                "description": (
                    "GO annotations attached to a given source reference, per "
                    "organism — e.g. the orthology or protein-domain pipelines."
                ),
                "params": [
                    {
                        "name": "reference_no", "label": "Source reference", "type": "select",
                        "required": True,
                        "options": [
                            {"value": str(r["reference_no"]), "label": r["label"]}
                            for r in GO_SOURCE_REFERENCES
                        ],
                        "allow_custom": True,
                    },
                    {"name": "since", "label": "Created on or after", "type": "date", "required": False},
                ],
            },
            {
                "id": "features_with_manual_go",
                "label": "Feature list: manual GO for one organism",
                "description": (
                    "The feature names with at least one manually curated, "
                    "non-ND GO annotation in the chosen organism "
                    "(orf19.* assembly-19 names excluded)."
                ),
                "params": [
                    {
                        "name": "organism", "label": "Organism", "type": "select", "required": True,
                        "options": organisms,
                    },
                ],
            },
            {
                "id": "curated_references",
                "label": "Curated references by organism",
                "description": (
                    "Unique PubMed IDs with feature-linked curation "
                    "(lit topics / curation status) per organism. Set a date "
                    "to count only references created since then."
                ),
                "params": [
                    {"name": "since", "label": "Reference created on or after", "type": "date", "required": False},
                ],
            },
            {
                "id": "curated_references_per_year",
                "label": "Curated references per publication year",
                "description": (
                    "Unique curated PubMed IDs grouped by publication year, "
                    "optionally for a single organism."
                ),
                "params": [
                    {
                        "name": "organism", "label": "Organism", "type": "select", "required": False,
                        "options": [{"value": "", "label": "All organisms"}] + organisms,
                    },
                    {"name": "since", "label": "Reference created on or after", "type": "date", "required": False},
                ],
            },
            {
                "id": "references_by_curation_status",
                "label": "References by curation status (full vs abstract)",
                "description": (
                    "Unique PubMed IDs grouped by curation status, e.g. "
                    "'Basic, lit guide, GO, Pheno curation done' (full) vs "
                    "'Done:Abstract curated, full text not curated'."
                ),
                "params": [
                    {"name": "since", "label": "Reference created on or after", "type": "date", "required": False},
                ],
            },
        ]

    def run(self, report_id: str, params: dict[str, Any]) -> dict:
        """Run a report and return {columns, rows}."""
        runners = {
            "phenotype_annotations": self._phenotype_annotations,
            "go_annotations": self._go_annotations,
            "go_by_source_reference": self._go_by_source_reference,
            "features_with_manual_go": self._features_with_manual_go,
            "curated_references": self._curated_references,
            "curated_references_per_year": self._curated_references_per_year,
            "references_by_curation_status": self._references_by_curation_status,
        }
        runner = runners.get(report_id)
        if not runner:
            raise CuratorReportError(f"Unknown report: {report_id}")
        return runner(params)

    # ------------------------------------------------------------------
    # Individual reports
    # ------------------------------------------------------------------

    def _organism_options(self) -> list[dict]:
        rows = self.db.execute(text(
            "SELECT DISTINCT o.organism_abbrev "
            "FROM MULTI.organism o JOIN MULTI.feature f ON f.organism_no = o.organism_no "
            "ORDER BY o.organism_abbrev"
        )).fetchall()
        return [{"value": r[0], "label": r[0]} for r in rows]

    def _phenotype_annotations(self, params: dict) -> dict:
        since = _validate_since(params.get("since"))
        rows = self.db.execute(text(
            "SELECT o.organism_abbrev, "
            "       COUNT(DISTINCT f.feature_no) AS features, "
            "       COUNT(DISTINCT pa.pheno_annotation_no) AS annotations "
            "FROM MULTI.pheno_annotation pa "
            "JOIN MULTI.feature f ON pa.feature_no = f.feature_no "
            "JOIN MULTI.organism o ON f.organism_no = o.organism_no "
            "WHERE (:since IS NULL OR pa.date_created >= TO_DATE(:since, 'YYYY-MM-DD')) "
            "GROUP BY o.organism_abbrev ORDER BY o.organism_abbrev"
        ), {"since": since}).fetchall()
        return {
            "columns": ["organism", "features_with_annotations", "phenotype_annotations"],
            "rows": [list(r) for r in rows],
        }

    def _go_annotations(self, params: dict) -> dict:
        since = _validate_since(params.get("since"))
        annotation_type = params.get("annotation_type") or None
        aspect = params.get("aspect") or None
        if aspect and aspect not in ("P", "F", "C"):
            raise CuratorReportError("aspect must be P, F, or C")
        rows = self.db.execute(text(
            "SELECT o.organism_abbrev, "
            "       COUNT(DISTINCT g.go_annotation_no) AS annotations, "
            "       COUNT(DISTINCT CASE WHEN f.feature_name NOT LIKE 'orf19%' "
            "                           THEN f.feature_no END) AS genes "
            "FROM MULTI.go_annotation g "
            "JOIN MULTI.feature f ON g.feature_no = f.feature_no "
            "JOIN MULTI.organism o ON f.organism_no = o.organism_no "
            "JOIN MULTI.go go ON go.go_no = g.go_no "
            "WHERE g.go_evidence != 'ND' "
            "  AND (:atype IS NULL OR g.annotation_type = :atype) "
            "  AND (:aspect IS NULL OR go.go_aspect = :aspect) "
            "  AND (:since IS NULL OR g.date_created >= TO_DATE(:since, 'YYYY-MM-DD')) "
            "GROUP BY o.organism_abbrev ORDER BY o.organism_abbrev"
        ), {"atype": annotation_type, "aspect": aspect, "since": since}).fetchall()
        return {
            "columns": ["organism", "go_annotations", "annotated_genes"],
            "rows": [list(r) for r in rows],
        }

    def _go_by_source_reference(self, params: dict) -> dict:
        since = _validate_since(params.get("since"))
        try:
            reference_no = int(params.get("reference_no"))
        except (TypeError, ValueError):
            raise CuratorReportError("reference_no must be an integer")
        rows = self.db.execute(text(
            "SELECT o.organism_abbrev, "
            "       COUNT(DISTINCT g.go_annotation_no) AS annotations "
            "FROM MULTI.go_annotation g "
            "JOIN MULTI.go_ref gr ON gr.go_annotation_no = g.go_annotation_no "
            "JOIN MULTI.feature f ON g.feature_no = f.feature_no "
            "JOIN MULTI.organism o ON f.organism_no = o.organism_no "
            "WHERE gr.reference_no = :refno "
            "  AND (:since IS NULL OR g.date_created >= TO_DATE(:since, 'YYYY-MM-DD')) "
            "GROUP BY o.organism_abbrev ORDER BY o.organism_abbrev"
        ), {"refno": reference_no, "since": since}).fetchall()
        return {
            "columns": ["organism", "go_annotations"],
            "rows": [list(r) for r in rows],
        }

    def _features_with_manual_go(self, params: dict) -> dict:
        organism = params.get("organism")
        if not organism:
            raise CuratorReportError("organism is required")
        rows = self.db.execute(text(
            "SELECT DISTINCT f.feature_name, f.gene_name "
            "FROM MULTI.go_annotation g "
            "JOIN MULTI.feature f ON g.feature_no = f.feature_no "
            "JOIN MULTI.organism o ON f.organism_no = o.organism_no "
            "WHERE g.annotation_type = 'manually curated' "
            "  AND g.go_evidence != 'ND' "
            "  AND f.feature_name NOT LIKE 'orf19%' "
            "  AND o.organism_abbrev = :organism "
            "ORDER BY f.feature_name"
        ), {"organism": organism}).fetchall()
        return {
            "columns": ["feature_name", "gene_name"],
            "rows": [list(r) for r in rows],
        }

    def _curated_references(self, params: dict) -> dict:
        since = _validate_since(params.get("since"))
        rows = self.db.execute(text(
            "SELECT o.organism_abbrev, COUNT(DISTINCT r.pubmed) AS refs "
            "FROM MULTI.feature f "
            "JOIN MULTI.refprop_feat rpf ON f.feature_no = rpf.feature_no "
            "JOIN MULTI.ref_property rp ON rpf.ref_property_no = rp.ref_property_no "
            "JOIN MULTI.organism o ON f.organism_no = o.organism_no "
            "JOIN MULTI.reference r ON r.reference_no = rp.reference_no "
            "WHERE (:since IS NULL OR r.date_created >= TO_DATE(:since, 'YYYY-MM-DD')) "
            "GROUP BY o.organism_abbrev ORDER BY o.organism_abbrev"
        ), {"since": since}).fetchall()
        return {
            "columns": ["organism", "curated_references"],
            "rows": [list(r) for r in rows],
        }

    def _curated_references_per_year(self, params: dict) -> dict:
        since = _validate_since(params.get("since"))
        organism = params.get("organism") or None
        rows = self.db.execute(text(
            "SELECT r.year, COUNT(DISTINCT r.pubmed) AS refs "
            "FROM MULTI.feature f "
            "JOIN MULTI.refprop_feat rpf ON f.feature_no = rpf.feature_no "
            "JOIN MULTI.ref_property rp ON rpf.ref_property_no = rp.ref_property_no "
            "JOIN MULTI.organism o ON f.organism_no = o.organism_no "
            "JOIN MULTI.reference r ON r.reference_no = rp.reference_no "
            "WHERE (:organism IS NULL OR o.organism_abbrev = :organism) "
            "  AND (:since IS NULL OR r.date_created >= TO_DATE(:since, 'YYYY-MM-DD')) "
            "GROUP BY r.year ORDER BY r.year"
        ), {"organism": organism, "since": since}).fetchall()
        return {
            "columns": ["publication_year", "curated_references"],
            "rows": [list(r) for r in rows],
        }

    def _references_by_curation_status(self, params: dict) -> dict:
        since = _validate_since(params.get("since"))
        rows = self.db.execute(text(
            "SELECT rp.property_value AS curation_status, "
            "       COUNT(DISTINCT r.pubmed) AS refs "
            "FROM MULTI.feature f "
            "JOIN MULTI.refprop_feat rpf ON f.feature_no = rpf.feature_no "
            "JOIN MULTI.ref_property rp ON rpf.ref_property_no = rp.ref_property_no "
            "JOIN MULTI.reference r ON r.reference_no = rp.reference_no "
            "WHERE rp.property_type = 'curation_status' "
            "  AND (:since IS NULL OR r.date_created >= TO_DATE(:since, 'YYYY-MM-DD')) "
            "GROUP BY rp.property_value ORDER BY refs DESC"
        ), {"since": since}).fetchall()
        return {
            "columns": ["curation_status", "references"],
            "rows": [list(r) for r in rows],
        }
