"""
CV Admin Service - Add and browse controlled-vocabulary terms.

Lets curators maintain the small, CGD-managed CVs (phenotype strain
backgrounds, literature topics, etc.) through the UI instead of direct SQL.
Large externally-managed ontologies (ChEBI, observable/APO, FAO) are
intentionally not editable here: their terms come from upstream releases.
"""

import logging
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from cgd.models.models import Cv, CvTerm, CvtermRelationship

logger = logging.getLogger(__name__)

# Relationship type used by the CGD-managed CVs (matches existing rows).
RELATIONSHIP_TYPE = "is a"

# CVs curators may edit through this tool. Featured CVs are surfaced first in
# the UI with a task-oriented label; the rest cover the rarer requests.
EDITABLE_CVS = {
    "strain_background": {
        "label": "Phenotype Strains",
        "description": (
            "Strain backgrounds offered by the Curate Phenotype tool. "
            "New strains usually go under a species group "
            "(e.g. C. albicans, C. glabrata)."
        ),
        "featured": True,
    },
    "literature_topic": {
        "label": "Literature Topics",
        "description": (
            "Topics offered by the Literature Guide curation tool. "
            "New topics usually go under a topic group "
            "(e.g. Gene Product Information, Other Topics)."
        ),
        "featured": True,
    },
    "curation_status": {
        "label": "Curation Statuses",
        "description": "Reference curation statuses used by the Lit Guide.",
        "featured": False,
    },
    "qualifier": {
        "label": "Phenotype Qualifiers",
        "description": "Qualifier values used in phenotype annotations.",
        "featured": False,
    },
    "mutant_type": {
        "label": "Mutant Types",
        "description": "Mutant types used in phenotype annotations.",
        "featured": False,
    },
    "experiment_type": {
        "label": "Experiment Types",
        "description": "Experiment types used in phenotype annotations.",
        "featured": False,
    },
    "virulence_model": {
        "label": "Virulence Models",
        "description": "Virulence/infection models used in phenotype annotations.",
        "featured": False,
    },
}


class CvAdminError(Exception):
    """Raised for validation or lookup failures in CV administration."""


class CvAdminService:
    def __init__(self, db: Session):
        self.db = db

    def _get_editable_cv(self, cv_name: str) -> Cv:
        if cv_name not in EDITABLE_CVS:
            raise CvAdminError(f"CV '{cv_name}' is not editable through this tool")
        cv = self.db.query(Cv).filter(Cv.cv_name == cv_name).first()
        if not cv:
            raise CvAdminError(f"CV '{cv_name}' not found in the database")
        return cv

    def list_cvs(self) -> list[dict]:
        """List editable CVs with term counts."""
        counts = dict(
            self.db.query(CvTerm.cv_no, func.count(CvTerm.cv_term_no))
            .group_by(CvTerm.cv_no)
            .all()
        )
        cvs = (
            self.db.query(Cv)
            .filter(Cv.cv_name.in_(EDITABLE_CVS.keys()))
            .all()
        )
        cv_by_name = {cv.cv_name: cv for cv in cvs}
        result = []
        for cv_name, meta in EDITABLE_CVS.items():
            cv = cv_by_name.get(cv_name)
            if not cv:
                continue
            result.append({
                "cv_no": cv.cv_no,
                "cv_name": cv_name,
                "label": meta["label"],
                "description": meta["description"],
                "featured": meta["featured"],
                "term_count": counts.get(cv.cv_no, 0),
            })
        return result

    def get_cv_terms(self, cv_name: str) -> dict:
        """
        Return all terms of a CV with their parent assignment.

        Terms without a parent are the top-level groups; the UI offers them
        (plus any term) as parents for new terms.
        """
        cv = self._get_editable_cv(cv_name)
        terms = (
            self.db.query(CvTerm)
            .filter(CvTerm.cv_no == cv.cv_no)
            .order_by(CvTerm.term_name)
            .all()
        )
        term_nos = {t.cv_term_no for t in terms}
        rels = (
            self.db.query(CvtermRelationship)
            .filter(CvtermRelationship.child_cv_term_no.in_(term_nos))
            .all()
        )
        # A term may in principle have several parents; keep the first.
        parent_of: dict[int, int] = {}
        for rel in rels:
            if rel.parent_cv_term_no in term_nos:
                parent_of.setdefault(rel.child_cv_term_no, rel.parent_cv_term_no)

        name_of = {t.cv_term_no: t.term_name for t in terms}
        out_terms = []
        for t in terms:
            parent_no = parent_of.get(t.cv_term_no)
            out_terms.append({
                "cv_term_no": t.cv_term_no,
                "term_name": t.term_name,
                "parent_cv_term_no": parent_no,
                "parent_term_name": name_of.get(parent_no),
                "date_created": t.date_created.isoformat() if t.date_created else None,
                "created_by": t.created_by,
            })
        return {
            "cv_no": cv.cv_no,
            "cv_name": cv_name,
            "terms": out_terms,
        }

    def add_term(
        self,
        cv_name: str,
        term_name: str,
        curator_userid: str,
        parent_cv_term_no: Optional[int] = None,
    ) -> dict:
        """
        Add a term to a CV, optionally under a parent term of the same CV.

        Mirrors the manual SQL recipe: INSERT into CV_TERM, then (if a parent
        was chosen) INSERT the 'is a' row into CVTERM_RELATIONSHIP.
        """
        cv = self._get_editable_cv(cv_name)

        term_name = (term_name or "").strip()
        if not term_name:
            raise CvAdminError("Term name must not be empty")
        if len(term_name) > 1024:
            raise CvAdminError("Term name exceeds 1024 characters")

        existing = (
            self.db.query(CvTerm)
            .filter(
                CvTerm.cv_no == cv.cv_no,
                func.upper(CvTerm.term_name) == term_name.upper(),
            )
            .first()
        )
        if existing:
            raise CvAdminError(
                f"Term '{existing.term_name}' already exists in this CV "
                f"(cv_term_no {existing.cv_term_no})"
            )

        parent = None
        if parent_cv_term_no is not None:
            parent = (
                self.db.query(CvTerm)
                .filter(CvTerm.cv_term_no == parent_cv_term_no)
                .first()
            )
            if not parent or parent.cv_no != cv.cv_no:
                raise CvAdminError(
                    "Parent term not found in this CV; pick a parent from the same vocabulary"
                )

        term = CvTerm(
            cv_no=cv.cv_no,
            term_name=term_name,
            created_by=curator_userid[:12].upper(),
        )
        self.db.add(term)
        self.db.flush()

        if parent is not None:
            rel = CvtermRelationship(
                child_cv_term_no=term.cv_term_no,
                parent_cv_term_no=parent.cv_term_no,
                relationship_type=RELATIONSHIP_TYPE,
                created_by=curator_userid[:12].upper(),
            )
            self.db.add(rel)

        self.db.commit()
        logger.info(
            "CV term added: cv=%s term='%s' cv_term_no=%s parent=%s by %s",
            cv_name, term_name, term.cv_term_no,
            parent.cv_term_no if parent else None, curator_userid,
        )
        return {
            "cv_term_no": term.cv_term_no,
            "term_name": term_name,
            "cv_name": cv_name,
            "parent_cv_term_no": parent.cv_term_no if parent else None,
            "parent_term_name": parent.term_name if parent else None,
        }
