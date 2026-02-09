"""
Gene Registry Curation Service - Process gene registry submissions.

Handles curator workflow for reviewing and committing gene registry
submissions to the database.
"""

import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from cgd.models.models import (
    Feature,
    Organism,
    Colleague,
    GeneReservation,
    CollGeneres,
    Alias,
    FeatAlias,
    RefLink,
)

logger = logging.getLogger(__name__)


class GeneRegistryCurationError(Exception):
    """Custom exception for gene registry curation errors."""

    pass


class GeneRegistryCurationService:
    """Service for processing gene registry submissions."""

    # Source for new features
    SOURCE = "CGD"

    def __init__(self, db: Session):
        self.db = db

    def _get_submission_dir(self) -> Path:
        """Get submission directory."""
        env_dir = os.environ.get("CGD_SUBMISSION_DIR")
        if env_dir:
            return Path(env_dir)
        return Path("/tmp/cgd_submissions/colleague")

    def _get_archive_dir(self) -> Path:
        """Get archive directory for processed submissions."""
        submission_dir = self._get_submission_dir()
        archive_dir = submission_dir / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        return archive_dir

    def list_pending_submissions(self) -> list[dict]:
        """
        List all pending gene registry submissions.

        Returns list of submission summaries.
        """
        submission_dir = self._get_submission_dir()

        if not submission_dir.exists():
            return []

        submissions = []
        for filepath in submission_dir.glob("gene_registry_*.json"):
            try:
                with open(filepath) as f:
                    data = json.load(f)

                # Extract submission ID from filename
                submission_id = filepath.stem

                submissions.append({
                    "id": submission_id,
                    "filename": filepath.name,
                    "gene_name": data.get("gene_name"),
                    "orf_name": data.get("orf_name"),
                    "organism": data.get("organism"),
                    "submitted_at": data.get("submitted_at"),
                    "colleague_no": data.get("colleague_no"),
                    "submitter_name": self._get_submitter_name(data),
                })
            except Exception as e:
                logger.warning(f"Failed to read submission {filepath}: {e}")
                continue

        # Sort by submission time, newest first
        submissions.sort(
            key=lambda x: x.get("submitted_at", ""),
            reverse=True,
        )

        return submissions

    def _get_submitter_name(self, data: dict) -> str:
        """Get submitter name from submission data."""
        inner_data = data.get("data", {})
        if inner_data.get("first_name") and inner_data.get("last_name"):
            return f"{inner_data['last_name']}, {inner_data['first_name']}"
        return "Unknown"

    def get_submission_details(self, submission_id: str) -> Optional[dict]:
        """
        Get full details of a submission.

        Returns submission data with additional database lookups.
        """
        submission_dir = self._get_submission_dir()
        filepath = submission_dir / f"{submission_id}.json"

        if not filepath.exists():
            return None

        with open(filepath) as f:
            data = json.load(f)

        inner_data = data.get("data", {})
        organism_abbrev = data.get("organism") or inner_data.get("organism")
        orf_name = data.get("orf_name") or inner_data.get("orf_name")

        # Look up additional info from database
        result = {
            "id": submission_id,
            "filename": filepath.name,
            "submitted_at": data.get("submitted_at"),
            "gene_name": data.get("gene_name") or inner_data.get("gene_name"),
            "orf_name": orf_name,
            "organism": organism_abbrev,
            "colleague_no": data.get("colleague_no") or inner_data.get("colleague_no"),
            "data": inner_data,
        }

        # Look up ORF info if provided
        if orf_name and organism_abbrev:
            organism = (
                self.db.query(Organism)
                .filter(Organism.organism_abbrev == organism_abbrev)
                .first()
            )
            if organism:
                feature = (
                    self.db.query(Feature)
                    .filter(
                        func.upper(Feature.feature_name) == orf_name.upper(),
                        Feature.organism_no == organism.organism_no,
                    )
                    .first()
                )
                if feature:
                    result["orf_info"] = {
                        "feature_no": feature.feature_no,
                        "feature_name": feature.feature_name,
                        "gene_name": feature.gene_name,
                        "feature_type": feature.feature_type,
                        "headline": feature.headline,
                        "name_description": feature.name_description,
                    }

        # Look up colleague info
        colleague_no = result.get("colleague_no")
        if colleague_no:
            colleague = (
                self.db.query(Colleague)
                .filter(Colleague.colleague_no == colleague_no)
                .first()
            )
            if colleague:
                result["colleague_info"] = {
                    "colleague_no": colleague.colleague_no,
                    "name": f"{colleague.last_name}, {colleague.first_name}",
                    "email": colleague.email,
                    "institution": colleague.institution,
                }

        return result

    def process_submission(
        self,
        submission_id: str,
        curator_userid: str,
        gene_name: str,
        orf_name: Optional[str],
        organism_abbrev: str,
        description: Optional[str] = None,
        headline: Optional[str] = None,
        aliases: Optional[list[str]] = None,
        reference_no: Optional[int] = None,
    ) -> dict:
        """
        Process (commit) a gene registry submission.

        Creates/updates Feature, GeneReservation, and links.
        """
        # Get organism
        organism = (
            self.db.query(Organism)
            .filter(Organism.organism_abbrev == organism_abbrev)
            .first()
        )
        if not organism:
            raise GeneRegistryCurationError(
                f"Organism '{organism_abbrev}' not found"
            )

        # Get submission data to find colleague
        submission = self.get_submission_details(submission_id)
        if not submission:
            raise GeneRegistryCurationError(
                f"Submission '{submission_id}' not found"
            )

        # Find colleague
        colleague_no = submission.get("colleague_no")
        inner_data = submission.get("data", {})

        if not colleague_no:
            # Try to find colleague by name and email
            last_name = inner_data.get("last_name")
            first_name = inner_data.get("first_name")
            email = inner_data.get("email")

            if last_name and first_name and email:
                colleague = (
                    self.db.query(Colleague)
                    .filter(
                        func.upper(Colleague.last_name) == last_name.upper(),
                        func.upper(Colleague.first_name) == first_name.upper(),
                        func.lower(Colleague.email) == email.lower(),
                    )
                    .first()
                )
                if colleague:
                    colleague_no = colleague.colleague_no

        if not colleague_no:
            raise GeneRegistryCurationError(
                "Colleague must be in database before processing gene registry. "
                "Please process the colleague submission first."
            )

        # Get or create feature
        feature = self._update_or_create_feature(
            orf_name=orf_name or gene_name.upper(),
            gene_name=gene_name,
            organism_no=organism.organism_no,
            description=description,
            headline=headline,
            curator_userid=curator_userid,
        )

        # Create gene reservation
        gene_reservation_no = self._create_gene_reservation(
            feature_no=feature.feature_no,
            colleague_no=colleague_no,
            curator_userid=curator_userid,
        )

        # Create aliases if provided
        if aliases:
            self._create_aliases(
                feature_no=feature.feature_no,
                aliases=aliases,
                reference_no=reference_no,
            )

        # Link gene name to reference if provided
        if reference_no:
            self._link_to_reference(
                feature_no=feature.feature_no,
                reference_no=reference_no,
                description=description,
                headline=headline,
            )

        self.db.commit()

        # Archive the submission file
        self._archive_submission(submission_id)

        logger.info(
            f"Processed gene registry {submission_id}: "
            f"feature_no={feature.feature_no}, gene_reservation_no={gene_reservation_no}"
        )

        return {
            "feature_no": feature.feature_no,
            "feature_name": feature.feature_name,
            "gene_name": feature.gene_name,
            "gene_reservation_no": gene_reservation_no,
        }

    def _update_or_create_feature(
        self,
        orf_name: str,
        gene_name: str,
        organism_no: int,
        description: Optional[str],
        headline: Optional[str],
        curator_userid: str,
    ) -> Feature:
        """Update existing feature or create new one."""
        # Look for existing feature
        feature = (
            self.db.query(Feature)
            .filter(
                func.upper(Feature.feature_name) == orf_name.upper(),
                Feature.organism_no == organism_no,
            )
            .first()
        )

        if feature:
            # Update existing feature
            updated = False
            if gene_name and feature.gene_name != gene_name:
                feature.gene_name = gene_name
                updated = True
            if description and feature.name_description != description:
                feature.name_description = description
                updated = True
            if headline and feature.headline != headline:
                feature.headline = headline
                updated = True

            if updated:
                self.db.flush()
                logger.info(f"Updated feature {feature.feature_no}")

            return feature

        # Create new feature (for unmapped genes)
        feature = Feature(
            organism_no=organism_no,
            feature_name=orf_name.upper(),
            gene_name=gene_name,
            feature_type="not physically mapped",
            name_description=description,
            headline=headline,
            source=self.SOURCE,
            created_by=curator_userid,
        )
        self.db.add(feature)
        self.db.flush()

        logger.info(f"Created feature {feature.feature_no}: {orf_name}")
        return feature

    def _create_gene_reservation(
        self,
        feature_no: int,
        colleague_no: int,
        curator_userid: str,
    ) -> int:
        """Create gene reservation and link to colleague."""
        # Check if reservation already exists
        existing = (
            self.db.query(GeneReservation)
            .filter(GeneReservation.feature_no == feature_no)
            .first()
        )
        if existing:
            logger.info(
                f"Gene reservation already exists for feature {feature_no}"
            )
            return existing.gene_reservation_no

        # Create reservation (expires in 12 months)
        from sqlalchemy import text
        expiration_result = self.db.execute(
            text("SELECT ADD_MONTHS(SYSDATE, 12) FROM DUAL")
        ).fetchone()
        expiration_date = expiration_result[0] if expiration_result else None

        gene_reservation = GeneReservation(
            feature_no=feature_no,
            expiration_date=expiration_date,
        )
        self.db.add(gene_reservation)
        self.db.flush()

        logger.info(
            f"Created gene_reservation {gene_reservation.gene_reservation_no} "
            f"for feature {feature_no}"
        )

        # Link to colleague
        coll_generes = CollGeneres(
            colleague_no=colleague_no,
            gene_reservation_no=gene_reservation.gene_reservation_no,
        )
        self.db.add(coll_generes)
        self.db.flush()

        logger.info(
            f"Linked colleague {colleague_no} to gene_reservation "
            f"{gene_reservation.gene_reservation_no}"
        )

        return gene_reservation.gene_reservation_no

    def _create_aliases(
        self,
        feature_no: int,
        aliases: list[str],
        reference_no: Optional[int] = None,
    ) -> list[int]:
        """Create alias entries for the feature."""
        feat_alias_nos = []

        for alias_name in aliases:
            if not alias_name.strip():
                continue

            # Get or create alias
            alias = (
                self.db.query(Alias)
                .filter(
                    func.upper(Alias.alias_name) == alias_name.upper().strip(),
                    Alias.alias_type == "Uniform",
                )
                .first()
            )

            if not alias:
                alias = Alias(
                    alias_name=alias_name.upper().strip(),
                    alias_type="Uniform",
                )
                self.db.add(alias)
                self.db.flush()
                logger.info(f"Created alias: {alias_name}")

            # Link to feature
            existing_link = (
                self.db.query(FeatAlias)
                .filter(
                    FeatAlias.feature_no == feature_no,
                    FeatAlias.alias_no == alias.alias_no,
                )
                .first()
            )

            if not existing_link:
                feat_alias = FeatAlias(
                    feature_no=feature_no,
                    alias_no=alias.alias_no,
                )
                self.db.add(feat_alias)
                self.db.flush()
                feat_alias_nos.append(feat_alias.feat_alias_no)
                logger.info(
                    f"Linked alias {alias.alias_no} to feature {feature_no}"
                )

                # Link to reference if provided
                if reference_no:
                    ref_link = RefLink(
                        reference_no=reference_no,
                        tab_name="FEAT_ALIAS",
                        col_name="FEAT_ALIAS_NO",
                        primary_key=feat_alias.feat_alias_no,
                    )
                    self.db.add(ref_link)

        return feat_alias_nos

    def _link_to_reference(
        self,
        feature_no: int,
        reference_no: int,
        description: Optional[str],
        headline: Optional[str],
    ) -> None:
        """Link feature fields to reference."""
        # Link gene name
        existing = (
            self.db.query(RefLink)
            .filter(
                RefLink.reference_no == reference_no,
                RefLink.tab_name == "FEATURE",
                RefLink.primary_key == feature_no,
                RefLink.col_name == "GENE_NAME",
            )
            .first()
        )
        if not existing:
            ref_link = RefLink(
                reference_no=reference_no,
                tab_name="FEATURE",
                col_name="GENE_NAME",
                primary_key=feature_no,
            )
            self.db.add(ref_link)

        # Link description
        if description:
            existing = (
                self.db.query(RefLink)
                .filter(
                    RefLink.reference_no == reference_no,
                    RefLink.tab_name == "FEATURE",
                    RefLink.primary_key == feature_no,
                    RefLink.col_name == "NAME_DESCRIPTION",
                )
                .first()
            )
            if not existing:
                ref_link = RefLink(
                    reference_no=reference_no,
                    tab_name="FEATURE",
                    col_name="NAME_DESCRIPTION",
                    primary_key=feature_no,
                )
                self.db.add(ref_link)

        # Link headline
        if headline:
            existing = (
                self.db.query(RefLink)
                .filter(
                    RefLink.reference_no == reference_no,
                    RefLink.tab_name == "FEATURE",
                    RefLink.primary_key == feature_no,
                    RefLink.col_name == "HEADLINE",
                )
                .first()
            )
            if not existing:
                ref_link = RefLink(
                    reference_no=reference_no,
                    tab_name="FEATURE",
                    col_name="HEADLINE",
                    primary_key=feature_no,
                )
                self.db.add(ref_link)

    def _archive_submission(self, submission_id: str) -> None:
        """Move submission file to archive directory."""
        submission_dir = self._get_submission_dir()
        archive_dir = self._get_archive_dir()

        source = submission_dir / f"{submission_id}.json"
        if source.exists():
            dest = archive_dir / f"{submission_id}.json"
            shutil.move(str(source), str(dest))
            logger.info(f"Archived submission {submission_id}")

    def delay_submission(
        self,
        submission_id: str,
        comment: Optional[str] = None,
        curator_userid: Optional[str] = None,
    ) -> bool:
        """
        Mark a submission as delayed.

        Adds a delay marker to the submission file.
        """
        submission_dir = self._get_submission_dir()
        filepath = submission_dir / f"{submission_id}.json"

        if not filepath.exists():
            return False

        with open(filepath) as f:
            data = json.load(f)

        data["delayed"] = True
        data["delay_comment"] = comment
        data["delayed_by"] = curator_userid
        data["delayed_at"] = datetime.now().isoformat()

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2, default=str)

        logger.info(f"Delayed submission {submission_id}")
        return True

    def delete_submission(self, submission_id: str) -> bool:
        """
        Delete a submission.

        Moves file to archive with 'deleted' marker.
        """
        submission_dir = self._get_submission_dir()
        filepath = submission_dir / f"{submission_id}.json"

        if not filepath.exists():
            return False

        # Add deleted marker and archive
        with open(filepath) as f:
            data = json.load(f)

        data["deleted"] = True
        data["deleted_at"] = datetime.now().isoformat()

        archive_dir = self._get_archive_dir()
        dest = archive_dir / f"{submission_id}_deleted.json"

        with open(dest, "w") as f:
            json.dump(data, f, indent=2, default=str)

        filepath.unlink()
        logger.info(f"Deleted submission {submission_id}")
        return True
