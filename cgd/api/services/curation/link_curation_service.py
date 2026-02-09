"""
Link Curation Service - Manage links and pull-downs for Locus page.

Handles adding and removing URL links that appear on the Locus page for features.
"""

import logging
from typing import Optional

from sqlalchemy import func, and_
from sqlalchemy.orm import Session

from cgd.models.models import (
    Feature,
    FeatUrl,
    Url,
    WebDisplay,
    Dbxref,
    DbxrefUrl,
    DbxrefFeat,
    Organism,
)

logger = logging.getLogger(__name__)


class LinkCurationError(Exception):
    """Custom exception for link curation errors."""

    pass


class LinkCurationService:
    """Service for managing feature links and pull-downs."""

    # Web page names that links apply to
    WEB_PAGE_NAMES = ["Locus", "Protein", "Phenotype"]

    def __init__(self, db: Session):
        self.db = db

    def get_feature_info(
        self, feature_name: str, organism_abbrev: str
    ) -> Optional[dict]:
        """
        Get feature info for link management.

        Returns feature info or None if not found.
        """
        feature = (
            self.db.query(Feature)
            .join(Organism, Feature.organism_no == Organism.organism_no)
            .filter(
                func.upper(Organism.organism_abbrev) == organism_abbrev.upper(),
            )
            .filter(
                (func.upper(Feature.feature_name) == feature_name.upper())
                | (func.upper(Feature.gene_name) == feature_name.upper())
            )
            .first()
        )

        if not feature:
            return None

        return {
            "feature_no": feature.feature_no,
            "feature_name": feature.feature_name,
            "gene_name": feature.gene_name,
            "feature_type": feature.feature_type,
            "dbxref_id": feature.dbxref_id,
        }

    def get_available_links(self, feature_type: str) -> list[dict]:
        """
        Get available link types for a feature type.

        Returns list of link definitions from web_display table.
        """
        # Get all web_display entries for Locus/Protein/Phenotype pages
        web_displays = (
            self.db.query(WebDisplay, Url)
            .join(Url, WebDisplay.url_no == Url.url_no)
            .filter(
                WebDisplay.web_page_name.in_(self.WEB_PAGE_NAMES)
            )
            .order_by(WebDisplay.label_location, WebDisplay.label_name)
            .all()
        )

        # Count how many features of this type have each link
        feat_url_counts = self._get_feat_url_counts(feature_type)
        dbxref_url_counts = self._get_dbxref_url_counts(feature_type)

        # Total features of this type
        total_features = (
            self.db.query(func.count(Feature.feature_no))
            .filter(Feature.feature_type == feature_type)
            .scalar()
        ) or 0

        links = []
        for wd, url in web_displays:
            # Determine if this link uses FEAT_URL or DBXREF_URL
            feat_count = feat_url_counts.get(url.url_no, 0)
            dbxref_count = dbxref_url_counts.get(url.url_no, 0)

            link_table = "FEAT_URL" if feat_count > 0 else "DBXREF_URL"
            count = feat_count or dbxref_count

            # Skip links with zero usage
            if count == 0:
                continue

            # Determine if "common to all" or "common to some"
            is_common_to_all = (total_features - count) <= 5  # MAX_MISSING_LINK_NUM

            links.append({
                "url_no": url.url_no,
                "url": url.url,
                "label_name": wd.label_name,
                "label_location": wd.label_location,
                "label_type": wd.label_type,
                "link_table": link_table,
                "usage_count": count,
                "is_common_to_all": is_common_to_all,
            })

        return links

    def get_feature_links(self, feature_no: int) -> list[dict]:
        """
        Get currently selected links for a feature.

        Returns list of url_no values that are linked to this feature.
        """
        # Get FEAT_URL links
        feat_urls = (
            self.db.query(FeatUrl.url_no)
            .filter(FeatUrl.feature_no == feature_no)
            .all()
        )

        # Get DBXREF_URL links via the feature's dbxref
        feature = (
            self.db.query(Feature)
            .filter(Feature.feature_no == feature_no)
            .first()
        )

        dbxref_urls = []
        if feature and feature.dbxref_id:
            # Find the feature's dbxref entry
            dbxrefs = (
                self.db.query(Dbxref)
                .filter(Dbxref.dbxref_id == feature.dbxref_id)
                .all()
            )

            for dbxref in dbxrefs:
                urls = (
                    self.db.query(DbxrefUrl.url_no)
                    .filter(DbxrefUrl.dbxref_no == dbxref.dbxref_no)
                    .all()
                )
                dbxref_urls.extend(urls)

        all_urls = [{"url_no": u[0], "link_table": "FEAT_URL"} for u in feat_urls]
        all_urls.extend([{"url_no": u[0], "link_table": "DBXREF_URL"} for u in dbxref_urls])

        return all_urls

    def update_feature_links(
        self,
        feature_no: int,
        selected_links: list[dict],
        curator_userid: str,
    ) -> dict:
        """
        Update links for a feature.

        selected_links is a list of dicts with url_no and link_table keys.
        """
        feature = (
            self.db.query(Feature)
            .filter(Feature.feature_no == feature_no)
            .first()
        )

        if not feature:
            raise LinkCurationError(f"Feature {feature_no} not found")

        # Get current links
        current_links = self.get_feature_links(feature_no)
        current_url_nos = {link["url_no"] for link in current_links}
        selected_url_nos = {link["url_no"] for link in selected_links}

        # Determine what to add and remove
        to_add = selected_url_nos - current_url_nos
        to_remove = current_url_nos - selected_url_nos

        added = 0
        removed = 0

        # Add new links
        for link in selected_links:
            if link["url_no"] in to_add:
                if link.get("link_table") == "FEAT_URL":
                    self._add_feat_url(feature_no, link["url_no"])
                else:
                    self._add_dbxref_url(feature, link["url_no"])
                added += 1

        # Remove old links
        for link in current_links:
            if link["url_no"] in to_remove:
                if link["link_table"] == "FEAT_URL":
                    self._remove_feat_url(feature_no, link["url_no"])
                else:
                    self._remove_dbxref_url(feature, link["url_no"])
                removed += 1

        self.db.commit()

        logger.info(
            f"Updated links for feature {feature_no}: added {added}, removed {removed}"
        )

        return {
            "added": added,
            "removed": removed,
            "total": len(selected_links),
        }

    def _get_feat_url_counts(self, feature_type: str) -> dict[int, int]:
        """Get count of features using each URL via FEAT_URL for a feature type."""
        results = (
            self.db.query(FeatUrl.url_no, func.count(FeatUrl.feature_no))
            .join(Feature, FeatUrl.feature_no == Feature.feature_no)
            .filter(Feature.feature_type == feature_type)
            .group_by(FeatUrl.url_no)
            .all()
        )
        return {url_no: count for url_no, count in results}

    def _get_dbxref_url_counts(self, feature_type: str) -> dict[int, int]:
        """Get count of features using each URL via DBXREF_URL for a feature type."""
        # This is more complex - need to go through dbxref_feat
        results = (
            self.db.query(DbxrefUrl.url_no, func.count(DbxrefFeat.feature_no))
            .join(Dbxref, DbxrefUrl.dbxref_no == Dbxref.dbxref_no)
            .join(DbxrefFeat, Dbxref.dbxref_no == DbxrefFeat.dbxref_no)
            .join(Feature, DbxrefFeat.feature_no == Feature.feature_no)
            .filter(Feature.feature_type == feature_type)
            .group_by(DbxrefUrl.url_no)
            .all()
        )
        return {url_no: count for url_no, count in results}

    def _add_feat_url(self, feature_no: int, url_no: int) -> None:
        """Add a FEAT_URL entry."""
        existing = (
            self.db.query(FeatUrl)
            .filter(
                FeatUrl.feature_no == feature_no,
                FeatUrl.url_no == url_no,
            )
            .first()
        )

        if existing:
            return

        feat_url = FeatUrl(
            feature_no=feature_no,
            url_no=url_no,
        )
        self.db.add(feat_url)
        self.db.flush()

        logger.info(f"Added feat_url: feature_no={feature_no}, url_no={url_no}")

    def _remove_feat_url(self, feature_no: int, url_no: int) -> None:
        """Remove a FEAT_URL entry."""
        feat_url = (
            self.db.query(FeatUrl)
            .filter(
                FeatUrl.feature_no == feature_no,
                FeatUrl.url_no == url_no,
            )
            .first()
        )

        if feat_url:
            self.db.delete(feat_url)
            self.db.flush()
            logger.info(f"Removed feat_url: feature_no={feature_no}, url_no={url_no}")

    def _add_dbxref_url(self, feature: Feature, url_no: int) -> None:
        """Add a DBXREF_URL entry for a feature."""
        if not feature.dbxref_id:
            logger.warning(
                f"Cannot add dbxref_url: feature {feature.feature_no} has no dbxref_id"
            )
            return

        # Find the feature's dbxref
        dbxref = (
            self.db.query(Dbxref)
            .filter(Dbxref.dbxref_id == feature.dbxref_id)
            .first()
        )

        if not dbxref:
            logger.warning(
                f"Cannot add dbxref_url: dbxref_id {feature.dbxref_id} not found"
            )
            return

        existing = (
            self.db.query(DbxrefUrl)
            .filter(
                DbxrefUrl.dbxref_no == dbxref.dbxref_no,
                DbxrefUrl.url_no == url_no,
            )
            .first()
        )

        if existing:
            return

        dbxref_url = DbxrefUrl(
            dbxref_no=dbxref.dbxref_no,
            url_no=url_no,
        )
        self.db.add(dbxref_url)
        self.db.flush()

        logger.info(
            f"Added dbxref_url: dbxref_no={dbxref.dbxref_no}, url_no={url_no}"
        )

    def _remove_dbxref_url(self, feature: Feature, url_no: int) -> None:
        """Remove a DBXREF_URL entry for a feature."""
        if not feature.dbxref_id:
            return

        dbxref = (
            self.db.query(Dbxref)
            .filter(Dbxref.dbxref_id == feature.dbxref_id)
            .first()
        )

        if not dbxref:
            return

        dbxref_url = (
            self.db.query(DbxrefUrl)
            .filter(
                DbxrefUrl.dbxref_no == dbxref.dbxref_no,
                DbxrefUrl.url_no == url_no,
            )
            .first()
        )

        if dbxref_url:
            self.db.delete(dbxref_url)
            self.db.flush()
            logger.info(
                f"Removed dbxref_url: dbxref_no={dbxref.dbxref_no}, url_no={url_no}"
            )
