"""
Colleague Curation Service - Business logic for colleague CRUD operations.

Mirrors functionality from legacy NewColleague.pm and UpdateColleague.pm:
- Process pending colleague submissions
- Create/update colleague records
- Manage colleague relationships (PI, associates)
- Manage colleague URLs, keywords, and features
"""

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from cgd.models.models import (
    Colleague,
    ColleagueRemark,
    CollUrl,
    CollKw,
    CollFeat,
    CollRelationship,
    Keyword,
    Url,
    Feature,
)

logger = logging.getLogger(__name__)

SOURCE = "CGD"


class ColleagueCurationError(Exception):
    """Raised when colleague curation validation fails."""

    pass


class ColleagueCurationService:
    """Service for colleague curation operations."""

    def __init__(self, db: Session):
        self.db = db

    def get_colleague_by_id(self, colleague_no: int) -> Optional[Colleague]:
        """Get colleague by ID."""
        return (
            self.db.query(Colleague)
            .filter(Colleague.colleague_no == colleague_no)
            .first()
        )

    def search_colleagues_by_name(
        self, first_name: str, last_name: str
    ) -> list[Colleague]:
        """Search colleagues by first and last name."""
        return (
            self.db.query(Colleague)
            .filter(
                func.upper(Colleague.first_name) == first_name.upper(),
                func.upper(Colleague.last_name) == last_name.upper(),
            )
            .all()
        )

    def get_all_colleagues(
        self, page: int = 1, page_size: int = 50
    ) -> tuple[list[dict], int]:
        """
        Get paginated list of all colleagues.

        Returns:
            Tuple of (list of colleague dicts, total count)
        """
        query = self.db.query(Colleague).order_by(
            Colleague.last_name, Colleague.first_name
        )

        total = query.count()
        colleagues = query.offset((page - 1) * page_size).limit(page_size).all()

        return (
            [
                {
                    "colleague_no": c.colleague_no,
                    "first_name": c.first_name,
                    "last_name": c.last_name,
                    "email": c.email,
                    "institution": c.institution,
                    "is_pi": c.is_pi,
                    "is_contact": c.is_contact,
                    "date_created": c.date_created.isoformat() if c.date_created else None,
                    "date_modified": c.date_modified.isoformat() if c.date_modified else None,
                }
                for c in colleagues
            ],
            total,
        )

    def get_colleague_details(self, colleague_no: int) -> dict:
        """
        Get detailed colleague info for curation.

        Returns all colleague fields plus relationships, URLs, keywords.
        """
        colleague = self.get_colleague_by_id(colleague_no)
        if not colleague:
            raise ColleagueCurationError(f"Colleague {colleague_no} not found")

        # Get URLs
        urls = []
        for coll_url in colleague.coll_url:
            url = self.db.query(Url).filter(Url.url_no == coll_url.url_no).first()
            if url:
                urls.append({
                    "coll_url_no": coll_url.coll_url_no,
                    "url_no": url.url_no,
                    "url_type": url.url_type,
                    "link": url.link,
                })

        # Get keywords
        keywords = []
        for coll_kw in colleague.coll_kw:
            kw = self.db.query(Keyword).filter(
                Keyword.keyword_no == coll_kw.keyword_no
            ).first()
            if kw:
                keywords.append({
                    "coll_kw_no": coll_kw.coll_kw_no,
                    "keyword_no": kw.keyword_no,
                    "keyword": kw.keyword,
                })

        # Get associated features
        features = []
        for coll_feat in colleague.coll_feat:
            feat = self.db.query(Feature).filter(
                Feature.feature_no == coll_feat.feature_no
            ).first()
            if feat:
                features.append({
                    "coll_feat_no": coll_feat.coll_feat_no,
                    "feature_no": feat.feature_no,
                    "feature_name": feat.feature_name,
                    "gene_name": feat.gene_name,
                })

        # Get relationships (PI, lab members, associates)
        relationships = []
        for rel in colleague.coll_relationship:
            assoc = self.get_colleague_by_id(rel.associate_no)
            if assoc:
                relationships.append({
                    "coll_relationship_no": rel.coll_relationship_no,
                    "associate_no": rel.associate_no,
                    "associate_name": f"{assoc.first_name} {assoc.last_name}",
                    "relationship_type": rel.relationship_type,
                })

        # Get remarks
        remarks = []
        for remark in colleague.colleague_remark:
            remarks.append({
                "colleague_remark_no": remark.colleague_remark_no,
                "remark_type": remark.remark_type,
                "remark_text": remark.remark_text,
                "date_created": remark.date_created.isoformat()
                if remark.date_created else None,
            })

        return {
            "colleague_no": colleague.colleague_no,
            "first_name": colleague.first_name,
            "last_name": colleague.last_name,
            "suffix": colleague.suffix,
            "other_last_name": colleague.other_last_name,
            "email": colleague.email,
            "profession": colleague.profession,
            "job_title": colleague.job_title,
            "institution": colleague.institution,
            "address1": colleague.address1,
            "address2": colleague.address2,
            "address3": colleague.address3,
            "address4": colleague.address4,
            "address5": colleague.address5,
            "city": colleague.city,
            "state": colleague.state,
            "region": colleague.region,
            "country": colleague.country,
            "postal_code": colleague.postal_code,
            "work_phone": colleague.work_phone,
            "other_phone": colleague.other_phone,
            "fax": colleague.fax,
            "is_pi": colleague.is_pi,
            "is_contact": colleague.is_contact,
            "source": colleague.source,
            "date_created": colleague.date_created.isoformat()
            if colleague.date_created else None,
            "date_modified": colleague.date_modified.isoformat()
            if colleague.date_modified else None,
            "created_by": colleague.created_by,
            "urls": urls,
            "keywords": keywords,
            "features": features,
            "relationships": relationships,
            "remarks": remarks,
        }

    def create_colleague(
        self,
        first_name: str,
        last_name: str,
        curator_userid: str,
        email: Optional[str] = None,
        institution: Optional[str] = None,
        is_pi: str = "N",
        is_contact: str = "N",
        **kwargs,
    ) -> int:
        """
        Create a new colleague.

        Returns:
            colleague_no
        """
        colleague = Colleague(
            first_name=first_name,
            last_name=last_name,
            email=email,
            institution=institution,
            is_pi=is_pi,
            is_contact=is_contact,
            source=SOURCE,
            created_by=curator_userid[:12],
            **{k: v for k, v in kwargs.items() if v is not None},
        )
        self.db.add(colleague)
        self.db.flush()

        logger.info(
            f"Created colleague {colleague.colleague_no}: "
            f"{first_name} {last_name}"
        )

        return colleague.colleague_no

    def update_colleague(
        self,
        colleague_no: int,
        curator_userid: str,
        **kwargs,
    ) -> bool:
        """
        Update an existing colleague.

        Returns:
            True if successful
        """
        colleague = self.get_colleague_by_id(colleague_no)
        if not colleague:
            raise ColleagueCurationError(f"Colleague {colleague_no} not found")

        # Update fields that are provided
        for field, value in kwargs.items():
            if hasattr(colleague, field) and value is not None:
                setattr(colleague, field, value)

        colleague.date_modified = datetime.now()
        self.db.commit()

        logger.info(f"Updated colleague {colleague_no} by {curator_userid}")

        return True

    def delete_colleague(self, colleague_no: int, curator_userid: str) -> bool:
        """
        Delete a colleague and all related records.

        Returns:
            True if successful
        """
        colleague = self.get_colleague_by_id(colleague_no)
        if not colleague:
            raise ColleagueCurationError(f"Colleague {colleague_no} not found")

        logger.info(
            f"Deleting colleague {colleague_no} "
            f"({colleague.first_name} {colleague.last_name}) by {curator_userid}"
        )

        # Delete related records (cascading deletes should handle most)
        self.db.query(CollUrl).filter(
            CollUrl.colleague_no == colleague_no
        ).delete()
        self.db.query(CollKw).filter(
            CollKw.colleague_no == colleague_no
        ).delete()
        self.db.query(CollFeat).filter(
            CollFeat.colleague_no == colleague_no
        ).delete()
        self.db.query(CollRelationship).filter(
            or_(
                CollRelationship.colleague_no == colleague_no,
                CollRelationship.associate_no == colleague_no,
            )
        ).delete()
        self.db.query(ColleagueRemark).filter(
            ColleagueRemark.colleague_no == colleague_no
        ).delete()

        # Delete colleague
        self.db.delete(colleague)
        self.db.commit()

        return True

    def add_colleague_url(
        self,
        colleague_no: int,
        url_type: str,
        link: str,
        curator_userid: str,
    ) -> int:
        """Add URL to colleague."""
        colleague = self.get_colleague_by_id(colleague_no)
        if not colleague:
            raise ColleagueCurationError(f"Colleague {colleague_no} not found")

        # Create or get URL
        url = self.db.query(Url).filter(
            Url.url_type == url_type,
            Url.link == link,
        ).first()

        if not url:
            url = Url(
                url_type=url_type,
                link=link,
                created_by=curator_userid[:12],
            )
            self.db.add(url)
            self.db.flush()

        # Create link
        coll_url = CollUrl(
            colleague_no=colleague_no,
            url_no=url.url_no,
        )
        self.db.add(coll_url)
        self.db.commit()

        return coll_url.coll_url_no

    def remove_colleague_url(self, coll_url_no: int) -> bool:
        """Remove URL from colleague."""
        coll_url = (
            self.db.query(CollUrl)
            .filter(CollUrl.coll_url_no == coll_url_no)
            .first()
        )
        if not coll_url:
            raise ColleagueCurationError(f"Colleague URL {coll_url_no} not found")

        self.db.delete(coll_url)
        self.db.commit()
        return True

    def add_colleague_keyword(
        self,
        colleague_no: int,
        keyword: str,
        curator_userid: str,
    ) -> int:
        """Add keyword to colleague."""
        colleague = self.get_colleague_by_id(colleague_no)
        if not colleague:
            raise ColleagueCurationError(f"Colleague {colleague_no} not found")

        # Create or get keyword
        kw = self.db.query(Keyword).filter(
            func.upper(Keyword.keyword) == keyword.upper()
        ).first()

        if not kw:
            kw = Keyword(
                keyword=keyword,
                created_by=curator_userid[:12],
            )
            self.db.add(kw)
            self.db.flush()

        # Check if already linked
        existing = (
            self.db.query(CollKw)
            .filter(
                CollKw.colleague_no == colleague_no,
                CollKw.keyword_no == kw.keyword_no,
            )
            .first()
        )
        if existing:
            return existing.coll_kw_no

        # Create link
        coll_kw = CollKw(
            colleague_no=colleague_no,
            keyword_no=kw.keyword_no,
        )
        self.db.add(coll_kw)
        self.db.commit()

        return coll_kw.coll_kw_no

    def remove_colleague_keyword(self, coll_kw_no: int) -> bool:
        """Remove keyword from colleague."""
        coll_kw = (
            self.db.query(CollKw)
            .filter(CollKw.coll_kw_no == coll_kw_no)
            .first()
        )
        if not coll_kw:
            raise ColleagueCurationError(f"Colleague keyword {coll_kw_no} not found")

        self.db.delete(coll_kw)
        self.db.commit()
        return True

    def add_colleague_feature(
        self,
        colleague_no: int,
        feature_name: str,
        curator_userid: str,
    ) -> int:
        """Add feature association to colleague."""
        colleague = self.get_colleague_by_id(colleague_no)
        if not colleague:
            raise ColleagueCurationError(f"Colleague {colleague_no} not found")

        # Find feature
        feature = (
            self.db.query(Feature)
            .filter(
                or_(
                    func.upper(Feature.feature_name) == feature_name.upper(),
                    func.upper(Feature.gene_name) == feature_name.upper(),
                )
            )
            .first()
        )
        if not feature:
            raise ColleagueCurationError(f"Feature '{feature_name}' not found")

        # Check if already linked
        existing = (
            self.db.query(CollFeat)
            .filter(
                CollFeat.colleague_no == colleague_no,
                CollFeat.feature_no == feature.feature_no,
            )
            .first()
        )
        if existing:
            return existing.coll_feat_no

        # Create link
        coll_feat = CollFeat(
            colleague_no=colleague_no,
            feature_no=feature.feature_no,
        )
        self.db.add(coll_feat)
        self.db.commit()

        return coll_feat.coll_feat_no

    def remove_colleague_feature(self, coll_feat_no: int) -> bool:
        """Remove feature from colleague."""
        coll_feat = (
            self.db.query(CollFeat)
            .filter(CollFeat.coll_feat_no == coll_feat_no)
            .first()
        )
        if not coll_feat:
            raise ColleagueCurationError(f"Colleague feature {coll_feat_no} not found")

        self.db.delete(coll_feat)
        self.db.commit()
        return True

    def add_colleague_relationship(
        self,
        colleague_no: int,
        associate_no: int,
        relationship_type: str,
    ) -> int:
        """Add relationship between colleagues."""
        colleague = self.get_colleague_by_id(colleague_no)
        if not colleague:
            raise ColleagueCurationError(f"Colleague {colleague_no} not found")

        associate = self.get_colleague_by_id(associate_no)
        if not associate:
            raise ColleagueCurationError(f"Associate {associate_no} not found")

        # Check if already exists
        existing = (
            self.db.query(CollRelationship)
            .filter(
                CollRelationship.colleague_no == colleague_no,
                CollRelationship.associate_no == associate_no,
                CollRelationship.relationship_type == relationship_type,
            )
            .first()
        )
        if existing:
            return existing.coll_relationship_no

        # Create relationship
        rel = CollRelationship(
            colleague_no=colleague_no,
            associate_no=associate_no,
            relationship_type=relationship_type,
        )
        self.db.add(rel)
        self.db.commit()

        return rel.coll_relationship_no

    def remove_colleague_relationship(self, coll_relationship_no: int) -> bool:
        """Remove relationship between colleagues."""
        rel = (
            self.db.query(CollRelationship)
            .filter(CollRelationship.coll_relationship_no == coll_relationship_no)
            .first()
        )
        if not rel:
            raise ColleagueCurationError(
                f"Relationship {coll_relationship_no} not found"
            )

        self.db.delete(rel)
        self.db.commit()
        return True

    def add_colleague_remark(
        self,
        colleague_no: int,
        remark_type: str,
        remark_text: str,
        curator_userid: str,
    ) -> int:
        """Add remark to colleague."""
        colleague = self.get_colleague_by_id(colleague_no)
        if not colleague:
            raise ColleagueCurationError(f"Colleague {colleague_no} not found")

        remark = ColleagueRemark(
            colleague_no=colleague_no,
            remark_type=remark_type,
            remark_text=remark_text,
            created_by=curator_userid[:12],
        )
        self.db.add(remark)
        self.db.commit()

        return remark.colleague_remark_no
