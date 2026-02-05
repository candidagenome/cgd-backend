"""
Colleague Search Service.

Provides colleague search and detail retrieval functionality.
"""
from __future__ import annotations

import logging
import math
from typing import List, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func, or_

from cgd.models.models import (
    Colleague, CollUrl, CollKw, CollFeat, CollRelationship,
    ColleagueRemark, Url, Keyword, Feature,
)
from cgd.schemas.colleague_schema import (
    ColleagueListItem,
    ColleagueDetail,
    ColleagueUrl,
    AssociatedColleague,
    AssociatedGene,
    ColleagueSearchResponse,
    ColleagueDetailResponse,
)

logger = logging.getLogger(__name__)


def _build_full_name(last_name: str, first_name: str, suffix: Optional[str] = None) -> str:
    """Build full name from components."""
    full_name = f"{last_name}, {first_name}"
    if suffix:
        full_name += f" {suffix}"
    return full_name


def _get_colleague_urls(db: Session, colleague_no: int) -> List[ColleagueUrl]:
    """Get URLs for a colleague."""
    urls = (
        db.query(Url.url, Url.url_type)
        .join(CollUrl, CollUrl.url_no == Url.url_no)
        .filter(CollUrl.colleague_no == colleague_no)
        .all()
    )
    return [
        ColleagueUrl(url=url, url_type=url_type)
        for url, url_type in urls
    ]


def _build_address(colleague: Colleague) -> Optional[str]:
    """Build formatted address from colleague fields."""
    parts = []

    # Address lines
    for addr in [colleague.address1, colleague.address2, colleague.address3,
                 colleague.address4, colleague.address5]:
        if addr:
            parts.append(addr)

    # City
    if colleague.city:
        parts.append(colleague.city)

    # State/Region and postal code
    state_line = []
    if colleague.state:
        state_line.append(colleague.state)
    elif colleague.region:
        state_line.append(colleague.region)
    if colleague.postal_code:
        state_line.append(colleague.postal_code)
    if state_line:
        parts.append(" ".join(state_line))

    # Country
    if colleague.country:
        parts.append(colleague.country)

    return "\n".join(parts) if parts else None


def search_colleagues(
    db: Session,
    last_name: str,
    page: int = 1,
    page_size: int = 20,
) -> ColleagueSearchResponse:
    """
    Search colleagues by last name.

    Args:
        db: Database session
        last_name: Last name to search (supports * wildcard)
        page: Page number (1-indexed)
        page_size: Results per page

    Returns:
        Search response with matching colleagues
    """
    if not last_name or not last_name.strip():
        return ColleagueSearchResponse(
            success=False,
            search_term="",
            error="Last name is required",
        )

    # Convert * to % for SQL LIKE
    search_term = last_name.strip()
    sql_pattern = search_term.replace("*", "%")
    wildcard_appended = False

    # Search by last_name or other_last_name (case-insensitive)
    base_query = (
        db.query(Colleague)
        .filter(
            or_(
                func.upper(Colleague.last_name).like(func.upper(sql_pattern)),
                func.upper(Colleague.other_last_name).like(func.upper(sql_pattern)),
            )
        )
    )

    # Get count
    total_count = base_query.count()

    # If no results and no wildcard, try with wildcard
    if total_count == 0 and "%" not in sql_pattern:
        wildcard_appended = True
        sql_pattern = sql_pattern + "%"
        base_query = (
            db.query(Colleague)
            .filter(
                or_(
                    func.upper(Colleague.last_name).like(func.upper(sql_pattern)),
                    func.upper(Colleague.other_last_name).like(func.upper(sql_pattern)),
                )
            )
        )
        total_count = base_query.count()

    total_pages = math.ceil(total_count / page_size) if total_count > 0 else 0

    # Get paginated results
    offset = (page - 1) * page_size
    colleagues = (
        base_query
        .order_by(Colleague.last_name, Colleague.first_name)
        .offset(offset)
        .limit(page_size)
        .all()
    )

    # Build response items
    items = []
    for coll in colleagues:
        urls = _get_colleague_urls(db, coll.colleague_no)
        items.append(ColleagueListItem(
            colleague_no=coll.colleague_no,
            last_name=coll.last_name,
            first_name=coll.first_name,
            full_name=_build_full_name(coll.last_name, coll.first_name, coll.suffix),
            institution=coll.institution,
            email=coll.email,
            work_phone=coll.work_phone,
            other_phone=coll.other_phone,
            fax=coll.fax,
            urls=urls,
        ))

    display_term = search_term + "*" if wildcard_appended else search_term

    return ColleagueSearchResponse(
        success=True,
        search_term=display_term,
        wildcard_appended=wildcard_appended,
        colleagues=items,
        total_count=total_count,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


def get_colleague_detail(
    db: Session,
    colleague_no: int,
) -> ColleagueDetailResponse:
    """
    Get detailed information for a colleague.

    Args:
        db: Database session
        colleague_no: Colleague ID

    Returns:
        Colleague detail response
    """
    colleague = (
        db.query(Colleague)
        .filter(Colleague.colleague_no == colleague_no)
        .first()
    )

    if not colleague:
        return ColleagueDetailResponse(
            success=False,
            error=f"Colleague with ID {colleague_no} not found",
        )

    # Get URLs
    urls = _get_colleague_urls(db, colleague_no)

    # Get lab heads (PI relationships where this colleague is the associate)
    lab_heads = []
    pi_rels = (
        db.query(CollRelationship)
        .filter(
            CollRelationship.associate_no == colleague_no,
            CollRelationship.relationship_type == "Lab member",
        )
        .all()
    )
    for rel in pi_rels:
        pi = db.query(Colleague).filter(Colleague.colleague_no == rel.colleague_no).first()
        if pi:
            lab_heads.append(AssociatedColleague(
                colleague_no=pi.colleague_no,
                full_name=_build_full_name(pi.last_name, pi.first_name, pi.suffix),
            ))

    # Get lab members (where this colleague is the PI)
    lab_members = []
    member_rels = (
        db.query(CollRelationship)
        .filter(
            CollRelationship.colleague_no == colleague_no,
            CollRelationship.relationship_type == "Lab member",
        )
        .all()
    )
    for rel in member_rels:
        member = db.query(Colleague).filter(Colleague.colleague_no == rel.associate_no).first()
        if member:
            lab_members.append(AssociatedColleague(
                colleague_no=member.colleague_no,
                full_name=_build_full_name(member.last_name, member.first_name, member.suffix),
            ))

    # Get associates/collaborators
    associates = []
    assoc_rels = (
        db.query(CollRelationship)
        .filter(
            CollRelationship.colleague_no == colleague_no,
            CollRelationship.relationship_type == "Associate",
        )
        .all()
    )
    for rel in assoc_rels:
        assoc = db.query(Colleague).filter(Colleague.colleague_no == rel.associate_no).first()
        if assoc:
            associates.append(AssociatedColleague(
                colleague_no=assoc.colleague_no,
                full_name=_build_full_name(assoc.last_name, assoc.first_name, assoc.suffix),
            ))

    # Get associated genes
    associated_genes = []
    coll_feats = (
        db.query(CollFeat)
        .filter(CollFeat.colleague_no == colleague_no)
        .all()
    )
    for cf in coll_feats:
        feature = db.query(Feature).filter(Feature.feature_no == cf.feature_no).first()
        if feature:
            associated_genes.append(AssociatedGene(
                feature_name=feature.feature_name,
                gene_name=feature.gene_name,
            ))

    # Get remarks (research interests and comments)
    research_interests = None
    public_comments = None
    remarks = (
        db.query(ColleagueRemark)
        .filter(ColleagueRemark.colleague_no == colleague_no)
        .all()
    )
    interests_list = []
    comments_list = []
    for remark in remarks:
        if remark.remark_type and "interest" in remark.remark_type.lower():
            interests_list.append(remark.remark)
        else:
            comments_list.append(remark.remark)
    if interests_list:
        research_interests = "\n".join(interests_list)
    if comments_list:
        public_comments = "\n".join(comments_list)

    # Get keywords
    keywords = None
    research_topics = None
    kw_entries = (
        db.query(Keyword.keyword, Keyword.kw_source)
        .join(CollKw, CollKw.keyword_no == Keyword.keyword_no)
        .filter(CollKw.colleague_no == colleague_no)
        .all()
    )
    kw_list = []
    topic_list = []
    for kw, kw_source in kw_entries:
        if kw_source and "keyword" in kw_source.lower():
            kw_list.append(kw)
        else:
            topic_list.append(kw)
    if kw_list:
        keywords = ", ".join(kw_list)
    if topic_list:
        research_topics = ", ".join(topic_list)

    detail = ColleagueDetail(
        colleague_no=colleague.colleague_no,
        last_name=colleague.last_name,
        first_name=colleague.first_name,
        full_name=_build_full_name(colleague.last_name, colleague.first_name, colleague.suffix),
        other_last_name=colleague.other_last_name,
        suffix=colleague.suffix,
        email=colleague.email,
        job_title=colleague.job_title,
        profession=colleague.profession,
        institution=colleague.institution,
        address=_build_address(colleague),
        city=colleague.city,
        state=colleague.state or colleague.region,
        country=colleague.country,
        postal_code=colleague.postal_code,
        work_phone=colleague.work_phone,
        other_phone=colleague.other_phone,
        fax=colleague.fax,
        urls=urls,
        lab_heads=lab_heads,
        lab_members=lab_members,
        associates=associates,
        associated_genes=associated_genes,
        research_interests=research_interests,
        research_topics=research_topics,
        keywords=keywords,
        public_comments=public_comments,
        date_modified=colleague.date_modified,
    )

    return ColleagueDetailResponse(
        success=True,
        colleague=detail,
    )
