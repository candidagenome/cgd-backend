"""
Colleague Search Schemas.
"""
from __future__ import annotations

from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, Field


class ColleagueUrl(BaseModel):
    """URL associated with a colleague."""
    url: str = Field(..., description="URL")
    url_type: Optional[str] = Field(None, description="URL type/description")


class ColleagueListItem(BaseModel):
    """Colleague info for search results list."""
    colleague_no: int = Field(..., description="Colleague ID")
    last_name: str = Field(..., description="Last name")
    first_name: str = Field(..., description="First name")
    full_name: str = Field(..., description="Full name (Last, First)")
    institution: Optional[str] = Field(None, description="Organization/Institution")
    email: Optional[str] = Field(None, description="Email address")
    work_phone: Optional[str] = Field(None, description="Work phone")
    other_phone: Optional[str] = Field(None, description="Other phone")
    fax: Optional[str] = Field(None, description="Fax number")
    urls: List[ColleagueUrl] = Field(default_factory=list, description="Web URLs")


class AssociatedColleague(BaseModel):
    """Brief colleague info for relationships."""
    colleague_no: int = Field(..., description="Colleague ID")
    full_name: str = Field(..., description="Full name")


class AssociatedGene(BaseModel):
    """Gene associated with colleague."""
    feature_name: str = Field(..., description="Feature/ORF name")
    gene_name: Optional[str] = Field(None, description="Gene name")


class ColleagueDetail(BaseModel):
    """Full colleague details."""
    colleague_no: int = Field(..., description="Colleague ID")
    last_name: str = Field(..., description="Last name")
    first_name: str = Field(..., description="First name")
    full_name: str = Field(..., description="Full name")
    other_last_name: Optional[str] = Field(None, description="Other last name (maiden name)")
    suffix: Optional[str] = Field(None, description="Suffix (Jr., Sr., etc.)")
    email: Optional[str] = Field(None, description="Email address")
    job_title: Optional[str] = Field(None, description="Position/Job title")
    profession: Optional[str] = Field(None, description="Profession")
    institution: Optional[str] = Field(None, description="Organization")
    address: Optional[str] = Field(None, description="Full address")
    city: Optional[str] = Field(None, description="City")
    state: Optional[str] = Field(None, description="State/Province")
    country: Optional[str] = Field(None, description="Country")
    postal_code: Optional[str] = Field(None, description="Postal code")
    work_phone: Optional[str] = Field(None, description="Work phone")
    other_phone: Optional[str] = Field(None, description="Other phone")
    fax: Optional[str] = Field(None, description="Fax number")
    urls: List[ColleagueUrl] = Field(default_factory=list, description="Web URLs")
    # Relationships
    lab_heads: List[AssociatedColleague] = Field(
        default_factory=list,
        description="Head of Lab (PI)"
    )
    lab_members: List[AssociatedColleague] = Field(
        default_factory=list,
        description="Members of my Lab"
    )
    associates: List[AssociatedColleague] = Field(
        default_factory=list,
        description="Associates/Collaborators"
    )
    # Associated genes
    associated_genes: List[AssociatedGene] = Field(
        default_factory=list,
        description="Associated genes"
    )
    # Research info
    research_interests: Optional[str] = Field(None, description="Research interests")
    research_topics: Optional[str] = Field(None, description="Research topics")
    keywords: Optional[str] = Field(None, description="Keywords")
    public_comments: Optional[str] = Field(None, description="Public comments")
    # Dates
    date_modified: Optional[datetime] = Field(None, description="Last update date")


class ColleagueSearchRequest(BaseModel):
    """Request for colleague search."""
    last_name: str = Field(..., min_length=1, description="Last name to search")
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(20, ge=1, le=100, description="Results per page")


class ColleagueSearchResponse(BaseModel):
    """Response for colleague search."""
    success: bool = Field(True, description="Whether search was successful")
    search_term: str = Field(..., description="Search term used")
    wildcard_appended: bool = Field(
        False,
        description="Whether wildcard was appended to search"
    )
    colleagues: List[ColleagueListItem] = Field(
        default_factory=list,
        description="List of matching colleagues"
    )
    total_count: int = Field(0, description="Total number of results")
    page: int = Field(1, description="Current page")
    page_size: int = Field(20, description="Items per page")
    total_pages: int = Field(0, description="Total pages")
    error: Optional[str] = Field(None, description="Error message if any")


class ColleagueDetailResponse(BaseModel):
    """Response for colleague detail."""
    success: bool = Field(True, description="Whether request was successful")
    colleague: Optional[ColleagueDetail] = Field(
        None,
        description="Colleague details"
    )
    error: Optional[str] = Field(None, description="Error message if any")
