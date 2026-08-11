"""
Curator Report Router - Canned database statistics for grant reporting.

Read-only. Requires curator authentication.
"""

import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.curator_report_service import (
    CuratorReportService,
    CuratorReportError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/curation/reports", tags=["curation-reports"])


class ParamOptionOut(BaseModel):
    value: str
    label: str


class ParamOut(BaseModel):
    name: str
    label: str
    type: str
    required: bool
    options: Optional[list[ParamOptionOut]] = None
    allow_custom: Optional[bool] = None


class ReportDefinitionOut(BaseModel):
    id: str
    label: str
    description: str
    params: list[ParamOut]


class ReportDefinitionsResponse(BaseModel):
    reports: list[ReportDefinitionOut]


class ReportResultResponse(BaseModel):
    report_id: str
    params: dict[str, Any]
    columns: list[str]
    rows: list[list[Any]]


@router.get("", response_model=ReportDefinitionsResponse)
def list_reports(current_user: CurrentUser, db: Session = Depends(get_db)):
    """Available reports and their parameter forms."""
    return ReportDefinitionsResponse(reports=CuratorReportService(db).get_definitions())


@router.get("/{report_id}", response_model=ReportResultResponse)
def run_report(
    report_id: str,
    request: Request,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Run a report; parameters are passed as query parameters."""
    params = dict(request.query_params)
    try:
        result = CuratorReportService(db).run(report_id, params)
    except CuratorReportError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    return ReportResultResponse(
        report_id=report_id,
        params=params,
        columns=result["columns"],
        rows=result["rows"],
    )
