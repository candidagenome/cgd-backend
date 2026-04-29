"""
Expression Data API Router.

Provides endpoints for gene expression analysis from RNA-seq data.
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.api.services.expression_service import (
    get_gene_expression,
    get_expression_config,
)
from cgd.schemas.expression_schema import (
    GeneExpressionResponse,
    ExpressionConfigResponse,
)

router = APIRouter(prefix="/expression", tags=["Expression"])


@router.get(
    "/gene/{gene_name}",
    response_model=GeneExpressionResponse,
    summary="Get expression data for a gene",
    description="""
    Returns RNA-seq expression data for a gene across multiple experimental conditions.

    Expression values are calculated as fold changes relative to control conditions
    within each study. Conditions are categorized into buckets:
    - **control**: Baseline/control conditions
    - **basic_biology**: Basic Candida biology studies (biofilm, morphology, etc.)
    - **kill_candida**: Antifungal/immune response studies
    - **stress**: Stress response studies (oxidative, nitrosative, etc.)
    """
)
def get_expression(
    gene_name: str,
    organism: str = Query(
        "C_albicans_SC5314_A22",
        description="Organism/genome assembly"
    ),
    db: Session = Depends(get_db)
) -> GeneExpressionResponse:
    """Get expression data for a gene."""
    return get_gene_expression(db, gene_name, organism)


@router.get(
    "/config",
    response_model=ExpressionConfigResponse,
    summary="Get expression data configuration",
    description="Returns available organisms, studies, and condition categories."
)
def get_config() -> ExpressionConfigResponse:
    """Get available expression datasets."""
    return get_expression_config()
