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
    get_similar_expression_genes,
    get_batch_expression_data,
)
from cgd.schemas.expression_schema import (
    GeneExpressionResponse,
    ExpressionConfigResponse,
    SimilarGenesResponse,
    BatchExpressionRequest,
    BatchExpressionResponse,
)

router = APIRouter(prefix="/api/expression", tags=["Expression"])


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
    "/gene/{gene_name}/similar",
    response_model=SimilarGenesResponse,
    summary="Find genes with similar expression profiles",
    description="""
    Finds genes with correlated expression profiles to the query gene.

    Uses RNA-seq fold change data across all experimental conditions to compute
    similarity. Genes with similar expression patterns often share biological
    functions or are co-regulated.

    **Similarity metrics:**
    - **pearson** (default): Pearson correlation coefficient
    - **spearman**: Spearman rank correlation (more robust to outliers)
    - **cosine**: Cosine similarity

    **Performance notes:**
    - First query for an organism may take 20-30 seconds (building profiles)
    - Subsequent queries use cached data and complete in <2 seconds
    """
)
def get_similar_genes(
    gene_name: str,
    organism: str = Query(
        "C_albicans_SC5314_A22",
        description="Organism/genome assembly to search"
    ),
    limit: int = Query(
        20,
        ge=1,
        le=100,
        description="Maximum number of similar genes to return"
    ),
    metric: str = Query(
        "pearson",
        description="Similarity metric: pearson, spearman, or cosine"
    ),
    min_conditions: int = Query(
        5,
        ge=1,
        description="Minimum shared conditions required for comparison"
    ),
    db: Session = Depends(get_db)
) -> SimilarGenesResponse:
    """Find genes with similar expression profiles to the query gene."""
    return get_similar_expression_genes(
        db, gene_name, organism, limit, metric, min_conditions
    )


@router.get(
    "/config",
    response_model=ExpressionConfigResponse,
    summary="Get expression data configuration",
    description="Returns available organisms, studies, and condition categories."
)
def get_config() -> ExpressionConfigResponse:
    """Get available expression datasets."""
    return get_expression_config()


@router.post(
    "/batch",
    response_model=BatchExpressionResponse,
    summary="Get expression data for multiple genes",
    description="""
    Returns expression data for multiple genes in a single request.

    This endpoint is optimized for the co-expression heatmap which needs
    expression data for multiple genes (query gene + similar genes).
    Much faster than making separate requests for each gene.

    **Request body:**
    - gene_names: List of gene names (max 50)
    - organism: Organism display name (e.g., "Candida albicans SC5314")
    """
)
def get_batch_expression(
    request: BatchExpressionRequest,
    db: Session = Depends(get_db)
) -> BatchExpressionResponse:
    """Get expression data for multiple genes in batch."""
    return get_batch_expression_data(db, request.gene_names, request.organism)
