"""
Expression Data Service.

Reads RNA-seq bigwig files and calculates fold changes for gene expression analysis.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Dict, List, Tuple

from sqlalchemy.orm import Session

from cgd.models.models import Feature, Seq, FeatLocation
from cgd.schemas.expression_schema import (
    GeneExpressionResponse,
    ExpressionStudy,
    ExpressionCondition,
    ExpressionConfigResponse,
)

logger = logging.getLogger(__name__)

# Try to import pyBigWig - gracefully handle if not installed
try:
    import pyBigWig
    PYBIGWIG_AVAILABLE = True
except ImportError:
    PYBIGWIG_AVAILABLE = False
    logger.warning("pyBigWig not installed - expression analysis will be unavailable")


# ============================================================================
# Configuration
# ============================================================================

# Base path for HTS data (bigwig files)
# This should be configured per environment
HTS_BASE_PATHS = {
    "C_albicans_SC5314": Path("/data/HTS/C_albicans_SC5314/bam"),
    "C_auris_B8441": Path("/data/HTS/C_auris_B8441/bam"),
    "C_glabrata_CBS138": Path("/data/HTS/C_glabrata_CBS138/bam"),
    "C_dubliniensis_CD36": Path("/data/HTS/C_dubliniensis_CD36/bam"),
    "C_parapsilosis_CDC317": Path("/data/HTS/C_parapsilosis_CDC317/bam"),
}

# Study configurations with metadata
# path_style: "old" = sorted_hits_bam2wig/sorted_hits.bigwig
#             "new" = {SRR_ID}_sorted_hits.bigwig
#             "lohse" = {cond}_bam2wig/{cond}.bigwig
EXPRESSION_STUDIES = {
    "C_albicans_SC5314": {
        "Bruno_2010": {
            "category": "Stress Response",
            "pmid": "20543895",
            "path_style": "old",
            "control": "nOxi",
            "conditions": {
                "nOxi": {"label": "Control (no stress)", "bucket": "control"},
                "lOxi": {"label": "Low H2O2 (0.5mM)", "bucket": "stress"},
                "hOxi": {"label": "High H2O2 (5mM)", "bucket": "stress"},
                "nitroSm": {"label": "Nitrosative control", "bucket": "control"},
                "nitroSp": {"label": "Nitrosative stress", "bucket": "stress"},
                "cwdm": {"label": "Cell wall control", "bucket": "control"},
                "cwdp": {"label": "Cell wall damage", "bucket": "stress"},
                "ph4": {"label": "pH 4 (acidic)", "bucket": "basic_biology"},
                "ph8": {"label": "pH 8 (alkaline)", "bucket": "basic_biology"},
                "ypd_ss": {"label": "YPD steady state", "bucket": "control"},
                "ypd_serum_ss": {"label": "Serum", "bucket": "basic_biology"},
            },
        },
        "Desai_2013": {
            "category": "Biofilm",
            "pmid": "24307631",
            "path_style": "old",
            "control": "sc_plnk",
            "conditions": {
                "sc_plnk": {"label": "Planktonic", "bucket": "control"},
                "sc_film": {"label": "Biofilm", "bucket": "basic_biology"},
            },
        },
        "Niemiec_2017": {
            "category": "Immune Response",
            "pmid": "28874114",
            "path_style": "old",
            "control": "chg_77",
            "conditions": {
                "chg_77": {"label": "PMN Control 0-min", "bucket": "control"},
                "chg_71": {"label": "PMN 15-min", "bucket": "kill_candida"},
                "chg_72": {"label": "PMN 30-min", "bucket": "kill_candida"},
                "chg_73": {"label": "PMN 60-min", "bucket": "kill_candida"},
                "chg_75": {"label": "PMN Hyphae Control", "bucket": "control"},
                "chg_79": {"label": "PMN Hyphae 15-min", "bucket": "kill_candida"},
                "chg_76": {"label": "NET Control", "bucket": "control"},
                "chg_70": {"label": "NETs Yeast", "bucket": "kill_candida"},
                "chg_69": {"label": "NETs Hyphae", "bucket": "kill_candida"},
            },
        },
        "Xie_2013": {
            "category": "Cell Type Switching",
            "pmid": "23326225",
            "path_style": "old",
            "control": "CY110wh",
            "conditions": {
                "CY110wh": {"label": "White cells", "bucket": "control"},
                "CY110op": {"label": "Opaque cells", "bucket": "basic_biology"},
            },
        },
        "Shivarathri_2019": {
            "category": "Antifungal Response",
            "pmid": "31263212",
            "path_style": "new",
            "control": "SRR8285058",
            "conditions": {
                "SRR8285058": {"label": "WT Control (rep I)", "bucket": "control"},
                "SRR8285059": {"label": "Caspofungin 15min (I)", "bucket": "kill_candida"},
                "SRR8285060": {"label": "Caspofungin 45min (I)", "bucket": "kill_candida"},
                "SRR8285064": {"label": "WT Control (rep II)", "bucket": "control"},
                "SRR8285065": {"label": "Caspofungin 15min (II)", "bucket": "kill_candida"},
                "SRR8285066": {"label": "Caspofungin 45min (II)", "bucket": "kill_candida"},
            },
        },
        "Glazier_2023": {
            "category": "Morphology/Media",
            "pmid": "37737633",
            "path_style": "new",
            "control": "SRR25396044",
            "conditions": {
                "SRR25396044": {"label": "YPD Control (A)", "bucket": "control"},
                "SRR25396043": {"label": "YPD Control (B)", "bucket": "control"},
                "SRR25396042": {"label": "RPMI 37°C (A)", "bucket": "basic_biology"},
                "SRR25396041": {"label": "RPMI 37°C (B)", "bucket": "basic_biology"},
                "SRR25396040": {"label": "Spider 37°C (A)", "bucket": "basic_biology"},
                "SRR25396039": {"label": "Spider 37°C (B)", "bucket": "basic_biology"},
            },
        },
        "Lohse_2016": {
            "category": "Cell Type Switching",
            "pmid": "27280690",
            "path_style": "lohse",
            "control": "wt_con",
            "conditions": {
                "wt_con": {"label": "White cells (control)", "bucket": "control"},
                "wt_gfp": {"label": "White cells (GFP)", "bucket": "control"},
                "op_con": {"label": "Opaque cells (control)", "bucket": "basic_biology"},
                "op_gfp": {"label": "Opaque cells (GFP)", "bucket": "basic_biology"},
            },
        },
        "Zhang_2024": {
            "category": "DNA Damage Response",
            "pmid": "35886903",
            "path_style": "new",
            "control": "SRR18188695",
            "conditions": {
                # Control (untreated) - 3 replicates
                "SRR18188695": {"label": "Control (rep 1)", "bucket": "control"},
                "SRR18188696": {"label": "Control (rep 2)", "bucket": "control"},
                "SRR18188697": {"label": "Control (rep 3)", "bucket": "control"},
                # MMS treated - 3 replicates
                "SRR18188698": {"label": "MMS treated (rep 1)", "bucket": "stress"},
                "SRR18188699": {"label": "MMS treated (rep 2)", "bucket": "stress"},
                "SRR18188700": {"label": "MMS treated (rep 3)", "bucket": "stress"},
            },
        },
    },
}

# Bucket category labels
BUCKET_LABELS = {
    "control": "Control",
    "basic_biology": "Basic Candida Biology",
    "kill_candida": "How to Kill Candida",
    "stress": "Stress Response",
}


# ============================================================================
# Helper Functions
# ============================================================================

def _get_organism_from_tag(organism_tag: str) -> str:
    """Convert organism tag to HTS directory name."""
    # Map common organism tags to HTS directory names
    tag_mapping = {
        "C_albicans_SC5314_A22": "C_albicans_SC5314",
        "C_albicans_SC5314": "C_albicans_SC5314",
        "C_auris_B8441": "C_auris_B8441",
        "C_glabrata_CBS138": "C_glabrata_CBS138",
        "C_dubliniensis_CD36": "C_dubliniensis_CD36",
        "C_parapsilosis_CDC317": "C_parapsilosis_CDC317",
    }
    return tag_mapping.get(organism_tag, organism_tag)


def _map_chromosome_to_ca22(chromosome: str) -> Optional[str]:
    """
    Map chromosome names from Ca21/Ca20/other formats to Ca22 format for bigwig files.

    Bigwig files use Ca22 assembly chromosome names like:
    - Ca22chr1A_C_albicans_SC5314
    - Ca22chr2A_C_albicans_SC5314

    Database may have:
    - Ca21chr2_C_albicans_SC5314
    - Ca20chr2
    - Contig19-10076
    """
    import re

    # Already Ca22 format
    if chromosome.startswith("Ca22chr"):
        return chromosome

    # Map Ca21 format: Ca21chr2_C_albicans_SC5314 -> Ca22chr2A_C_albicans_SC5314
    match = re.match(r"Ca21chr(\d+)(_C_albicans_SC5314)?", chromosome)
    if match:
        chr_num = match.group(1)
        return f"Ca22chr{chr_num}A_C_albicans_SC5314"

    # Map Ca20 format: Ca20chr2 -> Ca22chr2A_C_albicans_SC5314
    match = re.match(r"Ca20chr(\d+)", chromosome)
    if match:
        chr_num = match.group(1)
        return f"Ca22chr{chr_num}A_C_albicans_SC5314"

    # Map chrR format (ribosomal)
    if "chrR" in chromosome:
        return "Ca22chrRA_C_albicans_SC5314"

    # Cannot map contig or other formats
    return None


def _get_bigwig_path(
    base_path: Path,
    study: str,
    condition: str,
    study_info: dict,
    haplotype: str = "HapA"
) -> Path:
    """Construct path to bigwig file based on path style."""
    if study_info["path_style"] == "old":
        return base_path / study / haplotype / condition / "sorted_hits_bam2wig" / "sorted_hits.bigwig"
    elif study_info["path_style"] == "lohse":
        # Lohse_2016 style: {cond}_bam2wig/{cond}.bigwig
        return base_path / study / haplotype / condition / f"{condition}_bam2wig" / f"{condition}.bigwig"
    else:  # new style
        return base_path / study / haplotype / condition / f"{condition}_sorted_hits.bigwig"


def _get_expression_value(
    bigwig_path: Path,
    chromosome: str,
    start: int,
    end: int
) -> Optional[float]:
    """Get mean expression value for a genomic region from bigwig file."""
    if not PYBIGWIG_AVAILABLE:
        return None

    if not bigwig_path.exists():
        return None

    try:
        bw = pyBigWig.open(str(bigwig_path))
        if bw is None:
            return None

        # Handle minus strand genes where start > end
        if start > end:
            start, end = end, start

        # pyBigWig uses 0-based coordinates
        stats = bw.stats(chromosome, start - 1, end, type="mean")
        bw.close()

        if stats and stats[0] is not None:
            return stats[0]
        return 0.0
    except Exception as e:
        logger.debug(f"Error reading bigwig {bigwig_path}: {e}")
        return None


def _get_gene_location(
    db: Session,
    gene_name: str,
    organism_tag: str
) -> Optional[Tuple[str, str, str, int, int, str]]:
    """
    Get gene location info from database.
    Returns: (gene_name, feature_name, chromosome, start, end, description) or None

    For C. albicans, we prioritize Ca21/Ca22 locations over contigs because
    the bigwig files use Ca22 assembly coordinates.
    """
    # Find the feature by gene name or feature name
    feature = (
        db.query(Feature)
        .filter(
            (Feature.gene_name == gene_name) | (Feature.feature_name == gene_name)
        )
        .first()
    )

    if not feature:
        return None

    # Get all current locations for this feature
    locations = (
        db.query(FeatLocation)
        .filter(
            FeatLocation.feature_no == feature.feature_no,
            FeatLocation.is_loc_current == "Y"
        )
        .all()
    )

    if not locations:
        return None

    # Find the best location (prioritize Ca22 > Ca21 > Ca20 > others)
    best_location = None
    best_chromosome = None
    best_priority = -1

    for loc in locations:
        if not loc.root_seq_no:
            continue

        root_seq = db.query(Seq).filter(Seq.seq_no == loc.root_seq_no).first()
        if not root_seq:
            continue

        root_feature = db.query(Feature).filter(
            Feature.feature_no == root_seq.feature_no
        ).first()
        if not root_feature:
            continue

        chr_name = root_feature.feature_name

        # Determine priority
        priority = 0
        if chr_name.startswith("Ca22chr"):
            priority = 4
        elif chr_name.startswith("Ca21chr"):
            priority = 3
        elif chr_name.startswith("Ca20chr"):
            priority = 2
        elif "chr" in chr_name.lower():
            priority = 1

        if priority > best_priority:
            best_priority = priority
            best_location = loc
            best_chromosome = chr_name

    if not best_location or not best_chromosome:
        return None

    # Map chromosome name to Ca22 format for bigwig files
    ca22_chromosome = _map_chromosome_to_ca22(best_chromosome)
    if not ca22_chromosome:
        # If we can't map to Ca22, still return the location but with original name
        # The bigwig query will fail but we'll show a warning
        ca22_chromosome = best_chromosome

    return (
        feature.gene_name or feature.feature_name,
        feature.feature_name,
        ca22_chromosome,
        best_location.start_coord,
        best_location.stop_coord,
        feature.headline
    )


# ============================================================================
# Main Service Functions
# ============================================================================

def get_gene_expression(
    db: Session,
    gene_name: str,
    organism: str = "C_albicans_SC5314_A22"
) -> GeneExpressionResponse:
    """
    Get expression data for a gene across all available conditions.

    Args:
        db: Database session
        gene_name: Gene name or systematic name
        organism: Organism tag

    Returns:
        GeneExpressionResponse with expression data
    """
    if not PYBIGWIG_AVAILABLE:
        return GeneExpressionResponse(
            success=False,
            error="Expression analysis unavailable: pyBigWig not installed"
        )

    # Get gene location
    location_info = _get_gene_location(db, gene_name, organism)
    if not location_info:
        return GeneExpressionResponse(
            success=False,
            error=f"Gene '{gene_name}' not found or has no location data"
        )

    gene_name_db, feature_name, chromosome, start, end, description = location_info

    # Get organism directory
    organism_key = _get_organism_from_tag(organism)
    base_path = HTS_BASE_PATHS.get(organism_key)

    if not base_path or not base_path.exists():
        return GeneExpressionResponse(
            success=False,
            error=f"Expression data not available for organism: {organism}"
        )

    # Get studies for this organism
    studies_config = EXPRESSION_STUDIES.get(organism_key, {})
    if not studies_config:
        return GeneExpressionResponse(
            success=False,
            error=f"No expression studies configured for organism: {organism}"
        )

    # Process each study
    studies: List[ExpressionStudy] = []
    all_fold_changes: List[float] = []
    total_conditions = 0
    warnings: List[str] = []

    for study_id, study_info in studies_config.items():
        control_id = study_info["control"]
        control_path = _get_bigwig_path(base_path, study_id, control_id, study_info)

        # Get control expression value
        control_value = _get_expression_value(control_path, chromosome, start, end)
        if control_value is None or control_value <= 0:
            warnings.append(f"Could not read control data for {study_id}")
            continue

        # Process conditions
        conditions: List[ExpressionCondition] = []

        for cond_id, cond_info in study_info["conditions"].items():
            if cond_id == control_id:
                continue

            cond_path = _get_bigwig_path(base_path, study_id, cond_id, study_info)
            cond_value = _get_expression_value(cond_path, chromosome, start, end)

            if cond_value is None:
                continue

            fold_change = cond_value / control_value if control_value > 0 else 0
            fold_change = round(fold_change, 3)

            conditions.append(ExpressionCondition(
                condition_id=cond_id,
                label=cond_info["label"],
                value=round(cond_value, 2),
                fold_change=fold_change,
                bucket=cond_info["bucket"]
            ))

            all_fold_changes.append(fold_change)
            total_conditions += 1

        # Sort conditions by fold change (descending)
        conditions.sort(key=lambda x: x.fold_change, reverse=True)

        if conditions:
            studies.append(ExpressionStudy(
                study_id=study_id,
                category=study_info["category"],
                pmid=study_info.get("pmid"),
                control_id=control_id,
                control_value=round(control_value, 2),
                conditions=conditions
            ))

    # Calculate summary statistics
    max_up = max(all_fold_changes) if all_fold_changes else None
    max_down = min(all_fold_changes) if all_fold_changes else None

    return GeneExpressionResponse(
        success=True,
        gene_name=gene_name_db,
        feature_name=feature_name,
        description=description,
        chromosome=chromosome,
        start=start,
        end=end,
        studies=studies,
        total_conditions=total_conditions,
        max_upregulation=max_up,
        max_downregulation=max_down,
        warnings=warnings
    )


def get_expression_config() -> ExpressionConfigResponse:
    """Get available expression datasets configuration."""
    organisms = []
    for org_key, base_path in HTS_BASE_PATHS.items():
        organisms.append({
            "id": org_key,
            "name": org_key.replace("_", " "),
            "available": base_path.exists() if base_path else False
        })

    studies = []
    for org_key, org_studies in EXPRESSION_STUDIES.items():
        for study_id, study_info in org_studies.items():
            studies.append({
                "id": study_id,
                "organism": org_key,
                "category": study_info["category"],
                "pmid": study_info.get("pmid"),
                "condition_count": len(study_info["conditions"])
            })

    buckets = [
        {"id": k, "label": v}
        for k, v in BUCKET_LABELS.items()
    ]

    return ExpressionConfigResponse(
        organisms=organisms,
        studies=studies,
        buckets=buckets
    )
