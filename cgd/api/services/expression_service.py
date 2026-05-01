"""
Expression Data Service.

Reads RNA-seq bigwig files and calculates fold changes for gene expression analysis.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional, Dict, List, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import or_, func

from cgd.models.models import Feature, Seq, FeatLocation, Organism
from cgd.schemas.expression_schema import (
    GeneExpressionResponse,
    ExpressionStudy,
    ExpressionCondition,
    ExpressionConfigResponse,
    ExpressionDetailsResponse,
    ExpressionDetailsForOrganism,
    SimilarGene,
    SimilarGenesResponse,
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
# path_style: "old" = {study}/HapA/{cond}/sorted_hits_bam2wig/sorted_hits.bigwig (C. albicans old)
#             "new" = {study}/HapA/{cond}/{cond}_sorted_hits.bigwig (C. albicans new)
#             "lohse" = {study}/HapA/{cond}/{cond}_bam2wig/{cond}.bigwig (C. albicans Lohse)
#             "direct" = {study}/{cond}/{cond}_sorted_hits.bigwig (non-haplotype organisms)
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
        "Rai_2024": {
            "category": "Biofilm/Gene Regulation",
            "pmid": "38905306",
            "path_style": "new",
            "control": "ERR8278349",
            "conditions": {
                # SN152 wild-type (control strain)
                "ERR8278349": {"label": "WT SN152 (rep 1)", "bucket": "control"},
                "ERR8278350": {"label": "WT SN152 (rep 2)", "bucket": "control"},
                "ERR8278351": {"label": "WT SN152 (rep 3)", "bucket": "control"},
                # CEC4665 wild-type
                "ERR8278346": {"label": "WT CEC4665 (rep 1)", "bucket": "control"},
                "ERR8278347": {"label": "WT CEC4665 (rep 2)", "bucket": "control"},
                "ERR8278348": {"label": "WT CEC4665 (rep 3)", "bucket": "control"},
                # ZCF15 overexpression (TetO-ZCF15)
                "ERR8278352": {"label": "ZCF15-OE (rep 1)", "bucket": "basic_biology"},
                "ERR8278353": {"label": "ZCF15-OE (rep 2)", "bucket": "basic_biology"},
                "ERR8278354": {"label": "ZCF15-OE (rep 3)", "bucket": "basic_biology"},
                # ZCF26 overexpression (TetO-ZCF26)
                "ERR8278355": {"label": "ZCF26-OE (rep 1)", "bucket": "basic_biology"},
                "ERR8278356": {"label": "ZCF26-OE (rep 2)", "bucket": "basic_biology"},
                "ERR8278357": {"label": "ZCF26-OE (rep 3)", "bucket": "basic_biology"},
                # ZCF15 deletion
                "ERR8278358": {"label": "zcf15Δ (rep 1)", "bucket": "basic_biology"},
                "ERR8278359": {"label": "zcf15Δ (rep 2)", "bucket": "basic_biology"},
                "ERR8278360": {"label": "zcf15Δ (rep 3)", "bucket": "basic_biology"},
                # ZCF26 deletion
                "ERR8278361": {"label": "zcf26Δ (rep 1)", "bucket": "basic_biology"},
                "ERR8278362": {"label": "zcf26Δ (rep 2)", "bucket": "basic_biology"},
                "ERR8278363": {"label": "zcf26Δ (rep 3)", "bucket": "basic_biology"},
            },
        },
    },
    "C_auris_B8441": {
        "Shivarathri_2022": {
            "category": "Antifungal Response",
            "pmid": "35652307",
            "path_style": "direct",
            "control": "SRR17259761",
            "conditions": {
                # Amphotericin B Sensitive (control)
                "SRR17259761": {"label": "AmpB Sensitive (rep 1)", "bucket": "control"},
                "SRR17259762": {"label": "AmpB Sensitive (rep 2)", "bucket": "control"},
                "SRR17259763": {"label": "AmpB Sensitive (rep 3)", "bucket": "control"},
                "SRR17259764": {"label": "AmpB Sensitive (rep 4)", "bucket": "control"},
                "SRR17259765": {"label": "AmpB Sensitive (rep 5)", "bucket": "control"},
                "SRR17259766": {"label": "AmpB Sensitive (rep 6)", "bucket": "control"},
                # Amphotericin B Resistant
                "SRR17259767": {"label": "AmpB Resistant (rep 1)", "bucket": "kill_candida"},
                "SRR17259768": {"label": "AmpB Resistant (rep 2)", "bucket": "kill_candida"},
                "SRR17259769": {"label": "AmpB Resistant (rep 3)", "bucket": "kill_candida"},
                "SRR17259770": {"label": "AmpB Resistant (rep 4)", "bucket": "kill_candida"},
                "SRR17259771": {"label": "AmpB Resistant (rep 5)", "bucket": "kill_candida"},
                "SRR17259772": {"label": "AmpB Resistant (rep 6)", "bucket": "kill_candida"},
            },
        },
        "Jakab_2021": {
            "category": "Stress Response",
            "pmid": "31399405",
            "path_style": "direct",
            "control": "SRR10006214",
            "conditions": {
                # Control (no farnesol)
                "SRR10006214": {"label": "Control (rep 1)", "bucket": "control"},
                "SRR10006215": {"label": "Control (rep 2)", "bucket": "control"},
                "SRR10006216": {"label": "Control (rep 3)", "bucket": "control"},
                # Farnesol treated
                "SRR10006217": {"label": "Farnesol (rep 1)", "bucket": "stress"},
                "SRR10006218": {"label": "Farnesol (rep 2)", "bucket": "stress"},
                "SRR10006219": {"label": "Farnesol (rep 3)", "bucket": "stress"},
            },
        },
        "Balla_2023": {
            "category": "Stress Response",
            "pmid": "37532970",
            "path_style": "direct",
            "control": "SRR23266141",
            "conditions": {
                # Control
                "SRR23266141": {"label": "Control (rep 1)", "bucket": "control"},
                "SRR23266140": {"label": "Control (rep 2)", "bucket": "control"},
                "SRR23266139": {"label": "Control (rep 3)", "bucket": "control"},
                # Tyrosol treated
                "SRR23266138": {"label": "Tyrosol (rep 1)", "bucket": "stress"},
                "SRR23266137": {"label": "Tyrosol (rep 2)", "bucket": "stress"},
                "SRR23266136": {"label": "Tyrosol (rep 3)", "bucket": "stress"},
            },
        },
        "Biermann_2022": {
            "category": "Stress Response",
            "pmid": "35473297",
            "path_style": "direct",
            "control": "SRR17805794",
            "conditions": {
                # B11221 WT Control
                "SRR17805794": {"label": "B11221 WT Control (rep 1)", "bucket": "control"},
                "SRR17805793": {"label": "B11221 WT Control (rep 2)", "bucket": "control"},
                "SRR17805782": {"label": "B11221 WT Control (rep 3)", "bucket": "control"},
                # B11221 WT Methylglyoxal
                "SRR17805777": {"label": "B11221 WT Methylglyoxal (rep 1)", "bucket": "stress"},
                "SRR17805776": {"label": "B11221 WT Methylglyoxal (rep 2)", "bucket": "stress"},
                "SRR17805775": {"label": "B11221 WT Methylglyoxal (rep 3)", "bucket": "stress"},
                # B11221 WT Benomyl
                "SRR17805774": {"label": "B11221 WT Benomyl (rep 1)", "bucket": "stress"},
                "SRR17805773": {"label": "B11221 WT Benomyl (rep 2)", "bucket": "stress"},
                "SRR17805772": {"label": "B11221 WT Benomyl (rep 3)", "bucket": "stress"},
                # B11221 mrr1a Control
                "SRR17805771": {"label": "B11221 mrr1aΔ Control (rep 1)", "bucket": "basic_biology"},
                "SRR17805792": {"label": "B11221 mrr1aΔ Control (rep 2)", "bucket": "basic_biology"},
                "SRR17805791": {"label": "B11221 mrr1aΔ Control (rep 3)", "bucket": "basic_biology"},
                # B11221 mrr1a Methylglyoxal
                "SRR17805790": {"label": "B11221 mrr1aΔ Methylglyoxal (rep 1)", "bucket": "stress"},
                "SRR17805789": {"label": "B11221 mrr1aΔ Methylglyoxal (rep 2)", "bucket": "stress"},
                "SRR17805788": {"label": "B11221 mrr1aΔ Methylglyoxal (rep 3)", "bucket": "stress"},
                # B11221 mrr1a Benomyl
                "SRR17805787": {"label": "B11221 mrr1aΔ Benomyl (rep 1)", "bucket": "stress"},
                "SRR17805786": {"label": "B11221 mrr1aΔ Benomyl (rep 2)", "bucket": "stress"},
                "SRR17805785": {"label": "B11221 mrr1aΔ Benomyl (rep 3)", "bucket": "stress"},
                # AR0390 Control
                "SRR17805784": {"label": "AR0390 Control (rep 1)", "bucket": "control"},
                "SRR17805783": {"label": "AR0390 Control (rep 2)", "bucket": "control"},
                "SRR17805781": {"label": "AR0390 Control (rep 3)", "bucket": "control"},
                # AR0390 Methylglyoxal
                "SRR17805780": {"label": "AR0390 Methylglyoxal (rep 1)", "bucket": "stress"},
                "SRR17805779": {"label": "AR0390 Methylglyoxal (rep 2)", "bucket": "stress"},
                "SRR17805778": {"label": "AR0390 Methylglyoxal (rep 3)", "bucket": "stress"},
            },
        },
        "Chow_2023": {
            "category": "Gene Regulation",
            "pmid": "38014938",
            "path_style": "direct",
            "control": "SRR22315652",
            "conditions": {
                # WT Control
                "SRR22315652": {"label": "WT (rep 1)", "bucket": "control"},
                "SRR22315651": {"label": "WT (rep 2)", "bucket": "control"},
                "SRR22315650": {"label": "WT (rep 3)", "bucket": "control"},
                # ubr2 deletion
                "SRR22315649": {"label": "ubr2Δ (rep 1)", "bucket": "basic_biology"},
                "SRR22315648": {"label": "ubr2Δ (rep 2)", "bucket": "basic_biology"},
                "SRR22315647": {"label": "ubr2Δ (rep 3)", "bucket": "basic_biology"},
                # mub1 deletion
                "SRR22315646": {"label": "mub1Δ (rep 1)", "bucket": "basic_biology"},
                "SRR22315645": {"label": "mub1Δ (rep 2)", "bucket": "basic_biology"},
                "SRR22315644": {"label": "mub1Δ (rep 3)", "bucket": "basic_biology"},
            },
        },
        "Jenull_2021": {
            "category": "Strain Comparison",
            "pmid": "33937102",
            "path_style": "direct",
            "control": "SRR13576987",
            "conditions": {
                # CBS10913 (reference strain)
                "SRR13576987": {"label": "CBS10913 (rep 1)", "bucket": "control"},
                "SRR13576988": {"label": "CBS10913 (rep 2)", "bucket": "control"},
                "SRR13576989": {"label": "CBS10913 (rep 3)", "bucket": "control"},
                # 470140 (clinical isolate)
                "SRR13576978": {"label": "470140 (rep 1)", "bucket": "basic_biology"},
                "SRR13576979": {"label": "470140 (rep 2)", "bucket": "basic_biology"},
                "SRR13576980": {"label": "470140 (rep 3)", "bucket": "basic_biology"},
                # 470147 (clinical isolate)
                "SRR13576981": {"label": "470147 (rep 1)", "bucket": "basic_biology"},
                "SRR13576982": {"label": "470147 (rep 2)", "bucket": "basic_biology"},
                "SRR13576983": {"label": "470147 (rep 3)", "bucket": "basic_biology"},
                # 470154 (clinical isolate)
                "SRR13576984": {"label": "470154 (rep 1)", "bucket": "basic_biology"},
                "SRR13576985": {"label": "470154 (rep 2)", "bucket": "basic_biology"},
                "SRR13576986": {"label": "470154 (rep 3)", "bucket": "basic_biology"},
            },
        },
        "Pelletier_2024": {
            "category": "Morphology/Media",
            "pmid": "38466738",
            "path_style": "direct",
            "control": "SRR24915342",
            "conditions": {
                # UACa11 RPMI-1640 (control) - GSM7476311
                "SRR24915342": {"label": "UACa11 RPMI (rep 1.1)", "bucket": "control"},
                "SRR24915343": {"label": "UACa11 RPMI (rep 1.2)", "bucket": "control"},
                "SRR24915344": {"label": "UACa11 RPMI (rep 1.3)", "bucket": "control"},
                "SRR24915345": {"label": "UACa11 RPMI (rep 1.4)", "bucket": "control"},
                # UACa11 RPMI-1640 - GSM7476312
                "SRR24915338": {"label": "UACa11 RPMI (rep 2.1)", "bucket": "control"},
                "SRR24915339": {"label": "UACa11 RPMI (rep 2.2)", "bucket": "control"},
                "SRR24915340": {"label": "UACa11 RPMI (rep 2.3)", "bucket": "control"},
                "SRR24915341": {"label": "UACa11 RPMI (rep 2.4)", "bucket": "control"},
                # UACa11 RPMI-1640 - GSM7476313
                "SRR24915334": {"label": "UACa11 RPMI (rep 3.1)", "bucket": "control"},
                "SRR24915335": {"label": "UACa11 RPMI (rep 3.2)", "bucket": "control"},
                "SRR24915336": {"label": "UACa11 RPMI (rep 3.3)", "bucket": "control"},
                "SRR24915337": {"label": "UACa11 RPMI (rep 3.4)", "bucket": "control"},
                # UACa11 Sabouraud - GSM7476308
                "SRR24915354": {"label": "UACa11 Sabouraud (rep 1.1)", "bucket": "basic_biology"},
                "SRR24915355": {"label": "UACa11 Sabouraud (rep 1.2)", "bucket": "basic_biology"},
                "SRR24915356": {"label": "UACa11 Sabouraud (rep 1.3)", "bucket": "basic_biology"},
                "SRR24915357": {"label": "UACa11 Sabouraud (rep 1.4)", "bucket": "basic_biology"},
                # UACa11 Sabouraud - GSM7476309
                "SRR24915350": {"label": "UACa11 Sabouraud (rep 2.1)", "bucket": "basic_biology"},
                "SRR24915351": {"label": "UACa11 Sabouraud (rep 2.2)", "bucket": "basic_biology"},
                "SRR24915352": {"label": "UACa11 Sabouraud (rep 2.3)", "bucket": "basic_biology"},
                "SRR24915353": {"label": "UACa11 Sabouraud (rep 2.4)", "bucket": "basic_biology"},
                # UACa11 Sabouraud - GSM7476310
                "SRR24915346": {"label": "UACa11 Sabouraud (rep 3.1)", "bucket": "basic_biology"},
                "SRR24915347": {"label": "UACa11 Sabouraud (rep 3.2)", "bucket": "basic_biology"},
                "SRR24915348": {"label": "UACa11 Sabouraud (rep 3.3)", "bucket": "basic_biology"},
                "SRR24915349": {"label": "UACa11 Sabouraud (rep 3.4)", "bucket": "basic_biology"},
                # UACa20 RPMI-1640 - GSM7476305
                "SRR24915366": {"label": "UACa20 RPMI (rep 1.1)", "bucket": "control"},
                "SRR24915367": {"label": "UACa20 RPMI (rep 1.2)", "bucket": "control"},
                "SRR24915368": {"label": "UACa20 RPMI (rep 1.3)", "bucket": "control"},
                "SRR24915369": {"label": "UACa20 RPMI (rep 1.4)", "bucket": "control"},
                # UACa20 RPMI-1640 - GSM7476306
                "SRR24915362": {"label": "UACa20 RPMI (rep 2.1)", "bucket": "control"},
                "SRR24915363": {"label": "UACa20 RPMI (rep 2.2)", "bucket": "control"},
                "SRR24915364": {"label": "UACa20 RPMI (rep 2.3)", "bucket": "control"},
                "SRR24915365": {"label": "UACa20 RPMI (rep 2.4)", "bucket": "control"},
                # UACa20 RPMI-1640 - GSM7476307
                "SRR24915358": {"label": "UACa20 RPMI (rep 3.1)", "bucket": "control"},
                "SRR24915359": {"label": "UACa20 RPMI (rep 3.2)", "bucket": "control"},
                "SRR24915360": {"label": "UACa20 RPMI (rep 3.3)", "bucket": "control"},
                "SRR24915361": {"label": "UACa20 RPMI (rep 3.4)", "bucket": "control"},
                # UACa20 Sabouraud - GSM7476302
                "SRR24915378": {"label": "UACa20 Sabouraud (rep 1.1)", "bucket": "basic_biology"},
                "SRR24915379": {"label": "UACa20 Sabouraud (rep 1.2)", "bucket": "basic_biology"},
                "SRR24915380": {"label": "UACa20 Sabouraud (rep 1.3)", "bucket": "basic_biology"},
                "SRR24915381": {"label": "UACa20 Sabouraud (rep 1.4)", "bucket": "basic_biology"},
                # UACa20 Sabouraud - GSM7476303
                "SRR24915374": {"label": "UACa20 Sabouraud (rep 2.1)", "bucket": "basic_biology"},
                "SRR24915375": {"label": "UACa20 Sabouraud (rep 2.2)", "bucket": "basic_biology"},
                "SRR24915376": {"label": "UACa20 Sabouraud (rep 2.3)", "bucket": "basic_biology"},
                "SRR24915377": {"label": "UACa20 Sabouraud (rep 2.4)", "bucket": "basic_biology"},
                # UACa20 Sabouraud - GSM7476304
                "SRR24915370": {"label": "UACa20 Sabouraud (rep 3.1)", "bucket": "basic_biology"},
                "SRR24915371": {"label": "UACa20 Sabouraud (rep 3.2)", "bucket": "basic_biology"},
                "SRR24915372": {"label": "UACa20 Sabouraud (rep 3.3)", "bucket": "basic_biology"},
                "SRR24915373": {"label": "UACa20 Sabouraud (rep 3.4)", "bucket": "basic_biology"},
            },
        },
        "Simm_2022": {
            "category": "Antifungal Response",
            "pmid": "35412372",
            "path_style": "direct",
            "control": "SRR14758158",
            "conditions": {
                # Control (untreated) - GSM5363119 (2 runs)
                "SRR14758158": {"label": "Control (rep 1a)", "bucket": "control"},
                "SRR14758159": {"label": "Control (rep 1b)", "bucket": "control"},
                # Control - GSM5363120
                "SRR14758160": {"label": "Control (rep 2a)", "bucket": "control"},
                "SRR14758161": {"label": "Control (rep 2b)", "bucket": "control"},
                # Control - GSM5363121
                "SRR14758162": {"label": "Control (rep 3a)", "bucket": "control"},
                "SRR14758163": {"label": "Control (rep 3b)", "bucket": "control"},
                # Pyrvinium pamoate - GSM5363122
                "SRR14758164": {"label": "Pyrvinium pamoate (rep 1a)", "bucket": "kill_candida"},
                "SRR14758165": {"label": "Pyrvinium pamoate (rep 1b)", "bucket": "kill_candida"},
                # Pyrvinium pamoate - GSM5363123
                "SRR14758166": {"label": "Pyrvinium pamoate (rep 2a)", "bucket": "kill_candida"},
                "SRR14758167": {"label": "Pyrvinium pamoate (rep 2b)", "bucket": "kill_candida"},
                # Pyrvinium pamoate - GSM5363124
                "SRR14758168": {"label": "Pyrvinium pamoate (rep 3a)", "bucket": "kill_candida"},
                "SRR14758169": {"label": "Pyrvinium pamoate (rep 3b)", "bucket": "kill_candida"},
            },
        },
    },
    "C_glabrata_CBS138": {
        "Linde_2015": {
            "category": "Stress Response",
            "pmid": "25586221",
            "path_style": "direct",
            "control": "SRR1582640",
            "conditions": {
                # YPD control
                "SRR1582640": {"label": "YPD Control (rep 1)", "bucket": "control"},
                "SRR1582641": {"label": "YPD Control (rep 2)", "bucket": "control"},
                # pH 4 (acidic)
                "SRR1582643": {"label": "pH 4 (rep 1)", "bucket": "stress"},
                "SRR1582644": {"label": "pH 4 (rep 2)", "bucket": "stress"},
                # pH 8 (alkaline)
                "SRR1582646": {"label": "pH 8 (rep 1)", "bucket": "stress"},
                "SRR1582647": {"label": "pH 8 (rep 2)", "bucket": "stress"},
                # Nitrosative stress
                "SRR1582648": {"label": "Nitrosative (rep 1)", "bucket": "stress"},
                "SRR1582649": {"label": "Nitrosative (rep 2)", "bucket": "stress"},
                # Oxidative stress
                "SRR1582650": {"label": "Oxidative (rep 1)", "bucket": "stress"},
                "SRR1582651": {"label": "Oxidative (rep 2)", "bucket": "stress"},
            },
        },
        "Bhakt_2022": {
            "category": "Antifungal Response",
            "pmid": "36108742",
            "path_style": "direct",
            "control": "SRR19158466",
            "conditions": {
                # WT CAA (control)
                "SRR19158466": {"label": "WT CAA (rep 1)", "bucket": "control"},
                "SRR19158465": {"label": "WT CAA (rep 2)", "bucket": "control"},
                # WT Caspofungin
                "SRR19158464": {"label": "WT Caspofungin (rep 1)", "bucket": "kill_candida"},
                "SRR19158463": {"label": "WT Caspofungin (rep 2)", "bucket": "kill_candida"},
                # set4Δ CAA
                "SRR19158461": {"label": "set4Δ CAA (rep 2)", "bucket": "basic_biology"},
                # set4Δ Caspofungin
                "SRR19158460": {"label": "set4Δ Caspofungin (rep 1)", "bucket": "kill_candida"},
                "SRR19158459": {"label": "set4Δ Caspofungin (rep 2)", "bucket": "kill_candida"},
            },
        },
        "Kumar_2024": {
            "category": "Immune Response",
            "pmid": "38632999",
            "path_style": "direct",
            "control": "SRR24895750",
            "conditions": {
                # WT RPMI 2h (control) - GSM7473113
                "SRR24895750": {"label": "WT RPMI 2h (rep 1)", "bucket": "control"},
                # WT RPMI 2h - GSM7473117
                "SRR24895747": {"label": "WT RPMI 2h (rep 2)", "bucket": "control"},
                # WT Macrophage 2h - GSM7473114
                "SRR24895745": {"label": "WT Macrophage 2h (rep 1)", "bucket": "kill_candida"},
                # WT Macrophage 2h - GSM7473118
                "SRR24895743": {"label": "WT Macrophage 2h (rep 2)", "bucket": "kill_candida"},
                # WT Macrophage 10h - GSM7473126
                "SRR24895738": {"label": "WT Macrophage 10h (rep 2)", "bucket": "kill_candida"},
                # snf2Δ RPMI 2h - GSM7473115
                "SRR24895746": {"label": "snf2Δ RPMI 2h (rep 1)", "bucket": "basic_biology"},
                # snf2Δ RPMI 2h - GSM7473119
                "SRR24895748": {"label": "snf2Δ RPMI 2h (rep 2)", "bucket": "basic_biology"},
                # snf2Δ RPMI 10h - GSM7473123
                "SRR24895740": {"label": "snf2Δ RPMI 10h (rep 1)", "bucket": "basic_biology"},
                # snf2Δ RPMI 10h - GSM7473127
                "SRR24895751": {"label": "snf2Δ RPMI 10h (rep 2)", "bucket": "basic_biology"},
                # snf2Δ Macrophage 2h - GSM7473116
                "SRR24895744": {"label": "snf2Δ Macrophage 2h (rep 1)", "bucket": "kill_candida"},
                # snf2Δ Macrophage 2h - GSM7473120
                "SRR24895742": {"label": "snf2Δ Macrophage 2h (rep 2)", "bucket": "kill_candida"},
                # snf2Δ Macrophage 10h - GSM7473124
                "SRR24895737": {"label": "snf2Δ Macrophage 10h (rep 1)", "bucket": "kill_candida"},
                # snf2Δ Macrophage 10h - GSM7473128
                "SRR24895739": {"label": "snf2Δ Macrophage 10h (rep 2)", "bucket": "kill_candida"},
            },
        },
        "Ni_2023": {
            "category": "DNA Damage Response",
            "pmid": "37891489",
            "path_style": "direct",
            "control": "SRR24529963",
            "conditions": {
                # WT Untreated (no MMS)
                "SRR24529963": {"label": "WT Untreated (rep 1)", "bucket": "control"},
                "SRR24529962": {"label": "WT Untreated (rep 2)", "bucket": "control"},
                "SRR24529961": {"label": "WT Untreated (rep 3)", "bucket": "control"},
                # WT MMS treated
                "SRR24529954": {"label": "WT MMS (rep 1)", "bucket": "stress"},
                "SRR24529953": {"label": "WT MMS (rep 2)", "bucket": "stress"},
                "SRR24529952": {"label": "WT MMS (rep 3)", "bucket": "stress"},
                # ckb1Δ MMS
                "SRR24529951": {"label": "ckb1Δ MMS (rep 1)", "bucket": "stress"},
                "SRR24529950": {"label": "ckb1Δ MMS (rep 2)", "bucket": "stress"},
                # ckb2Δ Untreated
                "SRR24529956": {"label": "ckb2Δ Untreated (rep 2)", "bucket": "basic_biology"},
                "SRR24529955": {"label": "ckb2Δ Untreated (rep 3)", "bucket": "basic_biology"},
                # ckb2Δ MMS
                "SRR24529948": {"label": "ckb2Δ MMS (rep 1)", "bucket": "stress"},
                "SRR24529947": {"label": "ckb2Δ MMS (rep 2)", "bucket": "stress"},
                "SRR24529946": {"label": "ckb2Δ MMS (rep 3)", "bucket": "stress"},
            },
        },
        "Vu_2021": {
            "category": "Gene Regulation",
            "pmid": "34591857",
            "path_style": "direct",
            "control": "SRR15532683",
            "conditions": {
                # WT Control
                "SRR15532683": {"label": "WT (rep 1)", "bucket": "control"},
                "SRR15532684": {"label": "WT (rep 2)", "bucket": "control"},
                "SRR15532685": {"label": "WT (rep 3)", "bucket": "control"},
                # upc2a G898D mutant
                "SRR15532686": {"label": "upc2a G898D (rep 1)", "bucket": "basic_biology"},
                "SRR15532687": {"label": "upc2a G898D (rep 2)", "bucket": "basic_biology"},
                "SRR15532688": {"label": "upc2a G898D (rep 3)", "bucket": "basic_biology"},
            },
        },
    },
    "C_dubliniensis_CD36": {
        "Grumaz_2013": {
            "category": "Morphology",
            "pmid": "23547856",
            "path_style": "direct",
            "control": "SRR604750",
            "conditions": {
                # Yeast form (control)
                "SRR604750": {"label": "Yeast 30°C (rep 1)", "bucket": "control"},
                "SRR604752": {"label": "Yeast 30°C (rep 2)", "bucket": "control"},
                "SRR604753": {"label": "Yeast 30°C (rep 3)", "bucket": "control"},
                # Hyphal induction
                "SRR771365": {"label": "Hyphae 37°C (rep 1)", "bucket": "basic_biology"},
                "SRR771366": {"label": "Hyphae 37°C (rep 2)", "bucket": "basic_biology"},
            },
        },
        "Singh-Babakh_2021": {
            "category": "Gene Regulation",
            "pmid": "33723044",
            "path_style": "direct",
            "control": "SRR13833835",
            "conditions": {
                # Empty vector control
                "SRR13833835": {"label": "Empty vector (rep A)", "bucket": "control"},
                "SRR13833836": {"label": "Empty vector (rep B)", "bucket": "control"},
                # TYE7 overexpression
                "SRR13833829": {"label": "TYE7-OE (rep A)", "bucket": "basic_biology"},
                "SRR13833830": {"label": "TYE7-OE (rep B)", "bucket": "basic_biology"},
                # GAL4 overexpression
                "SRR13833831": {"label": "GAL4-OE (rep A)", "bucket": "basic_biology"},
                "SRR13833832": {"label": "GAL4-OE (rep B)", "bucket": "basic_biology"},
                # GLK1 overexpression
                "SRR13833833": {"label": "GLK1-OE (rep A)", "bucket": "basic_biology"},
                "SRR13833834": {"label": "GLK1-OE (rep B)", "bucket": "basic_biology"},
            },
        },
    },
    "C_parapsilosis_CDC317": {
        "Holland_2014": {
            "category": "Biofilm/Transcription Factors",
            "pmid": "24586159",
            "path_style": "old_direct",  # old style without HapA subdirectory
            "control": "wt_plnk_1",
            "conditions": {
                # Wild-type planktonic (control)
                "wt_plnk_1": {"label": "WT Planktonic (rep 1)", "bucket": "control"},
                "wt_plnk_2": {"label": "WT Planktonic (rep 2)", "bucket": "control"},
                "wt_plnk_3": {"label": "WT Planktonic (rep 3)", "bucket": "control"},
                # Wild-type biofilm
                "wt_film_1": {"label": "WT Biofilm (rep 1)", "bucket": "basic_biology"},
                "wt_film_2": {"label": "WT Biofilm (rep 2)", "bucket": "basic_biology"},
                "wt_film_3": {"label": "WT Biofilm (rep 3)", "bucket": "basic_biology"},
                # Transcription factor mutants
                "ace2_1": {"label": "ace2Δ (rep 1)", "bucket": "basic_biology"},
                "ace2_2": {"label": "ace2Δ (rep 2)", "bucket": "basic_biology"},
                "ace2_3": {"label": "ace2Δ (rep 3)", "bucket": "basic_biology"},
                "cph2_1": {"label": "cph2Δ (rep 1)", "bucket": "basic_biology"},
                "cph2_2": {"label": "cph2Δ (rep 2)", "bucket": "basic_biology"},
                "cph2_3": {"label": "cph2Δ (rep 3)", "bucket": "basic_biology"},
                "efg1_1": {"label": "efg1Δ (rep 1)", "bucket": "basic_biology"},
                "efg1_2": {"label": "efg1Δ (rep 2)", "bucket": "basic_biology"},
                "efg1_3": {"label": "efg1Δ (rep 3)", "bucket": "basic_biology"},
                "czf1_1": {"label": "czf1Δ (rep 1)", "bucket": "basic_biology"},
                "czf1_2": {"label": "czf1Δ (rep 2)", "bucket": "basic_biology"},
                "czf1_3": {"label": "czf1Δ (rep 3)", "bucket": "basic_biology"},
                "ume6_1": {"label": "ume6Δ (rep 1)", "bucket": "basic_biology"},
                "ume6_2": {"label": "ume6Δ (rep 2)", "bucket": "basic_biology"},
                "ume6_3": {"label": "ume6Δ (rep 3)", "bucket": "basic_biology"},
            },
        },
        "Connolly_2013": {
            "category": "Colony Morphology",
            "pmid": "23895281",
            "path_style": "direct",
            "control": "SRR575493",
            "conditions": {
                # WT Control
                "SRR575493": {"label": "WT (rep 1)", "bucket": "control"},
                "SRR575494": {"label": "WT (rep 2)", "bucket": "control"},
                "SRR575495": {"label": "WT (rep 3)", "bucket": "control"},
                # efg1Δ concentric colony
                "SRR575496": {"label": "efg1Δ Concentric (rep 1)", "bucket": "basic_biology"},
                "SRR575497": {"label": "efg1Δ Concentric (rep 2)", "bucket": "basic_biology"},
                "SRR575498": {"label": "efg1Δ Concentric (rep 3)", "bucket": "basic_biology"},
                # efg1Δ smooth colony
                "SRR575499": {"label": "efg1Δ Smooth (rep 1)", "bucket": "basic_biology"},
                "SRR575500": {"label": "efg1Δ Smooth (rep 2)", "bucket": "basic_biology"},
            },
        },
        "Guida_2011": {
            "category": "Morphology/Media",
            "pmid": "22192698",
            "path_style": "direct",
            "control": "SRR352254",
            "conditions": {
                # WT YPD 30°C O21 (normoxia)
                "SRR352254": {"label": "WT YPD 30°C (rep 1)", "bucket": "control"},
                "SRR352256": {"label": "WT YPD 30°C (rep 2)", "bucket": "control"},
                "SRR352257": {"label": "WT YPD 30°C (rep 3)", "bucket": "control"},
                "SRR352259": {"label": "WT YPD 30°C (rep 4)", "bucket": "control"},
                "SRR352261": {"label": "WT YPD 30°C (rep 5)", "bucket": "control"},
                "SRR352264": {"label": "WT YPD 30°C (rep 6)", "bucket": "control"},
                "SRR352266": {"label": "WT YPD 30°C (rep 7)", "bucket": "control"},
                "SRR352278": {"label": "WT YPD 30°C (rep 8)", "bucket": "control"},
                # WT BMW 30°C O21
                "SRR352253": {"label": "WT BMW 30°C (rep 1)", "bucket": "basic_biology"},
                "SRR352255": {"label": "WT BMW 30°C (rep 2)", "bucket": "basic_biology"},
                "SRR352258": {"label": "WT BMW 30°C (rep 3)", "bucket": "basic_biology"},
                "SRR352260": {"label": "WT BMW 30°C (rep 4)", "bucket": "basic_biology"},
                "SRR352262": {"label": "WT BMW 30°C (rep 5)", "bucket": "basic_biology"},
                "SRR352265": {"label": "WT BMW 30°C (rep 6)", "bucket": "basic_biology"},
                "SRR352277": {"label": "WT BMW 30°C (rep 7)", "bucket": "basic_biology"},
                # WT YPD 37°C O21
                "SRR352268": {"label": "WT YPD 37°C", "bucket": "basic_biology"},
                # WT Media mix 30°C-37°C O21
                "SRR352269": {"label": "WT Media mix 30-37°C", "bucket": "basic_biology"},
                # WT YPD 30°C O1 (hypoxia)
                "SRR352267": {"label": "WT YPD 30°C Hypoxia (rep 1)", "bucket": "stress"},
                "SRR352270": {"label": "WT YPD 30°C Hypoxia (rep 2)", "bucket": "stress"},
                "SRR352271": {"label": "WT YPD 30°C Hypoxia (rep 3)", "bucket": "stress"},
                "SRR352274": {"label": "WT YPD 30°C Hypoxia (rep 4)", "bucket": "stress"},
                # WT YPD 30°C O21
                "SRR352273": {"label": "WT YPD 30°C Normoxia (rep 9)", "bucket": "control"},
                "SRR352276": {"label": "WT YPD 30°C Normoxia (rep 10)", "bucket": "control"},
                # upc2Δ YPD 30°C Hypoxia
                "SRR352272": {"label": "upc2Δ YPD 30°C Hypoxia (rep 1)", "bucket": "stress"},
                "SRR352275": {"label": "upc2Δ YPD 30°C Hypoxia (rep 2)", "bucket": "stress"},
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


def _map_chromosome_for_bigwig(chromosome: str, hts_key: str) -> Optional[str]:
    """
    Map chromosome names from database format to bigwig format.

    Each organism has different chromosome naming conventions:
    - C. albicans: Ca22chr1A_C_albicans_SC5314
    - C. auris: Chr1_C_auris_B8441
    - C. glabrata: ChrA_C_glabrata_CBS138
    - C. dubliniensis: Chr1_C_dubliniensis_CD36
    - C. parapsilosis: Chr1_C_parapsilosis_CDC317

    Database may have various formats that need to be mapped.
    """
    import re

    # Handle C. albicans specifically (has Ca20/Ca21/Ca22 assemblies)
    if hts_key == "C_albicans_SC5314":
        # Already Ca22 format
        if chromosome.startswith("Ca22chr"):
            return chromosome

        # Map Ca21 format: Ca21chr2_C_albicans_SC5314 -> Ca22chr2A_C_albicans_SC5314
        match = re.match(r"Ca21chr(\d+|R)(_C_albicans_SC5314)?", chromosome)
        if match:
            chr_id = match.group(1)
            return f"Ca22chr{chr_id}A_C_albicans_SC5314"

        # Map Ca20 format: Ca20chr2 -> Ca22chr2A_C_albicans_SC5314
        match = re.match(r"Ca20chr(\d+|R)", chromosome)
        if match:
            chr_id = match.group(1)
            return f"Ca22chr{chr_id}A_C_albicans_SC5314"

        # Map chrR format (ribosomal)
        if "chrR" in chromosome:
            return "Ca22chrRA_C_albicans_SC5314"

        return None

    # Handle C. auris
    if hts_key == "C_auris_B8441":
        # Already correct format
        if chromosome.startswith("Chr") and "_C_auris_B8441" in chromosome:
            return chromosome
        # Map simple format: Chr1 -> Chr1_C_auris_B8441
        match = re.match(r"Chr(\d+)", chromosome)
        if match:
            return f"Chr{match.group(1)}_C_auris_B8441"
        return chromosome if "Chr" in chromosome else None

    # Handle C. glabrata
    if hts_key == "C_glabrata_CBS138":
        # Already correct format
        if chromosome.startswith("Chr") and "_C_glabrata_CBS138" in chromosome:
            return chromosome
        # Map simple format: ChrA -> ChrA_C_glabrata_CBS138
        match = re.match(r"Chr([A-Z])", chromosome)
        if match:
            return f"Chr{match.group(1)}_C_glabrata_CBS138"
        return chromosome if "Chr" in chromosome else None

    # Handle C. dubliniensis
    if hts_key == "C_dubliniensis_CD36":
        # Already correct format
        if chromosome.startswith("Chr") and "_C_dubliniensis_CD36" in chromosome:
            return chromosome
        # Map simple format: Chr1 -> Chr1_C_dubliniensis_CD36
        match = re.match(r"Chr(\d+|R)", chromosome)
        if match:
            return f"Chr{match.group(1)}_C_dubliniensis_CD36"
        return chromosome if "Chr" in chromosome else None

    # Handle C. parapsilosis (uses Contig names, not Chr)
    if hts_key == "C_parapsilosis_CDC317":
        # Already correct format with suffix
        if "_C_parapsilosis_CDC317" in chromosome:
            return chromosome
        # Map contig format: Contig005504 -> Contig005504_C_parapsilosis_CDC317
        match = re.match(r"(Contig\d+)", chromosome)
        if match:
            return f"{match.group(1)}_C_parapsilosis_CDC317"
        # Return as-is if it's a contig
        if chromosome.startswith("Contig"):
            return f"{chromosome}_C_parapsilosis_CDC317"
        return None

    # Unknown organism - return as-is
    return chromosome


def _map_chromosome_to_ca22(chromosome: str) -> Optional[str]:
    """
    Legacy function for C. albicans only.
    Kept for backward compatibility with existing code.
    """
    return _map_chromosome_for_bigwig(chromosome, "C_albicans_SC5314")


def _get_bigwig_path(
    base_path: Path,
    study: str,
    condition: str,
    study_info: dict,
    haplotype: str = "HapA"
) -> Path:
    """Construct path to bigwig file based on path style."""
    if study_info["path_style"] == "old":
        # C. albicans old style: {study}/HapA/{cond}/sorted_hits_bam2wig/sorted_hits.bigwig
        return base_path / study / haplotype / condition / "sorted_hits_bam2wig" / "sorted_hits.bigwig"
    elif study_info["path_style"] == "old_direct":
        # Old style without haplotype: {study}/{cond}/sorted_hits_bam2wig/sorted_hits.bigwig
        return base_path / study / condition / "sorted_hits_bam2wig" / "sorted_hits.bigwig"
    elif study_info["path_style"] == "lohse":
        # Lohse_2016 style: {study}/HapA/{cond}/{cond}_bam2wig/{cond}.bigwig
        return base_path / study / haplotype / condition / f"{condition}_bam2wig" / f"{condition}.bigwig"
    elif study_info["path_style"] == "direct":
        # Non-haplotype organisms: {study}/{cond}/{cond}_sorted_hits.bigwig
        return base_path / study / condition / f"{condition}_sorted_hits.bigwig"
    else:  # new style (C. albicans)
        # C. albicans new style: {study}/HapA/{cond}/{cond}_sorted_hits.bigwig
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


# ============================================================================
# Multi-organism Expression Details (follows locus endpoint pattern)
# ============================================================================

# Mapping from database organism names to HTS directory keys
ORGANISM_TO_HTS_KEY = {
    "Candida albicans SC5314": "C_albicans_SC5314",
    "Candida auris B8441": "C_auris_B8441",
    "Candida glabrata CBS138": "C_glabrata_CBS138",
    "Candida dubliniensis CD36": "C_dubliniensis_CD36",
    "Candida parapsilosis CDC317": "C_parapsilosis_CDC317",
}


def _get_expression_for_organism(
    db: Session,
    feature: Feature,
    organism_name: str
) -> Optional[ExpressionDetailsForOrganism]:
    """
    Get expression data for a specific feature/organism.

    Returns ExpressionDetailsForOrganism or None if no data available.
    """
    if not PYBIGWIG_AVAILABLE:
        return None

    # Map organism name to HTS directory key
    hts_key = ORGANISM_TO_HTS_KEY.get(organism_name)
    if not hts_key:
        return None

    # Check if we have studies configured for this organism
    studies_config = EXPRESSION_STUDIES.get(hts_key, {})
    if not studies_config:
        return None

    # Get base path for HTS data
    base_path = HTS_BASE_PATHS.get(hts_key)
    if not base_path or not base_path.exists():
        return None

    # Get gene location for this feature (pass hts_key for chromosome mapping)
    location_info = _get_gene_location_for_feature(db, feature, hts_key)
    if not location_info:
        return None

    chromosome, start, end = location_info

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

    # Only return data if we have studies
    if not studies:
        return None

    # Calculate summary statistics
    max_up = max(all_fold_changes) if all_fold_changes else None
    max_down = min(all_fold_changes) if all_fold_changes else None

    return ExpressionDetailsForOrganism(
        gene_name=feature.gene_name or feature.feature_name,
        feature_name=feature.feature_name,
        description=feature.headline,
        chromosome=chromosome,
        start=start,
        end=end,
        studies=studies,
        total_conditions=total_conditions,
        max_upregulation=max_up,
        max_downregulation=max_down,
        warnings=warnings
    )


def _get_gene_location_for_feature(
    db: Session,
    feature: Feature,
    hts_key: str = "C_albicans_SC5314"
) -> Optional[Tuple[str, int, int]]:
    """
    Get gene location info for a specific feature.
    Returns: (chromosome, start, end) or None

    Args:
        db: Database session
        feature: The feature to get location for
        hts_key: HTS directory key for chromosome mapping (e.g., "C_albicans_SC5314")
    """
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

    # Find the best location (prioritize newer assemblies)
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

        # Determine priority (C. albicans has specific assembly versions)
        priority = 0
        if hts_key == "C_albicans_SC5314":
            if chr_name.startswith("Ca22chr"):
                priority = 4
            elif chr_name.startswith("Ca21chr"):
                priority = 3
            elif chr_name.startswith("Ca20chr"):
                priority = 2
            elif "chr" in chr_name.lower():
                priority = 1
        elif hts_key == "C_parapsilosis_CDC317":
            # C. parapsilosis uses Contig names
            if chr_name.startswith("Contig"):
                priority = 2
            elif "contig" in chr_name.lower():
                priority = 1
        else:
            # For other organisms, prefer chromosome over contig
            if chr_name.startswith("Chr"):
                priority = 2
            elif "chr" in chr_name.lower():
                priority = 1

        if priority > best_priority:
            best_priority = priority
            best_location = loc
            best_chromosome = chr_name

    if not best_location or not best_chromosome:
        return None

    # Map chromosome name to bigwig format for this organism
    mapped_chromosome = _map_chromosome_for_bigwig(best_chromosome, hts_key)
    if not mapped_chromosome:
        mapped_chromosome = best_chromosome

    return (mapped_chromosome, best_location.start_coord, best_location.stop_coord)


def get_expression_details_by_organism(
    db: Session,
    name: str
) -> ExpressionDetailsResponse:
    """
    Get expression data for a gene, grouped by organism.

    This follows the same pattern as other locus endpoints
    (go_details, phenotype_details, etc.)

    Args:
        db: Database session
        name: Gene name, feature name, or dbxref_id

    Returns:
        ExpressionDetailsResponse with data keyed by organism name
    """
    n = name.strip()

    # Find features matching the name (case-insensitive)
    features = (
        db.query(Feature)
        .join(Seq, Seq.feature_no == Feature.feature_no)
        .join(Organism, Organism.organism_no == Feature.organism_no)
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
                func.upper(Feature.dbxref_id) == func.upper(n),
            )
        )
        .filter(func.lower(Feature.feature_type) != 'allele')
        .filter(Seq.is_seq_current == 'Y')
        .all()
    )

    if not features:
        return ExpressionDetailsResponse(results={})

    # Group by organism and get expression data
    results: dict[str, ExpressionDetailsForOrganism] = {}
    seen_organisms: set[str] = set()

    for feature in features:
        # Get organism name
        organism = feature.organism
        if not organism:
            continue

        organism_name = organism.organism_name
        if organism_name in seen_organisms:
            continue
        seen_organisms.add(organism_name)

        # Get expression data for this feature/organism
        expr_data = _get_expression_for_organism(db, feature, organism_name)
        if expr_data:
            results[organism_name] = expr_data

    return ExpressionDetailsResponse(results=results)


# ============================================================================
# Similar Expression Genes
# ============================================================================

# Try to import scipy for correlation computation
try:
    from scipy.stats import pearsonr, spearmanr
    from scipy.spatial.distance import cosine as cosine_distance
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    logger.warning("scipy not installed - similar genes analysis will be unavailable")

# Expression profile cache: {organism: {feature_name: {condition_id: fold_change}}}
# This is a simple dict cache that persists for the lifetime of the process
_expression_profile_cache: Dict[str, Dict[str, Dict[str, float]]] = {}

# Directory for pre-computed cache files
EXPRESSION_CACHE_DIR = Path("/data/cache/expression")


def _load_expression_cache_from_file(organism_key: str) -> Optional[Dict[str, Dict[str, float]]]:
    """
    Load pre-computed expression profiles from cache file.

    Cache files are built by scripts/build_expression_cache.py

    Returns:
        Dict mapping feature_name to expression profile, or None if not found
    """
    cache_file = EXPRESSION_CACHE_DIR / f"expression_profiles_{organism_key}.json"

    if not cache_file.exists():
        logger.info(f"Cache file not found: {cache_file}")
        return None

    try:
        logger.info(f"Loading expression cache from {cache_file}")
        with open(cache_file, 'r') as f:
            profiles = json.load(f)
        logger.info(f"Loaded {len(profiles)} gene profiles from cache")
        return profiles
    except Exception as e:
        logger.error(f"Error loading cache file {cache_file}: {e}")
        return None


def _get_all_condition_ids(organism_key: str) -> List[str]:
    """Get all condition IDs (excluding controls) for an organism, sorted for consistency."""
    studies_config = EXPRESSION_STUDIES.get(organism_key, {})
    condition_ids = []

    for study_id, study_info in studies_config.items():
        control_id = study_info["control"]
        for cond_id in study_info["conditions"]:
            if cond_id != control_id:
                # Use study_id + cond_id as unique key
                condition_ids.append(f"{study_id}:{cond_id}")

    return sorted(condition_ids)


def _build_expression_profile(
    db: Session,
    feature: Feature,
    organism_key: str,
    base_path: Path,
    studies_config: dict,
    hts_key: str
) -> Optional[Dict[str, float]]:
    """
    Build expression profile (fold changes) for a single gene across all conditions.

    Returns a dict mapping condition_id to fold_change, or None if location not found.
    """
    # Get gene location
    location_info = _get_gene_location_for_feature(db, feature, hts_key)
    if not location_info:
        return None

    chromosome, start, end = location_info
    profile: Dict[str, float] = {}

    for study_id, study_info in studies_config.items():
        control_id = study_info["control"]
        control_path = _get_bigwig_path(base_path, study_id, control_id, study_info)

        # Get control expression value
        control_value = _get_expression_value(control_path, chromosome, start, end)
        if control_value is None or control_value <= 0:
            continue

        # Process conditions
        for cond_id, cond_info in study_info["conditions"].items():
            if cond_id == control_id:
                continue

            cond_path = _get_bigwig_path(base_path, study_id, cond_id, study_info)
            cond_value = _get_expression_value(cond_path, chromosome, start, end)

            if cond_value is None:
                continue

            fold_change = cond_value / control_value
            condition_key = f"{study_id}:{cond_id}"
            profile[condition_key] = fold_change

    return profile if profile else None


def _build_all_expression_profiles(
    db: Session,
    organism_key: str
) -> Dict[str, Dict[str, float]]:
    """
    Build expression profiles for all genes in an organism.

    Returns dict mapping feature_name to expression profile.

    Loading order:
    1. In-memory cache (fastest, for repeated queries)
    2. Pre-computed cache file (fast, built by build_expression_cache.py)
    3. Real-time computation (slow, fallback only)
    """
    global _expression_profile_cache

    # 1. Return from in-memory cache if available
    if organism_key in _expression_profile_cache:
        return _expression_profile_cache[organism_key]

    # 2. Try to load from pre-computed cache file
    cached_profiles = _load_expression_cache_from_file(organism_key)
    if cached_profiles:
        _expression_profile_cache[organism_key] = cached_profiles
        return cached_profiles

    # 3. Fall back to real-time computation (slow)
    logger.warning(
        f"No cache file found for {organism_key}. "
        f"Building profiles in real-time (this will be slow). "
        f"Run scripts/build_expression_cache.py to pre-compute."
    )

    # Get base path for HTS data
    base_path = HTS_BASE_PATHS.get(organism_key)
    if not base_path or not base_path.exists():
        return {}

    studies_config = EXPRESSION_STUDIES.get(organism_key, {})
    if not studies_config:
        return {}

    # Map to HTS key for chromosome mapping
    hts_key = organism_key

    # Get organism name for querying
    organism_name_map = {v: k for k, v in ORGANISM_TO_HTS_KEY.items()}
    organism_name = organism_name_map.get(organism_key)
    if not organism_name:
        return {}

    # Get all ORF features for this organism
    organism_obj = db.query(Organism).filter(
        Organism.organism_name == organism_name
    ).first()
    if not organism_obj:
        return {}

    # Query all ORF features with locations
    features = (
        db.query(Feature)
        .filter(
            Feature.organism_no == organism_obj.organism_no,
            Feature.feature_type == 'ORF'
        )
        .all()
    )

    logger.info(f"Processing {len(features)} ORF features for {organism_key}")

    # Build profiles for all features
    profiles: Dict[str, Dict[str, float]] = {}
    processed = 0

    for feature in features:
        profile = _build_expression_profile(
            db, feature, organism_key, base_path, studies_config, hts_key
        )
        if profile:
            profiles[feature.feature_name] = profile
            processed += 1

        if processed % 500 == 0:
            logger.info(f"Processed {processed} genes for {organism_key}")

    logger.info(f"Built profiles for {len(profiles)} genes in {organism_key}")

    # Cache the results
    _expression_profile_cache[organism_key] = profiles

    return profiles


def _compute_correlation(
    profile1: Dict[str, float],
    profile2: Dict[str, float],
    metric: str = "pearson",
    min_conditions: int = 5
) -> Optional[Tuple[float, Optional[float], int]]:
    """
    Compute correlation between two expression profiles.

    Args:
        profile1: First expression profile
        profile2: Second expression profile
        metric: 'pearson', 'spearman', or 'cosine'
        min_conditions: Minimum shared conditions required

    Returns:
        (correlation, p_value, shared_conditions) or None if insufficient data
    """
    if not SCIPY_AVAILABLE:
        return None

    # Find shared conditions
    shared_keys = set(profile1.keys()) & set(profile2.keys())
    if len(shared_keys) < min_conditions:
        return None

    # Extract values for shared conditions (sorted for consistency)
    sorted_keys = sorted(shared_keys)
    values1 = [profile1[k] for k in sorted_keys]
    values2 = [profile2[k] for k in sorted_keys]

    try:
        if metric == "pearson":
            corr, p_value = pearsonr(values1, values2)
        elif metric == "spearman":
            corr, p_value = spearmanr(values1, values2)
        elif metric == "cosine":
            # Cosine similarity = 1 - cosine distance
            corr = 1.0 - cosine_distance(values1, values2)
            p_value = None  # No p-value for cosine similarity
        else:
            return None

        return (corr, p_value, len(shared_keys))
    except Exception as e:
        logger.debug(f"Correlation computation failed: {e}")
        return None


def get_similar_expression_genes(
    db: Session,
    gene_name: str,
    organism: str = "C_albicans_SC5314_A22",
    limit: int = 20,
    metric: str = "pearson",
    min_conditions: int = 5
) -> SimilarGenesResponse:
    """
    Find genes with similar expression profiles to the query gene.

    Args:
        db: Database session
        gene_name: Query gene name or systematic name
        organism: Organism tag
        limit: Maximum number of results (1-100)
        metric: Similarity metric ('pearson', 'spearman', 'cosine')
        min_conditions: Minimum shared conditions required

    Returns:
        SimilarGenesResponse with ranked list of similar genes
    """
    import time
    start_time = time.time()

    # Validate inputs
    if not PYBIGWIG_AVAILABLE:
        return SimilarGenesResponse(
            success=False,
            error="Expression analysis unavailable: pyBigWig not installed"
        )

    if not SCIPY_AVAILABLE:
        return SimilarGenesResponse(
            success=False,
            error="Similar genes analysis unavailable: scipy not installed"
        )

    if metric not in ("pearson", "spearman", "cosine"):
        return SimilarGenesResponse(
            success=False,
            error=f"Invalid metric: {metric}. Use 'pearson', 'spearman', or 'cosine'"
        )

    limit = max(1, min(100, limit))
    min_conditions = max(1, min_conditions)

    # Get organism directory key
    organism_key = _get_organism_from_tag(organism)

    # Check if studies exist for this organism
    if organism_key not in EXPRESSION_STUDIES:
        return SimilarGenesResponse(
            success=False,
            error=f"No expression studies configured for organism: {organism}"
        )

    # Find the query gene feature
    query_feature = (
        db.query(Feature)
        .filter(
            (Feature.gene_name == gene_name) | (Feature.feature_name == gene_name)
        )
        .first()
    )

    if not query_feature:
        return SimilarGenesResponse(
            success=False,
            error=f"Gene '{gene_name}' not found"
        )

    # Build all expression profiles (uses cache)
    all_profiles = _build_all_expression_profiles(db, organism_key)
    if not all_profiles:
        return SimilarGenesResponse(
            success=False,
            error=f"Could not build expression profiles for {organism}"
        )

    # Get query gene's profile
    query_profile = all_profiles.get(query_feature.feature_name)
    if not query_profile:
        return SimilarGenesResponse(
            success=False,
            error=f"No expression data available for '{gene_name}'"
        )

    # Compare against all other genes
    correlations: List[Tuple[str, float, Optional[float], int]] = []

    for feature_name, profile in all_profiles.items():
        # For C. albicans, skip old orf19.XXXX genes and B alleles (show only A22 A alleles)
        if organism_key == "C_albicans_SC5314" and (feature_name.startswith("orf19.") or feature_name.endswith("_B")):
            continue
        if feature_name == query_feature.feature_name:
            continue

        result = _compute_correlation(query_profile, profile, metric, min_conditions)
        if result:
            corr, p_value, shared = result
            correlations.append((feature_name, corr, p_value, shared))

    # Sort by correlation (descending for pearson/spearman, ascending distance for cosine)
    correlations.sort(key=lambda x: x[1], reverse=True)

    # Take top N
    top_correlations = correlations[:limit]

    # Fetch feature details for top results
    similar_genes: List[SimilarGene] = []
    feature_names = [c[0] for c in top_correlations]

    if feature_names:
        features_map = {
            f.feature_name: f
            for f in db.query(Feature).filter(
                Feature.feature_name.in_(feature_names)
            ).all()
        }

        for feature_name, corr, p_value, shared in top_correlations:
            feature = features_map.get(feature_name)
            similar_genes.append(SimilarGene(
                gene_name=feature.gene_name if feature else None,
                feature_name=feature_name,
                description=feature.headline if feature else None,
                correlation=round(corr, 4),
                p_value=round(p_value, 6) if p_value is not None else None,
                shared_conditions=shared
            ))

    computation_time = (time.time() - start_time) * 1000

    return SimilarGenesResponse(
        success=True,
        query_gene=query_feature.gene_name or query_feature.feature_name,
        query_feature_name=query_feature.feature_name,
        organism=organism,
        metric=metric,
        similar_genes=similar_genes,
        total_genes_compared=len(correlations),
        conditions_used=len(query_profile),
        computation_time_ms=round(computation_time, 2)
    )
