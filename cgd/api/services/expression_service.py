"""
Expression Data Service.

Reads RNA-seq bigwig files and calculates fold changes for gene expression analysis.
"""
from __future__ import annotations

import json
import logging
from collections import OrderedDict
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
    BatchGeneExpression,
    BatchExpressionResponse,
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
            "pmid": "20810668",
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
            "pmid": "23572557",
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
            "pmid": "23555196",
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
            "pmid": "38921373",
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
            "category": "Biofilm",
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
        "Iracane_2024_longRNA": {
            "category": "Mutation Comparison",
            "pmid": "38625945",
            "path_style": "direct",
            "control": "SRR27942832",
            "conditions": {
                "SRR27912204": {"label": "Iracane et al long RNA strain ago1-K361E (biol rep 1)", "bucket": "mutation"},
                "SRR27912324": {"label": "Iracane et al long RNA strain ago1-K361E (biol rep 2)", "bucket": "mutation"},
                "SRR27912626": {"label": "Iracane et al long RNA strain ago1-K361E (biol rep 3)", "bucket": "mutation"},
                "SRR27959451": {"label": "Iracane et al long RNA strain ago1 null mutant (biol rep 1)", "bucket": "mutation"},
                "SRR27959445": {"label": "Iracane et al long RNA strain ago1 null mutant (biol rep 2)", "bucket": "mutation"},
                "SRR27959444": {"label": "Iracane et al long RNA strain ago1 null mutant (biol rep 3)", "bucket": "mutation"},
                "SRR27926874": {"label": "Iracane et al long RNA strain ago1 null mutant (biol rep 4)", "bucket": "mutation"},
                "SRR27927865": {"label": "Iracane et al long RNA strain ago1 null mutant (biol rep 5)", "bucket": "mutation"},
                "SRR27942832": {"label": "Iracane et al long RNA control (biol rep 1)", "bucket": "control"},
                "SRR27959447": {"label": "Iracane et al long RNA control (biol rep 2)", "bucket": "control"},
                "SRR27959457": {"label": "Iracane et al long RNA control (biol rep 3)", "bucket": "control"},
                "SRR27928088": {"label": "Iracane et al long RNA control (biol rep 4)", "bucket": "control"},
                "SRR27928186": {"label": "Iracane et al long RNA control (biol rep 5)", "bucket": "control"},
                "SRR27928389": {"label": "Iracane et al long RNA control (biol rep 6)", "bucket": "control"},
            },
        },
        "Iracane_2024_sRNA": {
            "category": "Mutation Comparison",
            "pmid": "38625945",
            "path_style": "direct",
            "control": "SRR27911426",
            "conditions": {
                "SRR27911207": {"label": "Iracane et al short RNA strain ago1-K361E (biol rep 1)", "bucket": "mutation"},
                "SRR27911216": {"label": "Iracane et al short RNA strain ago1-K361E (biol rep 2)", "bucket": "mutation"},
                "SRR27911367": {"label": "Iracane et al short RNA strain ago1-K361E (biol rep 3)", "bucket": "mutation"},
                "SRR27911401": {"label": "Iracane et al short RNA strain ago1-K361E (biol rep 4)", "bucket": "mutation"},
                "SRR27911426": {"label": "Iracane et al short RNA control (biol rep 1)", "bucket": "control"},
                "SRR27911427": {"label": "Iracane et al short RNA control (biol rep 2)", "bucket": "control"},
                "SRR27911443": {"label": "Iracane et al short RNA control (biol rep 3)", "bucket": "control"},
                "SRR27911450": {"label": "Iracane et al short RNA control (biol rep 4)", "bucket": "control"},
                "SRR27911484": {"label": "Iracane et al short RNA control (biol rep 5)", "bucket": "control"},
                "SRR27911485": {"label": "Iracane et al short RNA control (biol rep 6)", "bucket": "control"},
            },
        },
        "Du_2015": {
            "category": "Stress Response",
            "pmid": "26350972",
            "ncbi_id": "GSE64659",
            "path_style": "direct",
            "control": "SRR2062587",
            "conditions": {
                # 5h timepoint
                "SRR2062587": {"label": "Glucose 5h", "bucket": "control", "group": "5h"},
                "SRR2062588": {"label": "GlcNAc 5h", "bucket": "basic_biology", "group": "5h"},
                "SRR2062586": {"label": "Sorbitol 5h", "bucket": "basic_biology", "group": "5h"},
                # 24h timepoint
                "SRR1740387": {"label": "Glucose 24h", "bucket": "control", "group": "24h"},
                "SRR1740388": {"label": "GlcNAc 24h", "bucket": "basic_biology", "group": "24h"},
                "SRR1740386": {"label": "Sorbitol 24h", "bucket": "basic_biology", "group": "24h"},
            },
        },
        "Menon_2026": {
            "category": "Gene Regulation",
            "pmid": "42003610",
            "ncbi_id": "GSE294295",
            "path_style": "direct",
            "control": "SRR33095670",
            "conditions": {
                # SC5314 WT: SLD baseline vs sulfur sources
                "SRR33095670": {"label": "WT SLD (rep 1)", "bucket": "control", "group": "WT sulfur"},
                "SRR33095671": {"label": "WT SLD (rep 2)", "bucket": "control", "group": "WT sulfur"},
                "SRR33095672": {"label": "WT SLD+Met/Cys (rep 1)", "bucket": "basic_biology", "group": "WT sulfur"},
                "SRR33095673": {"label": "WT SLD+Met/Cys (rep 2)", "bucket": "basic_biology", "group": "WT sulfur"},
                "SRR33095674": {"label": "WT SLD+taurine (rep 1)", "bucket": "basic_biology", "group": "WT sulfur"},
                "SRR33095675": {"label": "WT SLD+taurine (rep 2)", "bucket": "basic_biology", "group": "WT sulfur"},
                "SRR33095664": {"label": "WT SLD+ammonium sulfate (rep 1)", "bucket": "basic_biology", "group": "WT sulfur"},
                "SRR33095665": {"label": "WT SLD+ammonium sulfate (rep 2)", "bucket": "basic_biology", "group": "WT sulfur"},
                # SN250-gsh: SLD baseline vs glutathione
                "SRR33095663": {"label": "SN250 SLD (rep 1)", "bucket": "control", "group": "SN250 glutathione"},
                "SRR33095662": {"label": "SN250 SLD (rep 2)", "bucket": "control", "group": "SN250 glutathione"},
                "SRR33095661": {"label": "SN250 SLD+glutathione (rep 1)", "bucket": "basic_biology", "group": "SN250 glutathione"},
                "SRR33095660": {"label": "SN250 SLD+glutathione (rep 2)", "bucket": "basic_biology", "group": "SN250 glutathione"},
                # met32Δ-gsh: SLD baseline vs glutathione
                "SRR33095653": {"label": "met32Δ SLD (rep 1)", "bucket": "control", "group": "met32 glutathione"},
                "SRR33095652": {"label": "met32Δ SLD (rep 2)", "bucket": "control", "group": "met32 glutathione"},
                "SRR33095659": {"label": "met32Δ SLD+glutathione (rep 1)", "bucket": "basic_biology", "group": "met32 glutathione"},
                "SRR33095658": {"label": "met32Δ SLD+glutathione (rep 2)", "bucket": "basic_biology", "group": "met32 glutathione"},
                # Taurine: WT(SN250) vs met32Δ
                "SRR33095666": {"label": "WT(SN250) SLD+taurine (rep 1)", "bucket": "control", "group": "taurine met32"},
                "SRR33095667": {"label": "WT(SN250) SLD+taurine (rep 2)", "bucket": "control", "group": "taurine met32"},
                "SRR33095668": {"label": "met32Δ SLD+taurine (rep 1)", "bucket": "basic_biology", "group": "taurine met32"},
                "SRR33095669": {"label": "met32Δ SLD+taurine (rep 2)", "bucket": "basic_biology", "group": "taurine met32"},
            },
        },
        "Cravener_2023": {
            "category": "Strain Comparison",
            "pmid": "36696432",
            "ncbi_id": "PRJNA857655",
            "path_style": "direct",
            "control": "SRR20077262",
            "conditions": {
                # strain 12C
                "SRR20077262": {"label": "Cravener et al control strain 12C (Rep 1, HapA)", "bucket": "control", "group": "12C"},
                "SRR20077261": {"label": "Cravener et al control strain 12C (Rep 2, HapA)", "bucket": "control", "group": "12C"},
                "SRR20077260": {"label": "Cravener et al control strain 12C (Rep 3, HapA)", "bucket": "control", "group": "12C"},
                "SRR20077294": {"label": "Cravener et al efg1 strain 12C (Rep 1, HapA)", "bucket": "basic_biology", "group": "12C"},
                "SRR20077293": {"label": "Cravener et al efg1 strain 12C (Rep 2, HapA)", "bucket": "basic_biology", "group": "12C"},
                "SRR20077292": {"label": "Cravener et al efg1 strain 12C (Rep 3, HapA)", "bucket": "basic_biology", "group": "12C"},
                # strain 19F
                "SRR20077285": {"label": "Cravener et al control strain 19F (Rep 1, HapA)", "bucket": "control", "group": "19F"},
                "SRR20077259": {"label": "Cravener et al control strain 19F (Rep 2, HapA)", "bucket": "control", "group": "19F"},
                "SRR20077258": {"label": "Cravener et al control strain 19F (Rep 3, HapA)", "bucket": "control", "group": "19F"},
                "SRR20077254": {"label": "Cravener et al efg1 strain 19F (Rep 1, HapA)", "bucket": "basic_biology", "group": "19F"},
                "SRR20077253": {"label": "Cravener et al efg1 strain 19F (Rep 2, HapA)", "bucket": "basic_biology", "group": "19F"},
                "SRR20077252": {"label": "Cravener et al efg1 strain 19F (Rep 3, HapA)", "bucket": "basic_biology", "group": "19F"},
                # strain GC75
                "SRR20077210": {"label": "Cravener et al control strain GC75 (Rep 1, HapA)", "bucket": "control", "group": "GC75"},
                "SRR20077209": {"label": "Cravener et al control strain GC75 (Rep 2, HapA)", "bucket": "control", "group": "GC75"},
                "SRR20077208": {"label": "Cravener et al control strain GC75 (Rep 3, HapA)", "bucket": "control", "group": "GC75"},
                "SRR20077207": {"label": "Cravener et al efg1 strain GC75 (Rep 1, HapA)", "bucket": "basic_biology", "group": "GC75"},
                "SRR20077305": {"label": "Cravener et al efg1 strain GC75 (Rep 2, HapA)", "bucket": "basic_biology", "group": "GC75"},
                "SRR20077304": {"label": "Cravener et al efg1 strain GC75 (Rep 3, HapA)", "bucket": "basic_biology", "group": "GC75"},
                # strain L26
                "SRR20077288": {"label": "Cravener et al control strain L26 (Rep 1, HapA)", "bucket": "control", "group": "L26"},
                "SRR20077287": {"label": "Cravener et al control strain L26 (Rep 2, HapA)", "bucket": "control", "group": "L26"},
                "SRR20077286": {"label": "Cravener et al control strain L26 (Rep 3, HapA)", "bucket": "control", "group": "L26"},
                "SRR20077257": {"label": "Cravener et al efg1 strain L26 (Rep 1, HapA)", "bucket": "basic_biology", "group": "L26"},
                "SRR20077256": {"label": "Cravener et al efg1 strain L26 (Rep 2, HapA)", "bucket": "basic_biology", "group": "L26"},
                "SRR20077255": {"label": "Cravener et al efg1 strain L26 (Rep 3, HapA)", "bucket": "basic_biology", "group": "L26"},
                # strain P37005
                "SRR20077302": {"label": "Cravener et al control strain P37005 (Rep 1, HapA)", "bucket": "control", "group": "P37005"},
                "SRR20077301": {"label": "Cravener et al control strain P37005 (Rep 2, HapA)", "bucket": "control", "group": "P37005"},
                "SRR20077300": {"label": "Cravener et al control strain P37005 (Rep 3, HapA)", "bucket": "control", "group": "P37005"},
                "SRR20077296": {"label": "Cravener et al efg1 strain P37005 (Rep 1, HapA)", "bucket": "basic_biology", "group": "P37005"},
                "SRR20077271": {"label": "Cravener et al efg1 strain P37005 (Rep 2, HapA)", "bucket": "basic_biology", "group": "P37005"},
                "SRR20077269": {"label": "Cravener et al efg1 strain P37005 (Rep 3, HapA)", "bucket": "basic_biology", "group": "P37005"},
                # strain P37037
                "SRR20077299": {"label": "Cravener et al control strain P37037 (Rep 1, HapA)", "bucket": "control", "group": "P37037"},
                "SRR20077298": {"label": "Cravener et al control strain P37037 (Rep 2, HapA)", "bucket": "control", "group": "P37037"},
                "SRR20077297": {"label": "Cravener et al control strain P37037 (Rep 3, HapA)", "bucket": "control", "group": "P37037"},
                "SRR20077268": {"label": "Cravener et al efg1 strain P37037 (Rep 1, HapA)", "bucket": "basic_biology", "group": "P37037"},
                "SRR20077267": {"label": "Cravener et al efg1 strain P37037 (Rep 2, HapA)", "bucket": "basic_biology", "group": "P37037"},
                "SRR20077266": {"label": "Cravener et al efg1 strain P37037 (Rep 3, HapA)", "bucket": "basic_biology", "group": "P37037"},
                # strain P37039
                "SRR20077265": {"label": "Cravener et al control strain P37039 (Rep 1, HapA)", "bucket": "control", "group": "P37039"},
                "SRR20077264": {"label": "Cravener et al control strain P37039 (Rep 2, HapA)", "bucket": "control", "group": "P37039"},
                "SRR20077263": {"label": "Cravener et al control strain P37039 (Rep 3, HapA)", "bucket": "control", "group": "P37039"},
                "SRR20077291": {"label": "Cravener et al efg1 strain P37039 (Rep 1, HapA)", "bucket": "basic_biology", "group": "P37039"},
                "SRR20077290": {"label": "Cravener et al efg1 strain P37039 (Rep 2, HapA)", "bucket": "basic_biology", "group": "P37039"},
                "SRR20077289": {"label": "Cravener et al efg1 strain P37039 (Rep 3, HapA)", "bucket": "basic_biology", "group": "P37039"},
                # strain P57055
                "SRR20077282": {"label": "Cravener et al control strain P57055 (Rep 1, HapA)", "bucket": "control", "group": "P57055"},
                "SRR20077281": {"label": "Cravener et al control strain P57055 (Rep 2, HapA)", "bucket": "control", "group": "P57055"},
                "SRR20077280": {"label": "Cravener et al control strain P57055 (Rep 3, HapA)", "bucket": "control", "group": "P57055"},
                "SRR20077276": {"label": "Cravener et al efg1 strain P57055 (Rep 1, HapA)", "bucket": "basic_biology", "group": "P57055"},
                "SRR20077274": {"label": "Cravener et al efg1 strain P57055 (Rep 2, HapA)", "bucket": "basic_biology", "group": "P57055"},
                "SRR20077273": {"label": "Cravener et al efg1 strain P57055 (Rep 3, HapA)", "bucket": "basic_biology", "group": "P57055"},
                # strain P57072
                "SRR20077237": {"label": "Cravener et al control strain P57072 (Rep 1, HapA)", "bucket": "control", "group": "P57072"},
                "SRR20077236": {"label": "Cravener et al control strain P57072 (Rep 2, HapA)", "bucket": "control", "group": "P57072"},
                "SRR20077283": {"label": "Cravener et al control strain P57072 (Rep 3, HapA)", "bucket": "control", "group": "P57072"},
                "SRR20077279": {"label": "Cravener et al efg1 strain P57072 (Rep 1, HapA)", "bucket": "basic_biology", "group": "P57072"},
                "SRR20077278": {"label": "Cravener et al efg1 strain P57072 (Rep 2, HapA)", "bucket": "basic_biology", "group": "P57072"},
                "SRR20077277": {"label": "Cravener et al efg1 strain P57072 (Rep 3, HapA)", "bucket": "basic_biology", "group": "P57072"},
                # strain P75010
                "SRR20077251": {"label": "Cravener et al control strain P75010 (Rep 1, HapA)", "bucket": "control", "group": "P75010"},
                "SRR20077250": {"label": "Cravener et al control strain P75010 (Rep 2, HapA)", "bucket": "control", "group": "P75010"},
                "SRR20077248": {"label": "Cravener et al control strain P75010 (Rep 3, HapA)", "bucket": "control", "group": "P75010"},
                "SRR20077244": {"label": "Cravener et al efg1 strain P75010 (Rep 1, HapA)", "bucket": "basic_biology", "group": "P75010"},
                "SRR20077243": {"label": "Cravener et al efg1 strain P75010 (Rep 2, HapA)", "bucket": "basic_biology", "group": "P75010"},
                "SRR20077242": {"label": "Cravener et al efg1 strain P75010 (Rep 3, HapA)", "bucket": "basic_biology", "group": "P75010"},
                # strain P75016
                "SRR20077223": {"label": "Cravener et al control strain P75016 (Rep 1, HapA)", "bucket": "control", "group": "P75016"},
                "SRR20077222": {"label": "Cravener et al control strain P75016 (Rep 2, HapA)", "bucket": "control", "group": "P75016"},
                "SRR20077221": {"label": "Cravener et al control strain P75016 (Rep 3, HapA)", "bucket": "control", "group": "P75016"},
                "SRR20077216": {"label": "Cravener et al efg1 strain P75016 (Rep 1, HapA)", "bucket": "basic_biology", "group": "P75016"},
                "SRR20077215": {"label": "Cravener et al efg1 strain P75016 (Rep 2, HapA)", "bucket": "basic_biology", "group": "P75016"},
                "SRR20077214": {"label": "Cravener et al efg1 strain P75016 (Rep 3, HapA)", "bucket": "basic_biology", "group": "P75016"},
                # strain P75063
                "SRR20077220": {"label": "Cravener et al control strain P75063 (Rep 1, HapA)", "bucket": "control", "group": "P75063"},
                "SRR20077219": {"label": "Cravener et al control strain P75063 (Rep 2, HapA)", "bucket": "control", "group": "P75063"},
                "SRR20077218": {"label": "Cravener et al control strain P75063 (Rep 3, HapA)", "bucket": "control", "group": "P75063"},
                "SRR20077213": {"label": "Cravener et al efg1 strain P75063 (Rep 1, HapA)", "bucket": "basic_biology", "group": "P75063"},
                "SRR20077212": {"label": "Cravener et al efg1 strain P75063 (Rep 2, HapA)", "bucket": "basic_biology", "group": "P75063"},
                "SRR20077211": {"label": "Cravener et al efg1 strain P75063 (Rep 3, HapA)", "bucket": "basic_biology", "group": "P75063"},
                # strain P76067
                "SRR20077247": {"label": "Cravener et al control strain P76067 (Rep 1, HapA)", "bucket": "control", "group": "P76067"},
                "SRR20077246": {"label": "Cravener et al control strain P76067 (Rep 2, HapA)", "bucket": "control", "group": "P76067"},
                "SRR20077245": {"label": "Cravener et al control strain P76067 (Rep 3, HapA)", "bucket": "control", "group": "P76067"},
                "SRR20077241": {"label": "Cravener et al efg1 strain P76067 (Rep 1, HapA)", "bucket": "basic_biology", "group": "P76067"},
                "SRR20077240": {"label": "Cravener et al efg1 strain P76067 (Rep 2, HapA)", "bucket": "basic_biology", "group": "P76067"},
                "SRR20077239": {"label": "Cravener et al efg1 strain P76067 (Rep 3, HapA)", "bucket": "basic_biology", "group": "P76067"},
                # strain P78042
                "SRR20077233": {"label": "Cravener et al control strain P78042 (Rep 1, HapA)", "bucket": "control", "group": "P78042"},
                "SRR20077232": {"label": "Cravener et al control strain P78042 (Rep 2, HapA)", "bucket": "control", "group": "P78042"},
                "SRR20077231": {"label": "Cravener et al control strain P78042 (Rep 3, HapA)", "bucket": "control", "group": "P78042"},
                "SRR20077226": {"label": "Cravener et al efg1 strain P78042 (Rep 1, HapA)", "bucket": "basic_biology", "group": "P78042"},
                "SRR20077225": {"label": "Cravener et al efg1 strain P78042 (Rep 2, HapA)", "bucket": "basic_biology", "group": "P78042"},
                "SRR20077224": {"label": "Cravener et al efg1 strain P78042 (Rep 3, HapA)", "bucket": "basic_biology", "group": "P78042"},
                # strain P78048
                "SRR20077217": {"label": "Cravener et al control strain P78048 (Rep 1, HapA)", "bucket": "control", "group": "P78048"},
                "SRR20077306": {"label": "Cravener et al control strain P78048 (Rep 2, HapA)", "bucket": "control", "group": "P78048"},
                "SRR20077303": {"label": "Cravener et al control strain P78048 (Rep 3, HapA)", "bucket": "control", "group": "P78048"},
                "SRR20077238": {"label": "Cravener et al efg1 strain P78048 (Rep 1, HapA)", "bucket": "basic_biology", "group": "P78048"},
                "SRR20077275": {"label": "Cravener et al efg1 strain P78048 (Rep 2, HapA)", "bucket": "basic_biology", "group": "P78048"},
                "SRR20077228": {"label": "Cravener et al efg1 strain P78048 (Rep 3, HapA)", "bucket": "basic_biology", "group": "P78048"},
                # strain P87
                "SRR20077272": {"label": "Cravener et al control strain P87 (Rep 1, HapA)", "bucket": "control", "group": "P87"},
                "SRR20077235": {"label": "Cravener et al control strain P87 (Rep 2, HapA)", "bucket": "control", "group": "P87"},
                "SRR20077234": {"label": "Cravener et al control strain P87 (Rep 3, HapA)", "bucket": "control", "group": "P87"},
                "SRR20077230": {"label": "Cravener et al efg1 strain P87 (Rep 1, HapA)", "bucket": "basic_biology", "group": "P87"},
                "SRR20077229": {"label": "Cravener et al efg1 strain P87 (Rep 2, HapA)", "bucket": "basic_biology", "group": "P87"},
                "SRR20077227": {"label": "Cravener et al efg1 strain P87 (Rep 3, HapA)", "bucket": "basic_biology", "group": "P87"},
                # strain SC5314
                "SRR20077307": {"label": "Cravener et al control strain SC5314 (Rep 2, HapA)", "bucket": "control", "group": "SC5314"},
                "SRR20077270": {"label": "Cravener et al control strain SC5314 (Rep 3, HapA)", "bucket": "control", "group": "SC5314"},
                "SRR20077295": {"label": "Cravener et al efg1 strain SC5314 (Rep 1, HapA)", "bucket": "basic_biology", "group": "SC5314"},
                "SRR20077284": {"label": "Cravener et al efg1 strain SC5314 (Rep 2, HapA)", "bucket": "basic_biology", "group": "SC5314"},
                "SRR20077249": {"label": "Cravener et al efg1 strain SC5314 (Rep 3, HapA)", "bucket": "basic_biology", "group": "SC5314"},
            },
        },
        "Wang_2021": {
            "category": "Strain Comparison",
            "pmid": "33879584",
            "path_style": "direct",
            "control": "SRR11671454",
            "conditions": {
            "SRR11671452": {"label": "Wang et al bloodstream strain P34048  (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671470": {"label": "Wang et al bloodstream strain P34048  (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671460": {"label": "Wang et al bloodstream strain P37039  (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671483": {"label": "Wang et al bloodstream strain P37039 (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671450": {"label": "Wang et al bloodstream strain P57055  (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671468": {"label": "Wang et al bloodstream strain P57055  (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671449": {"label": "Wang et al bloodstream strain P57072  (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671466": {"label": "Wang et al bloodstream strain P57072  (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671456": {"label": "Wang et al bloodstream strain P60002  (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671472": {"label": "Wang et al bloodstream strain P60002 (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671463": {"label": "Wang et al bloodstream strain P75010  (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671486": {"label": "Wang et al bloodstream strain P75010 (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671447": {"label": "Wang et al bloodstream strain P75016  (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671475": {"label": "Wang et al bloodstream strain P75016 (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671474": {"label": "Wang et al bloodstream strain P75063  (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671478": {"label": "Wang et al bloodstream strain P75063 (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671446": {"label": "Wang et al bloodstream strain P76055  (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671465": {"label": "Wang et al bloodstream strain P76055 (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671445": {"label": "Wang et al bloodstream strain P76067 (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671464": {"label": "Wang et al bloodstream strain P76067 (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671451": {"label": "Wang et al bloodstream strain P78042 (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671469": {"label": "Wang et al bloodstream strain P78042 (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671455": {"label": "Wang et al bloodstream strain P78048 (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671479": {"label": "Wang et al bloodstream strain P78048 (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671453": {"label": "Wang et al bloodstream strain P94015 (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671471": {"label": "Wang et al bloodstream strain P94015 (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671454": {"label": "Wang et al wild-type strain SC5314 (Rep 1, HapA)", "bucket": "control"},
            "SRR11671477": {"label": "Wang et al wild-type strain SC5314  (Rep 2, HapA)", "bucket": "control"},
            "SRR11671459": {"label": "Wang et al oral strain 12C (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671482": {"label": "Wang et al oral strain 12C (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671448": {"label": "Wang et al oral strain GC75 (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671476": {"label": "Wang et al oral strain GC75 (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671458": {"label": "Wang et al oral strain P37005 (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671481": {"label": "Wang et al oral strain P37005 (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671457": {"label": "Wang et al oral strain P37037 (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671480": {"label": "Wang et al oral strain P37037 (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671467": {"label": "Wang et al oral strain P87 (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671473": {"label": "Wang et al oral strain P87 (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671462": {"label": "Wang et al oral strain 19F (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671485": {"label": "Wang et al oral strain 19F (Rep 2, HapA)", "bucket": "treatment"},
            "SRR11671461": {"label": "Wang et al oral strain L26 (Rep 1, HapA)", "bucket": "treatment"},
            "SRR11671484": {"label": "Wang et al oral strain L26 (Rep 2, HapA)", "bucket": "treatment"},
            },
        },
        "Lok_2026": {
            # Carbon source (glucose/galactose/fructose) x +/- fluconazole.
            # Group-based control per carbon source: untreated = control,
            # +fluconazole = experimental (kill_candida).
            "category": "Morphology/Media",
            "pmid": "41564980",  # PRJNA981293 (Lok et al)
            "ncbi_id": "PRJNA981293",
            "path_style": "direct",
            "control": "SRR24872940",  # fallback; group controls take precedence
            "conditions": {
                # Glucose
                "SRR24872940": {"label": "Glucose (rep 1a)", "bucket": "control", "group": "Glucose"},
                "SRR26084647": {"label": "Glucose (rep 1b)", "bucket": "control", "group": "Glucose"},
                "SRR26084646": {"label": "Glucose + fluconazole", "bucket": "kill_candida", "group": "Glucose"},
                # Galactose
                "SRR26084643": {"label": "Galactose", "bucket": "control", "group": "Galactose"},
                "SRR26084642": {"label": "Galactose + fluconazole", "bucket": "kill_candida", "group": "Galactose"},
                # Fructose
                "SRR26084645": {"label": "Fructose", "bucket": "control", "group": "Fructose"},
                "SRR26084644": {"label": "Fructose + fluconazole", "bucket": "kill_candida", "group": "Fructose"},
            },
        },
    },
    "C_auris_B8441": {
        "Kean_2018": {
            "category": "Biofilm",
            "pmid": "29997121",
            "path_style": "direct",
            "control": "SRR7411227",
            "conditions": {
                "SRR7411227": {"label": "Kean et al planktonic control (Rep 3, HapA)", "bucket": "control"},
                "SRR7411228": {"label": "Kean et al 4h biofilm (Rep 2, HapA)", "bucket": "treatment"},
                "SRR7411229": {"label": "Kean et al 24h biofilm pleural fluid (Rep 2, HapA)", "bucket": "treatment"},
                "SRR7411230": {"label": "Kean et al 24h biofilm pleural fluid (Rep 3, HapA)", "bucket": "treatment"},
                "SRR7411231": {"label": "Kean et al planktonic control (Rep 1, HapA)", "bucket": "control"},
                "SRR7411232": {"label": "Kean et al planktonic control (Rep 2, HapA)", "bucket": "control"},
                "SRR7411233": {"label": "Kean et al 4h biofilm pleural fluid (Rep 2, HapA)", "bucket": "treatment"},
                "SRR7411234": {"label": "Kean et al 12h biofilm pleural fluid (Rep 3, HapA)", "bucket": "treatment"},
                "SRR7411235": {"label": "Kean et al 12h biofilm pleural fluid (Rep 2, HapA)", "bucket": "treatment"},
                "SRR7411236": {"label": "Kean et al 12h biofilm pleural fluid (Rep 1, HapA)", "bucket": "treatment"},
                "SRR7411237": {"label": "Kean et al planktonic control pleural fluid (Rep 3, HapA)", "bucket": "control"},
                "SRR7411238": {"label": "Kean et al 4h biofilm pleural fluid (Rep 1, HapA)", "bucket": "treatment"},
                "SRR7411239": {"label": "Kean et al 24h biofilm pleural fluid (Rep 1, HapA)", "bucket": "treatment"},
                "SRR7411240": {"label": "Kean et al 4h biofilm (Rep 1, HapA)", "bucket": "treatment"},
                "SRR7411241": {"label": "Kean et al 12h biofilm (Rep 1, HapA)", "bucket": "treatment"},
                "SRR7411242": {"label": "Kean et al 24h biofilm (Rep 1, HapA)", "bucket": "treatment"},
                "SRR7411243": {"label": "Kean et al 24h biofilm (Rep 2, HapA)", "bucket": "treatment"},
                "SRR7411244": {"label": "Kean et al 24h biofilm (Rep 3, HapA)", "bucket": "treatment"},
                "SRR7411245": {"label": "Kean et al planktonic control pleural fluid (Rep 1, HapA)", "bucket": "control"},
                "SRR7411246": {"label": "Kean et al planktonic control pleural fluid (Rep 2, HapA)", "bucket": "control"},
                "SRR7411247": {"label": "Kean et al 12h biofilm (Rep 3, HapA)", "bucket": "treatment"},
                "SRR7411248": {"label": "Kean et al 12h biofilm (Rep 2, HapA)", "bucket": "treatment"},
            },
        },
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
            "pmid": "34643421",  # C. auris farnesol study (PRJNA746543)
            "path_style": "direct",
            "control": "SRR15131027",
            "conditions": {
                # Control (no farnesol)
                "SRR15131027": {"label": "Control (rep 1)", "bucket": "control"},
                "SRR15131028": {"label": "Control (rep 2)", "bucket": "control"},
                "SRR15131029": {"label": "Control (rep 3)", "bucket": "control"},
                # Farnesol treated
                "SRR15131030": {"label": "Farnesol (rep 1)", "bucket": "stress"},
                "SRR15131031": {"label": "Farnesol (rep 2)", "bucket": "stress"},
                "SRR15131032": {"label": "Farnesol (rep 3)", "bucket": "stress"},
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
        "Wang_2024": {
            # Strain comparison: non-aggregative AR0382 (B11109, control) vs
            # aggregative AR0387 (B8441), matched within each biofilm condition
            # (in vitro; in vivo mouse catheter). Group-based control so each
            # AR0387 sample is compared to the same-condition AR0382 baseline.
            "category": "Strain Comparison",
            "pmid": "39455573",  # PRJNA1086003 (Wang TW et al., Nat Commun 2024)
            "ncbi_id": "PRJNA1086003",
            "path_style": "direct",
            "control": "SRR28790270",  # fallback; group controls take precedence
            "conditions": {
                # In vitro biofilm -- AR0382 (non-aggregative) = control
                "SRR28790270": {"label": "AR0382 non-aggregative, in vitro (rep 1)", "bucket": "control", "group": "In vitro"},
                "SRR28790272": {"label": "AR0382 non-aggregative, in vitro (rep 2)", "bucket": "control", "group": "In vitro"},
                "SRR28790274": {"label": "AR0382 non-aggregative, in vitro (rep 3)", "bucket": "control", "group": "In vitro"},
                # In vitro biofilm -- AR0387 (aggregative) = experimental
                "SRR28790276": {"label": "AR0387 aggregative, in vitro (rep 1)", "bucket": "basic_biology", "group": "In vitro"},
                "SRR28790278": {"label": "AR0387 aggregative, in vitro (rep 2)", "bucket": "basic_biology", "group": "In vitro"},
                "SRR28790280": {"label": "AR0387 aggregative, in vitro (rep 3)", "bucket": "basic_biology", "group": "In vitro"},
                # In vivo mouse catheter -- AR0382 (non-aggregative) = control
                "SRR28791430": {"label": "AR0382 non-aggregative, in vivo catheter (rep 1)", "bucket": "control", "group": "In vivo catheter"},
                "SRR28791431": {"label": "AR0382 non-aggregative, in vivo catheter (rep 2)", "bucket": "control", "group": "In vivo catheter"},
                "SRR28791432": {"label": "AR0382 non-aggregative, in vivo catheter (rep 3)", "bucket": "control", "group": "In vivo catheter"},
                # In vivo mouse catheter -- AR0387 (aggregative) = experimental
                "SRR28791433": {"label": "AR0387 aggregative, in vivo catheter (rep 1)", "bucket": "basic_biology", "group": "In vivo catheter"},
                "SRR28791434": {"label": "AR0387 aggregative, in vivo catheter (rep 2)", "bucket": "basic_biology", "group": "In vivo catheter"},
                "SRR28791437": {"label": "AR0387 aggregative, in vivo catheter (rep 3)", "bucket": "basic_biology", "group": "In vivo catheter"},
                "SRR28791438": {"label": "AR0387 aggregative, in vivo catheter (rep 4)", "bucket": "basic_biology", "group": "In vivo catheter"},
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
            "category": "Biofilm",
            "pmid": "25233198",
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
            "category": "Morphology",
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

# Library sizes (total mapped reads in millions) for normalization
# Generated by scripts/extract_library_sizes.py using pysam
#
# Format: {organism: {study_id: {condition_id: library_size_in_millions}}}
# If a condition is missing, normalization will be skipped for that comparison
# and a warning will be logged.
NORMALIZE_BY_LIBRARY_SIZE = True

LIBRARY_SIZES: Dict[str, Dict[str, Dict[str, float]]] = {
    "C_albicans_SC5314": {
        "Bruno_2010": {
            "nOxi": 10.44,
            "lOxi": 10.3,
            "hOxi": 6.0,
            "nitroSm": 11.84,
            "nitroSp": 9.72,
            "cwdm": 10.68,
            "cwdp": 11.22,
            "ph4": 10.75,
            "ph8": 9.21,
            "ypd_ss": 19.26,
            "ypd_serum_ss": 20.03,
        },
        "Desai_2013": {
            "sc_plnk": 47.76,
            "sc_film": 41.48,
        },
        "Niemiec_2017": {
            "chg_77": 29.91,
            "chg_71": 17.59,
            "chg_72": 19.12,
            "chg_73": 15.68,
            "chg_75": 28.54,
            "chg_79": 16.59,
            "chg_76": 29.08,
            "chg_70": 25.71,
            "chg_69": 24.38,
        },
        "Xie_2013": {
            "CY110wh": 12.6,
            "CY110op": 12.17,
        },
        "Shivarathri_2019": {
            "SRR8285058": 14.93,
            "SRR8285059": 15.46,
            "SRR8285060": 12.52,
            "SRR8285064": 14.94,
            "SRR8285065": 15.6,
            "SRR8285066": 15.07,
        },
        "Glazier_2023": {
            "SRR25396044": 35.35,
            "SRR25396043": 34.06,
            "SRR25396042": 35.6,
            "SRR25396041": 33.85,
            "SRR25396040": 30.69,
            "SRR25396039": 43.9,
        },
        "Lohse_2016": {
            "wt_con": 47.53,
            "wt_gfp": 46.46,
            "op_con": 31.70,
            "op_gfp": 31.97,
        },
        "Zhang_2024": {
            "SRR18188695": 67.02,
            "SRR18188696": 57.96,
            "SRR18188697": 69.6,
            "SRR18188698": 61.85,
            "SRR18188699": 64.79,
            "SRR18188700": 41.97,
        },
        "Rai_2024": {
            "ERR8278349": 31.01,
            "ERR8278350": 27.35,
            "ERR8278351": 27.96,
            "ERR8278346": 25.54,
            "ERR8278347": 24.51,
            "ERR8278348": 24.26,
            "ERR8278352": 23.74,
            "ERR8278353": 21.81,
            "ERR8278354": 23.45,
            "ERR8278355": 24.46,
            "ERR8278356": 25.4,
            "ERR8278357": 26.37,
            "ERR8278358": 30.82,
            "ERR8278359": 31.02,
            "ERR8278360": 30.61,
            "ERR8278361": 21.68,
            "ERR8278362": 27.53,
            "ERR8278363": 36.29,
        },
    },
    "C_auris_B8441": {
        "Shivarathri_2022": {
            "SRR17259761": 60.04,
            "SRR17259762": 61.93,
            "SRR17259763": 59.08,
            "SRR17259764": 45.32,
            "SRR17259765": 47.18,
            "SRR17259766": 46.76,
            "SRR17259767": 64.27,
            "SRR17259768": 58.91,
            "SRR17259769": 47.74,
            "SRR17259770": 46.05,
            "SRR17259771": 54.73,
            "SRR17259772": 72.32,
        },
        "Jakab_2021": {
            "SRR15131027": 33.34,
            "SRR15131028": 35.08,
            "SRR15131029": 38.63,
            "SRR15131030": 33.57,
            "SRR15131031": 27.47,
            "SRR15131032": 27.16,
        },
        "Balla_2023": {
            "SRR23266141": 33.34,
            "SRR23266140": 35.08,
            "SRR23266139": 38.63,
            "SRR23266138": 28.79,
            "SRR23266137": 26.56,
            "SRR23266136": 30.87,
        },
        "Biermann_2022": {
            "SRR17805794": 58.39,
            "SRR17805793": 64.59,
            "SRR17805782": 51.47,
            "SRR17805777": 55.34,
            "SRR17805776": 55.02,
            "SRR17805775": 55.75,
            "SRR17805774": 52.04,
            "SRR17805773": 55.33,
            "SRR17805772": 60.58,
            "SRR17805771": 55.44,
            "SRR17805792": 57.03,
            "SRR17805791": 51.48,
            "SRR17805790": 54.14,
            "SRR17805789": 57.6,
            "SRR17805788": 51.24,
            "SRR17805787": 55.93,
            "SRR17805786": 53.54,
            "SRR17805785": 51.77,
            "SRR17805784": 50.58,
            "SRR17805783": 41.55,
            "SRR17805781": 55.71,
            "SRR17805780": 42.75,
            "SRR17805779": 54.51,
            "SRR17805778": 54.05,
        },
        "Chow_2023": {
            "SRR22315652": 53.81,
            "SRR22315651": 57.8,
            "SRR22315650": 51.83,
            "SRR22315649": 43.34,
            "SRR22315648": 49.62,
            "SRR22315647": 49.81,
            "SRR22315646": 52.9,
            "SRR22315645": 47.78,
            "SRR22315644": 48.77,
        },
        "Jenull_2021": {
            "SRR13576987": 95.88,
            "SRR13576988": 91.78,
            "SRR13576989": 75.74,
            "SRR13576978": 58.54,
            "SRR13576979": 56.92,
            "SRR13576980": 58.03,
            "SRR13576981": 96.35,
            "SRR13576982": 94.64,
            "SRR13576983": 107.34,
            "SRR13576984": 61.72,
            "SRR13576985": 69.78,
            "SRR13576986": 72.73,
        },
        "Pelletier_2024": {
            "SRR24915342": 6.6,
            "SRR24915343": 6.55,
            "SRR24915344": 6.55,
            "SRR24915345": 6.56,
            "SRR24915338": 6.9,
            "SRR24915339": 6.84,
            "SRR24915340": 6.82,
            "SRR24915341": 6.84,
            "SRR24915334": 6.93,
            "SRR24915335": 6.88,
            "SRR24915336": 6.85,
            "SRR24915337": 6.87,
            "SRR24915354": 6.83,
            "SRR24915355": 6.76,
            "SRR24915356": 6.74,
            "SRR24915357": 6.77,
            "SRR24915350": 7.28,
            "SRR24915351": 7.21,
            "SRR24915352": 7.18,
            "SRR24915353": 7.2,
            "SRR24915346": 7.06,
            "SRR24915347": 7.0,
            "SRR24915348": 6.97,
            "SRR24915349": 6.99,
            "SRR24915366": 7.3,
            "SRR24915367": 7.25,
            "SRR24915368": 7.23,
            "SRR24915369": 7.26,
            "SRR24915362": 6.79,
            "SRR24915363": 6.73,
            "SRR24915364": 6.68,
            "SRR24915365": 6.71,
            "SRR24915358": 7.04,
            "SRR24915359": 6.99,
            "SRR24915360": 6.95,
            "SRR24915361": 6.97,
            "SRR24915378": 7.32,
            "SRR24915379": 7.24,
            "SRR24915380": 7.2,
            "SRR24915381": 7.21,
            "SRR24915374": 6.99,
            "SRR24915375": 6.94,
            "SRR24915376": 6.92,
            "SRR24915377": 6.95,
            "SRR24915370": 7.52,
            "SRR24915371": 7.44,
            "SRR24915372": 7.43,
            "SRR24915373": 7.44,
        },
        "Simm_2022": {
            "SRR14758158": 29.36,
            "SRR14758159": 31.47,
            "SRR14758160": 23.12,
            "SRR14758161": 25.02,
            "SRR14758162": 16.15,
            "SRR14758163": 18.79,
            "SRR14758164": 14.32,
            "SRR14758165": 15.28,
            "SRR14758166": 21.25,
            "SRR14758167": 24.73,
            "SRR14758168": 21.18,
            "SRR14758169": 24.78,
        },
        "Wang_2024": {
            # AR0382 - In Vitro Biofilm
            "SRR28790270": 73.0,
            "SRR28790272": 83.0,
            "SRR28790274": 73.43,
            # AR0382 - In Vivo Catheter
            "SRR28791430": 49.86,
            "SRR28791431": 41.69,
            "SRR28791432": 51.14,
            # AR0387 - In Vitro Biofilm
            "SRR28790276": 64.31,
            "SRR28790278": 63.52,
            "SRR28790280": 57.68,
            # AR0387 - In Vivo Catheter
            "SRR28791433": 52.47,
            "SRR28791434": 42.40,
            "SRR28791437": 43.19,
            "SRR28791438": 50.38,
        },
    },
    "C_glabrata_CBS138": {
        "Linde_2015": {
            "SRR1582640": 34.77,
            "SRR1582641": 46.64,
            "SRR1582643": 34.99,
            "SRR1582644": 30.9,
            "SRR1582646": 32.53,
            "SRR1582647": 26.2,
            "SRR1582648": 26.61,
            "SRR1582649": 26.76,
            "SRR1582650": 21.21,
            "SRR1582651": 34.63,
        },
        "Bhakt_2022": {
            "SRR19158466": 65.85,
            "SRR19158465": 49.05,
            "SRR19158464": 52.76,
            "SRR19158463": 50.7,
            "SRR19158461": 48.99,
            "SRR19158460": 58.32,
            "SRR19158459": 52.18,
        },
        "Kumar_2024": {
            "SRR24895745": 49.73,
            "SRR24895743": 38.76,
            "SRR24895738": 43.5,
            "SRR24895746": 50.36,
            "SRR24895740": 48.66,
            "SRR24895744": 45.03,
            "SRR24895742": 54.01,
            "SRR24895737": 39.85,
            "SRR24895739": 56.05,
        },
        "Ni_2023": {
            "SRR24529963": 44.09,
            "SRR24529962": 43.37,
            "SRR24529961": 40.43,
            "SRR24529954": 55.58,
            "SRR24529953": 53.13,
            "SRR24529952": 53.37,
            "SRR24529951": 56.85,
            "SRR24529950": 57.02,
            "SRR24529956": 40.42,
            "SRR24529948": 50.53,
            "SRR24529947": 50.82,
            "SRR24529946": 51.41,
        },
        "Vu_2021": {
            "SRR15532683": 41.32,
            "SRR15532684": 48.76,
            "SRR15532685": 39.61,
            "SRR15532686": 53.14,
            "SRR15532687": 40.34,
            "SRR15532688": 49.71,
        },
    },
    "C_dubliniensis_CD36": {
        "Grumaz_2013": {
            "SRR604750": 32.67,
            "SRR604752": 30.78,
            "SRR604753": 31.09,
            "SRR771365": 15.01,
            "SRR771366": 14.72,
        },
        "Singh-Babakh_2021": {
            "SRR13833835": 15.86,
            "SRR13833836": 17.27,
            "SRR13833829": 16.38,
            "SRR13833830": 18.44,
            "SRR13833831": 18.13,
            "SRR13833832": 14.51,
            "SRR13833833": 15.08,
            "SRR13833834": 18.66,
        },
    },
    "C_parapsilosis_CDC317": {
        "Holland_2014": {
            "wt_plnk_1": 27.24,
            "wt_plnk_2": 27.26,
            "wt_plnk_3": 26.4,
            "wt_film_1": 26.19,
            "wt_film_2": 27.25,
            "wt_film_3": 26.66,
            "ace2_1": 26.53,
            "ace2_2": 25.93,
            "ace2_3": 27.26,
            "cph2_1": 25.41,
            "cph2_2": 25.6,
            "cph2_3": 26.23,
            "efg1_1": 26.37,
            "efg1_2": 26.62,
            "efg1_3": 27.0,
            "czf1_1": 25.27,
            "czf1_2": 26.53,
            "czf1_3": 25.45,
            "ume6_1": 26.55,
            "ume6_2": 26.06,
            "ume6_3": 26.18,
        },
        "Connolly_2013": {
            "SRR575493": 5.1,
            "SRR575494": 8.64,
            "SRR575495": 4.67,
            "SRR575496": 7.14,
            "SRR575497": 8.37,
            "SRR575498": 6.48,
            "SRR575499": 4.94,
            "SRR575500": 7.87,
        },
        "Guida_2011": {
            "SRR352254": 13.44,
            "SRR352256": 15.44,
            "SRR352257": 16.95,
            "SRR352259": 15.55,
            "SRR352261": 11.63,
            "SRR352264": 10.44,
            "SRR352266": 10.94,
            "SRR352278": 6.54,
            "SRR352253": 10.98,
            "SRR352255": 14.44,
            "SRR352258": 14.48,
            "SRR352260": 11.31,
            "SRR352262": 9.9,
            "SRR352265": 13.57,
            "SRR352277": 4.2,
            "SRR352268": 13.04,
            "SRR352269": 11.2,
            "SRR352267": 11.69,
            "SRR352270": 5.49,
            "SRR352271": 9.74,
            "SRR352274": 12.32,
            "SRR352273": 19.1,
            "SRR352276": 14.59,
            "SRR352272": 6.91,
            "SRR352275": 23.09,
        },
    },
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


def _bigwig_is_readable(bigwig_path: Path) -> bool:
    """
    True only if the file exists and is non-empty.

    A 0-byte bigwig (e.g. an aborted data copy) makes libBigWig print
    "[bwHdrRead] There was an error while reading in the header!" and can abort
    the interpreter (SIGABRT) rather than raise a catchable Python error, so
    such files must be filtered out before pyBigWig.open() ever sees them.
    """
    try:
        return bigwig_path.is_file() and bigwig_path.stat().st_size > 0
    except OSError:
        return False


def _stats_mean(bw, chromosome: str, start: int, end: int) -> Optional[float]:
    """Mean coverage over a region from an already-open bigWig handle."""
    # Handle minus strand genes where start > end
    if start > end:
        start, end = end, start
    # pyBigWig uses 0-based coordinates
    stats = bw.stats(chromosome, start - 1, end, type="mean")
    if stats and stats[0] is not None:
        return stats[0]
    return 0.0


class _BigWigPool:
    """
    Per-request cache of open bigWig handles.

    The batch and matrix endpoints read the *same* set of bigWig files once per
    gene. Without pooling, a 200-gene matrix reopens each (often 300+ MB) file
    ~200 times -- tens of thousands of opens that blow past the gunicorn request
    timeout. Opening each file once per request and reusing the handle across
    genes reduces that to at most one open per distinct file.

    NOT thread-safe: a single libBigWig handle keeps internal read state, so
    handles must never be shared across threads. Create one pool per request --
    sync FastAPI endpoints each run in their own threadpool thread, so a
    request-scoped pool is never touched concurrently.
    """

    def __init__(self, max_handles: int = 600):
        self._handles: "OrderedDict[str, object]" = OrderedDict()
        self._max_handles = max_handles

    def _handle(self, bigwig_path: Path):
        key = str(bigwig_path)
        bw = self._handles.get(key)
        if bw is not None:
            self._handles.move_to_end(key)
            return bw

        if not PYBIGWIG_AVAILABLE or not _bigwig_is_readable(bigwig_path):
            return None
        try:
            bw = pyBigWig.open(key)
        except Exception as e:
            logger.debug(f"Error opening bigwig {bigwig_path}: {e}")
            return None
        if bw is None:
            return None

        self._handles[key] = bw
        # Bound open file descriptors; evict least-recently-used if over cap.
        if len(self._handles) > self._max_handles:
            _, old = self._handles.popitem(last=False)
            try:
                old.close()
            except Exception:
                pass
        return bw

    def value(
        self, bigwig_path: Path, chromosome: str, start: int, end: int
    ) -> Optional[float]:
        bw = self._handle(bigwig_path)
        if bw is None:
            return None
        try:
            return _stats_mean(bw, chromosome, start, end)
        except Exception as e:
            logger.debug(f"Error reading bigwig {bigwig_path}: {e}")
            return None

    def close(self):
        for bw in self._handles.values():
            try:
                bw.close()
            except Exception:
                pass
        self._handles.clear()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def _get_expression_value(
    bigwig_path: Path,
    chromosome: str,
    start: int,
    end: int,
    pool: Optional["_BigWigPool"] = None,
) -> Optional[float]:
    """
    Get mean expression value for a genomic region from a bigwig file.

    When a per-request ``pool`` is supplied the open handle is reused across
    calls; otherwise the file is opened and closed for this single read.
    """
    if pool is not None:
        return pool.value(bigwig_path, chromosome, start, end)

    if not PYBIGWIG_AVAILABLE:
        return None

    if not _bigwig_is_readable(bigwig_path):
        return None

    bw = None
    try:
        bw = pyBigWig.open(str(bigwig_path))
        if bw is None:
            return None
        return _stats_mean(bw, chromosome, start, end)
    except Exception as e:
        logger.debug(f"Error reading bigwig {bigwig_path}: {e}")
        return None
    finally:
        if bw is not None:
            try:
                bw.close()
            except Exception:
                pass


def _get_library_size(
    organism_key: str,
    study_id: str,
    condition_id: str
) -> Optional[float]:
    """Get library size (in millions of reads) for a condition."""
    org_sizes = LIBRARY_SIZES.get(organism_key, {})
    study_sizes = org_sizes.get(study_id, {})
    return study_sizes.get(condition_id)


def _build_group_control_map(study_info: dict) -> Dict[str, str]:
    """
    Build a mapping of group -> control_id for a study.

    For studies using group-based control matching, each group should have
    exactly one condition with bucket="control". This function builds a map
    from group names to their control condition IDs.

    For backward compatibility, if no groups are defined, returns an empty dict
    and the caller should fall back to the study-level "control" field.

    Args:
        study_info: Study configuration dict with "conditions" field

    Returns:
        Dict mapping group name to control condition ID.
        Empty dict if no groups are defined.
    """
    group_controls: Dict[str, str] = {}

    for cond_id, cond_info in study_info.get("conditions", {}).items():
        group = cond_info.get("group")
        bucket = cond_info.get("bucket", "")

        if group and bucket == "control":
            if group in group_controls:
                logger.warning(
                    f"Multiple controls found for group '{group}': "
                    f"{group_controls[group]} and {cond_id}. Using first."
                )
            else:
                group_controls[group] = cond_id

    return group_controls


def _get_control_for_condition(
    study_info: dict,
    cond_id: str,
    group_control_map: Dict[str, str]
) -> Optional[str]:
    """
    Get the control condition ID for a given condition.

    Uses group-based matching if the condition has a "group" field,
    otherwise falls back to the study-level "control" field.

    Args:
        study_info: Study configuration dict
        cond_id: Condition ID to find control for
        group_control_map: Pre-built map from _build_group_control_map()

    Returns:
        Control condition ID, or None if not found
    """
    cond_info = study_info.get("conditions", {}).get(cond_id, {})
    group = cond_info.get("group")

    if group and group_control_map:
        # Use group-based control
        control_id = group_control_map.get(group)
        if not control_id:
            logger.warning(
                f"No control found for group '{group}' "
                f"(condition {cond_id})"
            )
        return control_id
    else:
        # Fall back to study-level control
        return study_info.get("control")


def _is_control_condition(study_info: dict, cond_id: str) -> bool:
    """
    Check if a condition is a control (bucket == "control").

    Args:
        study_info: Study configuration dict
        cond_id: Condition ID to check

    Returns:
        True if condition is a control, False otherwise
    """
    cond_info = study_info.get("conditions", {}).get(cond_id, {})
    return cond_info.get("bucket") == "control"


def _build_study_conditions(
    study_info: dict,
    study_id: str,
    base_path,
    chromosome: str,
    start: int,
    end: int,
    organism_key: str,
    control_values: Dict[str, float],
    group_control_map: Dict[str, str],
    pool: Optional["_BigWigPool"] = None,
) -> Tuple[List["ExpressionCondition"], List[float]]:
    """
    Build the ExpressionCondition list for a single study at one locus.

    Shared by the gene-expression and per-organism expression-details endpoints
    so they stay in lockstep.

    - Grouped studies (study uses per-group controls, i.e. group_control_map is
      non-empty): INCLUDE control conditions so each group can be shown as a unit
      (control baseline + treatment), tag every condition with its ``group`` and
      the ``control_label`` it is compared against, and order by
      (group, controls-first, label) so the frontend can render per-group blocks.
    - Ungrouped studies: legacy behaviour — exclude controls and sort by fold
      change descending; ``group``/``control_label`` stay None.

    Returns (conditions, non_control_fold_changes). Only non-control fold changes
    feed the headline summary stats / total_conditions, so adding control rows to
    grouped studies doesn't distort those numbers.
    """
    is_grouped = bool(group_control_map)
    conditions: List[ExpressionCondition] = []
    non_control_fold_changes: List[float] = []

    for cond_id, cond_info in study_info["conditions"].items():
        is_control = _is_control_condition(study_info, cond_id)

        # Ungrouped studies hide controls (baseline == 1x is uninformative there).
        if is_control and not is_grouped:
            continue

        control_id = _get_control_for_condition(
            study_info, cond_id, group_control_map
        )
        if not control_id or control_id not in control_values:
            continue

        control_value = control_values[control_id]

        cond_path = _get_bigwig_path(base_path, study_id, cond_id, study_info)
        cond_value = _get_expression_value(cond_path, chromosome, start, end, pool=pool)
        if cond_value is None:
            continue

        fold_change, _ = _calculate_fold_change(
            cond_value=cond_value,
            control_value=control_value,
            organism_key=organism_key,
            study_id=study_id,
            cond_id=cond_id,
            control_id=control_id,
        )

        conditions.append(ExpressionCondition(
            condition_id=cond_id,
            label=cond_info["label"],
            value=round(cond_value, 2),
            fold_change=fold_change,
            bucket=cond_info.get("bucket", ""),
            group=cond_info.get("group") if is_grouped else None,
            control_label=(
                study_info["conditions"].get(control_id, {}).get("label", control_id)
                if is_grouped else None
            ),
        ))

        if not is_control:
            non_control_fold_changes.append(fold_change)

    if is_grouped:
        # Per-group blocks: group, controls first, then by label (rep order).
        conditions.sort(key=lambda c: (
            c.group or "",
            0 if c.bucket == "control" else 1,
            c.label,
        ))
    else:
        conditions.sort(key=lambda c: c.fold_change, reverse=True)

    return conditions, non_control_fold_changes


def _calculate_fold_change(
    cond_value: float,
    control_value: float,
    organism_key: str = None,
    study_id: str = None,
    cond_id: str = None,
    control_id: str = None,
) -> Tuple[float, bool]:
    """
    Calculate fold change with optional library size normalization.

    If NORMALIZE_BY_LIBRARY_SIZE is True and library sizes are available,
    values are normalized to CPM (counts per million) before calculating
    the ratio. This corrects for differences in sequencing depth between
    samples.

    Fold change formula:
        Without normalization: cond_value / control_value
        With normalization:    (cond_value / cond_lib_size) / (control_value / control_lib_size)
                             = (cond_value * control_lib_size) / (control_value * cond_lib_size)

    Note: Gene length normalization is NOT needed because:
        - The same gene region is used for both condition and control
        - Gene length cancels out in the ratio

    Args:
        cond_value: Raw expression value for condition
        control_value: Raw expression value for control
        organism_key: Organism identifier (e.g., "C_albicans_SC5314")
        study_id: Study identifier (e.g., "Bruno_2010")
        cond_id: Condition identifier
        control_id: Control condition identifier

    Returns:
        Tuple of (fold_change, was_normalized)
    """
    if control_value <= 0:
        return (0.0, False)

    # Check if we should normalize
    if (NORMALIZE_BY_LIBRARY_SIZE and organism_key and study_id
            and cond_id and control_id):
        cond_lib_size = _get_library_size(organism_key, study_id, cond_id)
        control_lib_size = _get_library_size(organism_key, study_id, control_id)

        if cond_lib_size and control_lib_size and cond_lib_size > 0:
            # Normalized fold change:
            # (cond / cond_lib) / (control / control_lib)
            # = (cond * control_lib) / (control * cond_lib)
            fold_change = (cond_value * control_lib_size) / (control_value * cond_lib_size)
            return (round(fold_change, 3), True)
        else:
            # Library sizes not available - log warning once per study
            logger.debug(
                f"Library sizes not available for {study_id}, "
                f"using raw fold change"
            )

    # Fall back to raw fold change (no normalization)
    fold_change = cond_value / control_value
    return (round(fold_change, 3), False)


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
        # Build group -> control mapping for this study
        group_control_map = _build_group_control_map(study_info)
        default_control_id = study_info.get("control")

        # Cache control values (may have multiple controls for group-based studies)
        control_values: Dict[str, float] = {}
        control_ids_needed = set(group_control_map.values())
        if default_control_id:
            control_ids_needed.add(default_control_id)

        for ctrl_id in control_ids_needed:
            ctrl_path = _get_bigwig_path(base_path, study_id, ctrl_id, study_info)
            ctrl_value = _get_expression_value(ctrl_path, chromosome, start, end)
            if ctrl_value is not None and ctrl_value > 0:
                control_values[ctrl_id] = ctrl_value

        if not control_values:
            warnings.append(f"Could not read control data for {study_id}")
            continue

        # Process conditions (grouped studies include controls + per-group labels)
        conditions, study_fold_changes = _build_study_conditions(
            study_info, study_id, base_path, chromosome, start, end,
            organism_key, control_values, group_control_map,
        )
        all_fold_changes.extend(study_fold_changes)
        total_conditions += len(study_fold_changes)

        if conditions:
            # For display, use the first/default control
            display_control_id = default_control_id or next(iter(control_values.keys()), None)
            control_label = study_info["conditions"].get(display_control_id, {}).get("label", display_control_id)
            display_control_value = control_values.get(display_control_id, 0)
            studies.append(ExpressionStudy(
                study_id=study_id,
                category=study_info["category"],
                pmid=study_info.get("pmid"),
                control_id=display_control_id,
                control_label=control_label,
                control_value=round(display_control_value, 2),
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

# Reverse mapping from HTS directory keys to database organism names
HTS_KEY_TO_ORGANISM = {v: k for k, v in ORGANISM_TO_HTS_KEY.items()}


def _get_organism_no_from_key(db: Session, organism_key: str) -> Optional[int]:
    """
    Get organism_no from HTS directory key (e.g., 'C_albicans_SC5314').

    Returns the organism_no or None if not found.
    """
    organism_name = HTS_KEY_TO_ORGANISM.get(organism_key)
    if not organism_name:
        return None

    organism = db.query(Organism).filter(
        Organism.organism_name == organism_name
    ).first()

    return organism.organism_no if organism else None


def _get_expression_for_organism(
    db: Session,
    feature: Feature,
    organism_name: str,
    pool: Optional["_BigWigPool"] = None,
) -> Optional[ExpressionDetailsForOrganism]:
    """
    Get expression data for a specific feature/organism.

    Returns ExpressionDetailsForOrganism or None if no data available.

    ``pool`` is an optional per-request bigWig handle cache; callers that fetch
    many genes (batch/matrix) should pass a shared pool so each bigWig file is
    opened once per request rather than once per gene.
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
        # Build group -> control mapping for this study
        group_control_map = _build_group_control_map(study_info)
        default_control_id = study_info.get("control")

        # Cache control values (may have multiple controls for group-based studies)
        control_values: Dict[str, float] = {}
        control_ids_needed = set(group_control_map.values())
        if default_control_id:
            control_ids_needed.add(default_control_id)

        for ctrl_id in control_ids_needed:
            ctrl_path = _get_bigwig_path(base_path, study_id, ctrl_id, study_info)
            ctrl_value = _get_expression_value(ctrl_path, chromosome, start, end, pool=pool)
            if ctrl_value is not None and ctrl_value > 0:
                control_values[ctrl_id] = ctrl_value

        if not control_values:
            warnings.append(f"Could not read control data for {study_id}")
            continue

        # Process conditions (grouped studies include controls + per-group labels)
        conditions, study_fold_changes = _build_study_conditions(
            study_info, study_id, base_path, chromosome, start, end,
            hts_key, control_values, group_control_map, pool=pool,
        )
        all_fold_changes.extend(study_fold_changes)
        total_conditions += len(study_fold_changes)

        if conditions:
            # For display, use the first/default control
            display_control_id = default_control_id or next(iter(control_values.keys()), None)
            control_label = study_info["conditions"].get(display_control_id, {}).get("label", display_control_id)
            display_control_value = control_values.get(display_control_id, 0)
            studies.append(ExpressionStudy(
                study_id=study_id,
                category=study_info["category"],
                pmid=study_info.get("pmid"),
                control_id=display_control_id,
                control_label=control_label,
                control_value=round(display_control_value, 2),
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

    with _BigWigPool() as pool:
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
            expr_data = _get_expression_for_organism(db, feature, organism_name, pool=pool)
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

# Pre-computed correlation cache: {organism: (gene_names, gene_index, arrays...)}
_correlation_cache: Dict[str, tuple] = {}

# Directory for pre-computed cache files
EXPRESSION_CACHE_DIR = Path("/data/cache/expression")

# Try to import numpy
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    logger.warning("numpy not installed - pre-computed correlations unavailable")


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


def _load_correlation_cache(organism_key: str, metric: str = "pearson") -> Optional[tuple]:
    """
    Load pre-computed correlation cache from file.

    Cache files are built by scripts/build_correlation_cache.py

    Args:
        organism_key: Organism identifier
        metric: Correlation metric ('pearson', 'spearman', 'cosine')

    Returns:
        Tuple of (gene_names, gene_index, top_indices, top_correlations, top_pvalues, top_shared,
                  bottom_indices, bottom_correlations, bottom_pvalues, bottom_shared)
        or None if not available.
        Note: bottom_* arrays may be None if cache was built before anticorrelation support.
    """
    global _correlation_cache

    # Cache key includes metric
    cache_key = f"{organism_key}_{metric}"

    # Return from memory cache if available
    if cache_key in _correlation_cache:
        return _correlation_cache[cache_key]

    if not NUMPY_AVAILABLE:
        return None

    # Pearson uses original filename for backwards compatibility
    if metric == "pearson":
        cache_file = EXPRESSION_CACHE_DIR / f"correlations_{organism_key}.npz"
        index_file = EXPRESSION_CACHE_DIR / f"gene_index_{organism_key}.json"
    else:
        cache_file = EXPRESSION_CACHE_DIR / f"correlations_{metric}_{organism_key}.npz"
        index_file = EXPRESSION_CACHE_DIR / f"gene_index_{metric}_{organism_key}.json"

    if not cache_file.exists() or not index_file.exists():
        logger.debug(f"Correlation cache not found for {organism_key} ({metric})")
        return None

    try:
        logger.info(f"Loading {metric} correlation cache from {cache_file}")

        # Load numpy arrays
        data = np.load(cache_file)
        gene_names = data['gene_names'].tolist()
        top_indices = data['top_indices']
        top_correlations = data['top_correlations']
        top_pvalues = data['top_pvalues']
        top_shared = data['top_shared']

        # Load anticorrelated data if available (backwards compatible)
        bottom_indices = data.get('bottom_indices')
        bottom_correlations = data.get('bottom_correlations')
        bottom_pvalues = data.get('bottom_pvalues')
        bottom_shared = data.get('bottom_shared')

        # Load gene index
        with open(index_file, 'r') as f:
            gene_index = json.load(f)

        result = (gene_names, gene_index, top_indices, top_correlations, top_pvalues, top_shared,
                  bottom_indices, bottom_correlations, bottom_pvalues, bottom_shared)
        _correlation_cache[cache_key] = result

        logger.info(f"Loaded {metric} correlation cache: {len(gene_names)} genes")
        return result

    except Exception as e:
        logger.error(f"Error loading correlation cache {cache_file}: {e}")
        return None


def _get_similar_genes_from_cache(
    organism_key: str,
    query_feature_name: str,
    metric: str = "pearson",
    limit: int = 20,
    direction: str = "positive"
) -> Optional[List[Tuple[str, float, Optional[float], int]]]:
    """
    Get similar genes using pre-computed correlation cache.

    Args:
        organism_key: Organism identifier
        query_feature_name: Query gene feature name
        metric: Correlation metric ('pearson', 'spearman', 'cosine')
        limit: Maximum number of results
        direction: 'positive' for correlated, 'negative' for anticorrelated, 'both' for all

    Returns:
        List of (feature_name, correlation, p_value, shared_conditions) or None if cache unavailable
    """
    cache_data = _load_correlation_cache(organism_key, metric)
    if not cache_data:
        return None

    (gene_names, gene_index, top_indices, top_correlations, top_pvalues, top_shared,
     bottom_indices, bottom_correlations, bottom_pvalues, bottom_shared) = cache_data

    # Find query gene index
    query_idx = gene_index.get(query_feature_name)
    if query_idx is None:
        logger.debug(f"Query gene {query_feature_name} not found in {metric} correlation cache")
        return None

    results = []

    # Get positive correlations if requested
    if direction in ("positive", "both"):
        for i in range(min(limit, len(top_indices[query_idx]))):
            target_idx = top_indices[query_idx, i]
            if target_idx == 0 and i > 0:  # End of valid results
                break

            corr = float(top_correlations[query_idx, i])
            if corr == 0 and i > 0:  # No more valid correlations
                break

            pval = float(top_pvalues[query_idx, i])
            shared = int(top_shared[query_idx, i])
            target_name = gene_names[target_idx]

            results.append((target_name, corr, pval if pval > 0 else None, shared))

    # Get negative correlations if requested and available
    if direction in ("negative", "both") and bottom_indices is not None:
        for i in range(min(limit, len(bottom_indices[query_idx]))):
            target_idx = bottom_indices[query_idx, i]
            if target_idx == 0 and i > 0:  # End of valid results
                break

            corr = float(bottom_correlations[query_idx, i])
            if corr == 0 and i > 0:  # No more valid correlations
                break

            pval = float(bottom_pvalues[query_idx, i])
            shared = int(bottom_shared[query_idx, i])
            target_name = gene_names[target_idx]

            results.append((target_name, corr, pval if pval > 0 else None, shared))

    # For 'both', sort by actual correlation value (positive at top, negative at bottom)
    if direction == "both":
        results.sort(key=lambda x: x[1], reverse=True)
        # Trim to limit after combining
        results = results[:limit]

    return results


def _get_all_condition_ids(organism_key: str) -> List[str]:
    """Get all condition IDs (excluding controls) for an organism, sorted for consistency."""
    studies_config = EXPRESSION_STUDIES.get(organism_key, {})
    condition_ids = []

    for study_id, study_info in studies_config.items():
        for cond_id in study_info["conditions"]:
            # Skip control conditions (bucket == "control")
            if not _is_control_condition(study_info, cond_id):
                # Use study_id + cond_id as unique key
                condition_ids.append(f"{study_id}:{cond_id}")

    return sorted(condition_ids)


def _build_expression_profile(
    db: Session,
    feature: Feature,
    organism_key: str,
    base_path: Path,
    studies_config: dict,
    hts_key: str,
    pool: Optional["_BigWigPool"] = None,
) -> Optional[Dict[str, float]]:
    """
    Build expression profile (fold changes) for a single gene across all conditions.

    Returns a dict mapping condition_id to fold_change, or None if location not found.

    ``pool`` is an optional per-run bigWig handle cache; the whole-organism
    rebuild reads every study file for every gene, so a shared pool avoids
    reopening each file once per gene.
    """
    # Get gene location
    location_info = _get_gene_location_for_feature(db, feature, hts_key)
    if not location_info:
        return None

    chromosome, start, end = location_info
    profile: Dict[str, float] = {}

    for study_id, study_info in studies_config.items():
        # Build group -> control mapping for this study
        group_control_map = _build_group_control_map(study_info)
        default_control_id = study_info.get("control")

        # Cache control values (may have multiple controls for group-based studies)
        control_values: Dict[str, float] = {}
        control_ids_needed = set(group_control_map.values())
        if default_control_id:
            control_ids_needed.add(default_control_id)

        for ctrl_id in control_ids_needed:
            ctrl_path = _get_bigwig_path(base_path, study_id, ctrl_id, study_info)
            ctrl_value = _get_expression_value(ctrl_path, chromosome, start, end, pool=pool)
            if ctrl_value is not None and ctrl_value > 0:
                control_values[ctrl_id] = ctrl_value

        if not control_values:
            continue

        # Process conditions
        for cond_id, cond_info in study_info["conditions"].items():
            # Skip control conditions
            if _is_control_condition(study_info, cond_id):
                continue

            # Get the appropriate control for this condition
            control_id = _get_control_for_condition(
                study_info, cond_id, group_control_map
            )
            if not control_id or control_id not in control_values:
                continue

            control_value = control_values[control_id]

            cond_path = _get_bigwig_path(base_path, study_id, cond_id, study_info)
            cond_value = _get_expression_value(cond_path, chromosome, start, end, pool=pool)

            if cond_value is None:
                continue

            fold_change, _ = _calculate_fold_change(
                cond_value=cond_value,
                control_value=control_value,
                organism_key=hts_key,
                study_id=study_id,
                cond_id=cond_id,
                control_id=control_id,
            )
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

    # Shared handle pool: reuse each study's bigWig across all genes instead of
    # reopening it per gene during a full-organism rebuild.
    with _BigWigPool() as pool:
        for feature in features:
            profile = _build_expression_profile(
                db, feature, organism_key, base_path, studies_config, hts_key,
                pool=pool,
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
    min_conditions: int = 5,
    direction: str = "positive"
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
        direction: Correlation direction ('positive', 'negative', or 'both')

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

    # Get organism_no for downstream analysis (e.g., GO Term Finder)
    organism_no = _get_organism_no_from_key(db, organism_key)

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

    # Validate direction parameter
    if direction not in ("positive", "negative", "both"):
        return SimilarGenesResponse(
            success=False,
            error=f"Invalid direction: {direction}. Use 'positive', 'negative', or 'both'"
        )

    # Try pre-computed correlation cache first (supports all metrics)
    cached_correlations = _get_similar_genes_from_cache(
        organism_key, query_feature.feature_name, metric, limit, direction
    )
    if cached_correlations:
        logger.debug(f"Using pre-computed {metric} correlations for {query_feature.feature_name}")

        # Fetch feature details for cached results
        feature_names = [c[0] for c in cached_correlations]
        features_map = {
            f.feature_name: f
            for f in db.query(Feature).filter(
                Feature.feature_name.in_(feature_names)
            ).all()
        }

        similar_genes: List[SimilarGene] = []
        for feature_name, corr, p_value, shared in cached_correlations:
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
            organism_no=organism_no,
            metric=metric,
            similar_genes=similar_genes,
            total_genes_compared=len(cached_correlations),
            conditions_used=0,  # Unknown from cache
            computation_time_ms=round(computation_time, 2)
        )

    # Fall back to on-the-fly computation
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

    # Filter to genes worth comparing (skip orf19.XXXX and B alleles for C. albicans)
    candidate_profiles = {}
    for feature_name, profile in all_profiles.items():
        if organism_key == "C_albicans_SC5314" and (feature_name.startswith("orf19.") or feature_name.endswith("_B")):
            continue
        if feature_name == query_feature.feature_name:
            continue
        candidate_profiles[feature_name] = profile

    # Optimization: If too many candidates, limit to top 3000 by variance
    # This reduces computation from O(n) to O(3000) with minimal quality loss
    MAX_CANDIDATES = 3000
    if NUMPY_AVAILABLE and len(candidate_profiles) > MAX_CANDIDATES:
        # Compute variance for each profile using numpy (much faster)
        variances = []
        for feature_name, profile in candidate_profiles.items():
            if len(profile) >= 5:
                vals = np.array(list(profile.values()))
                var = np.var(vals)
                variances.append((feature_name, var))

        # Keep top N by variance (most variable genes are most informative)
        variances.sort(key=lambda x: x[1], reverse=True)
        top_genes = set(v[0] for v in variances[:MAX_CANDIDATES])
        candidate_profiles = {k: v for k, v in candidate_profiles.items() if k in top_genes}

    # Compare against candidate genes using fast numpy computation
    correlations: List[Tuple[str, float, Optional[float], int]] = []

    if NUMPY_AVAILABLE and metric == "pearson":
        # Fast path: use numpy for batch correlation
        query_keys = set(query_profile.keys())

        for feature_name, profile in candidate_profiles.items():
            # Find shared conditions
            shared_keys = query_keys & set(profile.keys())
            n_shared = len(shared_keys)

            if n_shared < min_conditions:
                continue

            # Get values for shared conditions
            sorted_keys = sorted(shared_keys)
            x = np.array([query_profile[k] for k in sorted_keys])
            y = np.array([profile[k] for k in sorted_keys])

            # Fast Pearson correlation using numpy
            x_centered = x - x.mean()
            y_centered = y - y.mean()
            numerator = (x_centered * y_centered).sum()
            x_std = np.sqrt((x_centered ** 2).sum())
            y_std = np.sqrt((y_centered ** 2).sum())

            if x_std == 0 or y_std == 0:
                continue

            corr = numerator / (x_std * y_std)

            if not np.isfinite(corr):
                continue

            # Approximate p-value for speed
            # Note: The approximation 2*exp(-0.5*t^2) can exceed 1 for small t,
            # so we cap at 1.0 to ensure valid probability values
            if abs(corr) < 0.9999 and n_shared > 2:
                t_stat = corr * np.sqrt((n_shared - 2) / (1 - corr ** 2))
                p_value = min(max(2 * np.exp(-0.5 * t_stat ** 2), 1e-10), 1.0)
            else:
                p_value = 1e-10

            correlations.append((feature_name, float(corr), float(p_value), n_shared))
    else:
        # Fallback to scipy
        for feature_name, profile in candidate_profiles.items():
            result = _compute_correlation(query_profile, profile, metric, min_conditions)
            if result:
                corr, p_value, shared = result
                correlations.append((feature_name, corr, p_value, shared))

    # Filter and sort by direction
    if direction == "positive":
        # Only positive correlations, sorted descending
        filtered = [c for c in correlations if c[1] > 0]
        filtered.sort(key=lambda x: x[1], reverse=True)
    elif direction == "negative":
        # Only negative correlations, sorted ascending (most negative first)
        filtered = [c for c in correlations if c[1] < 0]
        filtered.sort(key=lambda x: x[1], reverse=False)
    else:  # both
        # All correlations, sorted by actual value (positive at top, negative at bottom)
        filtered = correlations
        filtered.sort(key=lambda x: x[1], reverse=True)

    # Take top N
    top_correlations = filtered[:limit]

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
        organism_no=organism_no,
        metric=metric,
        similar_genes=similar_genes,
        total_genes_compared=len(correlations),
        conditions_used=len(query_profile),
        computation_time_ms=round(computation_time, 2)
    )


# ============================================================================
# Batch Expression Data (for multi-gene heatmap)
# ============================================================================

def get_batch_expression_data(
    db: Session,
    gene_names: List[str],
    organism: str = "Candida albicans SC5314"
) -> BatchExpressionResponse:
    """
    Get expression data for multiple genes in a single request.

    This is optimized for the co-expression heatmap which needs expression
    data for the query gene plus similar genes. Uses the pre-computed
    expression profile cache for fast response times.

    Args:
        db: Database session
        gene_names: List of gene names to fetch
        organism: Organism display name (e.g., "Candida albicans SC5314")

    Returns:
        BatchExpressionResponse with expression data for each gene
    """
    import time
    start_time = time.time()

    # Map organism display name to HTS key
    hts_key = ORGANISM_TO_HTS_KEY.get(organism)
    if not hts_key:
        return BatchExpressionResponse(
            success=False,
            results=[],
            genes_found=0,
            genes_missing=len(gene_names),
            computation_time_ms=0
        )

    # Get studies config for this organism
    studies_config = EXPRESSION_STUDIES.get(hts_key, {})
    if not studies_config:
        return BatchExpressionResponse(
            success=False,
            results=[],
            genes_found=0,
            genes_missing=len(gene_names),
            computation_time_ms=0
        )

    # Get base path for HTS data
    base_path = HTS_BASE_PATHS.get(hts_key)
    if not base_path or not base_path.exists():
        return BatchExpressionResponse(
            success=False,
            results=[],
            genes_found=0,
            genes_missing=len(gene_names),
            computation_time_ms=0
        )

    results: List[BatchGeneExpression] = []
    genes_found = 0
    genes_missing = 0

    # One shared handle pool for the whole request: every gene reads the same
    # set of bigWig files, so opening each once (instead of once per gene) is
    # what keeps a large batch/matrix under the request timeout.
    with _BigWigPool() as pool:
        # Process each gene
        for gene_name in gene_names:
            # Find the feature
            feature = (
                db.query(Feature)
                .filter(
                    (func.upper(Feature.gene_name) == func.upper(gene_name)) |
                    (func.upper(Feature.feature_name) == func.upper(gene_name))
                )
                .first()
            )

            if not feature:
                results.append(BatchGeneExpression(
                    gene_name=gene_name,
                    data=None,
                    error="Gene not found"
                ))
                genes_missing += 1
                continue

            # Get expression data for this feature
            expr_data = _get_expression_for_organism(db, feature, organism, pool=pool)

            if expr_data:
                results.append(BatchGeneExpression(
                    gene_name=gene_name,
                    data=expr_data,
                    error=None
                ))
                genes_found += 1
            else:
                results.append(BatchGeneExpression(
                    gene_name=gene_name,
                    data=None,
                    error="No expression data for organism"
                ))
                genes_missing += 1

    computation_time = (time.time() - start_time) * 1000

    return BatchExpressionResponse(
        success=True,
        results=results,
        genes_found=genes_found,
        genes_missing=genes_missing,
        computation_time_ms=round(computation_time, 2)
    )


def generate_expression_matrix_csv(
    db: Session,
    gene_names: List[str],
    organism: str = "Candida albicans SC5314",
    include_metadata: bool = True,
    correlations: Optional[Dict[str, float]] = None,
) -> str:
    """
    Generate a CSV expression matrix for multiple genes.

    The matrix has:
    - Rows: genes (one per row)
    - Columns: conditions (organized by study)
    - Values: fold change values

    Args:
        db: Database session
        gene_names: List of gene names to include
        organism: Organism display name
        include_metadata: Include gene description and correlation columns
        correlations: Optional dict mapping gene names to correlation values

    Returns:
        CSV string with expression matrix
    """
    from datetime import datetime
    import csv
    import io

    # Get batch expression data
    batch_response = get_batch_expression_data(db, gene_names, organism)

    if not batch_response.success or not batch_response.results:
        return "# Error: No expression data available\n"

    # Build condition list from all genes' data
    # condition_key = "study_id|condition_id" for uniqueness
    all_conditions = []  # List of (study_id, condition_id, label, category)
    condition_set = set()

    for gene_result in batch_response.results:
        if gene_result.data and gene_result.data.studies:
            for study in gene_result.data.studies:
                for condition in study.conditions:
                    key = f"{study.study_id}|{condition.condition_id}"
                    if key not in condition_set:
                        condition_set.add(key)
                        all_conditions.append((
                            study.study_id,
                            condition.condition_id,
                            condition.label,
                            study.category
                        ))

    # Sort conditions by study, then by condition id
    all_conditions.sort(key=lambda x: (x[0], x[1]))

    # Build CSV
    output = io.StringIO()

    # Header comments
    output.write(f"# Expression Matrix\n")
    output.write(f"# Organism: {organism}\n")
    output.write(f"# Genes: {len(gene_names)}\n")
    output.write(f"# Conditions: {len(all_conditions)}\n")
    output.write(f"# Data type: Fold Change (vs control)\n")
    output.write(f"# Generated: {datetime.now().isoformat()}\n")
    output.write(f"#\n")

    # Build header row
    header = ["Gene", "Feature Name"]
    if include_metadata:
        header.append("Description")
        if correlations:
            header.append("Correlation")

    # Add condition columns (format: "Study|Condition Label")
    for study_id, cond_id, label, category in all_conditions:
        header.append(f"{study_id}|{label}")

    output.write("\t".join(header) + "\n")

    # Build data rows
    for gene_result in batch_response.results:
        gene_name = gene_result.gene_name
        if not gene_result.data:
            continue

        data = gene_result.data
        row = [
            data.gene_name or gene_name,
            data.feature_name or ""
        ]

        if include_metadata:
            row.append(data.description or "")
            if correlations:
                corr = correlations.get(gene_name, "")
                row.append(str(corr) if corr != "" else "")

        # Build fold change lookup for this gene
        fc_lookup = {}  # key = "study_id|condition_id" -> fold_change
        if data.studies:
            for study in data.studies:
                for condition in study.conditions:
                    key = f"{study.study_id}|{condition.condition_id}"
                    fc_lookup[key] = condition.fold_change

        # Add fold change values for each condition
        for study_id, cond_id, label, category in all_conditions:
            key = f"{study_id}|{cond_id}"
            fc = fc_lookup.get(key)
            if fc is not None:
                row.append(f"{fc:.4f}")
            else:
                row.append("")  # Missing data

        output.write("\t".join(row) + "\n")

    return output.getvalue()
