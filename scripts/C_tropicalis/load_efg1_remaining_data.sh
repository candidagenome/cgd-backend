#!/usr/bin/env bash
#
# Reproducible recipe for loading the remaining locus data for the new
# C. tropicalis gene EFG1 / CTRG_00421.5 (feature_no 206940).
#
# This gene was created by the new curation "New Feature" tool and, at the time
# the InterProScan domains were loaded, that step was run by hand with no
# committed script. This file captures the reproducible commands so the work can
# be re-run or ported to prod.
#
# Context / status (checked 2026-07-22, DEV):
#   DONE : feature + CGDID, CDS subfeature (CTRG_00421.5_cds1),
#          genomic(1704bp)+protein(567aa) seqs, protein_info row,
#          InterProScan domains (6 hits: APSES/KilA-N HTH, MBP1, SOK2 PANTHER)
#   TODO : protein properties (automatable, below);
#          GO annotations, orthologs, description, literature (need curator data)
#
# The generic single-locus protein runners live in scripts/proteins/ and take
# --gene / --strain-abbrev (the "pkh2" in their names is historical). They are
# the targeted counterparts to the whole-strain loaders.
#
# Usage:
#   Run from the repo root on cgd-backend-dev with the venv active:
#     cd ~/work/cgd-backend && source venv/bin/activate
#     bash scripts/C_tropicalis/load_efg1_remaining_data.sh --dry-run
#     bash scripts/C_tropicalis/load_efg1_remaining_data.sh          # live
#
set -euo pipefail

GENE="EFG1"
STRAIN="C_tropicalis"
CREATED_BY="CGDADMIN"
DRY_RUN=""

for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN="--dry-run" ;;
        *) echo "Unknown argument: $arg" >&2; exit 1 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

echo "=== EFG1 / CTRG_00421.5 remaining-data load (${STRAIN}) ${DRY_RUN:-(LIVE)} ==="

# ---------------------------------------------------------------------------
# Step 1: Protein properties (MW, pI, length, GRAVY, aromaticity, AA comp).
# Codon usage (CAI/CBI/FOP) is left untouched unless a strain verified-CDS
# reference FASTA is passed via --coding-ref; it is currently NULL for this gene.
# ---------------------------------------------------------------------------
echo "--- Step 1: protein properties ---"
python scripts/proteins/run_pkh2_protein_props.py \
    --gene "${GENE}" \
    --strain-abbrev "${STRAIN}" \
    --created-by "${CREATED_BY}" \
    ${DRY_RUN}

# ---------------------------------------------------------------------------
# Steps below still require curator-provided data and are not automatable here.
# Left as documented placeholders:
#   - GO annotations   (go_annotation)      : load via curator GAF / annotation TSV
#   - Orthologs        (feat_homology)      : scripts/C_tropicalis/load_orthologs.py
#   - Description      (feat_para/paragraph): scripts/C_tropicalis/load_descriptions.py
#   - Literature       (refprop_feat)       : scripts/C_tropicalis/load_literature.py
# Each needs its input data file for EFG1 before it can run.
# ---------------------------------------------------------------------------

echo "=== Done. Remaining (curator data required): GO, orthologs, description, literature ==="
