#!/bin/bash
#
# run_pipeline.sh - Master script to run the complete dataset import pipeline
#
# Usage:
#   ./run_pipeline.sh <SPECIES> <ACCESSION> <STUDY_NAME> [OPTIONS]
#
# Examples:
#   ./run_pipeline.sh C_auris_B8441 PRJNA1086003 Wang_2024
#   ./run_pipeline.sh C_albicans_SC5314 GSE245114 Henry_2024 --pmid 38380913
#
# Steps:
#   1. Download FASTQ from SRA/GEO
#   2. Align reads to reference genome
#   3. Generate BigWig coverage files
#   4. Generate expression_service.py config
#   5. Copy BigWig to final location
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Default directories
WORK_DIR="/data/tmp/dataset_import"
FASTQ_DIR="$WORK_DIR/fastq"
ALIGNED_DIR="$WORK_DIR/aligned"
BIGWIG_DIR="$WORK_DIR/bigwig"

# Parse required arguments
SPECIES="$1"
ACCESSION="$2"
STUDY_NAME="$3"
shift 3 || true

# Parse optional arguments
PMID=""
NCBI_ID="$ACCESSION"
DESCRIPTION=""
SKIP_DOWNLOAD=false
SKIP_ALIGN=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --pmid)
            PMID="$2"
            shift 2
            ;;
        --description)
            DESCRIPTION="$2"
            shift 2
            ;;
        --skip-download)
            SKIP_DOWNLOAD=true
            shift
            ;;
        --skip-align)
            SKIP_ALIGN=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Validate arguments
if [ -z "$SPECIES" ] || [ -z "$ACCESSION" ] || [ -z "$STUDY_NAME" ]; then
    echo -e "${RED}Error: Missing required arguments${NC}"
    echo ""
    echo "Usage: $0 <SPECIES> <ACCESSION> <STUDY_NAME> [OPTIONS]"
    echo ""
    echo "Arguments:"
    echo "  SPECIES     Species ID (e.g., C_auris_B8441, C_albicans_SC5314)"
    echo "  ACCESSION   SRA/GEO accession (e.g., PRJNA1086003, GSE245114)"
    echo "  STUDY_NAME  Study name for config (e.g., Wang_2024)"
    echo ""
    echo "Options:"
    echo "  --pmid ID           PubMed ID"
    echo "  --description TEXT  Study description"
    echo "  --skip-download     Skip download step (use existing FASTQ)"
    echo "  --skip-align        Skip alignment step (use existing BAM)"
    echo "  --dry-run           Print commands without executing"
    echo ""
    echo "Supported species:"
    echo "  C_albicans_SC5314"
    echo "  C_auris_B8441"
    echo "  C_glabrata_CBS138"
    echo "  C_dubliniensis_CD36"
    echo "  C_parapsilosis_CDC317"
    echo "  C_tropicalis_MYA3404"
    exit 1
fi

# Final output directory
FINAL_DIR="/data/HTS/${SPECIES}/bam/${STUDY_NAME}"

# Print banner
echo ""
echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║           CGD Expression Dataset Import Pipeline              ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}Species:${NC}    $SPECIES"
echo -e "${GREEN}Accession:${NC}  $ACCESSION"
echo -e "${GREEN}Study:${NC}      $STUDY_NAME"
echo -e "${GREEN}PMID:${NC}       ${PMID:-'Not provided'}"
echo -e "${GREEN}Output:${NC}     $FINAL_DIR"
echo ""

if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}=== DRY RUN MODE - Commands will be printed but not executed ===${NC}"
    echo ""
fi

# Create directories
run_cmd() {
    if [ "$DRY_RUN" = true ]; then
        echo -e "${YELLOW}[DRY RUN]${NC} $*"
    else
        echo -e "${BLUE}[RUNNING]${NC} $*"
        "$@"
    fi
}

mkdir -p "$WORK_DIR" "$FASTQ_DIR" "$ALIGNED_DIR" "$BIGWIG_DIR"

# ============================================================
# STEP 1: Download FASTQ files
# ============================================================
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}STEP 1: Download FASTQ files${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if [ "$SKIP_DOWNLOAD" = true ]; then
    echo -e "${YELLOW}Skipping download (--skip-download)${NC}"
else
    run_cmd "$SCRIPT_DIR/01_download_sra.sh" "$ACCESSION" "$FASTQ_DIR/$STUDY_NAME"
fi

# Get list of samples (SRR IDs)
SAMPLES=$(ls "$FASTQ_DIR/$STUDY_NAME"/*.fastq.gz 2>/dev/null | \
    xargs -n1 basename | \
    sed 's/_[12]\.fastq\.gz$//' | \
    sed 's/\.fastq\.gz$//' | \
    sort -u)

if [ -z "$SAMPLES" ]; then
    echo -e "${RED}Error: No FASTQ files found in $FASTQ_DIR/$STUDY_NAME${NC}"
    exit 1
fi

SAMPLE_COUNT=$(echo "$SAMPLES" | wc -l)
echo ""
echo "Found $SAMPLE_COUNT samples:"
echo "$SAMPLES" | head -10
[ "$SAMPLE_COUNT" -gt 10 ] && echo "... and $((SAMPLE_COUNT - 10)) more"

# ============================================================
# STEP 2: Align reads
# ============================================================
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}STEP 2: Align reads to $SPECIES genome${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if [ "$SKIP_ALIGN" = true ]; then
    echo -e "${YELLOW}Skipping alignment (--skip-align)${NC}"
else
    for sample in $SAMPLES; do
        echo ""
        echo -e "${YELLOW}Aligning sample: $sample${NC}"
        run_cmd "$SCRIPT_DIR/02_align_reads.sh" "$SPECIES" "$FASTQ_DIR/$STUDY_NAME" "$sample" "$ALIGNED_DIR/$STUDY_NAME"
    done
fi

# ============================================================
# STEP 3: Generate BigWig files
# ============================================================
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}STEP 3: Generate BigWig coverage files${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

for sample in $SAMPLES; do
    BAM_FILE="$ALIGNED_DIR/$STUDY_NAME/${sample}.sorted.bam"
    if [ -f "$BAM_FILE" ]; then
        echo ""
        echo -e "${YELLOW}Generating BigWig for: $sample${NC}"
        run_cmd "$SCRIPT_DIR/03_generate_bigwig.sh" "$BAM_FILE" "$BIGWIG_DIR/$STUDY_NAME"
    else
        echo -e "${RED}Warning: BAM not found: $BAM_FILE${NC}"
    fi
done

# ============================================================
# STEP 4: Copy to final location
# ============================================================
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}STEP 4: Copy BigWig files to final location${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

run_cmd mkdir -p "$FINAL_DIR"
run_cmd cp "$BIGWIG_DIR/$STUDY_NAME"/*.bw "$FINAL_DIR/"
run_cmd cp "$BIGWIG_DIR/$STUDY_NAME"/*.library_size.txt "$FINAL_DIR/" 2>/dev/null || true

echo ""
echo "BigWig files in $FINAL_DIR:"
ls -lh "$FINAL_DIR"/*.bw 2>/dev/null | head -10

# ============================================================
# STEP 5: Generate expression_service.py config
# ============================================================
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}STEP 5: Generate expression_service.py configuration${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

CONFIG_ARGS="--species $SPECIES --study $STUDY_NAME --bigwig-dir $FINAL_DIR"
[ -n "$PMID" ] && CONFIG_ARGS="$CONFIG_ARGS --pmid $PMID"
[ -n "$NCBI_ID" ] && CONFIG_ARGS="$CONFIG_ARGS --ncbi $NCBI_ID"
[ -n "$DESCRIPTION" ] && CONFIG_ARGS="$CONFIG_ARGS --description \"$DESCRIPTION\""

echo ""
echo "Generated configuration:"
echo "========================"
python3 "$SCRIPT_DIR/04_generate_config.py" $CONFIG_ARGS

# ============================================================
# Summary
# ============================================================
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Pipeline Complete!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo "Summary:"
echo "  Species:      $SPECIES"
echo "  Study:        $STUDY_NAME"
echo "  Samples:      $SAMPLE_COUNT"
echo "  BigWig files: $FINAL_DIR"
echo ""
echo "Next steps:"
echo "  1. Add the generated config to cgd/api/services/expression_service.py"
echo "  2. Add library sizes to LIBRARY_SIZES dict"
echo "  3. Rebuild expression cache: python scripts/build_expression_cache.py --organism $SPECIES"
echo "  4. Add JBrowse2 track configuration (optional)"
echo ""
