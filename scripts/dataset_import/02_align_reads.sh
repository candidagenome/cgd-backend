#!/bin/bash
#
# 02_align_reads.sh - Align FASTQ reads to reference genome using HISAT2
#
# Usage:
#   ./02_align_reads.sh <SPECIES> <FASTQ_DIR> <SAMPLE_NAME> [OUTPUT_DIR]
#
# Examples:
#   ./02_align_reads.sh C_auris_B8441 /data/tmp/fastq SRR12345678
#   ./02_align_reads.sh C_albicans_SC5314 ./fastq control_rep1 ./output
#
# Input:
#   - Single-end: <SAMPLE_NAME>.fastq.gz or <SAMPLE_NAME>_1.fastq.gz
#   - Paired-end: <SAMPLE_NAME>_1.fastq.gz and <SAMPLE_NAME>_2.fastq.gz
#
# Output:
#   - <SAMPLE_NAME>.sorted.bam
#   - <SAMPLE_NAME>.sorted.bam.bai
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Script directory for config
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse arguments
SPECIES="$1"
FASTQ_DIR="$2"
SAMPLE_NAME="$3"
OUTPUT_DIR="${4:-/data/tmp/dataset_import/aligned}"

# Validate arguments
if [ -z "$SPECIES" ] || [ -z "$FASTQ_DIR" ] || [ -z "$SAMPLE_NAME" ]; then
    echo -e "${RED}Error: Missing required arguments${NC}"
    echo ""
    echo "Usage: $0 <SPECIES> <FASTQ_DIR> <SAMPLE_NAME> [OUTPUT_DIR]"
    echo ""
    echo "Species options:"
    echo "  C_albicans_SC5314"
    echo "  C_auris_B8441"
    echo "  C_glabrata_CBS138"
    echo "  C_dubliniensis_CD36"
    echo "  C_parapsilosis_CDC317"
    echo "  C_tropicalis_MYA3404"
    exit 1
fi

# Configuration - adjust paths as needed
THREADS=8
HISAT2_EXTRA_ARGS="--dta"

# Species-specific paths (should match config.yaml)
case "$SPECIES" in
    C_albicans_SC5314)
        HISAT2_INDEX="/data/genomes/C_albicans_SC5314/hisat2_index/C_albicans_SC5314"
        ;;
    C_auris_B8441)
        HISAT2_INDEX="/data/genomes/C_auris_B8441/hisat2_index/C_auris_B8441"
        ;;
    C_glabrata_CBS138)
        HISAT2_INDEX="/data/genomes/C_glabrata_CBS138/hisat2_index/C_glabrata_CBS138"
        ;;
    C_dubliniensis_CD36)
        HISAT2_INDEX="/data/genomes/C_dubliniensis_CD36/hisat2_index/C_dubliniensis_CD36"
        ;;
    C_parapsilosis_CDC317)
        HISAT2_INDEX="/data/genomes/C_parapsilosis_CDC317/hisat2_index/C_parapsilosis_CDC317"
        ;;
    C_tropicalis_MYA3404)
        HISAT2_INDEX="/data/genomes/C_tropicalis_MYA3404/hisat2_index/C_tropicalis_MYA3404"
        ;;
    *)
        echo -e "${RED}Error: Unknown species: $SPECIES${NC}"
        exit 1
        ;;
esac

# Check HISAT2 index exists
if [ ! -f "${HISAT2_INDEX}.1.ht2" ]; then
    echo -e "${RED}Error: HISAT2 index not found: ${HISAT2_INDEX}${NC}"
    echo "Please build the index first with hisat2-build"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo -e "${GREEN}=== CGD Read Alignment ===${NC}"
echo "Species: $SPECIES"
echo "Sample: $SAMPLE_NAME"
echo "FASTQ Dir: $FASTQ_DIR"
echo "Output: $OUTPUT_DIR"
echo "Threads: $THREADS"
echo ""

# Detect input files (paired-end vs single-end)
R1=""
R2=""

if [ -f "$FASTQ_DIR/${SAMPLE_NAME}_1.fastq.gz" ]; then
    R1="$FASTQ_DIR/${SAMPLE_NAME}_1.fastq.gz"
    if [ -f "$FASTQ_DIR/${SAMPLE_NAME}_2.fastq.gz" ]; then
        R2="$FASTQ_DIR/${SAMPLE_NAME}_2.fastq.gz"
        echo "Detected: Paired-end reads"
    else
        echo "Detected: Single-end reads (R1 only)"
    fi
elif [ -f "$FASTQ_DIR/${SAMPLE_NAME}.fastq.gz" ]; then
    R1="$FASTQ_DIR/${SAMPLE_NAME}.fastq.gz"
    echo "Detected: Single-end reads"
else
    echo -e "${RED}Error: No FASTQ files found for sample $SAMPLE_NAME${NC}"
    echo "Looking for:"
    echo "  $FASTQ_DIR/${SAMPLE_NAME}.fastq.gz"
    echo "  $FASTQ_DIR/${SAMPLE_NAME}_1.fastq.gz"
    exit 1
fi

# Output files
SAM_FILE="$OUTPUT_DIR/${SAMPLE_NAME}.sam"
BAM_FILE="$OUTPUT_DIR/${SAMPLE_NAME}.bam"
SORTED_BAM="$OUTPUT_DIR/${SAMPLE_NAME}.sorted.bam"
ALIGN_LOG="$OUTPUT_DIR/${SAMPLE_NAME}.align.log"

# Check if already done
if [ -f "$SORTED_BAM" ] && [ -f "${SORTED_BAM}.bai" ]; then
    echo -e "${YELLOW}Output already exists: $SORTED_BAM${NC}"
    echo "Delete to re-run alignment"
    exit 0
fi

# Step 1: Align with HISAT2
echo -e "${YELLOW}Step 1: Aligning with HISAT2...${NC}"

if [ -n "$R2" ]; then
    # Paired-end
    hisat2 -p $THREADS $HISAT2_EXTRA_ARGS \
        -x "$HISAT2_INDEX" \
        -1 "$R1" \
        -2 "$R2" \
        -S "$SAM_FILE" \
        2> "$ALIGN_LOG"
else
    # Single-end
    hisat2 -p $THREADS $HISAT2_EXTRA_ARGS \
        -x "$HISAT2_INDEX" \
        -U "$R1" \
        -S "$SAM_FILE" \
        2> "$ALIGN_LOG"
fi

# Print alignment stats
echo ""
echo "Alignment statistics:"
tail -5 "$ALIGN_LOG"
echo ""

# Step 2: Convert to BAM
echo -e "${YELLOW}Step 2: Converting to BAM...${NC}"
samtools view -@ $THREADS -bS "$SAM_FILE" > "$BAM_FILE"

# Step 3: Sort BAM
echo -e "${YELLOW}Step 3: Sorting BAM...${NC}"
samtools sort -@ $THREADS -o "$SORTED_BAM" "$BAM_FILE"

# Step 4: Index BAM
echo -e "${YELLOW}Step 4: Indexing BAM...${NC}"
samtools index -@ $THREADS "$SORTED_BAM"

# Step 5: Cleanup intermediate files
echo -e "${YELLOW}Step 5: Cleaning up...${NC}"
rm -f "$SAM_FILE" "$BAM_FILE"

# Calculate alignment rate
TOTAL_READS=$(grep "reads; of these:" "$ALIGN_LOG" | awk '{print $1}')
ALIGNED_RATE=$(grep "overall alignment rate" "$ALIGN_LOG" | awk '{print $1}')

echo ""
echo -e "${GREEN}=== Alignment Complete ===${NC}"
echo "Output: $SORTED_BAM"
echo "Total reads: $TOTAL_READS"
echo "Alignment rate: $ALIGNED_RATE"
echo ""
ls -lh "$SORTED_BAM"
