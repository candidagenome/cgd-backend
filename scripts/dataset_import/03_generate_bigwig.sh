#!/bin/bash
#
# 03_generate_bigwig.sh - Generate BigWig coverage files from BAM
#
# Usage:
#   ./03_generate_bigwig.sh <BAM_FILE> [OUTPUT_DIR]
#
# Examples:
#   ./03_generate_bigwig.sh /data/aligned/SRR12345678.sorted.bam
#   ./03_generate_bigwig.sh sample.sorted.bam /data/HTS/C_auris_B8441/bam
#
# Output:
#   - <SAMPLE_NAME>.bw (normalized BigWig)
#   - <SAMPLE_NAME>.library_size.txt (total mapped reads)
#
# Requires:
#   - deeptools (bamCoverage)
#   - samtools
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Parse arguments
BAM_FILE="$1"
OUTPUT_DIR="${2:-$(dirname "$BAM_FILE")}"

if [ -z "$BAM_FILE" ]; then
    echo -e "${RED}Error: No BAM file provided${NC}"
    echo ""
    echo "Usage: $0 <BAM_FILE> [OUTPUT_DIR]"
    echo ""
    echo "Examples:"
    echo "  $0 /data/aligned/sample.sorted.bam"
    echo "  $0 sample.sorted.bam /data/HTS/C_auris_B8441/bam"
    exit 1
fi

if [ ! -f "$BAM_FILE" ]; then
    echo -e "${RED}Error: BAM file not found: $BAM_FILE${NC}"
    exit 1
fi

# Configuration
THREADS=8
BIN_SIZE=10
NORMALIZE="CPM"  # Counts Per Million

# Get sample name from BAM filename
BASENAME=$(basename "$BAM_FILE")
SAMPLE_NAME="${BASENAME%.sorted.bam}"
SAMPLE_NAME="${SAMPLE_NAME%.bam}"

# Output files
BIGWIG_FILE="$OUTPUT_DIR/${SAMPLE_NAME}.bw"
LIBRARY_SIZE_FILE="$OUTPUT_DIR/${SAMPLE_NAME}.library_size.txt"

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo -e "${GREEN}=== CGD BigWig Generation ===${NC}"
echo "Input: $BAM_FILE"
echo "Output: $BIGWIG_FILE"
echo "Normalization: $NORMALIZE"
echo "Bin size: $BIN_SIZE"
echo ""

# Check if already done
if [ -f "$BIGWIG_FILE" ]; then
    echo -e "${YELLOW}Output already exists: $BIGWIG_FILE${NC}"
    echo "Delete to re-generate"
    exit 0
fi

# Step 1: Calculate library size (total mapped reads)
echo -e "${YELLOW}Step 1: Calculating library size...${NC}"

# Get mapped reads count
MAPPED_READS=$(samtools view -c -F 260 "$BAM_FILE")
MAPPED_MILLIONS=$(echo "scale=6; $MAPPED_READS / 1000000" | bc)

echo "Mapped reads: $MAPPED_READS"
echo "Library size (millions): $MAPPED_MILLIONS"

# Save library size for expression_service.py
echo "$MAPPED_READS" > "$LIBRARY_SIZE_FILE"
echo "Saved to: $LIBRARY_SIZE_FILE"
echo ""

# Step 2: Generate BigWig with deeptools
echo -e "${YELLOW}Step 2: Generating BigWig...${NC}"

# Check if BAM is indexed
if [ ! -f "${BAM_FILE}.bai" ]; then
    echo "Indexing BAM file..."
    samtools index -@ $THREADS "$BAM_FILE"
fi

# Generate BigWig
# --normalizeUsing CPM: Counts Per Million normalization
# --exactScaling: More accurate but slower
# --binSize: Resolution in bp
bamCoverage \
    --bam "$BAM_FILE" \
    --outFileName "$BIGWIG_FILE" \
    --binSize $BIN_SIZE \
    --normalizeUsing $NORMALIZE \
    --exactScaling \
    --numberOfProcessors $THREADS \
    2>&1 | tee "$OUTPUT_DIR/${SAMPLE_NAME}.bamcoverage.log"

echo ""
echo -e "${GREEN}=== BigWig Generation Complete ===${NC}"
echo "BigWig: $BIGWIG_FILE"
echo "Library size: $LIBRARY_SIZE_FILE"
echo ""
ls -lh "$BIGWIG_FILE"

# Optional: Validate BigWig
if command -v bigWigInfo &> /dev/null; then
    echo ""
    echo "BigWig info:"
    bigWigInfo "$BIGWIG_FILE" | head -10
fi
