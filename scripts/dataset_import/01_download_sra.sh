#!/bin/bash
#
# 01_download_sra.sh - Download FASTQ files from SRA/GEO
#
# Usage:
#   ./01_download_sra.sh <ACCESSION> [OUTPUT_DIR]
#
# Examples:
#   ./01_download_sra.sh PRJNA1086003
#   ./01_download_sra.sh GSE293594
#   ./01_download_sra.sh SRR12345678
#
# Supports:
#   - BioProject IDs (PRJNA*)
#   - GEO Series (GSE*)
#   - Individual SRA runs (SRR*, ERR*, DRR*)
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default output directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_OUTPUT_DIR="/data/tmp/dataset_import/fastq"

# Parse arguments
ACCESSION="$1"
OUTPUT_DIR="${2:-$DEFAULT_OUTPUT_DIR}"

if [ -z "$ACCESSION" ]; then
    echo -e "${RED}Error: No accession provided${NC}"
    echo "Usage: $0 <ACCESSION> [OUTPUT_DIR]"
    echo "  ACCESSION: PRJNA*, GSE*, SRR*, ERR*, or DRR*"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR"

echo -e "${GREEN}=== CGD Dataset Download ===${NC}"
echo "Accession: $ACCESSION"
echo "Output: $OUTPUT_DIR"
echo ""

# Function to download SRA runs
download_sra_run() {
    local run_id="$1"
    echo -e "${YELLOW}Downloading $run_id...${NC}"

    # Check if already downloaded
    if [ -f "${run_id}_1.fastq.gz" ] || [ -f "${run_id}.fastq.gz" ]; then
        echo "  Already exists, skipping"
        return 0
    fi

    # Download using prefetch (faster, uses Aspera if available)
    prefetch "$run_id" --max-size 100G -O . 2>/dev/null || {
        echo -e "${RED}  prefetch failed, trying direct download...${NC}"
    }

    # Convert to FASTQ
    echo "  Converting to FASTQ..."
    fasterq-dump "$run_id" --split-files --threads 4 -O . 2>/dev/null || {
        # Fallback for older SRA toolkit
        fastq-dump "$run_id" --split-files --gzip -O .
        return $?
    }

    # Compress if not already
    for f in ${run_id}*.fastq; do
        if [ -f "$f" ]; then
            echo "  Compressing $f..."
            gzip "$f"
        fi
    done

    # Clean up SRA cache
    rm -rf "$run_id"

    echo -e "${GREEN}  Done: $run_id${NC}"
}

# Function to get SRA run IDs from BioProject
get_runs_from_bioproject() {
    local bioproject="$1"
    echo -e "${YELLOW}Fetching SRA runs for BioProject $bioproject...${NC}" >&2

    # Use esearch/efetch to get run IDs
    esearch -db sra -query "$bioproject[BioProject]" 2>/dev/null | \
        efetch -format runinfo 2>/dev/null | \
        cut -d',' -f1 | \
        grep -E "^[SED]RR" || {
            echo -e "${RED}Error: Could not fetch runs for $bioproject${NC}" >&2
            echo "Make sure NCBI E-utilities (edirect) is installed" >&2
            exit 1
        }
}

# Function to get SRA run IDs from GEO
get_runs_from_geo() {
    local geo_id="$1"
    echo -e "${YELLOW}Fetching SRA runs for GEO $geo_id...${NC}" >&2

    # GEO to SRA mapping via NCBI
    esearch -db gds -query "$geo_id[Accession]" 2>/dev/null | \
        elink -target sra 2>/dev/null | \
        efetch -format runinfo 2>/dev/null | \
        cut -d',' -f1 | \
        grep -E "^[SED]RR" || {
            # Alternative: try ffq if available
            if command -v ffq &> /dev/null; then
                echo "  Trying ffq..." >&2
                ffq "$geo_id" 2>/dev/null | jq -r '.[].accession' | grep -E "^[SED]RR"
            else
                echo -e "${RED}Error: Could not fetch runs for $geo_id${NC}" >&2
                exit 1
            fi
        }
}

# Determine accession type and download
case "$ACCESSION" in
    PRJNA*|PRJEB*|PRJDB*)
        # BioProject
        echo "Detected: BioProject"
        RUNS=$(get_runs_from_bioproject "$ACCESSION")
        echo ""
        echo "Found $(echo "$RUNS" | wc -l) runs"
        echo ""

        for run in $RUNS; do
            download_sra_run "$run"
        done
        ;;

    GSE*|GSM*)
        # GEO accession
        echo "Detected: GEO accession"
        RUNS=$(get_runs_from_geo "$ACCESSION")
        echo ""
        echo "Found $(echo "$RUNS" | wc -l) runs"
        echo ""

        for run in $RUNS; do
            download_sra_run "$run"
        done
        ;;

    SRR*|ERR*|DRR*)
        # Individual SRA run
        echo "Detected: SRA run"
        download_sra_run "$ACCESSION"
        ;;

    *)
        echo -e "${RED}Error: Unknown accession format: $ACCESSION${NC}"
        echo "Supported: PRJNA*, PRJEB*, GSE*, GSM*, SRR*, ERR*, DRR*"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}=== Download Complete ===${NC}"
echo "Files saved to: $OUTPUT_DIR"
ls -lh "$OUTPUT_DIR"/*.fastq.gz 2>/dev/null | head -20 || echo "No FASTQ files found"
