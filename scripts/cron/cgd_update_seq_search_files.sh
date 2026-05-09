#!/usr/bin/env bash
#
# Update sequence search files for PatMatch and BLAST.
#
# This script creates searchable sequence files from weekly sequence dumps:
# - Plain-text FASTA files for PatMatch search
# - BLAST-formatted databases using makeblastdb
#
# Prerequisites:
# - Sequence dump files must exist in $DOWNLOAD_DIR/sequence/{strain}/
# - Run cgd_dump_sequences.sh first to generate source files
#
# Usage:
#   ./slack-cron.sh ./cgd_update_seq_search_files.sh
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Activate virtual environment if it exists
if [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    source "$PROJECT_ROOT/venv/bin/activate"
elif [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

# Change to project root for relative paths to work
cd "$PROJECT_ROOT"

echo "CGD Update Sequence Search Files"
echo "Generated: $(date)"
echo "========================================"

# Track errors but continue processing all species
errors=0

run_update() {
    local strain=$1
    echo ""
    echo "Processing $strain..."
    if ! python3 "$SCRIPT_DIR/update_seq_search_files.py" "$strain" 2>&1 | grep -v "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.*-" | grep -v "^$"; then
        echo "ERROR: Failed to process $strain"
        errors=$((errors + 1))
    fi
}

# Process all strains
run_update C_albicans_SC5314
run_update C_auris_B8441
run_update C_dubliniensis_CD36
run_update C_glabrata_CBS138
run_update C_parapsilosis_CDC317
run_update C_tropicalis_MYA-3404

echo ""
echo "========================================"

if [ $errors -gt 0 ]; then
    echo "ERROR: $errors species failed"
    exit 1
fi

echo "Sequence search file update completed successfully."
exit 0
