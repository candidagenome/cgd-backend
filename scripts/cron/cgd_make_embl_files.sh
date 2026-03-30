#!/usr/bin/env bash
#
# Generate EMBL format files for all CGD strains.
#
# This script creates EMBL format files containing chromosome sequences
# with ORF feature annotations for each strain.
#
# Output: $DOWNLOAD_DIR/embl/{strain}/{chromosome}.embl
#
# Usage:
#   ./slack-cron.sh ./cgd_make_embl_files.sh
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

echo "CGD Make EMBL Files"
echo "Generated: $(date)"
echo "========================================"

# Track errors but continue processing all species
errors=0

run_embl() {
    local strain=$1
    echo ""
    echo "Processing $strain..."
    if ! python3 "$SCRIPT_DIR/make_embl_files.py" "$strain" 2>&1 | grep -v "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.*-" | grep -v "^$"; then
        echo "ERROR: Failed to process $strain"
        errors=$((errors + 1))
    fi
}

# Process all strains
run_embl C_albicans_SC5314
run_embl C_dubliniensis_CD36
run_embl C_glabrata_CBS138
run_embl C_parapsilosis_CDC317
run_embl C_auris_B8441

echo ""
echo "========================================"

if [ $errors -gt 0 ]; then
    echo "ERROR: $errors species failed"
    exit 1
fi

echo "EMBL file generation completed successfully."
exit 0
