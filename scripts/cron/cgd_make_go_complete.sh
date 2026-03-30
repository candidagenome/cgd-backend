#!/usr/bin/env bash
#
# Make CGD GO-complete by assigning root GO terms to features without annotations.
#
# This script finds features that do not have any GO annotations for a particular
# aspect (P, F, C) and assigns the root term with 'ND' evidence code.
#
# Usage:
#   ./slack-cron.sh ./cgd_make_go_complete.sh
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

echo "CGD Make GO-Complete"
echo "Generated: $(date)"
echo "========================================"

# Track errors but continue processing all species
errors=0

run_go_complete() {
    local strain=$1
    echo ""
    echo "Processing $strain..."
    if ! python3 "$SCRIPT_DIR/make_go_complete.py" "$strain" 2>&1 | grep -v "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.*-" | grep -v "^$"; then
        echo "ERROR: Failed to process $strain"
        errors=$((errors + 1))
    fi
}

# Process all strains (matching the Perl script behavior)
# Note: C_auris and C_dubliniensis were commented out in original Perl script
run_go_complete C_albicans_SC5314
run_go_complete C_glabrata_CBS138
run_go_complete C_parapsilosis_CDC317

echo ""
echo "========================================"

if [ $errors -gt 0 ]; then
    echo "ERROR: $errors species failed"
    exit 1
fi

echo "GO-complete processing completed successfully."
exit 0
