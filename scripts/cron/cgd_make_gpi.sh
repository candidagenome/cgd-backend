#!/usr/bin/env bash
#
# Generate GPI (Gene Product Information) files for all CGD organisms.
#
# This script generates GPI 2.0 format files for submission to the GO Consortium.
#
# Usage:
#   ./slack-cron.sh ./cgd_make_gpi.sh
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

echo "CGD GPI File Generation"
echo "Generated: $(date)"
echo "========================================"

# Track errors but continue processing all species
errors=0

run_gpi() {
    local strain=$1
    echo ""
    echo "Generating GPI for $strain..."
    if ! python3 "$SCRIPT_DIR/make_gpi.py" "$strain" 2>&1 | grep -v "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.*-" | grep -v "^$"; then
        echo "ERROR: Failed to generate GPI for $strain"
        errors=$((errors + 1))
    fi
}

run_gpi C_albicans_SC5314
run_gpi C_dubliniensis_CD36
run_gpi C_glabrata_CBS138
run_gpi C_parapsilosis_CDC317
run_gpi C_auris_B8441
run_gpi C_tropicalis

echo ""
echo "========================================"

if [ $errors -gt 0 ]; then
    echo "ERROR: $errors species failed"
    exit 1
fi

echo "All GPI files generated successfully."
exit 0
