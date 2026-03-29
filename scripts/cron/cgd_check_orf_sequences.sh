#!/usr/bin/env bash
#
# Run ORF sequence checks for all CGD organisms.
#
# Usage:
#   ./slack-cron.sh ./cgd_check_orf_sequences.sh
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

echo "CGD ORF Sequence Check Report"
echo "Generated: $(date)"
echo "========================================"
echo ""

# Track errors but continue processing all species
errors=0

run_check() {
    local strain=$1
    shift
    if ! python3 "$SCRIPT_DIR/check_orf_sequences.py" "$strain" "$@" 2>&1 | grep -v "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.*-"; then
        echo "ERROR: Failed to check $strain"
        errors=$((errors + 1))
    fi
}

# C. albicans has both Assembly 19 and Assembly 22 ORFs, filter to Assembly 22 only
run_check C_albicans_SC5314 --assembly "Assembly 22"
run_check C_dubliniensis_CD36
run_check C_glabrata_CBS138
run_check C_parapsilosis_CDC317
run_check C_auris_B8441

echo "========================================"

if [ $errors -gt 0 ]; then
    echo "ERROR: $errors species failed"
    exit 1
fi

echo "All ORF checks completed successfully."
exit 0
