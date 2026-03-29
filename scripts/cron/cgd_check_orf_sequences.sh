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

# C. albicans has both Assembly 19 and Assembly 22 ORFs, filter to Assembly 22 only
for strain_args in "C_albicans_SC5314 --assembly Assembly 22" "C_dubliniensis_CD36" "C_glabrata_CBS138" "C_parapsilosis_CDC317" "C_auris_B8441"; do
    strain=$(echo "$strain_args" | awk '{print $1}')
    echo "Checking $strain..."

    # shellcheck disable=SC2086
    if ! python3 "$SCRIPT_DIR/check_orf_sequences.py" $strain_args 2>&1 | grep -v "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.*INFO"; then
        echo "ERROR: Failed to check $strain"
        errors=$((errors + 1))
    fi
done

echo "========================================"

if [ $errors -gt 0 ]; then
    echo "ERROR: $errors species failed"
    exit 1
fi

echo "All ORF checks completed successfully."
exit 0
