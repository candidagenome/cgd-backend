#!/usr/bin/env bash
#
# Generate curator progress reports for all CGD organisms.
# Python equivalent of cgd-curatorReports (Perl version).
#
# Usage:
#   ./slack-cron.sh ./cgd_curator_reports.sh
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

echo "Starting curator reports generation at $(date)"
echo "========================================"

# Track errors but continue processing all species
errors=0

for strain in C_albicans_SC5314 C_dubliniensis_CD36 C_glabrata_CBS138 C_parapsilosis_CDC317 C_auris_B8441; do
    echo "Processing $strain..."
    if ! python3 "$SCRIPT_DIR/curator_reports.py" --strain "$strain"; then
        echo "ERROR: Failed to generate report for $strain"
        errors=$((errors + 1))
    fi
done

echo "========================================"
echo "Finished curator reports generation at $(date)"

if [ $errors -gt 0 ]; then
    echo "ERROR: $errors species failed"
    exit 1
fi

exit 0
