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

echo "CGD Curator Progress Report"
echo "Generated: $(date)"
echo "========================================"
echo ""

# Track errors but continue processing all species
errors=0

for strain in C_albicans_SC5314 C_auris_B8441 C_dubliniensis_CD36 C_glabrata_CBS138 C_parapsilosis_CDC317 C_tropicalis; do
    output=$(python3 "$SCRIPT_DIR/curator_reports.py" --strain "$strain" 2>&1)
    exit_code=$?

    if [ $exit_code -ne 0 ]; then
        echo "ERROR: Failed to generate report for $strain"
        echo "$output"
        errors=$((errors + 1))
    else
        # Print summary (filter out INFO log lines)
        echo "$output" | grep -v "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.*INFO"
        echo ""
    fi
done

echo "========================================"

if [ $errors -gt 0 ]; then
    echo "ERROR: $errors species failed"
    exit 1
fi

echo "All reports generated successfully."
exit 0
