#!/usr/bin/env bash
#
# Update ORF classifications for all CGD organisms.
# Python equivalent of cgd-updateORFclassifications (Perl version).
#
# Usage:
#   ./slack-cron.sh ./cgd_update_orf_classifications.sh
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

echo "CGD ORF Classification Update"
echo "Generated: $(date)"
echo "========================================"

# Track errors but continue processing all species
errors=0

run_update() {
    local strain=$1
    echo ""
    echo "Updating $strain..."
    if ! python3 "$SCRIPT_DIR/update_orf_classifications.py" "$strain" 2>&1 | grep -v "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.*-" | grep -v "^$"; then
        echo "ERROR: Failed to update $strain"
        errors=$((errors + 1))
    fi
}

run_update C_albicans_SC5314
run_update C_dubliniensis_CD36
run_update C_glabrata_CBS138
run_update C_parapsilosis_CDC317
run_update C_auris_B8441

echo ""
echo "========================================"

if [ $errors -gt 0 ]; then
    echo "ERROR: $errors species failed"
    exit 1
fi

echo "All ORF classification updates completed successfully."
exit 0
