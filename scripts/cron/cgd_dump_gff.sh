#!/usr/bin/env bash
#
# Dump GFF files for all CGD organisms.
#
# Usage:
#   ./slack-cron.sh ./cgd_dump_gff.sh
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

# Output directory
OUTPUT_DIR="${DOWNLOAD_DIR:-$PROJECT_ROOT/data}/gff"
mkdir -p "$OUTPUT_DIR"

echo "CGD GFF Dump"
echo "Generated: $(date)"
echo "Output directory: $OUTPUT_DIR"
echo "========================================"

# Track errors but continue processing all species
errors=0

run_dump() {
    local strain=$1
    echo ""
    echo "Dumping $strain..."
    if ! python3 "$SCRIPT_DIR/dump_gff.py" "$strain" --output "$OUTPUT_DIR/${strain}.gff" 2>&1 | grep -v "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.*-" | grep -v "^$"; then
        echo "ERROR: Failed to dump $strain"
        errors=$((errors + 1))
    fi
}

run_dump C_albicans_SC5314
run_dump C_dubliniensis_CD36
run_dump C_glabrata_CBS138
run_dump C_parapsilosis_CDC317
run_dump C_auris_B8441

echo ""
echo "========================================"

if [ $errors -gt 0 ]; then
    echo "ERROR: $errors species failed"
    exit 1
fi

echo "All GFF dumps completed successfully."
exit 0
