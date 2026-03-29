#!/usr/bin/env bash
#
# Dump sequence files (FASTA) for all CGD organisms.
#
# Usage:
#   ./slack-cron.sh ./cgd_dump_sequences.sh
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

# Base output directory
BASE_OUTPUT_DIR="${DOWNLOAD_DIR:-$PROJECT_ROOT/data}/sequence"
mkdir -p "$BASE_OUTPUT_DIR"

echo "CGD Sequence Dump"
echo "Generated: $(date)"
echo "Output directory: $BASE_OUTPUT_DIR"
echo "========================================"

# Track errors but continue processing all species
errors=0

run_dump() {
    local strain=$1
    local seq_source=$2
    local output_suffix=$3

    if [ -n "$seq_source" ]; then
        local output_dir="$BASE_OUTPUT_DIR/${strain}${output_suffix}"
        echo ""
        echo "Dumping $strain ($seq_source)..."
        mkdir -p "$output_dir"
        if ! python3 "$SCRIPT_DIR/dump_sequence.py" "$strain" "$seq_source" --output-dir "$output_dir" 2>&1 | grep -v "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.*-" | grep -v "^$"; then
            echo "ERROR: Failed to dump $strain ($seq_source)"
            errors=$((errors + 1))
        fi
    else
        local output_dir="$BASE_OUTPUT_DIR/$strain"
        echo ""
        echo "Dumping $strain..."
        mkdir -p "$output_dir"
        if ! python3 "$SCRIPT_DIR/dump_sequence.py" "$strain" --output-dir "$output_dir" 2>&1 | grep -v "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.*-" | grep -v "^$"; then
            echo "ERROR: Failed to dump $strain"
            errors=$((errors + 1))
        fi
    fi
}

# C. albicans has multiple assemblies
run_dump C_albicans_SC5314 "Assembly 22" "_A22"
run_dump C_albicans_SC5314 "Assembly 21" "_A21"
run_dump C_albicans_SC5314 "Assembly 19" "_A19"

# Other species (auto-detect seq_source)
run_dump C_dubliniensis_CD36
run_dump C_glabrata_CBS138
run_dump C_parapsilosis_CDC317
run_dump C_auris_B8441

echo ""
echo "========================================"

if [ $errors -gt 0 ]; then
    echo "ERROR: $errors dumps failed"
    exit 1
fi

echo "All sequence dumps completed successfully."
exit 0
