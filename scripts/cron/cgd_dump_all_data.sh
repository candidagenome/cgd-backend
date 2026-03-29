#!/usr/bin/env bash
#
# Master script to dump all CGD data files.
#
# This script runs all individual dump scripts in sequence:
# - GFF files (gene annotations)
# - GTF files (gene annotations for RNA-seq tools)
# - Sequence files (FASTA - chromosomes, ORFs, proteins)
# - Chromosomal feature files (tab-delimited feature data)
# - Phenotype data files
#
# Usage:
#   ./slack-cron.sh ./cgd_dump_all_data.sh
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

echo "CGD Complete Data Dump"
echo "Generated: $(date)"
echo "========================================"
echo ""

# Track results
total_scripts=0
passed_scripts=0
failed_scripts=0
failed_names=""

run_script() {
    local script_name=$1
    local script_path="$SCRIPT_DIR/$script_name"

    total_scripts=$((total_scripts + 1))

    echo ""
    echo "========================================"
    echo "Running: $script_name"
    echo "========================================"
    echo ""

    if [ ! -x "$script_path" ]; then
        echo "ERROR: Script not found or not executable: $script_path"
        failed_scripts=$((failed_scripts + 1))
        failed_names="$failed_names $script_name"
        return 1
    fi

    if "$script_path"; then
        passed_scripts=$((passed_scripts + 1))
        echo ""
        echo "SUCCESS: $script_name completed"
        return 0
    else
        failed_scripts=$((failed_scripts + 1))
        failed_names="$failed_names $script_name"
        echo ""
        echo "FAILED: $script_name"
        return 1
    fi
}

# Run all dump scripts in sequence
run_script "cgd_dump_gff.sh"
run_script "cgd_dump_gtf.sh"
run_script "cgd_dump_sequences.sh"
run_script "cgd_dump_chromosomal_features.sh"
run_script "cgd_dump_phenotype_data.sh"

# Final summary
echo ""
echo "========================================"
echo "CGD Data Dump Summary"
echo "========================================"
echo ""
echo "Total scripts: $total_scripts"
echo "Passed: $passed_scripts"
echo "Failed: $failed_scripts"

if [ $failed_scripts -gt 0 ]; then
    echo ""
    echo "Failed scripts:$failed_names"
    echo ""
    echo "ERROR: Some data dumps failed"
    exit 1
fi

echo ""
echo "All data dumps completed successfully."
exit 0
