#!/bin/bash
# Run the transitive ortholog loading process
#
# Usage:
#   ./run_ortholog_loading.sh validate   # Run validation only
#   ./run_ortholog_loading.sh load       # Load after validation (dry-run)
#   ./run_ortholog_loading.sh load-real  # Actually load data
#   ./run_ortholog_loading.sh all        # Full process with dry-run load

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="/home/ec2-user/work/cgd-backend"
DATA_DIR="/data/cgob"

# Activate virtual environment
cd "$BACKEND_DIR"
source venv/bin/activate
export PYTHONPATH="$BACKEND_DIR"

echo "=== C. glabrata Transitive Ortholog Loading ==="
echo "Date: $(date)"
echo ""

case "${1:-help}" in
    validate)
        echo "Step 1: Validating transitive orthologs..."
        python "$SCRIPT_DIR/validate_transitive_orthologs.py" \
            --input "$DATA_DIR/cglabrata_transitive_orthologs.tsv" \
            --output "$DATA_DIR/validation_report.tsv" \
            --env "$BACKEND_DIR/.env"
        echo ""
        echo "Validation complete. Review report at: $DATA_DIR/validation_report.tsv"
        echo "Additions file at: $DATA_DIR/validation_report_additions.tsv"
        ;;

    load)
        echo "Step 2: Loading orthologs (DRY RUN)..."
        if [ ! -f "$DATA_DIR/validation_report_additions.tsv" ]; then
            echo "ERROR: Additions file not found. Run 'validate' first."
            exit 1
        fi
        python "$SCRIPT_DIR/load_transitive_orthologs.py" \
            --additions "$DATA_DIR/validation_report_additions.tsv" \
            --env "$BACKEND_DIR/.env" \
            --dry-run
        ;;

    load-real)
        echo "Step 2: Loading orthologs (REAL)..."
        if [ ! -f "$DATA_DIR/validation_report_additions.tsv" ]; then
            echo "ERROR: Additions file not found. Run 'validate' first."
            exit 1
        fi
        read -p "This will modify the database. Continue? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            python "$SCRIPT_DIR/load_transitive_orthologs.py" \
                --additions "$DATA_DIR/validation_report_additions.tsv" \
                --env "$BACKEND_DIR/.env" \
                --verify
        else
            echo "Aborted."
        fi
        ;;

    all)
        echo "Running full process with dry-run..."
        echo ""
        $0 validate
        echo ""
        $0 load
        ;;

    help|*)
        echo "Usage: $0 {validate|load|load-real|all}"
        echo ""
        echo "Commands:"
        echo "  validate   - Analyze transitive orthologs and generate additions file"
        echo "  load       - Preview loading (dry-run)"
        echo "  load-real  - Actually load data into database"
        echo "  all        - Run validate and load (dry-run)"
        echo ""
        echo "Typical workflow:"
        echo "  1. $0 validate     # Check what needs to be done"
        echo "  2. Review reports in $DATA_DIR/"
        echo "  3. $0 load         # Preview changes"
        echo "  4. $0 load-real    # Apply changes"
        ;;
esac
