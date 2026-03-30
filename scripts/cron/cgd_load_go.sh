#!/usr/bin/env bash
#
# Load/update GO (Gene Ontology) info from OBO file.
#
# This script downloads the latest gene_ontology.obo file from the GO Consortium
# and updates the GO table in the database.
#
# Usage:
#   ./slack-cron.sh ./cgd_load_go.sh
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

echo "CGD GO Ontology Load"
echo "Generated: $(date)"
echo "========================================"

if python3 "$SCRIPT_DIR/load_go.py" 2>&1 | grep -v "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.*-" | grep -v "^$"; then
    echo ""
    echo "========================================"
    echo "GO load completed successfully."
    exit 0
else
    echo ""
    echo "========================================"
    echo "ERROR: GO load failed."
    exit 1
fi
