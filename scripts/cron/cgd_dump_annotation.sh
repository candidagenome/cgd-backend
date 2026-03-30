#!/usr/bin/env bash
#
# Dump GO annotations to GAF (Gene Association File) format.
#
# This script generates gene_association.cgd file in GAF 2.0 format
# for submission to the GO Consortium.
#
# Usage:
#   ./slack-cron.sh ./cgd_dump_annotation.sh
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

echo "CGD GO Annotation Dump (GAF)"
echo "Generated: $(date)"
echo "========================================"

if python3 "$SCRIPT_DIR/dump_annotation.py" 2>&1 | grep -v "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.*-" | grep -v "^$"; then
    echo ""
    echo "========================================"
    echo "GO annotation dump completed successfully."
    exit 0
else
    echo ""
    echo "========================================"
    echo "ERROR: GO annotation dump failed."
    exit 1
fi
