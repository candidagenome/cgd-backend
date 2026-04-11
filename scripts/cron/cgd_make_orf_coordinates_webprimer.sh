#!/usr/bin/env bash
#
# Generate ORF coordinate files for web-primer.
#
# This script generates data files for the web-primer user interface:
# - orf_coordinates.table: ORF coordinates
# - orf2locus.table: ORF to gene name mapping
# - locus2orf.table: Gene name to ORF mapping
#
# Output: $DOWNLOAD_DIR/web-primer/
#
# Usage:
#   ./slack-cron.sh ./cgd_make_orf_coordinates_webprimer.sh
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

echo "CGD Make ORF Coordinates for Web-Primer"
echo "Generated: $(date)"
echo "========================================"

if python3 "$SCRIPT_DIR/make_orf_coordinates_webprimer.py" 2>&1 | grep -v "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.*-" | grep -v "^$"; then
    echo ""
    echo "========================================"
    echo "ORF coordinate generation completed successfully."
    exit 0
else
    echo ""
    echo "========================================"
    echo "ERROR: ORF coordinate generation failed."
    exit 1
fi
