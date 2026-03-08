#!/usr/bin/env bash
#
# Generate curator progress reports for all CGD organisms.
# Python equivalent of cgd-curatorReports (Perl version).
#
# Usage:
#   ./cgd_curator_reports.sh
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Activate virtual environment if it exists
if [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

# Change to project root for relative paths to work
cd "$PROJECT_ROOT"

echo "Starting curator reports generation at $(date)"
echo "========================================"

python3 "$SCRIPT_DIR/curator_reports.py" --strain C_albicans_SC5314
python3 "$SCRIPT_DIR/curator_reports.py" --strain C_dubliniensis_CD36
python3 "$SCRIPT_DIR/curator_reports.py" --strain C_glabrata_CBS138
python3 "$SCRIPT_DIR/curator_reports.py" --strain C_parapsilosis_CDC317
python3 "$SCRIPT_DIR/curator_reports.py" --strain C_auris_B8441

echo "========================================"
echo "Finished curator reports generation at $(date)"
