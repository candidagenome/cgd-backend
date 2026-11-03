#!/usr/bin/env bash
#
# Monthly PathoYeastract refresh (3rd of each month) for the locus Regulation tab.
# Downloads the current regulation flat files and rebuilds the per-gene JSON
# under REGULATION_DATA_DIR (default: $CGD_DATA_DIR/regulation/pathoyeastract);
# the API reads them per request, so no restart is needed. CGD-curated rows
# the new release also covers are emailed to REGULATION_EMAIL (only new
# ones); see scripts/regulation/load_pathoyeastract_flatfiles.py.
#
# Usage:
#   ./slack-cron.sh ./cgd_pathoyeastract_regulation.sh
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Activate virtual environment if it exists
if [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    source "$PROJECT_ROOT/venv/bin/activate"
elif [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

cd "$PROJECT_ROOT"

OUT="${REGULATION_DATA_DIR:-${CGD_DATA_DIR:-/data}/regulation/pathoyeastract}"

python3 scripts/regulation/load_pathoyeastract_flatfiles.py \
    --out "${OUT%/}" --email "${REGULATION_EMAIL:-}" "$@"
