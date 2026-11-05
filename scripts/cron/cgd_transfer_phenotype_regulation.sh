#!/usr/bin/env bash
#
# Daily transfer of RNA accumulation phenotypes into regulation data.
# Adds a REGULATION row (shown on the locus Regulation tab as "CGD phenotype")
# for each new phenotype whose mutated gene is a regulator and whose reporter
# is a CGD gene, and removes rows a changed phenotype no longer supports;
# a summary is emailed to REGULATION_EMAIL when rows change. See
# scripts/regulation/transfer_phenotype_regulation.py.
#
# Usage:
#   ./slack-cron.sh ./cgd_transfer_phenotype_regulation.sh [--dry-run]
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

python3 scripts/regulation/transfer_phenotype_regulation.py --email "${REGULATION_EMAIL:-}" "$@"
