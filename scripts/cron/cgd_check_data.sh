#!/usr/bin/env bash
#
# Check data for complex business rules.
#
# This script runs data integrity checks and reports violations
# to curators via email.
#
# Usage:
#   ./slack-cron.sh ./cgd_check_data.sh
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

echo "CGD Data Check"
echo "Generated: $(date)"
echo "========================================"

if python3 "$SCRIPT_DIR/check_data.py" 2>&1 | grep -v "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.*-" | grep -v "^$"; then
    echo ""
    echo "========================================"
    echo "Data check completed."
    exit 0
else
    echo ""
    echo "========================================"
    echo "Data check completed with issues found."
    exit 0
fi
