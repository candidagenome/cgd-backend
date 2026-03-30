#!/usr/bin/env bash
#
# Send agenda brief reminder email to curators.
#
# Meeting is Thursday 12:30PM, reminder sent Monday 9am.
#
# Usage:
#   ./slack-cron.sh ./cgd_send_agenda_reminder.sh
#
# Crontab entry (Monday 9am):
#   0 9 * * 1 cd /path/to/cgd-backend/scripts/cron && ./slack-cron.sh ./cgd_send_agenda_reminder.sh
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

echo "CGD Send Agenda Reminder"
echo "Generated: $(date)"
echo "========================================="

if python3 "$SCRIPT_DIR/send_agenda_reminder.py" 2>&1; then
    echo ""
    echo "========================================="
    echo "Agenda reminder sent successfully."
    exit 0
else
    echo ""
    echo "========================================="
    echo "ERROR: Failed to send agenda reminder."
    exit 1
fi
