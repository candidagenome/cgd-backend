#!/bin/bash
#
# Slack notification wrapper for cron jobs.
#
# Wraps any command and sends output to Slack when complete.
# Reads SLACK_WEBHOOK_URL from environment (via .env file).
#
# Usage:
#   ./slack-cron.sh /path/to/your/script.sh [args...]
#
# Environment:
#   SLACK_WEBHOOK_URL - Slack incoming webhook URL (required)
#

set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Load environment variables from .env if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# Check for webhook URL
if [ -z "$SLACK_WEBHOOK_URL" ]; then
    echo "Error: SLACK_WEBHOOK_URL not set in environment or .env file" >&2
    # Still run the command, just don't notify
    exec "$@"
fi

# Run the command and capture output
OUTPUT=$("$@" 2>&1)
EXIT_CODE=$?

# Truncate output if too long (Slack limit ~3000 chars for code blocks)
OUTPUT="${OUTPUT:0:3000}"

# Determine status
# Check for successful completion marker even if exit code is non-zero
# (minor errors like duplicate PMIDs shouldn't fail the whole job)
if [ $EXIT_CODE -eq 0 ]; then
    STATUS="Success"
    EMOJI=":white_check_mark:"
elif echo "$OUTPUT" | grep -q "Finished PubMed reference loading"; then
    # Pipeline completed successfully despite minor errors
    STATUS="Success (with minor errors)"
    EMOJI=":white_check_mark:"
    EXIT_CODE=0
else
    STATUS="Failed (exit code: $EXIT_CODE)"
    EMOJI=":x:"
fi

# Determine environment label
if [ "$ENV_STATE" = "prod" ] || [ "$ENV_STATE" = "production" ]; then
    ENV_LABEL="PROD"
else
    ENV_LABEL="DEV"
fi

# Use jq for proper JSON escaping
MESSAGE=$(printf "%s *Cron Job (%s):* \`%s\`\n*Status:* %s\n\`\`\`%s\`\`\`" "$EMOJI" "$ENV_LABEL" "$1" "$STATUS" "$OUTPUT")
JSON=$(jq -n --arg text "$MESSAGE" '{text: $text}')

curl -s -X POST -H "Content-type: application/json" --data "$JSON" "$SLACK_WEBHOOK_URL" > /dev/null

exit $EXIT_CODE
