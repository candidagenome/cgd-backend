#!/bin/bash
#
# Reindex Elasticsearch search index
#
# This script rebuilds the cgd_search index with current data from Oracle.
# Run daily to keep search results up to date.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_ROOT"
source .venv/bin/activate 2>/dev/null || source venv/bin/activate

echo "Starting Elasticsearch reindex at $(date)"
python -m cgd.cli.commands reindex
echo "Finished Elasticsearch reindex at $(date)"
