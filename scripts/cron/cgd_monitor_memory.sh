#!/bin/bash
# cgd_monitor_memory.sh
# Monitors CGD API memory usage and restarts if threshold exceeded
#
# Install on server:
#   sudo cp cgd_monitor_memory.sh /opt/cgd_api/scripts/monitor_memory.sh
#   sudo chmod +x /opt/cgd_api/scripts/monitor_memory.sh
#   echo '*/5 * * * * root /opt/cgd_api/scripts/monitor_memory.sh' | sudo tee /etc/cron.d/cgd-api-monitor
#
# Triggers restart when:
#   - System memory usage > 80%
#   - Any single gunicorn worker > 4GB (indicates memory leak)

THRESHOLD_PERCENT=80
WORKER_THRESHOLD_MB=4000
LOG_FILE="/opt/cgd_api/logs/monitor.log"
SERVICE_NAME="cgd-api"

# Get memory usage percentage
mem_used_percent=$(free | awk '/^Mem:/ {printf "%.0f", $3/$2 * 100}')

# Get largest gunicorn worker memory in MB
largest_worker_mb=$(ps aux --sort=-%mem | grep '[g]unicorn.*cgd' | head -1 | awk '{print int($6/1024)}')

timestamp=$(date '+%Y-%m-%d %H:%M:%S')

# Restart if system memory > threshold OR single worker > 4GB
if [ "$mem_used_percent" -gt "$THRESHOLD_PERCENT" ] || [ "$largest_worker_mb" -gt "$WORKER_THRESHOLD_MB" ]; then
    echo "[$timestamp] ALERT: Memory ${mem_used_percent}%, largest worker ${largest_worker_mb}MB - restarting $SERVICE_NAME" >> "$LOG_FILE"
    systemctl restart "$SERVICE_NAME"
    echo "[$timestamp] Service restarted" >> "$LOG_FILE"
else
    echo "[$timestamp] OK: Memory ${mem_used_percent}%, largest worker ${largest_worker_mb}MB" >> "$LOG_FILE"
fi
