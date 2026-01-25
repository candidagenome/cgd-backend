#!/usr/bin/env bash
set -euo pipefail

# --- Config (override via env vars) ---
SERVICE="${BACKEND_SERVICE:-cgd-api}"
REPO_DIR="${BACKEND_DIR:-/home/ec2-user/work/cgd-backend}"   # <-- update to your real backend repo path
HEALTH_URL="${BACKEND_HEALTH_URL:-http://127.0.0.1:8000/api/locus/ACT1}"

# If your backend runs from a venv and you want to auto-update deps on changes:
VENV_ACTIVATE="${BACKEND_VENV_ACTIVATE:-}"  # e.g. /home/ec2-user/venv/bin/activate (leave empty to skip venv)
REQ_FILE="${BACKEND_REQ_FILE:-requirements.txt}"

log() { printf "\n==> %s\n" "$*"; }
warn() { printf "WARNING: %s\n" "$*" >&2; }

log "Deploying backend"
log "Repo:    $REPO_DIR"
log "Service: $SERVICE"
log "Check:   $HEALTH_URL"

cd "$REPO_DIR"

log "Git status (before)"
git status -sb || true

log "Pull latest code"
git pull

# --- Optional: update Python deps if requirements changed ---
need_install="no"
if [ -n "$VENV_ACTIVATE" ] && [ -f "$REQ_FILE" ]; then
  if git rev-parse --verify -q HEAD@{1} >/dev/null 2>&1; then
    if git diff --name-only HEAD@{1} HEAD | grep -qE "(^|/)$REQ_FILE$"; then
      need_install="yes"
    fi
  else
    warn "No previous HEAD found; will install deps to be safe."
    need_install="yes"
  fi

  if [ "$need_install" = "yes" ]; then
    log "Activating venv: $VENV_ACTIVATE"
    # shellcheck disable=SC1090
    source "$VENV_ACTIVATE"
    log "Installing Python deps from $REQ_FILE"
    python -m pip install -U pip
    python -m pip install -r "$REQ_FILE"
  else
    log "No $REQ_FILE changes detected → skipping pip install"
  fi
else
  log "Skipping dependency install (set BACKEND_VENV_ACTIVATE and ensure $REQ_FILE exists to enable)"
fi

# --- Restart service ---
log "Restarting backend service: $SERVICE"
sudo systemctl restart "$SERVICE"

log "Service status"
systemctl status "$SERVICE" --no-pager || true

log "Recent logs"
sudo journalctl -u "$SERVICE" -n 120 --no-pager || true

# --- Port + health checks ---
log "Ports (8000/80/443)"
ss -lntp | egrep ':(8000|80|443)\b' || true

log "Backend health check"
curl -sS -i "$HEALTH_URL" | head -n 40 || true

log "Done ✅"
