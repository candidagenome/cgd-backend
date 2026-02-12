#!/usr/bin/env bash
set -euo pipefail

# --- Config (override via env vars) ---
SERVICE="${BACKEND_SERVICE:-cgd-api}"
REPO_DIR="${BACKEND_DIR:-/home/ec2-user/work/cgd-backend}"   # <-- update to your real backend repo path
HEALTH_URL="${BACKEND_HEALTH_URL:-http://127.0.0.1:8000/api/locus/ACT1}"

# Optional venv + deps install
VENV_ACTIVATE="${BACKEND_VENV_ACTIVATE:-}"                  # e.g. /home/ec2-user/work/cgd-backend/venv/bin/activate
REQ_FILE="${BACKEND_REQ_FILE:-requirements.txt}"
PIP_INSTALL_ON="${BACKEND_PIP_INSTALL_ON:-req_change}"      # always | req_change | never

# Restart / checks
WAIT_HOST="${BACKEND_WAIT_HOST:-127.0.0.1}"   # retained for logging only (port check is host-agnostic)
WAIT_PORT="${BACKEND_WAIT_PORT:-8000}"
WAIT_SECS="${BACKEND_WAIT_SECS:-30}"
HEALTH_RETRIES="${BACKEND_HEALTH_RETRIES:-30}"
HEALTH_SLEEP_SECS="${BACKEND_HEALTH_SLEEP_SECS:-1}"

log()  { printf "\n==> %s\n" "$*"; }
warn() { printf "WARNING: %s\n" "$*" >&2; }

die() {
  printf "ERROR: %s\n" "$*" >&2
  exit 1
}

wait_for_port() {
  local host="$1" port="$2" timeout="$3"
  log "Waiting for *:$port (up to ${timeout}s) [host hint: ${host}]"

  # NOTE: We intentionally do NOT require an exact host match.
  # Gunicorn may bind 0.0.0.0:8000 (common) or 127.0.0.1:8000.
  # Checking only the port avoids false negatives.
  for ((i=1; i<=timeout; i++)); do
    if ss -lntH "sport = :$port" 2>/dev/null | grep -q .; then
      log "Port is listening on at least one address: *:$port"
      return 0
    fi
    sleep 1
  done
  return 1
}

health_check() {
  local url="$1" retries="$2" sleep_secs="$3"
  log "Health check (retrying up to ${retries}x): $url"
  for ((i=1; i<=retries; i++)); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      log "Health check OK"
      return 0
    fi
    sleep "$sleep_secs"
  done
  return 1
}

log "Deploying backend"
log "Repo:    $REPO_DIR"
log "Service: $SERVICE"
log "Check:   $HEALTH_URL"

cd "$REPO_DIR"

log "Git status (before)"
git status -sb || true

log "Pull latest code"
PREV_HEAD="$(git rev-parse HEAD 2>/dev/null || true)"
git pull
NEW_HEAD="$(git rev-parse HEAD 2>/dev/null || true)"

# --- Optional: update Python deps ---
maybe_install_deps() {
  local need_install="no"

  if [ -z "$VENV_ACTIVATE" ]; then
    log "Skipping dependency install (BACKEND_VENV_ACTIVATE not set)"
    return 0
  fi

  if [ ! -f "$REQ_FILE" ]; then
    warn "Skipping dependency install ($REQ_FILE not found)"
    return 0
  fi

  case "$PIP_INSTALL_ON" in
    never)
      log "Dependency install disabled (BACKEND_PIP_INSTALL_ON=never)"
      return 0
      ;;
    always)
      need_install="yes"
      ;;
    req_change|*)
      # Install if requirements.txt changed between previous and new HEAD (when a pull actually moved HEAD)
      if [ -n "$PREV_HEAD" ] && [ -n "$NEW_HEAD" ] && [ "$PREV_HEAD" != "$NEW_HEAD" ]; then
        if git diff --name-only "$PREV_HEAD" "$NEW_HEAD" | grep -qx "$REQ_FILE"; then
          need_install="yes"
        fi
      else
        # If HEAD didn't change, we usually don't need to install
        need_install="no"
      fi
      ;;
  esac

  if [ "$need_install" = "yes" ]; then
    log "Activating venv: $VENV_ACTIVATE"
    # shellcheck disable=SC1090
    source "$VENV_ACTIVATE"
    log "Installing Python deps from $REQ_FILE"
    python -m pip install -U pip
    python -m pip install -r "$REQ_FILE"
  else
    log "No dependency install needed (mode=$PIP_INSTALL_ON, $REQ_FILE unchanged)"
  fi
}

maybe_install_deps

# --- Restart service ---
log "Restarting backend service: $SERVICE"
sudo systemctl restart "$SERVICE"

# Give systemd a moment to spawn workers before we check ports/logs
sleep 1

log "Service status"
sudo systemctl status "$SERVICE" -l --no-pager || true

log "Recent logs (last 2 minutes)"
sudo journalctl -u "$SERVICE" --since "2 minutes ago" --no-pager || true

# --- Port + health checks ---
log "Ports (8000/80/443)"
ss -lntp | egrep ':(8000|80|443)\b' || true

if ! wait_for_port "$WAIT_HOST" "$WAIT_PORT" "$WAIT_SECS"; then
  warn "Backend did not start listening on port $WAIT_PORT within ${WAIT_SECS}s"
  log "Service status (full)"
  sudo systemctl status "$SERVICE" -l --no-pager || true
  log "Logs since 10 minutes ago"
  sudo journalctl -u "$SERVICE" --since "10 minutes ago" --no-pager || true
  die "Backend port not listening"
fi

if ! health_check "$HEALTH_URL" "$HEALTH_RETRIES" "$HEALTH_SLEEP_SECS"; then
  warn "Health check failed after retries"
  log "Service status (full)"
  sudo systemctl status "$SERVICE" -l --no-pager || true
  log "Logs since 10 minutes ago"
  sudo journalctl -u "$SERVICE" --since "10 minutes ago" --no-pager || true
  # Still show a final attempt output
  curl -sS -i "$HEALTH_URL" | head -n 60 || true
  die "Backend health check failed"
fi

log "Backend health check (response header preview)"
curl -sS -i "$HEALTH_URL" | head -n 40 || true

log "Done ✅"
