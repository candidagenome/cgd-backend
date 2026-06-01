#!/bin/bash
set -e

FRONTEND_DIR=/opt/cgd_frontend
BACKEND_DIR=/opt/cgd_api
BACKEND_VENV=/opt/cgd_api/.venv/bin/activate

echo "=== Deploy frontend ==="
cd $FRONTEND_DIR
git pull
npm ci
npm run build

echo "=== Deploy backend ==="
cd $BACKEND_DIR
git pull
source $BACKEND_VENV
pip install -r requirements.txt
sudo service cgd-api restart

echo "=== Deploy finished ==="
