# EC2 setup (direct, no Docker)

These steps assume Amazon Linux 2/2023-ish and Nginx.

## 1) Install system packages
```bash
sudo yum update -y
sudo yum install -y python3 python3-pip nginx
```

## 2) Copy project to /opt
```bash
sudo mkdir -p /opt/cgd_api
sudo chown -R ec2-user:ec2-user /opt/cgd_api
# copy code into /opt/cgd_api (scp, rsync, git checkout, etc.)
```

## 3) Create venv + install deps
```bash
cd /opt/cgd_api
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 4) Create .env
```bash
cp .env.example .env
# edit DATABASE_URL (and DB_SCHEMA if needed)
```

## 5) Quick smoke test (optional)
```bash
source .venv/bin/activate
uvicorn cgd.main:app --host 0.0.0.0 --port 8000
# then curl: http://127.0.0.1:8000/health
```

## 6) Set up systemd service
```bash
sudo cp deploy/systemd/cgd-api.service /etc/systemd/system/cgd-api.service
# edit User/Group/WorkingDirectory paths in the service file if needed
sudo systemctl daemon-reload
sudo systemctl enable --now cgd-api
sudo systemctl status cgd-api
```

Logs:
```bash
journalctl -u cgd-api -f
```

## 7) Configure Nginx reverse proxy
```bash
sudo cp deploy/nginx/cgd-api.conf /etc/nginx/conf.d/cgd-api.conf
sudo nginx -t
sudo systemctl enable --now nginx
sudo systemctl reload nginx
```

Now `/api/*` should proxy to the backend.

## 8) Security group
Allow inbound:
- 80 (HTTP) or 443 (HTTPS) to Nginx
Do NOT expose 8000 publicly if you can avoid it.
