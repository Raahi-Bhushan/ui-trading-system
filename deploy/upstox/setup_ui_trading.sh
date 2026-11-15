#!/usr/bin/env bash
# Install / update the Upstox edition of ui-trading-system on Ubuntu, side by side with
# anything already on the box (e.g. JK Trade Desk on :8000). It never edits other apps.
#
#   sudo bash deploy/upstox/setup_ui_trading.sh            # from inside the unzipped folder
#   sudo HOSTNAME_PUBLIC=trade.example.com bash ...         # optional own domain
#
# Result: https://ui.<elastic-ip-with-dashes>.sslip.io  -> 127.0.0.1:5010 (dry-run by default)
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "Run with sudo"; exit 1; }
SRC="$(cd "$(dirname "$0")/../.." && pwd)"
BASE=/opt/ui-trading
APP=$BASE/app
SVC=ui-trading
PORT=5010

echo "==> Pre-flight"
if ss -ltnH "( sport = :$PORT )" | grep -q . && ! systemctl is-active -q $SVC; then
  echo "!! Port $PORT is already in use by another program - aborting without changes."; exit 1
fi
TOKEN=$(curl -s -m 3 -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 60" || true)
IP=$(curl -s -m 3 -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/public-ipv4 || curl -s https://checkip.amazonaws.com)
HOST="${HOSTNAME_PUBLIC:-ui.$(echo "$IP" | tr . -).sslip.io}"
echo "    public host: $HOST (IP $IP)"

echo "==> Packages"
apt-get install -y -qq python3-venv python3-dev build-essential rsync >/dev/null
command -v caddy >/dev/null || { echo "!! Caddy not installed (expected from the existing setup)"; exit 1; }

echo "==> Code -> $APP"
id uitrading >/dev/null 2>&1 || useradd --system --home "$BASE" --shell /usr/sbin/nologin uitrading
mkdir -p "$APP" "$BASE/.upstox_cache"
rsync -a --exclude .git --exclude .venv --exclude configfile.ini --exclude '*.db' --exclude '*.db-*' \
      --exclude .flask_secret --exclude logs --exclude __pycache__ "$SRC/" "$APP/"
[ -d "$APP/.venv" ] || python3 -m venv "$APP/.venv"
"$APP/.venv/bin/pip" install -q --upgrade pip
"$APP/.venv/bin/pip" install -q -r "$APP/requirements.txt"

echo "==> Config (kept if it already exists)"
CFG="$APP/configfile.ini"
NEWPASS=""
if [ ! -f "$CFG" ]; then
  NEWPASS=$(head -c 18 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 16)
  sed -e "s/YOUR_API_KEY/REPLACE_WITH_UPSTOX_API_KEY/; s/YOUR_API_SECRET/REPLACE_WITH_UPSTOX_API_SECRET/" \
      -e "s/YOUR_USERNAME/jk/; s/YOUR_PASSWORD/$NEWPASS/" \
      -e "s#server_base_url = http://127.0.0.1:5010#server_base_url = https://$HOST#" \
      "$APP/configfile.ini.example" > "$CFG"
fi
grep -q "^live_trading" "$CFG" || printf "\n[safety]\nlive_trading = false\n" >> "$CFG"
ENVF=/etc/ui-trading.env
[ -f "$ENVF" ] || cat > "$ENVF" <<ENV
# Upstox edition settings (restart ui-trading after editing)
UPSTOX_REDIRECT_URI=https://$HOST/login
UPSTOX_KITE_CACHE_DIR=$BASE/.upstox_cache
# If Upstox rejects orders asking for an algo name, set the exact Algo Name from My Apps:
UPSTOX_ALGO_NAME=
ENV
chown -R uitrading:uitrading "$BASE"
chmod 600 "$CFG" "$ENVF"; chown root:uitrading "$ENVF"; chmod 640 "$ENVF"

echo "==> Service"
cat > /etc/systemd/system/$SVC.service <<UNIT
[Unit]
Description=UI Trading System (Upstox edition)
After=network-online.target
Wants=network-online.target

[Service]
User=uitrading
WorkingDirectory=$APP
EnvironmentFile=$ENVF
Environment=PYTHONUNBUFFERED=1
ExecStart=$APP/.venv/bin/python flask_app.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable -q $SVC
systemctl restart $SVC

echo "==> HTTPS site (append-only; existing sites untouched)"
CF=/etc/caddy/Caddyfile
if ! grep -q "^$HOST {" "$CF"; then
  cp "$CF" "$CF.bak-$(date +%Y%m%d-%H%M%S)"
  printf "\n%s {\n    encode gzip\n    reverse_proxy 127.0.0.1:%s\n}\n" "$HOST" "$PORT" >> "$CF"
  if caddy validate --config "$CF" --adapter caddyfile >/dev/null 2>&1; then
    systemctl reload caddy
  else
    echo "!! Caddy config invalid - restoring backup"; cp "$(ls -t $CF.bak-* | head -1)" "$CF"; exit 1
  fi
fi

sleep 8
echo
echo "================================================================"
echo " Dashboard : https://$HOST   (DRY-RUN: no orders reach Upstox)"
echo " Login     : jk / ${NEWPASS:-<unchanged, see $CFG [gatekeeper]>}"
echo " Upstox app: create a NEW app in Upstox Developer Apps with"
echo "             Redirect URL  https://$HOST/login"
echo "             Static IP     $IP   (needed for order APIs)"
echo "             then put its key/secret in $CFG [kite_login_details]"
echo "             and: sudo systemctl restart $SVC"
echo " Logs      : sudo journalctl -u $SVC -f"
echo "================================================================"
systemctl is-active $SVC
