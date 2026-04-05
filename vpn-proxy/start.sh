#!/bin/bash
set -euo pipefail

# --- Start rsyslog Service (idempotent) ---
# Some restart paths can leave a stale pidfile or an already-running daemon.
# Don't fail container startup in that case.
if pgrep -x rsyslogd >/dev/null 2>&1; then
  echo "rsyslogd already running; skipping start."
else
  rm -f /run/rsyslogd.pid
  if ! rsyslogd; then
    echo "Warning: failed to start rsyslogd; continuing."
  fi
fi

# --- Wait for certificate file to be mounted ---
CERT_FILE="/etc/ipsec.d/cacerts/comodo.crt"
CERT_WAIT_TIMEOUT="${CERT_WAIT_TIMEOUT:-60}"
echo "Waiting for certificate file to appear at ${CERT_FILE}..."
start_ts=$(date +%s)
while [ ! -f "${CERT_FILE}" ]; do
  now_ts=$(date +%s)
  if [ $((now_ts - start_ts)) -ge "${CERT_WAIT_TIMEOUT}" ]; then
    echo "Certificate not found after ${CERT_WAIT_TIMEOUT}s. Exiting."
    exit 1
  fi
  sleep 1
done
echo "Certificate found. Proceeding with startup."

# --- Configure strongSwan VPN ---
cat > /etc/ipsec.conf <<EOF
config setup
    # Logs will be automatically sent to rsyslog

conn adguard
    keyexchange=ikev2
    right=${VPN_SERVER}
    rightid=${VPN_REMOTE_ID}
    rightauth=pubkey
    rightsubnet=0.0.0.0/0
    # The rightca line is intentionally removed to avoid the cosmetic log error
    leftsourceip=%config
    leftid=${VPN_USERNAME}
    leftauth=eap
    eap_identity=%identity
    ike=aes256-sha256-modp2048!
    esp=aes256-sha256!
    auto=start
    dpdaction=restart
    dpddelay=30s
    dpdtimeout=120s
EOF

cat > /etc/ipsec.secrets <<EOF
${VPN_USERNAME} : EAP "${VPN_PASSWORD}"
EOF

# --- Start strongSwan VPN (boot-race tolerant retries) ---
VPN_WAIT_TIMEOUT="${VPN_WAIT_TIMEOUT:-60}"
VPN_BOOT_MAX_ATTEMPTS="${VPN_BOOT_MAX_ATTEMPTS:-5}"
VPN_RETRY_DELAY="${VPN_RETRY_DELAY:-10}"
vpn_attempt=1
vpn_established=0

while [ "${vpn_attempt}" -le "${VPN_BOOT_MAX_ATTEMPTS}" ]; do
  echo "Starting VPN attempt ${vpn_attempt}/${VPN_BOOT_MAX_ATTEMPTS}..."

  # Ensure stale daemons/pidfiles don't poison retries.
  ipsec stop || true
  rm -f /var/run/starter.charon.pid /var/run/charon.pid

  ipsec start
  echo "Waiting up to ${VPN_WAIT_TIMEOUT}s for VPN to establish..."
  vpn_start_ts=$(date +%s)

  while true; do
    if ipsec status 2>/dev/null | grep -q "ESTABLISHED"; then
      vpn_established=1
      break
    fi

    now_ts=$(date +%s)
    if [ $((now_ts - vpn_start_ts)) -ge "${VPN_WAIT_TIMEOUT}" ]; then
      echo "Attempt ${vpn_attempt} failed: VPN did not establish in ${VPN_WAIT_TIMEOUT}s."
      break
    fi
    sleep 2
  done

  if [ "${vpn_established}" -eq 1 ]; then
    break
  fi

  vpn_attempt=$((vpn_attempt + 1))
  if [ "${vpn_attempt}" -le "${VPN_BOOT_MAX_ATTEMPTS}" ]; then
    echo "Retrying VPN startup in ${VPN_RETRY_DELAY}s..."
    sleep "${VPN_RETRY_DELAY}"
  fi
done

if [ "${vpn_established}" -ne 1 ]; then
  echo "VPN failed after ${VPN_BOOT_MAX_ATTEMPTS} attempts. Exiting to trigger restart."
  ipsec stop || true
  exit 1
fi

echo "VPN connection established."

# strongSwan pushes an external resolver into /etc/resolv.conf, but in this
# container that path is not reliable for Squid lookups. Docker's embedded DNS
# stays reachable from the namespace and resolves upstream addon hosts
# correctly, so restore it after the tunnel is up.
cat > /etc/resolv.conf <<'EOF'
nameserver 127.0.0.11
options ndots:0
EOF
echo "Restored Docker embedded DNS for proxy container lookups."

# --- Kill Switch Firewall Configuration (Robust Method) ---
PROXY_NET="172.22.0.0/16"
if command -v iptables-legacy >/dev/null 2>&1; then
  IPT="iptables-legacy"
else
  IPT="iptables"
fi

$IPT -F
$IPT -t nat -F
$IPT -P INPUT DROP
$IPT -P FORWARD DROP
$IPT -P OUTPUT DROP
$IPT -A INPUT -i lo -j ACCEPT
$IPT -A INPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
$IPT -A INPUT -p tcp --dport 8888 -s $PROXY_NET -j ACCEPT
$IPT -A OUTPUT -o lo -j ACCEPT
$IPT -A OUTPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
$IPT -A OUTPUT -p udp --dport 53 -j ACCEPT
$IPT -A OUTPUT -p udp -d "${VPN_SERVER}" --dport 500 -j ACCEPT
$IPT -A OUTPUT -p udp -d "${VPN_SERVER}" --dport 4500 -j ACCEPT
$IPT -A OUTPUT -p esp -d "${VPN_SERVER}" -j ACCEPT
$IPT -A OUTPUT -m policy --dir out --pol ipsec -j ACCEPT
$IPT -A FORWARD -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
$IPT -A FORWARD -s $PROXY_NET -m policy --dir out --pol ipsec -j ACCEPT
REAL_IF=$(ip route get ${VPN_SERVER} | awk '{print $5}')
if [ -z "${REAL_IF}" ]; then
  echo "Unable to determine egress interface for ${VPN_SERVER}. Exiting."
  exit 1
fi
$IPT -t nat -A POSTROUTING -s $PROXY_NET -o $REAL_IF -j MASQUERADE

echo "Robust kill switch enabled using IPsec policy matching."
echo "Starting Squid..."

# --- Start Squid Proxy ---
# Ensure no stale squid process/pid from a prior failed attempt.
pkill -x squid >/dev/null 2>&1 || true
sleep 1
rm -f /run/squid.pid

# Skip cache-dir initialization here to avoid startup races with foreground squid.
# This proxy runs with memory cache and no persistent cache_dir requirement.

# --- Background VPN Monitor Loop ---
# This subshell will check the VPN status every minute.
# If the connection is down, it will kill the main script (PID 1),
# which causes the container to exit and trigger a restart.
(
  # Wait 2 minutes before starting checks to allow for initial setup
  # and prevent restarts during brief network flaps.
  sleep 120
  while true; do
    if ! ipsec status | grep -q "ESTABLISHED"; then
      echo "VPN connection lost! Triggering container restart..."
      kill 1 # Send SIGTERM to PID 1 (this script) to initiate a graceful shutdown
      exit   # Exit the subshell
    fi
    sleep 60
  done
) &

# Run Squid in the foreground as PID 1 so container lifecycle matches proxy process health.
exec /usr/sbin/squid -N -d1
