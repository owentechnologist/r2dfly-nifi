#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_ROOT/scripts/version.sh"
echo "==> r2dfly version $R2DFLY_VERSION"

if [[ $# -lt 1 || -z "$1" ]]; then
  echo "Usage: ./check-cloud-resource-location.sh <url-or-domain>" >&2
  echo "Example: ./check-cloud-resource-location.sh xyz.dragonflydb.cloud" >&2
  exit 1
fi

# Clean up input by stripping protocol prefixes if present (e.g., http://, https://, redis://)
TARGET_RESOURCE=$(echo "$1" | sed -E 's|^[a-zA-Z0-9]+://||' | cut -d/ -f1 | cut -d: -f1)

echo "==> Resolving address for: $TARGET_RESOURCE"

TARGET_IP=$(dig +short "$TARGET_RESOURCE" | tail -n1)

if [[ -z "$TARGET_IP" ]]; then
  echo "Error: Could not resolve domain '$TARGET_RESOURCE' to an IP address." >&2
  exit 1
fi

echo "Resolved Target IP: $TARGET_IP"

# Cross-reference with AWS and GCP ranges using native Python
python3 -c "
import urllib.request, json, ipaddress, sys

target_ip_str = '$TARGET_IP'
try:
    ip = ipaddress.ip_address(target_ip_str)
except ValueError:
    print(f'Error: Python could not parse {target_ip_str} as a valid IP address.', file=sys.stderr)
    sys.exit(1)

# Check AWS Public IP Space
try:
    req = urllib.request.Request('https://ip-ranges.amazonaws.com/ip-ranges.json', headers={'User-Agent': 'Mozilla/5.0'})
    aws_data = json.loads(urllib.request.urlopen(req, timeout=5).read())
    for prefix in aws_data.get('prefixes', []):
        if 'ip_prefix' in prefix and ip in ipaddress.ip_network(prefix['ip_prefix']):
            print(f'➜ MATCH: AWS Region = {prefix[\"region\"]} ({prefix[\"service\"]})')
            sys.exit(0)
except Exception as e:
    pass

# Check GCP Public IP Space
try:
    req = urllib.request.Request('https://www.gstatic.com/ipranges/cloud.json', headers={'User-Agent': 'Mozilla/5.0'})
    gcp_data = json.loads(urllib.request.urlopen(req, timeout=5).read())
    for prefix in gcp_data.get('prefixes', []):
        if 'ipv4Prefix' in prefix and ip in ipaddress.ip_network(prefix['ipv4Prefix']):
            print(f'➜ MATCH: GCP Region = {prefix.get(\"scope\", \"Unknown\")} (Zone/Region)')
            sys.exit(0)
except Exception as e:
    pass

print('➜ No exact match found in AWS or GCP public ranges.')
"

