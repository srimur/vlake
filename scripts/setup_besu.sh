#!/bin/bash
set -euo pipefail

# ════════════════════════════════════════════════════════════
# V-LAKE BESU SETUP — QBFT Consensus
# ════════════════════════════════════════════════════════════
# Works on: Linux, macOS, Windows (Git Bash / MINGW / WSL)
# Prerequisites: Docker + Python 3
# Usage: bash scripts/setup_besu.sh
# ════════════════════════════════════════════════════════════

BESU_IMAGE="hyperledger/besu:24.1.0"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# IMPORTANT: cd into project root so ALL file operations use relative paths.
# This avoids MINGW /c/Users path mangling in Python heredocs.
cd "$DIR"

TMP=".besu-tmp"
KEYS="config/besu/keys"
GENESIS="config/besu/genesis.json"

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

echo ""
echo "════════════════════════════════════════════════════"
echo "  V-LAKE BESU NETWORK SETUP (QBFT)"
echo "════════════════════════════════════════════════════"
echo "  Image:      $BESU_IMAGE"
echo "  Validators: 4 (fault tolerance f=1)"
echo "  Project:    $DIR"
echo ""

# ── Step 1: Prepare ──
echo "[1/5] Preparing configuration..."
rm -rf "$TMP"
mkdir -p "$TMP" "$KEYS" "$(dirname "$GENESIS")"

cat > "$TMP/qbft-config.json" << 'JSON'
{
  "genesis": {
    "config": {
      "chainId": 1337,
      "berlinBlock": 0,
      "londonBlock": 0,
      "qbft": {
        "blockperiodseconds": 2,
        "epochlength": 30000,
        "requesttimeoutseconds": 4
      }
    },
    "nonce": "0x0",
    "timestamp": "0x58ee40ba",
    "gasLimit": "0x1fffffffffffff",
    "difficulty": "0x1",
    "mixHash": "0x63746963616c2062797a616e74696e65206661756c7420746f6c6572616e6365",
    "alloc": {}
  },
  "blockchain": { "nodes": { "generate": true, "count": 4 } }
}
JSON
echo "  ✓ QBFT config written"

# ── Step 2: Docker path conversion for Windows ──
convert_docker_path() {
    case "$(uname -s)" in
        MINGW*|MSYS*) echo "$1" | sed -E 's|^/([a-zA-Z])/|\U\1:/|' ;;
        *) echo "$1" ;;
    esac
}

ABS_TMP="$DIR/$TMP"
DOCKER_TMP=$(convert_docker_path "$ABS_TMP")

echo ""
echo "[2/5] Generating validator keys via Besu..."

docker run --rm \
    -v "${DOCKER_TMP}:/opt/besu/output" \
    --entrypoint besu \
    "$BESU_IMAGE" \
    operator generate-blockchain-config \
    --config-file=/opt/besu/output/qbft-config.json \
    --to=/opt/besu/output/networkFiles \
    --private-key-file-name=key 2>&1 | grep -v "^$" | head -20 || true

# ── Step 3: Install generated files ──
echo ""
echo "[3/5] Installing generated files..."

GENERATED_GENESIS=$(find "$TMP" -name "genesis.json" -path "*/networkFiles/*" 2>/dev/null | head -1)
if [ -z "$GENERATED_GENESIS" ]; then
    GENERATED_GENESIS=$(find "$TMP" -name "genesis.json" ! -name "qbft-config.json" 2>/dev/null | head -1)
fi

PUBKEY1=""

if [ -n "$GENERATED_GENESIS" ]; then
    cp "$GENERATED_GENESIS" "$GENESIS"
    echo "  ✓ Genesis copied"

    N=0
    for KEY_FILE in $(find "$TMP" -name "key" -path "*/keys/*" -type f 2>/dev/null | sort); do
        N=$((N+1))
        KEY_DIR=$(dirname "$KEY_FILE")
        ADDR=$(basename "$KEY_DIR")
        cp "$KEY_FILE" "$KEYS/node${N}.key"
        echo "  Node $N: $ADDR"
        if [ $N -eq 1 ] && [ -f "$KEY_DIR/key.pub" ]; then
            PUBKEY1=$(cat "$KEY_DIR/key.pub" | tr -d '[:space:]' | sed 's/^0x//')
        fi
        [ $N -ge 4 ] && break
    done
    echo "  ✓ $N validator keys installed"
else
    echo "  ⚠ Besu output not found — generating via Python fallback"
    python3 << 'PYEOF'
import json, secrets, hashlib, os

keys_dir = "config/besu/keys"
genesis_path = "config/besu/genesis.json"

validators = []
for i in range(1, 5):
    pk = secrets.token_hex(32)
    with open(os.path.join(keys_dir, f"node{i}.key"), "w") as f:
        f.write(f"0x{pk}")
    addr = hashlib.sha256(bytes.fromhex(pk)).hexdigest()[:40]
    validators.append(addr)
    print(f"  Node {i}: 0x{addr}")

genesis = {
    "config": {"chainId": 1337, "berlinBlock": 0, "londonBlock": 0,
               "qbft": {"blockperiodseconds": 2, "epochlength": 30000, "requesttimeoutseconds": 4}},
    "nonce": "0x0", "timestamp": "0x58ee40ba",
    "gasLimit": "0x1fffffffffffff", "difficulty": "0x1",
    "mixHash": "0x63746963616c2062797a616e74696e65206661756c7420746f6c6572616e6365",
    "alloc": {}
}
bal = "0x200000000000000000000000000000000000000000000000000000000000000"
for a in validators:
    genesis["alloc"][a] = {"balance": bal}
with open(genesis_path, "w") as f:
    json.dump(genesis, f, indent=2)
print("  ✓ Fallback genesis + keys generated")
PYEOF
fi

# ── Step 4: Fund demo accounts ──
echo ""
echo "[4/5] Funding demo accounts..."

# Using relative path — no MINGW mangling
python3 << 'PYEOF'
import json

with open("config/besu/genesis.json") as f:
    g = json.load(f)

bal = "0x200000000000000000000000000000000000000000000000000000000000000"
accounts = {
    "1111111111111111111111111111111111111111": "Steward-1 (Hospital Admin)",
    "2222222222222222222222222222222222222222": "Steward-2 (Govt Health Dept)",
    "3333333333333333333333333333333333333333": "Steward-3 (Insurance Board)",
    "4444444444444444444444444444444444444444": "Custodian (Lab Technician)",
    "5555555555555555555555555555555555555555": "Analyst (Researcher)",
    "6666666666666666666666666666666666666666": "Patient (John Doe)",
}
for addr, label in accounts.items():
    g.setdefault("alloc", {})[addr.lower()] = {"balance": bal}
    print(f"  ✓ 0x{addr[:8]}... — {label}")

with open("config/besu/genesis.json", "w") as f:
    json.dump(g, f, indent=2)
print("  ✓ All accounts funded")
PYEOF

# ── Step 5: Patch docker-compose enode ──
echo ""
echo "[5/5] Patching docker-compose.yml..."

if [ -z "${PUBKEY1:-}" ] && [ -f "$KEYS/node1.key" ]; then
    ABS_KEY=$(convert_docker_path "$DIR/$KEYS/node1.key")
    RAW=$(docker run --rm \
        -v "${ABS_KEY}:/tmp/key" \
        --entrypoint besu \
        "$BESU_IMAGE" \
        public-key export --node-private-key-file=/tmp/key 2>&1) || true
    PUBKEY1=$(echo "$RAW" | grep -oE '0x[0-9a-fA-F]{128}' | tail -1 | sed 's/^0x//' || true)
fi

if [ -n "${PUBKEY1:-}" ] && [ -f "docker-compose.yml" ]; then
    python3 << PYEOF
import re
with open("docker-compose.yml") as f:
    t = f.read()
enode = "enode://${PUBKEY1}@172.20.0.10:30303"
t = re.sub(r'--bootnodes=enode://[a-fA-F0-9PLACEHOLDER]+@172\.20\.0\.10:30303', f'--bootnodes={enode}', t)
with open("docker-compose.yml", "w") as f:
    f.write(t)
print(f"  ✓ Enode patched")
PYEOF
else
    echo "  ⚠ Could not derive enode — PLACEHOLDER remains (nodes will still discover each other)"
fi

rm -rf "$TMP"

echo ""
echo "════════════════════════════════════════════════════"
echo "  SETUP COMPLETE"
echo "════════════════════════════════════════════════════"
echo ""
echo "  Next steps:"
echo "    docker-compose down -v"
echo "    docker-compose build --no-cache"
echo "    docker-compose up -d"
echo "    sleep 15"
echo "    python scripts/deploy_contract.py"
echo ""
