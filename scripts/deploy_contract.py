#!/usr/bin/env python3
"""
V-Lake Smart Contract Deployment & Verification
═════════════════════════════════════════════════
Compiles VLakeGovernance.sol and deploys to Hyperledger Besu (QBFT).
Verifies all four contributions post-deployment.

Usage:
  pip install py-solc-x web3 eth-account
  python deploy_contract.py [--rpc URL] [--verify] [--smoke-test]

Requires:
  - Besu node running (default: http://localhost:8545)
  - solc 0.8.19 (auto-installed by py-solc-x)
"""

import argparse
import json
import os
import sys
import time

try:
    from solcx import compile_standard, install_solc, get_installed_solc_versions
    from web3 import Web3
    from eth_account import Account
except ImportError:
    print("Installing dependencies: py-solc-x web3 eth-account...")
    os.system("pip install py-solc-x web3 eth-account")
    from solcx import compile_standard, install_solc, get_installed_solc_versions
    from web3 import Web3
    from eth_account import Account

# ════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════
SOLC_VERSION = "0.8.19"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.join(SCRIPT_DIR, "..")
CONTRACT_DIR = os.path.join(PROJECT_DIR, "contracts")
BACKEND_DIR = os.path.join(PROJECT_DIR, "backend")
CONFIG_DIR = os.path.join(PROJECT_DIR, "config")

# ─────────────────────────────────────────────────────────────
# PUBLIC TEST KEYS — NOT SECRETS
# These are the well-known Hyperledger Besu / Truffle dev-mode
# private keys, pre-funded in the Besu dev genesis. They are
# documented publicly and exist in thousands of repos. They only
# control identities on a local chain id 1337.
#
# For production: load steward keys from a KMS / Vault / HSM and
# remove this block entirely.
# ─────────────────────────────────────────────────────────────
_STEWARD_KEYS = [
    ("0xae6ae8e5ccbfb04590405997ee2d52d2b330726137b875053c36d94e974d162f", "Steward-1 (Hospital Admin)"),
    ("0xc87509a1c067bbde78beb793e6fa76530b6382a4c0241e5e4a9ec0a0f44dc0d3", "Steward-2 (Govt Health Dept)"),
    ("0x8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63", "Steward-3 (Insurance Board)"),
]
STEWARD_ACCOUNTS = [
    {"address": Account.from_key(pk).address, "private_key": pk, "label": label}
    for pk, label in _STEWARD_KEYS
]

# Additional demo accounts
DEMO_ACCOUNTS = {
    "custodian":  "0x4444444444444444444444444444444444444444",
    "analyst":    "0x5555555555555555555555555555555555555555",
    "patient":    "0x6666666666666666666666666666666666666666",
}


def banner(text):
    w = max(len(text) + 4, 60)
    print(f"\n{'═' * w}")
    print(f"  {text}")
    print(f"{'═' * w}")


def step(n, text):
    print(f"\n[{n}] {text}")
    print(f"{'─' * 50}")


# ════════════════════════════════════════════════
# COMPILE
# ════════════════════════════════════════════════
def compile_contract():
    """Compile VLakeGovernance.sol with solc optimizer."""
    step("1/4", "Compiling Smart Contract")

    if SOLC_VERSION not in [str(v) for v in get_installed_solc_versions()]:
        print(f"  Installing solc {SOLC_VERSION}...")
        install_solc(SOLC_VERSION)
    else:
        print(f"  solc {SOLC_VERSION} already installed")

    contract_path = os.path.join(CONTRACT_DIR, "VLakeGovernance.sol")
    if not os.path.exists(contract_path):
        print(f"  ERROR: Contract not found at {contract_path}")
        sys.exit(1)

    with open(contract_path, "r") as f:
        source = f.read()

    print("  Compiling with optimizer (200 runs)...")
    compiled = compile_standard(
        {
            "language": "Solidity",
            "sources": {"VLakeGovernance.sol": {"content": source}},
            "settings": {
                "outputSelection": {
                    "*": {"*": ["abi", "evm.bytecode.object", "evm.deployedBytecode.object", "evm.gasEstimates"]}
                },
                "optimizer": {"enabled": True, "runs": 200},
                "viaIR": True
            }
        },
        solc_version=SOLC_VERSION
    )

    contract_data = compiled["contracts"]["VLakeGovernance.sol"]["VLakeGovernance"]
    abi = contract_data["abi"]
    bytecode = contract_data["evm"]["bytecode"]["object"]
    deployed_bytecode = contract_data["evm"]["deployedBytecode"]["object"]

    # Save ABI
    abi_path = os.path.join(BACKEND_DIR, "contract_abi.json")
    os.makedirs(BACKEND_DIR, exist_ok=True)
    with open(abi_path, "w") as f:
        json.dump(abi, f, indent=2)

    # Save full compilation artifact
    artifact_path = os.path.join(CONTRACT_DIR, "VLakeGovernance.json")
    with open(artifact_path, "w") as f:
        json.dump({
            "contractName": "VLakeGovernance",
            "abi": abi,
            "bytecode": f"0x{bytecode}",
            "deployedBytecode": f"0x{deployed_bytecode}",
            "compiler": {"version": SOLC_VERSION},
        }, f, indent=2)

    print(f"  ✓ ABI saved:       {abi_path}")
    print(f"  ✓ Artifact saved:  {artifact_path}")
    print(f"  ✓ ABI entries:     {len(abi)}")
    print(f"  ✓ Bytecode size:   {len(bytecode) // 2} bytes")
    print(f"  ✓ Events:          {sum(1 for a in abi if a.get('type') == 'event')}")
    print(f"  ✓ Functions:       {sum(1 for a in abi if a.get('type') == 'function')}")

    return abi, bytecode


# ════════════════════════════════════════════════
# DEPLOY
# ════════════════════════════════════════════════
def deploy_contract(abi, bytecode, rpc_url):
    """Deploy contract to Besu network."""
    step("2/4", "Deploying to Hyperledger Besu")

    print(f"  Connecting to {rpc_url}...")
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))

    if not w3.is_connected():
        print("  ✕ Cannot connect to Besu node")
        print("  Make sure the network is running: docker-compose up -d")
        sys.exit(1)

    chain_id = w3.eth.chain_id
    block = w3.eth.block_number
    print(f"  ✓ Connected — Chain ID: {chain_id}, Block: {block}")

    # Check consensus
    try:
        peers = w3.provider.make_request("net_peerCount", [])
        print(f"  ✓ Peers: {int(peers['result'], 16)}")
    except Exception:
        pass

    # Deployer = Steward-1
    deployer = STEWARD_ACCOUNTS[0]
    deployer_addr = Web3.to_checksum_address(deployer["address"])
    balance = w3.eth.get_balance(deployer_addr)
    print(f"  Deployer: {deployer_addr}")
    print(f"  Balance:  {w3.from_wei(balance, 'ether')} ETH")

    # Steward addresses for constructor
    steward_addrs = [Web3.to_checksum_address(s["address"]) for s in STEWARD_ACCOUNTS]
    print(f"\n  Deploying with {len(steward_addrs)} stewards:")
    for i, (s, a) in enumerate(zip(STEWARD_ACCOUNTS, steward_addrs)):
        print(f"    [{i+1}] {a} — {s['label']}")

    # Build & sign & send
    Contract = w3.eth.contract(abi=abi, bytecode=bytecode)
    nonce = w3.eth.get_transaction_count(deployer_addr)
    tx = Contract.constructor(steward_addrs).build_transaction({
        "from": deployer_addr,
        "nonce": nonce,
        "gas": 12_000_000,
        "gasPrice": 0,  # Free gas on permissioned network
        "chainId": chain_id,
    })

    print("\n  Signing and sending deployment transaction...")
    signed = w3.eth.account.sign_transaction(tx, deployer["private_key"])
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"  TX Hash: {tx_hash.hex()}")

    print("  Waiting for confirmation...")
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

    contract_address = receipt.contractAddress
    print(f"\n  {'═' * 50}")
    print(f"  ✓ CONTRACT DEPLOYED SUCCESSFULLY")
    print(f"  {'═' * 50}")
    print(f"  Address:   {contract_address}")
    print(f"  Block:     {receipt.blockNumber}")
    print(f"  Gas Used:  {receipt.gasUsed:,}")
    print(f"  TX Hash:   {receipt.transactionHash.hex()}")

    # Merge into .env (preserve any keys the user already set, e.g. VLAKE_MASTER_KEY)
    env_path = os.path.join(PROJECT_DIR, ".env")
    updates = {
        "CONTRACT_ADDRESS": contract_address,
        "BESU_RPC": rpc_url,
        "CHAIN_ID": str(chain_id),
        "DEPLOYED_AT_BLOCK": str(receipt.blockNumber),
        "DEPLOYED_AT": str(int(time.time())),
    }
    existing_lines = []
    seen_keys = set()
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "=" not in stripped:
                    existing_lines.append(line.rstrip("\n"))
                    continue
                key = stripped.split("=", 1)[0].strip()
                if key in updates:
                    existing_lines.append(f"{key}={updates[key]}")
                    seen_keys.add(key)
                else:
                    existing_lines.append(line.rstrip("\n"))
    for key, value in updates.items():
        if key not in seen_keys:
            existing_lines.append(f"{key}={value}")
    with open(env_path, "w") as f:
        f.write("\n".join(existing_lines).rstrip() + "\n")
    print(f"  ✓ Config merged into {env_path} (existing keys preserved)")

    return w3, contract_address


# ════════════════════════════════════════════════
# VERIFY DEPLOYMENT
# ════════════════════════════════════════════════
def verify_deployment(w3, abi, contract_address):
    """Verify all on-chain state is correct post-deployment."""
    step("3/4", "Verifying Deployment")

    contract = w3.eth.contract(address=contract_address, abi=abi)
    errors = []

    # Check stewards
    sc = contract.functions.stewardCount().call()
    print(f"  Steward count: {sc}")
    if sc != len(STEWARD_ACCOUNTS):
        errors.append(f"Expected {len(STEWARD_ACCOUNTS)} stewards, got {sc}")

    for s in STEWARD_ACCOUNTS:
        addr = Web3.to_checksum_address(s["address"])
        is_s = contract.functions.isSteward(addr).call()
        role = contract.functions.roles(addr).call()
        print(f"    {addr}: isSteward={is_s}, role={role}")
        if not is_s:
            errors.append(f"{addr} not registered as steward")
        if role != 1:  # Role.DATA_STEWARD = 1
            errors.append(f"{addr} has wrong role {role}, expected 1")

    # Check weights
    s, c, a, sub = contract.functions.getWeights().call()
    print(f"  WQC Weights: Steward={s}, Custodian={c}, Analyst={a}, Subject={sub}")
    if s != 3 or c != 2 or a != 1 or sub != 0:
        errors.append("WQC weights incorrect")

    # Check initial state
    dc = contract.functions.datasetCount().call()
    pc = contract.functions.proposalCount().call()
    print(f"  Datasets: {dc}, Proposals: {pc}")
    if dc != 0 or pc != 0:
        errors.append("Initial state not clean")

    # Check consent chain
    head = contract.functions.getConsentChainHead().call()
    cc = contract.functions.getConsentCount().call()
    print(f"  Consent chain: head={head.hex()}, count={cc}")

    if errors:
        print(f"\n  ✕ VERIFICATION FAILED:")
        for e in errors:
            print(f"    - {e}")
        return False
    else:
        print(f"\n  ✓ ALL CHECKS PASSED")
        return True


# ════════════════════════════════════════════════
# SMOKE TEST
# ════════════════════════════════════════════════
def smoke_test(w3, abi, contract_address):
    """End-to-end test: create dataset, proposal, vote, verify WQC."""
    step("4/4", "Running Smoke Test (on-chain)")

    contract = w3.eth.contract(address=contract_address, abi=abi)
    deployer = STEWARD_ACCOUNTS[0]
    deployer_addr = Web3.to_checksum_address(deployer["address"])
    chain_id = w3.eth.chain_id

    def send_tx(fn, sender=None):
        """Helper to build, sign, send a contract call."""
        s = sender or deployer
        addr = Web3.to_checksum_address(s["address"])
        nonce = w3.eth.get_transaction_count(addr)
        tx = fn.build_transaction({
            "from": addr, "nonce": nonce, "gas": 5_000_000,
            "gasPrice": 0, "chainId": chain_id
        })
        signed = w3.eth.account.sign_transaction(tx, s["private_key"])
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        return w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

    # 1. Create dataset
    print("  [test] Creating dataset...")
    r = send_tx(contract.functions.createDataset(
        "test_patients", "Smoke test dataset", "[]",
        0,  # SourceType.LOCAL_FILE
        "", True
    ))
    print(f"    ✓ Dataset created (gas: {r.gasUsed:,})")

    dc = contract.functions.datasetCount().call()
    assert dc == 1, f"Dataset count should be 1, got {dc}"

    # 2. Record ingestion with Merkle root
    print("  [test] Recording ingestion with Merkle root...")
    fake_root = Web3.keccak(text="vlake.test.merkle.root")
    r = send_tx(contract.functions.recordIngestion(1, fake_root, 100, 100, 7))
    print(f"    ✓ Ingestion recorded (gas: {r.gasUsed:,})")

    ds = contract.functions.datasets(1).call()
    assert ds[4] == fake_root, "Merkle root mismatch"  # index 4 = merkleRoot

    # 3. Create ASSIGN_CUSTODIAN proposal
    cust_addr = Web3.to_checksum_address(DEMO_ACCOUNTS["custodian"])
    print(f"  [test] Creating ASSIGN_CUSTODIAN proposal for {cust_addr}...")
    r = send_tx(contract.functions.createProposal(
        0,  # ASSIGN_CUSTODIAN
        1, cust_addr, "{}", 3600
    ))
    print(f"    ✓ Proposal #1 created (gas: {r.gasUsed:,})")

    # 4. Vote from all 3 stewards
    print("  [test] Voting from 3 stewards (WQC standard quorum)...")
    for i, s in enumerate(STEWARD_ACCOUNTS):
        r = send_tx(contract.functions.vote(1, True), sender=s)
        p = contract.functions.proposals(1).call()
        print(f"    Steward-{i+1} voted ✓ (yesWeight: {p[10]}, status: {p[6]})")

    # Check proposal was executed
    p = contract.functions.proposals(1).call()
    assert p[6] == 1, f"Proposal should be EXECUTED (1), got {p[6]}"
    qc = p[19]  # quorumCertificate
    print(f"    ✓ Proposal EXECUTED — Quorum Certificate: {qc.hex()[:32]}...")

    # 5. Verify custodian was assigned
    is_cust = contract.functions.isCustodian(1, cust_addr).call()
    assert is_cust, "Custodian not assigned"
    print(f"    ✓ {cust_addr} is now custodian of dataset 1")

    # 6. Register subject + link + delegate (C3: SSI)
    patient_addr = Web3.to_checksum_address(DEMO_ACCOUNTS["patient"])
    print(f"  [test] Registering subject (patient) + SSI link...")
    r = send_tx(contract.functions.registerSubject(patient_addr, "did:vlake:test123"))
    r = send_tx(contract.functions.linkSubjectToDataset(patient_addr, 1, "patient_id='P001'"))
    print(f"    ✓ Subject linked — consent chain updated")

    cc = contract.functions.getConsentCount().call()
    head = contract.functions.getConsentChainHead().call()
    print(f"    ✓ Consent chain: {cc} records, head={head.hex()[:32]}...")

    # 7. Log a query
    print("  [test] Logging query with attestation...")
    qh = Web3.keccak(text="SELECT * FROM test_patients WHERE patient_id='P001'")
    rh = Web3.keccak(text="result_hash_placeholder")
    r = send_tx(contract.functions.logQuery(patient_addr, 1, qh, rh, fake_root, True))
    print(f"    ✓ Query logged (gas: {r.gasUsed:,})")

    qlc = contract.functions.queryLogCount().call()
    assert qlc == 1, f"Query log count should be 1, got {qlc}"

    print(f"\n  {'═' * 50}")
    print(f"  ✓ ALL SMOKE TESTS PASSED")
    print(f"  {'═' * 50}")
    print(f"  Datasets:    {contract.functions.datasetCount().call()}")
    print(f"  Proposals:   {contract.functions.proposalCount().call()}")
    print(f"  Query Logs:  {contract.functions.queryLogCount().call()}")
    print(f"  Consents:    {contract.functions.getConsentCount().call()}")
    print(f"  Stewards:    {contract.functions.stewardCount().call()}")
    return True


# ════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="V-Lake Smart Contract Deployment")
    parser.add_argument("--rpc", default=os.getenv("BESU_RPC", "http://localhost:8545"), help="Besu RPC URL")
    parser.add_argument("--compile-only", action="store_true", help="Only compile, don't deploy")
    parser.add_argument("--skip-verify", action="store_true", help="Skip post-deployment verification")
    parser.add_argument("--smoke-test", action="store_true", default=True, help="Run smoke test after deploy")
    parser.add_argument("--no-smoke-test", action="store_true", help="Skip smoke test")
    args = parser.parse_args()

    banner("V-LAKE SMART CONTRACT DEPLOYMENT")
    print(f"  Network:   Hyperledger Besu (QBFT)")
    print(f"  Contract:  VLakeGovernance.sol")
    print(f"  Compiler:  solc {SOLC_VERSION}")
    print(f"  RPC:       {args.rpc}")
    print(f"  Stewards:  {len(STEWARD_ACCOUNTS)}")

    # Compile
    abi, bytecode = compile_contract()

    if args.compile_only:
        print("\n  ✓ Compilation complete (--compile-only). Skipping deployment.")
        return

    # Deploy
    try:
        w3, address = deploy_contract(abi, bytecode, args.rpc)
    except Exception as e:
        print(f"\n  ✕ Deployment failed: {e}")
        print(f"\n  To run in STANDALONE MODE (no Besu):")
        print(f"    cd backend && python app.py")
        print(f"  The backend auto-detects missing Besu and uses in-memory governance.")
        sys.exit(1)

    # Verify
    if not args.skip_verify:
        ok = verify_deployment(w3, abi, address)
        if not ok:
            sys.exit(1)

    # Smoke test
    if args.smoke_test and not args.no_smoke_test:
        try:
            smoke_test(w3, abi, address)
        except Exception as e:
            print(f"\n  ✕ Smoke test failed: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    banner("DEPLOYMENT COMPLETE")
    print(f"  Contract Address: {address}")
    print(f"  Saved to:         .env, backend/contract_abi.json")
    print(f"\n  Next steps:")
    print(f"    1. Start backend:  cd backend && python app.py")
    print(f"    2. Open frontend:  frontend/index.html")
    print(f"    3. Or use Docker:  docker-compose up -d")


if __name__ == "__main__":
    main()
