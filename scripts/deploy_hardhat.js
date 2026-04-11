/**
 * V-Lake Hardhat Deployment Script
 * Alternative to deploy_contract.py — uses Hardhat instead of py-solc-x
 *
 * Usage:
 *   npx hardhat run scripts/deploy_hardhat.js --network besu
 */

const { ethers } = require("hardhat");
const fs = require("fs");
const path = require("path");

async function main() {
  console.log("═══════════════════════════════════════════════════");
  console.log("  V-LAKE SMART CONTRACT DEPLOYMENT (Hardhat)");
  console.log("═══════════════════════════════════════════════════");

  const [deployer, steward2, steward3] = await ethers.getSigners();
  console.log(`  Deployer: ${deployer.address}`);

  const stewards = [deployer.address, steward2.address, steward3.address];
  console.log(`  Stewards: ${stewards.length}`);
  stewards.forEach((s, i) => console.log(`    [${i + 1}] ${s}`));

  // Deploy
  console.log("\n  Deploying VLakeGovernance...");
  const VLake = await ethers.getContractFactory("VLakeGovernance");
  const vlake = await VLake.deploy(stewards);
  await vlake.waitForDeployment();

  const address = await vlake.getAddress();
  console.log(`\n  ✓ Deployed at: ${address}`);

  // Verify
  const sc = await vlake.stewardCount();
  console.log(`  ✓ Steward count: ${sc}`);
  const [ws, wc, wa, wsub] = await vlake.getWeights();
  console.log(`  ✓ Weights: S=${ws} C=${wc} A=${wa} Sub=${wsub}`);

  // Save ABI
  const artifact = require("../artifacts/contracts/VLakeGovernance.sol/VLakeGovernance.json");
  const abiPath = path.join(__dirname, "..", "backend", "contract_abi.json");
  fs.mkdirSync(path.dirname(abiPath), { recursive: true });
  fs.writeFileSync(abiPath, JSON.stringify(artifact.abi, null, 2));
  console.log(`  ✓ ABI saved: ${abiPath}`);

  // Merge into .env (preserve any keys the user already set, e.g. VLAKE_MASTER_KEY)
  const envPath = path.join(__dirname, "..", ".env");
  const updates = {
    CONTRACT_ADDRESS: address,
    BESU_RPC: process.env.BESU_RPC || "http://localhost:8545",
  };
  const lines = [];
  const seen = new Set();
  if (fs.existsSync(envPath)) {
    for (const raw of fs.readFileSync(envPath, "utf8").split(/\r?\n/)) {
      const stripped = raw.trim();
      if (!stripped || stripped.startsWith("#") || !stripped.includes("=")) { lines.push(raw); continue; }
      const key = stripped.split("=", 1)[0].trim();
      if (key in updates) { lines.push(`${key}=${updates[key]}`); seen.add(key); }
      else { lines.push(raw); }
    }
  }
  for (const [k, v] of Object.entries(updates)) {
    if (!seen.has(k)) lines.push(`${k}=${v}`);
  }
  fs.writeFileSync(envPath, lines.join("\n").replace(/\n+$/, "") + "\n");
  console.log(`  ✓ Config merged into ${envPath} (existing keys preserved)`);

  console.log("\n═══════════════════════════════════════════════════");
  console.log("  DEPLOYMENT COMPLETE");
  console.log("═══════════════════════════════════════════════════");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
