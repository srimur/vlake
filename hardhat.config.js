require("@nomicfoundation/hardhat-toolbox");

/** @type import('hardhat/config').HardhatUserConfig */
module.exports = {
  solidity: {
    version: "0.8.19",
    settings: {
      optimizer: { enabled: true, runs: 200 },
    },
  },
  networks: {
    // Local Besu QBFT network.
    //
    // The three account values below are PUBLIC TEST KEYS — the well-known
    // Hyperledger Besu / Truffle dev-mode private keys, pre-funded in the Besu
    // dev genesis. They are NOT secrets, only control identities on local
    // chain id 1337, and are documented in thousands of repos. For production,
    // load steward keys from a KMS / Vault / HSM via STEWARD_KEYS_JSON or
    // similar and remove these literals.
    besu: {
      url: process.env.BESU_RPC || "http://localhost:8545",
      chainId: 1337,
      gasPrice: 0,
      accounts: [
        "0xae6ae8e5ccbfb04590405997ee2d52d2b330726137b875053c36d94e974d162f",
        "0xc87509a1c067bbde78beb793e6fa76530b6382a4c0241e5e4a9ec0a0f44dc0d3",
        "0x8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63",
      ],
    },
    // Hardhat local for fast testing
    hardhat: {
      chainId: 1337,
      gasPrice: 0,
    },
  },
  paths: {
    sources: "./contracts",
    tests: "./test",
    cache: "./cache",
    artifacts: "./artifacts",
  },
};
