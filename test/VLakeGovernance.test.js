const { expect } = require("chai");
const { ethers } = require("hardhat");

describe("VLakeGovernance", function () {
  let vlake;
  let steward1, steward2, steward3, custodian, analyst, patient, delegate;

  beforeEach(async function () {
    [steward1, steward2, steward3, custodian, analyst, patient, delegate] = await ethers.getSigners();
    const VLake = await ethers.getContractFactory("VLakeGovernance");
    vlake = await VLake.deploy([steward1.address, steward2.address, steward3.address]);
    await vlake.waitForDeployment();
  });

  describe("Deployment", function () {
    it("should register 3 stewards", async function () {
      expect(await vlake.stewardCount()).to.equal(3);
      expect(await vlake.isSteward(steward1.address)).to.be.true;
      expect(await vlake.isSteward(steward2.address)).to.be.true;
      expect(await vlake.isSteward(steward3.address)).to.be.true;
    });

    it("should set correct WQC weights", async function () {
      const [s, c, a, sub] = await vlake.getWeights();
      expect(s).to.equal(3);
      expect(c).to.equal(2);
      expect(a).to.equal(1);
      expect(sub).to.equal(0);
    });

    it("should reject zero stewards", async function () {
      const VLake = await ethers.getContractFactory("VLakeGovernance");
      await expect(VLake.deploy([])).to.be.revertedWith("Need >= 1 steward");
    });
  });

  describe("C1: Domain-Separated Merkle Tree", function () {
    beforeEach(async function () {
      await vlake.connect(steward1).createDataset("test", "desc", "[]", 0, "", true);
    });

    it("should record ingestion with Merkle root", async function () {
      const root = ethers.keccak256(ethers.toUtf8Bytes("vlake.test.root"));
      await vlake.connect(steward1).recordIngestion(1, root, 100, 100, 7);
      const ds = await vlake.datasets(1);
      expect(ds.merkleRoot).to.equal(root);
      expect(ds.rowCount).to.equal(100);
      expect(ds.leafCount).to.equal(100);
      expect(ds.treeDepth).to.equal(7);
    });

    it("should maintain Merkle history", async function () {
      const root1 = ethers.keccak256(ethers.toUtf8Bytes("root1"));
      const root2 = ethers.keccak256(ethers.toUtf8Bytes("root2"));
      await vlake.connect(steward1).recordIngestion(1, root1, 50, 50, 6);
      await vlake.connect(steward1).recordIngestion(1, root2, 100, 100, 7);
      const history = await vlake.getMerkleHistory(1);
      expect(history.length).to.equal(2);
      expect(history[0]).to.equal(root1);
      expect(history[1]).to.equal(root2);
    });

    it("should reject unauthorized ingestion", async function () {
      const root = ethers.keccak256(ethers.toUtf8Bytes("bad"));
      await expect(
        vlake.connect(analyst).recordIngestion(1, root, 10, 10, 4)
      ).to.be.revertedWith("Unauthorized");
    });
  });

  describe("C2: Weighted Quorum Consensus", function () {
    beforeEach(async function () {
      await vlake.connect(steward1).createDataset("test", "desc", "[]", 0, "", true);
    });

    it("ASSIGN_CUSTODIAN: standard quorum (>50% weight)", async function () {
      // Create proposal
      await vlake.connect(steward1).createProposal(0, 1, custodian.address, "{}", 3600);
      // 2 stewards approve → 6/9 weight = 67% > 50% → passes
      await vlake.connect(steward1).vote(1, true);
      await vlake.connect(steward2).vote(1, true);
      const p = await vlake.proposals(1);
      expect(p.status).to.equal(1); // EXECUTED
      expect(await vlake.isCustodian(1, custodian.address)).to.be.true;
    });

    it("REVOKE_ANALYST: emergency quorum (any 1 steward)", async function () {
      // First onboard analyst
      await vlake.connect(steward1).createProposal(1, 1, analyst.address, "{}", 3600);
      await vlake.connect(steward1).vote(1, true);
      await vlake.connect(steward2).vote(1, true);
      await vlake.connect(steward3).vote(1, true);

      // Now revoke — only 1 steward needed
      await vlake.connect(steward1).createProposal(4, 1, analyst.address, "{}", 3600);
      await vlake.connect(steward1).vote(2, true);
      const p = await vlake.proposals(2);
      expect(p.status).to.equal(1); // EXECUTED
    });

    it("ATTACH_POLICY: critical quorum (≥2/3 + all stewards)", async function () {
      await vlake.connect(steward1).createProposal(5, 1, ethers.ZeroAddress, '{"policy":"HIPAA"}', 3600);
      // Need ALL stewards for critical
      await vlake.connect(steward1).vote(1, true);
      let p = await vlake.proposals(1);
      expect(p.status).to.equal(0); // Still PENDING

      await vlake.connect(steward2).vote(1, true);
      p = await vlake.proposals(1);
      expect(p.status).to.equal(0); // Still PENDING

      await vlake.connect(steward3).vote(1, true);
      p = await vlake.proposals(1);
      expect(p.status).to.equal(1); // EXECUTED
    });

    it("should generate Quorum Certificate on finalization", async function () {
      await vlake.connect(steward1).createProposal(0, 1, custodian.address, "{}", 3600);
      await vlake.connect(steward1).vote(1, true);
      await vlake.connect(steward2).vote(1, true);
      const p = await vlake.proposals(1);
      expect(p.quorumCertificate).to.not.equal(ethers.ZeroHash);
    });

    it("should reject if steward vetoes critical proposal", async function () {
      await vlake.connect(steward1).createProposal(5, 1, ethers.ZeroAddress, '{"policy":"HIPAA"}', 3600);
      await vlake.connect(steward1).vote(1, true);
      await vlake.connect(steward2).vote(1, true);
      // Steward3 rejects → requireAllStewards fails
      await vlake.connect(steward3).vote(1, false);
      const p = await vlake.proposals(1);
      expect(p.status).to.equal(2); // REJECTED
    });

    it("should prevent double voting", async function () {
      await vlake.connect(steward1).createProposal(0, 1, custodian.address, "{}", 3600);
      await vlake.connect(steward1).vote(1, true);
      await expect(vlake.connect(steward1).vote(1, true)).to.be.revertedWith("Already voted");
    });

    it("should track weight correctly", async function () {
      // Add custodian first
      await vlake.connect(steward1).createProposal(0, 1, custodian.address, "{}", 3600);
      await vlake.connect(steward1).vote(1, true);
      await vlake.connect(steward2).vote(1, true);

      // Now ONBOARD_ANALYST — needs all stewards + custodian majority
      await vlake.connect(steward1).createProposal(1, 1, analyst.address, "{}", 3600);
      const [yW, nW, tW, rW, sY, sN, cY, cN, qType, qc] = await vlake.getProposalWQC(2);
      // Total: 3*3 + 1*2 = 11 weight
      expect(tW).to.equal(11);
    });
  });

  describe("C3: Self-Sovereign Identity (SSI)", function () {
    beforeEach(async function () {
      await vlake.connect(steward1).createDataset("patients", "PHI", "[]", 0, "", true);
    });

    it("should register subject with DID", async function () {
      await vlake.connect(steward1).registerSubject(patient.address, "did:vlake:abc123");
      expect(await vlake.roles(patient.address)).to.equal(4); // SUBJECT
      expect(await vlake.subjectDID(patient.address)).to.equal("did:vlake:abc123");
    });

    it("should link subject and update consent chain", async function () {
      await vlake.connect(steward1).registerSubject(patient.address, "did:vlake:abc123");
      await vlake.connect(steward1).linkSubjectToDataset(patient.address, 1, "patient_id='P001'");
      expect(await vlake.getConsentCount()).to.equal(1);
      expect(await vlake.getConsentChainHead()).to.not.equal(ethers.ZeroHash);
    });

    it("should allow subject to delegate access", async function () {
      await vlake.connect(steward1).registerSubject(patient.address, "did:vlake:abc123");
      await vlake.connect(steward1).linkSubjectToDataset(patient.address, 1, "patient_id='P001'");
      await vlake.connect(patient).createDelegation(delegate.address, 1, '{"columns":"diagnosis"}', 86400);

      expect(await vlake.getConsentCount()).to.equal(2); // LINK + DELEGATE
      const [hasAccess, level, , filter,] = await vlake.checkAccess(delegate.address, 1);
      expect(hasAccess).to.be.true;
      expect(filter).to.equal("patient_id='P001'");
    });

    it("should allow subject to revoke delegation (steward-independent)", async function () {
      await vlake.connect(steward1).registerSubject(patient.address, "did:vlake:abc123");
      await vlake.connect(steward1).linkSubjectToDataset(patient.address, 1, "patient_id='P001'");
      await vlake.connect(patient).createDelegation(delegate.address, 1, "", 86400);

      // Patient revokes — no steward needed
      await vlake.connect(patient).revokeDelegation(1);
      const [hasAccess] = await vlake.checkAccess(delegate.address, 1);
      expect(hasAccess).to.be.false;
      expect(await vlake.getConsentCount()).to.equal(3); // LINK + DELEGATE + REVOKE
    });

    it("should maintain hash-linked consent chain", async function () {
      await vlake.connect(steward1).registerSubject(patient.address, "did:vlake:abc123");
      await vlake.connect(steward1).linkSubjectToDataset(patient.address, 1, "patient_id='P001'");

      const consent1 = await vlake.consents(1);
      expect(consent1.prevHash).to.equal(ethers.ZeroHash); // First in chain

      await vlake.connect(patient).createDelegation(delegate.address, 1, "", 86400);
      const consent2 = await vlake.consents(2);
      expect(consent2.prevHash).to.equal(consent1.consentHash); // Chain link
    });

    it("should reject non-subject delegation", async function () {
      await expect(
        vlake.connect(analyst).createDelegation(delegate.address, 1, "", 86400)
      ).to.be.revertedWith("Not a subject");
    });
  });

  describe("C4: Federated Data Sources", function () {
    it("should create datasets with different source types", async function () {
      // LOCAL_FILE = 0, S3_MINIO = 1, POSTGRESQL = 2, etc.
      await vlake.connect(steward1).createDataset("s3_data", "S3", "[]", 1, "s3://bucket/path", false);
      await vlake.connect(steward1).createDataset("pg_data", "PG", "[]", 2, "postgresql://...", true);
      await vlake.connect(steward1).createDataset("kafka_data", "Kafka", "[]", 8, "kafka://topic", true);

      expect(await vlake.datasetCount()).to.equal(3);
      const ds1 = await vlake.datasets(1);
      expect(ds1.sourceType).to.equal(1); // S3_MINIO
      const ds2 = await vlake.datasets(2);
      expect(ds2.sourceType).to.equal(2); // POSTGRESQL
    });
  });

  describe("Query Audit", function () {
    beforeEach(async function () {
      await vlake.connect(steward1).createDataset("test", "desc", "[]", 0, "", true);
    });

    it("should log queries with attestation", async function () {
      const qh = ethers.keccak256(ethers.toUtf8Bytes("SELECT * FROM test"));
      const rh = ethers.keccak256(ethers.toUtf8Bytes("result"));
      const mr = ethers.keccak256(ethers.toUtf8Bytes("merkle_root"));
      await vlake.connect(steward1).logQuery(steward1.address, 1, qh, rh, mr, true);
      expect(await vlake.queryLogCount()).to.equal(1);
      const log = await vlake.queryLogs(1);
      expect(log.attestation).to.not.equal(ethers.ZeroHash);
    });
  });

  describe("Compliance", function () {
    it("should create and attach policies", async function () {
      await vlake.connect(steward1).createDataset("test", "desc", "[]", 0, "", true);
      await vlake.connect(steward1).createPolicy("HIPAA Safe Harbor", 0, '{"rules":[]}');
      await vlake.connect(steward1).attachPolicyToDataset(1, 1);
      const pids = await vlake.getDatasetPolicies(1);
      expect(pids.length).to.equal(1);
    });

    it("should record hash-linked attestations", async function () {
      await vlake.connect(steward1).createDataset("test", "desc", "[]", 0, "", true);
      const hash1 = ethers.keccak256(ethers.toUtf8Bytes("attestation1"));
      const hash2 = ethers.keccak256(ethers.toUtf8Bytes("attestation2"));
      await vlake.connect(steward1).recordAttestation(1, steward1.address, 1, true, hash1);
      await vlake.connect(steward1).recordAttestation(2, steward1.address, 1, true, hash2);

      const att2 = await vlake.attestations(2);
      expect(att2.prevAttestation).to.equal(hash1); // Chain link
      expect(await vlake.lastAttestationHash()).to.equal(hash2);
    });
  });
});
