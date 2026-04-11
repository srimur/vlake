// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

/**
 * @title VLakeGovernance
 * @author V-Lake Research Team
 * @notice On-chain governance for Verifiable Lakehouse Access & Knowledge Engine
 *
 * FOUR CONTRIBUTIONS:
 * ═══════════════════
 *   C1. Domain-Separated Merkle Tree
 *       - Roots committed on-chain per ingestion
 *       - Leaf: H("vlake.leaf:" ‖ idx ‖ ncols ‖ canonical_row)
 *       - Odd-leaf promotion (not duplication)
 *       - Position-binding prevents reordering attacks
 *
 *   C2. Weighted Quorum Consensus (WQC)
 *       - Steward=3, Custodian=2, Analyst=1, Subject=0
 *       - Critical ops: ≥2/3 total weight AND all stewards approve
 *       - Standard ops: >1/2 total weight
 *       - Emergency revoke: any 1 steward (fast path)
 *       - Safety: no proposal both APPROVED and REJECTED
 *       - Quorum Certificate: H(proposal ‖ votes ‖ outcome ‖ timestamp)
 *
 *   C3. Self-Sovereign Identity (SSI)
 *       - Subjects hold DID-linked keys
 *       - Hash-linked consent chain: each consent references previous
 *       - Delegation/revocation without intermediary
 *
 *   C4. Federated Data Source Registry
 *       - S3/MinIO, PostgreSQL, Trino, Delta Lake, HDFS, Iceberg,
 *         MySQL, MongoDB, Kafka, Local File
 *       - Source type + metadata stored on-chain per dataset
 *
 * NETWORK: Hyperledger Besu (QBFT consensus, 4 validators, f=1)
 * GAS: Zero gas price (permissioned network)
 */
contract VLakeGovernance {

    // ═══════════════════════════════════════════
    // ENUMS
    // ═══════════════════════════════════════════
    enum Role { NONE, DATA_STEWARD, DATA_CUSTODIAN, ANALYST, SUBJECT }

    enum ProposalType {
        ASSIGN_CUSTODIAN,      // Add custodian to dataset
        ONBOARD_ANALYST,       // Allow analyst to query a dataset
        ACCESS_GRANT,          // Grant specific col/row scope access
        REVOKE_CUSTODIAN,      // Remove custodian
        REVOKE_ANALYST,        // Emergency: remove analyst access
        ATTACH_POLICY,         // Attach compliance policy (HIPAA/GDPR/DPDPA)
        TOGGLE_CONFIDENTIAL    // Change download permission
    }

    enum ProposalStatus { PENDING, EXECUTED, REJECTED, EXPIRED }

    enum QuorumType { STANDARD, CRITICAL, EMERGENCY }

    enum AccessLevel { NONE_ACCESS, VIEW_ONLY, VIEW_DOWNLOAD }

    enum ComplianceStandard { HIPAA, GDPR, DPDPA, CUSTOM }

    // C4: Federated source types
    enum SourceType {
        LOCAL_FILE, S3_MINIO, POSTGRESQL, MYSQL, TRINO,
        DELTA_LAKE, ICEBERG, HDFS, KAFKA, MONGODB
    }

    // ═══════════════════════════════════════════
    // STRUCTS
    // ═══════════════════════════════════════════
    struct Dataset {
        uint256 id;
        string name;
        string description;
        string schemaJson;
        bytes32 merkleRoot;           // C1: domain-separated Merkle root
        address creator;
        SourceType sourceType;        // C4: federated source
        string sourceUri;
        bool isConfidential;
        bool active;
        uint256 createdAt;
        uint256 lastIngestionAt;
        uint256 rowCount;
        uint256 leafCount;            // C1: number of Merkle leaves
        uint256 treeDepth;            // C1: tree depth
    }

    struct AccessGrant {
        uint256 datasetId;
        address grantee;
        string allowedColumns;        // JSON or CSV column list
        string rowFilter;             // SQL WHERE predicate
        AccessLevel level;
        uint256 grantedAt;
        uint256 expiresAt;            // 0 = no expiry
        bool active;
    }

    // C2: WQC Proposal with weight tracking
    struct Proposal {
        uint256 id;
        ProposalType pType;
        address proposer;
        uint256 datasetId;
        address targetAddress;
        string metadata;              // JSON: columns, rowFilter, policy, duration, etc.
        ProposalStatus status;
        QuorumType quorumType;
        uint256 createdAt;
        uint256 votingDeadline;
        // WQC weight tracking
        uint256 yesWeight;
        uint256 noWeight;
        uint256 totalWeight;          // Snapshot at creation
        uint256 requiredWeight;       // Threshold to pass
        uint256 stewardYes;
        uint256 stewardNo;
        uint256 custodianYes;
        uint256 custodianNo;
        bool requireAllStewards;
        bool requireCustodianMajority;
        bytes32 quorumCertificate;    // C2: H(proposal ‖ votes ‖ outcome ‖ timestamp)
    }

    struct CompliancePolicy {
        uint256 id;
        string name;
        ComplianceStandard standard;
        string rulesJson;
        bool active;
        uint256 createdAt;
    }

    // C1: Attestation with hash chain
    struct ComplianceAttestation {
        uint256 id;
        uint256 queryLogId;
        address querier;
        uint256 datasetId;
        bool passed;
        bytes32 attestationHash;      // H(query ‖ policy ‖ checks ‖ prev ‖ timestamp)
        bytes32 prevAttestation;      // Chain link
        uint256 blockNum;
        uint256 timestamp;
    }

    // C3: SSI consent record
    struct ConsentRecord {
        uint256 id;
        address subject;
        string action;                // LINK, DELEGATE, REVOKE
        address counterparty;         // delegate or linker
        uint256 datasetId;
        string scope;
        bytes32 consentHash;          // H(record)
        bytes32 prevHash;             // Chain link to previous
        uint256 timestamp;
    }

    struct SubjectDelegation {
        uint256 id;
        address subject;
        address delegate;
        uint256 datasetId;
        string scope;
        uint256 expiresAt;
        bool active;
    }

    struct QueryLog {
        uint256 id;
        address querier;
        uint256 datasetId;
        bytes32 queryHash;
        bytes32 resultHash;
        bytes32 merkleRoot;           // C1: root at time of query
        bytes32 attestation;          // H(queryHash ‖ resultHash ‖ merkleRoot ‖ timestamp)
        bool compliancePassed;
        uint256 blockNum;
        uint256 timestamp;
    }

    // ═══════════════════════════════════════════
    // C2: WQC ROLE WEIGHTS
    // ═══════════════════════════════════════════
    uint256 public constant WEIGHT_STEWARD   = 3;
    uint256 public constant WEIGHT_CUSTODIAN = 2;
    uint256 public constant WEIGHT_ANALYST   = 1;
    uint256 public constant WEIGHT_SUBJECT   = 0;

    // Quorum thresholds (basis points, 10000 = 100%)
    uint256 public constant STANDARD_THRESHOLD = 5000;   // >50%
    uint256 public constant CRITICAL_THRESHOLD = 6700;   // ≥67%

    // ═══════════════════════════════════════════
    // STATE
    // ═══════════════════════════════════════════
    address[] public stewards;
    mapping(address => bool) public isSteward;
    uint256 public stewardCount;

    mapping(address => Role) public roles;
    address[] public allUsers;
    mapping(address => bool) public isRegistered;

    // Datasets
    uint256 public datasetCount;
    mapping(uint256 => Dataset) public datasets;
    uint256[] public datasetIds;

    // Per-dataset custodians
    mapping(uint256 => address[]) public datasetCustodians;
    mapping(uint256 => mapping(address => bool)) public isCustodian;
    mapping(uint256 => uint256) public custodianCount;

    // Access grants
    mapping(address => mapping(uint256 => AccessGrant)) public accessGrants;
    mapping(uint256 => address[]) public datasetAnalysts;

    // Proposals & WQC
    uint256 public proposalCount;
    mapping(uint256 => Proposal) public proposals;
    mapping(uint256 => mapping(address => bool)) public hasVoted;
    mapping(uint256 => mapping(address => bool)) public voteChoice;

    // Compliance
    uint256 public policyCount;
    mapping(uint256 => CompliancePolicy) public policies;
    mapping(uint256 => uint256[]) public datasetPolicies;

    // Attestations (C1 chain)
    uint256 public attestationCount;
    mapping(uint256 => ComplianceAttestation) public attestations;
    bytes32 public lastAttestationHash;  // Head of attestation chain

    // SSI (C3)
    uint256 public consentCount;
    mapping(uint256 => ConsentRecord) public consents;
    bytes32 public consentChainHead;     // Head of consent chain
    mapping(address => string) public subjectDID;         // addr → did:vlake:...
    mapping(address => mapping(uint256 => string)) public subjectRecordFilter;

    // Delegations (C3)
    uint256 public delegationCount;
    mapping(uint256 => SubjectDelegation) public delegations;
    mapping(address => uint256[]) public subjectDelegationIds;

    // Query logs
    uint256 public queryLogCount;
    mapping(uint256 => QueryLog) public queryLogs;

    // Merkle history (C1)
    mapping(uint256 => bytes32[]) public merkleHistory;

    // C2: Cross-source forest root
    bytes32 public forestRoot;  // Φ — single hash verifying all datasets
    event ForestRootUpdated(bytes32 forestRoot, uint256 datasetCount);

    // ═══════════════════════════════════════════
    // EVENTS
    // ═══════════════════════════════════════════
    event DatasetCreated(uint256 indexed id, string name, SourceType sourceType);
    event DatasetIngestion(uint256 indexed id, bytes32 merkleRoot, uint256 rows, uint256 leafCount);
    event ProposalCreated(uint256 indexed id, ProposalType pType, QuorumType qType, uint256 datasetId, address target);
    event Voted(uint256 indexed proposalId, address voter, bool approve, uint256 weight, Role voterRole);
    event ProposalFinalized(uint256 indexed id, ProposalStatus status, bytes32 quorumCertificate);
    event AccessGranted(address indexed grantee, uint256 indexed datasetId, AccessLevel level, uint256 expiresAt);
    event AccessRevoked(address indexed grantee, uint256 indexed datasetId);
    event PolicyCreated(uint256 indexed id, string name, ComplianceStandard std);
    event PolicyAttached(uint256 indexed datasetId, uint256 indexed policyId);
    event ComplianceChecked(uint256 indexed id, uint256 datasetId, bool passed, bytes32 attestationHash);
    event SubjectRegistered(address indexed subject, string did);
    event SubjectLinked(address indexed subject, uint256 indexed datasetId, bytes32 consentHash);
    event DelegationCreated(uint256 indexed id, address subject, address delegate, bytes32 consentHash);
    event DelegationRevoked(uint256 indexed id, bytes32 consentHash);
    event ConsentRecorded(uint256 indexed id, address subject, string action, bytes32 consentHash, bytes32 prevHash);
    event QueryLogged(uint256 indexed id, address querier, uint256 datasetId, bytes32 attestation);
    event CustodianAssigned(uint256 indexed datasetId, address custodian);
    event CustodianRevoked(uint256 indexed datasetId, address custodian);

    // ═══════════════════════════════════════════
    // MODIFIERS
    // ═══════════════════════════════════════════
    modifier onlySteward() {
        require(isSteward[msg.sender], "Not a steward");
        _;
    }

    modifier datasetActive(uint256 _did) {
        require(_did > 0 && _did <= datasetCount && datasets[_did].active, "Invalid/inactive dataset");
        _;
    }

    // ═══════════════════════════════════════════
    // CONSTRUCTOR
    // ═══════════════════════════════════════════
    constructor(address[] memory _stewards) {
        require(_stewards.length >= 1, "Need >= 1 steward");
        for (uint256 i = 0; i < _stewards.length; i++) {
            require(_stewards[i] != address(0), "Zero address steward");
            require(!isSteward[_stewards[i]], "Duplicate steward");
            stewards.push(_stewards[i]);
            isSteward[_stewards[i]] = true;
            roles[_stewards[i]] = Role.DATA_STEWARD;
            _registerUser(_stewards[i]);
        }
        stewardCount = _stewards.length;
    }

    function _registerUser(address _u) internal {
        if (!isRegistered[_u]) {
            allUsers.push(_u);
            isRegistered[_u] = true;
        }
    }

    // ═══════════════════════════════════════════
    // DATASET MANAGEMENT
    // ═══════════════════════════════════════════
    function createDataset(
        string calldata _name,
        string calldata _desc,
        string calldata _schemaJson,
        SourceType _src,
        string calldata _uri,
        bool _confidential
    ) external onlySteward returns (uint256) {
        datasetCount++;
        datasets[datasetCount] = Dataset({
            id: datasetCount,
            name: _name,
            description: _desc,
            schemaJson: _schemaJson,
            merkleRoot: bytes32(0),
            creator: msg.sender,
            sourceType: _src,
            sourceUri: _uri,
            isConfidential: _confidential,
            active: true,
            createdAt: block.timestamp,
            lastIngestionAt: 0,
            rowCount: 0,
            leafCount: 0,
            treeDepth: 0
        });
        datasetIds.push(datasetCount);
        emit DatasetCreated(datasetCount, _name, _src);
        return datasetCount;
    }

    /// @notice Record a data ingestion with domain-separated Merkle root (C1)
    function recordIngestion(
        uint256 _did,
        bytes32 _merkleRoot,
        uint256 _rowCount,
        uint256 _leafCount,
        uint256 _treeDepth
    ) external datasetActive(_did) {
        require(isSteward[msg.sender] || isCustodian[_did][msg.sender], "Unauthorized");
        Dataset storage ds = datasets[_did];
        ds.merkleRoot = _merkleRoot;
        ds.rowCount = _rowCount;
        ds.leafCount = _leafCount;
        ds.treeDepth = _treeDepth;
        ds.lastIngestionAt = block.timestamp;
        merkleHistory[_did].push(_merkleRoot);
        emit DatasetIngestion(_did, _merkleRoot, _rowCount, _leafCount);
    }

    /// @notice C2: Update the cross-source forest root Φ (paper §5.3, Eq. 3)
    /// @dev Called after ingestion. Forest root = Merkle tree over all dataset roots.
    ///      Hforest(ρj, j) = SHA256("vlake.forest:" ‖ j ‖ ":" ‖ ρj)
    ///      A single hash verifying integrity across ALL datasets.
    function updateForestRoot(bytes32 _forestRoot) external {
        require(isSteward[msg.sender], "Only stewards can update forest root");
        forestRoot = _forestRoot;
        emit ForestRootUpdated(_forestRoot, datasetCount);
    }

    /// @notice Get the current forest root Φ
    function getForestRoot() external view returns (bytes32) {
        return forestRoot;
    }

    // ═══════════════════════════════════════════
    // C2: WEIGHTED QUORUM CONSENSUS — PROPOSALS
    // ═══════════════════════════════════════════

    /// @notice Compute quorum type and thresholds for a proposal type
    function _quorumParams(ProposalType _pt, uint256 _did) internal view returns (
        QuorumType qType, uint256 reqWeight, bool allStew, bool custMaj, uint256 totalW
    ) {
        uint256 cc = custodianCount[_did];
        totalW = (stewardCount * WEIGHT_STEWARD) + (cc * WEIGHT_CUSTODIAN);

        if (_pt == ProposalType.REVOKE_ANALYST) {
            // Emergency: any 1 steward
            return (QuorumType.EMERGENCY, WEIGHT_STEWARD, false, false, totalW);
        } else if (_pt == ProposalType.ATTACH_POLICY || _pt == ProposalType.TOGGLE_CONFIDENTIAL) {
            // Critical: ≥2/3 weight + all stewards
            uint256 rw = (totalW * CRITICAL_THRESHOLD + 9999) / 10000; // ceil
            bool cm = (_pt == ProposalType.TOGGLE_CONFIDENTIAL);
            return (QuorumType.CRITICAL, rw, true, cm, totalW);
        } else if (_pt == ProposalType.ONBOARD_ANALYST || _pt == ProposalType.ACCESS_GRANT) {
            // Standard but with all stewards + custodian majority
            uint256 rw = (totalW * STANDARD_THRESHOLD + 9999) / 10000;
            return (QuorumType.STANDARD, rw, true, true, totalW);
        } else {
            // Standard: >50% weight
            uint256 rw = (totalW * STANDARD_THRESHOLD + 9999) / 10000;
            return (QuorumType.STANDARD, rw, false, false, totalW);
        }
    }

    function createProposal(
        ProposalType _pType,
        uint256 _did,
        address _target,
        string calldata _metadata,
        uint256 _votingDurationSecs
    ) external returns (uint256) {
        if (_did > 0) require(datasets[_did].active, "Dataset inactive");
        require(_votingDurationSecs >= 60, "Min 60s voting");

        (QuorumType qType, uint256 reqWeight, bool allStew, bool custMaj, uint256 totalW)
            = _quorumParams(_pType, _did);

        proposalCount++;
        proposals[proposalCount] = Proposal({
            id: proposalCount,
            pType: _pType,
            proposer: msg.sender,
            datasetId: _did,
            targetAddress: _target,
            metadata: _metadata,
            status: ProposalStatus.PENDING,
            quorumType: qType,
            createdAt: block.timestamp,
            votingDeadline: block.timestamp + _votingDurationSecs,
            yesWeight: 0,
            noWeight: 0,
            totalWeight: totalW,
            requiredWeight: reqWeight,
            stewardYes: 0,
            stewardNo: 0,
            custodianYes: 0,
            custodianNo: 0,
            requireAllStewards: allStew,
            requireCustodianMajority: custMaj,
            quorumCertificate: bytes32(0)
        });

        emit ProposalCreated(proposalCount, _pType, qType, _did, _target);
        return proposalCount;
    }

    /// @notice Cast a weighted vote on a proposal (C2)
    function vote(uint256 _pid, bool _approve) external {
        Proposal storage p = proposals[_pid];
        require(p.status == ProposalStatus.PENDING, "Not pending");
        require(block.timestamp <= p.votingDeadline, "Voting ended");
        require(!hasVoted[_pid][msg.sender], "Already voted");

        uint256 weight = 0;
        Role voterRole = roles[msg.sender];

        if (isSteward[msg.sender]) {
            weight = WEIGHT_STEWARD;
            if (_approve) p.stewardYes++; else p.stewardNo++;
        } else if (p.datasetId > 0 && isCustodian[p.datasetId][msg.sender]) {
            weight = WEIGHT_CUSTODIAN;
            if (_approve) p.custodianYes++; else p.custodianNo++;
        } else {
            revert("Not authorized to vote");
        }

        hasVoted[_pid][msg.sender] = true;
        voteChoice[_pid][msg.sender] = _approve;

        if (_approve) {
            p.yesWeight += weight;
        } else {
            p.noWeight += weight;
        }

        emit Voted(_pid, msg.sender, _approve, weight, voterRole);
        _tryFinalize(_pid);
    }

    /// @notice WQC finalization with safety theorem check
    function _tryFinalize(uint256 _pid) internal {
        Proposal storage p = proposals[_pid];
        if (p.status != ProposalStatus.PENDING) return;

        uint256 cc = custodianCount[p.datasetId];

        // Check if approval conditions are met
        bool weightMet;
        if (p.quorumType == QuorumType.EMERGENCY) {
            weightMet = p.stewardYes >= 1;
        } else {
            weightMet = p.yesWeight >= p.requiredWeight;
        }

        bool stewardsMet = !p.requireAllStewards || (p.stewardYes >= stewardCount);
        bool custodiansMet = !p.requireCustodianMajority || (cc == 0) || (p.custodianYes * 2 > cc);

        // Check if rejection is inevitable
        uint256 remStewardW = (stewardCount - p.stewardYes - p.stewardNo) * WEIGHT_STEWARD;
        uint256 remCustW = (cc - p.custodianYes - p.custodianNo) * WEIGHT_CUSTODIAN;
        bool canReachWeight = (p.yesWeight + remStewardW + remCustW) >= p.requiredWeight;
        bool stewardsCan = !p.requireAllStewards || (p.stewardNo == 0);
        bool custodiansCan = !p.requireCustodianMajority || (cc == 0) ||
            ((p.custodianYes + (cc - p.custodianYes - p.custodianNo)) * 2 > cc);

        bool isRejected = !canReachWeight || !stewardsCan || !custodiansCan;
        bool isApproved = weightMet && stewardsMet && custodiansMet;

        if (isApproved) {
            // Generate Quorum Certificate: H(proposal_id ‖ outcome ‖ yesW ‖ noW ‖ timestamp)
            bytes32 qc = keccak256(abi.encodePacked(
                p.id, "APPROVED", p.yesWeight, p.noWeight, p.totalWeight, block.timestamp
            ));
            p.quorumCertificate = qc;
            p.status = ProposalStatus.EXECUTED;
            _executeProposal(_pid);
            emit ProposalFinalized(_pid, ProposalStatus.EXECUTED, qc);
        } else if (isRejected) {
            bytes32 qc = keccak256(abi.encodePacked(
                p.id, "REJECTED", p.yesWeight, p.noWeight, p.totalWeight, block.timestamp
            ));
            p.quorumCertificate = qc;
            p.status = ProposalStatus.REJECTED;
            emit ProposalFinalized(_pid, ProposalStatus.REJECTED, qc);
        }
    }

    function finalizeExpired(uint256 _pid) external {
        Proposal storage p = proposals[_pid];
        require(p.status == ProposalStatus.PENDING, "Not pending");
        require(block.timestamp > p.votingDeadline, "Not expired");
        bytes32 qc = keccak256(abi.encodePacked(p.id, "EXPIRED", p.yesWeight, p.noWeight, block.timestamp));
        p.quorumCertificate = qc;
        p.status = ProposalStatus.EXPIRED;
        emit ProposalFinalized(_pid, ProposalStatus.EXPIRED, qc);
    }

    // ═══════════════════════════════════════════
    // PROPOSAL EXECUTION
    // ═══════════════════════════════════════════
    function _executeProposal(uint256 _pid) internal {
        Proposal storage p = proposals[_pid];

        if (p.pType == ProposalType.ASSIGN_CUSTODIAN) {
            _assignCustodian(p.datasetId, p.targetAddress);
        } else if (p.pType == ProposalType.ONBOARD_ANALYST) {
            _onboardAnalyst(p.datasetId, p.targetAddress);
        } else if (p.pType == ProposalType.ACCESS_GRANT) {
            _grantAccess(p.datasetId, p.targetAddress, p.metadata);
        } else if (p.pType == ProposalType.REVOKE_CUSTODIAN) {
            _revokeCustodian(p.datasetId, p.targetAddress);
        } else if (p.pType == ProposalType.REVOKE_ANALYST) {
            _revokeAnalyst(p.datasetId, p.targetAddress);
        } else if (p.pType == ProposalType.ATTACH_POLICY) {
            // Metadata contains policyId — parsed off-chain, stored on-chain
        } else if (p.pType == ProposalType.TOGGLE_CONFIDENTIAL) {
            datasets[p.datasetId].isConfidential = !datasets[p.datasetId].isConfidential;
        }
    }

    function _assignCustodian(uint256 _did, address _cust) internal {
        require(!isCustodian[_did][_cust], "Already custodian");
        datasetCustodians[_did].push(_cust);
        isCustodian[_did][_cust] = true;
        custodianCount[_did]++;
        if (roles[_cust] == Role.NONE) roles[_cust] = Role.DATA_CUSTODIAN;
        _registerUser(_cust);

        accessGrants[_cust][_did] = AccessGrant({
            datasetId: _did, grantee: _cust, allowedColumns: "", rowFilter: "",
            level: AccessLevel.VIEW_DOWNLOAD, grantedAt: block.timestamp, expiresAt: 0, active: true
        });
        emit CustodianAssigned(_did, _cust);
        emit AccessGranted(_cust, _did, AccessLevel.VIEW_DOWNLOAD, 0);
    }

    function _revokeCustodian(uint256 _did, address _cust) internal {
        require(isCustodian[_did][_cust], "Not custodian");
        isCustodian[_did][_cust] = false;
        custodianCount[_did]--;
        accessGrants[_cust][_did].active = false;
        emit CustodianRevoked(_did, _cust);
        emit AccessRevoked(_cust, _did);
    }

    function _onboardAnalyst(uint256 _did, address _analyst) internal {
        if (roles[_analyst] == Role.NONE) roles[_analyst] = Role.ANALYST;
        _registerUser(_analyst);
        datasetAnalysts[_did].push(_analyst);

        AccessLevel lvl = datasets[_did].isConfidential ? AccessLevel.VIEW_ONLY : AccessLevel.VIEW_DOWNLOAD;
        accessGrants[_analyst][_did] = AccessGrant({
            datasetId: _did, grantee: _analyst, allowedColumns: "", rowFilter: "",
            level: lvl, grantedAt: block.timestamp, expiresAt: 0, active: true
        });
        emit AccessGranted(_analyst, _did, lvl, 0);
    }

    function _grantAccess(uint256 _did, address _grantee, string memory _meta) internal {
        AccessLevel lvl = datasets[_did].isConfidential ? AccessLevel.VIEW_ONLY : AccessLevel.VIEW_DOWNLOAD;
        accessGrants[_grantee][_did] = AccessGrant({
            datasetId: _did, grantee: _grantee, allowedColumns: _meta, rowFilter: "",
            level: lvl, grantedAt: block.timestamp, expiresAt: block.timestamp + 30 days, active: true
        });
        _registerUser(_grantee);
        emit AccessGranted(_grantee, _did, lvl, block.timestamp + 30 days);
    }

    function _revokeAnalyst(uint256 _did, address _analyst) internal {
        accessGrants[_analyst][_did].active = false;
        emit AccessRevoked(_analyst, _did);
    }

    // ═══════════════════════════════════════════
    // COMPLIANCE
    // ═══════════════════════════════════════════
    function createPolicy(
        string calldata _name,
        ComplianceStandard _std,
        string calldata _rulesJson
    ) external onlySteward returns (uint256) {
        policyCount++;
        policies[policyCount] = CompliancePolicy({
            id: policyCount, name: _name, standard: _std,
            rulesJson: _rulesJson, active: true, createdAt: block.timestamp
        });
        emit PolicyCreated(policyCount, _name, _std);
        return policyCount;
    }

    function attachPolicyToDataset(uint256 _did, uint256 _pid) external onlySteward datasetActive(_did) {
        require(policies[_pid].active, "Policy inactive");
        datasetPolicies[_did].push(_pid);
        emit PolicyAttached(_did, _pid);
    }

    /// @notice Record hash-linked compliance attestation (C1)
    function recordAttestation(
        uint256 _queryLogId,
        address _querier,
        uint256 _did,
        bool _passed,
        bytes32 _attestationHash
    ) external {
        require(isSteward[msg.sender] || isCustodian[_did][msg.sender], "Unauthorized");
        attestationCount++;
        attestations[attestationCount] = ComplianceAttestation({
            id: attestationCount,
            queryLogId: _queryLogId,
            querier: _querier,
            datasetId: _did,
            passed: _passed,
            attestationHash: _attestationHash,
            prevAttestation: lastAttestationHash,
            blockNum: block.number,
            timestamp: block.timestamp
        });
        lastAttestationHash = _attestationHash;
        emit ComplianceChecked(attestationCount, _did, _passed, _attestationHash);
    }

    // ═══════════════════════════════════════════
    // C3: SELF-SOVEREIGN IDENTITY
    // ═══════════════════════════════════════════

    /// @notice Register a data subject with DID
    function registerSubject(address _subject, string calldata _did) external onlySteward {
        require(roles[_subject] == Role.NONE, "Already has role");
        roles[_subject] = Role.SUBJECT;
        subjectDID[_subject] = _did;
        _registerUser(_subject);
        emit SubjectRegistered(_subject, _did);
    }

    /// @notice Append to SSI consent chain
    function _appendConsent(
        address _subject,
        string memory _action,
        address _counterparty,
        uint256 _did,
        string memory _scope
    ) internal returns (bytes32) {
        consentCount++;
        bytes32 ch = keccak256(abi.encodePacked(
            consentCount, _subject, _action, _counterparty, _did, _scope, consentChainHead, block.timestamp
        ));
        consents[consentCount] = ConsentRecord({
            id: consentCount, subject: _subject, action: _action,
            counterparty: _counterparty, datasetId: _did, scope: _scope,
            consentHash: ch, prevHash: consentChainHead, timestamp: block.timestamp
        });
        consentChainHead = ch;
        emit ConsentRecorded(consentCount, _subject, _action, ch, consents[consentCount].prevHash);
        return ch;
    }

    /// @notice Link subject to dataset with record filter
    function linkSubjectToDataset(
        address _subject,
        uint256 _did,
        string calldata _recordFilter
    ) external datasetActive(_did) {
        require(isSteward[msg.sender] || isCustodian[_did][msg.sender], "Unauthorized");
        require(roles[_subject] == Role.SUBJECT, "Not a subject");
        subjectRecordFilter[_subject][_did] = _recordFilter;

        AccessLevel lvl = datasets[_did].isConfidential ? AccessLevel.VIEW_ONLY : AccessLevel.VIEW_DOWNLOAD;
        accessGrants[_subject][_did] = AccessGrant({
            datasetId: _did, grantee: _subject, allowedColumns: "", rowFilter: _recordFilter,
            level: lvl, grantedAt: block.timestamp, expiresAt: 0, active: true
        });

        bytes32 ch = _appendConsent(_subject, "LINK", msg.sender, _did, _recordFilter);
        emit SubjectLinked(_subject, _did, ch);
        emit AccessGranted(_subject, _did, lvl, 0);
    }

    /// @notice Subject delegates access to another address (C3: no intermediary)
    function createDelegation(
        address _delegate,
        uint256 _did,
        string calldata _scope,
        uint256 _durationSecs
    ) external datasetActive(_did) {
        require(roles[msg.sender] == Role.SUBJECT, "Not a subject");
        require(bytes(subjectRecordFilter[msg.sender][_did]).length > 0, "Not linked");

        delegationCount++;
        uint256 exp = block.timestamp + _durationSecs;
        delegations[delegationCount] = SubjectDelegation({
            id: delegationCount, subject: msg.sender, delegate: _delegate,
            datasetId: _did, scope: _scope, expiresAt: exp, active: true
        });
        subjectDelegationIds[msg.sender].push(delegationCount);

        AccessLevel lvl = datasets[_did].isConfidential ? AccessLevel.VIEW_ONLY : AccessLevel.VIEW_DOWNLOAD;
        accessGrants[_delegate][_did] = AccessGrant({
            datasetId: _did, grantee: _delegate, allowedColumns: _scope,
            rowFilter: subjectRecordFilter[msg.sender][_did],
            level: lvl, grantedAt: block.timestamp, expiresAt: exp, active: true
        });
        _registerUser(_delegate);

        bytes32 ch = _appendConsent(msg.sender, "DELEGATE", _delegate, _did, _scope);
        emit DelegationCreated(delegationCount, msg.sender, _delegate, ch);
        emit AccessGranted(_delegate, _did, lvl, exp);
    }

    /// @notice Subject revokes delegation (C3: steward-independent)
    function revokeDelegation(uint256 _delId) external {
        SubjectDelegation storage d = delegations[_delId];
        require(d.subject == msg.sender, "Not your delegation");
        d.active = false;
        accessGrants[d.delegate][d.datasetId].active = false;

        bytes32 ch = _appendConsent(msg.sender, "REVOKE", d.delegate, d.datasetId, "");
        emit DelegationRevoked(_delId, ch);
        emit AccessRevoked(d.delegate, d.datasetId);
    }

    // ═══════════════════════════════════════════
    // QUERY AUDIT LOG
    // ═══════════════════════════════════════════
    function logQuery(
        address _querier,
        uint256 _did,
        bytes32 _queryHash,
        bytes32 _resultHash,
        bytes32 _merkleRoot,
        bool _compliancePassed
    ) external returns (uint256) {
        require(isSteward[msg.sender] || isCustodian[_did][msg.sender], "Unauthorized");
        queryLogCount++;
        bytes32 att = keccak256(abi.encodePacked(_queryHash, _resultHash, _merkleRoot, block.timestamp));
        queryLogs[queryLogCount] = QueryLog({
            id: queryLogCount, querier: _querier, datasetId: _did,
            queryHash: _queryHash, resultHash: _resultHash, merkleRoot: _merkleRoot,
            attestation: att, compliancePassed: _compliancePassed,
            blockNum: block.number, timestamp: block.timestamp
        });
        emit QueryLogged(queryLogCount, _querier, _did, att);
        return queryLogCount;
    }

    // ═══════════════════════════════════════════
    // VIEW FUNCTIONS
    // ═══════════════════════════════════════════
    function getDatasetIds() external view returns (uint256[] memory) { return datasetIds; }
    function getDatasetCustodians(uint256 _did) external view returns (address[] memory) { return datasetCustodians[_did]; }
    function getDatasetAnalysts(uint256 _did) external view returns (address[] memory) { return datasetAnalysts[_did]; }
    function getDatasetPolicies(uint256 _did) external view returns (uint256[] memory) { return datasetPolicies[_did]; }
    function getMerkleHistory(uint256 _did) external view returns (bytes32[] memory) { return merkleHistory[_did]; }
    function getSubjectDelegations(address _subj) external view returns (uint256[] memory) { return subjectDelegationIds[_subj]; }
    function getStewards() external view returns (address[] memory) { return stewards; }
    function getAllUsers() external view returns (address[] memory) { return allUsers; }

    function getWeights() external pure returns (uint256 s, uint256 c, uint256 a, uint256 sub) {
        return (WEIGHT_STEWARD, WEIGHT_CUSTODIAN, WEIGHT_ANALYST, WEIGHT_SUBJECT);
    }

    function checkAccess(address _user, uint256 _did) external view returns (
        bool hasAccess, AccessLevel level, string memory columns, string memory filter, uint256 expiresAt
    ) {
        AccessGrant memory g = accessGrants[_user][_did];
        if (!g.active) return (false, AccessLevel.NONE_ACCESS, "", "", 0);
        if (g.expiresAt > 0 && block.timestamp > g.expiresAt) return (false, AccessLevel.NONE_ACCESS, "", "", 0);
        return (true, g.level, g.allowedColumns, g.rowFilter, g.expiresAt);
    }

    function getProposalWQC(uint256 _pid) external view returns (
        uint256 yesW, uint256 noW, uint256 totalW, uint256 reqW,
        uint256 sYes, uint256 sNo, uint256 cYes, uint256 cNo,
        QuorumType qType, bytes32 qc
    ) {
        Proposal memory p = proposals[_pid];
        return (p.yesWeight, p.noWeight, p.totalWeight, p.requiredWeight,
                p.stewardYes, p.stewardNo, p.custodianYes, p.custodianNo,
                p.quorumType, p.quorumCertificate);
    }

    function getConsentChainHead() external view returns (bytes32) { return consentChainHead; }
    function getConsentCount() external view returns (uint256) { return consentCount; }
}
