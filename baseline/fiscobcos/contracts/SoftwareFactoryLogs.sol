// SPDX-License-Identifier: MIT
pragma solidity ^0.8.11;

/**
 * SoftwareFactoryLogs - FISCO BCOS smart contract equivalent to SFChain attestation
 *
 * Corresponds to one log attestation row of the SFChain software_factory_logs table:
 *   id / category / timestamp / level / message / user_id /
 *   module / project / operation / status
 *
 * Attestation model (aligned with SFChain's "log -> transaction -> block" semantics):
 *   - Each log calls recordLog(...) once, i.e. one on-chain transaction;
 *     the full raw fields are permanently on chain with the tx calldata
 *     (retrievable via getTransactionByHash)
 *   - State storage: record index -> content digest (keccak256), equivalent to an
 *     on-chain verifiable attestation fingerprint
 *   - Event: LogRecorded carries the digest and key fields, persisted in the tx
 *     receipt, usable for indexing and verification
 *
 * Digest computation (segmented hashing to avoid stack-depth limits):
 *   h1 = keccak256(id, category, timestamp)
 *   h2 = keccak256(level, message, userId)
 *   h3 = keccak256(module, project, operation, status)
 *   digest = keccak256(h1, h2, h3)
 */
contract SoftwareFactoryLogs {
    /// @notice One software-factory operation log (fields identical to the SFChain software_factory_logs table)
    struct LogRecord {
        string id;        // unique log ID (32-char hex)
        string category;  // management / development / test / operations
        uint64 timestamp; // millisecond Unix timestamp
        string level;     // info / warning / error
        string message;   // log message
        string userId;    // operating user
        string module;    // module name
        string project;   // project name
        string operation; // operation type
        string status;    // success / failed
        string endorsement; // endorsement evidence JSON (isomorphic to SFChain Endorsement: user_id/public_key/signature/timestamp/role)
    }

    struct Meta {
        uint256 recordCount; // cumulative attestation count
        uint256 firstTsMs;   // first attested business timestamp (ms)
        uint256 lastTsMs;    // last attested business timestamp (ms)
    }

    Meta public meta;

    // index => content digest (attestation fingerprint)
    mapping(uint256 => bytes32) public recordDigests;
    // log-id digest => index (for lookup by id)
    mapping(bytes32 => uint256) public indexOfId;
    // log-id digest => endorsement evidence JSON (endorsement signature goes on chain with the tx, independently verifiable)
    mapping(bytes32 => string) public endorsementOf;

    event LogRecorded(
        uint256 indexed index,
        bytes32 indexed idHash,
        bytes32 digest,
        string category,
        uint64 timestamp,
        string operation
    );

    /**
     * Attest one software-factory operation log (corresponds to one SFChain log transaction)
     */
    function recordLog(LogRecord calldata log) external {
        _recordLog(log);
    }

    /**
     * Attest a log with a dynamic number of application endorsements.
     *
     * The contract deliberately does not require a fixed four-signature
     * policy. Every supplied signature is checked; the caller may submit one
     * or more endorsements according to the deployment policy.
     */
    function recordLogChecked(
        LogRecord calldata log,
        bytes32 messageHash,
        address[] calldata signers,
        bytes[] calldata signatures
    ) external {
        require(signers.length > 0, "at least one endorsement required");
        require(signers.length == signatures.length, "endorsement length mismatch");
        require(messageHash == endorsementHash(log), "message hash mismatch");
        for (uint256 i = 0; i < signatures.length; i++) {
            require(signatures[i].length == 65, "invalid endorsement signature");
            bytes32 r;
            bytes32 s;
            uint8 v;
            bytes calldata signature = signatures[i];
            assembly {
                r := calldataload(signature.offset)
                s := calldataload(add(signature.offset, 32))
                v := byte(0, calldataload(add(signature.offset, 64)))
            }
            if (v < 27) {
                v += 27;
            }
            require(
                ecrecover(messageHash, v, r, s) == signers[i],
                "invalid endorsement"
            );
        }
        _recordLog(log);
    }

    /// @notice Recompute the application-endorsement digest from the log fields.
    /// The endorsement JSON is deliberately excluded because it contains the
    /// signature over this digest itself.
    function endorsementHash(LogRecord calldata log) public pure returns (bytes32) {
        bytes32 h1 = keccak256(abi.encodePacked(
            log.id, "|", log.category, "|", log.timestamp
        ));
        bytes32 h2 = keccak256(abi.encodePacked(
            log.level, "|", log.message, "|", log.userId
        ));
        bytes32 h3 = keccak256(abi.encodePacked(
            log.module, "|", log.project, "|", log.operation, "|", log.status
        ));
        return keccak256(abi.encodePacked(h1, h2, h3));
    }

    function _recordLog(LogRecord calldata log) internal {
        uint256 index = meta.recordCount + 1;
        bytes32 idHash = keccak256(bytes(log.id));

        recordDigests[index] = digestOf(log);
        indexOfId[idHash] = index;
        endorsementOf[idHash] = log.endorsement;

        if (index == 1) {
            meta.firstTsMs = log.timestamp;
        }
        meta.lastTsMs = log.timestamp;
        meta.recordCount = index;

        emit LogRecorded(index, idHash, recordDigests[index], log.category, log.timestamp, log.operation);
    }

    /// @notice Compute the keccak256 digest of the whole log in segments (endorsement evidence included in the fingerprint)
    function digestOf(LogRecord calldata log) public pure returns (bytes32) {
        bytes32 h1 = keccak256(abi.encodePacked(log.id, log.category, log.timestamp));
        bytes32 h2 = keccak256(abi.encodePacked(log.level, log.message, log.userId));
        bytes32 h3 = keccak256(abi.encodePacked(log.module, log.project, log.operation, log.status));
        bytes32 h4 = keccak256(bytes(log.endorsement));
        return keccak256(abi.encodePacked(h1, h2, h3, h4));
    }

    /// @notice Query the on-chain endorsement evidence JSON by log id
    function getEndorsement(string calldata id) external view returns (string memory) {
        return endorsementOf[keccak256(bytes(id))];
    }

    /// @notice Query the cumulative attestation count
    function getRecordCount() external view returns (uint256) {
        return meta.recordCount;
    }

    /// @notice Query the attestation time range and count (first ts, last ts, count)
    function getMeta() external view returns (uint256 count, uint256 firstTsMs, uint256 lastTsMs) {
        return (meta.recordCount, meta.firstTsMs, meta.lastTsMs);
    }

    /// @notice Query the attestation digest by index
    function getRecordDigest(uint256 index) external view returns (bytes32) {
        return recordDigests[index];
    }

    /// @notice Query the attestation index by log id (0 means not found)
    function getIndexOfId(string calldata id) external view returns (uint256) {
        return indexOfId[keccak256(bytes(id))];
    }
}
