package main

// Source-level tests for evidence binding checks: covering matching inputs that
// pass, a self-consistent replacement block paired with an unrelated header chain,
// altered metadata, a record set not anchored to the role-retained root, and a
// header chain that does not cover the height.

import (
	"strings"
	"testing"

	"sfchain/pkg/types"
)

func mkTx(id string) types.Transaction {
	// CalculateTXID() recomputes from the transaction content (independent of the
	// TXID field), so each test transaction must carry distinct content fields for
	// its recomputed hash and Merkle root to differ
	return types.Transaction{TXID: id, UserID: "user-" + id}
}

// matching: the custodian block matches the role-retained header field by field,
// and the record set anchors to the role-retained root
func TestEvidenceBindingMatching(t *testing.T) {
	txs := []types.Transaction{mkTx("tx-1"), mkTx("tx-2")}
	root := merkleRootOf(txs)
	target := headerRow{BlockHash: "H1", PrevHash: "H0", Height: 3, Timestamp: 1.7e12,
		MerkleRoot: root, ChainType: 2}
	tgtH := target // identical role-retained header
	if err := verifyEvidenceBinding(target, tgtH, txs); err != nil {
		t.Fatalf("matching inputs must pass, got: %v", err)
	}
}

// replacement block, self-consistent but unrelated header: a custodian block whose
// own merkle root matches its own records, but whose header fields differ from the
// role-retained header, must be rejected.
func TestEvidenceBindingReplacementBlockRejected(t *testing.T) {
	roleTxs := []types.Transaction{mkTx("role-1"), mkTx("role-2")}
	root := merkleRootOf(roleTxs)
	tgtH := headerRow{BlockHash: "H1", PrevHash: "H0", Height: 3, Timestamp: 1.7e12,
		MerkleRoot: root, ChainType: 2}
	// attacker block: self-consistent (its merkle root matches its own records)
	fakeTxs := []types.Transaction{mkTx("fake-1"), mkTx("fake-2")}
	target := headerRow{BlockHash: "H1-FAKE", PrevHash: "H0", Height: 3, Timestamp: 1.7e12,
		MerkleRoot: merkleRootOf(fakeTxs), ChainType: 2}
	if err := verifyEvidenceBinding(target, tgtH, fakeTxs); err == nil {
		t.Fatal("self-consistent replacement block with unrelated header must be rejected")
	}
}

// hash-interpretation tolerance: the deployed chain carries two coexisting
// block-hash interpretations (custodian-side and role-side, each with its own
// role aggregate signature over it), so binding is over the consensus content
// fields and the role-retained merkle root; a differing stored-hash string alone
// (with all content fields and the record set intact) must still pass — the role
// hash itself is signature-anchored separately by the BLS step.
func TestEvidenceBindingHashInterpretationTolerated(t *testing.T) {
	txs := []types.Transaction{mkTx("tx-1")}
	root := merkleRootOf(txs)
	tgtH := headerRow{BlockHash: "H1", PrevHash: "H0", Height: 3, Timestamp: 1.7e12,
		MerkleRoot: root, ChainType: 2}
	target := tgtH
	target.BlockHash = "H1-OTHER-INTERPRETATION"
	if err := verifyEvidenceBinding(target, tgtH, txs); err != nil {
		t.Fatalf("content-bound inputs with the alternate hash interpretation must pass, got: %v", err)
	}
}

// altered metadata: every header field must bind; tamper each one in turn
func TestEvidenceBindingAlteredMetadataRejected(t *testing.T) {
	txs := []types.Transaction{mkTx("tx-1")}
	root := merkleRootOf(txs)
	base := headerRow{BlockHash: "H1", PrevHash: "H0", Height: 3, Timestamp: 1.7e12,
		MerkleRoot: root, ChainType: 2}
	tampered := []headerRow{
		{BlockHash: "H1", PrevHash: "H9", Height: 3, Timestamp: 1.7e12, MerkleRoot: root, ChainType: 2},
		{BlockHash: "H1", PrevHash: "H0", Height: 4, Timestamp: 1.7e12, MerkleRoot: root, ChainType: 2},
		{BlockHash: "H1", PrevHash: "H0", Height: 3, Timestamp: 1.8e12, MerkleRoot: root, ChainType: 2},
		{BlockHash: "H1", PrevHash: "H0", Height: 3, Timestamp: 1.7e12, MerkleRoot: root, ChainType: 3},
		{BlockHash: "H1", PrevHash: "H0", Height: 3, Timestamp: 1.7e12, MerkleRoot: "deadbeef", ChainType: 2},
	}
	for i, tgtH := range tampered {
		if err := verifyEvidenceBinding(base, tgtH, txs); err == nil {
			t.Fatalf("tampered metadata case %d must be rejected", i)
		}
	}
}

// record set must anchor to the role-retained root, not merely the custodian's claim:
// the custodian block and role header agree with each other, but the audited records
// do not match that root when recomputed.
func TestEvidenceBindingRecordSetMustAnchorToRoleRoot(t *testing.T) {
	roleTxs := []types.Transaction{mkTx("role-1")}
	root := merkleRootOf(roleTxs)
	target := headerRow{BlockHash: "H1", PrevHash: "H0", Height: 3, Timestamp: 1.7e12,
		MerkleRoot: root, ChainType: 2}
	tgtH := target
	// audited records differ from the role-retained set
	otherTxs := []types.Transaction{mkTx("other-1")}
	err := verifyEvidenceBinding(target, tgtH, otherTxs)
	if err == nil {
		t.Fatal("record set not anchoring to the role-retained root must be rejected")
	}
	if !strings.Contains(err.Error(), "role-retained merkle root") {
		t.Fatalf("expected role-root anchoring failure, got: %v", err)
	}
}

// missing input: the role header chain does not cover the height
func TestEvidenceBindingMissingHeaderRejected(t *testing.T) {
	txs := []types.Transaction{mkTx("tx-1")}
	target := headerRow{BlockHash: "H1", PrevHash: "H0", Height: 7, Timestamp: 1.7e12,
		MerkleRoot: merkleRootOf(txs), ChainType: 2}
	empty := headerRow{}
	if err := verifyEvidenceBinding(target, empty, txs); err == nil {
		t.Fatal("missing role header must be rejected")
	}
}
