package core

import (
	bls "github.com/herumi/bls-eth-go-binary/bls"
	"sfchain/pkg/crypto"
	"sfchain/pkg/types"
	"testing"
)

func TestSignatureCollectionAuthenticatesRosterAndMessage(t *testing.T) {
	var sk, other bls.SecretKey
	sk.SetByCSPRNG()
	other.SetByCSPRNG()
	header := &types.BlockHeader{BlockHeight: 1, ChainType: 1, Timestamp: 100}
	digest := header.CalculateHash()
	pub := crypto.SerializePublicKey(sk.GetPublicKey())
	n := &ManagementNode{
		BaseNode: BaseNode{NodeID: "m", NetworkNodes: map[string]*types.NodeInfo{
			"m": {NodeID: "m"}, "d": {NodeID: "d", PublicKey: pub},
			"t": {NodeID: "t"}, "o": {NodeID: "o"},
		}},
		blockCache:     map[string]*types.Block{digest: {Header: header}},
		signatureCache: map[string]map[string]*bls.Sign{digest: {}},
		publicKeyCache: map[string]map[string]*bls.PublicKey{digest: {}},
	}
	sig := crypto.SerializeSignature(sk.SignByte([]byte(digest)))
	n.ReceiveSignature(1, digest, "unknown", sig, pub)
	n.ReceiveSignature(1, digest, "d", sig, crypto.SerializePublicKey(other.GetPublicKey()))
	n.ReceiveSignature(1, digest, "d", crypto.SerializeSignature(sk.SignByte([]byte("wrong"))), pub)
	n.ReceiveSignature(2, digest, "d", sig, pub)
	if len(n.signatureCache[digest]) != 0 {
		t.Fatal("unauthenticated signature counted")
	}
	n.ReceiveSignature(1, digest, "d", sig, pub)
	n.ReceiveSignature(1, digest, "d", sig, pub)
	if len(n.signatureCache[digest]) != 1 {
		t.Fatal("valid signature rejected or duplicate counted")
	}
}

func TestEndorsementChecksContentNotOnlyStoredTXID(t *testing.T) {
	priv, pub, err := crypto.ECDSAGenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	tx := &types.Transaction{UserID: "u", TxType: 1,
		LogData: map[string]interface{}{"user_id": "u", "category": "management", "message": "original"}}
	tx.TXID = tx.CalculateTXID()
	tx.Endorsements = []types.Endorsement{{UserID: "u", PublicKey: pub, Signature: crypto.ECDSASign(priv, []byte(tx.TXID))}}
	n := &ManagementNode{TxPoolDB: &TransactionPoolDB{userPublicKeys: map[string]string{"u": pub}}}
	if !n.verifyTransactionUserSignature(tx) {
		t.Fatal("valid endorsement rejected")
	}
	tx.LogData["message"] = "replacement"
	if n.verifyTransactionUserSignature(tx) {
		t.Fatal("unchanged TXID concealed changed content")
	}
}
