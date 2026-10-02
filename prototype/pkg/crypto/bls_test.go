package crypto

import (
	"testing"

	bls "github.com/herumi/bls-eth-go-binary/bls"
)

func TestBLSSignAndVerify(t *testing.T) {
	secKey, pubKey, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	message := []byte("test message for BLS signature")

	sig := Sign(secKey, message)
	if !Verify(pubKey, message, sig) {
		t.Fatal("Signature verification failed")
	}

	t.Log("Single sign and verify succeeded")
}

func TestBLSAggregateSignAndVerify(t *testing.T) {
	secKey1, pubKey1, _ := GenerateKeyPair()
	secKey2, pubKey2, _ := GenerateKeyPair()
	secKey3, pubKey3, _ := GenerateKeyPair()
	secKey4, pubKey4, _ := GenerateKeyPair()

	message := []byte("test message for BLS aggregate signature")

	sig1 := Sign(secKey1, message)
	sig2 := Sign(secKey2, message)
	sig3 := Sign(secKey3, message)
	sig4 := Sign(secKey4, message)

	aggSig := AggregateSignatures([]*bls.Sign{sig1, sig2, sig3, sig4})
	aggPubKey := AggregatePublicKeys([]*bls.PublicKey{pubKey1, pubKey2, pubKey3, pubKey4})

	if !VerifyAggregatedSignature(aggPubKey, message, aggSig) {
		t.Fatal("Aggregate signature verification failed")
	}

	t.Log("Aggregate sign and verify succeeded")
}

func TestBLSAggregateSignWithSerializedKeys(t *testing.T) {
	secKey1, pubKey1, _ := GenerateKeyPair()
	secKey2, pubKey2, _ := GenerateKeyPair()

	message := []byte("test message for BLS aggregate signature")

	sig1 := Sign(secKey1, message)
	sig2 := Sign(secKey2, message)

	sig1Hex := SerializeSignature(sig1)
	sig2Hex := SerializeSignature(sig2)

	pubKey1Hex := SerializePublicKey(pubKey1)
	pubKey2Hex := SerializePublicKey(pubKey2)

	sig1Deserialized, _ := DeserializeSignature(sig1Hex)
	sig2Deserialized, _ := DeserializeSignature(sig2Hex)

	pubKey1Deserialized, _ := DeserializePublicKey(pubKey1Hex)
	pubKey2Deserialized, _ := DeserializePublicKey(pubKey2Hex)

	aggSig := AggregateSignatures([]*bls.Sign{sig1Deserialized, sig2Deserialized})
	aggPubKey := AggregatePublicKeys([]*bls.PublicKey{pubKey1Deserialized, pubKey2Deserialized})

	if !VerifyAggregatedSignature(aggPubKey, message, aggSig) {
		t.Fatal("Aggregate signature verification failed with serialized keys")
	}

	t.Log("Aggregate sign and verify with serialized keys succeeded")
}
