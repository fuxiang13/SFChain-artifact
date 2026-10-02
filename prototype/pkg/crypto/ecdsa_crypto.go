package crypto

// ECDSA endorsement signatures (secp256k1) - used for transaction-level endorsement verification
// Independent of the BLS block aggregate signature: BLS is used only for node-level block signature aggregation

import (
	"encoding/hex"
	"fmt"
	"log"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
)

// ECDSAGenerateKeyPair generates a secp256k1 key pair, returning (private key hex, public key hex)
func ECDSAGenerateKeyPair() (string, string, error) {
	key, err := ethcrypto.GenerateKey()
	if err != nil {
		return "", "", fmt.Errorf("failed to generate ECDSA key: %v", err)
	}
	privHex := hex.EncodeToString(ethcrypto.FromECDSA(key))
	pubBytes := ethcrypto.FromECDSAPub(&key.PublicKey)
	pubHex := hex.EncodeToString(pubBytes)
	return privHex, pubHex, nil
}

// ECDSASign signs a message with the private key, returning the signature hex (65 bytes: r(32)+s(32)+v(1))
func ECDSASign(privateKeyHex string, message []byte) string {
	key, err := ethcrypto.HexToECDSA(privateKeyHex)
	if err != nil {
		log.Printf("ECDSA private key parse failed: %v", err)
		return ""
	}
	hash := ethcrypto.Keccak256(message)
	sig, err := ethcrypto.Sign(hash, key)
	if err != nil {
		log.Printf("ECDSA signing failed: %v", err)
		return ""
	}
	return hex.EncodeToString(sig)
}

// ECDSAVerify verifies a signature and returns whether it is valid
func ECDSAVerify(publicKeyHex string, message []byte, signatureHex string) bool {
	pubBytes, err := hex.DecodeString(publicKeyHex)
	if err != nil {
		return false
	}
	sigBytes, err := hex.DecodeString(signatureHex)
	if err != nil || len(sigBytes) < 64 {
		return false
	}
	hash := ethcrypto.Keccak256(message)
	return ethcrypto.VerifySignature(pubBytes, hash, sigBytes[:64])
}

// ECDSAAddressFromPrivateKey derives the address from the private key (for debugging)
func ECDSAAddressFromPrivateKey(privateKeyHex string) string {
	key, err := ethcrypto.HexToECDSA(privateKeyHex)
	if err != nil {
		return ""
	}
	return ethcrypto.PubkeyToAddress(key.PublicKey).Hex()
}
