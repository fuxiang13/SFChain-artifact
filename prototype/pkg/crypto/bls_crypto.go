package crypto

import (
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"time"

	bls "github.com/herumi/bls-eth-go-binary/bls"
)

// Initialize the BLS library
func init() {
	if err := bls.Init(bls.BLS12_381); err != nil {
		log.Fatalf("Failed to initialize BLS library: %v", err)
	}
	if err := bls.SetETHmode(bls.EthModeDraft07); err != nil {
		log.Fatalf("Failed to set ETH mode: %v", err)
	}
	// Precomputation parameters were already set up in Init
}

// Signature cache
var (
	signatureCache    = make(map[string]string)
	verificationCache = make(map[string]bool)
	cacheMutex        = &sync.RWMutex{}
	cacheExpiration   = 10 * time.Minute
)

// GenerateKeyPair generates a BLS key pair
func GenerateKeyPair() (*bls.SecretKey, *bls.PublicKey, error) {
	secretKey := &bls.SecretKey{}
	secretKey.SetByCSPRNG() // no rand.Reader argument needed

	publicKey := secretKey.GetPublicKey()
	return secretKey, publicKey, nil
}

// Sign signs a message with the private key
func Sign(secretKey *bls.SecretKey, message []byte) *bls.Sign {
	// Build the cache key
	cacheKey := hex.EncodeToString(message) + "_" + SerializeSecretKey(secretKey)

	// Check the cache
	cacheMutex.RLock()
	if sigHex, exists := signatureCache[cacheKey]; exists {
		cacheMutex.RUnlock()
		sig, err := DeserializeSignature(sigHex)
		if err == nil {
			return sig
		}
	}
	cacheMutex.RUnlock()

	// Generate the signature
	sign := secretKey.SignByte(message)

	// Cache the signature
	cacheMutex.Lock()
	signatureCache[cacheKey] = SerializeSignature(sign)
	cacheMutex.Unlock()

	return sign
}

// Verify verifies a single signature with the public key
func Verify(publicKey *bls.PublicKey, message []byte, signature *bls.Sign) bool {
	// Build the cache key
	cacheKey := hex.EncodeToString(message) + "_" + SerializePublicKey(publicKey) + "_" + SerializeSignature(signature)

	// Check the cache
	cacheMutex.RLock()
	if result, exists := verificationCache[cacheKey]; exists {
		cacheMutex.RUnlock()
		return result
	}
	cacheMutex.RUnlock()

	// Verify the signature
	result := signature.VerifyByte(publicKey, message)

	// Cache the verification result
	cacheMutex.Lock()
	verificationCache[cacheKey] = result
	cacheMutex.Unlock()

	return result
}

// AggregateSignatures aggregates multiple signatures
func AggregateSignatures(signatures []*bls.Sign) *bls.Sign {
	if len(signatures) == 0 {
		return nil
	}

	// Aggregate signatures in parallel
	if len(signatures) > 10 {
		return parallelAggregateSignatures(signatures)
	}

	aggSig := bls.Sign{}
	for _, sig := range signatures {
		aggSig.Add(sig)
	}
	return &aggSig
}

// parallelAggregateSignatures aggregates signatures in parallel
func parallelAggregateSignatures(signatures []*bls.Sign) *bls.Sign {
	const batchSize = 5
	batches := (len(signatures) + batchSize - 1) / batchSize
	batchResults := make([]*bls.Sign, batches)
	var wg sync.WaitGroup

	for i := 0; i < batches; i++ {
		wg.Add(1)
		go func(batchIdx int) {
			defer wg.Done()
			start := batchIdx * batchSize
			end := start + batchSize
			if end > len(signatures) {
				end = len(signatures)
			}

			batchSig := bls.Sign{}
			for j := start; j < end; j++ {
				batchSig.Add(signatures[j])
			}
			batchResults[batchIdx] = &batchSig
		}(i)
	}

	wg.Wait()

	// Aggregate the per-batch results
	finalSig := bls.Sign{}
	for _, sig := range batchResults {
		if sig != nil {
			finalSig.Add(sig)
		}
	}
	return &finalSig
}

// AggregatePublicKeys aggregates multiple public keys
func AggregatePublicKeys(publicKeys []*bls.PublicKey) *bls.PublicKey {
	if len(publicKeys) == 0 {
		return nil
	}

	// Aggregate public keys in parallel
	if len(publicKeys) > 10 {
		return parallelAggregatePublicKeys(publicKeys)
	}

	aggPubKey := bls.PublicKey{}
	for _, pubKey := range publicKeys {
		aggPubKey.Add(pubKey)
	}
	return &aggPubKey
}

// parallelAggregatePublicKeys aggregates public keys in parallel
func parallelAggregatePublicKeys(publicKeys []*bls.PublicKey) *bls.PublicKey {
	const batchSize = 5
	batches := (len(publicKeys) + batchSize - 1) / batchSize
	batchResults := make([]*bls.PublicKey, batches)
	var wg sync.WaitGroup

	for i := 0; i < batches; i++ {
		wg.Add(1)
		go func(batchIdx int) {
			defer wg.Done()
			start := batchIdx * batchSize
			end := start + batchSize
			if end > len(publicKeys) {
				end = len(publicKeys)
			}

			batchPubKey := bls.PublicKey{}
			for j := start; j < end; j++ {
				batchPubKey.Add(publicKeys[j])
			}
			batchResults[batchIdx] = &batchPubKey
		}(i)
	}

	wg.Wait()

	// Aggregate the per-batch results
	finalPubKey := bls.PublicKey{}
	for _, pubKey := range batchResults {
		if pubKey != nil {
			finalPubKey.Add(pubKey)
		}
	}
	return &finalPubKey
}

// VerifyAggregatedSignature verifies an aggregated signature
func VerifyAggregatedSignature(
	aggregatedPublicKey *bls.PublicKey,
	message []byte,
	aggregatedSignature *bls.Sign,
) bool {
	// Build the cache key
	cacheKey := "agg_" + hex.EncodeToString(message) + "_" + SerializePublicKey(aggregatedPublicKey) + "_" + SerializeSignature(aggregatedSignature)

	// Check the cache
	cacheMutex.RLock()
	if result, exists := verificationCache[cacheKey]; exists {
		cacheMutex.RUnlock()
		return result
	}
	cacheMutex.RUnlock()

	// Verify the signature
	result := aggregatedSignature.VerifyByte(aggregatedPublicKey, message)

	// Cache the verification result
	cacheMutex.Lock()
	verificationCache[cacheKey] = result
	cacheMutex.Unlock()

	return result
}

// BatchVerify verifies signatures in batch
func BatchVerify(publicKeys []*bls.PublicKey, messages [][]byte, signatures []*bls.Sign) bool {
	if len(publicKeys) != len(messages) || len(messages) != len(signatures) {
		return false
	}

	// Verify in parallel
	results := make([]bool, len(publicKeys))
	var wg sync.WaitGroup

	for i := range publicKeys {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = Verify(publicKeys[idx], messages[idx], signatures[idx])
		}(i)
	}

	wg.Wait()

	// Check all verification results
	for _, result := range results {
		if !result {
			return false
		}
	}

	return true
}

// SerializeSignature serializes a signature into a hex string
func SerializeSignature(signature *bls.Sign) string {
	return hex.EncodeToString(signature.Serialize())
}

// DeserializeSignature deserializes a signature from a hex string
func DeserializeSignature(hexStr string) (*bls.Sign, error) {
	data, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, err
	}

	sig := &bls.Sign{}
	if err := sig.Deserialize(data); err != nil {
		return nil, err
	}
	return sig, nil
}

// SerializePublicKey serializes a public key into a hex string
func SerializePublicKey(publicKey *bls.PublicKey) string {
	return hex.EncodeToString(publicKey.Serialize())
}

// DeserializePublicKey deserializes a public key from a hex string
func DeserializePublicKey(hexStr string) (*bls.PublicKey, error) {
	data, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, err
	}

	pubKey := &bls.PublicKey{}
	if err := pubKey.Deserialize(data); err != nil {
		return nil, err
	}
	return pubKey, nil
}

// SerializeSecretKey serializes a secret key into a hex string
func SerializeSecretKey(secretKey *bls.SecretKey) string {
	return hex.EncodeToString(secretKey.Serialize())
}

// DeserializeSecretKey deserializes a secret key from a hex string
func DeserializeSecretKey(hexStr string) (*bls.SecretKey, error) {
	data, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, err
	}

	secretKey := &bls.SecretKey{}
	if err := secretKey.Deserialize(data); err != nil {
		return nil, err
	}
	return secretKey, nil
}

// VerifyManagementSignature verifies the management node signature
func VerifyManagementSignature(
	managementPublicKeyStr string,
	blockHash string,
	signatureStr string,
) (bool, error) {
	managementPubKey, err := DeserializePublicKey(managementPublicKeyStr)
	if err != nil {
		return false, fmt.Errorf("failed to deserialize management public key: %v", err)
	}

	signature, err := DeserializeSignature(signatureStr)
	if err != nil {
		return false, fmt.Errorf("failed to deserialize signature: %v", err)
	}

	isValid := Verify(managementPubKey, []byte(blockHash), signature)
	return isValid, nil
}

func SignBlockHeader(secretKeyStr string, blockHash string) (string, error) {
	secretKey, err := DeserializeSecretKey(secretKeyStr)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize secret key: %v", err)
	}

	signature := Sign(secretKey, []byte(blockHash))
	return SerializeSignature(signature), nil
}

func AggregateSignaturesFromStrings(signatureStrs []string) (string, error) {
	signatures := make([]*bls.Sign, 0, len(signatureStrs))
	for _, sigStr := range signatureStrs {
		sig, err := DeserializeSignature(sigStr)
		if err != nil {
			return "", fmt.Errorf("failed to deserialize signature: %v", err)
		}
		signatures = append(signatures, sig)
	}

	aggSig := AggregateSignatures(signatures)
	if aggSig == nil {
		return "", fmt.Errorf("failed to aggregate signatures")
	}

	return SerializeSignature(aggSig), nil
}

// Periodically clean the cache
func init() {
	go func() {
		for {
			time.Sleep(cacheExpiration / 2)
			cacheMutex.Lock()
			signatureCache = make(map[string]string)
			verificationCache = make(map[string]bool)
			cacheMutex.Unlock()
		}
	}()
}
