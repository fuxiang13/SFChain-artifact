package main

import (
	"encoding/hex"
	"fmt"
	"log"

	bls "github.com/herumi/bls-eth-go-binary/bls"
)

func init() {
	if err := bls.Init(bls.BLS12_381); err != nil {
		log.Fatalf("Failed to initialize BLS library: %v", err)
	}
	if err := bls.SetETHmode(bls.EthModeDraft07); err != nil {
		log.Fatalf("Failed to set ETH mode: %v", err)
	}
}

func main() {
	nodeNames := []string{"management-node-1", "development-node-1", "test-node-1", "operations-node-1"}

	fmt.Println("Generating BLS key pairs for the following nodes:")
	fmt.Println("========================================")

	for _, nodeName := range nodeNames {
		secretKey := bls.SecretKey{}
		secretKey.SetByCSPRNG()

		publicKey := secretKey.GetPublicKey()

		fmt.Printf("\n%s:\n", nodeName)
		fmt.Printf("  private_key: \"%s\"\n", hex.EncodeToString(secretKey.Serialize()))
		fmt.Printf("  public_key: \"%s\"\n", hex.EncodeToString(publicKey.Serialize()))
	}

	fmt.Println("\n========================================")
	fmt.Println("Update the corresponding node configuration files with these key pairs")
}
