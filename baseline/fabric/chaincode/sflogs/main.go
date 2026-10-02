package main

import (
	"fmt"

	"github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
)

// SFLogs is a minimal software-factory log store: one PutLog per log event,
// using the source-log identifier as the key.
type SFLogs struct {
	contractapi.Contract
}

// PutLog stores the payload under the log identifier.
func (s *SFLogs) PutLog(ctx contractapi.TransactionContextInterface, logID string, payload string) error {
	return ctx.GetStub().PutState(logID, []byte(payload))
}

// GetLog reads one record back.
func (s *SFLogs) GetLog(ctx contractapi.TransactionContextInterface, logID string) (string, error) {
	b, err := ctx.GetStub().GetState(logID)
	if err != nil {
		return "", fmt.Errorf("read %s: %v", logID, err)
	}
	if b == nil {
		return "", fmt.Errorf("log %s not found", logID)
	}
	return string(b), nil
}

func main() {
	cc, err := contractapi.NewChaincode(&SFLogs{})
	if err != nil {
		panic(err)
	}
	if err := cc.Start(); err != nil {
		panic(err)
	}
}
