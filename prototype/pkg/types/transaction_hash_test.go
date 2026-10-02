package types

import (
	"encoding/json"
	"testing"
)

func TestMillisecondLogTimestampSurvivesJSONRoundTrip(t *testing.T) {
	tx := Transaction{UserID: "u", LogData: map[string]interface{}{
		"timestamp": int64(1790838893266), "user_id": "u", "category": "test",
	}}
	tx.TXID = tx.CalculateTXID()
	raw, err := json.Marshal(tx)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Transaction
	if err = json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.CalculateTXID() != tx.TXID {
		t.Fatal("canonical timestamp changed hash")
	}
}
