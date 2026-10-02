package main

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// rq1EndorsementHash mirrors the Solidity evidence hash used by the checked
// RQ1 contract entry point.
func rq1EndorsementHash(rec LogRecord) common.Hash {
	segment := func(parts ...string) []byte {
		buf := make([]byte, 0, 128)
		for i, part := range parts {
			if i > 0 {
				buf = append(buf, '|')
			}
			buf = append(buf, part...)
		}
		return buf
	}

	h1Input := make([]byte, 0, 128)
	h1Input = append(h1Input, rec.Id...)
	h1Input = append(h1Input, '|')
	h1Input = append(h1Input, rec.Category...)
	h1Input = append(h1Input, '|')
	var timestamp [8]byte
	binary.BigEndian.PutUint64(timestamp[:], rec.Timestamp)
	h1Input = append(h1Input, timestamp[:]...)
	h1 := crypto.Keccak256(h1Input)
	h2 := crypto.Keccak256(segment(rec.Level, rec.Message, rec.UserId))
	h3 := crypto.Keccak256(segment(rec.Module, rec.Project, rec.Operation, rec.Status))
	return common.BytesToHash(crypto.Keccak256(append(append(h1, h2...), h3...)))
}
