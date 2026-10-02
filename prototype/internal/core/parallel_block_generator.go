package core

import (
	"log"
	"sfchain/pkg/types"
	"time"
)

type ParallelBlockGenerator struct {
	node      *ManagementNode
	chainType types.TransactionType
	running   bool
	stopChan  chan bool
}

func NewParallelBlockGenerator(node *ManagementNode, chainType types.TransactionType) *ParallelBlockGenerator {
	return &ParallelBlockGenerator{
		node:      node,
		chainType: chainType,
		stopChan:  make(chan bool),
	}
}

func (pg *ParallelBlockGenerator) Start() {
	if pg.running {
		return
	}

	pg.running = true
	go pg.generateBlocks()
}

func (pg *ParallelBlockGenerator) Stop() {
	if pg.running {
		pg.stopChan <- true
		pg.running = false
	}
}

func (pg *ParallelBlockGenerator) generateBlocks() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pg.tryGenerateBlock()
		case <-pg.stopChan:
			return
		}
	}
}

func (pg *ParallelBlockGenerator) tryGenerateBlock() {
	log.Printf("Attempting to generate a block for the %s chain", pg.chainType.String())
}
