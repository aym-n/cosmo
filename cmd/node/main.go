package main

import (
	"fmt"
	"github.com/aym-n/cosmo/raft"
)

func main() {
	cfg := raft.DefaultConfig("node1", nil, nil)
	fmt.Printf("Starting node: %s\n", cfg.NodeID)
}