package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	pb "github.com/aym-n/cosmo/rpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// nodeAddr maps node IDs to addresses (must match cmd/node peerAddrs)
var nodeAddr = map[string]string{
	"node1": "localhost:50051",
	"node2": "localhost:50052",
	"node3": "localhost:50053",
}

func main() {
	addr := flag.String("addr", "localhost:50051", "Node address")
	flag.Parse()

	conn, err := grpc.Dial(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)

	fmt.Printf("Connected to %s\n", *addr)
	fmt.Println("Type commands to submit (they will be replicated to all nodes). Type 'quit' to exit.")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		command := scanner.Text()
		if command == "quit" {
			break
		}
		if command == "" {
			continue
		}

		resp, err := client.SubmitCommand(context.Background(), &pb.SubmitCommandRequest{
			Command: []byte(command),
		})
		if err != nil {
			log.Printf("SubmitCommand error: %v", err)
			continue
		}
		if !resp.Success {
			if resp.LeaderId != "" {
				fmt.Printf("Not the leader. Leader is %s (try -addr %s)\n", resp.LeaderId, nodeAddr[resp.LeaderId])
			} else {
				fmt.Println("Submit failed (no leader?). Try again.")
			}
			continue
		}
		fmt.Printf("OK index=%d term=%d (replicated to cluster)\n", resp.Index, resp.Term)
	}
}