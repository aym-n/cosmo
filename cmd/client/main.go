package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aym-n/cosmo/statemachine"
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
	fmt.Println("KV commands: GET <key> | PUT <key> <value> | DELETE <key>. Type 'quit' to exit.")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "quit" {
			break
		}
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		switch strings.ToUpper(parts[0]) {
		case "GET":
			if len(parts) < 2 {
				fmt.Println("Usage: GET <key>")
				continue
			}
			resp, err := client.GetKey(context.Background(), &pb.GetKeyRequest{Key: parts[1]})
			if err != nil {
				log.Printf("GetKey error: %v", err)
				continue
			}
			if resp.Found {
				fmt.Println(resp.Value)
			} else {
				fmt.Println("(not found)")
			}

		case "PUT":
			if len(parts) < 3 {
				fmt.Println("Usage: PUT <key> <value>")
				continue
			}
			key, value := parts[1], strings.Join(parts[2:], " ")
			cmd := statemachine.NewPutCommand(key, value)
			encoded, err := cmd.Encode()
			if err != nil {
				log.Printf("Encode error: %v", err)
				continue
			}
			resp, err := client.SubmitCommand(context.Background(), &pb.SubmitCommandRequest{Command: encoded})
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
			fmt.Printf("OK index=%d\n", resp.Index)

		case "DELETE":
			if len(parts) < 2 {
				fmt.Println("Usage: DELETE <key>")
				continue
			}
			cmd := statemachine.NewDeleteCommand(parts[1])
			encoded, err := cmd.Encode()
			if err != nil {
				log.Printf("Encode error: %v", err)
				continue
			}
			resp, err := client.SubmitCommand(context.Background(), &pb.SubmitCommandRequest{Command: encoded})
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
			fmt.Printf("OK index=%d\n", resp.Index)

		default:
			fmt.Println("Unknown command. Use GET <key>, PUT <key> <value>, or DELETE <key>")
		}
	}
}