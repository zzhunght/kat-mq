package main

import (
	"context"
	"log"

	rpc "github.com/zzhunght/kat-mq/rpc/proto"
	"google.golang.org/grpc"
)

func main() {

	conn, err := grpc.NewClient("localhost:1234", grpc.WithInsecure())

	if err != nil {
		log.Fatal("Cannot connect to gRPC server: ", err)
	}

	defer conn.Close()

	client := rpc.NewMessageServiceClient(conn)

	messages, err := client.Subscribe(context.Background(), &rpc.Subcribe{Topic: "*"})

	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	for {
		msg, err := messages.Recv()
		if err != nil {
			log.Printf("Error receiving: %v", err)
			break
		}
		log.Printf("Received: %s", msg.Content)
	}
}
