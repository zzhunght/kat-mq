package main

import (
	"context"
	"fmt"
	"log"

	rpc "github.com/zzhunght/kat-mq/rpc/proto"
	"google.golang.org/grpc"
)

func main() {

	conn, err := grpc.DialContext(context.Background(), "localhost:1234", grpc.WithInsecure())

	if err != nil {
		log.Fatal("Cannot connect to gRPC server: ", err)
	}

	defer conn.Close()

	client := rpc.NewMessageServiceClient(conn)

	for i := 0; i < 1000000000; i++ {
		message := &rpc.PublishMessage{
			Content: fmt.Sprintf("Hello, gRPC! %v", i),
			Topic:   "*",
		}
		resp, err := client.Publish(context.Background(), message)
		if err != nil {
			log.Fatalf("Failed to publish message: %v", err)
		}

		if resp.Success {
			log.Printf(" publish message: %v", message)
		}
	}

	log.Println("Message published successfully!")
}
