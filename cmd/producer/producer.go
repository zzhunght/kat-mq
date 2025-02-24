package main

import (
	"context"
	"fmt"
	"log"
	"sync"

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

	wg := &sync.WaitGroup{}
	for j := 0; j < 2; j++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			for i := 0; i < 99999; i++ {
				message := &rpc.PublishMessage{
					Content: fmt.Sprintf("Hello, gRPC! %v", i*(j+1)),
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
			wg.Done()
		}(wg)
	}
	wg.Wait()

	log.Println("Message published successfully!")
}
