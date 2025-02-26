package main

import (
	"fmt"
	"log"

	katmq "github.com/zzhunght/kat-mq"
)

func main() {

	addr := "localhost:1234"

	client, err := katmq.NewProducer(addr)

	if err != nil {
		log.Fatal("Cannot connect to gRPC server: ", err)
	}
	defer client.Close()

	for i := 0; i < 10; i++ {
		content := fmt.Sprintf("Hello, gRPC! %v", i)
		topic := "*"
		resp, err := client.Publish(topic, []byte(content))
		if err != nil {
			log.Fatalf("Failed to publish message: %v", err)
		}

		if resp {
			log.Printf(" publish message: %v", i)
		}
	}

	log.Println("Message published successfully!")
}
