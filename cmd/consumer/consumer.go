package main

import (
	"flag"
	"fmt"
	"log"

	katmq "github.com/zzhunght/kat-mq"
)

type config struct {
	topic string
	group string
}

func main() {

	cfg := &config{topic: "*", group: "g1"}

	flag.StringVar(&cfg.topic, "topic", "*", "topic name")
	flag.StringVar(&cfg.group, "group", "g1", "group name")
	flag.Parse()

	addr := "localhost:1234"

	consumer, err := katmq.NewConsumer(addr)
	if err != nil {
		log.Fatal("Cannot connect to gRPC server: ", err)
	}

	defer consumer.Close()

	messages, err := consumer.StartConsume(cfg.topic, cfg.group)

	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	fmt.Println("Start consume")

	for msg := range messages {

		log.Printf("Received: %s", msg)
	}
}
