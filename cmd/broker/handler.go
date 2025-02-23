package main

import (
	"context"
	"fmt"

	rpc "github.com/zzhunght/kat-mq/rpc/proto"
)

func (s *server) Publish(ctx context.Context, msg *rpc.PublishMessage) (*rpc.PublishResponse, error) {
	s.queue.Add(msg.Content, msg.Topic)
	go s.sendToSubscriber(msg.Topic)
	// log.Print("Received message from client: ", msg)
	return &rpc.PublishResponse{Success: true}, nil
}

func (s *server) Consume(request *rpc.Subcribe, stream rpc.MessageService_ConsumeServer) error {
	fmt.Printf("Topic: %v , Group: %v \n", request.Topic, request.Group)
	subcribeChan := make(chan string, 10)

	if request.Topic == "" {
		return fmt.Errorf("topic is required")
	}

	if request.Group == "" {
		return fmt.Errorf("group is required")
	}

	s.subcribeToTopic(subcribeChan, request.Topic, request.Group)

	defer s.unsubcribeFromTopic(request.Topic, request.Group, subcribeChan)

	for {
		select {
		case msg := <-subcribeChan:
			if err := stream.Send(&rpc.Message{Content: msg}); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil // Ngắt khi client hủy
		}
	}
}
