package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/zzhunght/kat-mq/internal/data"
	rpc "github.com/zzhunght/kat-mq/rpc/proto"
)

type server struct {
	queue          *data.Queue
	subcribersChan map[string][]chan string
	mu             sync.Mutex
	rpc.UnimplementedMessageServiceServer
}

func NewServer() *server {
	return &server{
		queue:          data.NewQueue(),
		subcribersChan: make(map[string][]chan string),
	}
}

func (s *server) Publish(ctx context.Context, msg *rpc.PublishMessage) (*rpc.PublishResponse, error) {
	s.queue.Add(msg.Content, msg.Topic)
	go s.SendToSubscriber(msg.Topic)
	log.Print("Received message from client: ", msg)
	return &rpc.PublishResponse{Success: true}, nil
}

func (s *server) SendToSubscriber(topic string) {
	s.mu.Lock()
	subscribers, ok := s.subcribersChan[topic]
	s.mu.Unlock()

	if !ok || len(subscribers) == 0 {
		return
	}

	msg, err := s.queue.Pop(topic)
	if err != nil {
		log.Println("Error popping message from queue: ", err)
		return
	}

	for _, subscriber := range subscribers {
		select {
		case subscriber <- fmt.Sprintf("data: %v", msg):
		default:
			log.Printf("Skipped sending to a subscriber for topic %s", topic)
		}
	}
}

func (s *server) Subscribe(subscribe *rpc.Subcribe, stream rpc.MessageService_SubscribeServer) error {
	fmt.Print("Subscribe Topic: ", subscribe.Topic)
	subcribeChan := make(chan string, 10)

	s.mu.Lock()
	s.subcribersChan[subscribe.Topic] = append(s.subcribersChan[subscribe.Topic], subcribeChan)
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		subscribers := s.subcribersChan[subscribe.Topic]
		for i, ch := range subscribers {
			if ch == subcribeChan {
				s.subcribersChan[subscribe.Topic] = append(subscribers[:i], subscribers[i+1:]...)
				break
			}
		}
		if len(s.subcribersChan[subscribe.Topic]) == 0 {
			delete(s.subcribersChan, subscribe.Topic)
		}
		s.mu.Unlock()
		close(subcribeChan)
	}()

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
