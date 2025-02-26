package katmq

import (
	"context"
	"fmt"
	"log"
	"sync"

	rpc "github.com/zzhunght/kat-mq/internal/proto"
)

type consumerGroup map[string][]chan string

type consumer map[string]consumerGroup

type Broker struct {
	queue          *Queue
	consumer       consumer
	mu             sync.Mutex
	watcher        map[string]chan string
	topicProcessor map[string]int
	rpc.UnimplementedMessageServiceServer
}

func NewBroker() *Broker {
	return &Broker{
		queue:          NewQueue(),
		consumer:       make(consumer),
		watcher:        map[string]chan string{},
		topicProcessor: map[string]int{},
	}
}

func (s *Broker) Publish(ctx context.Context, msg *rpc.PublishMessage) (*rpc.PublishResponse, error) {
	watcher, exists := s.watcher[msg.Topic]

	if !exists {
		watcher = make(chan string, 1)
		go s.startWatcher(msg.Topic, watcher)
	}

	watcher <- msg.Content
	// log.Print("Received message from client: ", msg)
	return &rpc.PublishResponse{Success: true}, nil
}

func (s *Broker) Consume(request *rpc.Subcribe, stream rpc.MessageService_ConsumeServer) error {
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

func (s *Broker) subcribeToTopic(subCh chan string, topic string, group string) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	subcribeTopic, ok := s.consumer[topic]

	if !ok {
		subcribeTopic = make(consumerGroup)
		s.consumer[topic] = subcribeTopic
	}

	groupCh, ok := subcribeTopic[group]

	if !ok {
		groupCh = []chan string{}
	}

	groupCh = append(groupCh, subCh)
	subcribeTopic[group] = groupCh
	s.consumer[topic] = subcribeTopic

	_, ok = s.topicProcessor[topic]

	if !ok {
		s.topicProcessor[topic] = 1
		go s.processTopic(topic)
	}

	log.Printf("Subcriber : %v", s.consumer)

	return nil
}

func (s *Broker) processTopic(topic string) {

	for {
		s.sendToSubscriber(topic)
	}
}

func (s *Broker) startWatcher(topic string, ch chan string) {

	s.watcher[topic] = ch

	for msg := range ch {
		fmt.Printf("received message %v\n", msg)
		s.queue.Add(msg, topic)
	}
}

func (s *Broker) unsubcribeFromTopic(topic string, group string, ch chan string) error {
	log.Printf("Unsubscribe from topic: %v, group: %v\n", topic, group)
	s.mu.Lock()
	defer s.mu.Unlock()

	subcribeTopic := s.consumer[topic]
	groupCh, ok := subcribeTopic[group]

	if !ok {
		return fmt.Errorf("group not found")
	}

	for i, subCh := range groupCh {

		if subCh == ch {
			log.Printf("Unsubscribing from topic: %v, group: %v, consumer: %v\n", topic, group, ch)
			groupCh = append(groupCh[:i], groupCh[i+1:]...)
			subcribeTopic[group] = groupCh
			break
		}
	}
	s.consumer[topic] = subcribeTopic

	if len(groupCh) == 0 {
		delete(s.consumer[topic], group)
	}

	if len(subcribeTopic) == 0 {
		delete(s.consumer, topic)
	}

	log.Printf("Subcriber : %v", s.consumer)

	return nil
}

func (s *Broker) sendToSubscriber(topic string) {
	s.mu.Lock()
	subscribers, ok := s.consumer[topic]
	s.mu.Unlock()

	if !ok || len(subscribers) == 0 {
		return
	}
	msg, err := s.queue.Pop(topic)
	if err != nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for i, group := range subscribers {
		// round robin

		target := group[0]
		group = group[1:]

		select {
		case target <- fmt.Sprintf("data: %v", msg):
			// {
			// 	log.Printf("Sent message to subscriber index: %v\n", target)
			// }
		default:
			log.Printf("Skipped sending to a subscriber for topic %s", topic)
			s.queue.Add(msg, topic)
		}
		group = append(group, target)
		s.consumer[topic][i] = group
	}
}
