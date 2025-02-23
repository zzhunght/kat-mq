package main

import (
	"fmt"
	"log"
	"math/rand"
)

func (s *server) subcribeToTopic(subCh chan string, topic string, group string) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	subcribeTopic, ok := s.subcribersChan[topic]

	if !ok {
		subcribeTopic = make(consumerGroup)
		s.subcribersChan[topic] = subcribeTopic
	}

	groupCh, ok := subcribeTopic[group]

	if !ok {
		groupCh = []chan string{}
	}

	groupCh = append(groupCh, subCh)
	subcribeTopic[group] = groupCh
	s.subcribersChan[topic] = subcribeTopic

	log.Printf("Subcriber : %v", s.subcribersChan)

	return nil
}

func (s *server) unsubcribeFromTopic(topic string, group string, ch chan string) error {
	log.Printf("Unsubscribe from topic: %v, group: %v\n", topic, group)
	s.mu.Lock()
	defer s.mu.Unlock()

	subcribeTopic := s.subcribersChan[topic]
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
	s.subcribersChan[topic] = subcribeTopic

	if len(groupCh) == 0 {
		delete(s.subcribersChan[topic], group)
	}

	if len(subcribeTopic) == 0 {
		delete(s.subcribersChan, topic)
	}
	log.Printf("Subcriber : %v", s.subcribersChan)

	return nil
}

func (s *server) sendToSubscriber(topic string) {
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

	for _, group := range subscribers {

		randomConsumerInGroup := rand.Intn(len(group))
		subscriber := group[randomConsumerInGroup]
		select {
		case subscriber <- fmt.Sprintf("data: %v", msg):
			{
				log.Printf("Sent message to subscriber index: %v\n", randomConsumerInGroup)
			}
		default:
			log.Printf("Skipped sending to a subscriber for topic %s", topic)
		}
	}
}
