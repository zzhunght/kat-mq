package data

import (
	"errors"
	"sync"
)

var (
	ErrQueueEmpty = errors.New("queue empty")
)

// queue message là 1 hashmap chứa topic và message của từng topic
type Queue struct {
	message map[string][]string
	mu      sync.Mutex
}

func NewQueue() *Queue {
	return &Queue{message: map[string][]string{}}
}

func (q *Queue) Add(msg string, topic string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// log.Printf("Adding message: %s , to topic: %s", msg, topic)
	if msg != "" {
		q.message[topic] = append(q.message[topic], msg)
	}
}

func (q *Queue) Pop(topic string) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.message[topic]) == 0 {
		return "", ErrQueueEmpty
	}

	msg := q.message[topic][0]
	q.message[topic] = q.message[topic][1:]
	return msg, nil
}
