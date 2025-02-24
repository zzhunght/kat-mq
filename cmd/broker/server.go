package main

import (
	"sync"

	"github.com/zzhunght/kat-mq/internal/data"
	rpc "github.com/zzhunght/kat-mq/rpc/proto"
)

type consumerGroup map[string][]chan string

type consumer map[string]consumerGroup

type server struct {
	queue          *data.Queue
	consumer       consumer
	mu             sync.Mutex
	watcher        map[string]chan string
	topicProcessor map[string]int
	rpc.UnimplementedMessageServiceServer
}

func NewServer() *server {
	return &server{
		queue:          data.NewQueue(),
		consumer:       make(consumer),
		watcher:        map[string]chan string{},
		topicProcessor: map[string]int{},
	}
}
