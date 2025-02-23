package main

import (
	"sync"

	"github.com/zzhunght/kat-mq/internal/data"
	rpc "github.com/zzhunght/kat-mq/rpc/proto"
)

type consumerGroup map[string][]chan string

type subcribeTopic map[string]consumerGroup

type server struct {
	queue          *data.Queue
	subcribersChan subcribeTopic
	mu             sync.Mutex
	rpc.UnimplementedMessageServiceServer
}

func NewServer() *server {
	return &server{
		queue:          data.NewQueue(),
		subcribersChan: make(subcribeTopic),
	}
}
