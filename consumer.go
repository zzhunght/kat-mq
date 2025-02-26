package katmq

import (
	"context"
	"fmt"
	"log"

	rpc "github.com/zzhunght/kat-mq/internal/proto"
	"google.golang.org/grpc"
)

type Consumer struct {
	client   rpc.MessageServiceClient
	grpcConn *grpc.ClientConn
	msg      chan string
	done     chan struct{}
}

func NewConsumer(address string) (*Consumer, error) {

	conn, err := grpc.DialContext(context.Background(), address, grpc.WithInsecure())

	if err != nil {
		return nil, err
	}
	return &Consumer{
		client:   rpc.NewMessageServiceClient(conn),
		grpcConn: conn,
		msg:      make(chan string, 10),
		done:     make(chan struct{}),
	}, nil
}

func (c *Consumer) StartConsume(topic string, group string) (<-chan string, error) {

	messages, err := c.client.Consume(context.Background(), &rpc.Subcribe{Topic: topic, Group: group})

	if err != nil {
		return nil, err
	}

	go func() {
		defer close(c.msg)
		for {
			select {
			case <-c.done:
				fmt.Println("Consumed stop")
				return
			default:
				msg, err := messages.Recv()
				if err != nil {
					log.Printf("Error receiving: %v", err)
					break
				}
				c.msg <- msg.Content
			}
		}
	}()

	return c.msg, err
}

func (c *Consumer) Close() {
	defer close(c.done)
	if c.grpcConn != nil {
		c.grpcConn.Close()
	}
}
