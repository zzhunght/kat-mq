package katmq

import (
	"context"

	rpc "github.com/zzhunght/kat-mq/internal/proto"
	"google.golang.org/grpc"
)

type Producer struct {
	client   rpc.MessageServiceClient
	grpcConn *grpc.ClientConn
}

func NewProducer(address string) (*Producer, error) {

	conn, err := grpc.DialContext(context.Background(), address, grpc.WithInsecure())

	if err != nil {
		return nil, err
	}
	return &Producer{
		client:   rpc.NewMessageServiceClient(conn),
		grpcConn: conn,
	}, nil
}

func (p *Producer) Publish(topic string, msg []byte) (bool, error) {

	payload := &rpc.PublishMessage{
		Content: string(msg),
		Topic:   topic,
	}

	resp, err := p.client.Publish(context.Background(), payload)

	return resp.Success, err
}

func (p *Producer) Close() error {
	if p.grpcConn != nil {
		return p.grpcConn.Close()
	}
	return nil
}
