package main

import (
	"fmt"
	"log"
	"net"

	rpc "github.com/zzhunght/kat-mq/rpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {

	listener, err := net.Listen("tcp", "localhost:1234")

	if err != nil {
		fmt.Printf("Error when listening on port 1234: err : %v \n", err)
		panic(err)
	}

	server := NewServer()
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	rpc.RegisterMessageServiceServer(grpcServer, server)
	reflection.Register(grpcServer)

	log.Print("Starting gRPC  server")
	grpcServer.Serve(listener)
}
