// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.26.1
// source: proto/message.proto

package rpc

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	MessageService_Publish_FullMethodName = "/rpc.MessageService/Publish"
	MessageService_Consume_FullMethodName = "/rpc.MessageService/Consume"
)

// MessageServiceClient is the client API for MessageService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type MessageServiceClient interface {
	Publish(ctx context.Context, in *PublishMessage, opts ...grpc.CallOption) (*PublishResponse, error)
	Consume(ctx context.Context, in *Subcribe, opts ...grpc.CallOption) (grpc.ServerStreamingClient[Message], error)
}

type messageServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewMessageServiceClient(cc grpc.ClientConnInterface) MessageServiceClient {
	return &messageServiceClient{cc}
}

func (c *messageServiceClient) Publish(ctx context.Context, in *PublishMessage, opts ...grpc.CallOption) (*PublishResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(PublishResponse)
	err := c.cc.Invoke(ctx, MessageService_Publish_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *messageServiceClient) Consume(ctx context.Context, in *Subcribe, opts ...grpc.CallOption) (grpc.ServerStreamingClient[Message], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &MessageService_ServiceDesc.Streams[0], MessageService_Consume_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[Subcribe, Message]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type MessageService_ConsumeClient = grpc.ServerStreamingClient[Message]

// MessageServiceServer is the server API for MessageService service.
// All implementations must embed UnimplementedMessageServiceServer
// for forward compatibility.
type MessageServiceServer interface {
	Publish(context.Context, *PublishMessage) (*PublishResponse, error)
	Consume(*Subcribe, grpc.ServerStreamingServer[Message]) error
	mustEmbedUnimplementedMessageServiceServer()
}

// UnimplementedMessageServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedMessageServiceServer struct{}

func (UnimplementedMessageServiceServer) Publish(context.Context, *PublishMessage) (*PublishResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Publish not implemented")
}
func (UnimplementedMessageServiceServer) Consume(*Subcribe, grpc.ServerStreamingServer[Message]) error {
	return status.Errorf(codes.Unimplemented, "method Consume not implemented")
}
func (UnimplementedMessageServiceServer) mustEmbedUnimplementedMessageServiceServer() {}
func (UnimplementedMessageServiceServer) testEmbeddedByValue()                        {}

// UnsafeMessageServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to MessageServiceServer will
// result in compilation errors.
type UnsafeMessageServiceServer interface {
	mustEmbedUnimplementedMessageServiceServer()
}

func RegisterMessageServiceServer(s grpc.ServiceRegistrar, srv MessageServiceServer) {
	// If the following call pancis, it indicates UnimplementedMessageServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&MessageService_ServiceDesc, srv)
}

func _MessageService_Publish_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PublishMessage)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MessageServiceServer).Publish(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MessageService_Publish_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MessageServiceServer).Publish(ctx, req.(*PublishMessage))
	}
	return interceptor(ctx, in, info, handler)
}

func _MessageService_Consume_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(Subcribe)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(MessageServiceServer).Consume(m, &grpc.GenericServerStream[Subcribe, Message]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type MessageService_ConsumeServer = grpc.ServerStreamingServer[Message]

// MessageService_ServiceDesc is the grpc.ServiceDesc for MessageService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var MessageService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "rpc.MessageService",
	HandlerType: (*MessageServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Publish",
			Handler:    _MessageService_Publish_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Consume",
			Handler:       _MessageService_Consume_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "proto/message.proto",
}
