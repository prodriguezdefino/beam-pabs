//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//
// Protocol Buffers describing the Runner API, which is the runner-independent,
// SDK-independent definition of the Beam model.

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.27.3
// source: org/apache/beam/model/pipeline/v1/beam_runner_api.proto

package pipeline_v1

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
	TestStreamService_Events_FullMethodName = "/org.apache.beam.model.pipeline.v1.TestStreamService/Events"
)

// TestStreamServiceClient is the client API for TestStreamService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type TestStreamServiceClient interface {
	// A TestStream will request for events using this RPC.
	Events(ctx context.Context, in *EventsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[TestStreamPayload_Event], error)
}

type testStreamServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewTestStreamServiceClient(cc grpc.ClientConnInterface) TestStreamServiceClient {
	return &testStreamServiceClient{cc}
}

func (c *testStreamServiceClient) Events(ctx context.Context, in *EventsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[TestStreamPayload_Event], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &TestStreamService_ServiceDesc.Streams[0], TestStreamService_Events_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[EventsRequest, TestStreamPayload_Event]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type TestStreamService_EventsClient = grpc.ServerStreamingClient[TestStreamPayload_Event]

// TestStreamServiceServer is the server API for TestStreamService service.
// All implementations must embed UnimplementedTestStreamServiceServer
// for forward compatibility.
type TestStreamServiceServer interface {
	// A TestStream will request for events using this RPC.
	Events(*EventsRequest, grpc.ServerStreamingServer[TestStreamPayload_Event]) error
	mustEmbedUnimplementedTestStreamServiceServer()
}

// UnimplementedTestStreamServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedTestStreamServiceServer struct{}

func (UnimplementedTestStreamServiceServer) Events(*EventsRequest, grpc.ServerStreamingServer[TestStreamPayload_Event]) error {
	return status.Errorf(codes.Unimplemented, "method Events not implemented")
}
func (UnimplementedTestStreamServiceServer) mustEmbedUnimplementedTestStreamServiceServer() {}
func (UnimplementedTestStreamServiceServer) testEmbeddedByValue()                           {}

// UnsafeTestStreamServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to TestStreamServiceServer will
// result in compilation errors.
type UnsafeTestStreamServiceServer interface {
	mustEmbedUnimplementedTestStreamServiceServer()
}

func RegisterTestStreamServiceServer(s grpc.ServiceRegistrar, srv TestStreamServiceServer) {
	// If the following call pancis, it indicates UnimplementedTestStreamServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&TestStreamService_ServiceDesc, srv)
}

func _TestStreamService_Events_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(EventsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(TestStreamServiceServer).Events(m, &grpc.GenericServerStream[EventsRequest, TestStreamPayload_Event]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type TestStreamService_EventsServer = grpc.ServerStreamingServer[TestStreamPayload_Event]

// TestStreamService_ServiceDesc is the grpc.ServiceDesc for TestStreamService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var TestStreamService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "org.apache.beam.model.pipeline.v1.TestStreamService",
	HandlerType: (*TestStreamServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Events",
			Handler:       _TestStreamService_Events_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "org/apache/beam/model/pipeline/v1/beam_runner_api.proto",
}
