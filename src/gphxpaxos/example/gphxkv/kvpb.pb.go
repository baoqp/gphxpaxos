// Code generated by protoc-gen-go. DO NOT EDIT.
// source: kvpb.proto

/*
Package gphxkv is a generated protocol buffer package.

It is generated from these files:
	kvpb.proto

It has these top-level messages:
	KVOperator
	KVData
	KVResponse
*/
package gphxkv

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type KVOperator struct {
	Key      []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value    []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	Version  uint64 `protobuf:"varint,3,opt,name=version" json:"version,omitempty"`
	Operator uint32 `protobuf:"varint,4,opt,name=operator" json:"operator,omitempty"`
	Sid      uint32 `protobuf:"varint,5,opt,name=sid" json:"sid,omitempty"`
}

func (m *KVOperator) Reset()                    { *m = KVOperator{} }
func (m *KVOperator) String() string            { return proto.CompactTextString(m) }
func (*KVOperator) ProtoMessage()               {}
func (*KVOperator) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *KVOperator) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *KVOperator) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func (m *KVOperator) GetVersion() uint64 {
	if m != nil {
		return m.Version
	}
	return 0
}

func (m *KVOperator) GetOperator() uint32 {
	if m != nil {
		return m.Operator
	}
	return 0
}

func (m *KVOperator) GetSid() uint32 {
	if m != nil {
		return m.Sid
	}
	return 0
}

type KVData struct {
	Value     []byte `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
	Version   uint64 `protobuf:"varint,2,opt,name=version" json:"version,omitempty"`
	Isdeleted bool   `protobuf:"varint,3,opt,name=isdeleted" json:"isdeleted,omitempty"`
}

func (m *KVData) Reset()                    { *m = KVData{} }
func (m *KVData) String() string            { return proto.CompactTextString(m) }
func (*KVData) ProtoMessage()               {}
func (*KVData) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *KVData) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func (m *KVData) GetVersion() uint64 {
	if m != nil {
		return m.Version
	}
	return 0
}

func (m *KVData) GetIsdeleted() bool {
	if m != nil {
		return m.Isdeleted
	}
	return false
}

type KVResponse struct {
	Data         *KVData `protobuf:"bytes,1,opt,name=data" json:"data,omitempty"`
	Ret          int32   `protobuf:"varint,2,opt,name=ret" json:"ret,omitempty"`
	MasterNodeid uint64  `protobuf:"varint,3,opt,name=master_nodeid,json=masterNodeid" json:"master_nodeid,omitempty"`
}

func (m *KVResponse) Reset()                    { *m = KVResponse{} }
func (m *KVResponse) String() string            { return proto.CompactTextString(m) }
func (*KVResponse) ProtoMessage()               {}
func (*KVResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

func (m *KVResponse) GetData() *KVData {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *KVResponse) GetRet() int32 {
	if m != nil {
		return m.Ret
	}
	return 0
}

func (m *KVResponse) GetMasterNodeid() uint64 {
	if m != nil {
		return m.MasterNodeid
	}
	return 0
}

func init() {
	proto.RegisterType((*KVOperator)(nil), "gphxkv.KVOperator")
	proto.RegisterType((*KVData)(nil), "gphxkv.KVData")
	proto.RegisterType((*KVResponse)(nil), "gphxkv.KVResponse")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for PhxKVServer service

type PhxKVServerClient interface {
	Put(ctx context.Context, in *KVOperator, opts ...grpc.CallOption) (*KVResponse, error)
	GetLocal(ctx context.Context, in *KVOperator, opts ...grpc.CallOption) (*KVResponse, error)
	GetGlobal(ctx context.Context, in *KVOperator, opts ...grpc.CallOption) (*KVResponse, error)
	Delete(ctx context.Context, in *KVOperator, opts ...grpc.CallOption) (*KVResponse, error)
}

type phxKVServerClient struct {
	cc *grpc.ClientConn
}

func NewPhxKVServerClient(cc *grpc.ClientConn) PhxKVServerClient {
	return &phxKVServerClient{cc}
}

func (c *phxKVServerClient) Put(ctx context.Context, in *KVOperator, opts ...grpc.CallOption) (*KVResponse, error) {
	out := new(KVResponse)
	err := grpc.Invoke(ctx, "/gphxkv.PhxKVServer/Put", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *phxKVServerClient) GetLocal(ctx context.Context, in *KVOperator, opts ...grpc.CallOption) (*KVResponse, error) {
	out := new(KVResponse)
	err := grpc.Invoke(ctx, "/gphxkv.PhxKVServer/GetLocal", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *phxKVServerClient) GetGlobal(ctx context.Context, in *KVOperator, opts ...grpc.CallOption) (*KVResponse, error) {
	out := new(KVResponse)
	err := grpc.Invoke(ctx, "/gphxkv.PhxKVServer/GetGlobal", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *phxKVServerClient) Delete(ctx context.Context, in *KVOperator, opts ...grpc.CallOption) (*KVResponse, error) {
	out := new(KVResponse)
	err := grpc.Invoke(ctx, "/gphxkv.PhxKVServer/Delete", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for PhxKVServer service

type PhxKVServerServer interface {
	Put(context.Context, *KVOperator) (*KVResponse, error)
	GetLocal(context.Context, *KVOperator) (*KVResponse, error)
	GetGlobal(context.Context, *KVOperator) (*KVResponse, error)
	Delete(context.Context, *KVOperator) (*KVResponse, error)
}

func RegisterPhxKVServerServer(s *grpc.Server, srv PhxKVServerServer) {
	s.RegisterService(&_PhxKVServer_serviceDesc, srv)
}

func _PhxKVServer_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(KVOperator)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PhxKVServerServer).Put(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gphxkv.PhxKVServer/Put",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PhxKVServerServer).Put(ctx, req.(*KVOperator))
	}
	return interceptor(ctx, in, info, handler)
}

func _PhxKVServer_GetLocal_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(KVOperator)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PhxKVServerServer).GetLocal(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gphxkv.PhxKVServer/GetLocal",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PhxKVServerServer).GetLocal(ctx, req.(*KVOperator))
	}
	return interceptor(ctx, in, info, handler)
}

func _PhxKVServer_GetGlobal_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(KVOperator)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PhxKVServerServer).GetGlobal(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gphxkv.PhxKVServer/GetGlobal",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PhxKVServerServer).GetGlobal(ctx, req.(*KVOperator))
	}
	return interceptor(ctx, in, info, handler)
}

func _PhxKVServer_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(KVOperator)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PhxKVServerServer).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gphxkv.PhxKVServer/Delete",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PhxKVServerServer).Delete(ctx, req.(*KVOperator))
	}
	return interceptor(ctx, in, info, handler)
}

var _PhxKVServer_serviceDesc = grpc.ServiceDesc{
	ServiceName: "gphxkv.PhxKVServer",
	HandlerType: (*PhxKVServerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Put",
			Handler:    _PhxKVServer_Put_Handler,
		},
		{
			MethodName: "GetLocal",
			Handler:    _PhxKVServer_GetLocal_Handler,
		},
		{
			MethodName: "GetGlobal",
			Handler:    _PhxKVServer_GetGlobal_Handler,
		},
		{
			MethodName: "Delete",
			Handler:    _PhxKVServer_Delete_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "kvpb.proto",
}

func init() { proto.RegisterFile("kvpb.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 307 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x94, 0x92, 0xcf, 0x4a, 0xf3, 0x40,
	0x10, 0xc0, 0xbf, 0xed, 0x9f, 0x7c, 0xed, 0xb4, 0x15, 0x59, 0x3c, 0x2c, 0xc5, 0x43, 0x89, 0x97,
	0x9e, 0x2a, 0x54, 0x7d, 0x83, 0x42, 0x0f, 0x11, 0x2d, 0x2b, 0xe4, 0x2a, 0x1b, 0x77, 0x68, 0x43,
	0x62, 0x36, 0xec, 0x6e, 0x43, 0x05, 0xdf, 0xd5, 0x57, 0x91, 0xdd, 0x34, 0x46, 0x0f, 0x82, 0xbd,
	0xcd, 0xfc, 0x96, 0x99, 0xdf, 0xcc, 0xb0, 0x00, 0x59, 0x55, 0x26, 0x8b, 0x52, 0x2b, 0xab, 0x68,
	0xb0, 0x2d, 0x77, 0x87, 0xac, 0x0a, 0xdf, 0x01, 0xa2, 0xf8, 0xb1, 0x44, 0x2d, 0xac, 0xd2, 0xf4,
	0x1c, 0xba, 0x19, 0xbe, 0x31, 0x32, 0x23, 0xf3, 0x31, 0x77, 0x21, 0xbd, 0x80, 0x7e, 0x25, 0xf2,
	0x3d, 0xb2, 0x8e, 0x67, 0x75, 0x42, 0x19, 0xfc, 0xaf, 0x50, 0x9b, 0x54, 0x15, 0xac, 0x3b, 0x23,
	0xf3, 0x1e, 0x6f, 0x52, 0x3a, 0x85, 0x81, 0x3a, 0x76, 0x63, 0xbd, 0x19, 0x99, 0x4f, 0xf8, 0x57,
	0xee, 0xba, 0x9b, 0x54, 0xb2, 0xbe, 0xc7, 0x2e, 0x0c, 0x63, 0x08, 0xa2, 0x78, 0x25, 0xac, 0x68,
	0x3d, 0xe4, 0x17, 0x4f, 0xe7, 0xa7, 0xe7, 0x12, 0x86, 0xa9, 0x91, 0x98, 0xa3, 0x45, 0xe9, 0x67,
	0x18, 0xf0, 0x16, 0x84, 0x5b, 0xb7, 0x15, 0x47, 0x53, 0xaa, 0xc2, 0x20, 0x0d, 0xa1, 0x27, 0x85,
	0x15, 0xbe, 0xf5, 0x68, 0x79, 0xb6, 0xa8, 0x57, 0x5f, 0xd4, 0x66, 0xee, 0xdf, 0xdc, 0x6c, 0x1a,
	0xad, 0xb7, 0xf4, 0xb9, 0x0b, 0xe9, 0x15, 0x4c, 0x5e, 0x85, 0xb1, 0xa8, 0x9f, 0x0b, 0x25, 0x31,
	0x95, 0xc7, 0x4d, 0xc7, 0x35, 0x7c, 0xf0, 0x6c, 0xf9, 0x41, 0x60, 0xb4, 0xd9, 0x1d, 0xa2, 0xf8,
	0x09, 0x75, 0x85, 0x9a, 0x5e, 0x43, 0x77, 0xb3, 0xb7, 0x94, 0xb6, 0x8e, 0xe6, 0xb6, 0xd3, 0x6f,
	0xac, 0x99, 0x2c, 0xfc, 0x47, 0x6f, 0x61, 0xb0, 0x46, 0x7b, 0xaf, 0x5e, 0x44, 0x7e, 0x42, 0xd5,
	0x1d, 0x0c, 0xd7, 0x68, 0xd7, 0xb9, 0x4a, 0x4e, 0x2a, 0x5b, 0x42, 0xb0, 0xf2, 0x17, 0xfa, 0x7b,
	0x4d, 0x12, 0xf8, 0xff, 0x72, 0xf3, 0x19, 0x00, 0x00, 0xff, 0xff, 0x1b, 0x25, 0x1f, 0xe1, 0x3d,
	0x02, 0x00, 0x00,
}