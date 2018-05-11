package gphxkv

import (
	"context"
	"gphxpaxos"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/reflection"
	"net"
	"google.golang.org/grpc"

	"fmt"
)

// 实现proto中定义的PhxKVServerServer接口
type KVServer struct {
	kvPaxos *KVPaxos
}

func NewKVServer(myNode gphxpaxos.NodeInfo, nodeList gphxpaxos.NodeInfoList,
	dbPath string, paxosLogPath string) *KVServer {

	return &KVServer{
		kvPaxos: NewKvPaxos(myNode, nodeList, dbPath, paxosLogPath),
	}
}

func (s *KVServer) Init() error {
	err := s.kvPaxos.RunPaxos()
	if err != nil {
		return err
	}

	grpcPort := s.kvPaxos.myNode.Port + 10000

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	RegisterPhxKVServerServer(grpcServer, s)
	// Register reflection service on gRPC server.
	reflection.Register(grpcServer)
	log.Infof("--start listen on %d---", grpcPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
		return err
	}

	return nil
}

func (s *KVServer) Put(ctx context.Context, request *KVOperator) (
	reply *KVResponse, err error) {

	reply = &KVResponse{}

	if !s.kvPaxos.IsIMMaster(request.GetKey()) {
		reply.Ret = KVStatusCode_MASTER_REDIRECT
		reply.MasterNodeid = s.kvPaxos.GetMaster(request.GetKey()).NodeId
		err = nil

		log.Infof(" I'm not master, need redirect, master nodeid i saw %d, key %s version %d",
			reply.MasterNodeid, string(request.GetKey()), request.GetVersion())

		return
	}

	err = s.kvPaxos.Put(request.GetKey(), request.GetValue(), request.GetVersion())
	reply.Ret = KVStatusToCode(err)
	return
}

func (s *KVServer) GetLocal(ctx context.Context, request *KVOperator) (
	reply *KVResponse, err error) {
	reply = &KVResponse{}
	value, version, err := s.kvPaxos.GetLocal(request.GetKey())
	if err == KVStatus_SUCC {
		data := &KVData{
			Value:     value,
			Version:   version,
			Isdeleted: false,
		}
		reply.Data = data
	} else if err == KVStatus_KEY_NOTEXIST {
		data := &KVData{
			Version:   version,
			Isdeleted: true,
		}
		reply.Data = data
	}

	reply.Ret = KVStatusToCode(err)
	return
}

func (s *KVServer) GetGlobal(ctx context.Context, request *KVOperator) (
	reply *KVResponse, err error) {
	reply = &KVResponse{}
	if !s.kvPaxos.IsIMMaster(request.GetKey()) {
		reply.Ret = KVStatusCode_MASTER_REDIRECT
		reply.MasterNodeid = s.kvPaxos.GetMaster(request.GetKey()).NodeId
		err = nil

		log.Infof(" I'm not master, need redirect, master nodeid i saw %d, key %s version %d",
			reply.MasterNodeid, string(request.GetKey()), request.GetVersion())

		return
	}

	reply, err = s.GetLocal(ctx, request)
	return
}

func (s *KVServer) Delete(ctx context.Context, request *KVOperator) (
	reply *KVResponse, err error) {
	reply = &KVResponse{}
	if !s.kvPaxos.IsIMMaster(request.GetKey()) {
		reply.Ret = KVStatusCode_MASTER_REDIRECT
		reply.MasterNodeid = s.kvPaxos.GetMaster(request.GetKey()).NodeId
		err = nil

		log.Infof(" I'm not master, need redirect, master nodeid i saw %d, key %s version %d",
			reply.MasterNodeid, string(request.GetKey()), request.GetVersion())

		return
	}

	err = s.kvPaxos.Delete(request.GetKey(), request.GetVersion())
	reply.Ret = KVStatusToCode(err)
	return
}
