package gphxkv

import (
	"google.golang.org/grpc"
	"gphxpaxos"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"
	"context"
)

type KVClient struct {
	grpcClient PhxKVServerClient
}

func (c *KVClient) GetConn(nodeId uint64) (PhxKVServerClient, error) {
	nodeInfo := gphxpaxos.NewNodeInfoWithId(nodeId)

	//for test multi node in one machine, each node have difference grpc_port.
	//but normally, every node's grpc_port is same, so you just set your grpc_port.
	//if you change paxos_port/grpc_port's relation, you must modify this line.
	grpcPort := nodeInfo.Port + 10000
	address := fmt.Sprintf("%s:%d", nodeInfo.Ip, grpcPort)

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return nil, err
	}

	grpcClient := NewPhxKVServerClient(conn)
	return grpcClient, nil
}

func NewKVClient(nodeId uint64) (*KVClient, error) {
	kvClient := &KVClient{}
	var err error
	kvClient.grpcClient, err = kvClient.GetConn(nodeId)
	if err != nil {
		return nil, err
	}
	return kvClient, nil
}

// deep是重试次数
func (c *KVClient) Put(key []byte, value []byte, version uint64, deep int) error {
	if deep > 3 {
		return KVStatus_FAIL
	}

	request := &KVOperator{
		Key:      key,
		Value:    value,
		Version:  version,
		Operator: KVOperatorType_WRITE,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	response, err := c.grpcClient.Put(ctx, request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
		return KVStatus_FAIL
	}

	if response.GetRet() == KVStatusCode_MASTER_REDIRECT {
		if response.GetMasterNodeid() != gphxpaxos.NULL_NODEID {
			c.grpcClient, err = c.GetConn(response.GetMasterNodeid())
			if err != nil {
				return err
			}

			return c.Put(key, value, version, deep+1)
		} else {
			return KVStatus_NO_MASTER
		}
	}

	return CodeToKVStatus(response.GetRet())

}

func (c *KVClient) GetLocal(key []byte) ([]byte, uint64, error) {
	request := &KVOperator{
		Key:      key,
		Operator: KVOperatorType_READ,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	response, err := c.grpcClient.GetLocal(ctx, request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
		return nil, 0, KVStatus_FAIL
	}

	data := response.GetData()
	return data.GetValue(), data.GetVersion(), CodeToKVStatus(response.GetRet())
}

func (c *KVClient) GetLocalWithVersion(key []byte, minVersion uint64) ([]byte, uint64, error) {
	 value, version, err := c.GetLocal(key)

	 if err == KVStatus_SUCC {
		if version < minVersion {
			return nil, 0, KVStatus_VERSION_NOTEXIST
		}

		return value, version, KVStatus_SUCC
	 }

	 return nil, 0, err
}



func (c *KVClient) Delete(key []byte, version uint64, deep int) error {

	request := &KVOperator{
		Key:      key,
		Version:  version,
		Operator: KVOperatorType_DELETE,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	response, err := c.grpcClient.Delete(ctx, request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
		return KVStatus_FAIL
	}

	if response.GetRet() == KVStatusCode_MASTER_REDIRECT {
		if response.GetMasterNodeid() != gphxpaxos.NULL_NODEID {
			c.grpcClient, err = c.GetConn(response.GetMasterNodeid())
			if err != nil {
				return err
			}

			return c.Delete(key, version, deep+1)
		} else {
			return KVStatus_NO_MASTER
		}
	}

	return CodeToKVStatus(response.GetRet())

}





func (c *KVClient) GetGlobal(key []byte, deep int) ([]byte, uint64, error) {

	if deep > 3 {
		return nil, 0, KVStatus_FAIL
	}


	request := &KVOperator{
		Key:      key,
		Operator: KVOperatorType_READ,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	response, err := c.grpcClient.GetGlobal(ctx, request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
		return nil, 0, KVStatus_FAIL
	}

	if response.GetRet() == KVStatusCode_MASTER_REDIRECT {
		if response.GetMasterNodeid() != gphxpaxos.NULL_NODEID {
			c.grpcClient, err = c.GetConn(response.GetMasterNodeid())
			if err != nil {
				return nil, 0, err
			}

			return c.GetGlobal(key, deep+1)
		} else  {
			return nil, 0, KVStatus_NO_MASTER
		}
	}

	data := response.GetData()
	return data.GetValue(), data.GetVersion(), CodeToKVStatus(response.GetRet())
}








