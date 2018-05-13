package gphxkv

import (
	"testing"
	"gphxpaxos"
	"fmt"
)

func TestKVClient(t *testing.T) {
	nodeInfo := gphxpaxos.FromString("127.0.0.1:11111")

	client,err := NewKVClient(nodeInfo.NodeId)
	if err != nil {
		panic(err)
	}

	key := []byte("hello")
	value := []byte("world")
	version := uint64(0)

	err = client.Put(key, value, version, 0)

	value_, version_ , err := client.GetGlobal(key, 0 )
	fmt.Printf("%s, %d, %v \n", string(value_), version_, err)

	value = []byte("paxos")
	err = client.Put(key, value,  version_, 0)

	value_, version_ , err = client.GetGlobal(key, 0 )
	fmt.Printf("%s, %d, %v \n", string(value_), version_, err)

	client.Delete(key, version_, 0)

	value_, version_ , err = client.GetGlobal(key, 0 )
	fmt.Printf("%s, %d, %v \n", string(value_), version_, err)
}
