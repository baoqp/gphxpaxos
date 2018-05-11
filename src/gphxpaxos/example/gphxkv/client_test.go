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
	version := uint64(10)

	// TODO NO_MASTER
	err = client.Put(key, value, version, 0)
	fmt.Println(err)

	value_, version_ , err := client.GetGlobal(key, 0 )
	fmt.Printf("%s, %d, %v \n", string(value_), version_, err)
}
