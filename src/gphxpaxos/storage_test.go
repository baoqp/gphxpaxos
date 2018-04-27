package gphxpaxos

import (
	"testing"
	"fmt"
	"github.com/gogo/protobuf/proto"
)

func TestStorage(t *testing.T) {
	logStorage := &MultiDatabase{}
	err := logStorage.Init("test_storage.log", 1)
	if err != nil {
		fmt.Println(err)
	}

	paxosLog := NewPaxosLog(logStorage)
	value := []byte("hello world")
	stateData := &AcceptorStateData{
		InstanceID:     proto.Uint64(1),
		PromiseID:      proto.Uint64(5),
		PromiseNodeID:  proto.Uint64(100),
		AcceptedID:     proto.Uint64(200),
		AcceptedNodeID: proto.Uint64(300),
		AcceptedValue:  value,
		Checksum:       proto.Uint32(10086),
	}

	options := &WriteOptions{Sync: true}
	paxosLog.WriteState(options, 0, 1, stateData)

	var state = &AcceptorStateData{}
	paxosLog.ReadState(0, 1, state)

	fmt.Println(state.GetAcceptedID())
	fmt.Println(string(state.GetAcceptedValue()))
}
