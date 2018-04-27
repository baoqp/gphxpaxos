package gphxpaxos

import (
	"github.com/golang/protobuf/proto"
	"fmt"
	log "github.com/sirupsen/logrus"
)

type PaxosLog struct {
	logStorage LogStorage
}

func NewPaxosLog(logStorage LogStorage) *PaxosLog {
	return &PaxosLog{logStorage: logStorage}
}

func (paxosLog *PaxosLog) WriteLog(options *WriteOptions, groupIdx int32,
	instanceId uint64, value []byte) error {

	state := &AcceptorStateData{
		InstanceID:     &instanceId, //也可以使用 proto.Uint64 包装下
		AcceptedValue:  value,
		PromiseID:      &UINT64_0,
		PromiseNodeID:  &UINT64_0,
		AcceptedID:     &UINT64_0,
		AcceptedNodeID: &UINT64_0,
	}
	return paxosLog.WriteState(options, groupIdx, instanceId, state)
}

func (paxosLog *PaxosLog) ReadLog(groupIdx int32, instanceId uint64) ([]byte, error) {
	var state = &AcceptorStateData{}
	err := paxosLog.ReadState(groupIdx, instanceId, state)

	if err != nil {
		log.Errorf("Read log error ")
		return nil, err
	}

	value := state.AcceptedValue

	return value, nil
}

func (paxosLog *PaxosLog) GetMaxInstanceIdFromLog(groupIdx int32) (uint64, error) {
	instanceId, err := paxosLog.logStorage.GetMaxInstanceId(groupIdx)
	if err != nil {
		return INVALID_INSTANCEID, err
	}

	return instanceId, nil
}

func (paxosLog *PaxosLog) WriteState(options *WriteOptions, groupIdx int32, instanceId uint64,
	state *AcceptorStateData) error {

	value, err := proto.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal state error")
	}

	err = paxosLog.logStorage.Put(options, groupIdx, instanceId, value)

	if err != nil {
		log.Errorf("write state error")
		return err
	}
	return nil
}

func (paxosLog *PaxosLog) ReadState(groupIdx int32, instanceId uint64, state *AcceptorStateData) error {
	value, err := paxosLog.logStorage.Get(groupIdx, instanceId)

	if err != nil {
		return err
	}

	err = proto.Unmarshal(value, state)
	if err != nil {
		log.Errorf("Read State error caused by unmarshal error %v", err)
		return err
	}
	return nil
}
