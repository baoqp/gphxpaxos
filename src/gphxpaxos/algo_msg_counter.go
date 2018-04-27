package gphxpaxos

import "sync"


// 统计各个acceptor返回的数据
type MsgCounter struct {
	config *Config

	receiveMsgNodeIDMaps         map[uint64]bool
	rejectMsgNodeIDMaps          map[uint64]bool
	promiseOrAcceptMsgNodeIDMaps map[uint64]bool
	mutex sync.Mutex
}

func NewMsgCounter(config *Config) *MsgCounter {
	counter := &MsgCounter{
		config: config,
	}

	return counter
}

func (msgCounter *MsgCounter) StartNewRound() {
	msgCounter.receiveMsgNodeIDMaps = make(map[uint64]bool, 0)
	msgCounter.rejectMsgNodeIDMaps = make(map[uint64]bool, 0)
	msgCounter.promiseOrAcceptMsgNodeIDMaps = make(map[uint64]bool, 0)
}

func (msgCounter *MsgCounter) AddReceive(nodeId uint64) {
	msgCounter.mutex.Lock()
	defer msgCounter.mutex.Unlock()
	msgCounter.receiveMsgNodeIDMaps[nodeId] = true
}

func (msgCounter *MsgCounter) AddReject(nodeId uint64) {
	msgCounter.mutex.Lock()
	defer msgCounter.mutex.Unlock()
	msgCounter.rejectMsgNodeIDMaps[nodeId] = true
}

func (msgCounter *MsgCounter) AddPromiseOrAccept(nodeId uint64) {
	msgCounter.mutex.Lock()
	defer msgCounter.mutex.Unlock()
	msgCounter.promiseOrAcceptMsgNodeIDMaps[nodeId] = true
}

func (msgCounter *MsgCounter) IsPassedOnThisRound() bool {
	msgCounter.mutex.Lock()
	defer msgCounter.mutex.Unlock()
	return len(msgCounter.promiseOrAcceptMsgNodeIDMaps) >= msgCounter.config.GetMajorityCount()
}

func (msgCounter *MsgCounter) GetPassedCount() int {
	msgCounter.mutex.Lock()
	defer msgCounter.mutex.Unlock()
	return len(msgCounter.promiseOrAcceptMsgNodeIDMaps)
}

func (msgCounter *MsgCounter) IsRejectedOnThisRound() bool {
	msgCounter.mutex.Lock()
	defer msgCounter.mutex.Unlock()
	return len(msgCounter.rejectMsgNodeIDMaps) >= msgCounter.config.GetMajorityCount()
}

func (msgCounter *MsgCounter) IsAllReceiveOnThisRound() bool {
	msgCounter.mutex.Lock()
	defer msgCounter.mutex.Unlock()
	return len(msgCounter.receiveMsgNodeIDMaps) == msgCounter.config.GetNodeCount()
}
