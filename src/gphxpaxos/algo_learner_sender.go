package gphxpaxos

import (
	"time"
	"sync"
	log "github.com/sirupsen/logrus"
	"gphxpaxos/util"
)

type LearnerSender struct {
	config          *Config
	learner         *Learner
	paxosLog        *PaxosLog
	isSending       bool
	beginInstanceID uint64
	sendToNodeID    uint64
	isConfirmed     bool
	ackInstanceID   uint64
	absLastAckTime  uint64
	absLastSendTime uint64
	isEnd           bool
	isStart         bool
	mutex           sync.Mutex
}

func NewLearnerSender(instance *Instance, learner *Learner) *LearnerSender {
	sender := &LearnerSender{
		config:   instance.config,
		learner:  learner,
		paxosLog: instance.paxosLog,
		isEnd:    false,
		isStart:  false,
	}

	sender.SendDone()

	return sender
}

func (learnerSender *LearnerSender) Start() {
	util.StartRoutine(learnerSender.main)
}

func (learnerSender *LearnerSender) Stop() {
	learnerSender.isEnd = true
}

func (learnerSender *LearnerSender) main() {
	learnerSender.isStart = true

	for {
		learnerSender.WaitToSend()

		if learnerSender.isEnd {
			return
		}

		learnerSender.SendLearnedValue(learnerSender.beginInstanceID, learnerSender.sendToNodeID)

		learnerSender.SendDone()
	}
}

func (learnerSender *LearnerSender) ReleshSending() {
	learnerSender.absLastSendTime = util.NowTimeMs()
}

func (learnerSender *LearnerSender) IsImSending() bool {
	if !learnerSender.isSending {
		return false
	}

	nowTime := util.NowTimeMs()
	var passTime uint64 = 0
	if nowTime > learnerSender.absLastSendTime {
		passTime = nowTime - learnerSender.absLastSendTime
	}

	if passTime >= uint64(GetLearnerSenderPrepareTimeoutMs()) {
		return false
	}

	return true
}

func (learnerSender *LearnerSender) CheckAck(sendInstanceId uint64) bool {
	if sendInstanceId < learnerSender.ackInstanceID {
		log.Info("Already catch up, ack instanceid %d now send instanceid %d",
			learnerSender.ackInstanceID, sendInstanceId)
		return false
	}

	for sendInstanceId > learnerSender.ackInstanceID+uint64(GetLearnerSender_Ack_Lead()) {
		nowTime := util.NowTimeMs()
		var passTime uint64 = 0
		if nowTime > learnerSender.absLastAckTime {
			passTime = nowTime - learnerSender.absLastAckTime
		}

		if passTime >= uint64(GetLearnerSender_Ack_Lead()) {
			log.Errorf("Ack timeout, last acktime %d now send instanceid %d",
				learnerSender.absLastAckTime, sendInstanceId)
			return false
		}

		time.Sleep(10 * time.Millisecond)
	}

	return true
}

func (learnerSender *LearnerSender) Prepare(beginInstanceId uint64, sendToNodeId uint64) bool {
	learnerSender.mutex.Lock()

	prepareRet := false
	if !learnerSender.IsImSending() && !learnerSender.isConfirmed {
		prepareRet = true

		learnerSender.isSending = true
		learnerSender.absLastSendTime = util.NowTimeMs()
		learnerSender.absLastAckTime = learnerSender.absLastSendTime
		learnerSender.beginInstanceID = beginInstanceId
		learnerSender.ackInstanceID = beginInstanceId
		learnerSender.sendToNodeID = sendToNodeId
	}

	learnerSender.mutex.Unlock()
	return prepareRet
}

func (learnerSender *LearnerSender) Confirm(beginInstanceId uint64, sendToNodeId uint64) bool {
	learnerSender.mutex.Lock()

	confirmRet := false
	if learnerSender.IsImSending() && !learnerSender.isConfirmed {
		if learnerSender.beginInstanceID == beginInstanceId && learnerSender.sendToNodeID == sendToNodeId {
			confirmRet = true
			learnerSender.isConfirmed = true
		}
	}

	learnerSender.mutex.Unlock()
	return confirmRet
}

func (learnerSender *LearnerSender) Ack(ackInstanceId uint64, fromNodeId uint64) {
	learnerSender.mutex.Lock()
	if learnerSender.IsImSending() && learnerSender.isConfirmed {
		if learnerSender.sendToNodeID == fromNodeId {
			if ackInstanceId > learnerSender.ackInstanceID {
				learnerSender.ackInstanceID = ackInstanceId
				learnerSender.absLastAckTime = util.NowTimeMs()
			}
		}
	}
	learnerSender.mutex.Unlock()
}

func (learnerSender *LearnerSender) WaitToSend() {
	learnerSender.mutex.Lock()

	for !learnerSender.isConfirmed {
		time.Sleep(100 * time.Millisecond)
		if learnerSender.isEnd {
			break
		}
	}

	learnerSender.mutex.Unlock()
}

func (learnerSender *LearnerSender) SendLearnedValue(beginInstanceId uint64, sendToNodeId uint64) {
	log.Info("BeginInstanceID %d SendToNodeID %d", beginInstanceId, sendToNodeId)

	sendInstanceId := beginInstanceId

	sendQps := uint64(GetLearnerSenderSendQps())
	var sleepMs uint64 = 1
	if sendQps > 1000 {
		sleepMs = sendQps/1000 + 1
	}
	var sendInterval uint64 = sleepMs

	var sendCnt uint64 = 0
	var lastCksum uint32
	for sendInstanceId < learnerSender.learner.GetInstanceId() {
		err := learnerSender.SendOne(sendInstanceId, sendToNodeId, &lastCksum)
		if err != nil {
			log.Errorf("SendOne fail, SendInstanceID %d SendToNodeID %d error %v",
				sendInstanceId, sendToNodeId, err)
			return
		}

		if !learnerSender.CheckAck(sendInstanceId) {
			break
		}

		sendCnt++
		sendInstanceId++
		learnerSender.ReleshSending()

		if sendCnt >= sendInterval {
			sendCnt = 0
			time.Sleep(time.Duration(sleepMs) * time.Microsecond)
		}
	}
}

func (learnerSender *LearnerSender) SendOne(sendInstanceId uint64, sendToNodeId uint64, lastCksum *uint32) error {
	var state = &AcceptorStateData{}
	err := learnerSender.paxosLog.ReadState(learnerSender.config.GetMyGroupId(), sendInstanceId, state)
	if err != nil {
		return err
	}

	ballot := NewBallotNumber(state.GetAcceptedID(), state.GetAcceptedNodeID())

	err = learnerSender.learner.SendLearnValue(sendToNodeId, sendInstanceId, ballot,
		state.GetAcceptedValue(), *lastCksum, true)

	*lastCksum = state.GetChecksum()

	return err
}

func (learnerSender *LearnerSender) SendDone() {
	learnerSender.mutex.Lock()

	learnerSender.isSending = false
	learnerSender.isConfirmed = false
	learnerSender.beginInstanceID = INVALID_INSTANCEID
	learnerSender.sendToNodeID = NULL_NODEID
	learnerSender.absLastAckTime = 0
	learnerSender.ackInstanceID = 0
	learnerSender.absLastSendTime = 0

	learnerSender.mutex.Unlock()
}
