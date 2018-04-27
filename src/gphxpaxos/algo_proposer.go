package gphxpaxos

import (
	log "github.com/sirupsen/logrus"
	"gphxpaxos/util"
	"github.com/golang/protobuf/proto"
	"math/rand"
)

const (
	PAUSE   = iota
	PREPARE
	ACCEPT
)

//-----------------------------------------------ProposerState-------------------------------------------//
type ProposerState struct {
	config     *Config
	value      []byte
	proposalId uint64

	// save the highest other propose id,
	// next propose id = max(proposalId, highestOtherProposalId) + 1
	highestOtherProposalId uint64 // 从acceptor返回的其他proposer提的proposalId的最大值

	// save pre-accept ballot number
	highestOtherPreAcceptBallot BallotNumber

	state int
}

func newProposalState(config *Config) *ProposerState {
	proposalState := new(ProposerState)
	proposalState.config = config
	proposalState.proposalId = 1

	return proposalState.init()
}

func (proposerState *ProposerState) init() *ProposerState {
	proposerState.highestOtherProposalId = 0
	proposerState.value = nil
	proposerState.state = PAUSE

	return proposerState
}

func (proposerState *ProposerState) getState() int {
	return proposerState.state
}

func (proposerState *ProposerState) setState(state int) {
	proposerState.state = state
}

func (proposerState *ProposerState) setStartProposalId(proposalId uint64) {
	proposerState.proposalId = proposalId
}

// 更新proposalId
func (proposerState *ProposerState) newPrepare() {
	log.Infof("start proposalId %d highestOther %d myNodeId %d",
		proposerState.proposalId, proposerState.highestOtherProposalId, proposerState.config.GetMyNodeId())

	// next propose id = max(proposalId, highestOtherProposalId) + 1
	maxProposalId := proposerState.highestOtherProposalId
	if proposerState.proposalId > proposerState.highestOtherProposalId {
		maxProposalId = proposerState.proposalId
	}

	proposerState.proposalId = maxProposalId + 1

	log.Infof("end proposalid %d", proposerState.proposalId)
}

func (proposerState *ProposerState) AddPreAcceptValue(otherPreAcceptBallot BallotNumber, otherPreAcceptValue []byte) {

	if otherPreAcceptBallot.IsNull() {
		return
	}

	// update value only when the ballot >  highestOtherPreAcceptBallot
	if otherPreAcceptBallot.GT(&proposerState.highestOtherPreAcceptBallot) {
		proposerState.highestOtherPreAcceptBallot = otherPreAcceptBallot
		proposerState.value = util.CopyBytes(otherPreAcceptValue)
	}
}

func (proposerState *ProposerState) GetProposalId() uint64 {
	return proposerState.proposalId
}

func (proposerState *ProposerState) GetValue() []byte {
	return proposerState.value
}

func (proposerState *ProposerState) SetValue(value []byte) {
	proposerState.value = util.CopyBytes(value)
}

func (proposerState *ProposerState) SetOtherProposalId(otherProposalId uint64) {
	if otherProposalId > proposerState.highestOtherProposalId {
		proposerState.highestOtherProposalId = otherProposalId
	}
}

func (proposerState *ProposerState) ResetHighestOtherPreAcceptBallot() {
	proposerState.highestOtherPreAcceptBallot.Reset()
}

//-------------------------------------------Proposer---------------------------------------------//

type Proposer struct {
	*Base

	config               *Config
	state                *ProposerState
	msgCounter           *MsgCounter
	learner              *Learner
	preparing            bool
	prepareTimerId       uint32
	acceptTimerId        uint32
	lastPrepareTimeoutMs uint32
	lastAcceptTimeoutMs  uint32
	canSkipPrepare       bool
	wasRejectBySomeone   bool
	timerThread          *util.TimerThread
	lastStartTimeMs      uint64
}

func NewProposer(instance *Instance) *Proposer {
	proposer := &Proposer{
		Base:        newBase(instance),
		config:      instance.config,
		state:       newProposalState(instance.config),
		msgCounter:  NewMsgCounter(instance.config),
		learner:     instance.learner,
		timerThread: instance.timerThread,
	}

	proposer.InitForNewPaxosInstance(false)
	proposer.lastPrepareTimeoutMs = proposer.config.GetPrepareTimeoutMs()
	proposer.lastAcceptTimeoutMs = proposer.config.GetAcceptTimeoutMs()

	return proposer
}

func (proposer *Proposer) InitForNewPaxosInstance(isMyCommit bool) {

	proposer.msgCounter.StartNewRound()
	proposer.state.init()

	proposer.exitPrepare()
	proposer.exitAccept()
}

func (proposer *Proposer) NewInstance(isMyComit bool) {
	proposer.Base.newInstance()
	proposer.InitForNewPaxosInstance(isMyComit)
}

func (proposer *Proposer) setStartProposalID(proposalId uint64) {
	proposer.state.setStartProposalId(proposalId)
}

func (proposer *Proposer) isWorking() bool {
	return proposer.prepareTimerId > 0 || proposer.acceptTimerId > 0
}

func (proposer *Proposer) NewValue(value []byte) {
	if len(proposer.state.GetValue()) == 0 {
		proposer.state.SetValue(value)
	}

	proposer.lastPrepareTimeoutMs = GetStartPrepareTimeoutMs()
	proposer.lastAcceptTimeoutMs = GetStartAcceptTimeoutMs()

	proposer.lastStartTimeMs = util.NowTimeMs()

	// TODO paxos的优化 multi-paxos  ???
	if proposer.canSkipPrepare && !proposer.wasRejectBySomeone {
		log.Infof("skip prepare,directly start accept")
		proposer.accept()
	} else {
		proposer.prepare(proposer.wasRejectBySomeone)
	}
}

/*
func (proposer *Proposer) isTimeout() bool {
	now := util.NowTimeMs()
	diff := now - proposer.lastStartTimeMs
	log.Debugf("[%s]diff %d, timeout %d", proposer.instance.String(), diff, proposer.timeOutMs)
	if uint32(diff) >= proposer.timeOutMs {
		proposer.timeOutMs = 0
	}

	if proposer.timeOutMs <= 0 {
		log.Debug("[%s]instance %d timeout", proposer.instance.String(), proposer.instanceId)
		proposer.instance.commitctx.setResult(PaxosTryCommitRet_Timeout, proposer.instanceId, []byte(""))
		return true
	}
	proposer.timeOutMs -= uint32(diff) // 不断减去消耗的时间，当余量<=0说明已经超时
	proposer.lastStartTimeMs = now

	return false
}
*/

func (proposer *Proposer) prepare(needNewBallot bool) {


	base := proposer.Base
	state := proposer.state

	// first reset all state
	proposer.exitAccept()
	proposer.state.setState(PREPARE)
	proposer.canSkipPrepare = false
	proposer.wasRejectBySomeone = false
	proposer.state.ResetHighestOtherPreAcceptBallot()

	if needNewBallot {
		proposer.state.newPrepare()
	}

	log.Infof("[%s]start prepare now.instanceid %d mynodeid %d state.proposal id %d state.valuelen %d new %v",
		proposer.instance.String(), proposer.GetInstanceId(), proposer.config.GetMyNodeId(), state.GetProposalId(), len(state.GetValue()), needNewBallot)

	// pack paxos prepare msg and broadcast
	msg := &PaxosMsg{
		MsgType:    proto.Int32(MsgType_PaxosPrepare),
		InstanceID: proto.Uint64(base.GetInstanceId()),
		NodeID:     proto.Uint64(proposer.config.GetMyNodeId()),
		ProposalID: proto.Uint64(state.GetProposalId()),
	}

	proposer.msgCounter.StartNewRound()
	proposer.addPrepareTimer(0)

	base.broadcastMessage(msg, BroadcastMessage_Type_RunSelf_First, Default_SendType)
}

func (proposer *Proposer) exitAccept() {
	if proposer.acceptTimerId != 0 {
		proposer.timerThread.DelTimer(proposer.acceptTimerId)
		proposer.acceptTimerId = 0
	}
}

func (proposer *Proposer) exitPrepare() {
	// 清除timer， 否则会触发回调
	if proposer.prepareTimerId != 0 {
		proposer.timerThread.DelTimer(proposer.prepareTimerId)
		proposer.prepareTimerId = 0
	}
}

func (proposer *Proposer) addPrepareTimer(timeOutMs uint32) {
	if proposer.prepareTimerId != 0 {
		proposer.timerThread.DelTimer(proposer.prepareTimerId)
		proposer.prepareTimerId = 0
	}

	if timeOutMs > 0 {
		proposer.prepareTimerId = proposer.timerThread.AddTimer(timeOutMs, Timer_Proposer_Prepare_Timeout, proposer.instance)
		return
	}

	proposer.prepareTimerId = proposer.timerThread.AddTimer(proposer.lastPrepareTimeoutMs, Timer_Proposer_Prepare_Timeout, proposer.instance)
	proposer.lastPrepareTimeoutMs *= 2
	if proposer.lastPrepareTimeoutMs > GetMaxPrepareTimeoutMs() {
		proposer.lastPrepareTimeoutMs = GetMaxPrepareTimeoutMs()
	}
}

func (proposer *Proposer) addAcceptTimer(timeOutMs uint32) {

	if proposer.acceptTimerId != 0 {
		proposer.timerThread.DelTimer(proposer.acceptTimerId)
		proposer.acceptTimerId = 0
	}

	if timeOutMs > 0 {
		proposer.acceptTimerId = proposer.timerThread.AddTimer(timeOutMs, Timer_Proposer_Accept_Timeout, proposer.instance)
		return
	}
	proposer.acceptTimerId = proposer.timerThread.AddTimer(proposer.lastAcceptTimeoutMs, Timer_Proposer_Accept_Timeout, proposer.instance)
	proposer.lastAcceptTimeoutMs *= 2
	if proposer.lastAcceptTimeoutMs > GetMaxAcceptTimeoutMs() {
		proposer.lastAcceptTimeoutMs = GetMaxAcceptTimeoutMs()
	}
}

func (proposer *Proposer) OnPrepareReply(msg *PaxosMsg) error {

	log.Infof("[%s]OnPrepareReply from %d with value %s ", proposer.instance.String(), msg.GetNodeID(), string(msg.GetValue()))

	if proposer.state.state != PREPARE {
		log.Errorf("[%s]proposer state not PREPARE", proposer.instance.String())
		return nil
	}

	if msg.GetProposalID() != proposer.state.GetProposalId() {
		// 可能是上一次prepare的应答消息，比如网络延迟等引起的
		log.Errorf("[%s]msg proposal id %d not same to proposer proposal id",
			proposer.instance.String(), msg.GetProposalID(), proposer.state.GetProposalId())
		return nil
	}

	proposer.msgCounter.AddReceive(msg.GetNodeID())

	if msg.GetRejectByPromiseID() == 0 {
		ballot := NewBallotNumber(msg.GetPreAcceptID(), msg.GetPreAcceptNodeID())
		proposer.msgCounter.AddPromiseOrAccept(msg.GetNodeID())
		proposer.state.AddPreAcceptValue(*ballot, msg.GetValue())
		log.Debug("[%s]prepare accepted", proposer.instance.String())
	} else {
		proposer.msgCounter.AddReject(msg.GetNodeID())
		proposer.wasRejectBySomeone = true
		proposer.state.SetOtherProposalId(msg.GetRejectByPromiseID())
		log.Debug("[%s]prepare rejected", proposer.instance.String())
	}

	log.Debug("[%s]%d prepare pass count:%d, major count:%d", proposer.instance.String(), proposer.GetInstanceId(),
		proposer.msgCounter.GetPassedCount(), proposer.config.GetMajorityCount())

	if proposer.msgCounter.IsPassedOnThisRound() {
		log.Infof("[Prepare Pass]")
		proposer.canSkipPrepare = true
		proposer.exitPrepare()
		proposer.accept() // 进入accept阶段
	} else if proposer.msgCounter.IsRejectedOnThisRound() || proposer.msgCounter.IsAllReceiveOnThisRound() {
		log.Infof("[Prepare Deny] wait 30ms and restart prepare") // 未通过，等待30ms开始新一轮的prepare
		proposer.addPrepareTimer(uint32(rand.Intn(30) + 10))
	}

	return nil
}

func (proposer *Proposer) accept() {

	base := proposer.Base
	state := proposer.state

	log.Infof("[%s]start accept %s", proposer.instance.String(), string(state.GetValue()))

	proposer.exitAccept()
	proposer.state.setState(ACCEPT)

	msg := &PaxosMsg{
		MsgType:      proto.Int32(MsgType_PaxosAccept),
		InstanceID:   proto.Uint64(base.GetInstanceId()),
		NodeID:       proto.Uint64(proposer.config.GetMyNodeId()),
		ProposalID:   proto.Uint64(state.GetProposalId()),
		Value:        state.GetValue(),
		LastChecksum: proto.Uint32(base.GetLastChecksum()),
	}

	proposer.msgCounter.StartNewRound()

	proposer.addAcceptTimer(0)

	base.broadcastMessage(msg, BroadcastMessage_Type_RunSelf_Final, Default_SendType)
}

func (proposer *Proposer) OnAcceptReply(msg *PaxosMsg) error {

	log.Infof("[%s]OnAcceptReply from %d with value %s ", proposer.instance.String(), msg.GetNodeID(), string(msg.GetValue()))

	state := proposer.state
	log.Infof("[%s]START msg.proposalId %d, state.proposalId %d, msg.from %d, rejectby %d",
		proposer.instance.String(), msg.GetProposalID(), state.GetProposalId(), msg.GetNodeID(), msg.GetRejectByPromiseID())

	base := proposer.Base

	if state.state != ACCEPT {
		log.Errorf("[%s]proposer state not ACCEPT", proposer.instance.String())
		return nil
	}

	if msg.GetProposalID() != state.GetProposalId() {
		log.Errorf("[%s]msg proposal id %d not same to proposer proposal id",
			proposer.instance.String(), msg.GetProposalID(), proposer.state.GetProposalId())
		return nil
	}

	msgCounter := proposer.msgCounter
	if msg.GetRejectByPromiseID() == 0 {
		log.Debug("[%s]accept accepted", proposer.instance.String())
		msgCounter.AddPromiseOrAccept(msg.GetNodeID())
	} else {
		log.Debug("[%s]accept rejected", proposer.instance.String())
		msgCounter.AddReject(msg.GetNodeID())
		proposer.wasRejectBySomeone = true
		state.SetOtherProposalId(msg.GetRejectByPromiseID())
	}

	if msgCounter.IsPassedOnThisRound() {
		log.Infof("[Accept Pass]")
		proposer.exitAccept()
		proposer.learner.ProposerSendSuccess(base.GetInstanceId(), state.GetProposalId())
		log.Infof("[%s]instance %d passed", proposer.instance.String(), msg.GetInstanceID())
	} else if proposer.msgCounter.IsRejectedOnThisRound() || proposer.msgCounter.IsAllReceiveOnThisRound() {
		log.Infof("[Accept Waiting]")
		proposer.addAcceptTimer(uint32(rand.Intn(50) + 10))
	}

	log.Infof("OnAcceptReply END")
	return nil
}

func (proposer *Proposer) onPrepareTimeout() {
	log.Info("[prepare timeout]")
	proposer.prepare(proposer.wasRejectBySomeone)
}

func (proposer *Proposer) onAcceptTimeout() {
	proposer.prepare(proposer.wasRejectBySomeone)
}


func (proposer *Proposer) CancelSkipPrepare() {
	proposer.canSkipPrepare = false
}