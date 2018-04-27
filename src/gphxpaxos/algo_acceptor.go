package gphxpaxos

import (
	log "github.com/sirupsen/logrus"
	"gphxpaxos/util"
	"github.com/golang/protobuf/proto"
)

//----------------------------------------------AcceptorState-------------------------------------------//
type AcceptorState struct {
	promiseNum   *BallotNumber
	acceptedNum  *BallotNumber
	acceptValues []byte
	checkSum     uint32
	paxosLog     *PaxosLog
	config       *Config
	syncTimes    int
}

func newAcceptorState(config *Config, paxosLog *PaxosLog) *AcceptorState {
	acceptorState := &AcceptorState{
		config:      config,
		paxosLog:    paxosLog,
		syncTimes:   0,
		acceptedNum: NewBallotNumber(0, 0),
		promiseNum:  NewBallotNumber(0, 0),
	}
	acceptorState.init()

	return acceptorState
}

func (acceptorState *AcceptorState) init() {
	acceptorState.acceptedNum.Reset()
	acceptorState.checkSum = 0
	acceptorState.acceptValues = []byte("")
}



func (acceptorState *AcceptorState) GetPromiseNum() *BallotNumber {
	return acceptorState.promiseNum
}

func (acceptorState *AcceptorState) SetPromiseNum(promiseNum *BallotNumber) {
	acceptorState.promiseNum.Clone(promiseNum)
}

func (acceptorState *AcceptorState) GetAcceptedNum() *BallotNumber {
	return acceptorState.acceptedNum
}

func (acceptorState *AcceptorState) SetAcceptedNum(acceptedNum *BallotNumber) {
	acceptorState.acceptedNum.Clone(acceptedNum)
}

func (acceptorState *AcceptorState) GetAcceptedValue() []byte {
	return acceptorState.acceptValues
}

func (acceptorState *AcceptorState) SetAcceptedValue(acceptedValue []byte) {
	acceptorState.acceptValues = acceptedValue
}

func (acceptorState *AcceptorState) GetChecksum() uint32 {
	return acceptorState.checkSum
}

// AcceptorState 持久化
func (acceptorState *AcceptorState) Persist(instanceid uint64, lastCheckSum uint32) error {
	if instanceid > 0 && lastCheckSum == 0 {
		acceptorState.checkSum = 0
	} else if len(acceptorState.acceptValues) > 0 {
		acceptorState.checkSum = util.Crc32(lastCheckSum, acceptorState.acceptValues, CRC32_SKIP)
	}

	var state = &AcceptorStateData{
		InstanceID:     proto.Uint64(instanceid),
		PromiseID:      proto.Uint64(acceptorState.promiseNum.proposalId),
		PromiseNodeID:  proto.Uint64(acceptorState.promiseNum.nodeId),
		AcceptedID:     proto.Uint64(acceptorState.acceptedNum.proposalId),
		AcceptedNodeID: proto.Uint64(acceptorState.acceptedNum.nodeId),
		AcceptedValue:  acceptorState.acceptValues,
		Checksum:       proto.Uint32(acceptorState.checkSum),
	}

	var options = WriteOptions{
		Sync: acceptorState.config.LogSync(),
	}

	// TODO  不应该每次都刷盘么???
	if options.Sync {
		acceptorState.syncTimes++
		if acceptorState.syncTimes > acceptorState.config.SyncInterval() {
			acceptorState.syncTimes = 0
		} else {
			options.Sync = false
		}
	}

	err := acceptorState.paxosLog.WriteState(&options, acceptorState.config.GetMyGroupId(), instanceid, state)
	if err != nil {
		return err
	}

	log.Infof("instanceId %d promiseId %d promiseNodeId %d "+
		"acceptedId %d acceptednodeId %d value_len %d ckSum %d",
		instanceid, acceptorState.promiseNum.proposalId,
		acceptorState.promiseNum.nodeId, acceptorState.acceptedNum.proposalId, acceptorState.acceptedNum.nodeId,
		len(acceptorState.acceptValues), acceptorState.checkSum)

	return nil
}

// 从磁盘中读取已保存的数据
func (acceptorState *AcceptorState) Load() (uint64, error) {
	myGroupId := acceptorState.config.GetMyGroupId()
	instanceid, err := acceptorState.paxosLog.GetMaxInstanceIdFromLog(myGroupId)

	if err != nil && err != ErrKeyNotFound {
		log.Errorf("Load max instance id fail:%v", err)
		return INVALID_INSTANCEID, err
	}

	if err == ErrKeyNotFound {
		log.Infof("empty database")
		return 0, nil
	}

	var state = &AcceptorStateData{}
	err = acceptorState.paxosLog.ReadState(myGroupId, instanceid, state)
	if err != nil {
		return instanceid, err
	}

	acceptorState.promiseNum.proposalId = state.GetPromiseID()
	acceptorState.promiseNum.nodeId = state.GetPromiseNodeID()
	acceptorState.acceptedNum.proposalId = state.GetAcceptedID()
	acceptorState.acceptedNum.nodeId = state.GetAcceptedNodeID()
	acceptorState.acceptValues = state.GetAcceptedValue()
	acceptorState.checkSum = state.GetChecksum()

	log.Infof("instanceId %d promiseId %d promiseNodeId %d "+
		"acceptedId %d acceptednodeId %d value_len %d ckSum %d",
		instanceid, acceptorState.promiseNum.proposalId,
		acceptorState.promiseNum.nodeId, acceptorState.acceptedNum.proposalId, acceptorState.acceptedNum.nodeId,
		len(acceptorState.acceptValues), acceptorState.checkSum)
	return instanceid, nil
}

//----------------------------------------------Acceptor-------------------------------------------//

type Acceptor struct {
	*Base

	config *Config
	state  *AcceptorState
}

func NewAcceptor(instance *Instance) *Acceptor {
	acceptor := &Acceptor{
		Base:   newBase(instance),
		state:  newAcceptorState(instance.config, instance.paxosLog),
		config: instance.config,
	}

	return acceptor
}

func (acceptor *Acceptor) Init() error {
	instanceId, err := acceptor.state.Load()
	if err != nil {
		log.Errorf("load state fail:%v", err)
		return err
	}

	if instanceId == 0 {
		log.Infof("empty database")
	}

	acceptor.setInstanceId(instanceId)

	log.Infof("Acceptor Init OK")

	return nil
}

func (acceptor *Acceptor) InitForNewPaxosInstance() {
	acceptor.state.init()
}

func (acceptor *Acceptor) NewInstance() {
	acceptor.Base.newInstance()
	acceptor.InitForNewPaxosInstance()
}

func (acceptor *Acceptor) GetAcceptorState() *AcceptorState {
	return acceptor.state
}

// handle paxos prepare msg 处理prepare msg
func (acceptor *Acceptor) onPrepare(msg *PaxosMsg) error {


	log.Infof("[%s]start prepare msg instanceId %d, from %d, proposalId %d, value_len %d",
		acceptor.instance.String(), msg.GetInstanceID(), msg.GetNodeID(), msg.GetProposalID(), len(msg.GetValue()))

	reply := &PaxosMsg{
		InstanceID: proto.Uint64(acceptor.GetInstanceId()),
		NodeID:     proto.Uint64(acceptor.config.GetMyNodeId()),
		ProposalID: proto.Uint64(msg.GetProposalID()),
		MsgType:    proto.Int32(MsgType_PaxosPrepareReply),
	}

	ballot := NewBallotNumber(msg.GetProposalID(), msg.GetNodeID())
	state := acceptor.state

	// Acceptor 处理prepare请求的基本逻辑
	//if (req.n > highest_promised_n)
	//  highest_promised_n = req.n
	//  reply :prepare_resp, {
	//		:n => highest_acc.n,
	//		:value => highest_acc.value
	//	}
	//else
	//  reject
	if ballot.GT(state.GetPromiseNum()) {
		log.Infof("[onPrepare Pass] req ballot %s promise bollot %s", ballot.String(), state.GetPromiseNum().String())
		log.Debugf("[%s][promise]promiseId %d, promiseNodeDd %d, preAcceptedId %d, preAcceptedNodeDd %d",
			acceptor.instance.String(), state.GetPromiseNum().proposalId, state.GetPromiseNum().nodeId,
			state.GetAcceptedNum().proposalId, state.GetAcceptedNum().nodeId)

		reply.PreAcceptID = proto.Uint64(state.GetAcceptedNum().proposalId)
		reply.PreAcceptNodeID = proto.Uint64(state.GetAcceptedNum().nodeId)

		if state.GetAcceptedNum().proposalId > 0 { // acceptedNum.proposalId > 0 说明已有接受过的value
			reply.Value = util.CopyBytes(state.GetAcceptedValue())
			log.Debug("[%s]return preaccept value:%s", acceptor.instance.String(), string(reply.Value))
		}

		state.SetPromiseNum(ballot)

		err := state.Persist(acceptor.GetInstanceId(), acceptor.Base.GetLastChecksum())
		if err != nil {
			log.Errorf("persist fail, now instanceid %d ret %v", acceptor.GetInstanceId(), err)
			return err
		}
	} else {
		log.Infof("[onPrepare reject] req ballot %s promise bollot %s ", ballot.String(), state.GetPromiseNum().String())
		log.Debugf("[reject]promiseid %d, promisenodeid %d",
			state.GetPromiseNum().proposalId, state.GetPromiseNum().nodeId)

		reply.RejectByPromiseID = proto.Uint64(state.GetPromiseNum().proposalId)
	}

	replyNodeId := msg.GetNodeID()
	log.Infof("[%s]end prepare instanceid %d replynodeid %d", acceptor.instance.String(), acceptor.GetInstanceId(), replyNodeId)

	acceptor.Base.sendPaxosMessage(replyNodeId, reply, Default_SendType)

	return nil
}

// handle paxos accept msg
func (acceptor *Acceptor) onAccept(msg *PaxosMsg) error {
	log.Infof("[%s]start accept msg instanceId %d, from %d, proposalId %d, value_len %d",
		acceptor.instance.String(), msg.GetInstanceID(), msg.GetNodeID(), msg.GetProposalID(), len(msg.GetValue()))

	reply := &PaxosMsg{
		InstanceID: proto.Uint64(acceptor.GetInstanceId()),
		NodeID:     proto.Uint64(acceptor.config.GetMyNodeId()),
		ProposalID: proto.Uint64(msg.GetProposalID()),
		MsgType:    proto.Int32(MsgType_PaxosAcceptReply),
	}

	ballot := NewBallotNumber(msg.GetProposalID(), msg.GetNodeID())
	state := acceptor.state

	//#Acceptor处理Accept请求的基本逻辑
	//if (req.n >= highest_promised_n)
	//    highest_acc = {:n => req.n, :value => req.value} // 更新acceptor的状态
	//    reply :accept_resp
	//else
	//  reject

	if ballot.GE(state.GetPromiseNum()) {
		log.Debug("[promise]promiseid %d, promisenodeid %d, preacceptedid %d, preacceptednodeid %d",
			state.GetPromiseNum().proposalId, state.GetPromiseNum().nodeId,
			state.GetAcceptedNum().proposalId, state.GetAcceptedNum().nodeId)

		state.SetPromiseNum(ballot)
		state.SetAcceptedNum(ballot)
		state.SetAcceptedValue(msg.GetValue())

		err := state.Persist(acceptor.GetInstanceId(), acceptor.Base.GetLastChecksum())
		if err != nil {
			log.Errorf("persist fail, now instanceid %d ret %v", acceptor.GetInstanceId(), err)
			return err
		}
	} else {
		log.Debug("[reject]promiseid %d, promisenodeid %d",
			state.GetPromiseNum().proposalId, state.GetPromiseNum().nodeId)

		reply.RejectByPromiseID = proto.Uint64(state.GetPromiseNum().proposalId)
	}

	replyNodeId := msg.GetNodeID()
	log.Infof("[%s]end accept instanceid %d replynodeid %d", acceptor.instance.String(), acceptor.GetInstanceId(), replyNodeId)

	acceptor.Base.sendPaxosMessage(replyNodeId, reply, Default_SendType)

	return nil
}
