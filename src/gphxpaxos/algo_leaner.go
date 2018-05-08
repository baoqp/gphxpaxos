package gphxpaxos

import (
	log "github.com/sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"gphxpaxos/util"
)

//-------------------------------------------------LearnerState-----------------------------------------//

type LearnerState struct {
	config       *Config
	learnedValue []byte
	isLearned    bool
	newChecksum  uint32
	paxosLog     *PaxosLog
}

func NewLearnerState(instance *Instance) *LearnerState {
	state := &LearnerState{
		config:   instance.config,
		paxosLog: instance.paxosLog,
	}
	state.Init()
	return state
}

func (learnerState *LearnerState) GetNewChecksum() uint32 {
	return learnerState.newChecksum
}

func (learnerState *LearnerState) LearnValueWithoutWrite(instanceId uint64, value []byte, checksum uint32) {
	learnerState.learnedValue = value
	learnerState.isLearned = true
	learnerState.newChecksum = checksum
}

func (learnerState *LearnerState) LearnValue(instanceId uint64, learnedBallot BallotNumber,
	value []byte, lastChecksum uint32) error {

	if instanceId == 0 && lastChecksum == 0 {
		learnerState.newChecksum = 0
	} else if len(value) > 0 {
		learnerState.newChecksum = util.Crc32(lastChecksum, value, CRC32_SKIP)
	}

	state := AcceptorStateData{
		InstanceID:     proto.Uint64(instanceId),
		AcceptedValue:  proto.NewBuffer(value).Bytes(),
		PromiseID:      proto.Uint64(learnedBallot.proposalId),
		PromiseNodeID:  proto.Uint64(learnedBallot.nodeId),
		AcceptedID:     proto.Uint64(learnedBallot.proposalId),
		AcceptedNodeID: proto.Uint64(learnedBallot.nodeId),
		Checksum:       proto.Uint32(learnerState.newChecksum),
	}

	// TODO
	options := WriteOptions{
		Sync: false,
	}

	err := learnerState.paxosLog.WriteState(&options,
		learnerState.config.GetMyGroupId(), instanceId, &state)

	if err != nil {
		log.Errorf("storage writestate fail, instanceid %d valuelen %d err %v",
			instanceId, len(value), err)
		return err
	}

	learnerState.LearnValueWithoutWrite(instanceId, value, learnerState.newChecksum)

	return nil
}

func (learnerState *LearnerState) GetLearnValue() []byte {
	return learnerState.learnedValue
}

func (learnerState *LearnerState) IsLearned() bool {
	return learnerState.isLearned
}

func (learnerState *LearnerState) Init() {
	learnerState.learnedValue = nil
	learnerState.isLearned = false
	learnerState.newChecksum = 0
}

//-------------------------------------------------Learner-----------------------------------------//
// learner同时封装了学习者和被学习者的功能
type Learner struct {
	*Base
	instance                        *Instance
	paxosLog                        *PaxosLog
	acceptor                        *Acceptor
	state                           *LearnerState
	isImLearning                    bool
	highestSeenInstanceID           uint64
	highestSeenInstanceIDFromNodeID uint64
	lastAckInstanceId               uint64
	askforlearnNoopTimerID          uint32
	sender                          *LearnerSender
	timerThread                     *util.TimerThread
	ckReceiver                      *CheckpointReceiver
	ckSender                        *CheckpointSender
	ckMnger                         *CheckpointManager
	factory                         *SMFac
}

func NewLearner(instance *Instance) *Learner {
	learner := &Learner{
		Base:                            newBase(instance),
		paxosLog:                        instance.paxosLog,
		acceptor:                        instance.acceptor,
		isImLearning:                    false,
		highestSeenInstanceID:           0,
		highestSeenInstanceIDFromNodeID: NULL_NODEID,
		lastAckInstanceId:               0,
		state:                           NewLearnerState(instance),
		instance:                        instance,
		timerThread:                     instance.timerThread,
		ckReceiver:                      NewCheckpointReceiver(instance.config, instance.logStorage),
		ckMnger:                         instance.ckMnger,
		factory:                         instance.factory,
	}

	learner.sender = NewLearnerSender(instance, learner)
	learner.InitForNewPaxosInstance(false)

	return learner
}

func (learner *Learner) InitForNewPaxosInstance(isMyCommit bool) {
	learner.state.Init()
}

func (learner *Learner) NewInstance(isMyComit bool) {
	learner.Base.newInstance()
	learner.InitForNewPaxosInstance(isMyComit)
	log.Debug("[%s]now learner instance id %d", learner.instance.String(), learner.Base.GetInstanceId())
}

func (learner *Learner) Start() {
	learner.sender.Start()
}

func (learner *Learner) IsLearned() bool {
	return learner.state.IsLearned()
}

func (learner *Learner) GetLearnValue() []byte {
	return learner.state.GetLearnValue()
}

func (learner *Learner) Stop() {
	learner.sender.Stop()
}

func (learner *Learner) IsImLatest() bool {
	return learner.GetInstanceId()+1 >= learner.highestSeenInstanceID
}

func (learner *Learner) GetSeenLatestInstanceID() uint64 {
	return learner.highestSeenInstanceID
}

func (learner *Learner) SetSeenInstanceID(instanceId uint64, fromNodeId uint64) {
	if instanceId > learner.highestSeenInstanceID {
		learner.highestSeenInstanceID = instanceId
		learner.highestSeenInstanceIDFromNodeID = fromNodeId
	}
}

func (learner *Learner) GetNewChecksum() uint32 {
	return learner.state.GetNewChecksum()
}

func (learner *Learner) ResetAskforLearnNoop(timeout int) {
	if learner.askforlearnNoopTimerID > 0 {
		learner.timerThread.DelTimer(learner.askforlearnNoopTimerID)
	}

	learner.askforlearnNoopTimerID = learner.timerThread.AddTimer(uint32(timeout),
		Timer_Learner_Askforlearn_noop, learner.instance)
}

func (learner *Learner) AskforLearnNoop() {
	learner.ResetAskforLearnNoop(GetAskforLearnInterval())
	learner.isImLearning = false
	learner.askforLearn()
}

//广播AskForLearn消息
func (learner *Learner) askforLearn() {
	log.Info("start learn")

	base := learner.Base

	msg := &PaxosMsg{
		InstanceID: proto.Uint64(learner.GetInstanceId()),
		NodeID:     proto.Uint64(learner.config.GetMyNodeId()),
		MsgType:    proto.Int32(MsgType_PaxosLearner_AskforLearn),
	}

	if learner.config.IsIMFollower() {
		msg.ProposalNodeID = proto.Uint64(learner.config.GetFollowToNodeID())
	}

	log.Infof("end instanceid %d, mynodeid %d", msg.GetInstanceID(), msg.GetNodeID())

	base.broadcastMessage(msg, BroadcastMessage_Type_RunSelf_None, Default_SendType)

	base.BroadcastMessageToTempNode(msg, Default_SendType)  // TODO 目前不支持UDP
}

func (learner *Learner) OnAskforLearn(msg *PaxosMsg) {
	log.Info("start msg.instanceid %d now.instanceid %d msg.fromnodeid %d",
		msg.GetInstanceID(), learner.GetInstanceId(), msg.GetNodeID())

	learner.SetSeenInstanceID(msg.GetInstanceID(), msg.GetNodeID())

	if msg.GetProposalNodeID() == learner.config.GetMyNodeId() {
		log.Infof("found a node %d follow me", msg.GetNodeID())
		learner.config.AddFollowerNode(msg.GetNodeID())
	}

	if msg.GetInstanceID() >= learner.GetInstanceId() {
		return
	}

	// minChosenInstanceID 是某个镜像checkpoint的最小的instanceId
	if msg.GetInstanceID() >= learner.instance.ckMnger.GetMinChosenInstanceID() {
		if !learner.sender.Prepare(msg.GetInstanceID(), msg.GetNodeID()) {
			log.Errorf("learner sender working for others")

			if msg.GetInstanceID() == learner.GetInstanceId()-1 {
				log.Info("instanceid only difference one, just send this value to other")
				var state = &AcceptorStateData{}
				err := learner.paxosLog.ReadState(learner.config.GetMyGroupId(), msg.GetInstanceID(), state)
				if err == nil {
					ballot := NewBallotNumber(state.GetAcceptedID(), state.GetAcceptedNodeID())
					learner.SendLearnValue(msg.GetNodeID(), msg.GetInstanceID(), ballot,
						state.GetAcceptedValue(), 0, false)
				}
			}
			return
		}
	}

	learner.sendNowInstanceID(msg.GetInstanceID(), msg.GetNodeID())
}

func (learner *Learner) sendNowInstanceID(instanceId uint64, sendNodeId uint64) {
	msg := &PaxosMsg{
		InstanceID:          proto.Uint64(instanceId),
		NodeID:              proto.Uint64(learner.config.GetMyNodeId()),
		MsgType:             proto.Int32(MsgType_PaxosLearner_SendNowInstanceID),
		NowInstanceID:       proto.Uint64(learner.GetInstanceId()),
		MinChosenInstanceID: proto.Uint64(learner.ckMnger.GetMinChosenInstanceID()),
	}

	//instanceid too close not need to send vsm/master
	if learner.GetInstanceId() - instanceId > 50 {

		systemVarBuffer, err := learner.config.GetSystemVSM().GetCheckpointBuffer()
		if err == nil {
			msg.SystemVariables = util.CopyBytes(systemVarBuffer)
		}

		if learner.config.GetMasterSM() != nil {
			masterVarBuffer, err := learner.config.GetMasterSM().GetCheckpointBuffer()
			if err == nil {
				msg.MasterVariables = util.CopyBytes([]byte(masterVarBuffer))
			}
		}
	}

	learner.sendPaxosMessage(sendNodeId, msg, Default_SendType)
}


func (learner *Learner) OnSendNowInstanceId(msg *PaxosMsg) {
	instance := learner.instance
	instanceId := learner.instanceId

	log.Infof("[%s]start msg.instanceid %d now.instanceid %d msg.from_nodeid %d msg.maxinstanceid %d",
		instance.String(), msg.GetInstanceID(), learner.instanceId, msg.GetNodeID(), msg.GetNowInstanceID())

	learner.SetSeenInstanceID(msg.GetNowInstanceID(), msg.GetNodeID())

	// TODO 不同的错误需要分开
	systemVariablesChange, err := learner.config.GetSystemVSM().UpdateByCheckpoint(msg.SystemVariables)
	if systemVariablesChange && err == nil {
		log.Info("systemVariables changed!, all thing need to reflesh, so skip this msg")
		return
	}

	if masterSM := learner.config.GetMasterSM(); masterSM != nil {
		var masterVariablesChange bool
		err := masterSM.UpdateByCheckpoint(msg.MasterVariables, &masterVariablesChange)
		if masterVariablesChange && err == nil {
			log.Info("systemVariables changed!, all thing need to reflesh, so skip this msg")
			return
		}
	}

	// msg.GetInstanceID()是leaner在OnSendNowInstanceId之前发送给被学习者的本身的instanceId，不相等，
	// 说明learner本身的状态有更新了
	if msg.GetInstanceID() != instanceId {
		log.Errorf("[%s]lag msg instanceid %d, skip", instance.String(), msg.GetInstanceID())
		return
	}

	// 被学习者的instanceId落后于自身
	if msg.GetNowInstanceID() <= instanceId {
		log.Errorf("[%s]lag msg instanceid %d, skip", instance.String(), msg.GetNowInstanceID())
		return
	}

	// 需要发送快照
	if msg.GetMinChosenInstanceID() > instanceId {
		log.Infof("my instanceid %d small than other's minChosenInstanceId %d, other nodeid %d",
			learner.instanceId, msg.GetMinChosenInstanceID(), msg.GetNodeID())

		learner.AskforCheckpoint(msg.GetNodeID())
	} else if !learner.isImLearning {
		learner.confirmAskForLearn(msg.GetNodeID()) // 确认开始向msg.GetNodeID()代表的节点学习
	}
}

////////////////////////////////////////////////////////////////////////////////////////
func (learner *Learner) confirmAskForLearn(sendNodeId uint64) {
	msg := &PaxosMsg{
		InstanceID: proto.Uint64(learner.instanceId),
		NodeID:     proto.Uint64(learner.config.GetMyNodeId()),
		MsgType:    proto.Int32(MsgType_PaxosLearner_ConfirmAskforLearn),
	}
	learner.sendPaxosMessage(sendNodeId, msg, Default_SendType)
	learner.isImLearning = true
}

func (learner *Learner) OnConfirmAskForLearn(msg *PaxosMsg) {
	log.Infof("start msg.instanceid %d msg.from nodeid %d", msg.GetInstanceID(), msg.GetNodeID())

	if !learner.sender.Confirm(msg.GetInstanceID(), msg.GetNodeID()) {
		log.Errorf("learner sender confirm fail, maybe is lag msg")
		return
	}

	log.Info("ok, success confirm")
}

func (learner *Learner) SendLearnValue(sendNodeId uint64, learnInstanceId uint64,
	ballot *BallotNumber, value []byte, cksum uint32, needAck bool) error {
	var paxosMsg = &PaxosMsg{
		MsgType:        proto.Int32(MsgType_PaxosLearner_SendLearnValue),
		InstanceID:     proto.Uint64(learnInstanceId),
		NodeID:         proto.Uint64(learner.config.GetMyNodeId()),
		ProposalNodeID: proto.Uint64(ballot.nodeId),
		ProposalID:     proto.Uint64(ballot.proposalId),
		Value:          value,
		LastChecksum:   proto.Uint32(cksum),
	}

	if needAck {
		paxosMsg.Flag = proto.Uint32(PaxosMsgFlagType_SendLearnValue_NeedAck)
	}

	return learner.sendPaxosMessage(sendNodeId, paxosMsg, Default_SendType)
}

// 接收到被学习者所发来的消息
func (learner *Learner) OnSendLearnValue(msg *PaxosMsg) {
	log.Infof("START Msg.InstanceID %d Now.InstanceID %d Msg.ballot_proposalid %d "+
		"Msg.ballot_nodeid %d Msg.ValueSize %d",
		msg.GetInstanceID(), learner.GetInstanceId(), msg.GetProposalID(),
		msg.GetNodeID(), len(msg.Value))

	if msg.GetInstanceID() > learner.GetInstanceId() {
		log.Debug("[Latest Msg] i can't learn")
		return
	}

	if msg.GetInstanceID() < learner.GetInstanceId() {
		log.Debug("[Lag Msg] no need to learn")
	} else {
		ballot := NewBallotNumber(msg.GetProposalID(), msg.GetProposalNodeID())
		err := learner.state.LearnValue(msg.GetInstanceID(), *ballot,
			msg.GetValue(), learner.GetLastChecksum())

		if err != nil {
			log.Errorf("LearnState.LearnValue fail:%v", err)
			return
		}
		log.Infof("END LearnValue OK, proposalid %d proposalid_nodeid %d valueLen %d",
			msg.GetProposalID(), msg.GetNodeID(), len(msg.Value))
	}

	if msg.GetFlag() == PaxosMsgFlagType_SendLearnValue_NeedAck {
		learner.ResetAskforLearnNoop(GetAskforLearnInterval())
		learner.SendLearnValueAck(msg.GetNodeID())
	}
}

// 发送确认信息
// 并不是每学习一个value, 就发送ack，而是学习了ack_lead个value之后才发送ack,这样可以减少网络请求
func (learner *Learner) SendLearnValueAck(sendNodeId uint64) {
	log.Infof("START LastAck.Instanceid %d Now.Instanceid %d",
		learner.lastAckInstanceId, learner.GetInstanceId())

	if learner.GetInstanceId() < learner.lastAckInstanceId+uint64(GetLearnerReceiver_Ack_Lead()) {
		log.Info("no need ack")
		return
	}

	learner.lastAckInstanceId = learner.GetInstanceId()

	msg := &PaxosMsg{
		InstanceID: proto.Uint64(learner.GetInstanceId()),
		MsgType:    proto.Int32(MsgType_PaxosLearner_SendLearnValue_Ack),
		NodeID:     proto.Uint64(learner.config.GetMyNodeId()),
	}

	learner.sendPaxosMessage(sendNodeId, msg, Default_SendType)

	log.Info("END.OK")
}

// 接收到ack信息，更新sender信息
func (learner *Learner) OnSendLearnValueAck(msg *PaxosMsg) {
	log.Info("Msg.Ack.Instanceid %d Msg.from_nodeid %d", msg.GetInstanceID(), msg.GetNodeID())
	learner.sender.Ack(msg.GetInstanceID(), msg.GetNodeID())
}

func (learner *Learner) getSeenLatestInstanceId() uint64 {
	return learner.highestSeenInstanceID
}

////////////////////////////////////////////////////////////////////////////////////////////////

func (learner *Learner) TransmitToFollower() {
	if learner.config.GetMyFollowerCount() == 0 {
		return
	}

	acceptor := learner.acceptor
	msg := &PaxosMsg{
		MsgType:        proto.Int32(MsgType_PaxosLearner_SendLearnValue),
		InstanceID:     proto.Uint64(learner.GetInstanceId()),
		NodeID:         proto.Uint64(learner.config.GetMyNodeId()),
		ProposalNodeID: proto.Uint64(acceptor.GetAcceptorState().acceptedNum.nodeId),
		ProposalID:     proto.Uint64(acceptor.GetAcceptorState().acceptedNum.proposalId),
		Value:          acceptor.GetAcceptorState().GetAcceptedValue(),
		LastChecksum:   proto.Uint32(learner.GetLastChecksum()),
	}

	learner.BroadcastMessageToFollower(msg, Default_SendType)

	log.Info("OK")
}

// TODO   proposer accept被多数接受调用该方法
func (learner *Learner) ProposerSendSuccess(instanceId uint64, proposalId uint64) {
	msg := &PaxosMsg{
		MsgType:      proto.Int32(MsgType_PaxosLearner_ProposerSendSuccess),
		InstanceID:   proto.Uint64(instanceId),
		NodeID:       proto.Uint64(learner.config.GetMyNodeId()),
		ProposalID:   proto.Uint64(proposalId),
		LastChecksum: proto.Uint32(learner.GetLastChecksum()),
	}

	learner.broadcastMessage(msg, BroadcastMessage_Type_RunSelf_First, Default_SendType)
}

/*
	TODO learner的状态要proposer的accept请求被多数通过才能更新 ???
 */
func (learner *Learner) OnProposerSendSuccess(msg *PaxosMsg) {
	log.Infof("[%s]OnProposerSendSuccess Msg.InstanceID %d Now.InstanceID %d Msg.ProposalID %d "+
		"State.AcceptedID %d State.AcceptedNodeID %d, Msg.from_nodeid %d",
		learner.instance.String(), msg.GetInstanceID(), learner.GetInstanceId(), msg.GetProposalID(),
		learner.acceptor.GetAcceptorState().acceptedNum.proposalId,
		learner.acceptor.GetAcceptorState().acceptedNum.nodeId,
		msg.GetNodeID())

	if msg.GetInstanceID() != learner.GetInstanceId() {
		log.Debugf("instance id %d not same as msg instance id %d",
			learner.GetInstanceId(), msg.GetInstanceID())
		return
	}

	if learner.acceptor.GetAcceptorState().acceptedNum.IsNull() {
		log.Debug("not accepted any proposal")
		return
	}

	ballot := NewBallotNumber(msg.GetProposalID(), msg.GetNodeID())
	if !learner.acceptor.GetAcceptorState().acceptedNum.EQ(ballot) {
		log.Debugf("[%s]proposal ballot %s not same to accepted ballot %s", learner.instance.String(),
			learner.acceptor.GetAcceptorState().acceptedNum.String(), ballot.String())
		return
	}

	learner.state.LearnValueWithoutWrite(msg.GetInstanceID(),
		learner.acceptor.GetAcceptorState().GetAcceptedValue(),
		learner.acceptor.GetAcceptorState().GetChecksum())

	log.Info("learn value instanceid %d ok", msg.GetInstanceID())
	learner.TransmitToFollower()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TODO
func (learner *Learner) AskforCheckpoint(sendNodeId uint64) error {
	err := learner.ckMnger.PrepareForAskforCheckpoint(sendNodeId)
	if err != nil {
		return err
	}

	msg := &PaxosMsg{
		MsgType:    proto.Int32(MsgType_PaxosLearner_AskforCheckpoint),
		InstanceID: proto.Uint64(learner.instanceId),
		NodeID:     proto.Uint64(learner.config.GetMyNodeId()),
	}

	return learner.sendPaxosMessage(sendNodeId, msg, Default_SendType)
}

func (learner *Learner) OnAskforCheckpoint(msg *PaxosMsg) {
	ckSender := learner.GetNewCheckpointSender(msg.GetNodeID())
	if ckSender != nil {
		ckSender.Start()
	} else {
		log.Errorf("Checkpoint Sender is running")
	}
}

func (learner *Learner) SendCheckpointBegin(sendNodeId uint64, uuid uint64,
	sequence uint64, ckInstanceId uint64) error {
	ckMsg := &CheckpointMsg{
		MsgType:              proto.Int32(CheckpointMsgType_SendFile),
		NodeID:               proto.Uint64(learner.config.GetMyNodeId()),
		Flag:                 proto.Int32(CheckpointSendFileFlag_BEGIN),
		UUID:                 proto.Uint64(uuid),
		Sequence:             proto.Uint64(sequence),
		CheckpointInstanceID: proto.Uint64(ckInstanceId),
	}

	return learner.sendCheckpointMessage(sendNodeId, ckMsg, Default_SendType)
}

func (learner *Learner) SendCheckpoint(sendNodeId uint64, uuid uint64,
	sequence uint64, ckInstanceId uint64, ckssum uint32,
	filePath string, smid int32, offset uint64, buffer []byte) error {
	ckMsg := &CheckpointMsg{
		MsgType:              proto.Int32(CheckpointMsgType_SendFile),
		NodeID:               proto.Uint64(learner.config.GetMyNodeId()),
		Flag:                 proto.Int32(CheckpointSendFileFlag_ING),
		UUID:                 proto.Uint64(uuid),
		Sequence:             proto.Uint64(sequence),
		CheckpointInstanceID: proto.Uint64(ckInstanceId),
		Checksum:             proto.Uint32(ckssum),
		FilePath:             proto.String(filePath),
		SMID:                 proto.Int(int(smid)),
		Offset:               proto.Uint64(offset),
		Buffer:               buffer,
	}

	return learner.sendCheckpointMessage(sendNodeId, ckMsg, Default_SendType)
}

func (learner *Learner) SendCheckpointEnd(sendNodeId uint64, uuid uint64,
	sequence uint64, ckInstanceId uint64) error {
	ckMsg := &CheckpointMsg{
		MsgType:              proto.Int32(CheckpointMsgType_SendFile),
		NodeID:               proto.Uint64(learner.config.GetMyNodeId()),
		Flag:                 proto.Int32(CheckpointSendFileFlag_END),
		UUID:                 proto.Uint64(uuid),
		Sequence:             proto.Uint64(sequence),
		CheckpointInstanceID: proto.Uint64(ckInstanceId),
	}

	return learner.sendCheckpointMessage(sendNodeId, ckMsg, Default_SendType)
}

func (learner *Learner) OnSendCheckpoint(ckMsg *CheckpointMsg) {
	log.Info("start uuid %d flag %d sequence %d cpi %d checksum %d smid %d offset %d filepath %s",
		ckMsg.GetUUID(), ckMsg.GetFlag(), ckMsg.GetSequence(), ckMsg.GetCheckpointInstanceID(),
		ckMsg.GetChecksum(), ckMsg.GetSMID(), ckMsg.GetOffset(), ckMsg.GetFilePath())

	var err error
	switch ckMsg.GetFlag() {
	case CheckpointSendFileFlag_BEGIN:
		err = learner.OnSendCheckpointBegin(ckMsg)
		break
	case CheckpointSendFileFlag_ING:
		err = learner.OnSendCheckpointIng(ckMsg)
		break
	case CheckpointSendFileFlag_END:
		err = learner.OnSendCheckpointEnd(ckMsg)
		break
	}

	if err != nil {
		log.Errorf("[FATAL]rest checkpoint receiver and reset askforlearn")
		learner.ckReceiver.Reset()
		learner.ResetAskforLearnNoop(5000)
		learner.SendCheckpointAck(ckMsg.GetNodeID(), ckMsg.GetUUID(), ckMsg.GetSequence(), CheckpointSendFileAckFlag_Fail)
	} else {
		learner.SendCheckpointAck(ckMsg.GetNodeID(), ckMsg.GetUUID(), ckMsg.GetSequence(), CheckpointSendFileAckFlag_OK)
		learner.ResetAskforLearnNoop(10000)
	}
}

func (learner *Learner) OnSendCheckpointBegin(ckMsg *CheckpointMsg) error {
	err := learner.ckReceiver.NewReceiver(ckMsg.GetNodeID(), ckMsg.GetUUID())
	if err != nil {
		return err
	}

	err = learner.ckMnger.SetMinChosenInstanceId(ckMsg.GetCheckpointInstanceID())
	if err != nil {
		return err
	}

	return nil
}

func (learner *Learner) OnSendCheckpointIng(ckMsg *CheckpointMsg) error {
	return learner.ckReceiver.ReceiveCheckpoint(ckMsg)
}

func (learner *Learner) OnSendCheckpointEnd(ckMsg *CheckpointMsg) error {
	if !learner.ckReceiver.IsReceiverFinish(ckMsg.GetNodeID(), ckMsg.GetUUID(), ckMsg.GetSequence()) {
		log.Errorf("receive end msg but receiver not finish")
		return ErrInvalidMsg
	}

	smList := learner.factory.GetSMList()
	for _, sm := range smList {
		smid := sm.SMID()
		if smid == SYSTEM_V_SMID || smid == MASTER_V_SMID {
			continue
		}

		tmpDirPath := learner.ckReceiver.GetTmpDirPath(smid)
		filePathList, err := util.IterDir(tmpDirPath)
		if err != nil {
			log.Errorf("IterDir fail, dirpath %s", tmpDirPath)
		}

		if filePathList == nil || len(filePathList) == 0 {
			continue
		}

		// 载入镜像
		err = sm.LoadCheckpointState(learner.config.GetMyGroupId(), tmpDirPath,
			filePathList, ckMsg.GetCheckpointInstanceID())

		if err != nil {
			return err
		}
	}

	return nil
}

func (learner *Learner) SendCheckpointAck(sendNodeId uint64, uuid uint64, sequence uint64, flag int) error {
	ckMsg := &CheckpointMsg{
		MsgType:  proto.Int32(CheckpointMsgType_SendFile_Ack),
		NodeID:   proto.Uint64(learner.config.GetMyNodeId()),
		UUID:     proto.Uint64(uuid),
		Sequence: proto.Uint64(sequence),
		Flag:     proto.Int32(CheckpointSendFileFlag_ING),
	}

	return learner.sendCheckpointMessage(sendNodeId, ckMsg, Default_SendType)
}

func (learner *Learner) OnSendCheckpointAck(ckMsg *CheckpointMsg) {
	log.Info("START flag %d", ckMsg.GetFlag())

	if learner.ckSender != nil && !learner.ckSender.IsEnd() {
		if ckMsg.GetFlag() == CheckpointSendFileAckFlag_OK {
			learner.ckSender.Ack(ckMsg.GetNodeID(), ckMsg.GetUUID(), ckMsg.GetSequence())
		} else {
			learner.ckSender.End()
		}
	}
}

func (learner *Learner) GetNewCheckpointSender(sendNodeId uint64) *CheckpointSender {
	if learner.ckSender != nil {
		if learner.ckSender.isEnd {
			learner.ckSender = nil
		}
	}

	if learner.ckSender == nil {
		learner.ckSender = NewCheckpointSender(sendNodeId, learner.config, learner,
			learner.factory, learner.ckMnger)
		return learner.ckSender
	}

	return nil
}
