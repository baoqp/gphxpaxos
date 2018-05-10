package gphxpaxos

import (
	log "github.com/sirupsen/logrus"
	"fmt"
	"time"
	"gphxpaxos/util"
	"container/list"
	"github.com/golang/protobuf/proto"
)

type CommitMsg struct {
}

type Instance struct {
	config     *Config
	logStorage LogStorage
	paxosLog   *PaxosLog
	committer  *Committer
	commitctx  *CommitContext
	proposer   *Proposer
	learner    *Learner
	acceptor   *Acceptor
	name       string
	factory    *SMFac

	transport MsgTransport

	timerThread *util.TimerThread

	endChan chan bool
	end     bool

	commitChan    chan CommitMsg
	paxosMsgChan  chan *PaxosMsg
	commitTimerId uint32

	retryMsgList *list.List

	ckMnger      *CheckpointManager
	lastChecksum uint32
}

func NewInstance(cfg *Config, logstorage LogStorage, transport MsgTransport,
	useCkReplayer bool) *Instance {

	instance := &Instance{
		config:       cfg,
		logStorage:   logstorage,
		transport:    transport,
		paxosLog:     NewPaxosLog(logstorage),
		factory:      NewSMFac(cfg.GetMyGroupId()),
		timerThread:  util.NewTimerThread(),
		endChan:      make(chan bool),
		commitChan:   make(chan CommitMsg),
		paxosMsgChan: make(chan *PaxosMsg, 100),
		retryMsgList: list.New(),
	}

	instance.acceptor = NewAcceptor(instance)
	instance.ckMnger = NewCheckpointManager(cfg, instance.factory, logstorage, useCkReplayer)

	instance.commitctx = newCommitContext(instance)
	instance.committer = newCommitter(instance)

	instance.learner = NewLearner(instance)
	instance.proposer = NewProposer(instance)

	instance.name = fmt.Sprintf("%s-%d", cfg.GetOptions().MyNodeInfo.String(), cfg.GetMyNodeId())

	return instance
}

func (instance *Instance) getProposer() *Proposer {
	return instance.proposer
}
func (instance *Instance) getAcceptor() *Acceptor {
	return instance.acceptor
}

func (instance *Instance) Init() error {
	//Must init acceptor first, because the max instanceid is record in acceptor state.
	err := instance.acceptor.Init()
	if err != nil {
		return err
	}

	instance.ckMnger.Init()

	// 从状态机获取checkpointInstanceID
	cpInstanceId := instance.ckMnger.GetCheckpointInstanceId() + 1

	log.Infof("acceptor OK, log.instanceid %d,  instanceid %d",
		instance.acceptor.GetInstanceId(), cpInstanceId)

	nowInstanceId := cpInstanceId

	// 重放日志
	if nowInstanceId < instance.acceptor.GetInstanceId() {
		err := instance.PlayLog(nowInstanceId, instance.acceptor.GetInstanceId())
		if err != nil {
			return err
		}
		nowInstanceId = instance.acceptor.GetInstanceId()
	} else {
		if nowInstanceId > instance.acceptor.GetInstanceId() {
			instance.acceptor.InitForNewPaxosInstance()
		}
		instance.acceptor.setInstanceId(nowInstanceId)
	}

	log.Infof("now instance id: %d", nowInstanceId)

	instance.learner.setInstanceId(nowInstanceId)
	instance.proposer.setInstanceId(nowInstanceId)
	instance.proposer.setStartProposalID(
		instance.acceptor.GetAcceptorState().GetPromiseNum().proposalId + 1)

	instance.ckMnger.SetMaxChosenInstanceId(nowInstanceId)

	err = instance.InitLastCheckSum()
	if err != nil {
		return err
	}

	// 重置learner的定时器，定时器到时会广播AskForLearn消息，以保证本节点能保持较新的状态
	instance.learner.ResetAskforLearnNoop(GetAskforLearnInterval()) //GetAskforLearnInterval

	log.Info("init instance ok")
	return nil
}

// instance main loop
func (instance *Instance) main() {
	end := false
	for !end {
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-instance.endChan:
			end = true
			break
		case <-instance.commitChan:
			instance.onCommit()
			break
		case msg := <-instance.paxosMsgChan:
			instance.OnReceivePaxosMsg(msg, false)
			break
		case <-timer.C:
			break
		}

		timer.Stop()
		instance.dealRetryMsg()
	}
}

// TODO
func (instance *Instance) Start() {
	instance.learner.Start()
	instance.ckMnger.Start()
	util.StartRoutine(instance.main)
}

func (instance *Instance) Stop() {
	instance.end = true
	instance.endChan <- true

	// instance.transport.Close()
	close(instance.paxosMsgChan)
	close(instance.commitChan)
	close(instance.endChan)
	instance.timerThread.Stop()
}

func (instance *Instance) Status(instanceId uint64) (Status, []byte) {
	if instanceId < instance.acceptor.GetInstanceId() {
		value, _, _ := instance.GetInstanceValue(instanceId)
		return Decided, value
	}

	return Pending, nil
}

func (instance *Instance) GetCheckpointCleaner() *Cleaner {
	return instance.ckMnger.GetCleaner()
}

func (instance *Instance) GetCheckpointReplayer() *Replayer {
	return instance.ckMnger.GetReplayer()
}

func (instance *Instance) InitLastCheckSum() error {
	acceptor := instance.acceptor
	ckMnger := instance.ckMnger

	if acceptor.GetInstanceId() == 0 {
		instance.lastChecksum = 0
		return nil
	}

	if acceptor.GetInstanceId() <= ckMnger.GetMinChosenInstanceID() {
		instance.lastChecksum = 0
		return nil
	}
	var state = &AcceptorStateData{}
	err := instance.paxosLog.ReadState(instance.config.GetMyGroupId(), acceptor.GetInstanceId()-1, state)
	if err != nil && err != ErrKeyNotFound {
		return err
	}

	if err == ErrKeyNotFound {
		log.Warnf("last checksum not exist, now instance id %d", instance.acceptor.GetInstanceId())
		instance.lastChecksum = 0
		return nil
	}

	instance.lastChecksum = state.GetChecksum()
	log.Infof("OK, last checksum %d", instance.lastChecksum)

	return nil
}

func (instance *Instance) PlayLog(beginInstanceId uint64, endInstanceId uint64) error {

	if beginInstanceId < instance.ckMnger.GetMinChosenInstanceID() {
		log.Errorf("now instanceid %d small than chosen instanceid %d",
			beginInstanceId, instance.ckMnger.GetMinChosenInstanceID())

		return ErrInvalidInstanceId
	}

	for instanceId := beginInstanceId; instanceId < endInstanceId; instanceId++ {

		var state = &AcceptorStateData{}
		err := instance.paxosLog.ReadState(instance.groupId(), instanceId, state)
		if err != nil {
			log.Errorf("read instance %d log fail %v", instanceId, err)
			return err
		}

		err = instance.factory.Execute(instance.groupId(), instanceId, state.GetAcceptedValue(), nil)
		if err != nil {
			log.Errorf("execute instanceid %d fail:%v", instanceId, err)
			return err
		}
	}

	return nil
}


// try to propose a value, return instanceid end error
func (instance *Instance) Propose(value []byte) (uint64, error) {
	log.Debug("[%s]try to propose value %s", instance.name, string(value))
	return instance.committer.NewValue(value)
}


func (instance *Instance) dealRetryMsg() {
	len := instance.retryMsgList.Len()
	hasRetry := false
	for i := 0; i < len; i++ {
		obj := instance.retryMsgList.Front()
		msg := obj.Value.(*PaxosMsg)
		msgInstanceId := msg.GetInstanceID()
		nowInstanceId := instance.GetNowInstanceId()

		if msgInstanceId > nowInstanceId {
			break
		} else if msgInstanceId == nowInstanceId + 1 {
			if hasRetry {
				instance.OnReceivePaxosMsg(msg, true)
				log.Debug("[%s]retry msg i+1 instanceid %d", msgInstanceId)
			} else {
				break
			}
		} else if msgInstanceId == nowInstanceId {
			instance.OnReceivePaxosMsg(msg, false)
			log.Debug("[%s]retry msg instanceid %d", msgInstanceId)
			hasRetry = true
		}

		instance.retryMsgList.Remove(obj)
	}
}

func (instance *Instance) addRetryMsg(msg *PaxosMsg) {
	if instance.retryMsgList.Len() > RETRY_QUEUE_MAX_LEN {
		obj := instance.retryMsgList.Front()
		instance.retryMsgList.Remove(obj)
	}
	instance.retryMsgList.PushBack(msg)
}

func (instance *Instance) clearRetryMsg() {
	instance.retryMsgList = list.New()
}

func (instance *Instance) GetNowInstanceId() uint64 {
	return instance.acceptor.GetInstanceId()
}

func (instance *Instance) sendCommitMsg() {
	instance.commitChan <- CommitMsg{}
}

// handle commit message
func (instance *Instance) onCommit() { //  gphxpaxos instance.cpp CheckNewValue

	if !instance.commitctx.isNewCommit() {
		return
	}

	if !instance.learner.IsImLatest() {
		return
	}

	if instance.config.IsIMFollower() {
		log.Errorf("[%s]I'm follower, skip commit new value", instance.name)
		instance.commitctx.setResultOnlyRet(PaxosTryCommitRet_Follower_Cannot_Commit)
		return
	}

	if !instance.config.CheckConfig() {
		instance.commitctx.setResultOnlyRet(PaxosTryCommitRet_Im_Not_In_Membership)
		return
	}

	commitValue := instance.commitctx.getCommitValue()
	if len(commitValue) > GetMaxValueSize() {
		log.Errorf("[%s]value size %d to large, skip commit new value", instance.name, len(commitValue))
		instance.commitctx.setResultOnlyRet(PaxosTryCommitRet_Value_Size_TooLarge)
	}

	timeOutMs := instance.commitctx.StartCommit(instance.proposer.GetInstanceId())

	if timeOutMs > 0 {
		instance.commitTimerId = instance.timerThread.AddTimer(timeOutMs, Timer_Instance_Commit_Timeout, instance)
	}


	if instance.config.IsUseMembership() &&
		(instance.proposer.GetInstanceId() == 0 || instance.config.GetGid() == 0) {

		log.Infof("Need to init system variables, Now.InstanceID %d Now.Gid %d",
			instance.proposer.GetInstanceId(), instance.config.GetGid())

		gid := util.GenGid(instance.config.MyNodeId)
		initSVOpValue, err := instance.config.GetSystemVSM().CreateGidOPValue(gid)

		if err != nil {
			log.Errorf("instance on Commit CreateGid_OPValue failed, %v", err)
			return
		}

		value := instance.factory.PackPaxosValue(initSVOpValue, instance.config.GetSystemVSM().SMID())
		instance.proposer.NewValue(value)

	} else {
		log.Debug("[%s]start commit instance %d, timeout:%d", instance.String(), instance.proposer.GetInstanceId(), timeOutMs)

		if instance.config.OpenChangeValueBeforePropose {
			instance.factory.BeforePropose(instance.groupId(), instance.commitctx.getCommitValue())
		}

		instance.proposer.NewValue(instance.commitctx.getCommitValue())
	}
}

func (instance *Instance) String() string {
	return instance.name
}

func (instance *Instance) GetLastChecksum() uint32 {
	return instance.lastChecksum
}

func (instance *Instance) GetInstanceValue(instanceId uint64) ([]byte, int32, error) {
	if instanceId >= instance.acceptor.GetInstanceId() {
		return nil, -1, Paxos_GetInstanceValue_Value_Not_Chosen_Yet
	}
	var state = &AcceptorStateData{}
	err := instance.paxosLog.ReadState(instance.groupId(), instanceId, state)
	if err != nil {
		return nil, -1, err
	}

	value, smid := instance.factory.UnpackPaxosValue(state.GetAcceptedValue())
	return value, smid, nil
}

// TODO
func (instance *Instance) isCheckSumValid(msg *PaxosMsg) bool {
	return true
}

func (instance *Instance) NewInstance(isMyCommit bool) {
	instance.acceptor.NewInstance()
	instance.proposer.NewInstance(isMyCommit)
	instance.learner.NewInstance(isMyCommit)
}

func (instance *Instance) receiveMsgForLearner(msg *PaxosMsg) error {
	log.Infof("[%s]recv msg %d for learner", instance.name, msg.GetMsgType())
	learner := instance.learner
	msgType := msg.GetMsgType()

	switch msgType {
	case MsgType_PaxosLearner_AskforLearn:
		learner.OnAskforLearn(msg)
		break
	case MsgType_PaxosLearner_SendLearnValue:
		learner.OnSendLearnValue(msg)
		break
	case MsgType_PaxosLearner_ProposerSendSuccess:
		learner.OnProposerSendSuccess(msg)
		break
	case MsgType_PaxosLearner_SendNowInstanceID:
		learner.OnSendNowInstanceId(msg)
		break
	case MsgType_PaxosLearner_ConfirmAskforLearn:
		learner.OnConfirmAskForLearn(msg)
		break
	case MsgType_PaxosLearner_SendLearnValue_Ack:
		learner.OnSendLearnValueAck(msg)
		break
	case MsgType_PaxosLearner_AskforCheckpoint:
		learner.OnAskforCheckpoint(msg)
		break
	}
	if learner.IsLearned() {
		commitCtx := instance.commitctx
		isMyCommit, smCtx := commitCtx.IsMyCommit(msg.GetNodeID(), learner.GetInstanceId(), learner.GetLearnValue())
		if isMyCommit {
			log.Debug("[%s]instance %d is my commit", instance.name, learner.GetInstanceId())
		} else {
			log.Debug("[%s]instance %d is not my commit", instance.name, learner.GetInstanceId())
		}

		err := instance.SMExecute(learner.GetInstanceId(), learner.GetLearnValue(), isMyCommit, smCtx)
		if err != nil {
			log.Errorf("SMExecute fail, instanceId %d, not increase instanceId", learner.GetInstanceId())

			commitCtx.setResult(PaxosTryCommitRet_ExecuteFail, learner.GetInstanceId(), learner.GetLearnValue())
			instance.proposer.CancelSkipPrepare()
			return err
		}

		commitCtx.setResult(PaxosTryCommitRet_OK, learner.GetInstanceId(), learner.GetLearnValue())
		instance.lastChecksum = instance.learner.GetNewChecksum()
		instance.NewInstance(isMyCommit)

		log.Infof("[%s]new paxos instance has started, Now instance id:proposer %d, acceptor %d, learner %d",
			instance.name, instance.proposer.GetInstanceId(), instance.acceptor.GetInstanceId(), instance.learner.GetInstanceId())

		instance.ckMnger.SetMaxChosenInstanceId(instance.acceptor.GetInstanceId())
	}
	return nil
}

func (instance *Instance) receiveMsgForProposer(msg *PaxosMsg) error {
	if instance.config.IsIMFollower() {
		log.Errorf("[%s]follower skip %d msg", instance.name, msg.GetMsgType())
		return nil
	}

	msgInstanceId := msg.GetInstanceID()
	proposerInstanceId := instance.proposer.GetInstanceId()

	if msgInstanceId != proposerInstanceId {
		log.Errorf("[%s]msg instance id %d not same to proposer instance id %d",
			instance.name, msgInstanceId, proposerInstanceId)
		return nil
	}

	msgType := msg.GetMsgType()
	if msgType == MsgType_PaxosPrepareReply {
		return instance.proposer.OnPrepareReply(msg)
	} else if msgType == MsgType_PaxosAcceptReply {
		return instance.proposer.OnAcceptReply(msg)
	}

	return ErrInvalidMsg
}

// handle msg type which for acceptor
func (instance *Instance) receiveMsgForAcceptor(msg *PaxosMsg, isRetry bool) error {
	if instance.config.IsIMFollower() {
		log.Errorf("[%s]follower skip %d msg", instance.name, msg.GetMsgType())
		return nil
	}

	msgInstanceId := msg.GetInstanceID()
	acceptorInstanceId := instance.acceptor.GetInstanceId()

	log.Infof("[%s]msg instance %d, acceptor instance %d", instance.name, msgInstanceId, acceptorInstanceId)

	// msgInstanceId == acceptorInstanceId + 1  means acceptor instance has been approved
	// so just learn it
	// 此处是处理重复消息的时候才会用到
	if msgInstanceId == acceptorInstanceId + 1 {
		// skip success message
		newMsg := &PaxosMsg{}
		util.CopyStruct(newMsg, *msg) // *newMsg = *msg
		newMsg.InstanceID = proto.Uint64(acceptorInstanceId)
		newMsg.MsgType = proto.Int(MsgType_PaxosLearner_ProposerSendSuccess)
		log.Debug("learn it, node id: %d:%d", newMsg.GetNodeID(), msg.GetNodeID())
		instance.receiveMsgForLearner(newMsg)
	}

	msgType := msg.GetMsgType()

	// msg instanceId == acceptorInstanceId 是当前正在投票处理的消息
	// msg instanceId == acceptorInstanceId means this msg is what acceptor processing
	// so call the acceptor function to handle it
	if msgInstanceId == acceptorInstanceId {
		if msgType == MsgType_PaxosPrepare {
			return instance.acceptor.onPrepare(msg)
		} else if msgType == MsgType_PaxosAccept {
			return instance.acceptor.onAccept(msg)
		}

		// never reach here
		log.Errorf("wrong msg type %d", msgType)
		return ErrInvalidMsg
	} else if !isRetry && msgInstanceId > acceptorInstanceId {
		if msgInstanceId < acceptorInstanceId+RETRY_QUEUE_MAX_LEN {
			//need retry msg precondition
			//  1. prepare or accept msg
			//  2. msg.instanceid > nowinstanceid.
			//    (if < nowinstanceid, this msg is expire)
			//  3. msg.instanceid >= seen latestinstanceid.
			//    (if < seen latestinstanceid, proposer don't need reply with this instanceid anymore.)
			//  4. msg.instanceid close to nowinstanceid.
			instance.addRetryMsg(msg)
		} else {
			instance.clearRetryMsg()
		}
	}

	return nil
}

func (instance *Instance) OnReceivePaxosMsg(msg *PaxosMsg, isRetry bool) error {
	proposer := instance.proposer
	learner := instance.learner
	msgType := msg.GetMsgType()

	log.Infof("[%s]instance id %d, msg instance id:%d, msgtype: %d, from: %d, my node id:%d, latest instanceid %d",
		instance.name, proposer.GetInstanceId(), msg.GetInstanceID(), msgType, msg.GetNodeID(),
		instance.config.GetMyNodeId(), learner.getSeenLatestInstanceId())

	// handle msg for acceptor
	if msgType == MsgType_PaxosPrepare || msgType == MsgType_PaxosAccept {
		// 如果不是配置文件中已经配置的节点，则增加一个临时节点
		if !instance.config.IsValidNodeID(msg.GetNodeID()) {
			instance.config.AddTmpNodeOnlyForLearn(msg.GetNodeID())
			log.Errorf("[%s]is not valid node id", instance.name)
			return nil
		}

		if !instance.isCheckSumValid(msg) {
			log.Errorf("[%s]checksum invalid", instance.name)
			return ErrInvalidMsg
		}

		return instance.receiveMsgForAcceptor(msg, isRetry)
	}

	// handle paxos prepare and accept reply msg
	if msgType == MsgType_PaxosPrepareReply || msgType == MsgType_PaxosAcceptReply {
		return instance.receiveMsgForProposer(msg)
	}

	// handler msg for learner
	if msgType == MsgType_PaxosLearner_AskforLearn ||
		msgType == MsgType_PaxosLearner_SendLearnValue ||
		msgType == MsgType_PaxosLearner_ProposerSendSuccess ||
		msgType == MsgType_PaxosLearner_ConfirmAskforLearn ||
		msgType == MsgType_PaxosLearner_SendNowInstanceID ||
		msgType == MsgType_PaxosLearner_SendLearnValue_Ack ||
		msgType == MsgType_PaxosLearner_AskforCheckpoint {
		if !instance.isCheckSumValid(msg) {
			return ErrInvalidMsg
		}

		return instance.receiveMsgForLearner(msg)
	}

	log.Errorf("invalid msg %d", msgType)
	return ErrInvalidMsg
}

func (instance *Instance) OnReceiveCheckpointMsg(checkpointMsg *CheckpointMsg) {
	log.Infof("Now.InstanceID %d MsgType %d Msg.from_nodeid %d My.nodeid %d flag %d"+
		" uuid %d sequence %d checksum %d offset %d buffsize %d filepath %s",
		instance.acceptor.GetInstanceId(), checkpointMsg.GetMsgType(), checkpointMsg.GetNodeID(),
		instance.config.MyNodeId, checkpointMsg.GetFlag(), checkpointMsg.GetUUID(), checkpointMsg.GetSequence(),
		checkpointMsg.GetChecksum(), checkpointMsg.GetOffset(), len(checkpointMsg.GetBuffer()),
		checkpointMsg.GetFilePath())

	if checkpointMsg.GetMsgType() == CheckpointMsgType_SendFile {
		if !instance.ckMnger.InAskforcheckpointMode() {
			log.Info("not in ask for checkpoint mode, ignord checkpoint msg")
			return
		}

		instance.learner.OnSendCheckpoint(checkpointMsg)

	} else if checkpointMsg.GetMsgType() == CheckpointMsgType_SendFile_Ack {
		instance.learner.OnSendCheckpointAck(checkpointMsg)
	}

}

func (instance *Instance) OnTimeout(timer *util.Timer) {
	if timer.TimerType == Timer_Proposer_Prepare_Timeout {
		instance.proposer.onPrepareTimeout()
		return
	}

	if timer.TimerType == Timer_Proposer_Accept_Timeout {
		instance.proposer.onAcceptTimeout()
		return
	}

	if timer.TimerType == Timer_Learner_Askforlearn_noop {
		instance.learner.AskforLearnNoop()
		return
	}

	if timer.TimerType == Timer_Instance_Commit_Timeout {
		instance.OnNewValueCommitTimeout()
		return
	}

}

func (instance *Instance) OnNewValueCommitTimeout() {
	instance.proposer.exitPrepare()
	instance.proposer.exitAccept()
	instance.commitctx.setResult(PaxosTryCommitRet_Timeout, instance.proposer.GetInstanceId(), nil)
}

func (instance *Instance) OnReceiveMsg(buffer []byte, messageLen int) error {

	if instance.end {
		return nil
	}

	header := &Header{}

	body, err := UnpackBaseMsg(buffer, header)

	if err != nil {
		return err
	}

	cmd := header.GetCmdid()

	if cmd == MsgCmd_PaxosMsg {

		if instance.ckMnger.InAskforcheckpointMode() {
			log.Info("in ask for checkpoint mode, ignord paxosmsg")
		}

		var msg PaxosMsg
		err := proto.Unmarshal(body, &msg)
		if err != nil {
			log.Errorf("[%s]unmarshal msg error %v", instance.name, err)
			return err
		}
		instance.paxosMsgChan <- &msg

	} else if cmd == MsgCmd_CheckpointMsg {
		var msg CheckpointMsg
		err := proto.Unmarshal(body, &msg)
		if err != nil {
			log.Errorf("[%s]unmarshal msg error %v", instance.name, err)
			return err
		}

		if !instance.ReceiveMsgHeaderCheck(header, msg.GetNodeID()) {
			return nil
		}

		instance.OnReceiveCheckpointMsg(&msg)
	}

	return nil
}

func (instance *Instance) ReceiveMsgHeaderCheck(header *Header, fromNodeId uint64) bool {

	if instance.config.GetGid() == 0 ||
		header.GetGid() == 0 {
		return true
	}

	if instance.config.GetGid() != header.GetGid() {

		log.Errorf("Header check fail, header.gid %d config.gid %d, msg.from_nodeid %d",
			header.GetGid(), instance.config.GetGid(), fromNodeId)

		return false
	}

	return true
}

/////////////////////////////////////////////////////////////

func (instance *Instance) AddStateMachine(sm StateMachine) {
	instance.factory.AddSM(sm)
}

func (instance *Instance) SMExecute(instanceId uint64, value []byte,
	isMyCommit bool, smCtx *SMCtx) error {

	return instance.factory.Execute(instance.groupId(), instanceId, value, smCtx)
}

func (instance *Instance) groupId() int32 {
	return instance.config.GetMyGroupId()
}

func (instance *Instance) GetCommitter() *Committer {
	return instance.committer
}
