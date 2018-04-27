package gphxpaxos

import (
	"errors"
	"github.com/golang/protobuf/proto"
	"fmt"
	"gphxpaxos/util"
	log "github.com/sirupsen/logrus"
)

type Node struct {
	GroupList        []*Group
	MasterList       []*MasterMgr
	ProposeBatchList []*ProposeBatch

	defaultLogStorage *MultiDatabase
	defaultNetWork    NetWork
	myNodeId          uint64
}

var LOGPATHERR = errors.New("LogStorage Path is null")

func (node *Node) RunNode(options *Options)  error {

	if options.IsLargeValueMode {
		SetAsLargeBufferMode()
	}
	SetGroupCount(options.GroupCount)
	network, err := node.Init(options)
	if err != nil {
		return err
	}

	network.SetNode(node)
	network.RunNetWork()
	return  nil
}

func (node *Node) InitLogStorage(options *Options) (LogStorage, error) {
	if len(options.LogStoragePath) == 0 {
		return nil, LOGPATHERR
	}

	node.defaultLogStorage = &MultiDatabase{}
	err := node.defaultLogStorage.Init(options.LogStoragePath, options.GroupCount)

	if err != nil {
		return nil, err
	}

	return node.defaultLogStorage, nil
}

func (node *Node) InitNetWork(options *Options) (NetWork, error) {
	if options.NetWork != nil {
		return options.NetWork, nil
	}

	dfNet := NewDefaultNetWork(options.MyNodeInfo.Ip, options.MyNodeInfo.Port)
	err := dfNet.Init()

	if err != nil {
		log.Errorf("init default network fail, ret %v", err)
		return nil, err
	}
	node.defaultNetWork = dfNet
	log.Info("use default network")

	return dfNet, nil

}

// 检查options设置是否合理
func (node *Node) CheckOptions(options *Options) error {

	if len(options.LogStoragePath) == 0 && options.LogStorage == nil {
		return errors.New("no logpath and logstorage is null")
	}

	if options.GroupCount > 200 {
		return fmt.Errorf("group count %d is too large", options.GroupCount)
	}

	if options.GroupCount <= 0 {
		return fmt.Errorf("group count %d is small than zero or equal to zero", options.GroupCount)
	}

	for _, followerNode := range options.FollowerNodeInfoList {
		if followerNode.MyNode.NodeId == followerNode.FollowNode.NodeId {
			return fmt.Errorf("self node ip %s port %d equal to follow node",
				followerNode.MyNode.Ip, followerNode.MyNode.Port)
		}
	}

	for _, groupSMInfo := range options.GroupSMInfoList {
		if groupSMInfo.GroupIdx >= int32(options.GroupCount) {
			return fmt.Errorf("SM GroupIdx %d large than GroupCount %d",
				groupSMInfo.GroupIdx, options.GroupCount)
		}
	}

	return nil
}

func (node *Node) InitStateMachine(options *Options) {
	for _, groupSMInfo := range options.GroupSMInfoList {
		for _, sm := range groupSMInfo.SMList {
			node.AddStateMachine(groupSMInfo.GroupIdx, sm)
		}
	}
}

func (node *Node) RunMaster(options *Options) {
	for _, groupSMInfo := range options.GroupSMInfoList {
		if groupSMInfo.IsUseMaster {
			if !node.GroupList[groupSMInfo.GroupIdx].config.IsIMFollower() {
				node.MasterList[groupSMInfo.GroupIdx].RunMaster()
			}
		}
	}
}

func (node *Node) RunProposalBatch() {
	for _, proposalBatch := range node.ProposeBatchList {
		proposalBatch.Start()
	}
}

func (node *Node) Init(options *Options) (NetWork, error) {
	err := node.CheckOptions(options)
	if err != nil {
		return nil, err
	}

	node.myNodeId = options.MyNodeInfo.NodeId

	//step1 init logstorage
	logStorage, err := node.InitLogStorage(options)
	if err != nil {
		return nil,err
	}
	options.LogStorage = logStorage

	//step2 init network
	network, err := node.InitNetWork(options)
	if err != nil {
		return nil, err
	}

	//step3 build masterlist
	for i := 0; i < options.GroupCount; i++ {
		masterMgr := NewMasterMgr(node, int32(i), logStorage, options.MasterChangeCallback)
		node.MasterList = append(node.MasterList, masterMgr)

		err := masterMgr.Init()
		if err != nil {
			return nil, err
		}
	}

	//step4 build grouplist
	for i := 0; i < options.GroupCount; i++ {
		group := NewGroup(logStorage, network, node.MasterList[i].GetMasterSM(), int32(i), options)
		node.GroupList = append(node.GroupList, group)
	}

	//step5 build batchpropose
	if options.UseBatchPropose {
		for i := 0; i < options.GroupCount; i++ {
			proposalBatch := NewProposeBatch(int32(i), node)
			node.ProposeBatchList = append(node.ProposeBatchList, proposalBatch)
		}
	}

	//step6 init statemachine
	node.InitStateMachine(options)

	//step7 init group  TODO init group parallel
	for _, group := range node.GroupList {
		group.StartInit()
	}

	for _, group := range node.GroupList {
		group.Start()
	}

	node.RunMaster(options)
	node.RunProposeBatch()

	return network, nil

}

func (node *Node) RunProposeBatch() {
	for _, proposeBatch := range node.ProposeBatchList {
		proposeBatch.Start()
	}
}

func (node *Node) CheckGroupId(groupId int32) bool {
	if groupId < 0 || int(groupId) >= len(node.GroupList) {
		return false
	}
	return true
}

//Base function.
func (node *Node) Propose(groupIdx int32, value []byte, instanceId *uint64) error {

	return node.ProposeWithCtx(groupIdx, value, instanceId, nil)
}

func (node *Node) ProposeWithCtx(groupIdx int32, value []byte, instanceId *uint64, smCtx *SMCtx) error {
	if !node.CheckGroupId(groupIdx) {
		return Paxos_GroupIdxWrong
	}

	var err error
	*instanceId, err = node.GroupList[int(groupIdx)].GetCommitter().NewValueGetID(value, smCtx)
	if err == PaxosTryCommitRet_OK {
		return nil
	}
	return err

}

func (node *Node) GetNowInstanceID(groupIdx int32) uint64 {

	if !node.CheckGroupId(groupIdx) {
		return INVALID_VERSION
	}

	return node.GroupList[int(groupIdx)].GetInstance().GetNowInstanceId()

}

func (node *Node) OnReceiveMessage(message []byte, messageLen int) {
	var groupId int32 = -1
	util.DecodeInt32(message, 0, &groupId)

	if node.CheckGroupId(groupId) {
		log.Errorf("Message groupid %d wrong, groupsize %d", groupId, len(node.GroupList))
	}

	node.GroupList[groupId].instance.OnReceiveMsg(message, messageLen)
}

//This function will add state machine to all group.
func (node *Node) AddStateMachineToAllGroup(sm StateMachine) {
	for _, group := range node.GroupList {
		group.AddStateMachine(sm)
	}
}

func (node *Node) AddStateMachine(groupIdx int32, sm StateMachine) {
	if !node.CheckGroupId(groupIdx) {
		return
	}

	node.GroupList[int(groupIdx)].AddStateMachine(sm)
}

func (node *Node) GetMyNodeId() uint64 {
	return node.myNodeId
}

//Timeout control.
func (node *Node) SetTimeoutMs(timeoutMs uint32) {
	for _, group := range node.GroupList {
		group.GetCommitter().SetTimeoutMs(timeoutMs)
	}
}

func (node *Node) GetMinChosenInstanceID(groupIdx int) error { return nil }

//State machine.

//-----------------------------------Checkpoint--------------------------------------//

//Set the number you want to keep paxoslog's count.
//We will only delete paxoslog before checkpoint instanceid.
//If llHoldCount < 300, we will set it to 300. Not suggest too small holdcount.
func (node *Node) SetHoldPaxosLogCount(holdCount uint64) {
	for _, group := range node.GroupList {
		group.GetCheckpointCleaner().SetHoldPaxosLogCount(holdCount)
	}
}

//Replayer is to help sm make checkpoint.
//Checkpoint replayer default is paused, if you not use this, ignord this function.
//If sm use ExecuteForCheckpoint to make checkpoint, you need to run replayer(you can run in any time).

//Pause checkpoint replayer.
func (node *Node) PauseCheckpointReplayer() {
	for _, group := range node.GroupList {
		group.GetCheckpointReplayer().Pause()
	}
}

//Continue to run replayer
func (node *Node) ContinueCheckpointReplayer() {
	for _, group := range node.GroupList {
		group.GetCheckpointReplayer().Continue()
	}
}

//Paxos log cleaner working for deleting paxoslog before checkpoint instanceid.
//Paxos log cleaner default is pausing.

//pause paxos log cleaner.
func (node *Node) PausePaxosLogCleaner() {
	for _, group := range node.GroupList {
		group.GetCheckpointCleaner().Pause()
	}
}

//Continue to run paxos log cleaner.
func (node *Node) ContinuePaxosLogCleaner() {
	for _, group := range node.GroupList {
		group.GetCheckpointCleaner().Continue()
	}
}

//------------------------------------Membership---------------------------------------//

func (node *Node) ProposalMembership(systemVM *SystemVSM, groupIdx int32,
	nodeInfoList NodeInfoList, version uint64) error {

	var value = make([]byte, 0)
	err := systemVM.Membership_OPValue(nodeInfoList, version, &value)

	if err != nil {
		return Paxos_SystemError
	}

	ctx := &SMCtx{SMID: SYSTEM_V_SMID, PCtx: -1}
	var instanceId uint64
	err = node.ProposeWithCtx(groupIdx, value, &instanceId, ctx)
	if err != nil {
		return err
	}
	return nil
}

//Show now membership.
func (node *Node) ShowMembership(groupIdx int32, nodeInfoList *NodeInfoList) error {

	if !node.CheckGroupId(groupIdx) {
		return Paxos_GroupIdxWrong
	}

	systemVSM := node.GroupList[groupIdx].GetConfig().GetSystemVSM()
	version := uint64(0)
	systemVSM.GetMembership(nodeInfoList, &version)

	return nil
}

//Add a paxos node to membership.
func (node *Node) AddMember(groupIdx int32, oNode *NodeInfo) error {

	if !node.CheckGroupId(groupIdx) {
		return Paxos_GroupIdxWrong
	}

	systemVSM := node.GroupList[int(groupIdx)].GetConfig().GetSystemVSM()

	if systemVSM.GetGid() == 0 {
		return Paxos_MembershipOp_NoGid
	}

	version := uint64(0)
	var nodeInfoList = NodeInfoList{}
	systemVSM.GetMembership(&nodeInfoList, &version)

	for _, nodeInfo := range nodeInfoList {
		if nodeInfo.NodeId == oNode.NodeId {
			return Paxos_MembershipOp_NoGid
		}
	}

	nodeInfoList = append(nodeInfoList, oNode)

	return node.ProposalMembership(systemVSM, groupIdx, nodeInfoList, version)
}

//Remove a paxos node from membership.
func (node *Node) RemoveMember(groupIdx int32, oNode *NodeInfo) error {
	if !node.CheckGroupId(groupIdx) {
		return Paxos_GroupIdxWrong
	}

	systemVSM := node.GroupList[int(groupIdx)].GetConfig().GetSystemVSM()

	if systemVSM.GetGid() == 0 {
		return Paxos_MembershipOp_NoGid
	}

	version := uint64(0)
	var nodeInfoList = NodeInfoList{}
	systemVSM.GetMembership(&nodeInfoList, &version)

	var nodeInfoListAfter = NodeInfoList{}
	nodeExist := false
	for _, nodeInfo := range nodeInfoList {
		if nodeInfo.NodeId == oNode.NodeId {
			nodeExist = true
		} else {
			nodeInfoListAfter = append(nodeInfoListAfter, nodeInfo)
		}
	}

	if !nodeExist {
		return Paxos_MembershipOp_Remove_NodeNotExist
	}

	return node.ProposalMembership(systemVSM, groupIdx, nodeInfoList, version)

}

//Change membership by one node to another node.
func (node *Node) ChangeMember(groupIdx int32, fromNode *NodeInfo, toNode *NodeInfo) error {
	if !node.CheckGroupId(groupIdx) {
		return Paxos_GroupIdxWrong
	}

	systemVSM := node.GroupList[int(groupIdx)].GetConfig().GetSystemVSM()

	if systemVSM.GetGid() == 0 {
		return Paxos_MembershipOp_NoGid
	}

	version := uint64(0)
	var nodeInfoList = NodeInfoList{}
	systemVSM.GetMembership(&nodeInfoList, &version)

	var nodeInfoListAfter = NodeInfoList{}
	fromNodeExist := false
	toNodeExist := false

	for _, nodeInfo := range nodeInfoList {
		if nodeInfo.NodeId == fromNode.NodeId {
			fromNodeExist = true
			continue
		} else if nodeInfo.NodeId == toNode.NodeId {
			toNodeExist = true
			continue
		}

		nodeInfoListAfter = append(nodeInfoListAfter, nodeInfo)

	}

	if !fromNodeExist && toNodeExist {
		return Paxos_MembershipOp_Change_NoChange
	}

	nodeInfoListAfter = append(nodeInfoListAfter, toNode)

	return node.ProposalMembership(systemVSM, groupIdx, nodeInfoList, version)
}

//------------------------------------Master---------------------------------------//

//Check who is master.
func (node *Node) GetMaster(groupIdx int32) *NodeInfo {

	if !node.CheckGroupId(groupIdx) {
		return &NodeInfo{NodeId: NULL_NODEID}
	}

	nodeInfo := NewNodeInfoWithId(node.MasterList[int(groupIdx)].GetMasterSM().GetMaster())
	return nodeInfo
}

//Check who is master and get version.
func (node *Node) GetMasterWithVersion(groupIdx int32, version uint64) *NodeInfo {
	if !node.CheckGroupId(groupIdx) {
		return &NodeInfo{NodeId: NULL_NODEID}
	}

	nodeInfo := NewNodeInfoWithId(node.MasterList[int(groupIdx)].GetMasterSM().GetMasterWithVersion(&version))

	return nodeInfo

}

//Check is i'm master.
func (node *Node) IsIMMaster(groupIdx int32) bool {
	if !node.CheckGroupId(groupIdx) {
		return false
	}

	return node.MasterList[int(groupIdx)].GetMasterSM().IsIMMaster()
}

func (node *Node) SetMasterLease(groupIdx int32, leaseTimeMs int) error {
	if !node.CheckGroupId(groupIdx) {
		return Paxos_GroupIdxWrong
	}

	node.MasterList[int(groupIdx)].SetLeaseTime(leaseTimeMs)
	return nil
}

func (node *Node) DropMaster(groupIdx int32) error {
	if !node.CheckGroupId(groupIdx) {
		return Paxos_GroupIdxWrong
	}

	node.MasterList[int(groupIdx)].DropMaster()
	return nil
}

//Qos

//If many threads propose same group, that some threads will be on waiting status.
//Set max hold threads, and we will reject some propose request to avoid to many threads be holded.
//Reject propose request will get retcode(PaxosTryCommitRet_TooManyThreadWaiting_Reject), check on def.h.
func (node *Node) SetMaxHoldThreads(groupIdx int32, maxHoldThreads int) {
	if !node.CheckGroupId(groupIdx) {
		return
	}
	node.GroupList[int(groupIdx)].GetCommitter().SetMaxHoldThreads(int32(maxHoldThreads))
}

//To avoid threads be holded too long time, we use this threshold to reject some propose to control thread's wait time.
func (node *Node) SetProposeWaitTimeThresholdMS(groupIdx int32, iWaitTimeThresholdMS int) {
	// TODO
}

//write disk
func (node *Node) SetLogSync(groupIdx int32, logSync bool) {
	if !node.CheckGroupId(groupIdx) {
		return
	}
	// TODO
}

//Not suggest to use this function
//pair: value,smid.
//Because of BatchPropose, a InstanceID maybe include multi-value.

type ValuePair struct {
	Value []byte
	SMID  int32
}

func (node *Node) GetInstanceValue(groupIdx int32, instanceId uint64, retValues *[]*ValuePair) error {
	if !node.CheckGroupId(groupIdx) {
		return Paxos_GroupIdxWrong
	}

	value, smid, err := node.GroupList[int(groupIdx)].GetInstance().GetInstanceValue(instanceId)

	if err != nil {
		return err
	}

	if smid == BATCH_PROPOSE_SMID {
		var batchValues = &BatchPaxosValues{}
		err = proto.Unmarshal(value, batchValues)
		if err != nil {
			return Paxos_SystemError
		}

		for _, value := range batchValues.Values {
			valuePair := &ValuePair{Value: value.GetValue(), SMID: value.GetSMID()}
			*retValues = append(*retValues, valuePair)
		}

	} else {
		valuePair := &ValuePair{value, smid}
		*retValues = append(*retValues, valuePair)
	}

	return nil
}

//-----------------------------------BatchPropose--------------------------------------//

//Only set options::bUserBatchPropose as true can use this batch API.
//Warning: BatchProposal will have same llInstanceID returned but different iBatchIndex.
//Batch values's execute order in StateMachine is certain, the return value iBatchIndex
//means the execute order index, start from 0.
func (node *Node) BatchPropose(groupIdx int32, value []byte, instanceId uint64, batchIndex uint32) error {
	return node.BatchProposeWithCtx(groupIdx, value, instanceId, batchIndex, nil)
}

func (node *Node) BatchProposeWithCtx(groupIdx int32, value []byte, instanceId uint64, batchIndex uint32, smCtx *SMCtx) error {

	if !node.CheckGroupId(groupIdx) {
		return Paxos_GroupIdxWrong
	}

	if len(node.ProposeBatchList) == 0 {
		return Paxos_SystemError
	}

	return node.ProposeBatchList[int(groupIdx)].Propose(value, instanceId, batchIndex, smCtx)

}

//PhxPaxos will batch proposal while waiting proposals count reach to BatchCount,
//or wait time reach to BatchDelayTimeMs.
func (node *Node) SetBatchCount(groupIdx int32, batchCount int) {

	if !node.CheckGroupId(groupIdx) {
		return
	}

	if len(node.ProposeBatchList) == 0 {
		return
	}

	node.ProposeBatchList[int(groupIdx)].SetBatchCount(batchCount)
}

func (node *Node) SetBatchDelayTimeMs(groupIdx int32, delay uint64) {
	if !node.CheckGroupId(groupIdx) {
		return
	}

	if len(node.ProposeBatchList) == 0 {
		return
	}

	node.ProposeBatchList[int(groupIdx)].SetBatchDelayTimeMs(delay)
}
