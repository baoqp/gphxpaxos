package gphxpaxos

import (
	"gphxpaxos/util"
	log "github.com/sirupsen/logrus"
	"time"
)

type MasterMgr struct {
	leaseTime      int
	isEnd          bool
	isStarted      bool
	myGroupId      int32
	needDropMaster bool

	paxosNode       *Node
	defaultMasterSM *MasterStateMachine
}

func NewMasterMgr(paxosNode *Node, groupId int32, logStorage LogStorage,
	callback MasterChangeCallback) *MasterMgr {

	masterMgr := &MasterMgr{isEnd: false, isStarted: false, needDropMaster: false,
		leaseTime: 10000, paxosNode: paxosNode}
	masterMgr.defaultMasterSM = NewMasterStateMachine(groupId, paxosNode.GetMyNodeId(), logStorage, callback)
	return masterMgr
}

func (mgr *MasterMgr) Init() error {
	return mgr.defaultMasterSM.Init()
}

func (mgr *MasterMgr) SetLeaseTime(leaseTimeMs int) {
	if leaseTimeMs < 10000 {
		return
	}
	mgr.leaseTime = leaseTimeMs
}

func (mgr *MasterMgr) DropMaster() {
	mgr.needDropMaster = true
}

func (mgr *MasterMgr) StopMaster() {
	if mgr.isStarted {
		mgr.isEnd = true
		// TODO how to stop
	}
}

func (mgr *MasterMgr) RunMaster() {
	mgr.Start()
}

func (mgr *MasterMgr) Start() {
	// TODO run mgr main()
}

func (mgr *MasterMgr) main() {
	mgr.isStarted = true

	for !mgr.isEnd {
		leaseTime := mgr.leaseTime
		beginTime := util.NowTimeMs()

		mgr.TryBeMaster(leaseTime)

		continueLeaseTimeout := (leaseTime - 100 ) / 4
		continueLeaseTimeout = continueLeaseTimeout / 2 + util.Rand(continueLeaseTimeout) // TODO ???

		if mgr.needDropMaster {
			mgr.needDropMaster = false
			continueLeaseTimeout = leaseTime * 2
			// 需要放弃master角色，所以等待大于租约的时间以让租约过期
			log.Infof("Need drop master, this round wait time %dms", continueLeaseTimeout)
		}


		endTime := util.NowTimeMs()

		runTime := uint64(0)
		if endTime > beginTime {
			runTime =  endTime - beginTime
		}

		needSleepTime := uint64(0)
		if uint64(continueLeaseTimeout) > runTime {
			needSleepTime = uint64(continueLeaseTimeout) - runTime
		}

		log.Infof("TryBeMaster, sleep time %dms", needSleepTime)
		time.Sleep(time.Duration(needSleepTime) * time.Millisecond)
	}
}

func (mgr *MasterMgr) TryBeMaster(leaseTime int) {
	masterNodeId := NULL_NODEID
	masterVersion := uint64(0)

	//step 1 check exist master and get version
	mgr.defaultMasterSM.SafeGetMaster(&masterNodeId, &masterVersion)
	if masterNodeId != NULL_NODEID && masterNodeId != mgr.paxosNode.GetMyNodeId() {
		log.Infof("Ohter as master, can't try be master, masterid %d myid %d",
			masterNodeId, mgr.paxosNode.GetMyNodeId())
		return
	}

	//step 2 try be master
	value, err := MakeOpValue(mgr.paxosNode.GetMyNodeId(), masterVersion,
		int32(leaseTime), MasterOperatorType_Complete)

	if err != nil {
		log.Error("Make paxos value fail")
		return
	}

	masterLeaseTimeout := uint64(leaseTime - 100)
	absMasterTimeout := util.NowTimeMs() + masterLeaseTimeout
	commitInstanceId := uint64(0)

	ctx := &SMCtx{SMID:MASTER_V_SMID, PCtx:absMasterTimeout}

	mgr.paxosNode.ProposeWithCtx(mgr.myGroupId, value, &commitInstanceId, ctx)
}

func (mgr *MasterMgr)  GetMasterSM() *MasterStateMachine {
	return mgr.defaultMasterSM
}











