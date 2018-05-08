package gphxpaxos

import "gphxpaxos/util"

type CheckpointManager struct {
	config     *Config
	logStorage LogStorage
	factory    *SMFac
	cleaner    *Cleaner
	replayer   *Replayer

	minChosenInstanceId    uint64  // TODO ???
	maxChosenInstanceId    uint64  // TODO ???
	inAskforCheckpointMode bool
	useCheckpointReplayer  bool

	needAskSet               map[uint64]bool
	lastAskforCheckpointTime uint64
}

func NewCheckpointManager(config *Config, factory *SMFac,
	logStorage LogStorage, useReplayer bool) *CheckpointManager {

	mnger := &CheckpointManager{
		config:                config,
		logStorage:            logStorage,
		factory:               factory,
		useCheckpointReplayer: useReplayer,
	}

	mnger.cleaner = NewCleaner(config, factory, logStorage, mnger)
	if useReplayer {
		mnger.replayer = NewReplayer(config, factory, logStorage, mnger)
	}
	return mnger
}

func (checkpointManager *CheckpointManager) Init() error {
	instanceId, err := checkpointManager.logStorage.GetMinChosenInstanceId(
		checkpointManager.config.GetMyGroupId())

	if err != nil {
		return err
	}

	checkpointManager.minChosenInstanceId = instanceId
	// TODO 如果logStore中保存的MinChosenInstanceId不对，是由什么原因引起的，有什么影响 ???
	err = checkpointManager.cleaner.FixMinChosenInstanceID(checkpointManager.minChosenInstanceId)
	if err != nil {
		return err
	}
	return nil
}

func (checkpointManager *CheckpointManager) Start() {
	checkpointManager.cleaner.Start()
	if checkpointManager.useCheckpointReplayer {
		checkpointManager.replayer.Start()
	}
}

func (checkpointManager *CheckpointManager) Stop() {
	if checkpointManager.useCheckpointReplayer {
		checkpointManager.replayer.Stop()
	}
	checkpointManager.cleaner.Stop()
}

func (checkpointManager *CheckpointManager) GetReplayer() *Replayer {
	return checkpointManager.replayer
}

func (checkpointManager *CheckpointManager) GetCleaner() *Cleaner {
	return checkpointManager.cleaner
}

func (checkpointManager *CheckpointManager) PrepareForAskforCheckpoint(sendNodeId uint64) error {
	checkpointManager.needAskSet[sendNodeId] = true
	if checkpointManager.lastAskforCheckpointTime == 0 {
		checkpointManager.lastAskforCheckpointTime = util.NowTimeMs()
	}

	now := util.NowTimeMs()
	if now >= checkpointManager.lastAskforCheckpointTime+60000 {

	} else {
		if len(checkpointManager.needAskSet) < checkpointManager.config.GetMajorityCount() {

		}
	}

	checkpointManager.lastAskforCheckpointTime = 0
	checkpointManager.inAskforCheckpointMode = true

	return nil
}

func (checkpointManager *CheckpointManager) InAskforcheckpointMode() bool {
	return checkpointManager.inAskforCheckpointMode
}


func (checkpointManager *CheckpointManager) GetMinChosenInstanceID() uint64 {
	return checkpointManager.minChosenInstanceId
}

func (checkpointManager *CheckpointManager) GetMaxChosenInstanceID() uint64 {
	return checkpointManager.maxChosenInstanceId
}

func (checkpointManager *CheckpointManager) SetMaxChosenInstanceId(instanceId uint64) {
	checkpointManager.maxChosenInstanceId = instanceId
}


func (checkpointManager *CheckpointManager) SetMinChosenInstanceId(instanceId uint64) error {

	options := &WriteOptions{
		Sync: true,
	}

	err := checkpointManager.logStorage.SetMinChosenInstanceId(options, checkpointManager.groupId(), instanceId)
	if err != nil {
		return err
	}

	checkpointManager.minChosenInstanceId = instanceId
	return nil
}

// TODO checkoutpoint即快照，是对状态机的状态做的快照，多个状态机如何处理 ???
func (checkpointManager *CheckpointManager) GetCheckpointInstanceID() uint64 {
	return checkpointManager.factory.GetCheckpointInstanceId(checkpointManager.groupId())
}

func (checkpointManager *CheckpointManager) groupId() int32 {
	return checkpointManager.config.GetMyGroupId()
}
