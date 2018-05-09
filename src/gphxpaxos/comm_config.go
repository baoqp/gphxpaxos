package gphxpaxos

import (
	"math"
	"gphxpaxos/util"
	log "github.com/sirupsen/logrus"
)

const TmpNodeTimeout = 60000

type Config struct {
	*Options

	MyNodeId  uint64
	MyGroupId int32

	IsFollower     bool
	followToNodeId uint64

	SystemVSM          *SystemVSM
	MasterStateMachine *MasterStateMachine

	MyFollowerMap       map[uint64]uint64
	TmpNodeOnlyForLearn map[uint64]uint64

	MajorCnt int
}

func NewConfig(options *Options, groupId int32) *Config {
	return &Config{
		MyNodeId: options.MyNodeInfo.NodeId,
		Options:   options,
		MyGroupId: groupId,
		MajorCnt:  int(math.Floor(float64(len(options.NodeInfoList))/2)) + 1,
	}
}

func (config *Config) Init() error {
	config.SystemVSM = NewSystemVSM(config.MyGroupId, config.MyNodeId, config.LogStorage,
		config.MembershipChangeCallback)
	err := config.SystemVSM.Init()
	if err != nil {
		return err
	}

	config.SystemVSM.AddNodeIDList(config.NodeInfoList)

	return nil
}

func (config *Config) CheckConfig() bool {

	if !config.SystemVSM.IsIMInMembership() {
		log.Errorf("my node %d is not in membership", config.MyNodeId)
		return false
	}

	return true
}

func (config *Config) GetOptions() *Options {
	return config.Options
}

func (config *Config) LogSync() bool {
	return config.Options.Sync
}

func (config *Config) SetLogSync(logSync bool) {
	config.Options.Sync = logSync
}

func (config *Config) SyncInterval() int {
	return config.Options.SyncInternal
}

func (config *Config) GetMyGroupId() int32 {
	return config.MyGroupId
}

func (config *Config) GetGroupCount() int {
	return config.GroupCount
}

func (config *Config) GetGid() uint64 {
	return config.SystemVSM.GetGid()
}

func (config *Config) GetMyNodeId() uint64 {
	return config.MyNodeId
}

func (config *Config) GetMajorityCount() int {
	return config.MajorCnt
}

func (config *Config) GetNodeCount() int {
	return config.SystemVSM.GetNodeCount()
}

func (config *Config) IsIMFollower() bool {
	return config.IsFollower
}

func (config *Config) GetFollowToNodeID() uint64 {
	return config.followToNodeId
}

func (config *Config) GetMyFollowerCount() int {

	return len(config.MyFollowerMap)
}

func (config *Config) AddFollowerNode(followerNodeId uint64) {
	config.MyFollowerMap[followerNodeId] = util.NowTimeMs() + uint64(GetAskforLearnInterval()*3)
}

func (config *Config) GetMyFollowerMap() map[uint64]uint64 {
	nowTime := util.NowTimeMs()

	for k, v := range config.MyFollowerMap {
		if v < nowTime {
			log.Errorf("follower %d timeout, nowtimems %d tmpnode last add time %d",
				k, nowTime, v)
			delete(config.MyFollowerMap, k)
		}
	}

	return config.MyFollowerMap
}

// 从状态机获取表示成员的NodeInfo
func (config *Config) AddTmpNodeOnlyForLearn(nodeId uint64) {
	var nodesInfos = NodeInfoList{}
	var version uint64
	config.SystemVSM.GetMembership(&nodesInfos, &version)

	for _, nodeInfo := range nodesInfos {
		if nodeInfo.NodeId == nodeId {
			return
		}
	}
	config.TmpNodeOnlyForLearn[nodeId] = util.NowTimeMs() + TmpNodeTimeout
}

func (config *Config) GetTmpNodeMap() map[uint64]uint64 {
	nowTime := util.NowTimeMs()

	for k, v := range config.TmpNodeOnlyForLearn {
		if v < nowTime {
			log.Errorf("tmpnode %d timeout, nowtimems %d tmpnode last add time %d",
				k, nowTime, v)
			delete(config.TmpNodeOnlyForLearn, k)
		}
	}

	return config.TmpNodeOnlyForLearn
}

func (config *Config) GetSystemVSM() *SystemVSM {
	return config.SystemVSM
}

func (config *Config) SetMasterSM(masterSM *MasterStateMachine) {
	config.MasterStateMachine = masterSM
}

func (config *Config) GetMasterSM() *MasterStateMachine {
	return config.MasterStateMachine
}

func (config *Config) IsUseMembership() bool {
	return config.UseMemebership
}

func (config *Config) IsValidNodeID(nodeId uint64) bool {
	return config.SystemVSM.IsValidNodeID(nodeId)
}

func (config *Config) GetAskforLearnTimeoutMs() int {
	return 2000
}

func (config *Config) GetPrepareTimeoutMs() uint32 {
	return 3000
}

func (config *Config) GetAcceptTimeoutMs() uint32 {
	return 3000
}
