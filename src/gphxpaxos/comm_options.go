package gphxpaxos

import (
	"fmt"
	"gphxpaxos/util"
	"strings"
	"strconv"
)

type NodeInfoList []*NodeInfo

type NodeInfo struct {
	Ip     string
	Port   int
	NodeId uint64
}

func (nodeInfo *NodeInfo) String() string {
	return fmt.Sprintf("%s:%d", nodeInfo.Ip, nodeInfo.Port)
}

func MakeNodeId(nodeInfo *NodeInfo) {
	ip := util.Inet_addr(nodeInfo.Ip)
	// 高32位是ip, 低32位是port
	nodeInfo.NodeId = uint64(ip)<<32 | uint64(nodeInfo.Port)
}

func FromString(pcStr string) *NodeInfo {
	nodeInfo := &NodeInfo{}
	pcArr := strings.Split(pcStr, ":")
	nodeInfo.Ip = pcArr[0]
	nodeInfo.Port, _ = strconv.Atoi(pcArr[1])
	MakeNodeId(nodeInfo)
	return nodeInfo
}

func (nodeInfo *NodeInfo) ParseNodeId() {
	nodeInfo.Port = int(nodeInfo.NodeId & (0xffffffff))
	ip := uint32(nodeInfo.NodeId >> 32)
	nodeInfo.Ip = inet_ntoa(ip)
}

func inet_ntoa(ip uint32) string {
	return fmt.Sprintf("%d.%d.%d.%d", byte(ip), byte(ip>>8), byte(ip>>16), byte(ip>>24))
}

func NewNodeInfoWithId(nodeId uint64) *NodeInfo {
	nodeInfo := &NodeInfo{NodeId: nodeId}
	nodeInfo.ParseNodeId()
	return nodeInfo
}

func NewNodeInfo(ip string, port int) *NodeInfo {
	nodeInfo := &NodeInfo{
		Ip:   ip,
		Port: port,
	}
	MakeNodeId(nodeInfo)
	return nodeInfo
}

type FollowerNodeInfoList []*FollowerNodeInfo

type FollowerNodeInfo struct {
	MyNode     NodeInfo
	FollowNode NodeInfo
}

// 两个回调函数
type MembershipChangeCallback func(groupidx int32, list NodeInfoList)
type MasterChangeCallback func(groupidx int32, nodeInfo *NodeInfo, version uint64)

// group的状态机数据
type GroupSMInfo struct {
	GroupIdx    int32
	SMList      []StateMachine
	IsUseMaster bool // 是否使用内置的状态机来进行master选举
}

type GroupSMInfoList []*GroupSMInfo

type Options struct {
	LogStorage                   LogStorage
	LogStoragePath               string
	Sync                         bool
	SyncInternal                 int
	NetWork                      NetWork
	GroupCount                   int
	UseMemebership               bool
	MyNodeInfo                   NodeInfo
	NodeInfoList                 NodeInfoList
	MembershipChangeCallback     MembershipChangeCallback
	MasterChangeCallback         MasterChangeCallback
	GroupSMInfoList              GroupSMInfoList
	FollowerNodeInfoList         FollowerNodeInfoList
	UseCheckpointReplayer        bool
	UseBatchPropose              bool
	OpenChangeValueBeforePropose bool
	IsLargeValueMode             bool
}
