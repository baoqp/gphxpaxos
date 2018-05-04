package election

import (
	"gphxpaxos"
	"fmt"
	log "github.com/sirupsen/logrus"
)

type Election struct {
	myNode    gphxpaxos.NodeInfo
	nodeList  gphxpaxos.NodeInfoList
	paxosNode *gphxpaxos.Node
}

func (e *Election) MakeLogStoragePath() string {
	path := fmt.Sprintf("./log_%s_%d", e.myNode.Ip, e.myNode.Port)
	return path
}

func (e *Election) OnMasterChange(groupIdx int, newMaster  gphxpaxos.NodeInfo,  version uint64) {
	fmt.Printf("master change!!! groupidx %d newmaster ip %s port %d version %d \n",
		groupIdx, newMaster.Ip, newMaster.Port, version)
}

func (e *Election) RunPaxos() error {
	options := &gphxpaxos.Options{}
	logPath := e.MakeLogStoragePath()
	options.LogStoragePath = logPath
	options.GroupCount = 1
	options.MyNodeInfo = e.myNode
	options.NodeInfoList = e.nodeList
	groupSMInfo := &gphxpaxos.GroupSMInfo{}
	groupSMInfo.GroupIdx = 0
	groupSMInfo.IsUseMaster = true
	options.GroupSMInfoList = append(options.GroupSMInfoList, groupSMInfo)

	e.paxosNode = &gphxpaxos.Node{}
	err := e.paxosNode.RunNode(options)

	if err != nil {
		return err
	}

	e.paxosNode.SetMasterLease(0, 3000)

	log.Info("run paxos ok")
	return nil
}

func (e *Election) GetMaster() *gphxpaxos.NodeInfo {
	return e.paxosNode.GetMaster(0)
}


func (e *Election) GetMasterWithVersion(version uint64) *gphxpaxos.NodeInfo {
	return e.paxosNode.GetMasterWithVersion(0, version)
}

func (e *Election) IsMaster() bool {
	return e.paxosNode.IsIMMaster(0)
}
