package echo

import (
	"gphxpaxos"
	"fmt"
	log "github.com/sirupsen/logrus"
)

type EchoServer struct {
	myNode    gphxpaxos.NodeInfo
	nodeList  gphxpaxos.NodeInfoList
	paxosNode *gphxpaxos.Node
	echoSM    *EchoSM
}

func (s *EchoServer) MakeLogStoragePath() string {
	path := fmt.Sprintf("./log_%s_%d", s.myNode.Ip, s.myNode.Port)
	return path
}

func (s *EchoServer) RunPaxos() error {
	options := &gphxpaxos.Options{}
	logPath := s.MakeLogStoragePath()
	options.LogStoragePath = logPath
	options.GroupCount = 1
	options.MyNodeInfo = s.myNode
	options.NodeInfoList = s.nodeList
	groupSMInfo := &gphxpaxos.GroupSMInfo{}
	groupSMInfo.GroupIdx = 0
	groupSMInfo.SMList = append(groupSMInfo.SMList,s.echoSM)
	options.GroupSMInfoList = append(options.GroupSMInfoList, groupSMInfo)

	s.paxosNode = &gphxpaxos.Node{}
	err := s.paxosNode.RunNode(options)



	if err != nil {
		return err
	}
	log.Info("run paxos ok")
	return nil
}


func (s *EchoServer) Echo(reqValue string) (string, error) {
	echoSMCTX := &EchoSMCtx{}
	ctx := &gphxpaxos.SMCtx{SMID:1, PCtx: echoSMCTX}
	instanceId := uint64(0)
	err := s.paxosNode.ProposeWithCtx(0, []byte(reqValue), &instanceId, ctx)
	if err != nil {
		return "", err
	}

	if echoSMCTX.executeRet != 0 {
		return "", fmt.Errorf("echo sm excute fail, excuteret %d", echoSMCTX.executeRet)
	}

	return string(echoSMCTX.echoRespValue), nil
}
