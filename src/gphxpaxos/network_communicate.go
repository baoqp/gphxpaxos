package gphxpaxos

import (
	log "github.com/sirupsen/logrus"
	"fmt"
	"errors"
)

// 实现MsgTransport接口
type Communicate struct {
	cfg      *Config
	network  NetWork
	myNodeId uint64
}

func NewCommunicate(config *Config, nodeId uint64, network NetWork) *Communicate {
	return &Communicate{
		cfg:      config,
		network:  network,
		myNodeId: nodeId,
	}
}

// TODO 目前忽略sendType
func (c *Communicate) SendMessage(groupIdx int32, sendToNodeId uint64, value []byte, sendType int) error {
	MAX_VALUE_SIZE := GetMaxBufferSize()

	if len(value) > MAX_VALUE_SIZE {
		errMsg := fmt.Sprintf("Message size too large %d, max size %d, skip message",
			len(value), MAX_VALUE_SIZE)

		log.Errorf(errMsg)
		return errors.New(errMsg)
	}

	sendToNode := NewNodeInfoWithId(sendToNodeId)
	return c.network.SendMessageTCP(groupIdx, sendToNode.Ip, sendToNode.Port, value)
}

func (c *Communicate) BroadcastMessage(groupIdx int32, value []byte, sendType int) error {
	var nodeInfoList = NodeInfoList{}
	var version uint64
	c.cfg.SystemVSM.GetMembership(&nodeInfoList, &version)

	for _, nodeInfo := range nodeInfoList {
		if nodeInfo.NodeId != c.myNodeId {
			c.network.SendMessageTCP(groupIdx, nodeInfo.Ip, nodeInfo.Port, value)
		}
	}
	return nil
}

func (c *Communicate) BroadcastMessageFollower(groupIdx int32, value []byte, sendType int) error {

	for k := range c.cfg.GetMyFollowerMap() {
		if k != c.myNodeId {
			c.SendMessage(groupIdx, k, value, sendType)
		}
	}

	return nil
}

func (c *Communicate) BroadcastMessageTempNode(groupIdx int32, value []byte, sendType int) error {
	for k := range c.cfg.GetTmpNodeMap() {
		if k != c.myNodeId {
			c.SendMessage(groupIdx, k, value, sendType)
		}
	}
	return nil
}
