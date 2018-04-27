package gphxpaxos

import (
	"testing"
	"fmt"
)


func TestNodeInfo(t *testing.T) {
	nodeInfo := &NodeInfo{
	Ip:   "127.0.0.1",
	Port: 8080,
	}

	MakeNodeId(nodeInfo)

	fmt.Println(nodeInfo.NodeId)

	nodeId := nodeInfo.NodeId
	nodeInfo_c := &NodeInfo{
	NodeId : nodeId,
	}
	nodeInfo_c.ParseNodeId()

	fmt.Printf("%s:%d", nodeInfo_c.Ip, nodeInfo_c.Port)
}

func TestConfig(t *testing.T) {

}
