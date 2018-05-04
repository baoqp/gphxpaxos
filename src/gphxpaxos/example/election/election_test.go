package election

import (
	"gphxpaxos"
	"strings"
	"testing"
	"fmt"
	"gphxpaxos/util"
)

func parseNode(nodeInfoStr string) (*gphxpaxos.NodeInfo, *gphxpaxos.NodeInfoList) {
	nodeArr := strings.Split(nodeInfoStr, " ")
	myNode := gphxpaxos.FromString(nodeArr[0])
	nodeListStr := nodeArr[1]
	nodeArr = strings.Split(nodeListStr, ",")
	nodeList := gphxpaxos.NodeInfoList{}
	for _, nodeInfo := range nodeArr {
		node := gphxpaxos.FromString(nodeInfo)
		nodeList = append(nodeList, node)
	}
	return myNode, &nodeList
}

func TestEcho(t *testing.T) {
	go election1()
	go election2()
	go election3()
	select {}
}

func election1() {
	myNode, nodeList := parseNode("127.0.0.1:11111 127.0.0.1:11111,127.0.0.1:11112,127.0.0.1:11113")

	election := &Election{myNode: *myNode, nodeList: *nodeList}

	err := election.RunPaxos()
	fmt.Printf("myNodeIs is %d \r\n", election.myNode.NodeId)

	if err != nil {
		fmt.Printf("run paxos failed, %v", err)
	} else {
		for {
			util.SleepMs(1000)
			version := uint64(0)
			masterNode := election.GetMasterWithVersion(version)
			fmt.Printf("master: nodeid %d version %d ip %s port %d\n", masterNode.NodeId,
				version, masterNode.Ip, masterNode.Port)
		}
	}

}


func election2() {
	myNode, nodeList := parseNode("127.0.0.1:11112 127.0.0.1:11111,127.0.0.1:11112,127.0.0.1:11113")

	election := &Election{myNode: *myNode, nodeList: *nodeList}

	err := election.RunPaxos()
	fmt.Printf("myNodeIs is %d \r\n", election.myNode.NodeId)

	if err != nil {
		fmt.Printf("run paxos failed, %v", err)
	} else {
		for {
			util.SleepMs(1000)
			version := uint64(0)
			masterNode := election.GetMasterWithVersion(version)
			fmt.Printf("master: nodeid %d version %d ip %s port %d\n", masterNode.NodeId,
				version, masterNode.Ip, masterNode.Port)
		}
	}

}



func election3() {
	myNode, nodeList := parseNode("127.0.0.1:11113 127.0.0.1:11111,127.0.0.1:11112,127.0.0.1:11113")

	election := &Election{myNode: *myNode, nodeList: *nodeList}

	err := election.RunPaxos()
	fmt.Printf("myNodeIs is %d \r\n", election.myNode.NodeId)

	if err != nil {
		fmt.Printf("run paxos failed, %v", err)
	} else {
		for {
			util.SleepMs(1000)
			version := uint64(0)
			masterNode := election.GetMasterWithVersion(version)
			fmt.Printf("master: nodeid %d version %d ip %s port %d\n", masterNode.NodeId,
				version, masterNode.Ip, masterNode.Port)
		}
	}

}