package main

import (
	"strings"
	"fmt"

	"gphxpaxos"
	"gphxpaxos/example/gphxkv"
	"flag"
)

func parseNode(nodeInfoStr string) (gphxpaxos.NodeInfo, gphxpaxos.NodeInfoList) {
	nodeArr := strings.Split(nodeInfoStr, " ")
	myNode := gphxpaxos.FromString(nodeArr[0])
	nodeListStr := nodeArr[1]
	nodeArr = strings.Split(nodeListStr, ",")
	nodeList := gphxpaxos.NodeInfoList{}
	for _, nodeInfo := range nodeArr {
		node := gphxpaxos.FromString(nodeInfo)
		nodeList = append(nodeList, node)
	}
	return *myNode, nodeList
}

func  MakeLogStoragePath(ip string, port int ) string {
	path := fmt.Sprintf("./kv_%s_%d", ip, port)
	return path
}


func RunServer1() {
	myNode, nodeList := parseNode("127.0.0.1:11111 127.0.0.1:11111,127.0.0.1:11112,127.0.0.1:11113")
	serverStorePath := MakeLogStoragePath(myNode.Ip, myNode.Port)
	kvServer := gphxkv.NewKVServer(myNode,nodeList, serverStorePath + "/db",  serverStorePath + "/log")
	kvServer.Init()
}


func RunServer2() {
	myNode, nodeList := parseNode("127.0.0.1:11112 127.0.0.1:11111,127.0.0.1:11112,127.0.0.1:11113")
	serverStorePath := MakeLogStoragePath(myNode.Ip, myNode.Port)
	kvServer := gphxkv.NewKVServer(myNode,nodeList, serverStorePath + "/db",  serverStorePath + "/log")
	kvServer.Init()
}

func RunServer3() {
	myNode, nodeList := parseNode("127.0.0.1:11113 127.0.0.1:11111,127.0.0.1:11112,127.0.0.1:11113")
	serverStorePath := MakeLogStoragePath(myNode.Ip, myNode.Port)
	kvServer := gphxkv.NewKVServer(myNode,nodeList, serverStorePath + "/db",  serverStorePath + "/log")
	kvServer.Init()
}

func main() {
	s := flag.Int("s", 0, "run server")
	flag.Parse()

	if *s == 1 {
		RunServer1()
	} else if *s == 2 {
		RunServer2()
	} else if *s == 3 {
		RunServer3()
	} else {
		fmt.Println("-----do nothing-----")
	}
}

