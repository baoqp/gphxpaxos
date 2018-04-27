package gphxpaxos

import (
	"testing"
	"mysqlBinlogSync/util"
)

func TestNetWork(t *testing.T) {
	dn1 := &DefaultNetWork{port:7081}
	//dn2 := &DefaultNetWork{port:8082}
	dn1.RunNetWork()
	//dn2.RunNetWork()

 	for !dn1.end {
 		util.SleepMs(2000)
	}
}

// SendMessageTCP(groupIdx int32, ip string, port int, message []byte)
func TestNetWork2(t *testing.T) {
	client := &DefaultNetWork{}
	client.SendMessageTCP(1, "127.0.0.1", 7081, []byte("hello world"))
}