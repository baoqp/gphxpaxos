package gphxpaxos

const (
	Message_SendType_UDP = 0
	Message_SendType_TCP = 1
	Default_SendType =  Message_SendType_TCP
)



// 网络传输接口
type NetWork interface {

	RunNetWork() error

	StopNetWork() error

	SendMessageTCP(groupIdx int32, ip string, port int, message []byte) error

	SendMessageUDP(groupIdx int32, ip string, port int, message []byte) error

	OnReceiveMessage(message []byte, messageLen int) error

	SetNode(node *Node)
}

type MsgTransport interface {

	SendMessage(groupIdx int32, sendToNodeId uint64, value []byte, sendType int) error

	BroadcastMessage(groupIdx int32, value []byte, sendType int) error

	BroadcastMessageFollower(groupIdx int32, value []byte, sendType int) error

	BroadcastMessageTempNode(groupIdx int32, value []byte, sendType int) error
}


