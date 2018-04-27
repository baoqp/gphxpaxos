package gphxpaxos

import (
	"net"
	"time"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb/errors"
	log "github.com/sirupsen/logrus"
)

var ErrBadConn = errors.New("connection was bad")

// 默认的network实现
type DefaultNetWork struct {
	node *Node
	end  bool
	ip   string
	port int
}

func NewDefaultNetWork(ip string, port int) *DefaultNetWork {
	return &DefaultNetWork{ip: ip, port: port}
}

func (dfNetWork *DefaultNetWork) Init() error {
	return nil
}

func (dfNetWork *DefaultNetWork) SetNode(node *Node) {
	dfNetWork.node = node
}

// start listening
func (dfNetWork *DefaultNetWork) RunNetWork() error {

	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: dfNetWork.port, Zone: ""})

	if err != nil {
		log.Errorf("start listening failed")
		return err
	}

	log.Infof("start accept tcp request on port %d", dfNetWork.port)
	go dfNetWork.serve(listener)

	return nil
}

func (dfNetWork *DefaultNetWork) serve(listener *net.TCPListener) {
	for true {
		if dfNetWork.end {
			log.Info("stop accept tcp request.")
			break
		}

		conn, err := listener.AcceptTCP()

		if err != nil {
			log.Errorf("tcp accept error %v", err)
		}
		log.Infof("receive connect from %s", conn.RemoteAddr().String())

		data, err := dfNetWork.Read(conn)
		if err != nil {
			log.Info("read data error %v", err)
		} else {
			dfNetWork.OnReceiveMessage(data, len(data))
		}
		conn.Close()
	}
}

func (dfNetWork *DefaultNetWork) Read(conn net.Conn) ([]byte, error) {
	var bufLen = 2048
	var data []byte
	var buf = make([]byte, bufLen)

	for true {
		var read = 0
		var err error = nil

		read, err = conn.Read(buf)
		if err != nil {
			return nil, err
		}

		data = append(data, buf[:read]...)

		if read < bufLen {
			return data, nil
		}
	}

	return data, nil

}

func (dfNetWork *DefaultNetWork) StopNetWork() error {
	return nil
}

func (dfNetWork *DefaultNetWork) OnReceiveMessage(message []byte, messageLen int) error {
	if dfNetWork.node != nil {
		dfNetWork.node.OnReceiveMessage(message, messageLen)
	} else {
		log.Infof("receive msglen %d but with node is nil, msg is %s", messageLen, string(message))
	}
	return nil
}

func (dfNetWork *DefaultNetWork) SendMessageTCP(groupIdx int32, ip string, port int, message []byte) error {
	conn, err := dfNetWork.connect(ip, port)
	if err != nil {
		log.Errorf("connect to  %s:%d failed", ip, port)
		return ErrBadConn
	}

	if n, err := conn.Write(message); err != nil || n != len(message) {
		log.Errorf("send tcp message failed %v", err)
		return ErrBadConn
	}
	conn.Close()
	log.Info("send msg done.")

	return nil
}

func (dfNetWork *DefaultNetWork) SendMessageUDP(groupIdx int32, ip string, port int, message []byte) error {
	return dfNetWork.SendMessageTCP(groupIdx, ip, port, message)
}

// 维持长连接和重试
func (dfNetWork *DefaultNetWork) connect(ip string, port int) (net.Conn, error) {
	addr := fmt.Sprintf("%s:%d", ip, port)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
