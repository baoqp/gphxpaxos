package gphxkv

import (
	log "github.com/sirupsen/logrus"
	"gphxpaxos"
)

type KVPaxos struct {
	myNode       gphxpaxos.NodeInfo
	nodeList     gphxpaxos.NodeInfoList
	paxosNode    *gphxpaxos.Node
	paxosLogPath string

	dbPath     string
	groupCount int
	kvSM       *KVSM
}

func NewKvPaxos(myNode gphxpaxos.NodeInfo, nodeList gphxpaxos.NodeInfoList,
	dbPath string, paxosLogPath string) *KVPaxos {

	return &KVPaxos{
		myNode:       myNode,
		nodeList:     nodeList,
		paxosLogPath: paxosLogPath,
		dbPath:       dbPath,
		kvSM:         newKVSM(dbPath),
		groupCount:   3,
	}
}

func (p *KVPaxos) GetGroupIdx(key []byte) int {
	hash := 0
	for _, b := range key {
		hash += hash*7 + int(b)
	}

	return hash % p.groupCount
}

func (p *KVPaxos) GetMaster(key []byte) *gphxpaxos.NodeInfo {
	groupIdx := p.GetGroupIdx(key)
	return p.paxosNode.GetMaster(int32(groupIdx))
}

func (p *KVPaxos) IsIMMaste(key []byte) bool {
	groupIdx := p.GetGroupIdx(key)
	return p.paxosNode.IsIMMaster(int32(groupIdx))
}

func (p *KVPaxos) RunPaxos() error {

	err := p.kvSM.Init()
	if err != nil {
		return err
	}

	options := &gphxpaxos.Options{}
	options.LogStoragePath = p.paxosLogPath
	//this groupcount means run paxos group count.
	//every paxos group is independent, there are no any communicate between any 2 paxos group.
	options.GroupCount = p.groupCount
	options.MyNodeInfo = p.myNode
	options.NodeInfoList = p.nodeList
	for groupIdx := 0; groupIdx < p.groupCount; groupIdx ++ {
		groupSMInfo := &gphxpaxos.GroupSMInfo{}
		groupSMInfo.GroupIdx = int32(groupIdx)
		groupSMInfo.SMList = append(groupSMInfo.SMList, p.kvSM)
		options.GroupSMInfoList = append(options.GroupSMInfoList, groupSMInfo)
	}

	p.paxosNode = &gphxpaxos.Node{}
	err = p.paxosNode.RunNode(options)
	if err != nil {
		return err
	}
	log.Info("run paxos ok")
	return nil
}

func (p *KVPaxos) KVPropose(key []byte, value []byte, kvSMCtx *KVSMCtx) error {
	groupIdx := p.GetGroupIdx(key)

	ctx := &gphxpaxos.SMCtx{
		SMID: p.kvSM.SMID(),
		PCtx: kvSMCtx,
	}

	var instanceId uint64
	err := p.paxosNode.ProposeWithCtx(int32(groupIdx), value, &instanceId, ctx)
	if err != nil {
		log.Errorf("paxos propose fail, key %s groupidx %d err %v",
			string(key), groupIdx, err)
		return err
	}

	return nil
}


func (p *KVPaxos) Put(key []byte, value []byte, version uint64) error {
	data := p.kvSM.MakeSetOpValue(key, value, version)
	kvSMCtx := NewKVSMCtx()
	err :=p.KVPropose(key, data, kvSMCtx)
	if err != nil {
		return KVStatus_FAIL
	}

	if kvSMCtx.executeRet == KV_OK {
		return KVStatus_SUCC
	} else if kvSMCtx.executeRet == KV_KEY_VERSION_CONFLICT {
		return KVStatus_VERSION_CONFLICT
	}
	return KVStatus_FAIL
}

// TODO getLocal能保证拿到最新的???
func (p *KVPaxos) GetLocal(key []byte) ([]byte, uint64,  error) {
	value ,version, err := p.kvSM.GetDBClient().Get(key)
	if err == KV_OK {
		return value, version, KVStatus_SUCC
	} else if err == KV_KEY_NOTEXIST {
		return nil, 0, KVStatus_KEY_NOTEXIST
	}

	return nil, 0, KVStatus_FAIL
}

func (p *KVPaxos) Delete(key []byte, version uint64) error {
	data := p.kvSM.MakeDelOpValue(key, version)
	kvSMCtx := NewKVSMCtx()
	err :=p.KVPropose(key, data, kvSMCtx)
	if err != nil {
		return KVStatus_FAIL
	}

	if kvSMCtx.executeRet == KV_OK {
		return KVStatus_SUCC
	} else if kvSMCtx.executeRet == KV_KEY_VERSION_CONFLICT {
		return KVStatus_VERSION_CONFLICT
	}
	return KVStatus_FAIL
}




