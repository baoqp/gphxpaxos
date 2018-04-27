package gphxpaxos

import (
	"sync"
    proto "github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"gphxpaxos/util"
)

type PendingProposal struct {
	value          []byte
	instanceId     uint64
	ctx            *SMCtx
	batchIndex     uint32
	notifier       chan error
	absEnqueueTime uint64
}

type ProposeBatch struct {
	myGroupId    int32
	paxosNode    *Node
	mutex        sync.Mutex
	isEnd        bool
	isStarted    bool
	queue        []*PendingProposal
	nowQueueSize int

	batchCount       int
	batchDelayTimeMs uint64
	batchMaxSize     int
}

func NewProposeBatch(groupId int32, node *Node) *ProposeBatch {

	proposeBatch := &ProposeBatch{myGroupId: groupId, paxosNode: node,
		isEnd: false, isStarted: false, nowQueueSize: 0, batchCount: 5,
		batchDelayTimeMs: 20, batchMaxSize: 500 * 1024,
	}
	proposeBatch.queue = make([]*PendingProposal, 0)

	return proposeBatch
}

func (pb *ProposeBatch) main() {
	pb.isStarted = true

	timeStat := util.NewTimeStat()
	for true {

		if pb.isEnd {
			break
		}
		timeStat.Point()

		// TODO 并发问题
		var requests = make([]*PendingProposal, 0)
		pb.PluckProposal(&requests)
		pb.DoProposal(requests)

		passTime := timeStat.Point()

		needSleepTime := uint64(0)

		if passTime < pb.batchDelayTimeMs {
			needSleepTime = pb.batchDelayTimeMs - passTime
		}

		if pb.NeedBatch() {
			needSleepTime = 0
		}

		if needSleepTime > 0 {
			// TODO
			util.SleepMs(needSleepTime)
		}

	}

	// 异常
	for queueLen := len(pb.queue); queueLen > 0; queueLen = len(pb.queue) {
		proposal := pb.queue[queueLen-1]
		pb.queue = pb.queue[:queueLen-1]
		proposal.notifier <- Paxos_SystemError
	}

}


func (pb *ProposeBatch) Start() {
	util.StartRoutine(pb.main)
}

func (pb *ProposeBatch) Stop() {
	if pb.isStarted {
		pb.mutex.Lock()
		defer pb.mutex.Unlock()
		pb.isEnd = true
	}
}

func (pb *ProposeBatch) NeedBatch() bool {

	if len(pb.queue) > pb.batchCount || pb.nowQueueSize > pb.batchMaxSize {
		return true
	} else if len(pb.queue) > 0 {
		proposal := pb.queue[0]
		nowTime := util.NowTimeMs()

		proposalPassTime := uint64(0)
		if nowTime > proposal.absEnqueueTime {
			proposalPassTime = nowTime - proposal.absEnqueueTime
		}

		if proposalPassTime > pb.batchDelayTimeMs {
			return true
		}
	}

	return false
}

func (pb *ProposeBatch) SetBatchCount(batchCount int) {
	pb.batchCount = batchCount
}

func (pb *ProposeBatch) SetBatchDelayTimeMs(delay uint64) {
	pb.batchDelayTimeMs = delay
}

func (pb *ProposeBatch) Propose(value []byte, instanceId uint64, batchIndex uint32, ctx *SMCtx) error {
	if pb.isEnd {
		return Paxos_SystemError
	}
	notifier := make(chan error)
	pb.AddProposal(value, instanceId, batchIndex, ctx, notifier)
	return <-notifier
}

// TODO 并发问题 在设计pb.queue的操作时候需要加锁
func (pb *ProposeBatch) AddProposal(value []byte, instacneId uint64, batchIndex uint32,
	ctx *SMCtx, notifier chan error) {

	proposal := &PendingProposal{
		value:          value,
		ctx:            ctx,
		instanceId:     instacneId,
		batchIndex:     batchIndex,
		absEnqueueTime: util.NowTimeMs(),
		notifier:       notifier,
	}

	pb.queue = append(pb.queue, proposal)
	pb.nowQueueSize += len(value)

	if pb.NeedBatch() {
		log.Debugf("direct batch, queue size %d value size %d", len(pb.queue), pb.nowQueueSize)
		var requests = make([]*PendingProposal, 0)
		pb.PluckProposal(&requests)
		pb.DoProposal(requests)
	}
}

// 从队列中取出部分proposal组成一个batch
func (pb *ProposeBatch) PluckProposal(requests *[]*PendingProposal) {
	pluckCount := 0
	pluckSize := 0

	for queueLen := len(pb.queue); queueLen > 0; queueLen = len(pb.queue) {
		proposal := pb.queue[queueLen-1]
		*requests = append(*requests, proposal)
		pluckCount ++
		pluckSize += len(proposal.value)
		pb.nowQueueSize -= len(proposal.value)
		pb.queue = pb.queue[:queueLen-1]

		if pluckCount > pb.batchCount || pluckSize > pb.batchMaxSize {
			break
		}
	}

	if len(*requests) > 0 {
		log.Debugf("pluck %d request", len(*requests))
	}

}

func (pb *ProposeBatch) DoProposal(requests []*PendingProposal) {

	if len(requests) == 0 {
		return
	}

	if len(requests) == 1 {
		pb.OnlyOnePropose(requests[0])
		return
	}

	var batchValues = &BatchPaxosValues{}
	var batchSMCtx = &BatchSMCtx{}

	for _, proposal := range requests {
		value := &PaxosValue{}
		value.SMID = proto.Int32(0)
		if proposal.ctx != nil {
			value.SMID = proto.Int32(proposal.ctx.SMID)
		}
		value.Value = proposal.value
		batchValues.Values = append(batchValues.Values, value)
		batchSMCtx.SMCtxList = append(batchSMCtx.SMCtxList, proposal.ctx)
	}

	ctx := &SMCtx{SMID: BATCH_PROPOSE_SMID, PCtx: batchSMCtx}
	buffer, err := proto.Marshal(batchValues)
	instanceId := uint64(0)

	var ret error
	if err == nil {
		ret = pb.paxosNode.ProposeWithCtx(pb.myGroupId, buffer, &instanceId, ctx)
		if ret != nil {
			log.Errorf("real propose fail, ret %v", ret)
		}
	} else {
		log.Errorf("BatchValues SerializeToString fail")
		ret = Paxos_SystemError
	}

	for i, proposal := range requests {
		proposal.batchIndex = uint32(i)
		proposal.instanceId = instanceId
		proposal.notifier <- ret
	}

}

func (pb *ProposeBatch) OnlyOnePropose(proposal *PendingProposal) {
	err := pb.paxosNode.ProposeWithCtx(pb.myGroupId, proposal.value, &proposal.instanceId, proposal.ctx)
	proposal.notifier <- err
}
