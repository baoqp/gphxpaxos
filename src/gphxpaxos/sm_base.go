package gphxpaxos

import (
	log "github.com/sirupsen/logrus"
	"errors"
	"gphxpaxos/util"
	"github.com/golang/protobuf/proto"
)

type BatchSMCtx struct {
	SMCtxList []*SMCtx
}

var InvalidPaxosValue = errors.New("invalid paxos value")
var ValuesSMSizeNotMatch = errors.New("values SM size not match")
var ZeroSMID = errors.New("zero smid")
var EmptyStateMachine = errors.New("empty statemachines")
var UnknownSMID = errors.New("unknown smid")

type SMFac struct {
	myGroupIdx    int32
	stateMachines map[int32]StateMachine
}

func NewSMFac(groupIdx int32) *SMFac {
	return &SMFac{
		myGroupIdx:    groupIdx,
		stateMachines: make(map[int32]StateMachine, 0),
	}
}

func (smFac *SMFac) Execute(groupIdx int32, instanceId uint64, paxosValue []byte, ctx *SMCtx) error {
	if !isValidPaxosValue(paxosValue) {
		log.Errorf("value wrong, instanceid %d size %d", instanceId, len(paxosValue))
		return InvalidPaxosValue
	}

	body, smid := smFac.UnpackPaxosValue(paxosValue)
	if smid == 0 {
		return ZeroSMID
	}

	if smid == BATCH_PROPOSE_SMID {
		var batchSMCtx *BatchSMCtx = nil
		if ctx != nil && ctx.PCtx != nil {
			batchSMCtx = ctx.PCtx.(*BatchSMCtx)
		}

		return smFac.BatchExecute(groupIdx, instanceId, body, batchSMCtx)
	} else {
		return smFac.DoExecute(groupIdx, instanceId, body, smid, ctx)
	}

	return nil
}

func (smFac *SMFac) BatchExecute(groupIdx int32, instanceId uint64, paxosValue []byte, ctx *BatchSMCtx) error {
	var batchValue BatchPaxosValues
	err := proto.Unmarshal(paxosValue, &batchValue)

	if err != nil {
		log.Errorf("BatchPaxosValue wrong, instanceid %d", instanceId)
		return InvalidPaxosValue
	}

	if ctx != nil {
		if len(ctx.SMCtxList) != len(batchValue.Values) {
			log.Errorf("BatchPaxosValue size and BatchSMCtx SM size not equal")
			return ValuesSMSizeNotMatch
		}
	}

	for idx, value := range batchValue.Values {

		var smCtx *SMCtx = nil
		if ctx != nil {
			smCtx = ctx.SMCtxList[idx]
		}

		err = smFac.DoExecute(groupIdx, instanceId, value.Value, *value.SMID, smCtx)

		if err != nil {
			return err
		}
	}

	return nil

}

func (smFac *SMFac) DoExecute(groupIdx int32, instanceId uint64, body []byte, smid int32, ctx *SMCtx) error {
	if len(smFac.stateMachines) == 0 {
		log.Errorf("no sm, instanceid %d", instanceId)
		return EmptyStateMachine
	}

	sm, exist := smFac.stateMachines[smid]
	if !exist {
		log.Errorf("unknown smid %d instanceid %d", smid, instanceId)
		return UnknownSMID
	}

	return sm.Execute(groupIdx, instanceId, body, ctx)
}

func (smFac *SMFac) ExecuteForCheckpoint(groupIdx int32, instanceId uint64, paxosValue []byte) error {
	if !isValidPaxosValue(paxosValue) {
		log.Errorf("value wrong, instanceid %d size %d", instanceId, len(paxosValue))
		return InvalidPaxosValue
	}

	body, smid := smFac.UnpackPaxosValue(paxosValue)
	if smid == 0 {
		return ZeroSMID
	}

	if smid == BATCH_PROPOSE_SMID {

	} else {
		return smFac.DoExecuteForCheckpoint(groupIdx, instanceId, body, smid)
	}

	return nil
}

func (smFac *SMFac) BatchExecuteForCheckpoint(groupIdx int32, instanceId uint64, paxosValue []byte) error {
	var batchValue BatchPaxosValues
	err := proto.Unmarshal(paxosValue, &batchValue)

	if err != nil {
		log.Errorf("BatchPaxosValue wrong, instanceid %d", instanceId)
		return InvalidPaxosValue
	}

	for _, value := range batchValue.Values {

		err = smFac.DoExecuteForCheckpoint(groupIdx, instanceId, value.Value, *value.SMID)

		if err != nil {
			return err
		}
	}

	return nil

}

func (smFac *SMFac) DoExecuteForCheckpoint(groupIdx int32, instanceId uint64, body []byte, smid int32) error {
	if len(smFac.stateMachines) == 0 {
		log.Errorf("no sm, instanceid %d", instanceId)
		return EmptyStateMachine
	}

	sm, exist := smFac.stateMachines[smid]
	if !exist {
		log.Errorf("unknown smid %d instanceid %d", smid, instanceId)
		return UnknownSMID
	}

	return sm.ExecuteForCheckpoint(groupIdx, instanceId, body)
}

func (smFac *SMFac) AddSM(statemachine StateMachine) {
	_, exist := smFac.stateMachines[statemachine.SMID()]
	if exist {
		return
	}

	smFac.stateMachines[statemachine.SMID()] = statemachine
}

func (smFac *SMFac) GetCheckpointInstanceId(groupIdx int32) uint64 {
	cpInstanceId := INVALID_INSTANCEID
	cpInstanceIdInsize := INVALID_INSTANCEID

	haveUseSm := false

	// system variables
	// master variables
	// if no user state machine, system and master's can use.
	// if have user state machine, use user'state machine's checkpointInstanceId.
	for smid, sm := range smFac.stateMachines {

		instanceId := sm.GetCheckpointInstanceId(groupIdx)

		if smid == SYSTEM_V_SMID || smid == MASTER_V_SMID {

			if instanceId == INVALID_INSTANCEID {
				continue
			}

			if instanceId > cpInstanceIdInsize || cpInstanceIdInsize == INVALID_INSTANCEID {
				cpInstanceIdInsize = instanceId
			}

			continue
		}

		haveUseSm = true

		if instanceId == INVALID_INSTANCEID {
			continue
		}

		if instanceId > cpInstanceId || cpInstanceId == INVALID_INSTANCEID {
			cpInstanceId = instanceId
		}
	}

	if haveUseSm {
		return cpInstanceId
	}
	return cpInstanceIdInsize
}

func (smFac *SMFac) GetSMList() map[int32]StateMachine {
	return smFac.stateMachines
}

func (smFac *SMFac) UnpackPaxosValue(value []byte) ([]byte, int32) {
	var smid int32
	util.DecodeInt32(value, 0, &smid)
	return value[INT32SIZE:], smid
}

func (smFac *SMFac) PackPaxosValue(value []byte, smid int32) []byte {
	buf := make([] byte, INT32SIZE)
	util.EncodeInt32(buf, 0, smid)
	return util.AppendBytes(buf, value)
}

func isValidPaxosValue(value []byte) bool {
	if len(value) < INT32SIZE {
		return false
	}
	return true
}

// TODO
func (smFac *SMFac) BeforePropose(groupIdx int32, value []byte) {

}

// TODO
func (smFac *SMFac) BeforeBatchPropose(groupIdx int32, value []byte) {

}

// TODO
func (smFac *SMFac) BeforeProposeCall(groupIdx int32, smid int32, value []byte, change bool) {

}
