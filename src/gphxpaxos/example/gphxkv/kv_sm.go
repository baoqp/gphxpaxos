package gphxkv

import (
	"gphxpaxos"
	log "github.com/sirupsen/logrus"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"gphxpaxos/util"
	"math"
)

type KVSMCtx struct {
	executeRet  error
	readValue   []byte
	readVersion uint64
}

func NewKVSMCtx() *KVSMCtx {

	return &KVSMCtx{
		executeRet:  nil,
		readVersion: 0,
	}
}

//////////////////////////////////////////////////////////

type KVSM struct {
	dbPath   string
	dbClient *LevelDBClient

	checkpointInstanceId    uint64
	skipSyncCheckpointTimes int
}

func newKVSM(dbPath string) *KVSM {
	return &KVSM{
		dbPath:dbPath,
	}
}

func (sm *KVSM) Init() error {
	err := sm.dbClient.Init(sm.dbPath)
	if err != nil {
		return err
	}

	sm.checkpointInstanceId, err = sm.dbClient.GetCheckpointInstanceId()
	if err != nil && err != KV_KEY_NOTEXIST {
		return err
	}

	if err == KV_KEY_NOTEXIST {
		sm.checkpointInstanceId = gphxpaxos.NoCheckpoint
	}
	return nil
}

func (sm *KVSM) SyncCheckpointInstanceId(instanceId uint64) error {

	if sm.skipSyncCheckpointTimes < 5 {
		sm.skipSyncCheckpointTimes ++
		log.Debugf("no need to sync checkpoint, skiptimes %d", sm.skipSyncCheckpointTimes)
		return nil
	}

	err := sm.dbClient.SetCheckpointInstanceId(instanceId)
	if err != nil {
		log.Errorf("LevelDBClient::SetCheckpointInstanceID fail, ret %v instanceid %d",
			err, instanceId)
		return err
	}

	sm.checkpointInstanceId = instanceId
	sm.skipSyncCheckpointTimes = 0

	return nil
}

func (sm *KVSM) Execute(groupIdx int32, instanceId uint64, paxosValue []byte,
	context *gphxpaxos.SMCtx) error {

	var operator KVOperator
	err := proto.Unmarshal(paxosValue, &operator)
	if err != nil {
		log.Error("KVOperator data wrong")

		//wrong oper data, just skip
		return nil
	}

	var executeRet error
	var readValue []byte
	var readVersion uint64

	if operator.GetOperator() == KVOperatorType_READ {
		readValue, readVersion, executeRet = sm.dbClient.Get(operator.GetKey())
	} else if operator.GetOperator() == KVOperatorType_WRITE {
		executeRet = sm.dbClient.Set(operator.GetKey(), operator.GetValue(),
			operator.GetVersion())
	} else if operator.GetOperator() == KVOperatorType_DELETE {
		executeRet = sm.dbClient.Del(operator.GetKey(), operator.GetVersion())
	} else {
		log.Errorf("unknown op %d", operator.GetOperator())
		return nil
	}

	if executeRet == KV_SYS_FAIL {
		// need retry
		return KV_SYS_FAIL
	} else {
		if context != nil && context.PCtx != nil {
			kvSMCtx := context.PCtx.(*KVSMCtx)
			kvSMCtx.executeRet = executeRet
			kvSMCtx.readValue = readValue
			kvSMCtx.readVersion = readVersion
		}

		sm.SyncCheckpointInstanceId(instanceId)

		return nil
	}

}

func (sm *KVSM) SMID() int32 {
	return 1
}

func (sm *KVSM) ExecuteForCheckpoint(groupIdx int32, instanceId uint64,
	paxosValue []byte) error {
	return nil
}

func (sm *KVSM) GetCheckpointInstanceId(groupIdx int32) uint64 {
	return sm.checkpointInstanceId
}

//have checkpoint, but not impl auto copy checkpoint to other node, so return fail.
func (sm *KVSM) LockCheckpointState() error {
	return errors.New("not impl yet")
}

func (sm *KVSM) GetCheckpointState(groupIdx int32, dirPath *string, fileList []string) error {
	return errors.New("not impl yet")
}

func (sm *KVSM) UnLockCheckpointState() {

}

func (sm *KVSM) LoadCheckpointState(groupIdx int32, checkpointTmpFileDirPath string,
	fileList []string, checkpointInstanceID uint64) error {
	return errors.New("not impl yet")
}

func (sm *KVSM) BeforePropose(groupIdx int32, value *[]byte) {

}

func (sm *KVSM) NeedCallBeforePropose() bool {
	return false
}

////////////////////////////////////////////////////
func (sm *KVSM) MakeOpValue(key []byte, value []byte, verison uint64,
	opType uint32) []byte {

	kvOper := &KVOperator{
		Key:      key,
		Value:    value,
		Version:  verison,
		Operator: opType,
		Sid:      uint32(util.Rand(math.MaxInt32)),
	}

	data, _ := proto.Marshal(kvOper)
	return data
}

func (sm *KVSM) MakeGetOpValue(key []byte) []byte {
	return sm.MakeOpValue(key, nil, 0, KVOperatorType_READ)
}

func (sm *KVSM) MakeSetOpValue(key []byte, value []byte, version uint64) []byte {
	return sm.MakeOpValue(key, value, version, KVOperatorType_WRITE)
}

func (sm *KVSM) MakeDelOpValue(key []byte, version uint64) []byte {
	return sm.MakeOpValue(key, nil, version, KVOperatorType_DELETE)
}

func (sm *KVSM) GetDBClient() *LevelDBClient {
	return sm.dbClient
}
