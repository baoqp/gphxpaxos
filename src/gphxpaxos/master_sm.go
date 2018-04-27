package gphxpaxos

import (
	log "github.com/sirupsen/logrus"
	"math"
	"sync"
	"gphxpaxos/util"
	"github.com/golang/protobuf/proto"
)

// 实现InsideSM接口
type MasterStateMachine struct {
	myGroupId            int32
	myNodeId             uint64
	mvStore              *MasterVariablesStore
	masterNodeId         uint64
	masterVersion        uint64
	leaseTime            int
	absExpireTime        uint64
	mutex                sync.Mutex
	masterChangeCallback MasterChangeCallback
}

func NewMasterStateMachine(groupId int32, myNodeId uint64, logstorage LogStorage,
	masterChangeCallback MasterChangeCallback) *MasterStateMachine {

	return &MasterStateMachine{
		myGroupId:            groupId,
		myNodeId:             myNodeId,
		mvStore:              NewMasterVariablesStore(logstorage),
		masterNodeId:         NULL_NODEID,
		masterVersion:        uint64(INVALID_VERSION),
		leaseTime:            0,
		absExpireTime:        0,
		masterChangeCallback: masterChangeCallback,
	}
}

func (masterSM *MasterStateMachine) Init() error {
	var variables MasterVariables
	err := masterSM.mvStore.Read(masterSM.myGroupId, &variables)
	if err != nil && err != ErrKeyNotFound {
		log.Errorf("Master variables read from store fail %v", err)
		return err
	}

	if err == ErrKeyNotFound {
		log.Infof("no master variables exist")
	} else {
		masterSM.masterVersion = variables.GetVersion()
		if variables.GetVersion() == masterSM.myNodeId {
			masterSM.masterNodeId = NULL_NODEID
			masterSM.absExpireTime = 0
		} else {
			masterSM.masterNodeId = variables.GetMasterNodeid()
			masterSM.absExpireTime = util.NowTimeMs() + uint64(variables.GetLeaseTime())
		}
	}

	log.Infof("OK, master nodeid %d version %d expiretime %d",
		masterSM.masterNodeId, masterSM.masterVersion, masterSM.absExpireTime)
	return nil
}


func (masterSM *MasterStateMachine) ExecuteForCheckpoint(groupIdx int32, instanceId uint64, paxosValue []byte) error {
	return nil
}

func (masterSM *MasterStateMachine) GetCheckpointInstanceId(groupIdx int32) uint64 {
	return masterSM.masterVersion
}

func (masterSM *MasterStateMachine) GetCheckpointState(groupIdx int32, dirPath *string, fileList []string) error {
	return nil
}

func (masterSM *MasterStateMachine)  LockCheckpointState() error {
	return nil
}

func (masterSM *MasterStateMachine) UnLockCheckpointState() {

}

func (masterSM *MasterStateMachine) LoadCheckpointState(groupIdx int32, checkpointTmpFileDirPath string,
	fileList []string, checkpointInstanceID uint64) error {

	return nil
}

func (masterSM *MasterStateMachine) UpdateMasterToStore(masterNodeId uint64, version uint64,
	leaseTime int32) error {

	variables := &MasterVariables{
		MasterNodeid: proto.Uint64(masterNodeId),
		Version:      proto.Uint64(version),
		LeaseTime:    proto.Uint32(uint32(leaseTime)),
	}

	options := &WriteOptions{
		Sync: false,
	}

	return masterSM.mvStore.Write(options, masterSM.myGroupId, variables)
}

func (masterSM *MasterStateMachine) LearnMaster(instanceId uint64, operator *MasterOperator,
	absMasterTimeout uint64) error {

	masterSM.mutex.Lock()
	defer masterSM.mutex.Unlock()

	if operator.GetLastversion() != 0 &&
		instanceId > masterSM.masterVersion &&
		operator.GetLastversion() != masterSM.masterVersion {

		log.Errorf("other last version %d not same to my last version %d, instanceid %d",
			operator.GetLastversion(), masterSM.masterVersion, instanceId)

		log.Errorf("try to fix, set my master version %d as other last version %d, instanceid %d",
			masterSM.masterVersion, operator.GetLastversion(), instanceId)
		masterSM.masterVersion = operator.GetLastversion()

	}

	if operator.GetVersion() != masterSM.masterVersion {
		log.Errorf("version conflit, op version %d now master version %d",
			operator.GetVersion(), masterSM.masterVersion)

		return nil
	}

	err := masterSM.UpdateMasterToStore(operator.GetNodeid(), instanceId, operator.GetTimeout())
	if err != nil {
		log.Errorf("UpdateMasterToStore fail %v", err)
		return err
	}

	masterChange := false
	if masterSM.masterNodeId != operator.GetNodeid() {
		masterChange = true
	}

	masterSM.masterNodeId = operator.GetNodeid()
	if masterSM.masterNodeId == masterSM.myNodeId {
		masterSM.absExpireTime = absMasterTimeout
		log.Infof("Be master success, absexpiretime %d", masterSM.absExpireTime)
	} else {
		masterSM.absExpireTime = util.NowTimeMs() + uint64(operator.GetTimeout())
		log.Infof("Other be master, absexpiretime %d", masterSM.absExpireTime)
	}

	masterSM.leaseTime = int(operator.GetTimeout())
	masterSM.masterVersion = instanceId

	if masterChange {
		if masterSM.masterChangeCallback != nil {
			masterSM.masterChangeCallback(masterSM.myGroupId,
				NewNodeInfoWithId(masterSM.masterNodeId), masterSM.masterVersion)
		}
	}

	log.Infof("OK, masternodeid %d version %d abstimeout %d",
		masterSM.masterNodeId, masterSM.masterVersion, masterSM.absExpireTime)

	return nil
}

func (masterSM *MasterStateMachine) SafeGetMaster(masterNodeId *uint64, masterVersion *uint64) {
	masterSM.mutex.Lock()
	defer masterSM.mutex.Unlock()

	if util.NowTimeMs() >= masterSM.absExpireTime {
		*masterNodeId = NULL_NODEID
	} else {
		*masterNodeId = masterSM.masterNodeId
	}
	*masterVersion = masterSM.masterVersion

}

func (masterSM *MasterStateMachine) GetMaster() uint64 {
	if util.NowTimeMs() >= masterSM.absExpireTime {
		return NULL_NODEID
	}

	return masterSM.masterNodeId
}

func (masterSM *MasterStateMachine) GetMasterWithVersion(version *uint64) uint64 {
	masterNodeId := NULL_NODEID
	masterSM.SafeGetMaster(&masterNodeId, version)
	return masterNodeId
}

func (masterSM *MasterStateMachine) IsIMMaster() bool {
	return masterSM.GetMaster() == masterSM.myNodeId
}

func (masterSM *MasterStateMachine) SMID() int32 {
	return MASTER_V_SMID
}

////////////////////////////////////////////////////////////////////////////////////////////
const MasterOperatorType_Complete = 1

func (masterSM *MasterStateMachine) Execute(groupIdx int32, instanceId uint64, value []byte,
	ctx *SMCtx) error {
	var operator = &MasterOperator{}
	err := proto.Unmarshal(value, operator)
	if err != nil {
		log.Errorf("oMasterOper data wrong %v", err)
		return err
	}

	if operator.GetOperator() == MasterOperatorType_Complete {
		var absMasterTimeout uint64 = 0
		if ctx != nil && ctx.PCtx != nil {
			absMasterTimeout = *(ctx.PCtx.(*uint64))
		}

		log.Infof("absmaster timeout %v", absMasterTimeout)

		err = masterSM.LearnMaster(instanceId, operator, absMasterTimeout)
		if err != nil {
			return err
		}
	} else {
		log.Errorf("unknown op %d", operator.GetOperator())
		return nil
	}

	return nil
}

func MakeOpValue(nodeId uint64, version uint64,
	timeout int32, op uint32) ([]byte, error) {
	operator := &MasterOperator{
		Nodeid:   proto.Uint64(nodeId),
		Version:  proto.Uint64(version),
		Timeout:  proto.Int32(timeout),
		Operator: proto.Uint32(op),
		Sid:      proto.Uint32(uint32(util.Rand(math.MaxUint32))),
	}

	return proto.Marshal(operator)
}

func (masterSM *MasterStateMachine) GetCheckpointBuffer() ([]byte, error) {
	masterSM.mutex.Lock()
	defer masterSM.mutex.Unlock()

	if masterSM.masterVersion == INVALID_VERSION {
		return nil, nil
	}

	v := &MasterVariables{
		MasterNodeid: proto.Uint64(masterSM.masterNodeId),
		Version:      proto.Uint64(masterSM.masterVersion),
		LeaseTime:    proto.Uint32(uint32(masterSM.leaseTime)),
	}

	return proto.Marshal(v)
}

func (masterSM *MasterStateMachine) UpdateByCheckpoint(buffer []byte, change *bool) error {
	if len(buffer) == 0 {
		return nil
	}

	var variables = &MasterVariables{}
	err := proto.Unmarshal(buffer, variables)
	if err != nil {
		log.Errorf("Variables.ParseFromArray fail: %v", err)
		return err
	}

	if variables.GetVersion() <= masterSM.masterVersion && masterSM.masterVersion != INVALID_VERSION {
		log.Infof("lag checkpoint, no need update, cp.version %d now.version %d",
			variables.GetVersion(), masterSM.masterVersion)
		return nil
	}

	err = masterSM.UpdateMasterToStore(variables.GetMasterNodeid(), variables.GetVersion(), int32(variables.GetLeaseTime()))
	if err != nil {
		return err
	}

	log.Infof("ok, cp.version %d cp.masternodeid %d old.version %d old.masternodeid %d",
		variables.GetVersion(), variables.GetMasterNodeid(), masterSM.masterVersion, masterSM.masterNodeId)

	masterChange := false
	masterSM.masterVersion = variables.GetVersion()

	if variables.GetMasterNodeid() == masterSM.myNodeId {
		masterSM.masterNodeId = NULL_NODEID
		masterSM.absExpireTime = 0
	} else {
		if masterSM.masterNodeId != variables.GetMasterNodeid() {
			masterChange = true
		}

		masterSM.masterNodeId = variables.GetMasterNodeid()
		masterSM.absExpireTime = util.NowTimeMs() + uint64(variables.GetLeaseTime())
	}

	if masterChange {
		if masterSM.masterChangeCallback != nil {
			masterSM.masterChangeCallback(masterSM.myGroupId,
				NewNodeInfoWithId(masterSM.masterNodeId), masterSM.masterVersion)
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////

func (masterSM *MasterStateMachine) BeforePropose(groupId int32, value *[]byte) {
	masterSM.mutex.Lock()
	defer masterSM.mutex.Unlock()

	var operator = &MasterOperator{}
	err := proto.Unmarshal(*value, operator)
	if err != nil {
		return
	}

	operator.Lastversion = proto.Uint64(masterSM.masterVersion)
	*value, err = proto.Marshal(operator) // 类似于interceptor
}

func (masterSM *MasterStateMachine) NeedCallBeforePropose() bool {
	return true
}
