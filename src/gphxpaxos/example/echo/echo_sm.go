package echo

import (
	"gphxpaxos"
	log "github.com/sirupsen/logrus"
)


type EchoSMCtx struct {
	executeRet int
	echoRespValue []byte
}

type EchoSM struct {
}

func (echoSM *EchoSM) Execute(groupIdx int32, instanceId uint64, paxosValue []byte, context *gphxpaxos.SMCtx) error {

	log.Infof("[SM Execute] ok, smid %d instanceid %lu value %s \n",
		echoSM.SMID(), instanceId, string(paxosValue))

	if context != nil && context.PCtx != nil {
		echoSMCtx := context.PCtx.(*EchoSMCtx)
		echoSMCtx.executeRet = 0
		echoSMCtx.echoRespValue = paxosValue
	}

	return nil
}

func (echoSM *EchoSM) SMID() int32 {
	return 1
}

func (echoSM *EchoSM) ExecuteForCheckpoint(groupIdx int32, instanceId uint64, paxosValue []byte) error {
	return nil
}

func (echoSM *EchoSM) GetCheckpointInstanceId(groupIdx int32) uint64 {
	return uint64(0)
}

func (echoSM *EchoSM) LockCheckpointState() error {
	return nil
}

func (echoSM *EchoSM) GetCheckpointState(groupIdx int32, dirPath *string, fileList []string) error {
	return nil
}

func (echoSM *EchoSM) UnLockCheckpointState() {

}

func (echoSM *EchoSM) LoadCheckpointState(groupIdx int32, checkpointTmpFileDirPath string,
	fileList []string, checkpointInstanceID uint64) error {

	return nil
}

func (echoSM *EchoSM) BeforePropose(groupIdx int32, value *[]byte) {

}

func (echoSM *EchoSM) NeedCallBeforePropose() bool {
	return false
}
