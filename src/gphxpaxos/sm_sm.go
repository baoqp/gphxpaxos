package gphxpaxos

// 状态机上下文
type SMCtx struct {
	SMID int32
	PCtx interface{}
}

// 状态机接口
type StateMachine interface {
	Execute(groupIdx int32, instanceId uint64, paxosValue []byte, context *SMCtx) error

	SMID() int32

	ExecuteForCheckpoint(groupIdx int32, instanceId uint64, paxosValue []byte) error

	GetCheckpointInstanceId(groupIdx int32) uint64

	LockCheckpointState() error

	GetCheckpointState(groupIdx int32, dirPath *string, fileList []string) error

	UnLockCheckpointState()

	LoadCheckpointState(groupIdx int32, checkpointTmpFileDirPath string,
		fileList []string, checkpointInstanceID uint64) error

	BeforePropose(groupIdx int32, value *[]byte)

	NeedCallBeforePropose() bool
}

type InsideSM interface {
	StateMachine

	GetCheckpointBuffer(cpBuffer *string) error
	UpdateByCheckpoint(cpBuffer *string, change bool) error
}
