package gphxpaxos

import "math"

const (
	MINCHOSEN_KEY       = math.MaxUint64 - 1 // TODO ???
	SYSTEMVARIABLES_KEY = MINCHOSEN_KEY - 1
	MASTERVARIABLES_KEY = MINCHOSEN_KEY - 2
)

type WriteOptions struct {
	Sync bool
}


/*
  LogStorage接口，可以根据需要有不同的实现。

  在phxpaxos的实现中, 实际数据value保存在文件（vfile)中（log_store封装了相关操作），在LevelDB中保存了value的索引

  LevelDB中数据格式：
    key - instance
    value format - fileid(int32) + file offset(uint64) + cksum of file value(uint32)

  元文件保存了当前正在使用的vfile的fileid
  meta file format(data path/vpath/meta):
    current file id(int32)
    file id cksum(uint32)

  value文件格式
  data file(data path/vpath/fileid.f) data format:
    data len(int32)
    value(data len) format:
      instance id(uint64)
      acceptor state data(data len - sizeof(uint64))
 */

type LogStorage interface {

	GetLogStorageDirPath(groupIdx int32) (string, error)

	Get(groupIdx int32, instanceId uint64) ([]byte, error)

	Put(writeOptions *WriteOptions, groupIdx int32, instanceId uint64, value []byte) error

	Del(writeOptions *WriteOptions, groupIdx int32, instanceId uint64) error

	GetMaxInstanceId(groupIdx int32) (uint64, error)

	SetMinChosenInstanceId(writeOptions *WriteOptions, groupIdx int32, minInstanceId uint64) error

	GetMinChosenInstanceId(groupIdx int32) (uint64, error)

	ClearAllLog(groupIdx int32) error

	SetSystemVariables(writeOptions *WriteOptions, groupIdx int32, value []byte) error

	GetSystemVariables(groupIdx int32) ([]byte, error)

	SetMasterVariables(writeOptions *WriteOptions, groupIdx int32, value []byte) error

	GetMasterVariables(groupIdx int32) ([]byte, error)
}

