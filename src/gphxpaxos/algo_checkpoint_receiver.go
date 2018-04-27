package gphxpaxos

import (
	"io/ioutil"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
	"fmt"
)

type CheckpointReceiver struct {
	config        *Config
	logStorage    LogStorage
	senderNodeId  uint64
	uuid          uint64
	sequence      uint64
	hasInitDirMap map[string]bool
}

func NewCheckpointReceiver(config *Config, logStorage LogStorage) *CheckpointReceiver {
	ckRver := &CheckpointReceiver{
		config:     config,
		logStorage: logStorage,
	}

	ckRver.Reset()

	return ckRver
}

func (checkpointReceiver *CheckpointReceiver) Reset() {
	checkpointReceiver.hasInitDirMap = make(map[string]bool, 0)
	checkpointReceiver.senderNodeId = NULL_NODEID
	checkpointReceiver.uuid = 0
	checkpointReceiver.sequence = 0
}

func (checkpointReceiver *CheckpointReceiver) NewReceiver(senderNodeId uint64, uuid uint64) error {
	err := checkpointReceiver.ClearCheckpointTmp()
	if err != nil {
		return err
	}

	err = checkpointReceiver.logStorage.ClearAllLog(checkpointReceiver.config.GetMyGroupId())
	if err != nil {
		return err
	}

	checkpointReceiver.hasInitDirMap = make(map[string]bool, 0)
	checkpointReceiver.senderNodeId = senderNodeId
	checkpointReceiver.uuid = uuid
	checkpointReceiver.sequence = 0

	return nil
}

func (checkpointReceiver *CheckpointReceiver) ClearCheckpointTmp() error {
	logStoragePath, _ := checkpointReceiver.logStorage.GetLogStorageDirPath(checkpointReceiver.config.GetMyGroupId())
	files, err := ioutil.ReadDir(logStoragePath)

	for _, file := range files {
		if strings.Contains(file.Name(), "cp_tmp_") {
			err = os.Remove(logStoragePath + "/" + file.Name())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (checkpointReceiver *CheckpointReceiver) IsReceiverFinish(senderNodeId uint64, uuid uint64, endSequence uint64) bool {
	if senderNodeId != checkpointReceiver.senderNodeId {
		return false
	}

	if uuid != checkpointReceiver.uuid {
		return false
	}

	if endSequence != checkpointReceiver.sequence {
		return false
	}

	return true
}

func (checkpointReceiver *CheckpointReceiver) GetTmpDirPath(smid int32) string {
	logStoragePath, _ := checkpointReceiver.logStorage.GetLogStorageDirPath(checkpointReceiver.config.GetMyGroupId())
	return fmt.Sprintf("%s/cp_tmp_%d", logStoragePath, smid)
}

func (checkpointReceiver *CheckpointReceiver) InitFilePath(filePath string) (string, error) {
	newFilePath := "/" + filePath + "/"
	dirList := make([]string, 0)

	dirName := ""
	for i := 0; i < len(newFilePath); i++ {
		if newFilePath[i] == '/' {
			if len(dirName) > 0 {
				dirList = append(dirList, dirName)
			}

			dirName = ""
		} else {
			dirName += fmt.Sprintf("%c", newFilePath[i])
		}
	}

	formatFilePath := "/"
	for i, dir := range dirList {
		if i+1 == len(dirList) {
			formatFilePath += dir
		} else {
			formatFilePath += dir + "/"
			_, exist := checkpointReceiver.hasInitDirMap[formatFilePath]
			if !exist {
				err := checkpointReceiver.CreateDir(formatFilePath)
				if err != nil {
					return "", err
				}

				checkpointReceiver.hasInitDirMap[formatFilePath] = true
			}
		}
	}

	log.Debug("ok, format filepath %s", formatFilePath)
	return formatFilePath, nil
}

func (checkpointReceiver *CheckpointReceiver) CreateDir(dirPath string) error {
	_, err := os.Stat(dirPath)
	if os.IsNotExist(err) {
		return os.Mkdir(dirPath, os.ModeDir)
	}

	return nil
}

func (checkpointReceiver *CheckpointReceiver) ReceiveCheckpoint(ckMsg *CheckpointMsg) error {
	if ckMsg.GetNodeID() != checkpointReceiver.senderNodeId || ckMsg.GetUUID() != checkpointReceiver.uuid {
		return ErrInvalidMsg
	}

	if ckMsg.GetSequence() == checkpointReceiver.sequence {
		log.Errorf("msg already received, msg sequence %d receiver sequence %d",
			ckMsg.GetSequence(), checkpointReceiver.sequence)
		return nil
	}

	if ckMsg.GetSequence() != checkpointReceiver.sequence + 1 {
		log.Errorf("msg sequence wrong, msg sequence %d receiver sequence %d",
			ckMsg.GetSequence(), checkpointReceiver.sequence)
		return ErrInvalidMsg
	}

	filePath := checkpointReceiver.GetTmpDirPath(ckMsg.GetSMID()) + "/" + ckMsg.GetFilePath()
	formatFilePath, err := checkpointReceiver.InitFilePath(filePath)
	if err != nil {
		return err
	}

	file, err := os.Open(formatFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	offset, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}

	if uint64(offset) != ckMsg.GetOffset() {
		log.Errorf("wrong msg, file offset %d msg offset %d", offset, ckMsg.GetOffset())
		return ErrInvalidMsg
	}

	writeLen, err := file.Write(ckMsg.GetBuffer())
	if err != nil || writeLen != len(ckMsg.GetBuffer()) {
		log.Errorf("write fail, write len %d", writeLen)
		return ErrWriteFileFail
	}

	checkpointReceiver.sequence += 1
	log.Debug("end ok, write len %d", writeLen)
	return nil
}
