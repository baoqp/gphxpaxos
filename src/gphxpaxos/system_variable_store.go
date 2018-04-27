package gphxpaxos

import (
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type SystemVariablesStore struct {
	logstorage LogStorage
}

func NewSystemVariablesStore(logstorage LogStorage) *SystemVariablesStore {
	return &SystemVariablesStore{
		logstorage: logstorage,
	}
}

func (s *SystemVariablesStore) Write(writeOptions *WriteOptions, groupIdx int32,
	variables *SystemVariables) error {

	buffer, err := proto.Marshal(variables)

	if err != nil {
		log.Errorf("Variables.Serialize fail")
		return nil
	}

	err = s.logstorage.SetSystemVariables(writeOptions, groupIdx, buffer)

	if err != nil {
		log.Errorf("DB.Put fail, groupidx %d bufferlen %zu ret %v",
			groupIdx, len(buffer), err)
		return err
	}

	return nil
}


func (s *SystemVariablesStore) Read(groupIdx int32, variables *SystemVariables) error {
	buffer, err := s.logstorage.GetSystemVariables(groupIdx)
	if err != nil {
		return err
	}
	// TODO not found error

	 return proto.Unmarshal(buffer, variables)
}