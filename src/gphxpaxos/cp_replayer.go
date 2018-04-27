package gphxpaxos

import (
	"time"
	log "github.com/sirupsen/logrus"
	"gphxpaxos/util"
)

type Replayer struct {
	config   *Config
	paxosLog *PaxosLog
	factory  *SMFac
	ckmnger  *CheckpointManager

	isPaused bool
	isEnd    bool
	isStart  bool
	canRun   bool
}

func NewReplayer(config *Config, factory *SMFac,
	logStorage LogStorage, mnger *CheckpointManager) *Replayer {
	replayer := &Replayer{
		config:   config,
		paxosLog: NewPaxosLog(logStorage),
		factory:  factory,
		ckmnger:  mnger,
	}

	return replayer
}

func (replayer *Replayer) Start() {
	util.StartRoutine(replayer.main)
}

func (replayer *Replayer) Stop() {
	replayer.isEnd = true
}

func (replayer *Replayer) Pause() {
	replayer.canRun = false
}

func (replayer *Replayer) IsPaused() bool {
	return replayer.isPaused
}

func (replayer *Replayer) Continue() {
	replayer.isPaused = false
	replayer.canRun = true
}

func (replayer *Replayer) main() {

	instanceId := replayer.factory.GetCheckpointInstanceId(replayer.config.GetMyGroupId()) + 1

	for {
		if replayer.isEnd {
			break
		}

		if !replayer.canRun {
			replayer.isPaused = true
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		if instanceId >= replayer.ckmnger.GetMaxChosenInstanceID() {
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		err := replayer.PlayOne(instanceId)
		if err == nil {
			instanceId += 1
		} else {
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (replayer *Replayer) PlayOne(instanceId uint64) error {
	var state = &AcceptorStateData{}
	err := replayer.paxosLog.ReadState(replayer.config.GetMyGroupId(), instanceId, state)
	if err != nil {
		return err
	}

	err = replayer.factory.ExecuteForCheckpoint(replayer.config.GetMyGroupId(), instanceId, state.GetAcceptedValue())
	if err != nil {
		log.Errorf("checkpoint sm execute fail:%v, instanceid:%d", err, instanceId)
		return err
	}

	return nil
}
