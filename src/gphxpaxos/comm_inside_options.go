package gphxpaxos

import (
	"sync"
	"gphxpaxos/util"
)

type InsideOptions struct {
	isLargeBufferMode bool
	isIMFollower      bool
	groupCount        int
}

var once sync.Once
var insideOptions *InsideOptions

func GetInsideOptions() *InsideOptions {
	once.Do(func() {
		insideOptions = &InsideOptions{
			isLargeBufferMode: false,
			isIMFollower:      false,
			groupCount:        1}

	})

	return insideOptions
}

func SetAsLargeBufferMode() {
	GetInsideOptions().isLargeBufferMode = true
}

func SetAsFollower() {
	GetInsideOptions().isIMFollower = true
}

func SetGroupCount(iGroupCount int) {
	GetInsideOptions().groupCount = iGroupCount
}

func GetMaxBufferSize() int {
	if GetInsideOptions().isLargeBufferMode {
		return 52428800
	}
	return 10485760
}

func GetStartPrepareTimeoutMs() uint32 {
	if GetInsideOptions().isLargeBufferMode {
		return 15000
	}
	return 2000
}

func GetStartAcceptTimeoutMs() uint32 {
	if GetInsideOptions().isLargeBufferMode {
		return 15000
	}
	return 2000
}

func GetMaxPrepareTimeoutMs() uint32 {
	if GetInsideOptions().isLargeBufferMode {
		return 90000
	}
	return 8000
}

func GetMaxAcceptTimeoutMs() uint32 {
	if GetInsideOptions().isLargeBufferMode {
		return 90000
	}
	return 8000
}

func GetMaxIOLoopQueueLen() int {

	insideOptions := GetInsideOptions()
	if insideOptions.isLargeBufferMode {
		return 1024/insideOptions.groupCount + 100
	}
	return 10240/insideOptions.groupCount + 1000

}

func GetMaxQueueLen() int {
	if GetInsideOptions().isLargeBufferMode {
		return 1024
	}
	return 10240
}

func GetAskforLearnInterval() int {

	insideOptions := GetInsideOptions()

	if !insideOptions.isIMFollower {
		if insideOptions.isLargeBufferMode {
			return 50000 + util.Rand(10000)
		} else {
			return 2500 + util.Rand(500)
		}
	} else {
		if insideOptions.isLargeBufferMode {
			return 30000 + util.Rand(15000)
		} else {
			return 2000 + util.Rand(1000)
		}
	}
}

func GetLearnerReceiver_Ack_Lead() int {
	if GetInsideOptions().isLargeBufferMode {
		return 2
	}
	return 4
}

func GetLearnerSenderPrepareTimeoutMs() int {
	if GetInsideOptions().isLargeBufferMode {
		return 6000
	}

	return 5000
}

func GetLearnerSender_Ack_TimeoutMs() int {
	if GetInsideOptions().isLargeBufferMode {
		return 6000
	}

	return 5000
}

func GetLearnerSender_Ack_Lead() int {
	if GetInsideOptions().isLargeBufferMode {
		return 5
	}

	return 21
}

func GetTcpOutQueueDropTimeMs() int {
	if GetInsideOptions().isLargeBufferMode {
		return 20000
	}

	return 5000
}

func GetLogFileMaxSize() int {
	if GetInsideOptions().isLargeBufferMode {
		return 524288000
	}

	return 104857600
}

func GetTcpConnectionNonActiveTimeout() int {
	if GetInsideOptions().isLargeBufferMode {
		return 600000
	}

	return 60000
}

func GetLearnerSenderSendQps() int {
	insideOptions := GetInsideOptions()
	if insideOptions.isLargeBufferMode {
		return 10000 / insideOptions.groupCount
	}

	return 100000 / insideOptions.groupCount
}

func GetCleanerDeleteQps() int {
	insideOptions := GetInsideOptions()
	if insideOptions.isLargeBufferMode {
		return 30000 / insideOptions.groupCount
	}

	return 300000 / insideOptions.groupCount
}

// TODO
func GetMaxCommitTimeoutMs() uint32 {
	return 5000
}

func GetMaxValueSize() int {
	return 10240
}


