package util

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
	"container/list"
)

// TODO startChan的作用???
func StartRoutine(f func()) {
	startChan := make(chan bool)
	go func() {
		startChan <- true
		f()
	}()

	<-startChan
	log.Info("start Routine done ")
}

type TimerThread struct {
	// save timer id
	nowTimerId uint32

	// for check timer id existing
	existTimerIdMap map[uint32]bool

	// mutex protect timer exist map
	mapMutex sync.Mutex

	// mutex protect timer lists
	mutex sync.Mutex

	// new added timers saved in newTimerList
	newTimerList *list.List

	// being added into head saved in currentTimerList
	currentTimerList *list.List

	// thread end flag
	end bool

	// now time (in ms)
	now uint64
}

type TimerObj interface {
	OnTimeout(timer *Timer)
}

type Timer struct {
	Id        uint32
	Obj       TimerObj
	AbsTime   uint64
	TimerType int
}

// TODO 实现一个高性能的Delay queue
func NewTimerThread() *TimerThread {
	timerThread := &TimerThread{
		nowTimerId:       1,
		existTimerIdMap:  make(map[uint32]bool, 0),
		newTimerList:     list.New(),
		currentTimerList: list.New(),
		end:              false,
		now:              NowTimeMs(),
	}

	StartRoutine(timerThread.main)
	return timerThread
}

func (timerThread *TimerThread) Stop() {
	timerThread.end = true
}

func (timerThread *TimerThread) main() {
	for !timerThread.end {
		// fire every 1 ms
		timerChan := time.NewTimer(1 * time.Millisecond).C
		<-timerChan
		timerThread.now = NowTimeMs()

		// iterator current timer list
		len := timerThread.currentTimerList.Len()
		for i := 0; i < len; i++ {
			obj := timerThread.currentTimerList.Front()
			timer := obj.Value.(*Timer)
			if timer.AbsTime > timerThread.now { // 时间还未到
				break
			}

			timerThread.currentTimerList.Remove(obj)
			timerThread.fireTimeout(timer)
		}

		timerThread.mutex.Lock()
		if timerThread.currentTimerList.Len() > 0 && timerThread.newTimerList.Len() > 0 {
			currentFirstTimer := timerThread.currentTimerList.Front().Value.(*Timer)
			newFirstTimer := timerThread.newTimerList.Front().Value.(*Timer)
			if currentFirstTimer.AbsTime > newFirstTimer.AbsTime {
				timerThread.currentTimerList, timerThread.newTimerList =
					timerThread.newTimerList,timerThread.currentTimerList
			}
		} else if timerThread.currentTimerList.Len() == 0{
			timerThread.currentTimerList, timerThread.newTimerList =
				timerThread.newTimerList,timerThread.currentTimerList
		}


		timerThread.mutex.Unlock()

	}
}

func (timerThread *TimerThread) fireTimeout(timer *Timer) {
	id := timer.Id
	timerThread.mapMutex.Lock()
	_, ok := timerThread.existTimerIdMap[id]
	timerThread.mapMutex.Unlock()

	if ok {
		log.Debug("fire timeout:%v, %d", timer.Obj, timer.TimerType)
		timer.Obj.OnTimeout(timer)
	}
}

func (timerThread *TimerThread) AddTimer(timeoutMs uint32, timeType int, obj TimerObj) uint32 {
	timerThread.mutex.Lock()
	absTime := timerThread.now + uint64(timeoutMs) // 过期时间
	timer := newTimer(timerThread.nowTimerId, absTime, timeType, obj)
	timerId := timerThread.nowTimerId
	timerThread.nowTimerId += 1

	var e *list.Element = nil
	for e = timerThread.newTimerList.Front(); e != nil; e = e.Next() {
		if e.Value.(*Timer).AbsTime > timer.AbsTime {
			timerThread.newTimerList.InsertBefore(timer, e)
			break
		}
	}

	if e == nil {
		timerThread.newTimerList.PushBack(timer)
	}

	// add into exist timer map
	timerThread.mapMutex.Lock()
	timerThread.existTimerIdMap[timerId] = true
	timerThread.mapMutex.Unlock()

	timerThread.mutex.Unlock()

	return timerId
}

func (timerThread *TimerThread) DelTimer(timerId uint32) {
	timerThread.mapMutex.Lock()
	delete(timerThread.existTimerIdMap, timerId)
	timerThread.mapMutex.Unlock()
}

func newTimer(timerId uint32, absTime uint64, timeType int, obj TimerObj) *Timer {
	return &Timer{
		Id:        timerId,
		AbsTime:   absTime,
		Obj:       obj,
		TimerType: timeType,
	}
}
