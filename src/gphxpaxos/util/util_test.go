package util

import (
	"testing"

	"fmt"

)

type TimeOutCallback struct {
	name string
}

func (t *TimeOutCallback) OnTimeout(timer *Timer) {
	fmt.Printf("%s->%d \r\n", t.name, timer.AbsTime)
}

func A() {
	fmt.Println("hello world")
}

func B(t *TimerThread) {
	i := 0
	for true {
		timeOut := Rand(5000)
		t.AddTimer(500, 1, &TimeOutCallback{fmt.Sprintf("%8d", timeOut)} )
		i++

		if i % 100 == 0 {
			SleepMs(uint64(Rand(100)))
		}

		if i > 1000 {
			break
		}
	}


}

func TestUtil(t *testing.T) {

	timeThread := NewTimerThread()
	B(timeThread)
	SleepMs(100000)
}
