package surge

import (
	"fmt"
	"time"
)

//
// interfaces
//
type EventInterface interface {
	GetSource() RunnerInterface
	GetCreationTime() time.Time
	GetTriggerTime() time.Time
	GetTarget() RunnerInterface
	String() string
}

//
// generic unicast event that must trigger at a certain time
//
type TimedUcastEvent struct {
	crtime time.Time
	source RunnerInterface
	thtime time.Time
	target RunnerInterface
}

func newTimedUcastEvent(src RunnerInterface, when time.Duration, tgt RunnerInterface) *TimedUcastEvent {
	assert(when > 0)
	triggertime := Now.Add(when)
	return &TimedUcastEvent{
		crtime: Now,
		source: src,
		thtime: triggertime,
		target: tgt}
}

func (e TimedUcastEvent) GetSource() RunnerInterface { return e.source }
func (e TimedUcastEvent) GetCreationTime() time.Time { return e.crtime }
func (e TimedUcastEvent) GetTriggerTime() time.Time  { return e.thtime }
func (e TimedUcastEvent) GetTarget() RunnerInterface { return e.target }

func (e TimedUcastEvent) String() string {
	dcreated := e.crtime.Sub(time.Time{})
	dtriggered := e.thtime.Sub(time.Time{})
	return fmt.Sprintf("[Event src=%v,%11.10v,%11.10v,tgt=%v]", e.source.String(), dcreated, dtriggered, e.target.String())
}
