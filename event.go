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
	GetTio() *Tio
	GetGroup() GroupInterface
	String() string

	setOneArg(arg interface{})
}

//
// generic event that must trigger at a certain time
//
type TimedAnyEvent struct {
	crtime time.Time
	source RunnerInterface
	thtime time.Time
	//
	target RunnerInterface
	tio    *Tio
	group  GroupInterface
}

func newTimedAnyEvent(src RunnerInterface, when time.Duration, args ...interface{}) *TimedAnyEvent {
	assert(when > 0)
	triggertime := Now.Add(when)
	ev := &TimedAnyEvent{
		crtime: Now,
		source: src,
		thtime: triggertime,
		target: nil,
		tio:    nil,
		group:  nil}
	ev.setArgs(args)
	return ev
}

func (e *TimedAnyEvent) setArgs(args []interface{}) {
	for i := 0; i < len(args); i++ {
		e.setOneArg(args[i])
	}
}

func (e *TimedAnyEvent) setOneArg(a interface{}) {
	switch a.(type) {
	case RunnerInterface:
		e.target = a.(RunnerInterface)
	case *Tio:
		e.tio = a.(*Tio)
	case GroupInterface:
		e.group = a.(GroupInterface)
	default:
		assert(false, fmt.Sprintf("unexpected type: %#v", a))
	}
}

//
// interfaces
//
func (e *TimedAnyEvent) GetSource() RunnerInterface { return e.source }
func (e *TimedAnyEvent) GetCreationTime() time.Time { return e.crtime }
func (e *TimedAnyEvent) GetTriggerTime() time.Time  { return e.thtime }
func (e *TimedAnyEvent) GetTarget() RunnerInterface { return e.target }
func (e *TimedAnyEvent) GetTio() *Tio               { return e.tio }
func (e *TimedAnyEvent) GetGroup() GroupInterface   { return e.group }

func (e *TimedAnyEvent) String() string {
	dcreated := e.crtime.Sub(time.Time{})
	dtriggered := e.thtime.Sub(time.Time{})
	return fmt.Sprintf("[Event src=%v,%11.10v,%11.10v,tgt=%v]", e.source.String(), dcreated, dtriggered, e.target.String())
}
