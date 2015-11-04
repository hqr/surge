package surge

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

//
// types
//
type Tio struct {
	id       int64
	sid      int64 // short id for logs
	pipeline *Pipeline
	strtime  time.Time
	fintime  time.Time
	source   RunnerInterface

	index int
	event EventInterface
	done  bool
	err   error
}

func NewTio(src RunnerInterface, p *Pipeline) *Tio {
	assert(p.Count() > 0)

	uqid, printid := uqrandom64(src.GetID())
	return &Tio{id: uqid, sid: printid, pipeline: p, index: -1, source: src}
}

func (tio *Tio) GetStage() (string, int) {
	stage := tio.pipeline.GetStage(tio.index)
	return stage.name, tio.index
}

// advance the stage, generate and send anonymous tio event to the next stage's target
func (tio *Tio) nextAnon(when time.Duration, tgt RunnerInterface) {
	var ev *TimedUcastEvent
	if tio.index == -1 {
		ev = newTimedUcastEvent(tio.source, when, tgt)
	} else {
		ev = newTimedUcastEvent(tio.event.GetTarget(), when, tgt)
	}
	tio.next(ev)
}

// advance the stage & send specific event to the next stage's target
func (tio *Tio) next(newev EventInterface) {
	var src RunnerInterface
	if tio.index == -1 {
		src = tio.source
		tio.strtime = Now
	} else {
		assert(tio.index < tio.pipeline.Count()-1)
		src = newev.GetSource()
	}
	newev.SetExtension(tio)
	tio.event = newev
	tio.index++

	log(LogV, "stage-next", tio.String())

	src.Send(tio.event, true) // blocking
}

func (tio *Tio) doStage(r RunnerInterface) error {
	assert(r == tio.event.GetTarget())

	stage := tio.pipeline.GetStage(tio.index)
	assert(tio.index == stage.index)

	methodValue := reflect.ValueOf(r).MethodByName(stage.handler)
	rcValue := methodValue.Call([]reflect.Value{reflect.ValueOf(tio.event)})

	if tio.index == tio.pipeline.Count()-1 {
		tio.fintime = Now
		tio.done = true
	}
	if rcValue[0].IsNil() {
		if tio.done {
			log(LogV, tio.String())
		} else {
			log(LogV, "stage-done", stage.name, tio.String())
		}
		return nil
	}
	tio.err = errors.New(rcValue[0].Elem().Elem().Field(0).String())
	log(tio.String(), r.String())
	return tio.err
}

func (tio *Tio) String() string {
	if tio.done {
		if tio.err == nil {
			return fmt.Sprintf("[tio#%d done]", tio.sid)
		}
		return fmt.Sprintf("ERROR: [tio#%d failed,%#v]", tio.sid, tio.err)
	}
	stage := tio.pipeline.GetStage(tio.index)
	if tio.err == nil {
		return fmt.Sprintf("[tio#%d,stage(%s,%d)]", tio.sid, stage.name, tio.index)
	}
	return fmt.Sprintf("ERROR: [tio#%d,stage(%s,%d) failed]", tio.sid, stage.name, tio.index)
}
