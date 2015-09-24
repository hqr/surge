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

	uqid := uqrandom64(src.GetId())
	printid := uqid & 0xffff
	return &Tio{id: uqid, sid: printid, pipeline: p, index: -1, source: src}
}

type TimedTioEvent struct {
	TimedUcastEvent
	tio *Tio
}

func (tio *Tio) newTimedTioEvent(src RunnerInterface, when time.Duration, tgt RunnerInterface) {
	ev := newTimedUcastEvent(src, when, tgt)
	tio.event = &TimedTioEvent{*ev, tio}
}

func (tio *Tio) GetStage() (string, int) {
	stage := tio.pipeline.GetStage(tio.index)
	return stage.name, tio.index
}

// advance the stage, generate and send tio event to the target
func (tio *Tio) next(caller RunnerInterface, when time.Duration, tgt RunnerInterface) {
	var src RunnerInterface = nil
	if tio.index == -1 {
		src = tio.source
		tio.strtime = Now
	} else {
		assert(tio.index < tio.pipeline.Count()-1)
		src = tio.event.GetTarget()
	}
	assert(caller == src)

	tio.newTimedTioEvent(src, when, tgt)
	tio.index++

	log(LOG_V, "stage-next-started", tio.String(), tio.event.String())
	txch, _ := src.getChannels(tgt)
	txch <- tio.event
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
			log(LOG_V, tio.String())
		} else {
			log(LOG_V, "stage-done", stage.name, tio.String())
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
		} else {
			return fmt.Sprintf("ERROR: [tio#%d failed]", tio.sid, tio.err)
		}
	}
	stage := tio.pipeline.GetStage(tio.index)
	if tio.err == nil {
		return fmt.Sprintf("[tio#%d,stage(%s,%d)]", tio.sid, stage.name, tio.index)
	}
	return fmt.Sprintf("ERROR: [tio#%d,stage(%s,%d) failed]", tio.sid, stage.name, tio.index)
}
