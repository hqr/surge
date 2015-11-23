// Package surge provides a framework for discrete event simulation, as well as
// a number of models for Unsolicited and Reservation Group based Edge-driven
// load balancing. Targeted modeling area includes large and super-large storage
// clusters with multiple access points (referred to as "gateways") and multiple
// storage targets (referred to as "servers").
//
package surge

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

//
// Tio (transactional IO) provides generic way to execute a sequence of
// operations as one compound transaction. The important part of each tio
// instance is its pipeline - typically, a static object instantiated at the
// model's init time - that *declares* this aforementioned sequence (of
// operations or stages) for all the future tios of a given type.
//
// In addition to Tio.pipeline, start and stop time. source node
// that has created this tio instance and a few other properties,
// each tio has a unique ID (Tio.id) and its printable counterpart tio.sid -
// the latter for easy tracking in the logs.
//
// Propagation of tio through its pipeline is accomplished via tio.next()
// while execution of the current stage is done via tio.doStage()
// The latter uses generic Go reflect interfaces to execute declared
// callbacks - the pipeline stage handlers. Once the last stage is reached
// and executed, tio.done is set to true.
// Strict ordering of (passing through) the pipeline stages is enforced..
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
	var ev *TimedAnyEvent
	if tio.index == -1 {
		ev = newTimedAnyEvent(tio.source, when, tgt)
	} else {
		ev = newTimedAnyEvent(tio.event.GetTarget(), when, tgt)
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
	newev.setOneArg(tio)
	tio.event = newev
	tio.index++

	log(LogV, "stage-next", tio.String())

	group := newev.GetGroup()
	if group == nil {
		src.Send(newev, SmethodWait) // blocking
	} else {
		group.SendGroup(newev, SmethodDirectInsert)
	}
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
