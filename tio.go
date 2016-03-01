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
// Propagation of tio through the pipeline is accomplished via tio.next()
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

	cid            int64
	chunksid       int64
	parent         *Tio
	flow           *Flow
	children       map[RunnerInterface]*Tio
	repnum         int
	target         RunnerInterface
	removeWhenDone bool
}

func newTio(src RunnerInterface, p *Pipeline, args []interface{}) *Tio {
	assert(p.Count() > 0)
	uqid, printid := uqrandom64(src.GetID())
	tio := &Tio{
		id:             uqid,
		sid:            printid,
		pipeline:       p,
		index:          -1,
		source:         src,
		removeWhenDone: true}
	for i := 0; i < len(args); i++ {
		tio.setOneArg(args[i])
	}
	// linkage
	if tio.parent == nil {
		tio.source.AddTio(tio) // see RemoveTio below
		return tio
	}
	if tio.parent.children == nil {
		tio.parent.children = make(map[RunnerInterface]*Tio, 2)
	}
	tio.parent.children[tio.target] = tio
	return tio
}

func (tio *Tio) setOneArg(a interface{}) {
	switch a.(type) {
	case *Chunk:
		tio.cid = a.(*Chunk).cid
		tio.chunksid = a.(*Chunk).sid
	case *PutReplica:
		tio.repnum = a.(*PutReplica).num
		tio.cid = a.(*PutReplica).chunk.cid
		tio.chunksid = a.(*PutReplica).chunk.sid
	case *Tio:
		assert(tio.parent == nil)
		tio.parent = a.(*Tio)
	case RunnerInterface:
		tio.target = a.(RunnerInterface)
	case *Flow:
		tio.flow = a.(*Flow)
	default:
		assert(false, fmt.Sprintf("unexpected type: %#v", a))
	}
}

func (tioparent *Tio) haschild(tiochild *Tio) bool {
	return tioparent.children[tiochild.target] == tiochild
}

func (tioparent *Tio) getchild(srv RunnerInterface) *Tio {
	tiochild, ok := tioparent.children[srv]
	assert(ok, "tiochild does not exist: "+tioparent.String()+" for: "+srv.String())
	return tiochild
}

func (tio *Tio) GetStage() (string, int) {
	stage := tio.pipeline.GetStage(tio.index)
	return stage.name, tio.index
}

// advance the stage, generate and send anonymous tio sizeof()-size event
// to the next stage's target
func (tio *Tio) nextAnon(when time.Duration, tgt RunnerInterface) {
	var ev *TimedAnyEvent
	if tio.index == -1 {
		ev = newTimedAnyEvent(tio.source, when, tgt)
	} else {
		ev = newTimedAnyEvent(tio.event.GetTarget(), when, tgt)
	}
	tio.next(ev, SmethodWait)
}

// advance the stage & send specific event to the next stage's target
func (tio *Tio) next(newev EventInterface, sendhow SendMethodEnum) {
	var src RunnerInterface

	if tio.index == -1 {
		src = tio.source
		tio.strtime = Now
	} else {
		src = newev.GetSource()
	}
	newev.setOneArg(tio)
	tio.event = newev
	tio.index++

	//
	// event-enforced staging
	//
	tiostage := newev.GetTioStage()
	if len(tiostage) > 0 {
		tio.index = tio.pipeline.IndexOf(tiostage)
		assert(tio.index >= 0)
	}

	assert(tio.index < tio.pipeline.Count(), tio.String()+","+newev.String())

	log(LogV, "stage-next-send", src.String(), tio.String())
	src.Send(newev, sendhow)
	log(LogVV, "stage-next-sent", src.String(), tio.String())
}

func (tio *Tio) doStage(r RunnerInterface, args ...interface{}) error {
	tioevent := tio.event
	if len(args) > 0 {
		tioevent = args[0].(EventInterface)
	}
	assert(r == tioevent.GetTarget(), r.String()+"!="+tioevent.GetTarget().String())
	assert(tio == tioevent.GetTio())

	stage := tio.pipeline.GetStage(tio.index)
	assert(tio.index == stage.index)

	methodValue := reflect.ValueOf(r).MethodByName(stage.handler)
	// assert(!methodValue.IsValid())
	rcValue := methodValue.Call([]reflect.Value{reflect.ValueOf(tioevent)})

	// tio's own sources finalizes
	if r == tio.source && tio.index == tio.pipeline.Count()-1 {
		tio.fintime = Now
		log("dostage-tio-done", r.String(), tio.String())
		tio.done = true
		if tio.parent == nil {
			if tio.removeWhenDone {
				log("dostage-tio-done-removed", r.String(), tio.String())
				tio.source.RemoveTio(tio)
			}
		} else {
			assert(tio.parent.haschild(tio), "tioparent="+tio.parent.String()+",child="+tio.String())
			if tio.removeWhenDone {
				log("dostage-tio-done-removed", r.String(), tio.String())
				delete(tio.parent.children, tio.target)
				if len(tio.parent.children) == 0 && tio.parent.removeWhenDone {
					tio.parent.source.RemoveTio(tio.parent)
				}
			}
		}
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

func (tio *Tio) abort() {
	log("abort", tio.String())
	if tio.parent == nil {
		tio.children = nil
		tio.source.RemoveTio(tio)
	} else {
		if tio.parent.children != nil {
			delete(tio.parent.children, tio.target)
		}
	}
}

func (tio *Tio) String() string {
	tioidstr := fmt.Sprintf("%d", tio.sid)
	if tio.repnum != 0 {
		tioidstr += fmt.Sprintf("(c#%d(%d))", tio.chunksid, tio.repnum)
	} else if tio.cid != 0 {
		tioidstr += fmt.Sprintf("(c#%d)", tio.chunksid)
	}
	if tio.done {
		if tio.err == nil {
			return fmt.Sprintf("[tio#%s,done,%s=>%s]", tioidstr, tio.source.String(), tio.target.String())
		}
		return fmt.Sprintf("ERROR: [tio#%s,failed,%#v,%s=>%s]", tioidstr, tio.err, tio.source.String(), tio.target.String())
	}
	if tio.index < 0 {
		return fmt.Sprintf("[tio#%s,init]", tioidstr)
	}
	if tio.err == nil {
		return fmt.Sprintf("[tio#%s,%d,%s=>%s]", tioidstr, tio.index, tio.source.String(), tio.target.String())
	}
	return fmt.Sprintf("ERROR: [tio#%s,%d failed]", tioidstr, tio.index)
}
