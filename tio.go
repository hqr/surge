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
type TioInterface interface {
	// accessors
	haschild(tiochild TioInterface) bool

	// more accessors
	GetStage() (string, int)
	GetSource() RunnerInterface
	GetID() int64
	GetSid() int64
	GetCid() int64
	GetChunkSid() int64
	GetTarget() RunnerInterface
	GetParent() TioInterface
	GetFlow() FlowInterface
	SetFlow(flow FlowInterface)
	GetTioChild(tgt RunnerInterface) TioInterface
	addTioChild(tiochild TioInterface)
	hasChildren() bool
	GetNumTioChildren() int
	DelTioChild(tgt RunnerInterface)
	GetRepnum() int
	IsInit() bool
	Done() bool
	RemoveWhenDone() bool
	GetTio() TioInterface

	// real stuff
	nextAnon(when time.Duration, tgt RunnerInterface)
	next(newev EventInterface, sendhow SendMethodEnum)
	doStage(r RunnerInterface, args ...interface{}) error
	abort()
	String() string
}

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
	parent         TioInterface
	flow           FlowInterface
	children       map[RunnerInterface]TioInterface
	target         RunnerInterface
	rtio           TioInterface
	repnum         int
	removeWhenDone bool
}

// resource reservations -- bids
type TioRr struct {
	Tio
	bid *PutBid
}

// long flow -- track offset in the IO itself
type TioOffset struct {
	Tio
	offset     int64
	totalbytes int64
}

//===============================================================================
//
// c-tors
//
//================================================================================
func NewTio(src RunnerInterface, p *Pipeline, args ...interface{}) *Tio {
	assert(p.Count() > 0)
	uqid, printid := uqrandom64(src.GetID())
	tio := &Tio{
		id:             uqid,
		sid:            printid,
		pipeline:       p,
		index:          -1,
		source:         src,
		removeWhenDone: true}

	tio.init(args)
	tio.rtio = tio
	return tio
}

func (tio *Tio) init(args []interface{}) {
	for i := 0; i < len(args); i++ {
		tio.setOneArg(args[i])
	}
	// linkage
	if tio.parent == nil {
		tio.source.AddTio(tio) // see RemoveTio below
		return
	}
	tio.parent.addTioChild(tio)
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
	case TioInterface:
		assert(tio.parent == nil)
		tio.parent = a.(TioInterface)
	case RunnerInterface:
		tio.target = a.(RunnerInterface)
	case FlowInterface:
		tio.flow = a.(FlowInterface)
	case bool:
		tio.removeWhenDone = a.(bool)
	default:
		assert(false, fmt.Sprintf("unexpected type: %#v", a))
	}
}

func NewTioRr(src RunnerInterface, p *Pipeline, args ...interface{}) *TioRr {
	uqid, printid := uqrandom64(src.GetID())
	tio := &Tio{
		id:             uqid,
		sid:            printid,
		pipeline:       p,
		index:          -1,
		source:         src,
		removeWhenDone: true}
	tiorr := &TioRr{*tio, nil}
	tiorr.init(args)
	tiorr.rtio = tiorr
	return tiorr
}

func (tio *TioRr) init(args []interface{}) {
	for i := 0; i < len(args); i++ {
		tio.setOneArg(args[i])
	}
	// linkage
	if tio.parent == nil {
		tio.source.AddTio(tio) // see RemoveTio below
		return
	}
	tio.parent.addTioChild(tio)
}

func NewTioOffset(src RunnerInterface, p *Pipeline, args ...interface{}) *TioOffset {
	uqid, printid := uqrandom64(src.GetID())
	tio := &Tio{
		id:             uqid,
		sid:            printid,
		pipeline:       p,
		index:          -1,
		source:         src,
		removeWhenDone: true}
	tiof := &TioOffset{*tio, 0, 0}
	tiof.init(args)
	tiof.rtio = tiof
	return tiof
}

func (tio *TioOffset) init(args []interface{}) {
	for i := 0; i < len(args); i++ {
		tio.setOneArg(args[i])

		replica, ok := args[i].(*PutReplica)
		if ok {
			tio.totalbytes = replica.chunk.sizeb
		}
	}
	// linkage
	if tio.parent == nil {
		tio.source.AddTio(tio) // see RemoveTio below
		return
	}
	assert(tio.totalbytes > 0)
	tio.parent.addTioChild(tio)
}

//===============================================================================
//
// methods
//
//================================================================================
func (tio *Tio) GetID() int64                                 { return tio.id }
func (tio *Tio) GetSid() int64                                { return tio.sid }
func (tio *Tio) GetCid() int64                                { return tio.cid }
func (tio *Tio) GetSource() RunnerInterface                   { return tio.source }
func (tio *Tio) GetChunkSid() int64                           { return tio.chunksid }
func (tio *Tio) GetTarget() RunnerInterface                   { return tio.target }
func (tio *Tio) GetParent() TioInterface                      { return tio.parent.GetTio() }
func (tio *Tio) GetFlow() FlowInterface                       { return tio.flow }
func (tio *Tio) SetFlow(flow FlowInterface)                   { tio.flow = flow }
func (tio *Tio) GetTioChild(tgt RunnerInterface) TioInterface { return tio.children[tgt].GetTio() }

func (tio *Tio) GetNumTioChildren() int          { return len(tio.children) }
func (tio *Tio) DelTioChild(tgt RunnerInterface) { delete(tio.children, tgt) }
func (tio *Tio) GetRepnum() int                  { return tio.repnum }
func (tio *Tio) IsInit() bool                    { return tio.index == -1 }
func (tio *Tio) Done() bool                      { return tio.done }
func (tio *Tio) RemoveWhenDone() bool            { return tio.removeWhenDone }
func (tio *Tio) GetTio() TioInterface            { return tio.rtio }

func (tioparent *Tio) hasChildren() bool {
	if tioparent.children == nil {
		return false
	}
	return len(tioparent.children) > 0
}
func (tioparent *Tio) addTioChild(tiochild TioInterface) {
	if tioparent.children == nil {
		tioparent.children = make(map[RunnerInterface]TioInterface, 2)
	}
	tioparent.children[tiochild.GetTarget()] = tiochild
}

func (tioparent *Tio) haschild(tiochild TioInterface) bool {
	tgt := tiochild.GetTarget()
	child, ok := tioparent.children[tgt]
	if !ok {
		return false
	}
	return child.GetTio() == tiochild.GetTio()
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
	assert(tio.GetTio() == tioevent.GetTio())

	stage := tio.pipeline.GetStage(tio.index)
	assert(tio.index == stage.index)

	methodValue := reflect.ValueOf(r).MethodByName(stage.handler)
	// assert(!methodValue.IsValid())
	rcValue := methodValue.Call([]reflect.Value{reflect.ValueOf(tioevent)})

	// tio's own sources finalizes
	if r == tio.source && tio.index == tio.pipeline.Count()-1 {
		tio.fintime = Now
		log(LogV, "dostage-tio-done", r.String(), tio.String())
		tio.done = true
		if tio.parent == nil {
			if tio.removeWhenDone {
				log(LogV, "dostage-tio-done-removed", r.String(), tio.String())
				tio.source.RemoveTio(tio)
			}
		} else {
			tioparent := tio.GetParent()
			assert(tioparent.haschild(tio), "tioparent="+tioparent.String()+",child="+tio.String())
			if tio.removeWhenDone {
				log(LogV, "dostage-tio-done-removed", r.String(), tio.String())
				tioparent.DelTioChild(tio.target)
				if !tioparent.hasChildren() && tioparent.RemoveWhenDone() {
					tioparent.GetSource().RemoveTio(tioparent)
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
		if tio.GetParent().hasChildren() {
			// delete(tio.parent.children, tio.target)
			tio.GetParent().DelTioChild(tio.target)
		}
	}
}

func (tio *Tio) String() string {
	tioidstr := fmt.Sprintf("%d", tio.GetSid())
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
		if tio.source == nil || tio.target == nil {
			return fmt.Sprintf("[tio#%s,init]", tioidstr)
		} else {
			return fmt.Sprintf("[tio#%s,init,%s=>%s]", tioidstr, tio.source.String(), tio.target.String())
		}
	}
	if tio.err == nil {
		return fmt.Sprintf("[tio#%s,%d,%s=>%s]", tioidstr, tio.index, tio.source.String(), tio.target.String())
	}
	return fmt.Sprintf("ERROR: [tio#%s,%d failed]", tioidstr, tio.index)
}
