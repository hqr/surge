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

//=====================================================================
//
// UCH* models
//
//=====================================================================
type UchEvent struct {
	TimedAnyEvent
}

type UchControlEvent struct {
	UchEvent
	cid int64
}

type UchReplicaPutRequestEvent struct {
	UchControlEvent
	num   int // replica num
	sizeb int // size in bytes
}

func newUchReplicaPutRequestEvent(gwy RunnerInterface, srv RunnerInterface, rep *PutReplica, args ...interface{}) *UchReplicaPutRequestEvent {
	at := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, srv)

	e := &UchReplicaPutRequestEvent{UchControlEvent{UchEvent{*timedev}, rep.chunk.cid}, rep.num, rep.chunk.sizeb}
	e.setArgs(args)
	return e
}

func newMcastReplicaPutRequestEvent(gwy RunnerInterface, group GroupInterface, rep *PutReplica, args ...interface{}) *UchReplicaPutRequestEvent {
	at := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, group)

	e := &UchReplicaPutRequestEvent{UchControlEvent{UchEvent{*timedev}, rep.chunk.cid}, rep.num, rep.chunk.sizeb}
	e.setArgs(args)
	return e
}

type UchReplicaPutRequestAckEvent struct {
	UchControlEvent
	num int // replica num
}

func newUchReplicaPutRequestAckEvent(srv RunnerInterface, gwy RunnerInterface, chunkid int64, repnum int) *UchReplicaPutRequestAckEvent {
	atnet := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(srv, atnet, gwy)
	return &UchReplicaPutRequestAckEvent{UchControlEvent{UchEvent{*timedev}, chunkid}, repnum}
}

type UchReplicaPutAckEvent struct {
	UchControlEvent
	num int // replica num
}

func (e *UchReplicaPutAckEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[PutAckEvent src=%v,tgt=%v,chunk#%d,num=%d]", e.source.String(), e.target.String(), printid, e.num)
}

func newUchReplicaPutAckEvent(srv RunnerInterface, gwy RunnerInterface, chunkid int64, repnum int, atdisk time.Duration) *UchReplicaPutAckEvent {
	atnet := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	at := atnet + atdisk
	timedev := newTimedAnyEvent(srv, at, gwy)
	return &UchReplicaPutAckEvent{UchControlEvent{UchEvent{*timedev}, chunkid}, repnum}
}

type UchRateSetEvent struct {
	UchControlEvent
	tobandwidth int64 // bits/sec
	num         int   // replica num
}

type UchRateInitEvent struct {
	UchRateSetEvent
}

func (e *UchRateInitEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[RateInitEvent src=%v,tgt=%v,chunk#%d,num=%d]", e.source.String(), e.target.String(), printid, e.num)
}

func newUchRateSetEvent(srv RunnerInterface, gwy RunnerInterface, rate int64, chunkid int64, repnum int, args ...interface{}) *UchRateSetEvent {
	at := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(srv, at, gwy)
	// factor in the L2+L3+L4 headers overhead + ARP, etc.
	rate -= rate * int64(configNetwork.overheadpct) / int64(100)

	e := &UchRateSetEvent{UchControlEvent{UchEvent{*timedev}, chunkid}, rate, repnum}
	e.setArgs(args)
	return e
}

func (e *UchRateSetEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[RateSetEvent src=%v,tgt=%v,chunk#%d,num=%d]", e.source.String(), e.target.String(), printid, e.num)
}

func newUchRateInitEvent(srv RunnerInterface, gwy RunnerInterface, rate int64, chunkid int64, repnum int) *UchRateInitEvent {
	ev := newUchRateSetEvent(srv, gwy, rate, chunkid, repnum)
	return &UchRateInitEvent{*ev}
}

type UchDingAimdEvent struct {
	UchControlEvent
	num int // replica num
}

func newUchDingAimdEvent(srv RunnerInterface, gwy RunnerInterface, chunkid int64, repnum int, args ...interface{}) *UchDingAimdEvent {
	at := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(srv, at, gwy)

	e := &UchDingAimdEvent{UchControlEvent{UchEvent{*timedev}, chunkid}, repnum}
	e.setArgs(args)
	return e
}

func (e *UchDingAimdEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[DingAimdEvent src=%v,tgt=%v,chunk#%d,num=%d]", e.source.String(), e.target.String(), printid, e.num)
}

type UchDataEvent struct {
	UchEvent
	cid         int64
	num         int
	offset      int
	tobandwidth int64
}

type UchReplicaDataEvent struct {
	UchDataEvent
}

func newUchReplicaDataEvent(gwy RunnerInterface, srv RunnerInterface, rep *PutReplica, flow *Flow, frsize int, args ...interface{}) *UchReplicaDataEvent {
	at := sizeToDuration(frsize, "B", flow.tobandwidth, "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, srv)

	e := &UchReplicaDataEvent{UchDataEvent{UchEvent{*timedev}, rep.chunk.cid, rep.num, flow.offset, flow.tobandwidth}}
	e.setArgs(args)
	return e
}

func (e *UchReplicaDataEvent) String() string {
	printid := uqrand(e.cid)
	dcreated := e.crtime.Sub(time.Time{})
	dtriggered := e.thtime.Sub(time.Time{})
	return fmt.Sprintf("[ReplicaDataEvent src=%v,tgt=%v,chunk#%d,num=%d,offset=%d,(%11.10v,%11.10v)]",
		e.source.String(), e.target.String(), printid, e.num, e.offset, dcreated, dtriggered)
}
