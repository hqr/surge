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
// event processing and cloning callback types
//
type processEventCb func(ev EventInterface) bool
type cloneEventCb func(src EventInterface) EventInterface

type EventClonerInterface interface {
	clone(ev EventInterface) EventInterface
}

//
// generic event that must trigger at a certain time
//
type TimedAnyEvent struct {
	crtime time.Time
	source RunnerInterface
	thtime time.Time
	//
	target      RunnerInterface
	tio         *Tio
	targetgroup GroupInterface
}

func newTimedAnyEvent(src RunnerInterface, when time.Duration, args ...interface{}) *TimedAnyEvent {
	assert(when > 0)
	triggertime := Now.Add(when)
	ev := &TimedAnyEvent{
		crtime:      Now,
		source:      src,
		thtime:      triggertime,
		target:      nil,
		tio:         nil,
		targetgroup: nil}
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
		e.targetgroup = a.(GroupInterface)
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
func (e *TimedAnyEvent) GetGroup() GroupInterface   { return e.targetgroup }

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
type zEvent struct {
	TimedAnyEvent
}

type zControlEvent struct {
	zEvent
	cid int64
}

type ReplicaPutRequestEvent struct {
	zControlEvent
	num   int // replica num
	sizeb int // size in bytes
}

func newReplicaPutRequestEvent(gwy RunnerInterface, srv RunnerInterface, rep *PutReplica, tio *Tio) *ReplicaPutRequestEvent {
	at := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, srv, tio)

	return &ReplicaPutRequestEvent{zControlEvent{zEvent{*timedev}, rep.chunk.cid}, rep.num, rep.chunk.sizeb}
}

type McastChunkPutRequestEvent struct {
	zControlEvent
	sizeb int // size in bytes
}

func newMcastChunkPutRequestEvent(gwy RunnerInterface, group GroupInterface, chunk *Chunk, srv RunnerInterface, tio *Tio) *McastChunkPutRequestEvent {
	at := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, group, srv, tio)

	return &McastChunkPutRequestEvent{zControlEvent{zEvent{*timedev}, chunk.cid}, chunk.sizeb}
}

func (e *McastChunkPutRequestEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[McastChunkPutRequestEvent src=%v,tgt=%v,chunk#%d,ngt-group=%s]", e.source.String(), e.target.String(), printid, e.targetgroup.String())
}

type McastChunkPutAcceptEvent struct {
	zControlEvent
	rzvgroup GroupInterface
	sizeb    int // size in bytes
}

func newMcastChunkPutAcceptEvent(gwy RunnerInterface, ngtgroup GroupInterface, chunk *Chunk, rzvgroup GroupInterface, tio *Tio) *McastChunkPutAcceptEvent {
	at := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, ngtgroup, tio, tio.target)

	return &McastChunkPutAcceptEvent{zControlEvent{zEvent{*timedev}, chunk.cid}, rzvgroup, chunk.sizeb}
}

func (e *McastChunkPutAcceptEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[McastChunkPutAcceptEvent src=%v,tgt=%v,chunk#%d,rzv-group=%s]", e.source.String(), e.target.String(), printid, e.rzvgroup.String())
}

type BidEvent struct {
	zControlEvent
	bid int // replica num
}

func (e *BidEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[McastBidEvent src=%v,tgt=%v,chunk#%d,bid=%d]", e.source.String(), e.target.String(), printid, e.bid)
}

func newBidEvent(srv RunnerInterface, gwy RunnerInterface, group GroupInterface, chunkid int64, bid int, tio *Tio) *BidEvent {
	atnet := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(srv, atnet, gwy, group, tio)
	return &BidEvent{zControlEvent{zEvent{*timedev}, chunkid}, bid}
}

// acks ReplicaPut request, not to confuse with ReplicaPutAck
type ReplicaPutRequestAckEvent struct {
	zControlEvent
	num int // replica num
}

func newReplicaPutRequestAckEvent(srv RunnerInterface, gwy RunnerInterface, flow *Flow, tio *Tio) *ReplicaPutRequestAckEvent {
	atnet := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(srv, atnet, gwy, tio)
	assert(flow.cid == tio.cid)
	assert(flow.repnum == tio.repnum)
	return &ReplicaPutRequestAckEvent{zControlEvent{zEvent{*timedev}, flow.cid}, flow.repnum}
}

// acks receiving the entire replica data, not to confuse with PutRequestAck
type ReplicaPutAckEvent struct {
	zControlEvent
	num int // replica num
}

func (e *ReplicaPutAckEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[PutAckEvent src=%v,tgt=%v,chunk#%d,num=%d]", e.source.String(), e.target.String(), printid, e.num)
}

func newReplicaPutAckEvent(srv RunnerInterface, gwy RunnerInterface, flow *Flow, tio *Tio, atdisk time.Duration) *ReplicaPutAckEvent {
	atnet := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	at := atnet + atdisk
	timedev := newTimedAnyEvent(srv, at, gwy, tio)
	assert(flow.cid == tio.cid)
	assert(flow.repnum == tio.repnum)
	return &ReplicaPutAckEvent{zControlEvent{zEvent{*timedev}, flow.cid}, flow.repnum}
}

type zDataEvent struct {
	zEvent
	cid         int64
	num         int
	offset      int
	tobandwidth int64
}

type ReplicaDataEvent struct {
	zDataEvent
}

func newReplicaDataEvent(gwy RunnerInterface, srv RunnerInterface, rep *PutReplica, flow *Flow, frsize int, tio *Tio) *ReplicaDataEvent {
	at := sizeToDuration(frsize, "B", flow.tobandwidth, "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, srv, tio)
	assert(flow.cid == tio.cid)
	assert(flow.repnum == tio.repnum)
	assert(tio.cid == rep.chunk.cid)
	return &ReplicaDataEvent{zDataEvent{zEvent{*timedev}, rep.chunk.cid, rep.num, flow.offset, flow.tobandwidth}}
}

func (e *ReplicaDataEvent) String() string {
	printid := uqrand(e.cid)
	dcreated := e.crtime.Sub(time.Time{})
	dtriggered := e.thtime.Sub(time.Time{})
	if e.targetgroup == nil {
		return fmt.Sprintf("[ReplicaDataEvent src=%v,tgt=%v,chunk#%d,num=%d,offset=%d,(%11.10v,%11.10v)]",
			e.source.String(), e.target.String(), printid, e.num, e.offset, dcreated, dtriggered)
	}
	return fmt.Sprintf("[mcast-ReplicaDataEvent src=%v,tgt=%v,group=%v,chunk#%d,offset=%d,(%11.10v,%11.10v)]",
		e.source.String(), e.target.String(), e.targetgroup.String(), printid, e.offset, dcreated, dtriggered)
}

// note: constructs ReplicaDataEvent with srv == nil and group != nil
func newMcastChunkDataEvent(gwy RunnerInterface, rzvgroup GroupInterface, chunk *Chunk, flow *Flow, frsize int, tio *Tio) *ReplicaDataEvent {
	at := sizeToDuration(frsize, "B", flow.tobandwidth, "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, rzvgroup, tio, tio.target)

	return &ReplicaDataEvent{zDataEvent{zEvent{*timedev}, chunk.cid, 0, flow.offset, flow.tobandwidth}}
}
