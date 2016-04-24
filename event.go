package surge

import (
	"fmt"
	"time"
	"unsafe"
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
	GetSize() int // size in  bytes
	IsMcast() bool
	GetTioStage() string

	String() string

	setOneArg(arg interface{})
}

//
// event processing and cloning callback types
//
type processEventCb func(ev EventInterface) int
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
	sizeb       int // bytes
	mcast       bool
	tiostage    string
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
		targetgroup: nil,
		sizeb:       0,
		mcast:       false}
	ev.setArgs(args)

	if ev.sizeb == 0 {
		ev.sizeb = int(unsafe.Sizeof(*ev))
	}
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
	case int:
		e.sizeb = a.(int)
	case bool:
		e.mcast = a.(bool)
	case string:
		e.tiostage = a.(string)
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
func (e *TimedAnyEvent) GetSize() int               { return e.sizeb }
func (e *TimedAnyEvent) IsMcast() bool              { return e.mcast }
func (e *TimedAnyEvent) GetTioStage() string        { return e.tiostage }

func (e *TimedAnyEvent) String() string {
	dcreated := e.crtime.Sub(time.Time{})
	dtriggered := e.thtime.Sub(time.Time{})
	pref := "Event"
	if e.mcast {
		pref = "Mcast"
	}
	return fmt.Sprintf("[%s src=%v,%11.10v,%11.10v,tgt=%v]", pref, e.source.String(), dcreated, dtriggered, e.target.String())
}

//=====================================================================
//
// put-chunk unicast & multicast models: control & data
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
	at := configNetwork.durationControlPDU + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, srv, tio, configNetwork.sizeControlPDU)

	return &ReplicaPutRequestEvent{zControlEvent{zEvent{*timedev}, rep.chunk.cid}, rep.num, rep.chunk.sizeb}
}

type McastChunkPutRequestEvent struct {
	zControlEvent
	sizeb    int       // size in bytes
	winleft  time.Time // reserve bid at or after this time
	rzvgroup *RzvGroup // rendezvous group that has been already negotiated via previous McastChunkPut...
}

func newMcastChunkPutRequestEvent(gwy RunnerInterface, group GroupInterface, chunk *Chunk, srv RunnerInterface, tio *Tio) *McastChunkPutRequestEvent {
	at := configNetwork.durationControlPDU + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, group, srv, tio, true, configNetwork.sizeControlPDU)
	cntrlev := &zControlEvent{zEvent{*timedev}, chunk.cid}

	e := &McastChunkPutRequestEvent{zControlEvent: *cntrlev, sizeb: chunk.sizeb, winleft: TimeNil}
	return e
}

func (e *McastChunkPutRequestEvent) String() string {
	printid := uqrand(e.cid)
	s := fmt.Sprintf("MCPRE %v=>%v,c#%d,ngt-group=%s", e.source.String(), e.target.String(), printid, e.targetgroup.String())
	win := &TimWin{e.winleft, TimeNil}

	if win.isNil() {
		return fmt.Sprintf("[%s]", s)
	}
	return fmt.Sprintf("[%s,win=%s]", s, win.String())
}

type McastChunkPutAcceptEvent struct {
	zControlEvent
	rzvgroup GroupInterface
	bid      *PutBid
	sizeb    int // size in bytes
}

func newMcastChunkPutAcceptEvent(gwy RunnerInterface, ngtgroup GroupInterface, chunk *Chunk, rzvgroup GroupInterface, tio *Tio) *McastChunkPutAcceptEvent {
	at := configNetwork.durationControlPDU + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, ngtgroup, tio, tio.target, true, configNetwork.sizeControlPDU)

	return &McastChunkPutAcceptEvent{zControlEvent{zEvent{*timedev}, chunk.cid}, rzvgroup, nil, chunk.sizeb}
}

func (e *McastChunkPutAcceptEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[MCPAE %v=>%v,c#%d,%s]", e.source.String(), e.target.String(), printid, e.rzvgroup.String())
}

// carries one server bid => gwy
type BidEvent struct {
	zControlEvent
	bid *PutBid
}

func (e *BidEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[MBE %v=>%v,c#%d]", e.source.String(), e.target.String(), printid)
}

func newBidEvent(srv RunnerInterface, gwy RunnerInterface, group GroupInterface, chunkid int64, bid *PutBid, tio *Tio) *BidEvent {
	atnet := configNetwork.durationControlPDU + config.timeClusterTrip
	timedev := newTimedAnyEvent(srv, atnet, gwy, group, tio, configNetwork.sizeControlPDU)
	return &BidEvent{zControlEvent{zEvent{*timedev}, chunkid}, bid}
}

// carries multiple bids generated by proxy on behalf of selected targets
type ProxyBidsEvent struct {
	zControlEvent
	bids  []*PutBid
	sizeb int // chunk size in bytes
}

func (e *ProxyBidsEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[ProxyBE %v=>%v,c#%d]", e.source.String(), e.target.String(), printid)
}

func newProxyBidsEvent(srv RunnerInterface, gwy RunnerInterface, group GroupInterface, chunkid int64, tio *Tio, numbids int, chunksizeb int, args ...*PutBid) *ProxyBidsEvent {
	atnet := configNetwork.durationControlPDU + config.timeClusterTrip
	timedev := newTimedAnyEvent(srv, atnet, gwy, group, tio, configNetwork.sizeControlPDU)
	bids := make([]*PutBid, numbids)
	for i := 0; i < len(args); i++ {
		bids[i] = args[i]
	}
	return &ProxyBidsEvent{zControlEvent{zEvent{*timedev}, chunkid}, bids, chunksizeb}
}

// acks ReplicaPut request, not to confuse with ReplicaPutAck
type ReplicaPutRequestAckEvent struct {
	zControlEvent
	num int // replica num
}

func newReplicaPutRequestAckEvent(srv RunnerInterface, gwy RunnerInterface, flow *Flow, tio *Tio, diskdelay time.Duration) *ReplicaPutRequestAckEvent {
	atnet := configNetwork.durationControlPDU + config.timeClusterTrip + diskdelay
	timedev := newTimedAnyEvent(srv, atnet, gwy, tio, configNetwork.sizeControlPDU)
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
	dtriggered := e.thtime.Sub(time.Time{})
	return fmt.Sprintf("[RPAE %v=>%v,c#%d(%d),at=%11.10v]", e.source.String(), e.target.String(), printid, e.num, dtriggered)
}

func newReplicaPutAckEvent(srv RunnerInterface, gwy RunnerInterface, flow *Flow, tio *Tio, atdisk time.Duration) *ReplicaPutAckEvent {
	atnet := configNetwork.durationControlPDU + config.timeClusterTrip
	at := atnet + atdisk
	timedev := newTimedAnyEvent(srv, at, gwy, tio, configNetwork.sizeControlPDU)
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

func newReplicaDataEvent(gwy RunnerInterface, srv RunnerInterface, cid int64, repnum int, flow *Flow, frsize int) *ReplicaDataEvent {
	tio := flow.tio
	assert(flow.cid == tio.cid)
	assert(cid == tio.cid)
	assert(flow.repnum == tio.repnum)

	at := sizeToDuration(frsize, "B", flow.tobandwidth, "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, srv, tio, frsize)
	return &ReplicaDataEvent{zDataEvent{zEvent{*timedev}, cid, repnum, flow.offset, flow.tobandwidth}}
}

func (e *ReplicaDataEvent) String() string {
	printid := uqrand(e.cid)
	dcreated := e.crtime.Sub(time.Time{})
	dtriggered := e.thtime.Sub(time.Time{})
	if e.targetgroup == nil {
		return fmt.Sprintf("[RDE %v=>%v,c#%d,num=%d,offset=%d,(%11.10v,%11.10v)]",
			e.source.String(), e.target.String(), printid, e.num, e.offset, dcreated, dtriggered)
	}
	return fmt.Sprintf("[mcast-RDE %v=>%v,group=%v,c#%d,offset=%d,(%11.10v,%11.10v)]",
		e.source.String(), e.target.String(), e.targetgroup.String(), printid, e.offset, dcreated, dtriggered)
}

// note: constructs ReplicaDataEvent with srv == nil and group != nil
func newMcastChunkDataEvent(gwy RunnerInterface, rzvgroup GroupInterface, chunk *Chunk, flow *Flow, frsize int, tio *Tio) *ReplicaDataEvent {
	at := sizeToDuration(frsize, "B", flow.tobandwidth, "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, rzvgroup, tio, tio.target, true, frsize)

	return &ReplicaDataEvent{zDataEvent{zEvent{*timedev}, chunk.cid, 0, flow.offset, flow.tobandwidth}}
}
