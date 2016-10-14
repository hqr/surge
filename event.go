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
	GetTio() TioInterface
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
	tio         TioInterface
	targetgroup GroupInterface
	sizeb       int64 // bytes
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
		ev.sizeb = int64(unsafe.Sizeof(*ev))
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
	case TioInterface:
		e.tio = a.(TioInterface)
	case GroupInterface:
		e.targetgroup = a.(GroupInterface)
	case int:
		e.sizeb = int64(a.(int))
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
func (e *TimedAnyEvent) GetTio() TioInterface       { return e.tio.GetTio() }
func (e *TimedAnyEvent) GetGroup() GroupInterface   { return e.targetgroup }
func (e *TimedAnyEvent) GetSize() int               { return int(e.sizeb) }
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
	sizeb int64 // size in bytes
	num   int   // replica num
}

func newReplicaPutRequestEvent(gwy RunnerInterface, srv RunnerInterface, rep *PutReplica, tio TioInterface) *ReplicaPutRequestEvent {
	at := configNetwork.durationControlPDU + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, srv, tio, configNetwork.sizeControlPDU)

	return &ReplicaPutRequestEvent{zControlEvent{zEvent{*timedev}, rep.chunk.cid}, rep.chunk.sizeb, rep.num}
}

type McastChunkPutRequestEvent struct {
	zControlEvent
	sizeb    int64     // size in bytes
	winleft  time.Time // reserve bid at or after this time
	rzvgroup *RzvGroup // rendezvous group that has been already negotiated via previous McastChunkPut...
}

func newMcastChunkPutRequestEvent(gwy RunnerInterface, group GroupInterface, chunk *Chunk, srv RunnerInterface, tio TioInterface) *McastChunkPutRequestEvent {
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
	sizeb    int64 // size in bytes
}

func newMcastChunkPutAcceptEvent(gwy RunnerInterface, ngtgroup GroupInterface, chunk *Chunk, rzvgroup GroupInterface, tio TioInterface) *McastChunkPutAcceptEvent {
	at := configNetwork.durationControlPDU + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, ngtgroup, tio, tio.GetTarget(), true, configNetwork.sizeControlPDU)

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

func newBidEvent(srv RunnerInterface, gwy RunnerInterface, group GroupInterface, chunkid int64, bid *PutBid, tio TioInterface) *BidEvent {
	atnet := configNetwork.durationControlPDU + config.timeClusterTrip
	timedev := newTimedAnyEvent(srv, atnet, gwy, group, tio, configNetwork.sizeControlPDU)
	return &BidEvent{zControlEvent{zEvent{*timedev}, chunkid}, bid}
}

// carries multiple bids generated by proxy on behalf of selected targets
type ProxyBidsEvent struct {
	zControlEvent
	bids  []*PutBid
	sizeb int64 // chunk size in bytes
}

func (e *ProxyBidsEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[ProxyBE %v=>%v,c#%d]", e.source.String(), e.target.String(), printid)
}

func newProxyBidsEvent(srv RunnerInterface, gwy RunnerInterface, group GroupInterface, chunkid int64, tio TioInterface, numbids int, chunksizeb int64, args ...*PutBid) *ProxyBidsEvent {
	atnet := configNetwork.durationControlPDU + config.timeClusterTrip
	timedev := newTimedAnyEvent(srv, atnet, gwy, group, tio, configNetwork.sizeControlPDU)
	bids := make([]*PutBid, numbids)
	for i := 0; i < len(args); i++ {
		bids[i] = args[i]
	}
	return &ProxyBidsEvent{zControlEvent{zEvent{*timedev}, chunkid}, bids, chunksizeb}
}

// bid-done event, server => proxy
type BidDoneEvent struct {
	zControlEvent
	bid            *PutBid
	reservedIOdone time.Time
}

func (e *BidDoneEvent) String() string {
	printid := uqrand(e.cid)
	reservedIOdone := fmt.Sprintf("%-12.10v", e.reservedIOdone.Sub(time.Time{}))
	return fmt.Sprintf("[BDE %v=>%v,c#%d],%s", e.source.String(), e.target.String(), printid, reservedIOdone)
}

func newBidDoneEvent(srv RunnerInterface, proxy RunnerInterface, bid *PutBid, reservedIOdone time.Time) *BidDoneEvent {
	atnet := configNetwork.durationControlPDU + config.timeClusterTrip
	timedev := newTimedAnyEvent(srv, atnet, proxy, bid.tio, configNetwork.sizeControlPDU)
	return &BidDoneEvent{zControlEvent{zEvent{*timedev}, bid.tio.cid}, bid, reservedIOdone}
}

// acks ReplicaPut request, not to confuse with ReplicaPutAck
type ReplicaPutRequestAckEvent struct {
	zControlEvent
	num int // replica num
}

func newReplicaPutRequestAckEvent(srv RunnerInterface, gwy RunnerInterface, flow FlowInterface, tio TioInterface, diskdelay time.Duration) *ReplicaPutRequestAckEvent {
	atnet := configNetwork.durationControlPDU + config.timeClusterTrip + diskdelay
	timedev := newTimedAnyEvent(srv, atnet, gwy, tio, configNetwork.sizeControlPDU)
	assert(flow.GetCid() == tio.GetCid())
	assert(flow.GetRepnum() == tio.GetRepnum())
	return &ReplicaPutRequestAckEvent{zControlEvent{zEvent{*timedev}, flow.GetCid()}, flow.GetRepnum()}
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

func newReplicaPutAckEvent(srv RunnerInterface, gwy RunnerInterface, tio TioInterface, atdisk time.Duration) *ReplicaPutAckEvent {
	atnet := configNetwork.durationControlPDU + config.timeClusterTrip
	at := atnet + atdisk
	timedev := newTimedAnyEvent(srv, at, gwy, tio, configNetwork.sizeControlPDU)
	return &ReplicaPutAckEvent{zControlEvent{zEvent{*timedev}, tio.GetCid()}, tio.GetRepnum()}
}

type zDataEvent struct {
	zEvent
	cid         int64
	num         int
	offset      int64
	tobandwidth int64
}

type ReplicaDataEvent struct {
	zDataEvent
}

func newReplicaDataEvent(gwy RunnerInterface, srv RunnerInterface, cid int64, repnum int, flow FlowInterface, frsize int) *ReplicaDataEvent {
	tio := flow.GetTio()
	assert(flow.GetCid() == tio.GetCid())
	assert(cid == tio.GetCid())
	assert(flow.GetRepnum() == tio.GetRepnum())

	at := sizeToDuration(frsize, "B", flow.getbw(), "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, srv, tio, frsize)
	return &ReplicaDataEvent{zDataEvent{zEvent{*timedev}, cid, repnum, flow.getoffset(), flow.getbw()}}
}

// alternative c-tor
func newRepDataEvent(gwy RunnerInterface, srv RunnerInterface, tio TioInterface, offset int64, frsize int) *ReplicaDataEvent {
	flow := tio.GetFlow()
	at := sizeToDuration(frsize, "B", flow.getbw(), "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, srv, tio, frsize)
	return &ReplicaDataEvent{zDataEvent{zEvent{*timedev}, tio.GetCid(), tio.GetRepnum(), offset, flow.getbw()}}
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
func newMcastChunkDataEvent(gwy RunnerInterface, rzvgroup GroupInterface, chunk *Chunk, flow FlowInterface, frsize int, tio TioInterface) *ReplicaDataEvent {
	at := sizeToDuration(frsize, "B", flow.getbw(), "b") + config.timeClusterTrip
	timedev := newTimedAnyEvent(gwy, at, rzvgroup, tio, tio.GetTarget(), true, frsize)

	return &ReplicaDataEvent{zDataEvent{zEvent{*timedev}, chunk.cid, 0, flow.getoffset(), flow.getbw()}}
}
