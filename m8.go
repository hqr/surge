// Package surge provides a framework for discrete event simulation, as well as
// a number of models for Unsolicited and Reservation Group based Edge-driven
// load balancing.
//
// modelEight (m8) models Replicast(tm) -
// a new storage clustering protocol that leverages multicast replication,
// (the term "Replicast" is derived from replication-oriented multicasting)
// dynamic sub-group load balancing and sub-group based congestion control.
//
// Unlike UCH-* models in this package, Replicast makes resource reservations
// prior to initiating data flows. The flow commences if and when the time
// window is reserved, to allow the entire multicast data transfer to start,
// proceed at the full solicited bandwidth 9Gbps
// (which is 90% of the full 10GE, or configurable),
// and complete without other flows interfering in the process.
//
// A multicast flow that delivers a 1MB (or configurable) chunk to
// 3 servers (thus producing 3 stored replicas of the chunk)
// must take care of reserving a single time window that "satisfies"
// all the 3 servers simultaneously. More exactly, in this model (m8):
//
// 1) gateways make reservations of the server's memory and bandwidth
// 2) multiple storage servers receive the reservation request
//    - all storage servers are grouped into pre-configured multicast
//      groups a.k.a negotiating groups
// 3) each of the servers in the group responds with a bid carrying
//    a time window reserved by the server for this request
//
// Similar to all the other models in this package, multiple storage gateways
// independently and concurrently communicate to storage servers,
// each one trying to make the best and the earliest reservation for itself.
// Given the same number of clustered nodes and the same disk and network
// bandwidth, the resulting throughput, latency, utilization statistics
// can be compared and analyzed across both unicast and multicast (Replicast)
// families of protocols.
//
package surge

import (
	"fmt"
	"sync/atomic"
	"time"
)

type modelEight struct {
	ModelGeneric
	putpipeline *Pipeline
}

//========================================================================
// m8 nodes
//========================================================================
type gatewayEight struct {
	GatewayMcast
	bids        *GatewayBidQueue
	prevgroupid int
	// extra state for requestMoreReplicas()
	ngobj     *NgtGroup
	tioparent *TioRr
	lastright time.Time
	requestat time.Time
	// max bid window delay: controls renegotiation, doubles every miss
	maxbidwait     time.Duration
	maxbidwaitprev time.Duration
}

type serverEight struct {
	ServerUch
	bids ServerBidQueueInterface
}

//
// static & init
//
var m8 modelEight

func init() {
	p := NewPipeline()
	p.AddStage(&PipelineStage{name: "REQUEST-NG", handler: "M8requestng"})
	p.AddStage(&PipelineStage{name: "BID", handler: "M8receivebid"})
	p.AddStage(&PipelineStage{name: "ACCEPT-NG", handler: "M8acceptng"})
	p.AddStage(&PipelineStage{name: "REPLICA-ACK", handler: "M8replicack"})

	m8.putpipeline = p

	d := NewStatsDescriptors("8")
	d.registerCommonProtoStats()

	props := make(map[string]interface{}, 1)
	props["description"] = "Replicast-H: multicast control with unicast data delivery"
	RegisterModel("8", &m8, props)
}

//==================================================================
//
// gatewayEight methods
//
//==================================================================
// TODO: add comment
func (r *gatewayEight) Run() {
	r.state = RstateRunning

	go func() {
		for r.state == RstateRunning {
			// previous chunk done? Tx link is available? If yes,
			// start a new put-chunk operation
			if r.chunk == nil {
				if r.rb.above(int64(configNetwork.sizeControlPDU * 8)) {
					r.startNewChunk()
				}
			}
			// recv
			r.receiveEnqueue()
			r.processPendingEvents(r.rxcb)

			// the second condition (rzvgroup.getCount() ...)
			// corresponds to the post-negotiation phase when
			// all the bids are already received, "filtered" via findBestIntersection()
			// and the corresponding servers selected for the chunk transfer.
			//
			if r.chunk != nil {
				r.sendata()
			}
			if r.requestat.Equal(TimeNil) {
				continue
			}
			if Now.Sub(r.requestat) >= 0 {
				r.requestMoreReplicas(r.ngobj, r.tioparent, r.lastright)
			}
		}
		r.closeTxChannels()
	}()
}

// Similar to unicast models in this package,
// startNewChunk is in effect a minimalistic embedded client.
// The client, via startNewChunk, generates a new random chunk if and once
// the required number of replicas (configStorage.numReplicas)
// of the previous chunk are transmitted,
// written to disks and then ACK-ed by the respective servers.
//
// Since all the gateways in the model run the same code, the cumulative
// effect of randomly generated chunks by many dozens or hundreds
// gateways means that there's probably no need, simulation-wise,
// to have each gateway generate multiple chunks simultaneously..
//
func (r *gatewayEight) startNewChunk() {
	assert(r.chunk == nil)
	r.chunk = NewChunk(r, configStorage.sizeDataChunk*1024)

	r.numreplicas = 0
	r.rzvgroup.init(0, true) // cleanup

	// given a pseudo-random chunk id, select a multicast group
	// to store this chunk
	ngid := r.selectNgtGroup(r.chunk.cid, r.prevgroupid)
	ngobj := NewNgtGroup(ngid)
	r.prevgroupid = ngid

	//
	// construct multicast flow (control plane) and its tio-parent
	// construct all tio children to be used later, maybe
	// note the relationship:
	//       gateway => (tio-child, unicast flow) => server
	// unicast flows are created later, on a per selected-bid basis
	//
	tioparent := NewTioRr(r.realobject(), r.putpipeline, r.chunk)
	mcastflow := NewFlow(r.realobject(), r.chunk.cid, tioparent, ngobj)
	targets := ngobj.getmembers()
	for _, srv := range targets {
		//
		// child tios are preallocated for the duration of chunk
		// at least some of the child tios will execute the same
		// stage(s) multiple times
		// they are created flow-less: unicast flows are c-tored
		// later on demand
		// Note also that multiple-times-same-stage tios should
		// not be automatically removed when done
		//
		tio := NewTioRr(r.realobject(), r.putpipeline, tioparent, r.chunk, srv)
		tio.removeWhenDone = false
	}
	assert(tioparent.GetNumTioChildren() == ngobj.getCount())

	mcastflow.setbw(configNetwork.linkbpsControl)
	mcastflow.totalbytes = r.chunk.sizeb
	mcastflow.rb = &DummyRateBucket{}

	log("gwy-new-chunk", mcastflow.String(), tioparent.String())

	r.ngobj = ngobj
	r.tioparent = tioparent

	// recompute maxbidwait
	r.maxbidwait, r.maxbidwaitprev = (r.maxbidwait+r.maxbidwaitprev)/2, r.maxbidwait

	r.requestMoreReplicas(ngobj, tioparent, Now)
}

func (r *gatewayEight) requestMoreReplicas(ngobj *NgtGroup, tioparent *TioRr, left time.Time) {
	targets := ngobj.getmembers()
	mcastflow := tioparent.GetFlow().(*Flow)

	r.lastright = TimeNil
	r.requestat = TimeNil

	assert(r.rzvgroup.getCount() < configStorage.numReplicas, r.String()+","+r.rzvgroup.String())

	if r.rzvgroup.getCount() > 0 {
		x := left.Sub(time.Time{})
		log("requesting-more-reps-at", r.String(), r.rzvgroup.String(), x)
	}

	// remove previous bids if any, empty the gateway's queue for the new ones
	r.bids.cleanup()

	// start negotiating; each server in the negotiating group
	// *** except those that are already in rzvgroup ***
	// will subsequently respond with a bid
	//
	// atomic on/off here and elsewhere is done to make sure that
	// all the put-request messages are issued at
	// the same exact system tick, to simulate multicast IP router
	// simultaneously transmitting a buffered frame on all designated ports
	//
	atomic.StoreInt64(&r.NowMcasting, 1)
	for _, srv := range targets {
		// if the bid is already accepted, the corresponding tio
		// is owned by the server
		if r.rzvgroup.hasmember(srv) {
			continue
		}
		tio := tioparent.GetTioChild(srv)
		//
		// the MCPR event carries extra information - comments below
		//
		ev := newMcastChunkPutRequestEvent(r.realobject(), ngobj, r.chunk, srv, tio)
		ev.winleft = left          // allocate bid at or after this time, please
		ev.rzvgroup = r.rzvgroup   // exclude these servers, already selected before
		ev.tiostage = "REQUEST-NG" // force tio to execute this event's stage

		tio.next(ev, SmethodDirectInsert)
	}
	atomic.StoreInt64(&r.NowMcasting, 0)

	r.rb.use(int64(configNetwork.sizeControlPDU * 8))
	mcastflow.GetRb().use(int64(configNetwork.sizeControlPDU * 8))
	mcastflow.timeTxDone = Now.Add(configNetwork.durationControlPDU)

	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
}

//==========================
// gatewayEight TIO handlers
//==========================
// M8receivebid is invoked by the generic tio-processing code when
// the latter (tio) undergoes the corresponding pipeline stage named
// "BID"
// The pipeline itself is declared at the top of this model.
func (r *gatewayEight) M8receivebid(ev EventInterface) error {
	tioevent := ev.(*BidEvent)
	tiochild := tioevent.GetTio().(*TioRr)
	tioparent := tiochild.GetParent().(*TioRr)
	srv := tioevent.GetSource()
	group := tioevent.GetGroup()
	ngobj, ok := group.(*NgtGroup)

	assert(ok)
	assert(tioparent.haschild(tiochild))
	assert(ngobj.hasmember(srv))
	assert(!r.rzvgroup.hasmember(srv))
	assert(r.rzvgroup.getCount() < configStorage.numReplicas)

	log(LogVV, r.String(), "::M8receivebid()", tioevent.String())

	//
	// keep rescheduling until each server in the negotiating group
	// *** except those that were already selected and are currently rzv-group members ***
	// responds with a bid
	//
	n := r.bids.receiveBid(tiochild, tioevent.bid)
	rzvcnt := r.rzvgroup.getCount()
	if n < configReplicast.sizeNgtGroup-rzvcnt {
		return nil
	}
	//
	// collected all bid responses - now find the best
	//
	r.bids.filterBestSequence(r.chunk, configStorage.numReplicas-rzvcnt)
	l := len(r.bids.pending)
	log("filtered-bids", r.bids.StringBids())
	assert(l > 0 && l <= configStorage.numReplicas-rzvcnt, l)

	incrzvgroup := &RzvGroup{servers: make([]NodeRunnerInterface, l)}

	bid0 := r.bids.pending[0]
	idletime := bid0.win.left.Sub(Now)
	cstr := fmt.Sprintf("c#%d", r.chunk.sid)
	// renegotiate if the closest bid is twice beyond maxwait
	if idletime >= r.maxbidwait*2 {
		log("WARNING: abort-retry", cstr, "idle", idletime, "maxwait", r.maxbidwait)
		r.maxbidwait *= 2
		r.bids.cleanup()
		r.accept(ngobj, tioparent, incrzvgroup)
		r.scheduleMore(tioparent, cstr)
		return nil
	}
	// given to-be-accepted new bids create the corresponding unicast flows
	// use temp ("incremental") rendezvous group to indicate newly accepted
	for k := 0; k < l; k++ {
		bid := r.bids.pending[k]
		tio := bid.tio
		tio.bid = bid
		srv := tio.GetTarget()

		incrzvgroup.servers[k] = srv
	}
	log("recv-bid-incrzvgroup", incrzvgroup.String(), cstr)

	// generate and send PutAccept
	r.accept(ngobj, tioparent, incrzvgroup)

	// merge-sort new servers
	r.mergerzv(incrzvgroup)
	log("updated-rzvgroup", r.String(), r.rzvgroup.String(), cstr)

	newcnt := r.rzvgroup.getCount()
	assert(newcnt == rzvcnt+l)
	if newcnt == configStorage.numReplicas {
		return nil
	}

	r.scheduleMore(tioparent, cstr)
	return nil
}

// construct unicast flows
// gateway => (child tio, unicast flow) => target server
func (r *gatewayEight) newflow(srv NodeRunnerInterface, tio *TioRr) {
	flow := NewFlow(r.realobject(), r.chunk.cid, srv, tio)
	assert(tio.GetFlow() == flow)
	flow.rb = &DummyRateBucket{}
	flow.setbw(configNetwork.linkbpsData)
	flow.totalbytes = r.chunk.sizeb
	log("gwy-new-flow", flow.String(), tio.bid.String())
}

func (r *gatewayEight) scheduleMore(tioparent *TioRr, cstr string) {
	mcastflow := tioparent.GetFlow().(*Flow)
	l := len(r.bids.pending)

	if l > 0 {
		r.lastright = r.bids.pending[l-1].win.right
		r.requestat = r.bids.pending[l-1].win.left
	} else {
		r.requestat = Now.Add(r.maxbidwait / 4) // FIXME: configReplicast
		r.lastright = r.requestat
	}
	t := mcastflow.timeTxDone.Add(config.timeClusterTrip + configNetwork.durationControlPDU/2)
	if r.requestat.Before(t) {
		r.requestat = t
	}
	x := r.requestat.Sub(time.Time{})
	log("more-reps-later", r.String(), cstr, x)
}

func (r *gatewayEight) mergerzv(incrzvgroup *RzvGroup) {
	rzv := r.rzvgroup.servers
	rzvcnt := r.rzvgroup.getCount()
	l := incrzvgroup.getCount()
	for k := 0; k < l; k++ {
		ns := incrzvgroup.servers[k]
		id := ns.GetID()
		if rzvcnt+k == 0 {
			rzv[0] = ns
			continue
		}
		if id > rzv[rzvcnt+k-1].GetID() {
			rzv[rzvcnt+k] = ns
			continue
		}
		for i := 0; i < rzvcnt+k; i++ {
			if id < rzv[i].GetID() {
				copy(rzv[i+1:], rzv[i:])
				rzv[i] = ns
				break
			}
		}
	}
}

// accept-ng control/stage
func (r *gatewayEight) accept(ngobj *NgtGroup, tioparent *TioRr, incrzvgroup *RzvGroup) {
	targets := ngobj.getmembers()

	//
	// send multicast put-accept to all servers in the negotiating group
	// (ngobj);
	// atomic on/off here is done to make sure all the events are issued at
	// the same exact system tick, to simulate multicast IP router
	// simultaneously transmitting a buffered frame on all designated ports
	//
	atomic.StoreInt64(&r.NowMcasting, 1)
	cnt := 0
	for _, srv := range targets {
		// if the bid is already accepted, the corresponding tio
		// is owned by the server: skip the send therefore
		if r.rzvgroup.hasmember(srv) {
			continue
		}
		tio := tioparent.GetTioChild(srv).(*TioRr)
		acceptev := newMcastChunkPutAcceptEvent(r.realobject(), ngobj, r.chunk, incrzvgroup, tio)
		//
		// force the pre-allocated tio to execute this event's stage,
		// possibly multiple times
		//
		acceptev.tiostage = "ACCEPT-NG" // force the pre-allocated tio to execute this event's stage
		for k := 0; k < len(r.bids.pending); k++ {
			bid := r.bids.pending[k]
			if bid.tio == acceptev.GetTio() {
				acceptev.bid = bid
				assert(tio.bid == bid, bid.String())
				cnt++
			}
		}
		tio.next(acceptev, SmethodDirectInsert)
	}
	assert(cnt == incrzvgroup.getCount(), fmt.Sprintf("%d:%d:%d", cnt, incrzvgroup.getCount(), len(r.bids.pending)))
	atomic.StoreInt64(&r.NowMcasting, 0)

	r.rb.use(int64(configNetwork.sizeControlPDU * 8))
	mcastflow := tioparent.GetFlow().(*Flow)
	mcastflow.GetRb().use(int64(configNetwork.sizeControlPDU * 8))
	mcastflow.timeTxDone = Now.Add(configNetwork.durationControlPDU)

	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
}

// send data periodically walks all pending IO requests and transmits
// data (for the in-progress chunks), when permitted
func (r *gatewayEight) sendata() {
	frsize := int64(configNetwork.sizeFrame)

	for _, tioparentint := range r.tios {
		tioparent := tioparentint.(*TioRr)
		for srv, tioint := range tioparent.children {
			if !r.rzvgroup.hasmember(srv) {
				continue
			}
			tio := tioint.(*TioRr)
			flowint := tio.GetFlow()
			if flowint == nil {
				if tio.bid == nil {
					continue
				}
				r.newflow(srv, tio)
				flowint = tio.GetFlow()
			}
			flow := flowint.(*Flow)
			assert(flow.GetTio() == tio)
			if flow.getoffset() >= r.chunk.sizeb {
				continue
			}
			bid := tio.bid
			if Now.Before(bid.win.left) {
				continue
			}
			if flow.timeTxDone.After(Now) {
				continue
			}
			// do send data packet
			assert(flow.totalbytes == r.chunk.sizeb)
			if flow.getoffset()+frsize > r.chunk.sizeb {
				frsize = r.chunk.sizeb - flow.getoffset()
			}

			flow.incoffset(int(frsize))
			newbits := frsize * 8
			ev := newReplicaDataEvent(r.realobject(), srv, r.chunk.cid, 0, flow, int(frsize))
			r.Send(ev, SmethodWait)

			d := time.Duration(newbits) * time.Second / time.Duration(flow.getbw())
			flow.timeTxDone = Now.Add(d)
			atomic.AddInt64(&r.txbytestats, int64(frsize))
		}
	}
}

// M8replicack is invoked by the generic tio-processing code when
// the latter (tio) undergoes the pipeline stage named "REPLICA-ACK"
// This callback processes replica put ack from the server
//
// The pipeline itself is declared at the top of this model.
func (r *gatewayEight) M8replicack(ev EventInterface) error {
	tioevent := ev.(*ReplicaPutAckEvent)
	tio := ev.GetTio().(*TioRr)
	tioparent := tio.GetParent().(*TioRr)
	flow := tio.GetFlow().(*Flow)

	assert(flow.GetCid() == tioevent.cid)
	assert(tioparent.haschild(tio))
	assert(r.chunk != nil, "chunk nil,"+tioevent.String()+","+r.rzvgroup.String())

	if r.replicackCommon(tioevent) == ChunkDone {
		cstr := fmt.Sprintf("c#%d", flow.GetSid())
		log(r.String(), cstr, "=>", r.rzvgroup.String())
		r.bids.cleanup()

		// cleanup tios
		// tioparent.children = nil - too early inside pipeline doStage()
		r.RemoveTio(tioparent)
		r.ngobj = nil
		r.tioparent = nil
		r.requestat = TimeNil
		r.lastright = TimeNil
	}

	return nil
}

//==================================================================
//
// serverEight methods
//
//==================================================================
// Run provides the servers's receive callback that executes both control path
// (via doStage()) and receives chunk/replica data, via DataEvent
//
func (r *serverEight) Run() {
	r.state = RstateRunning

	// FIXME: m7/m8 copy-paste
	rxcallback := func(ev EventInterface) int {
		switch ev.(type) {
		case *ReplicaDataEvent:
			tioevent := ev.(*ReplicaDataEvent)
			k, bid := r.bids.findBid(bidFindChunk, tioevent.cid)
			assert(bid != nil, tioevent.String())
			assert(bid.state == bidStateAccepted)
			assert(!Now.Before(bid.win.left), bid.String())
			if Now.After(bid.win.right) {
				diff := Now.Sub(bid.win.right)
				if diff > config.timeClusterTrip {
					s := fmt.Sprintf("WARNING: receiving data past bid deadline,%v,%s", diff, bid.String())
					log(LogBoth, s, k)
				}
			}
			// once the entire replica is received, we can cleanup
			// the corresponding accepted bids without waiting for them
			// to self-expire
			//
			if r.receiveReplicaData(tioevent) == ReplicaDone {
				log(LogV, "repdone-del-bid", bid.String())
				r.bids.deleteBid(k)
			}
		default:
			tio := ev.GetTio()
			log(LogV, "rxcallback", r.String(), ev.String())
			// ev.GetSize() == configNetwork.sizeControlPDU
			r.addBusyDuration(configNetwork.sizeControlPDU, configNetwork.linkbpsControl, NetControlBusy)

			tio.doStage(r.realobject(), ev)
		}

		return ev.GetSize()
	}

	// this goroutine main loop
	go func() {
		for r.state == RstateRunning {
			r.receiveEnqueue()
			r.processPendingEvents(rxcallback)
		}

		r.closeTxChannels()
	}()
}

//==========================
// serverEight TIO handlers
//==========================
// M8requestng is invoked by the generic tio-processing code when
// the latter executes pipeline stage named "REQUEST-NG"
// This callback processes put-chunk request from the gateway
//
// The pipeline itself is declared at the top of this model.
func (r *serverEight) M8requestng(ev EventInterface) error {
	log(LogVV, r.String(), "::M8requestng()", ev.String())

	tioevent := ev.(*McastChunkPutRequestEvent)
	tio := tioevent.GetTio().(*TioRr)
	gwy := tioevent.GetSource()
	ngobj := tioevent.GetGroup()
	rzvgroup := tioevent.rzvgroup

	assert(tio.GetSource() == gwy)
	assert(ngobj.hasmember(r.realobject()))
	assert(rzvgroup != nil)

	// do nothing if previously selected
	if rzvgroup.hasmember(r.realobject()) {
		return nil
	}

	// assuming (! FIXME) the new chunk has already arrived,
	// compute disk queue delay with respect to the configured maxDiskQueueChunks
	diskIOdone := r.disk.lastIOdone()
	delay := diskdelay(Now, diskIOdone)

	var bid *PutBid
	win := &TimWin{tioevent.winleft, TimeNil}
	bid = r.bids.createBid(tio, delay, win)
	if delay > 0 {
		log("srv-delayed-bid", bid.String(), delay)
	}
	bidev := newBidEvent(r.realobject(), gwy, ngobj, tioevent.cid, bid, tio)
	bidev.tiostage = "BID" // force tio to execute this event's stage
	log(LogV, "srv-bid", bidev.String())

	tio.next(bidev, SmethodWait)
	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
	return nil
}

// M8acceptng is invoked by the generic tio-processing code when
// the latter executes pipeline stage named "REQUEST-NG"
// This callback processes put-accept message from the gateway
// and further calls acceptBid() or cancelBid(), depending on whether
// the gateway has selected this server or not.
//
// The pipeline itself is declared at the top of this model.
func (r *serverEight) M8acceptng(ev EventInterface) error {
	tioevent := ev.(*McastChunkPutAcceptEvent)
	log(LogVV, r.String(), "::M8acceptng()", tioevent.String())
	gwy := tioevent.GetSource()
	ngobj := tioevent.GetGroup()
	assert(ngobj.hasmember(r))
	tio := tioevent.GetTio().(*TioRr)

	rzvgroup := tioevent.rzvgroup
	if !rzvgroup.hasmember(r) {
		r.bids.cancelBid(tio)
		return nil
	}

	// accepted
	gwyflow := tio.GetFlow().(*Flow)
	assert(gwyflow.GetCid() == tioevent.cid)
	assert(tio.bid == tioevent.bid, tio.bid.String()+","+tioevent.String())
	r.bids.acceptBid(tio, tio.bid)

	//new server's flow
	flow := NewFlow(gwy, tioevent.cid, r.realobject(), tio)
	flow.totalbytes = tioevent.sizeb
	flow.setbw(configNetwork.linkbpsData)
	log("srv-new-flow", flow.String(), tio.bid.String())
	r.flowsfrom.insertFlow(flow)

	return nil
}

//==================================================================
//
// modelEight interface methods
//
//==================================================================
func (m *modelEight) NewGateway(i int) NodeRunnerInterface {
	rgwy := m.newGatewayEight(i)
	rgwy.rptr = rgwy // realobject
	bids := NewGatewayBidQueue(rgwy)
	rgwy.bids = bids
	return rgwy
}

func (m *modelEight) newGatewayEight(i int) *gatewayEight {
	gwy := NewGatewayMcast(i, m8.putpipeline)
	gwy.rb = &DummyRateBucket{}
	gwy.rzvgroup.incremental = true

	rgwy := &gatewayEight{GatewayMcast: *gwy}
	rgwy.requestat = TimeNil
	rgwy.rxcb = rgwy.rxcallbackMcast
	rgwy.maxbidwait = configReplicast.maxBidWait
	rgwy.maxbidwaitprev = configReplicast.maxBidWait
	return rgwy
}

func (m *modelEight) NewServer(i int) NodeRunnerInterface {
	rsrv := m.newServerEight(i)
	rsrv.flowsfrom = NewFlowDir(rsrv, config.numGateways)

	bids := NewServerSparseBidQueue(rsrv, 0)
	rsrv.bids = bids
	rsrv.ServerUch.rptr = rsrv
	return rsrv
}

func (m *modelEight) newServerEight(i int) *serverEight {
	srv := NewServerUchRegChannels(i, m8.putpipeline, DtypeConstLatency, 0)
	rsrv := &serverEight{ServerUch: *srv}
	return rsrv
}

func (m *modelEight) Configure() {
	configReplicast.bidMultiplierPct = 110
	configureReplicast(false)
	if configReplicast.durationBidWindow < configNetwork.netdurationFrame*3 {
		configReplicast.bidMultiplierPct = 120
		configureReplicast(false)
	}
	configReplicast.minduration = configReplicast.durationBidWindow - config.timeClusterTrip
}
