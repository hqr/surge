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
	"sort"
	"sync/atomic"
	"time"
)

type modelEight struct {
	putpipeline *Pipeline
}

//========================================================================
// m8 nodes
//========================================================================
type gatewayEight struct {
	GatewayMcast
	bids        *GatewayBidQueue
	prevgroupid int
	// FIXME: extra state for requestMoreReplicas()
	ngobj         *NgtGroup
	lastright     time.Time
	prevlastright time.Time
	requestat     time.Time
	tioparent     *Tio
}

type serverEight struct {
	ServerUch
	bids *ServerSparseBidQueue
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
// Run contains the gateway's receive callback and its goroutine. Each of the
// gateway instances (the running number of which is configured as
// config.numGateways) has a type gatewayFive and spends all its given runtime
// inside its own goroutine.
//
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
			r.processPendingEvents(r.rxcallback)

			// the second condition (rzvgroup.getCount() ...)
			// corresponds to the post-negotiation phase when
			// all the bids are already received, "filtered" via findBestIntersection()
			// and the corresponding servers selected for the chunk transfer.
			//
			if r.chunk != nil {
				r.sendata()
			}
			diff := Now.Sub(r.requestat)
			if diff >= 0 {
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
	tioparent := r.putpipeline.NewTio(r, r.chunk)
	mcastflow := NewFlow(r, r.chunk.cid, tioparent, ngobj)
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
		tio := r.putpipeline.NewTio(r, tioparent, r.chunk, srv)
		tio.removeWhenDone = false
	}
	assert(len(tioparent.children) == ngobj.getCount())

	mcastflow.tobandwidth = configNetwork.linkbpsControl
	mcastflow.totalbytes = r.chunk.sizeb
	mcastflow.rb = &DummyRateBucket{}

	log("gwy-new-chunk", mcastflow.String(), tioparent.String())

	r.requestMoreReplicas(ngobj, tioparent, Now)
}

func (r *gatewayEight) requestMoreReplicas(ngobj *NgtGroup, tioparent *Tio, left time.Time) {
	targets := ngobj.getmembers()
	mcastflow := tioparent.flow

	assert(!left.Equal(TimeNil))
	if r.lastright.Equal(left) {
		r.prevlastright = r.lastright
	}
	r.lastright = TimeNil
	r.requestat = TimeNil
	r.tioparent = nil
	r.ngobj = nil

	assert(r.rzvgroup.getCount() < configStorage.numReplicas)

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
		tio := tioparent.children[srv]
		//
		// the MCPR event carries extra information - comments below
		//
		ev := newMcastChunkPutRequestEvent(r, ngobj, r.chunk, srv, tio)
		ev.winleft = left          // allocate bid at or after this time, please
		ev.rzvgroup = r.rzvgroup   // exclude these servers, already selected before
		ev.tiostage = "REQUEST-NG" // force tio to execute this event's stage

		tio.next(ev, SmethodDirectInsert)
	}
	atomic.StoreInt64(&r.NowMcasting, 0)

	r.rb.use(int64(configNetwork.sizeControlPDU * 8))
	mcastflow.rb.use(int64(configNetwork.sizeControlPDU * 8))
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
	tiochild := tioevent.GetTio()
	tioparent := tiochild.parent
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
	// critical point: filterBestSequence
	//
	r.bids.filterBestSequence(r.chunk, configStorage.numReplicas-rzvcnt)
	l := len(r.bids.pending)
	assert(len(r.bids.pending) > 0)
	cstr := fmt.Sprintf("chunk#%d", r.chunk.sid)
	log("enough-bids", r.bids.StringBids())

	// given to-be-accepted new bids create the corresponding unicast flows
	// use temp ("incremental") rendezvous group to indicate newly accepted
	incrzvgroup := &RzvGroup{servers: make([]RunnerInterface, l)}
	for k := 0; k < l; k++ {
		bid := r.bids.pending[k]
		tio := bid.tio
		assert(tioparent.haschild(tio))
		srv := tio.target
		assert(tio == tioparent.children[srv])

		assert(ngobj.hasmember(srv))
		assert(!r.rzvgroup.hasmember(srv))

		// construct unicast flows
		// gateway => (child tio, unicast flow) => target server
		flow := NewFlow(r, r.chunk.cid, srv, tio)
		assert(tio.flow == flow)
		flow.rb = &DummyRateBucket{}
		flow.tobandwidth = configNetwork.linkbpsData
		flow.totalbytes = r.chunk.sizeb
		flow.extension = bid
		log("gwy-new-flow", flow.String(), bid.String())

		incrzvgroup.servers[k] = srv
	}
	log("recv-bid-incrzvgroup", incrzvgroup.String(), cstr)

	// generate and send PutAccept
	r.accept(ngobj, tioparent, incrzvgroup)

	// merge
	for k := 0; k < l; k++ {
		r.rzvgroup.servers[rzvcnt+k] = incrzvgroup.servers[k]
	}
	r.sortrzv(r.rzvgroup)
	log("updated-rzvgroup", r.String(), r.rzvgroup.String(), cstr)

	if r.rzvgroup.getCount() == configStorage.numReplicas {
		return nil
	}
	//
	// requestMoreReplicas if required
	// - in-between the accepted bids
	// - immediately, if possible
	// - scheduled at a later time
	//
	bid0 := r.bids.pending[0]
	r.lastright = r.bids.pending[l-1].win.right
	r.requestat = TimeNil
	r.tioparent = tioparent
	r.ngobj = ngobj

	if !r.prevlastright.Equal(TimeNil) && bid0.win.left.Sub(r.prevlastright) > configNetwork.durationControlPDU {
		x := r.prevlastright
		// need time to Accept
		if x.Sub(Now) > configNetwork.durationControlPDU+config.timeClusterTrip {
			r.requestat = x
			y := x.Sub(time.Time{})
			log("more-reps-before", r.String(), y)
			return nil
		}
	}

	if l > 1 {
		bid1 := r.bids.pending[1]
		if bid1.win.left.Sub(bid0.win.right) > configNetwork.durationControlPDU {
			r.requestat = bid0.win.right
			x := r.requestat.Sub(time.Time{})
			log("more-reps-between", r.String(), x)
			return nil
		}
	}

	r.requestat = r.lastright.Add(-configNetwork.durationControlPDU * 2)
	if r.requestat.Before(r.bids.pending[l-1].win.left) {
		r.requestat = r.bids.pending[l-1].win.left
	}
	x := r.requestat.Sub(time.Time{})
	log("more-reps-later", r.String(), x)
	return nil
}

func (r *gatewayEight) sortrzv(rzvgroup *RzvGroup) {
	cnt := rzvgroup.getCount()
	if cnt == 1 {
		return
	}
	ids := make([]int, cnt)
	for k := 0; k < cnt; k++ {
		assert(rzvgroup.servers[k] != nil)
		ids[k] = rzvgroup.servers[k].GetID()
	}
	sort.Ints(ids)
	for k := 0; k < cnt; k++ {
		rzvgroup.servers[k] = r.eps[ids[k]]
	}

}

// accept-ng control/stage
func (r *gatewayEight) accept(ngobj *NgtGroup, tioparent *Tio, incrzvgroup *RzvGroup) {
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
		tio := tioparent.children[srv]
		acceptev := newMcastChunkPutAcceptEvent(r, ngobj, r.chunk, incrzvgroup, tio)
		//
		// force the pre-allocated tio to execute this event's stage,
		// possibly multiple times
		//
		acceptev.tiostage = "ACCEPT-NG" // force the pre-allocated tio to execute this event's stage
		//
		// FIXME: redundant versus gwy-flow.extension
		//
		for k := 0; k < len(r.bids.pending); k++ {
			bid := r.bids.pending[k]
			if bid.tio == acceptev.tio {
				acceptev.bid = bid
				cnt++
			}
		}
		tio.next(acceptev, SmethodDirectInsert)
	}
	assert(cnt == incrzvgroup.getCount())
	atomic.StoreInt64(&r.NowMcasting, 0)

	r.rb.use(int64(configNetwork.sizeControlPDU * 8))
	mcastflow := tioparent.flow
	mcastflow.rb.use(int64(configNetwork.sizeControlPDU * 8))
	mcastflow.timeTxDone = Now.Add(configNetwork.durationControlPDU)

	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))

	// once the required number of selected bids is reached:
	// cleanup redundant child tios
	if r.rzvgroup.getCount() == configStorage.numReplicas {
		for _, srv := range targets {
			if !r.rzvgroup.hasmember(srv) {
				delete(tioparent.children, srv)
			}
		}
		assert(len(tioparent.children) == r.rzvgroup.getCount())
	}
}

// send data periodically walks all pending IO requests and transmits
// data (for the in-progress chunks), when permitted
func (r *gatewayEight) sendata() {
	frsize := configNetwork.sizeFrame

	for _, tioparent := range r.tios {
		mcastflow := tioparent.flow
		if mcastflow.timeTxDone.After(Now) {
			continue
		}
		for srv, tio := range tioparent.children {
			if !r.rzvgroup.hasmember(srv) {
				continue
			}
			flow := tio.flow
			if flow == nil {
				continue
			}
			assert(flow.tio == tio)
			if flow.offset >= r.chunk.sizeb {
				continue
			}
			bid, ok := flow.extension.(*PutBid)
			assert(ok)
			if Now.Before(bid.win.left) {
				continue
			}
			if flow.timeTxDone.After(Now) {
				continue
			}
			// do send data packet
			assert(flow.totalbytes == r.chunk.sizeb)
			if flow.offset+frsize > r.chunk.sizeb {
				frsize = r.chunk.sizeb - flow.offset
			}

			flow.offset += frsize
			newbits := int64(frsize * 8)
			ev := newReplicaDataEvent(r, srv, r.chunk.cid, 0, flow, frsize)
			r.Send(ev, SmethodWait)

			d := time.Duration(newbits) * time.Second / time.Duration(flow.tobandwidth)
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
	tio := ev.GetTio()
	tioparent := tio.parent
	flow := tio.flow

	assert(flow.cid == tioevent.cid)
	assert(tioparent.haschild(tio))
	assert(r.chunk != nil, "chunk nil,"+tioevent.String()+","+r.rzvgroup.String())

	if r.replicackCommon(tioevent) == ChunkDone {
		cstr := fmt.Sprintf("chunk#%d", flow.sid)
		log(r.String(), cstr, "=>", r.rzvgroup.String())
		r.bids.cleanup()

		// cleanup tios
		//tioparent.children = nil
		r.RemoveTio(tioparent)
		r.tioparent = nil
		r.requestat = TimeNil
		r.lastright = TimeNil
		r.prevlastright = TimeNil
		r.ngobj = nil
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

			log(LogVV, "SRV::rxcallback: chunk data", tioevent.String(), bid.String())
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
			log(LogV, "SRV::rxcallback", ev.String(), r.String())
			// ev.GetSize() == configNetwork.sizeControlPDU
			r.addBusyDuration(configNetwork.sizeControlPDU, configNetwork.linkbpsControl, NetControlBusy)

			tio.doStage(r, ev)
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
	tio := tioevent.GetTio()
	gwy := tioevent.GetSource()
	ngobj := tioevent.GetGroup()
	rzvgroup := tioevent.rzvgroup

	assert(tio.source == gwy)
	assert(ngobj.hasmember(r))
	assert(rzvgroup != nil)

	// do nothing if previously selected
	if rzvgroup.hasmember(r) {
		return nil
	}

	// compute disk-queue related delay
	var diskdelay time.Duration
	num, duration := r.disk.queueDepth(DqdChunks)
	if num >= configStorage.maxDiskQueueChunks && duration > configNetwork.netdurationDataChunk {
		// options: randomize, handle >> max chunks
		if num > configStorage.maxDiskQueueChunks {
			diskdelay = duration
		} else {
			diskdelay = duration - configNetwork.netdurationDataChunk
		}
	}

	var bid *PutBid
	win := &TimWin{tioevent.winleft, TimeNil}
	bid = r.bids.createBid(tio, diskdelay, win)
	if diskdelay > 0 {
		log("srv-delayed-bid", bid.String(), diskdelay)
	}
	bidev := newBidEvent(r, gwy, ngobj, tioevent.cid, bid, tio)
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
	tio := tioevent.GetTio()

	rzvgroup := tioevent.rzvgroup
	if !rzvgroup.hasmember(r) {
		r.bids.cancelBid(tio)
		return nil
	}

	// accepted
	gwyflow := tio.flow
	assert(gwyflow.cid == tioevent.cid)
	mybid := gwyflow.extension.(*PutBid)
	//
	// FIXME: gwyflow.extension == tioevent.bid is REDUNDANT
	//
	assert(mybid == tioevent.bid, mybid.String()+","+tioevent.String())
	r.bids.acceptBid(tio, mybid)

	//new server's flow
	flow := NewFlow(gwy, tioevent.cid, r, tio)
	flow.totalbytes = tioevent.sizeb
	flow.tobandwidth = configNetwork.linkbpsData
	log("srv-new-flow", flow.String(), mybid.String())
	r.flowsfrom.insertFlow(flow)

	return nil
}

//==================================================================
//
// modelEight interface methods
//
//==================================================================
func (m *modelEight) NewGateway(i int) RunnerInterface {
	gwy := NewGatewayMcast(i, m8.putpipeline)
	gwy.rb = &DummyRateBucket{}
	gwy.rzvgroup.incremental = true

	rgwy := &gatewayEight{GatewayMcast: *gwy}
	rgwy.rptr = rgwy // realobject
	bids := NewGatewayBidQueue(rgwy)
	rgwy.bids = bids
	rgwy.requestat = TimeNil
	rgwy.prevlastright = TimeNil
	rgwy.lastright = TimeNil
	return rgwy
}

func (m *modelEight) NewServer(i int) RunnerInterface {
	srv := NewServerUch(i, m5.putpipeline)
	rsrv := &serverEight{ServerUch: *srv}
	rsrv.ServerUch.rptr = rsrv
	rsrv.flowsfrom = NewFlowDir(rsrv, config.numGateways)
	bids := NewServerSparseBidQueue(rsrv, 0)
	rsrv.bids = bids
	return rsrv
}

func (m *modelEight) Configure() {
	configReplicast.bidMultiplierPct = 110
	configureReplicast(false)
	configReplicast.minduration = configReplicast.durationBidWindow - config.timeClusterTrip
}
