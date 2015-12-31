// Package surge provides a framework for discrete event simulation, as well as
// a number of models for Unsolicited and Reservation Group based Edge-driven
// load balancing.
//
// modelSeven (m7) models Replicast(tm) -
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
// all the 3 servers simultaneously. More exactly, in this model (m7):
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
	"os"
	"sort"
	"sync/atomic"
	"time"
)

type modelSeven struct {
	putpipeline *Pipeline
}

//========================================================================
// m7 nodes
//========================================================================
type gatewaySeven struct {
	GatewayMcast
	bids        *GatewayBidQueue
	NowMcasting int64
	prevgroupid int
}

type serverSeven struct {
	ServerUch
	bids *ServerBidQueue
}

//
// static & init
//
var m7 modelSeven

func init() {
	p := NewPipeline()
	p.AddStage(&PipelineStage{name: "REQUEST-NG", handler: "M7requestng"})
	p.AddStage(&PipelineStage{name: "BID", handler: "M7receivebid"})
	p.AddStage(&PipelineStage{name: "ACCEPT-NG", handler: "M7acceptng"})
	p.AddStage(&PipelineStage{name: "REPLICA-ACK", handler: "M7replicack"})

	m7.putpipeline = p

	d := NewStatsDescriptors("7")
	d.Register("rxbusydata", StatsKindPercentage, StatsScopeServer)
	d.Register("rxbusy", StatsKindPercentage, StatsScopeServer) // data + control
	d.Register("diskbusy", StatsKindPercentage, StatsScopeServer)
	d.Register("chunk", StatsKindCount, StatsScopeGateway)
	d.Register("replica", StatsKindCount, StatsScopeGateway)
	d.Register("txbytes", StatsKindByteCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxbytes", StatsKindByteCount, StatsScopeServer|StatsScopeGateway)
	d.Register("disk-queue-depth", StatsKindSampleCount, StatsScopeServer)

	props := make(map[string]interface{}, 1)
	props["description"] = "Basic Replicast(tm)"
	RegisterModel("7", &m7, props)
}

//==================================================================
//
// gatewaySeven methods
//
//==================================================================
// Run contains the gateway's receive callback and its goroutine. Each of the
// gateway instances (the running number of which is configured as
// config.numGateways) has a type gatewayFive and spends all its given runtime
// inside its own goroutine.
//
// As per rxcallback below, the gateway handles the model's pipeline stages
// (via generic doStage)
//
func (r *gatewaySeven) Run() {
	r.state = RstateRunning

	rxcallback := func(ev EventInterface) int {
		tio := ev.GetTio()
		log(LogV, "GWY::rxcallback", tio.String())
		tio.doStage(r, ev)
		if tio.done {
			log(LogV, "tio-done", tio.String())
			atomic.AddInt64(&r.tiostats, int64(1))
		}
		return ev.GetSize()
	}

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
			r.processPendingEvents(rxcallback)

			// the second condition (rzvgroup.getCount() ...)
			// corresponds to the post-negotiation phase when
			// all the bids are already received, "filtered" via filterBestBids()
			// and the corresponding servers selected for the chunk transfer.
			//
			if r.chunk != nil && r.rzvgroup.getCount() == configStorage.numReplicas {
				r.sendata()
			}
		}
		r.closeTxChannels()
	}()
}

//==========================
// gatewaySeven TIO handlers
//==========================
// M7receivebid is invoked by the generic tio-processing code when
// the latter (tio) undergoes the corresponding pipeline stage named
// "BID"
// The pipeline itself is declared at the top of this model.
func (r *gatewaySeven) M7receivebid(ev EventInterface) error {
	tioevent := ev.(*BidEvent)
	tiochild := tioevent.GetTio()
	tioparent := tiochild.parent
	assert(tioparent.haschild(tiochild))
	log(LogVV, r.String(), "::M7receivebid()", tioevent.String())
	srv := tioevent.GetSource()
	group := tioevent.GetGroup()
	ngobj, ok := group.(*NgtGroup)
	assert(ok)
	assert(ngobj.hasmember(srv))

	n := r.bids.receiveBid(tiochild, tioevent.bid)
	// keep reschedulng until each server in the multicast group
	// responds with a bid
	if n < configReplicast.sizeNgtGroup {
		return nil
	}
	computedbid := r.bids.filterBestBids(r.chunk)

	// returned nil indicates a failure:
	// received bids are too far apart to produce a common
	// usable reservation window for the multicast put.
	//
	// Cancel all server bids and cleanup -
	// note that rzvgroup is nil-inited at this point
	if computedbid == nil {
		r.accept(ngobj, tioparent)

		r.bids.cleanup()
		tioparent.abort()
		r.chunk = nil
		return nil
	}

	mcastflow := tioparent.flow
	mcastflow.extension = computedbid

	// fill in the multicast rendezvous group for chunk data transfer
	// note that pending[] bids at these point are already "filtered"
	// to contain only those bids that were selected
	ids := make([]int, configStorage.numReplicas)
	for k := 0; k < configStorage.numReplicas; k++ {
		bid := r.bids.pending[k]
		assert(ngobj.hasmember(bid.tio.target))
		ids[k] = bid.tio.target.GetID()

		assert(r.eps[ids[k]] == bid.tio.target)
	}
	sort.Ints(ids)
	for k := 0; k < configStorage.numReplicas; k++ {
		r.rzvgroup.servers[k] = r.eps[ids[k]]
	}

	r.rzvgroup.init(ngobj.getID(), false)
	assert(r.rzvgroup.getCount() == configStorage.numReplicas)

	r.accept(ngobj, tioparent)
	mcastflow.tobandwidth = configNetwork.linkbpsData
	return nil
}

// accept-ng control/stage
func (r *gatewaySeven) accept(ngobj *NgtGroup, tioparent *Tio) {
	assert(len(tioparent.children) == ngobj.getCount())

	targets := ngobj.getmembers()

	//
	// send multicast put-accept to all servers in the negotiating group
	// (ngobj);
	// carry the 3 (or configured) servers selected for the operation
	// (rzvgroup)
	// inside this put-accept message
	//
	// atomic on/off here is done to make sure all the events are issued at
	// the same exact system tick, to simulate multicast IP router
	// simultaneously transmitting a buffered frame on all designated ports
	//
	atomic.StoreInt64(&r.NowMcasting, 1)
	for _, srv := range targets {
		tio := tioparent.children[srv]
		acceptev := newMcastChunkPutAcceptEvent(r, ngobj, r.chunk, r.rzvgroup, tio)
		tio.next(acceptev, SmethodDirectInsert)
	}
	atomic.StoreInt64(&r.NowMcasting, 0)

	r.rb.use(int64(configNetwork.sizeControlPDU * 8))
	mcastflow := tioparent.flow
	mcastflow.rb.use(int64(configNetwork.sizeControlPDU * 8))
	mcastflow.timeTxDone = Now.Add(configNetwork.durationControlPDU)

	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))

	for _, srv := range targets {
		// cleanup child tios that weren't accepted
		if !r.rzvgroup.hasmember(srv) {
			delete(tioparent.children, srv)
		}
	}
	assert(len(tioparent.children) == r.rzvgroup.getCount())
}

// M7replicack is invoked by the generic tio-processing code when
// the latter (tio) undergoes the pipeline stage named "REPLICA-ACK"
// This callback processes replica put ack from the server
//
// The pipeline itself is declared at the top of this model.
func (r *gatewaySeven) M7replicack(ev EventInterface) error {
	tioevent := ev.(*ReplicaPutAckEvent)
	tio := ev.GetTio()
	tioparent := tio.parent
	assert(tioparent.haschild(tio))
	group := tioevent.GetGroup()
	flow := tio.flow
	assert(flow.cid == tioevent.cid)
	assert(group == r.rzvgroup)

	assert(r.chunk != nil, "chunk nil,"+tioevent.String()+","+r.rzvgroup.String())
	assert(r.rzvgroup.getCount() == configStorage.numReplicas, "incomplete group,"+r.String()+","+r.rzvgroup.String()+","+tioevent.String())
	cstr := fmt.Sprintf("chunk#%d", flow.sid)

	if r.replicackCommon(tioevent) == ChunkDone {
		log(r.String(), cstr, "=>", r.rzvgroup.String())
		r.bids.cleanup()
	}

	return nil
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
func (r *gatewaySeven) startNewChunk() {
	assert(r.chunk == nil)
	r.chunk = NewChunk(r, configStorage.sizeDataChunk*1024)

	r.numreplicas = 0
	r.rzvgroup.init(0, true) // cleanup

	// given a pseudo-random chunk id, select a multicast group
	// to store this chunk
	ngid := r.selectNgtGroup(r.chunk.cid, r.prevgroupid)
	ngobj := NewNgtGroup(ngid)

	// create flow and tios
	targets := ngobj.getmembers()
	assert(len(targets) == configReplicast.sizeNgtGroup)

	// new IO request and new multicast flow
	tioparent := r.putpipeline.NewTio(r, r.chunk)
	mcastflow := NewFlow(r, r.chunk.cid, tioparent, ngobj)

	// children tios; each server must see its own copy of
	// the message
	for _, srv := range targets {
		r.putpipeline.NewTio(r, tioparent, r.chunk, mcastflow, srv)
	}
	assert(len(tioparent.children) == ngobj.getCount(), fmt.Sprintf("%d != %d", len(tioparent.children), ngobj.getCount()))

	mcastflow.tobandwidth = int64(0)
	mcastflow.totalbytes = r.chunk.sizeb
	mcastflow.rb = &DummyRateBucket{}

	log("gwy-new-mcastflow-tio", mcastflow.String(), tioparent.String())

	// start negotiating; each server in the negotiating group
	// will subsequently respond with a bid
	//
	// atomic on/off here and elsewhere is done to make sure that
	// all the put-request messages are issued at
	// the same exact system tick, to simulate multicast IP router
	// simultaneously transmitting a buffered frame on all designated ports
	//
	atomic.StoreInt64(&r.NowMcasting, 1)
	for _, srv := range targets {
		tio := tioparent.children[srv]
		ev := newMcastChunkPutRequestEvent(r, ngobj, r.chunk, srv, tio)
		tio.next(ev, SmethodDirectInsert)
	}
	atomic.StoreInt64(&r.NowMcasting, 0)

	r.rb.use(int64(configNetwork.sizeControlPDU * 8))
	mcastflow.rb.use(int64(configNetwork.sizeControlPDU * 8))
	mcastflow.timeTxDone = Now.Add(configNetwork.durationControlPDU)

	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
	r.prevgroupid = ngid
}

// send data periodically walks all pending IO requests and transmits
// data (for the in-progress chunks), when permitted
func (r *gatewaySeven) sendata() {
	frsize := configNetwork.sizeFrame
	for _, tioparent := range r.tios {
		mcastflow := tioparent.flow
		if mcastflow.timeTxDone.After(Now) {
			continue
		}
		if mcastflow.offset >= r.chunk.sizeb {
			continue
		}
		assert(mcastflow.tobandwidth == configNetwork.linkbpsData)

		computedbid := mcastflow.extension.(*PutBid)
		if computedbid == nil {
			continue
		}
		if Now.Before(computedbid.win.left) {
			continue
		}
		if mcastflow.offset+frsize > r.chunk.sizeb {
			frsize = r.chunk.sizeb - mcastflow.offset
		}

		mcastflow.offset += frsize
		newbits := int64(frsize * 8)

		targets := r.rzvgroup.getmembers()

		// multicast via SmethodDirectInsert
		atomic.StoreInt64(&r.NowMcasting, 1)
		for _, srv := range targets {
			tio := tioparent.children[srv]
			ev := newMcastChunkDataEvent(r, r.rzvgroup, r.chunk, mcastflow, frsize, tio)
			srv.Send(ev, SmethodDirectInsert)
		}
		atomic.StoreInt64(&r.NowMcasting, 0)

		d := time.Duration(newbits) * time.Second / time.Duration(mcastflow.tobandwidth)
		mcastflow.timeTxDone = Now.Add(d)
		atomic.AddInt64(&r.txbytestats, int64(frsize))
	}
}

//=============
// multicast vs. system time
//=============
func (r *gatewaySeven) NowIsDone() bool {
	if atomic.LoadInt64(&r.NowMcasting) > 0 {
		return false
	}
	return r.RunnerBase.NowIsDone()
}

//==================================================================
//
// serverSeven methods
//
//==================================================================
// Run provides the servers's receive callback that executes both control path
// (via doStage()) and receives chunk/replica data, via DataEvent
//
func (r *serverSeven) Run() {
	r.state = RstateRunning

	rxcallback := func(ev EventInterface) int {
		switch ev.(type) {
		case *ReplicaDataEvent:
			tioevent := ev.(*ReplicaDataEvent)
			k, bid := r.bids.findBid(bidFindChunk, tioevent.cid)
			assert(bid != nil)
			assert(bid.state == bidStateAccepted)
			assert(!Now.Before(bid.win.left), bid.String())
			if Now.After(bid.win.right) {
				diff := Now.Sub(bid.win.right)
				if diff > config.timeClusterTrip {
					s := fmt.Sprintf("receiving data past bid deadline,%v,%s", diff, bid.String())
					log(LogBoth, s, k)
				}
			}

			log(LogVV, "SRV::rxcallback: chunk data", tioevent.String(), bid.String())
			// once the entire chunk is received, we can cleanup
			// the corresponding accepted bids without waiting for them
			// to self-expire
			//
			if r.receiveReplicaData(tioevent) == ReplicaDone {
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
// serverSeven TIO handlers
//==========================
// M7requestng is invoked by the generic tio-processing code when
// the latter executes pipeline stage named "REQUEST-NG"
// This callback processes put-chunk request from the gateway
//
// The pipeline itself is declared at the top of this model.
func (r *serverSeven) M7requestng(ev EventInterface) error {
	log(r.String(), "::M7requestng()", ev.String())

	tioevent := ev.(*McastChunkPutRequestEvent)
	tio := tioevent.GetTio()
	gwy := tioevent.GetSource()
	assert(tio.source == gwy)
	ngobj := tioevent.GetGroup()
	assert(ngobj.hasmember(r))

	// respond to the put-request with a bid
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
	bid = r.bids.createBid(tio, diskdelay)
	if diskdelay > 0 {
		log("srv-delayed-bid", bid.String(), diskdelay)
	}
	bidev := newBidEvent(r, gwy, ngobj, tioevent.cid, bid, tio)
	log("srv-bid", bidev.String())

	tio.next(bidev, SmethodWait)
	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
	return nil
}

// M7acceptng is invoked by the generic tio-processing code when
// the latter executes pipeline stage named "REQUEST-NG"
// This callback processes put-accept message from the gateway
// and further calls acceptBid() or cancelBid(), depending on whether
// the gateway has selected this server or not.
//
// The pipeline itself is declared at the top of this model.
func (r *serverSeven) M7acceptng(ev EventInterface) error {
	tioevent := ev.(*McastChunkPutAcceptEvent)
	log(r.String(), "::M7acceptng()", tioevent.String())
	gwy := tioevent.GetSource()
	ngobj := tioevent.GetGroup()
	assert(ngobj.hasmember(r))
	tio := tioevent.GetTio()

	rzvgroup := tioevent.rzvgroup
	if !rzvgroup.hasmember(r) {
		r.bids.cancelBid(tio)
		return nil
	}
	gwyflow := tio.flow
	computedbid := gwyflow.extension.(*PutBid)
	r.bids.acceptBid(tio, computedbid)

	//new server's flow
	flow := NewFlow(gwy, tioevent.cid, r, tio)
	flow.totalbytes = tioevent.sizeb
	flow.tobandwidth = configNetwork.linkbpsData
	log("srv-new-flow", flow.String(), computedbid.String())
	r.flowsfrom.insertFlow(flow)

	return nil
}

//==================================================================
//
// modelSeven interface methods
//
//==================================================================
func (m *modelSeven) NewGateway(i int) RunnerInterface {
	gwy := NewGatewayMcast(i, m7.putpipeline)
	gwy.rb = &DummyRateBucket{}

	rgwy := &gatewaySeven{GatewayMcast: *gwy}
	rgwy.rptr = rgwy
	bids := NewGatewayBidQueue(rgwy)
	rgwy.bids = bids
	return rgwy
}

func (m *modelSeven) NewServer(i int) RunnerInterface {
	srv := NewServerUch(i, m5.putpipeline)
	rsrv := &serverSeven{ServerUch: *srv}
	rsrv.ServerUch.rptr = rsrv
	rsrv.flowsfrom = NewFlowDir(rsrv, config.numGateways)
	bids := NewServerBidQueue(rsrv, 0)
	rsrv.bids = bids
	return rsrv
}

func (m *modelSeven) Configure() {
	rem := config.numServers % configReplicast.sizeNgtGroup
	if rem > 0 {
		// FIXME: model.go to support late numServers setting
		config.numServers -= rem
		if config.numServers == configReplicast.sizeNgtGroup {
			log(LogBoth, "Cannot execute the model with a single negotiating group configured, exiting..")
			os.Exit(1)
		}
		log(LogBoth, "NOTE: adjusting the number of servers down, to divide by size of the negotiating group:", config.numServers)
	}

	// only for this model:
	// recompute some of the network config defaults
	// based on the replicast solicited/unsolicited percentage
	//
	configNetwork.linkbpsData = configNetwork.linkbps * int64(configReplicast.solicitedLinkPct) / int64(100)
	configNetwork.linkbpsControl = configNetwork.linkbps - configNetwork.linkbpsData

	configNetwork.durationControlPDU = time.Duration(configNetwork.sizeControlPDU*8) * time.Second / time.Duration(configNetwork.linkbpsControl)
	configNetwork.netdurationDataChunk = time.Duration(configStorage.sizeDataChunk*1024*8) * time.Second / time.Duration(configNetwork.linkbpsData)
}
