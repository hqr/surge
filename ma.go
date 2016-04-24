package surge

import (
	"fmt"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"
)

type modelSevenPrx struct {
	putpipeline *Pipeline
}

//========================================================================
// mA nodes
//========================================================================
// FIXME: same exact as gatewaySeven - try to embed
type gatewaySevenX struct {
	GatewayMcast
	bids        *GatewayBidQueue
	prevgroupid int
}

type serverSevenX struct {
	ServerUch
	bids *ServerSparseBidQueue
}

type serverSevenProxy struct {
	serverSevenX
	allbids []*ServerSparseBidQueue // ditto
	delayed *TxQueue
}

//
// static & init
//
var mA modelSevenPrx

func init() {
	p := NewPipeline()
	p.AddStage(&PipelineStage{name: "REQUEST-PROXY", handler: "M7X_request"})
	p.AddStage(&PipelineStage{name: "BID", handler: "M7X_receivebid"})
	p.AddStage(&PipelineStage{name: "REPLICA-ACK", handler: "M7X_replicack"})

	mA.putpipeline = p

	d := NewStatsDescriptors("a")
	d.registerCommonProtoStats()

	props := make(map[string]interface{}, 1)
	props["description"] = "Unicast proxied control, multicast data"
	RegisterModel("a", &mA, props)
}

//==================================================================
//
// gatewaySevenX methods
//
//==================================================================
// The gatetway's goroutine uses generic rxcallback that strictly processes
// pipeline events
//
func (r *gatewaySevenX) Run() {
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
			if r.chunk != nil && r.rzvgroup.getCount() == configStorage.numReplicas {
				r.sendata()
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
func (r *gatewaySevenX) startNewChunk() {
	assert(r.chunk == nil)
	r.chunk = NewChunk(r, configStorage.sizeDataChunk*1024)

	r.numreplicas = 0
	r.rzvgroup.init(0, true) // cleanup

	// given a pseudo-random chunk id, select a multicast group
	// to store this chunk
	ngid := r.selectNgtGroup(r.chunk.cid, r.prevgroupid)
	ngobj := NewNgtGroup(ngid)
	targets := ngobj.getmembers()
	assert(len(targets) == configReplicast.sizeNgtGroup)

	// establish flow with the designated proxy - leader of the group
	// conventionally, the proxy is simply the:
	firstidx := (ngid - 1) * configReplicast.sizeNgtGroup
	srvproxy := allServers[firstidx]

	tioparent := r.putpipeline.NewTio(r, r.chunk)
	mcastflow := NewFlow(r, r.chunk.cid, tioparent, ngobj)

	var tioproxy *Tio
	// children tios; each server must work via its own peer-to-peer cmd
	for _, srv := range targets {
		tiochild := r.putpipeline.NewTio(r, tioparent, r.chunk, mcastflow, srv)
		if srv == srvproxy {
			tioproxy = tiochild
		}
	}
	assert(tioproxy != nil)

	mcastflow.tobandwidth = configNetwork.linkbpsControl
	mcastflow.totalbytes = r.chunk.sizeb
	mcastflow.rb = NewDatagramRateBucket(configNetwork.linkbpsControl)

	log("gwy-new-mcastflow-tio", mcastflow.String(), tioparent.String())

	// call ::M7X_request() on the srvproxy
	ev := newMcastChunkPutRequestEvent(r, ngobj, r.chunk, srvproxy, tioparent)
	tioproxy.next(ev, SmethodWait)

	r.rb.use(int64(configNetwork.sizeControlPDU * 8))
	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
	r.prevgroupid = ngid
}

//==========================
// gatewaySevenX TIO handlers
//==========================
func (r *gatewaySevenX) M7X_receivebid(ev EventInterface) error {
	bidsev := ev.(*ProxyBidsEvent)
	assert(len(bidsev.bids) == configStorage.numReplicas)
	tioproxy := bidsev.GetTio()
	tioparent := tioproxy.parent
	log(r.String(), "::M7X_receivebid()", bidsev.String())
	proxy := bidsev.GetSource()
	group := bidsev.GetGroup()
	ngobj, ok := group.(*NgtGroup)
	assert(ok)
	assert(ngobj.hasmember(proxy))
	mcastflow := tioparent.flow

	// fill in the multicast rendezvous group for chunk data transfer
	// note that pending[] bids at these point are already "filtered"
	// to contain only those bids that were selected
	ids := make([]int, configStorage.numReplicas)
	t := TimeNil
	for k := 0; k < configStorage.numReplicas; k++ {
		bid := bidsev.bids[k]
		if t.Equal(TimeNil) {
			t = bid.win.left
		} else {
			assert(t.Equal(bid.win.left))
		}
		assert(ngobj.hasmember(bid.tio.target))
		ids[k] = bid.tio.target.GetID()

		assert(r.eps[ids[k]] == bid.tio.target)
	}
	mcastflow.extension = &t
	sort.Ints(ids)
	for k := 0; k < configStorage.numReplicas; k++ {
		r.rzvgroup.servers[k] = r.eps[ids[k]]
	}

	r.rzvgroup.init(ngobj.getID(), false)
	assert(r.rzvgroup.getCount() == configStorage.numReplicas)
	// cleanup child tios that weren't accepted
	targets := ngobj.getmembers()
	for _, srv := range targets {
		if !r.rzvgroup.hasmember(srv) {
			delete(tioparent.children, srv)
		}
	}
	assert(len(tioparent.children) == r.rzvgroup.getCount())

	mcastflow.tobandwidth = configNetwork.linkbpsData
	mcastflow.rb.setrate(configNetwork.linkbpsData)
	return nil
}

// M7X_replicack is invoked by the generic tio-processing code when
// the latter (tio) undergoes the pipeline stage named "REPLICA-ACK"
// This callback processes replica put ack from the server
//
// The pipeline itself is declared at the top of this model.
func (r *gatewaySevenX) M7X_replicack(ev EventInterface) error {
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
	cstr := fmt.Sprintf("c#%d", flow.sid)

	if r.replicackCommon(tioevent) == ChunkDone {
		log(r.String(), cstr, "=>", r.rzvgroup.String())
		r.bids.cleanup()
	}

	return nil
}

// send data periodically walks all pending IO requests and transmits
// data (for the in-progress chunks), when permitted
func (r *gatewaySevenX) sendata() {
	frsize := configNetwork.sizeFrame
	targets := r.rzvgroup.getmembers()
	for _, tioparent := range r.tios {
		mcastflow := tioparent.flow
		pt := mcastflow.extension.(*time.Time)
		if pt == nil {
			continue
		}
		t := *pt
		if t.After(Now) {
			continue
		}
		if mcastflow.offset >= r.chunk.sizeb {
			continue
		}
		assert(mcastflow.tobandwidth == configNetwork.linkbpsData)

		if mcastflow.offset+frsize > r.chunk.sizeb {
			frsize = r.chunk.sizeb - mcastflow.offset
		}
		if !mcastflow.rb.use(int64(frsize * 8)) {
			continue
		}

		mcastflow.offset += frsize

		// multicast via SmethodDirectInsert
		atomic.StoreInt64(&r.NowMcasting, 1)
		for _, srv := range targets {
			tio := tioparent.children[srv]
			ev := newMcastChunkDataEvent(r, r.rzvgroup, r.chunk, mcastflow, frsize, tio)
			srv.Send(ev, SmethodDirectInsert)
		}
		atomic.StoreInt64(&r.NowMcasting, 0)

		atomic.AddInt64(&r.txbytestats, int64(frsize))
	}
}

//==================================================================
//
// serverSevenX methods
//
//==================================================================
func (r *serverSevenX) rxcallback(ev EventInterface) int {
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

		log(LogVV, "SRV::rxcallback:chunk-data", tioevent.String(), bid.String())
		// once the entire chunk is received:
		// 1) generate ReplicaPutAckEvent inside the common receiveReplicaData()
		// 2) cleanup the corresponding accepted bids without waiting for them
		//    to self-expire
		if r.receiveReplicaData(tioevent) == ReplicaDone {
			r.bids.deleteBid(k)
		}
	case *ProxyBidsEvent:
		bidsev := ev.(*ProxyBidsEvent)
		r.handleProxiedBid(bidsev)
	default:
		tio := ev.GetTio()
		log(LogV, "SRV::rxcallback", ev.String(), r.String())
		r.addBusyDuration(configNetwork.sizeControlPDU, configNetwork.linkbpsControl, NetControlBusy)

		tio.doStage(r.realobject(), ev)
	}

	return ev.GetSize()
}

func (r *serverSevenX) Run() {
	r.state = RstateRunning

	// main loop
	go func() {
		for r.state == RstateRunning {
			r.handleDelayed()
			r.receiveEnqueue()
			r.processPendingEvents(r.rxcallback)
		}

		r.closeTxChannels()
	}()
}

func (r *serverSevenX) handleDelayed() {
	proxy, ok := r.realobject().(*serverSevenProxy)
	if !ok {
		return
	}
	if proxy.delayed.depth() == 0 {
		return
	}

	txbits := int64(4 * configNetwork.sizeControlPDU)
	if proxy.rb.below(txbits) {
		return
	}
	ev := proxy.delayed.popEvent()
	tioevent := ev.(*McastChunkPutRequestEvent)
	tioproxy := tioevent.GetTio()
	log(r.String(), "do-handle-delayed", tioevent.String(), tioproxy.String())
	proxy.M7X_request(ev)
}

func (r *serverSevenX) handleProxiedBid(bidsev *ProxyBidsEvent) {
	tio := bidsev.tio
	bid := bidsev.bids[0]
	log("SRV::rxcallback:new-proxied-bid", bid.String(), tio.String())
	r.bids.insertBid(bid)
	// new server's flow right away..
	assert(r.realobject() == tio.target, r.String()+"!="+tio.target.String()+","+tio.String())
	flow := NewFlow(tio.source, bidsev.cid, r.realobject(), tio)
	flow.totalbytes = bidsev.sizeb
	flow.tobandwidth = configNetwork.linkbpsData
	log("srv-new-flow", flow.String(), bid.String())
	r.flowsfrom.insertFlow(flow)
}

//==============================
//
// serverSevenProxy TIO handlers
//
//==============================
// FIXME: rewrite using separate sorted BidQueue{last-bid}
func (r *serverSevenProxy) M7X_request(ev EventInterface) error {
	//
	// need enough room to transmit (configStorage.numReplicas+1)
	// control PDUs inside this func;
	// otherwise - reschedule via separate Tx queue
	//
	tioevent := ev.(*McastChunkPutRequestEvent)
	txbits := int64((configStorage.numReplicas + 1) * configNetwork.sizeControlPDU)
	tioproxy := tioevent.GetTio()
	if r.rb.below(txbits) {
		r.delayed.insertEvent(tioevent)
		log(r.String(), "::M7X_request()", "delay", tioevent.String(), r.delayed.depth())
		return nil
	}

	log(r.String(), "::M7X_request()", tioevent.String())

	gwy := tioevent.GetSource()
	assert(tioproxy.source == gwy)
	ngobj := tioevent.GetGroup()
	assert(ngobj.hasmember(r))

	// cleanup older bids inside the proxy's local allbids[]
	r.cleanupAllBids()

	// 1. initialize the search
	t0 := time.Time{}
	dummybid := &PutBid{
		crtime: t0,
		win:    TimWin{t0, t0},
		tio:    tioproxy,
		state:  bidStateAccepted,
	}
	bestqueues := make([]int, configStorage.numReplicas)
	bestbids := make([]*PutBid, configStorage.numReplicas)
	for j := 0; j < configStorage.numReplicas; j++ {
		bestqueues[j] = -1
		bestbids[j] = dummybid
	}

	// 2. select best bid queues
	r.selectBestBidQueues(bestqueues, bestbids, dummybid)

	// 3. compute the new "left" for the selected
	newleft := t0
	for j := 0; j < configStorage.numReplicas; j++ {
		b := bestbids[j]
		if b.win.right.After(newleft) {
			newleft = b.win.right
		}
	}
	earliestnotify := Now.Add(configNetwork.durationControlPDU + config.timeClusterTrip)
	// FIXME: more exactly the cond to not add BidGap is:
	//        - the very first bid, OR
	//        - is empty longer than BidGap
	if !newleft.Equal(t0) {
		newleft = newleft.Add(configReplicast.durationBidGap + config.timeClusterTrip)
	}
	if newleft.Before(earliestnotify) {
		newleft = earliestnotify
	}

	// 4. - create bids for those selected
	//    - accept the bids right away
	//    - and send each of these new bids into their corresponding targets
	bidsev := newProxyBidsEvent(r, gwy, ngobj, tioevent.cid, tioproxy, configStorage.numReplicas, tioevent.sizeb)
	for j := 0; j < configStorage.numReplicas; j++ {
		newbid := r.autoAcceptOnBehalf(tioproxy, r.allbids[bestqueues[j]], newleft, ngobj, tioevent.sizeb)
		q := r.allbids[bestqueues[j]]
		log("proxy-bid", j, gwy.String(), "=>", q.r.String(), newbid.String())
		bidsev.bids[j] = newbid
	}

	// 5. execute the pipeline stage vis-à-vis the requesting gateway
	log("proxy-bidsev", bidsev.String())
	bidsev.tiostage = "BID" // force tio to execute this event's stage
	tioproxy.next(bidsev, SmethodWait)

	// 6. force children tios to have the same stage as the proxy's
	tioparent := tioproxy.parent
	_, index := tioproxy.GetStage()
	for _, tiochild := range tioparent.children {
		if tiochild != tioproxy {
			tiochild.index = index
		}
	}

	r.rb.use(txbits)
	atomic.AddInt64(&r.txbytestats, txbits)
	return nil
}

func (r *serverSevenProxy) cleanupAllBids() {
	for i := 0; i < configReplicast.sizeNgtGroup; i++ {
		qi := r.allbids[i]
		l := len(qi.pending)
		for k := l - 1; k >= 0; k-- {
			bd := qi.pending[k]
			diff := Now.Sub(bd.win.right)
			if diff > configNetwork.durationControlPDU {
				qi.deleteBid(k)
			}
		}
	}
}

func (r *serverSevenProxy) selectBestBidQueues(bestqueues []int, bestbids []*PutBid, dummybid *PutBid) {
	for j := 0; j < configStorage.numReplicas; j++ {
		// randomize a bit
		idx := rand.Intn(configReplicast.sizeNgtGroup)
		for cnt := 0; cnt < configReplicast.sizeNgtGroup; cnt++ {
			i := idx + cnt
			if i >= configReplicast.sizeNgtGroup {
				i -= configReplicast.sizeNgtGroup
			}

			// skip [i]?
			alreadyused := false
			for k := 0; k < configStorage.numReplicas; k++ {
				if bestqueues[k] == i {
					alreadyused = true
					break
				}
			}
			if alreadyused {
				continue
			}

			// init [j]
			if bestqueues[j] == -1 {
				bestqueues[j] = i
				bestbids[j] = dummybid
				qi := r.allbids[i]
				li := len(qi.pending)
				if li > 0 {
					bi := qi.pending[li-1]
					bestbids[j] = bi
				}
				continue
			}
			// compare
			qi := r.allbids[i]
			li := len(qi.pending)
			qj := r.allbids[bestqueues[j]]
			lj := len(qj.pending)
			// cannot be better than empty
			if lj == 0 {
				break
			}
			if li == 0 {
				bestqueues[j] = i
				bestbids[j] = dummybid
				break
			}
			bj := qj.pending[lj-1]
			bi := qi.pending[li-1]
			// replace [j]
			if bi.win.left.Before(bj.win.left) {
				bestqueues[j] = i
				bestbids[j] = bi
			}
		}
	}
}

func (srvproxy *serverSevenProxy) autoAcceptOnBehalf(tioproxy *Tio, bestqueue *ServerSparseBidQueue, newleft time.Time, ngobj GroupInterface, chunksizeb int) *PutBid {
	srv := bestqueue.r
	tioparent := tioproxy.parent
	tiochild := tioparent.children[srv]
	newbid := NewPutBid(tiochild, newleft)
	newbid.state = bidStateAccepted
	tiochild.bid = newbid
	bestqueue.insertBid(newbid)

	bidev := newProxyBidsEvent(srvproxy, srv, ngobj, tioproxy.cid, tiochild, 1, chunksizeb, newbid)
	if srv.GetID() == srvproxy.GetID() {
		log("self-handle", bidev.String())
		srvproxy.handleProxiedBid(bidev)
		return newbid
	}

	srvproxy.Send(bidev, SmethodDirectInsert)
	return newbid
}

//==================================================================
//
// modelSevenPrx interface methods
//
//==================================================================
func (m *modelSevenPrx) NewGateway(i int) RunnerInterface {
	gwy := NewGatewayMcast(i, mA.putpipeline)

	// rate bucket: Tx, control bandwidth
	gwy.rb = NewDatagramRateBucket(configNetwork.linkbpsControl)

	rgwy := &gatewaySevenX{GatewayMcast: *gwy}
	rgwy.rptr = rgwy // realobject
	bids := NewGatewayBidQueue(rgwy)
	rgwy.bids = bids
	rgwy.rxcb = rgwy.rxcallbackMcast
	return rgwy
}

// FIXME: add receive side *control* bw ratebucket to prevent
//        Rx bursts beyond linkbpsControl
func (m *modelSevenPrx) NewServer(i int) RunnerInterface {
	srv := NewServerUch(i, mA.putpipeline)
	rsrv := &serverSevenX{ServerUch: *srv}
	bids := NewServerSparseBidQueue(rsrv, 0)
	rsrv.bids = bids

	thisidx := i - 1
	ngid := thisidx/configReplicast.sizeNgtGroup + 1
	firstidx := (ngid - 1) * configReplicast.sizeNgtGroup
	// insert my bid queue right into the proxy's array - hopefully created by the time we are here
	if thisidx != firstidx {
		rsrv.flowsfrom = NewFlowDir(rsrv, config.numGateways)
		prsrv := allServers[firstidx].(*serverSevenProxy)
		prsrv.allbids[thisidx-firstidx] = NewServerSparseBidQueue(rsrv, 0)
		rsrv.rptr = rsrv
		return rsrv
	}
	// the first server in the group is the proxy
	allbids := make([]*ServerSparseBidQueue, configReplicast.sizeNgtGroup)
	prsrv := &serverSevenProxy{serverSevenX: *rsrv, allbids: allbids}
	prsrv.flowsfrom = NewFlowDir(prsrv, config.numGateways)
	prsrv.rptr = prsrv
	allbids[0] = NewServerSparseBidQueue(prsrv, 0)

	// rate bucket: Tx control bandwidth (== Rx control bandwidth)
	// note: in the future models may want to use overcommit, etc.
	//       for now just fixed-constant
	// when rb.below() place the new put requests into the Tx delayed queue
	prsrv.rb = NewDatagramRateBucket(configNetwork.linkbpsControl)
	prsrv.delayed = NewTxQueue(prsrv, 8)

	return prsrv
}

func (m *modelSevenPrx) Configure() {
	configureReplicast(false)
	// minimal bid multipleir: same exact bid win done by proxy:
	// no need to increase the "overlapping" chance
	configReplicast.bidMultiplierPct = 110

	x := (configReplicast.durationBidWindow - configReplicast.minduration) / configNetwork.netdurationFrame
	if x > 4 {
		configReplicast.minduration = configNetwork.netdurationDataChunk + configNetwork.netdurationFrame*(x/3)
	}
}
