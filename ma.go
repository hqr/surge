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
	allbids        []*ServerSparseBidQueue
	reservedIOdone []time.Time
	delayed        *TxQueue
}

//
// static & init
//
var mA modelSevenPrx
var dskdur2netdurPct time.Duration

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

	if r.replicackCommon(tioevent) == ChunkDone {
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
		// 2) delete the bid from the local queue
		// 3) notify, to cleanup the proxy's allbids queue
		if r.receiveReplicaData(tioevent) == ReplicaDone {
			r.bids.deleteBid(k)
			r.notifyProxyBidDone(bid)
		}
	case *ProxyBidsEvent:
		bidsev := ev.(*ProxyBidsEvent)
		r.handleProxiedBid(bidsev)
	case *BidDoneEvent:
		proxy, _ := r.realobject().(*serverSevenProxy)
		bdoneEvent := ev.(*BidDoneEvent)
		proxy.handleBidDone(false, bdoneEvent.bid, bdoneEvent.reservedIOdone)
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
			r.handleTxDelayed()
			r.receiveEnqueue()
			r.processPendingEvents(r.rxcallback)
		}

		r.closeTxChannels()
	}()
}

func (r *serverSevenX) notifyProxyBidDone(bid *PutBid) {
	proxy, ok := r.realobject().(*serverSevenProxy)
	if ok {
		// self, in place
		proxy.handleBidDone(true, bid, r.disk.lastIOdone)
		return
	}
	// resolve proxy
	thisidx := r.GetID() - 1
	ngid := thisidx/configReplicast.sizeNgtGroup + 1
	firstidx := (ngid - 1) * configReplicast.sizeNgtGroup
	assert(thisidx != firstidx)
	proxy = allServers[firstidx].(*serverSevenProxy)

	// estimate reservedIOdone (and therefore diskdelay) at the time
	// of the future new bid, that is at the time that equals
	// last-pending-bid.win.right
	reservedIOdone := r.futureReservedIOdone()

	// create bid-done event
	bdoneEvent := newBidDoneEvent(r, proxy, bid, reservedIOdone)
	log(LogV, "notify-bid-done", bdoneEvent.String())

	// send
	// FIXME: encapsulate as srv.Send(bdoneEvent, SmethodWait)
	txch, _ := r.getExtraChannels(proxy)
	txch <- bdoneEvent
	r.updateTxBytes(bdoneEvent)
}

func (r *serverSevenX) futureReservedIOdone() time.Time {
	reservedIOdone := r.disk.lastIOdone
	l := len(r.bids.pending)
	if l == 0 {
		return reservedIOdone
	}

	for k := 0; k < l; k++ {
		bid := r.bids.pending[k]
		thenNow := bid.win.right
		if thenNow.Before(reservedIOdone) {
			reservedIOdone = reservedIOdone.Add(configStorage.dskdurationDataChunk)
		} else {
			reservedIOdone = thenNow.Add(configStorage.dskdurationDataChunk)
		}
	}
	return reservedIOdone
}

func (r *serverSevenX) handleTxDelayed() {
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
	log(LogV, "SRV::rxcallback:new-proxied-bid", bid.String(), tio.String())
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
	//    NOTE: group load-balancing by proxy happens here
	//
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

	// disk delay is computed and applied here
	var diskdelay, latency time.Duration
	for j := 0; j < configStorage.numReplicas; j++ {
		qj := r.allbids[bestqueues[j]]
		dj := r.reservedIOdone[bestqueues[j]]
		sj, delay := r.timeStartWritingNewChunk(qj, dj)
		diskdelay += delay

		// FIXME: debug-only: estimate chunk latency
		fj := sj.Add(configStorage.dskdurationDataChunk)
		lj := fj.Sub(Now)
		lj += 2 * (configNetwork.durationControlPDU + config.timeClusterTrip)
		if lj > latency {
			latency = lj
		}
	}
	diskdelay /= time.Duration(configStorage.numReplicas)
	newleft = newleft.Add(diskdelay)

	// FIXME: debug, remove
	cstr := fmt.Sprintf("c#%d", tioproxy.chunksid)
	log("estimate-late", r.String(), cstr, latency)
	if latency > (configNetwork.netdurationDataChunk+configStorage.dskdurationDataChunk)*3 {
		r.logDebug(bestqueues[0:], newleft, diskdelay, cstr)
	}

	// 4. - create bids for those selected
	//    - accept the bids right away
	//    - and send each of these new bids into their corresponding targets
	bidsev := newProxyBidsEvent(r, gwy, ngobj, tioevent.cid, tioproxy, configStorage.numReplicas, tioevent.sizeb)
	for j := 0; j < configStorage.numReplicas; j++ {
		qj := r.allbids[bestqueues[j]]
		newbid := r.autoAcceptOnBehalf(tioproxy, qj, newleft, ngobj, tioevent.sizeb)
		q := r.allbids[bestqueues[j]]
		log("proxy-bid", j, gwy.String(), "=>", q.r.String(), newbid.String())
		bidsev.bids[j] = newbid
	}

	// 5. execute the pipeline stage vis-à-vis the requesting gateway
	log(LogV, "proxy-bidsev", bidsev.String())
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

func (r *serverSevenProxy) logDebug(bestqueues []int, newleft time.Time, delay time.Duration, cstr string) {
	log("============= ", r.String(), cstr, " ========================================")
	for i := 0; i < configReplicast.sizeNgtGroup; i++ {
		qi := r.allbids[i]
		di := r.reservedIOdone[i]
		bidstr := "<nil>"
		l := len(qi.pending)
		if l > 0 {
			bidstr = qi.pending[l-1].String()
		}
		s, d := r.timeStartWritingNewChunk(qi, di)
		log(qi.r.String(), ": ", bidstr, fmt.Sprintf("startW=%-12.10v", s.Sub(time.Time{})), d)
	}
	sincestartup := newleft.Sub(time.Time{})
	ts := fmt.Sprintf("%-12.10v", sincestartup)
	log("SELECTED:(newleft, delay)", cstr, ts, delay)
	for j := 0; j < configStorage.numReplicas; j++ {
		qj := r.allbids[bestqueues[j]]
		dj := r.reservedIOdone[bestqueues[j]]
		bidstr := "<nil>"
		l := len(qj.pending)
		if l > 0 {
			bidstr = qj.pending[l-1].String()
		}
		s, d := r.timeStartWritingNewChunk(qj, dj)
		log(qj.r.String(), ": ", bidstr, fmt.Sprintf("startW=%-12.10v", s.Sub(time.Time{})), d)
	}
}

func (r *serverSevenProxy) handleBidDone(isproxy bool, bid *PutBid, reservedIOdone time.Time) {
	srv := bid.tio.target
	// must be the [0] if the bid is owned by the proxy itself
	i := 0
	to := 1
	// otherwise, in the range 1-:-max
	if !isproxy {
		i = 1
		to = configReplicast.sizeNgtGroup
	}
	for ; i < to; i++ {
		qi := r.allbids[i]
		if qi.r != srv {
			assert(qi.r.GetID() != srv.GetID())
			continue
		}
		l := len(qi.pending)
		k := 0
		// update reservedIOdone aka lastIOdone
		r.reservedIOdone[i] = reservedIOdone

		// delete this bid from the local proxy's queue
		for ; k < l; k++ {
			bd := qi.pending[k]
			if bd != bid {
				assert(bd.tio != bid.tio)
				diff := Now.Sub(bd.win.right)
				if diff > 2*configNetwork.durationControlPDU {
					log(LogBoth, "WARNING: stale bid, missing BidDone ack", bd.String())
				}
				continue
			}
			qi.deleteBid(k)
			break
		}
		assert(k < l)
		break
	}
	assert(i < configReplicast.sizeNgtGroup)
}

//
// select the best 3 (or numReplicas) servers for the new put-chunk request
//
func (r *serverSevenProxy) selectBestBidQueues(bestqueues []int, bestbids []*PutBid, dummybid *PutBid) {
	for j := 0; j < configStorage.numReplicas; j++ {
		// randomize a bit
		idx := rand.Intn(configReplicast.sizeNgtGroup)
		for cnt := 0; cnt < configReplicast.sizeNgtGroup; cnt++ {
			i := idx + cnt
			if i >= configReplicast.sizeNgtGroup {
				i -= configReplicast.sizeNgtGroup
			}

			// skip [i] if already used
			alreadyused := false
			for k := 0; k < j; k++ {
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
			// estimate time the [i] server will start writing new chunk
			qi := r.allbids[i]
			di := r.reservedIOdone[i]
			si, _ := r.timeStartWritingNewChunk(qi, di)

			// estimate time the [j] server will start writing new chunk
			qj := r.allbids[bestqueues[j]]
			dj := r.reservedIOdone[bestqueues[j]]
			sj, _ := r.timeStartWritingNewChunk(qj, dj)

			// compare and decide
			if si.Before(sj) {
				bestqueues[j] = i
				li := len(qi.pending)
				if li == 0 {
					bestbids[j] = dummybid
				} else {
					bi := qi.pending[li-1]
					bestbids[j] = bi
				}
			}
			// FIXME: if si.Equal(sj) compare respective reservedIOdone
		}
	}
}

//
// given bid queue and estimated time to complete already scheduled chunks (reservedIOdone),
// compute the time when the corresponding server will start writing the new chunk
//
func (r *serverSevenProxy) timeStartWritingNewChunk(q *ServerSparseBidQueue, reservedIOdone time.Time) (time.Time, time.Duration) {
	var ntime, stime time.Time

	// network time, i.e. the time to deliver complete new chunk to the server q.r
	l := len(q.pending)
	earliestnotify := Now.Add(configNetwork.durationControlPDU + config.timeClusterTrip)
	if l == 0 {
		ntime = earliestnotify.Add(configNetwork.netdurationDataChunk + config.timeClusterTrip)
	} else {
		lastbid := q.pending[l-1]
		newleft := lastbid.win.right.Add(configReplicast.durationBidGap + config.timeClusterTrip)
		if newleft.Before(earliestnotify) {
			ntime = earliestnotify.Add(configNetwork.netdurationDataChunk + config.timeClusterTrip)
		} else {
			ntime = newleft.Add(configNetwork.netdurationDataChunk + config.timeClusterTrip)
		}
	}

	// start writing immediately if the new chunk gets delivered *after*
	// the server will have finished already queued writes:
	// stime = ntime + delay
	delay := diskdelay(ntime, reservedIOdone)
	stime = ntime.Add(delay)
	return stime, delay
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

	// FIXME: encapsulate as srvproxy.Send(bidev, SmethodWait)
	txch, _ := srvproxy.getExtraChannels(srv)
	txch <- bidev
	srvproxy.updateTxBytes(bidev)

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
	// first, init only the regular to/from gateways' channels
	srv := NewServerUchExtraChannels(i, mA.putpipeline)

	rsrv := &serverSevenX{ServerUch: *srv}
	bids := NewServerSparseBidQueue(rsrv, 0)
	rsrv.bids = bids

	thisidx := i - 1
	ngid := thisidx/configReplicast.sizeNgtGroup + 1
	firstidx := (ngid - 1) * configReplicast.sizeNgtGroup
	//
	// non-proxy member of the group:
	// - insert my bid queue right into the proxy's array - hopefully created by the time we are here
	// - init a single Tx and a single Rx channel to/from the proxy
	//
	if thisidx != firstidx {
		prsrv := allServers[firstidx].(*serverSevenProxy)
		prsrv.allbids[thisidx-firstidx] = NewServerSparseBidQueue(rsrv, 0)

		rsrv.initExtraChannels(1)
		rsrv.initCases()

		rsrv.rptr = rsrv
		rsrv.flowsfrom = NewFlowDir(rsrv, config.numGateways)
		return rsrv
	}
	//
	// construct and initialize the group's proxy
	//
	allbids := make([]*ServerSparseBidQueue, configReplicast.sizeNgtGroup)
	reservedIOdone := make([]time.Time, configReplicast.sizeNgtGroup)
	prsrv := &serverSevenProxy{serverSevenX: *rsrv, allbids: allbids, reservedIOdone: reservedIOdone}
	prsrv.flowsfrom = NewFlowDir(prsrv, config.numGateways)
	prsrv.rptr = prsrv
	allbids[0] = NewServerSparseBidQueue(prsrv, 0)

	prsrv.initExtraChannels(configReplicast.sizeNgtGroup - 1)
	prsrv.initCases()

	// rate bucket: Tx control bandwidth (== Rx control bandwidth)
	// note: in the future models may want to use overcommit, etc.
	//       for now just fixed-constant
	// when rb.below() place the new put requests into the Tx delayed queue
	prsrv.rb = NewDatagramRateBucket(configNetwork.linkbpsControl)
	prsrv.delayed = NewTxQueue(prsrv, 8)

	return prsrv
}

func (m *modelSevenPrx) Configure() {
	// minimal bid multipleir: same exact bid win done by proxy:
	// no need to increase the "overlapping" chance
	configReplicast.bidMultiplierPct = 110
	configureReplicast(false)

	dskdur2netdurPct = configStorage.dskdurationDataChunk * 100 / configNetwork.netdurationDataChunk

	// setup extra channels: foreach group { proxy <=> other servers in the group }
	for firstidx := 0; firstidx < config.numServers; firstidx += configReplicast.sizeNgtGroup {
		ngid := firstidx/configReplicast.sizeNgtGroup + 1
		ngobj := NewNgtGroup(ngid)
		targets := ngobj.getmembers()
		srvproxy := targets[0]
		for j := 1; j < configReplicast.sizeNgtGroup; j++ {
			sr := targets[j]
			txch := make(chan EventInterface, config.channelBuffer)
			rxch := make(chan EventInterface, config.channelBuffer)
			srvproxy.setExtraChannels(sr, j-1, txch, rxch)
			sr.setExtraChannels(srvproxy, 0, rxch, txch)
		}
	}
}
