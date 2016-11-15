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
	allQbids       []*ServerSparseBidQueue
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

	tioparent := NewTioRr(r, r.putpipeline, r.chunk)
	mcastflow := NewFlow(r, r.chunk.cid, tioparent, ngobj)

	var tioproxy *TioRr
	// children tios; each server must work via its own peer-to-peer cmd
	for _, srv := range targets {
		tiochild := NewTioRr(r, r.putpipeline, tioparent, r.chunk, mcastflow, srv)
		if srv == srvproxy {
			tioproxy = tiochild
		}
	}
	assert(tioproxy != nil)

	mcastflow.setbw(configNetwork.linkbpsControl)
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
	tioproxy := bidsev.GetTio().(*TioRr)
	tioparent := tioproxy.parent
	log(r.String(), "::M7X_receivebid()", bidsev.String())
	proxy := bidsev.GetSource()
	group := bidsev.GetGroup()
	ngobj, ok := group.(*NgtGroup)
	assert(ok)
	assert(ngobj.hasmember(proxy))
	mcastflow := tioparent.GetFlow().(*Flow)

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
		assert(ngobj.hasmember(bid.tio.GetTarget()))
		ids[k] = bid.tio.GetTarget().GetID()

		assert(r.eps[ids[k]] == bid.tio.GetTarget())
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
			tioparent.DelTioChild(srv)
		}
	}
	assert(tioparent.GetNumTioChildren() == r.rzvgroup.getCount())

	mcastflow.setbw(configNetwork.linkbpsData)
	mcastflow.GetRb().setrate(configNetwork.linkbpsData)
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
	tioparent := tio.GetParent()
	assert(tioparent.haschild(tio))
	group := tioevent.GetGroup()
	flow := tio.GetFlow()
	assert(flow.GetCid() == tioevent.cid)
	assert(group == r.rzvgroup)

	if r.replicackCommon(tioevent) == ChunkDone {
		r.bids.cleanup()
	}

	return nil
}

// send data periodically walks all pending IO requests and transmits
// data (for the in-progress chunks), when permitted
func (r *gatewaySevenX) sendata() {
	frsize := int64(configNetwork.sizeFrame)
	targets := r.rzvgroup.getmembers()
	for _, tioparentint := range r.tios {
		tioparent := tioparentint.(*TioRr)
		mcastflow := tioparent.GetFlow().(*Flow)
		pt := mcastflow.extension.(*time.Time)
		if pt == nil {
			continue
		}
		t := *pt
		if t.After(Now) {
			continue
		}
		if mcastflow.getoffset() >= r.chunk.sizeb {
			continue
		}
		assert(mcastflow.getbw() == configNetwork.linkbpsData)

		if mcastflow.getoffset()+frsize > r.chunk.sizeb {
			frsize = r.chunk.sizeb - mcastflow.getoffset()
		}
		if !mcastflow.GetRb().use(frsize * 8) {
			continue
		}

		mcastflow.incoffset(int(frsize))

		// multicast via SmethodDirectInsert
		atomic.StoreInt64(&r.NowMcasting, 1)
		for _, srv := range targets {
			tio := tioparent.GetTioChild(srv).(*TioRr)
			ev := newMcastChunkDataEvent(r, r.rzvgroup, r.chunk, mcastflow, int(frsize), tio)
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
		// once the entire chunk is received:
		//    1) push it into the disk's local queue (receiveReplicaData)
		//    2) generate ReplicaPutAckEvent (receiveReplicaData)
		//    3) delete the bid from the local queue
		//    4) simulate 50% reading if requested via the bid itself
		//    5) finally, notify the proxy, to cleanup its local respective allQbids queue
		if r.receiveReplicaData(tioevent) == ReplicaDone {
			r.bids.deleteBid(k)
			// read
			if bid.win.right.Sub(bid.win.left) > configReplicast.durationBidWindow {
				tio := ev.GetTio().(*TioRr)
				r.disk.lastIOdone = r.disk.lastIOdone.Add(configStorage.dskdurationDataChunk)
				r.addBusyDuration(configStorage.sizeDataChunk*1024, configStorage.diskbps, DiskBusy)
				log("read", tio.String(), fmt.Sprintf("%-12.10v", r.disk.lastIOdone.Sub(time.Time{})))
			}

			r.notifyProxyBidDone(bid)
		}
	case *ProxyBidsEvent:
		bidsev := ev.(*ProxyBidsEvent)
		r.handleProxiedBid(bidsev)
	case *BidDoneEvent:
		proxy, _ := r.realobject().(*serverSevenProxy)
		bdoneEvent := ev.(*BidDoneEvent)
		proxy.handleBidDone(bdoneEvent.bid, bdoneEvent.reservedIOdone)
	default:
		tio := ev.GetTio().(*TioRr)
		log(LogV, "rxcallback", r.String(), ev.String())
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
		q0 := proxy.allQbids[0]

		assert(q0.r.GetID() == proxy.GetID())
		assert(bid == q0.pending[0])
		q0.deleteBid(0)

		proxy.update_reservedIOdone(0, r.disk.lastIOdone)
		return
	}
	// resolve proxy
	thisidx := r.GetID() - 1
	ngid := thisidx/configReplicast.sizeNgtGroup + 1
	firstidx := (ngid - 1) * configReplicast.sizeNgtGroup
	assert(thisidx != firstidx)
	proxy = allServers[firstidx].(*serverSevenProxy)

	// create bid-done event
	bdoneEvent := newBidDoneEvent(r, proxy, bid, r.disk.lastIOdone)
	log(LogV, "notify-bid-done", bdoneEvent.String())

	// send
	// FIXME: encapsulate as srv.Send(bdoneEvent, SmethodWait)
	txch, _ := r.getExtraChannels(proxy)
	txch <- bdoneEvent
	r.updateTxBytes(bdoneEvent)
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
	tioproxy := tioevent.GetTio().(*TioRr)
	log(r.String(), "do-handle-delayed", tioevent.String(), tioproxy.String())
	proxy.M7X_request(ev)
}

func (r *serverSevenX) handleProxiedBid(bidsev *ProxyBidsEvent) {
	tio := bidsev.GetTio()
	bid := bidsev.bids[0]
	log(LogV, "new-proxied-bid", bid.String(), tio.String())
	r.bids.insertBid(bid)
	// new server's flow right away..
	assert(r.realobject() == tio.GetTarget(), r.String()+"!="+tio.GetTarget().String()+","+tio.String())
	flow := NewFlow(tio.GetSource(), bidsev.cid, r.realobject(), tio)
	flow.totalbytes = bidsev.sizeb
	flow.setbw(configNetwork.linkbpsData)
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
	tioproxy := tioevent.GetTio().(*TioRr)
	if r.rb.below(txbits) {
		r.delayed.insertEvent(tioevent)
		log(r.String(), "::M7X_request()", "delay", tioevent.String(), r.delayed.depth())
		return nil
	}

	log(r.String(), "::M7X_request()", tioevent.String())

	cstr := fmt.Sprintf("c#%d", tioproxy.chunksid)
	gwy := tioevent.GetSource()
	assert(tioproxy.GetSource() == gwy)
	ngobj := tioevent.GetGroup()
	assert(ngobj.hasmember(r))

	// 1. initialize the search, allocate 1 extra slot for bestqueue
	bestqueues := make([]int, configStorage.numReplicas)
	for j := 0; j < configStorage.numReplicas; j++ {
		bestqueues[j] = -1
	}
	// TODO; need better multi-objective optimization than
	//       this current idle-win mechanism
	// r.selectBestQueues(bestqueues[0:], cstr)
	r.gatewayBestBidQueues(bestqueues)

	// 3. compute the new "left" for the selected with respect to the maximum diskdelay required
	//    (network-wise, mcast data transfer must have identical reservations from all the selected servesr)
	newleft := time.Time{}
	for j := 0; j < configStorage.numReplicas; j++ {
		qj := r.allQbids[bestqueues[j]]
		dj := r.reservedIOdone[bestqueues[j]]
		newleft_j, _, _ := r.estimateSrvTimes(qj, dj)
		if newleft_j.After(newleft) {
			newleft = newleft_j
		}
	}
	// FIXME: debug, remove
	r.logDebug(tioevent, bestqueues[0:], newleft, cstr)

	// 4. - to read or not to read? round robin between selected servers, if 50% reading requested via CLI
	//    - create bids for those selected
	//    - accept the bids right away
	//    - and send each of these new bids into their corresponding targets
	bidsev := newProxyBidsEvent(r, gwy, ngobj, tioevent.cid, tioproxy, configStorage.numReplicas, tioevent.sizeb)
	read_j := -1
	if configStorage.read {
		read_j = int(tioproxy.cid % int64(configStorage.numReplicas))
	}
	chkLatency := time.Duration(0)
	for j := 0; j < configStorage.numReplicas; j++ {
		bqj := bestqueues[j]
		isReader := (read_j == j)
		newbid, repLatency := r.autoAcceptOnBehalf(tioevent, bqj, newleft, ngobj, isReader)
		log("proxy-bid", newbid.String(), "ack-estimated", fmt.Sprintf("%-12.10v", r.reservedIOdone[bqj].Sub(time.Time{})), repLatency)

		if repLatency > chkLatency {
			chkLatency = repLatency
		}

		bidsev.bids[j] = newbid
	}
	x := int64(chkLatency) / 1000
	log("estimate-late", r.String(), cstr, chkLatency, x)

	// 5. execute the pipeline stage vis-à-vis the requesting gateway
	log(LogV, "proxy-bidsev", bidsev.String())
	bidsev.tiostage = "BID" // force tio to execute this event's stage
	tioproxy.next(bidsev, SmethodWait)

	// 6. force children tios to have the same stage as the proxy's
	tioparent := tioproxy.GetParent().(*TioRr)
	_, index := tioproxy.GetStage()
	for _, tiochildint := range tioparent.children {
		tiochild := tiochildint.(*TioRr)
		if tiochild != tioproxy {
			tiochild.index = index
		}
	}

	r.rb.use(txbits)
	atomic.AddInt64(&r.txbytestats, txbits)
	return nil
}

//
// FIXME: this specific piece of code relies on the numReplicas == 3 - see assert below
//
func (r *serverSevenProxy) selectBestQueues(bestqueues []int, cstr string) {
	bestqueuesExtra := make([]int, configStorage.numReplicas+1)
	for j := 0; j < configStorage.numReplicas+1; j++ {
		bestqueuesExtra[j] = -1
	}

	// PASS #1: select (n + 1) best queues from the gateway's latency perspective
	r.gatewayBestBidQueues(bestqueuesExtra)

	// PASS #2: compare best queues from the servers (min idle-time) perspective
	assert(configStorage.numReplicas == 3)
	lessidlewin := false
	a, b, c := 0, 1, 2
	if r.serverIdle(bestqueuesExtra, a, b, c) > r.serverIdle(bestqueuesExtra, 0, 1, 3) {
		lessidlewin = true
		a, b, c = 0, 1, 3
	}
	if r.serverIdle(bestqueuesExtra, a, b, c) > r.serverIdle(bestqueuesExtra, 0, 2, 3) {
		lessidlewin = true
		a, b, c = 0, 2, 3
	}
	if r.serverIdle(bestqueuesExtra, a, b, c) > r.serverIdle(bestqueuesExtra, 1, 2, 3) {
		lessidlewin = true
		a, b, c = 1, 2, 3
	}
	if lessidlewin {
		log("idle-win", cstr, bestqueuesExtra[0], bestqueuesExtra[1], bestqueuesExtra[2], "===>", bestqueuesExtra[a], bestqueuesExtra[b], bestqueuesExtra[c])
	}
	bestqueues[0] = bestqueuesExtra[a]
	bestqueues[1] = bestqueuesExtra[b]
	bestqueues[2] = bestqueuesExtra[c]
}

func (r *serverSevenProxy) logDebug(tioevent *McastChunkPutRequestEvent, bestqueues []int, newleft time.Time, cstr string) {
	log("============= SELECTED ", r.String(), cstr, fmt.Sprintf("newleft=%-12.10v", newleft.Sub(time.Time{})), " =================")
	for i := 0; i < configReplicast.sizeNgtGroup; i++ {
		qi := r.allQbids[i]
		di := r.reservedIOdone[i]
		bidstr := "<nil>"
		l := len(qi.pending)
		if l > 0 {
			bidstr = qi.pending[l-1].String()
		}
		_, s, d := r.estimateSrvTimes(qi, di)
		selected := "   "
		for k := 0; k < len(bestqueues); k++ {
			if bestqueues[k] == i {
				selected = "=> "
				break
			}
		}
		log(selected, qi.r.String(), cstr, bidstr, l, fmt.Sprintf("startW=%-12.10v", s.Sub(time.Time{})), d)
	}
}

func (r *serverSevenProxy) handleBidDone(bid *PutBid, reservedIOdone_fromServer time.Time) {
	srv := bid.tio.GetTarget()
	i := 1
	for ; i < configReplicast.sizeNgtGroup; i++ {
		qi := r.allQbids[i]
		if qi.r.GetID() != srv.GetID() {
			continue
		}

		// delete this bid from the local proxy's queue
		l := len(qi.pending)
		k := 0
		for ; k < l; k++ {
			bd := qi.pending[k]
			if bd == bid {
				qi.deleteBid(k)
				break
			}
		}
		assert(k < l, bid.String())
		break
	}
	assert(i < configReplicast.sizeNgtGroup, srv.String())
	//
	// recompute the server's disk queue that is tracked by the proxy via reservedIOdone[]
	//
	r.update_reservedIOdone(i, reservedIOdone_fromServer)
}

func (r *serverSevenProxy) update_reservedIOdone(i int, reservedIOdone_fromServer time.Time) {
	reservedIOdone := reservedIOdone_fromServer
	qi := r.allQbids[i]
	l := len(qi.pending)
	for k := 0; k < l; k++ {
		bid := qi.pending[k]
		thenNow := bid.win.right
		if thenNow.Before(reservedIOdone) {
			reservedIOdone = reservedIOdone.Add(configStorage.dskdurationDataChunk)
		} else {
			reservedIOdone = thenNow.Add(configStorage.dskdurationDataChunk)
		}
	}
	r.reservedIOdone[i] = reservedIOdone
}

//
// select the best servers for the new put-chunk request
//
func (r *serverSevenProxy) gatewayBestBidQueues(bestqueues []int) {
	for j := 0; j < len(bestqueues); j++ {
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
				continue
			}
			// estimate time the [i] server will start writing new chunk
			qi := r.allQbids[i]
			di := r.reservedIOdone[i]
			_, si, _ := r.estimateSrvTimes(qi, di)

			// estimate time the [j] server will start writing new chunk
			qj := r.allQbids[bestqueues[j]]
			dj := r.reservedIOdone[bestqueues[j]]
			_, sj, _ := r.estimateSrvTimes(qj, dj)

			// compare and decide
			if si.Before(sj) {
				bestqueues[j] = i
			}
		}
	}
}

func (r *serverSevenProxy) serverIdle(bestqueuesExtra []int, args ...int) time.Duration {
	var idle time.Duration
	newleft := time.Time{}
	for j := 0; j < len(args); j++ {
		bqj := bestqueuesExtra[args[j]]
		qj := r.allQbids[bqj]
		dj := r.reservedIOdone[bqj]
		newleft_j, _, _ := r.estimateSrvTimes(qj, dj)
		if newleft_j.After(newleft) {
			newleft = newleft_j
		}
	}
	for j := 0; j < len(args); j++ {
		bqj := bestqueuesExtra[args[j]]
		qj := r.allQbids[bqj]
		dj := r.reservedIOdone[bqj]
		newleft_j, _, _ := r.estimateSrvTimes(qj, dj)
		idle += newleft.Sub(newleft_j)
	}
	return idle
}

//
// given bid queue and estimated time to complete already scheduled chunks (reservedIOdone),
// compute the time when the corresponding server will start writing the new chunk
//
// Returns: (newleft, start-writing-new-chunk, diskdelay)
//
func (r *serverSevenProxy) estimateSrvTimes(q *ServerSparseBidQueue, reservedIOdone time.Time) (time.Time, time.Time, time.Duration) {
	var newleft, ntime, stime time.Time

	// network time, i.e. the time to deliver complete new chunk to the server q.r
	l := len(q.pending)
	earliestnotify := Now.Add(configNetwork.durationControlPDU + config.timeClusterTrip)
	if l == 0 {
		newleft = earliestnotify
		ntime = earliestnotify.Add(configNetwork.netdurationDataChunk + config.timeClusterTrip)
	} else {
		lastbid := q.pending[l-1]
		newleft = lastbid.win.right.Add(configReplicast.durationBidGap + config.timeClusterTrip)
		if newleft.Before(earliestnotify) {
			ntime = earliestnotify.Add(configNetwork.netdurationDataChunk + config.timeClusterTrip)
		} else {
			ntime = newleft.Add(configNetwork.netdurationDataChunk + config.timeClusterTrip)
		}
	}

	// start writing immediately if the new chunk gets delivered *after*
	// the server will have finished already queued writes,
	// otherwise finish them off first..
	delay := diskdelay(ntime, reservedIOdone)
	stime = ntime
	if stime.Before(reservedIOdone) {
		stime = reservedIOdone
	}
	stime = stime.Add(delay)
	return newleft, stime, delay
}

func (r *serverSevenProxy) autoAcceptOnBehalf(tioevent *McastChunkPutRequestEvent, bqj int, newleft time.Time, ngobj GroupInterface, isReader bool) (*PutBid, time.Duration) {
	qj := r.allQbids[bqj]
	dj := r.reservedIOdone[bqj]
	srv := qj.r
	tioproxy := tioevent.GetTio().(*TioRr)
	tioparent := tioproxy.GetParent().(*TioRr)
	tiochild := tioparent.GetTioChild(srv).(*TioRr)
	chkCreated := tioevent.GetCreationTime()

	newbid := NewPutBid(tiochild, newleft)
	newbid.state = bidStateAccepted

	// simulate reading
	if isReader {
		newbid.win.right = newbid.win.right.Add(config.timeClusterTrip * 2)
	}

	tiochild.bid = newbid
	qj.insertBid(newbid)

	// fix local reservedIOdone for this server's disk queue right away
	thenNow := newbid.win.right
	if thenNow.Before(dj) {
		dj = dj.Add(configStorage.dskdurationDataChunk)
	} else {
		dj = thenNow.Add(configStorage.dskdurationDataChunk)
	}
	r.reservedIOdone[bqj] = dj
	putAckrecv := dj.Add(configNetwork.durationControlPDU + config.timeClusterTrip)
	repLatency := putAckrecv.Sub(chkCreated)

	bidev := newProxyBidsEvent(r, srv, ngobj, tioproxy.cid, tiochild, 1, tioevent.sizeb, newbid)
	if srv.GetID() == r.GetID() {
		log("self-handle", bidev.String())
		r.handleProxiedBid(bidev)
		return newbid, repLatency
	}

	// FIXME: encapsulate as r.Send(bidev, SmethodWait)
	txch, _ := r.getExtraChannels(srv)
	txch <- bidev
	r.updateTxBytes(bidev)

	return newbid, repLatency
}

//==================================================================
//
// modelSevenPrx interface methods
//
//==================================================================
func (m *modelSevenPrx) NewGateway(i int) NodeRunnerInterface {
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
func (m *modelSevenPrx) NewServer(i int) NodeRunnerInterface {
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
		prsrv.allQbids[thisidx-firstidx] = NewServerSparseBidQueue(rsrv, 0)

		rsrv.initExtraChannels(1)
		rsrv.initCases()

		rsrv.rptr = rsrv
		rsrv.flowsfrom = NewFlowDir(rsrv, config.numGateways)
		return rsrv
	}
	//
	// construct and initialize the group's proxy
	//
	allQbids := make([]*ServerSparseBidQueue, configReplicast.sizeNgtGroup)
	reservedIOdone := make([]time.Time, configReplicast.sizeNgtGroup)
	prsrv := &serverSevenProxy{serverSevenX: *rsrv, allQbids: allQbids, reservedIOdone: reservedIOdone}
	prsrv.flowsfrom = NewFlowDir(prsrv, config.numGateways)
	prsrv.rptr = prsrv
	allQbids[0] = NewServerSparseBidQueue(prsrv, 0)

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

func (m *modelSevenPrx) PreConfig() {}

func (m *modelSevenPrx) PostConfig() {
	// minimal bid multipleir: same exact bid win done by proxy:
	// no need to increase the "overlapping" chance
	configReplicast.bidMultiplierPct = 140
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

