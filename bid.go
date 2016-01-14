// Package surge provides a framework for discrete event simulation, as well as
// a number of models for Unsolicited and Reservation Group based Edge-driven
// load balancing.
//
//====================================================================================
// Theory of Operation: Reservations and Bids
//
// 1) storage gateway makes a *reservation* of the server's capacity and
//    link-bandwidth resources
// 2) multiple storage servers receive the reservation request
//    2.a) all storage servers are grouped into pre-configured multicast groups
// 3) each of the recipient servers in a multicast group responds with
//    a *bid* - the PutBid type
//
//====================================================================================
package surge

import (
	"fmt"
	"time"
)

const initialBidQueueSize int = 16

type bidStateEnum int

// bidStateEnum enumerates the 3 possible states of any outstanding server's bid
//
// a bid starts as a "tentative" (bidStateTentative) and over its lifecycle
// undergoes the following transitions:
// 	* tentative ==> accepted
// 	* tentative ==> canceled
// 	* canceled ==> accepted (a.k.a. "un-canceling" in the log)
//
// Further, an accepted chunk gets removed by the server when the latter
// completes the corresponding requests and sends an ACK to the requesting
// gateway. A canceled bid self-expires when the system time moves beyond
// its time window, or more exactly, when the remaining time reserved in the
// bid itself is less than the minimum required..
//
const (
	bidStateTentative bidStateEnum = iota
	bidStateAccepted
	bidStateCanceled
)

type bidFindEnum int

const (
	bidFindServer bidFindEnum = iota
	bidFindGateway
	bidFindChunk
	bidFindState
)

//=====================================================================
// type PutBid
//=====================================================================
// PutBid represents server's bid - the server's response specifying
// the time window the server reserved for the corresponding request
// Requesting gateway will select the best bids out of all bids from
// servers in the multicast group
//
// Semantics of the TimeWin.left: the earliest time gateway is permitted
// to send the first byte of data, which also means that the server will
// see it at least one trip (config.timeClusterTrip) time later
type TimWin struct {
	left  time.Time
	right time.Time
}

func (win *TimWin) isNil() bool {
	return win.left.Equal(TimeNil) && win.right.Equal(TimeNil)
}

func (win *TimWin) String() string {
	if win.left.Equal(TimeNil) && win.right.Equal(TimeNil) {
		return "(-,-)"
	}
	left := win.left.Sub(time.Time{})
	right := win.right.Sub(time.Time{})
	if !win.left.Equal(TimeNil) && !win.right.Equal(TimeNil) {
		return fmt.Sprintf("(%11.10v,%11.10v)]", left, right)
	}
	if win.left.Equal(TimeNil) {
		return fmt.Sprintf("(-,%11.10v)]", right)
	}
	return fmt.Sprintf("(%11.10v,-)]", left)
}

type PutBid struct {
	crtime time.Time    // this bid creation time
	win    TimWin       // time window reserved for requesting gateway
	tio    *Tio         // associated IO request from the gateway
	state  bidStateEnum // bid state
}

func NewPutBid(io *Tio, begin time.Time, args ...interface{}) *PutBid {
	// TODO: fixed-size chunk
	end := begin.Add(configReplicast.durationBidWindow)
	bid := &PutBid{
		crtime: Now,
		win:    TimWin{begin, end},
		tio:    io,
		state:  bidStateTentative,
	}
	for i := 0; i < len(args); i++ {
		bid.setOneArg(args[i])
	}
	return bid
}

func (bid *PutBid) setOneArg(a interface{}) {
	switch a.(type) {
	case time.Time:
		bid.win.right = a.(time.Time)
	default:
		assert(false, fmt.Sprintf("unexpected type: %#v", a))
	}
}

func (bid *PutBid) stringState() string {
	switch bid.state {
	case bidStateTentative:
		return "tentative"
	case bidStateCanceled:
		return "canceled"
	case bidStateAccepted:
		return "accepted"
	}
	return ""
}

func (bid *PutBid) String() string {
	s := bid.stringState()
	if bid.tio.target != nil {
		tgt := bid.tio.target.String()
		return fmt.Sprintf("[%s(chunk#%d):srv=%v,%s,gwy=%v]", s, bid.tio.chunksid, tgt, bid.win.String(), bid.tio.source.String())
	}
	return fmt.Sprintf("[(chunk#%d):%s,gwy=%v]", bid.tio.chunksid, bid.win.String(), bid.tio.source.String())
}

//
// type BidQueue
//
type BidQueue struct {
	pending []*PutBid
	r       RunnerInterface
}

func NewBidQueue(ri RunnerInterface, size int) *BidQueue {
	if size == 0 {
		size = initialBidQueueSize
	}
	q := make([]*PutBid, size)

	return &BidQueue{
		pending: q[0:0],
		r:       ri,
	}
}

// insertBid maintains a sorted order of the bids:
// by the left boundary of the bid's time window
func (q *BidQueue) insertBid(bid *PutBid) {
	l := len(q.pending)
	if l == cap(q.pending) {
		log(LogV, "growing bidqueue", q.r.String(), cap(q.pending))
	}

	q.pending = append(q.pending, nil)
	t := bid.win.left
	k := 0
	// sort by the left boundary of the bid's time window
	for ; k < l; k++ {
		tt := q.pending[k].win.left
		if !t.After(tt) {
			break
		}
	}
	if k == l {
		q.pending[l] = bid
		return
	}
	copy(q.pending[k+1:], q.pending[k:])
	q.pending[k] = bid
}

func (q *BidQueue) deleteBid(k int) {
	l := len(q.pending)
	if k < l-1 {
		copy(q.pending[k:], q.pending[k+1:])
	}
	q.pending[l-1] = nil
	q.pending = q.pending[:l-1]
}

func (q *BidQueue) findBid(by bidFindEnum, val interface{}) (int, *PutBid) {
	l := len(q.pending)
	for k := 0; k < l; k++ {
		var cmpval interface{}
		switch by {
		case bidFindServer:
			cmpval = q.pending[k].tio.target
		case bidFindGateway:
			cmpval = q.pending[k].tio.source
		case bidFindChunk:
			cmpval = q.pending[k].tio.cid
		case bidFindState:
			cmpval = q.pending[k].state
		}
		if val == cmpval {
			return k, q.pending[k]
		}
	}
	return -1, nil
}

func (q *BidQueue) StringBids(includestate bool) string {
	l := len(q.pending)
	s := ""
	for k := 0; k < l; k++ {
		bid := q.pending[k]
		left := bid.win.left.Sub(time.Time{})
		right := bid.win.right.Sub(time.Time{})
		if includestate {
			s += fmt.Sprintf("%s(%11.10v,%11.10v)", bid.stringState(), left, right)
		} else {
			s += fmt.Sprintf("(%11.10v,%11.10v)", left, right)
		}
	}
	return s
}

//=====================================================================
// type ServerRegBidQueue
//=====================================================================
// ServerRegBidQueue is a BidQueue of bids issued by a single given storage
// server at the earliest possible time, given the current server's
// conditions, including:
// - already existing bids
// - disk queue, or rather its size versus the configured maximum.
//
// The object embeds BidQueue where each bid (PutBid type) is
// in one of the 3 enumerated states, as per bidStateEnum. In addition
// to its state, each bid contains a time window the server reserves
// for the requesting gateway.
//
// There's a regular and fixed distance between any two adjacent bids
// in this queue, which makes it Reg(ular)BidQueue.
// (note: compare with ServerSparseBidQueue type)
//
// In addition to being a sorted BidQueue, ServerRegBidQueue tracks the count
// of canceled bids resulting from the server _not_ being selected for the
// transaction.
//
// A canceled bid may be further re-reserved, fully or partially, by another
// storage gateway.
//
type ServerRegBidQueue struct {
	BidQueue
	canceled int
}

func NewServerRegBidQueue(ri RunnerInterface, size int) *ServerRegBidQueue {
	q := NewBidQueue(ri, size)
	return &ServerRegBidQueue{*q, 0}
}

// createBid() generates a new PutBid that the server, owner of this
// ServerRegBidQueue, then sends to the requesting storage gateway.
// The latter collects all bids from the group of servers that includes
// this server, first,
// and selects certain criteria-satisfying configStorage.numReplicas
// number of the bids, second.
// The gateway's logic is coded inside findBestIntersection() function
// which can be viewed as a client-side counterpart of this createBid()
//
// createBid() itself walks a thin line, so to speak. On one hand, the
// reserved time window must be wide enough to provide for good chances
// of the subsequent overlap between all collected bids, the "overlap"
// that must further accomodate the request in question.
//
// The corresponding logic is in the gateway's findBestIntersection() method, where
// findBestIntersection() failure causes chunk abort with subsequent rescheduling.
//
// On another hand, bid overprovisioning leads to cumulative idle time and,
// ultimately, underperforming server and the entire cluster.
//
// More comments inside.
//
func (q *ServerRegBidQueue) createBid(tio *Tio, diskdelay time.Duration) *PutBid {
	q.expire()

	// the earliest time the new bid created here can possibly reach
	// the requesting gateway
	earliestnotify := Now.Add(configNetwork.durationControlPDU + config.timeClusterTrip)
	l := len(q.pending)
	// First, make an attempt to reuse an existing non-accepted ("canceled") bid
	// Non-zero diskdelay indicates configReplicast.maxDiskQueueChunks
	// pending in the disk queue, waiting to get written,
	// in which case we skip down to create a new bid to the right of the
	// existing rightmost..
	if q.canceled > 0 && diskdelay == 0 && l <= 3 {
		for k := l - 1; k >= 0; k-- {
			bid := q.pending[k]
			if bid.state != bidStateCanceled {
				continue
			}
			if earliestnotify.After(bid.win.left) {
				if bid.win.right.Sub(earliestnotify) < configReplicast.durationBidWindow {
					break
				}
			}
			q.canceled--
			if l > 1 {
				s := fmt.Sprintf("(%d/%d)", k, l-1)
				log("un-canceling", s, diskdelay, q.StringBids(true))
			}
			bid.state = bidStateTentative
			bid.tio = tio
			return bid
		}
	}
	// compute left boundary of the future time window to be reserved for the
	// requesting gateway
	// Note: newleft here is the time gateway is permitted to send the first byte
	// which also means that the server will see it at least one trip
	// (config.timeClusterTrip) time later
	var newleft time.Time
	if l > 0 {
		lastbidright := q.pending[l-1].win.right
		assert(!lastbidright.Before(Now))
		newleft = lastbidright.Add(configReplicast.durationBidGap + config.timeClusterTrip)
		if newleft.Before(earliestnotify) {
			newleft = earliestnotify
		}
	} else {
		newleft = earliestnotify
	}

	if diskdelay > 0 {
		earliestdiskdelay := Now.Add(diskdelay)
		if earliestdiskdelay.After(newleft) {
			newleft = earliestdiskdelay
		}
	}

	bid := NewPutBid(tio, newleft)

	// if the server is idle. adjust the right boundary to increase this
	// server's selection chances
	if l == 0 && diskdelay < configNetwork.netdurationDataChunk {
		bid.win.right = bid.win.right.Add(configNetwork.netdurationDataChunk * 2)
		log("srv-idle-extend-bid", bid.String())
	}

	q.insertBid(bid)
	return bid
}

func (q *ServerRegBidQueue) insertBid(bid *PutBid) {
	l := len(q.pending)
	if l == cap(q.pending) {
		log(LogVV, "growing bidqueue", q.r.String(), cap(q.pending))
	}
	q.pending = append(q.pending, nil)
	q.pending[l] = bid
}

// cancelBid is called in the server's receive path, to handle
// a non-accepted (canceled) bid
func (q *ServerRegBidQueue) cancelBid(replytio *Tio) {
	q.expire()

	cid := replytio.cid
	k, bid := q.findBid(bidFindChunk, cid)

	assert(bid != nil, "failed to find bid,"+q.r.String()+","+replytio.String())
	assert(bid.tio == replytio)
	assert(bid.state == bidStateTentative)

	bid.state = bidStateCanceled
	log(LogV, bid.String())
	q.canceled++

	// try merge with adjacents
	l := len(q.pending)
	if k < l-1 {
		nextbid := q.pending[k+1]
		if nextbid.state == bidStateCanceled {
			q.deleteBid(k + 1)
			q.canceled--
			bid.win.right = nextbid.win.right
			log("bid-canceled-merge-right", bid.String())
		}
	}
	if k > 0 {
		prevbid := q.pending[k-1]
		if prevbid.state == bidStateCanceled {
			q.deleteBid(k - 1)
			q.canceled--
			bid.win.left = prevbid.win.left
			log("bid-canceled-merge-left", bid.String())
		}
	}
}

// acceptBid is called in the server's receive path, to accept the bid as the
// name implies. The function receives the associated IO request (the first arg)
// and the time window that the gateway "computed" after having collected and
// considered all the rest bids from the multicast group (second argument)
// The latter is included in the same PutBid structure, for convenience.
//
// Related integrity constraint: computedbid must fully fit into the time window
// of the server's corresponding pending reservation. More assertions below.
//
// In addition to validations and bid-state transitions, this method "trims"
// the newly accepted bid while simultaneously trying to extend the time
// window of its next canceled reservation, if exists.
//
func (q *ServerRegBidQueue) acceptBid(replytio *Tio, computedbid *PutBid) {
	q.expire()

	cid := replytio.cid
	k, bid := q.findBid(bidFindChunk, cid)
	assert(bid != nil)

	assert(bid.state == bidStateTentative)
	assert(bid.tio == replytio)
	assert(!bid.win.left.After(computedbid.win.left), computedbid.String()+","+bid.String())
	assert(!bid.win.right.Before(computedbid.win.right), computedbid.String()+","+bid.String())

	bid.state = bidStateAccepted
	log("bid-accept-trim", bid.String(), computedbid.String())

	//
	// trim the accepted bid
	//
	bid.win.left = computedbid.win.left
	bid.win.right = computedbid.win.right

	//
	// adjust adjacent canceled bids if exist
	//
	l := len(q.pending)
	if k < l-1 {
		nextbid := q.pending[k+1]
		d := nextbid.win.left.Sub(bid.win.right)
		if nextbid.state == bidStateCanceled && d > configReplicast.durationBidGap+configNetwork.durationControlPDU {
			nextbid.win.left = bid.win.right.Add(configReplicast.durationBidGap)
			log("trim:bid-canceled-extend-left", nextbid.String(), d-configReplicast.durationBidGap)
		}
	}
	if k > 0 {
		prevbid := q.pending[k-1]
		d := bid.win.left.Sub(prevbid.win.right)
		if prevbid.state == bidStateCanceled && d > configReplicast.durationBidGap+configNetwork.durationControlPDU {
			prevbid.win.right = bid.win.left.Add(-configReplicast.durationBidGap)
			log("trim:bid-canceled-extend-right", prevbid.String(), d-configReplicast.durationBidGap)
		}
	}
}

// expire canceled bids
// Note that "tentative" bids are normally get canceled first while accepted
// bids do get deleted explicitly upon IO completion, hence:
// two logged warnings below
func (q *ServerRegBidQueue) expire() {
	// first, forget canceled bids on top of the queue
	l := len(q.pending)
	for k := l - 1; k >= 0; k-- {
		bid := q.pending[k]
		if bid.state != bidStateCanceled {
			break
		}
		q.deleteBid(k)
		q.canceled--
	}

	earliestnotify := Now.Add(configNetwork.durationControlPDU + config.timeClusterTrip)
	earliestEndReceive := Now.Add(configNetwork.netdurationDataChunk)

	// expired older canceled bids if any
	keepwalking := true
	for keepwalking {
		keepwalking = false
		for k := 0; k < len(q.pending); k++ {
			bid := q.pending[k]
			if bid.state != bidStateCanceled {
				continue
			}
			if earliestnotify.After(bid.win.left) {
				if bid.win.right.Sub(earliestnotify) < configReplicast.durationBidWindow {
					q.deleteBid(k)
					log("expired-and-removed", q.r.String(), bid.String(), k)
					q.canceled--
					keepwalking = true
					break
				}
			}
		}
	}

	// validation only
	for k := 0; k < len(q.pending); k++ {
		bid := q.pending[k]
		switch bid.state {
		case bidStateTentative:
			if earliestEndReceive.After(bid.win.right) {
				log(LogBoth, "WARNING: timeout waiting for accept/cancel", q.r.String(), bid.String(), k)
			}
		case bidStateAccepted:
			if Now.After(bid.win.right) {
				log(LogBoth, "WARNING: accepted bid linger past its deadline", q.r.String(), bid.String(), k)
			}
		}
	}
}

//=====================================================================
// type ServerSparseBidQueue
//=====================================================================
// ServerSparseBidQueue is a BidQueue of bids issued by a single given storage
// server at or after the requested time, given the current server's
// conditions, including:
// - already existing bids
// - disk queue, or rather its size versus the configured maximum.
//
// The object embeds BidQueue where each bid (PutBid type) is
// in one of the 3 enumerated states, as per bidStateEnum. In addition
// to its state, each bid contains a time window the server reserves
// for the requesting gateway.
//
// Unlike the "regular" bid queue type, ServerSparseBidQueue does not
// keep canceled bids around and does not rely on regular inter-bid gap
// (note: compare with ServerRegBidQueue type)
//
type ServerSparseBidQueue struct {
	BidQueue
}

func NewServerSparseBidQueue(ri RunnerInterface, size int) *ServerSparseBidQueue {
	q := NewBidQueue(ri, size)
	return &ServerSparseBidQueue{*q}
}

func (q *ServerSparseBidQueue) createBid(tio *Tio, diskdelay time.Duration, rwin *TimWin) *PutBid {
	// the earliest time can possibly notify the requesting gateway
	earliestnotify := Now.Add(configNetwork.durationControlPDU + config.timeClusterTrip)
	earliestdiskdelay := Now.Add(diskdelay)

	newleft := rwin.left
	if newleft.Equal(TimeNil) {
		newleft = Now
	}
	if newleft.Before(earliestnotify) {
		newleft = earliestnotify
	}
	if newleft.Before(earliestdiskdelay) {
		newleft = earliestdiskdelay
	}
	for k := 0; k < len(q.pending); k++ {
		bid := q.pending[k]
		if newleft.Before(bid.win.left) {
			if bid.win.left.Sub(newleft) >= configReplicast.durationBidWindow+configReplicast.durationBidGap {
				break
			}
		}
		earliestnextleft := bid.win.right.Add(configReplicast.durationBidGap + config.timeClusterTrip)
		if newleft.Before(earliestnextleft) {
			newleft = earliestnextleft
		}
	}
	bid := NewPutBid(tio, newleft)
	q.insertBid(bid)
	return bid
}

func (q *ServerSparseBidQueue) cancelBid(replytio *Tio) {
	cid := replytio.cid
	k, bid := q.findBid(bidFindChunk, cid)

	assert(bid != nil, "failed to find bid,"+q.r.String()+","+replytio.String())
	assert(bid.tio == replytio)
	assert(bid.state == bidStateTentative)

	q.deleteBid(k)
	log(LogV, bid.String())
}

func (q *ServerSparseBidQueue) acceptBid(replytio *Tio, mybid *PutBid) {
	cid := replytio.cid
	_, bid := q.findBid(bidFindChunk, cid)
	assert(bid != nil, "WARNING: failed to find bid,"+q.r.String()+","+replytio.String())

	assert(bid.state == bidStateTentative)
	assert(bid.tio == replytio)
	assert(!bid.win.left.After(mybid.win.left), mybid.String()+","+bid.String())
	assert(!bid.win.right.Before(mybid.win.right), mybid.String()+","+bid.String())

	bid.state = bidStateAccepted
	log("bid-accept", bid.String(), mybid.String())
	bid.win.left = mybid.win.left
	bid.win.right = mybid.win.right
}

//=====================================================================
// GatewayBidQueue
//=====================================================================
// GatewayBidQueue is a BidQueue of the bids generated by targeted servers
// in response to the gateway's IO (tio) request.
// Gateway accumulates the bids and, once the group-size count is reached,
// "filters" them through the findBestIntersection() method of the GatewayBidQueue
//
type GatewayBidQueue struct {
	BidQueue
}

func NewGatewayBidQueue(ri RunnerInterface) *GatewayBidQueue {
	size := configReplicast.sizeNgtGroup
	q := NewBidQueue(ri, size)
	return &GatewayBidQueue{BidQueue: *q}
}

func (q *GatewayBidQueue) receiveBid(tio *Tio, bid *PutBid) int {
	assert(q.r == tio.source)
	assert(len(q.pending) < configReplicast.sizeNgtGroup)

	q.insertBid(bid)
	return len(q.pending)
}

// findBestIntersection selects the best configStorage.numReplicas bids,
// if possible.
//
func (q *GatewayBidQueue) findBestIntersection(chunk *Chunk) *PutBid {
	l := len(q.pending)
	assert(l == configReplicast.sizeNgtGroup)
	var begin, end time.Time
	earliestbegin := Now.Add(configNetwork.durationControlPDU)

	//
	// BidQueue is sorted, the code uses its sorted-ness by
	// the time windows left boundaries: earliest first
	k := 0
	for ; k < l-configStorage.numReplicas; k++ {
		bid1 := q.pending[k]
		bid3 := q.pending[k+configStorage.numReplicas-1]
		assert(!bid1.win.left.After(bid3.win.left), bid1.String()+","+bid3.String())

		begin = bid3.win.left
		if earliestbegin.After(begin) {
			begin = earliestbegin
		}
		end = bid1.win.right
		if end.After(bid3.win.right) {
			end = bid3.win.right
		}
		// this is not looking very good if the num replicas > 3
		// for no is okay though
		if configStorage.numReplicas > 2 {
			bid2 := q.pending[k+configStorage.numReplicas-2]
			if end.After(bid2.win.right) {
				end = bid2.win.right
			}
		}
		d := end.Sub(begin)
		if d < configReplicast.minduration {
			continue
		}
		// found! adjust the time window to take only what's required and no more
		if d > configReplicast.minduration {
			end = begin.Add(configReplicast.minduration)
		}
		break
	}
	// Failure: received bids are too far apart to produce a common
	// usable reservation window. The corresponding put-request will
	// now be aborted, time and resources effectively wasted,
	// start again.,
	//
	if k == l-configStorage.numReplicas {
		// insiffucient bid overlap
		log("WARNING: chunk-abort", q.r.String(), chunk.String(), q.StringBids(false))
		return nil
	}

	// cleanup everything except the (future) rzvgroup-accepted:
	// delete the rest bids from the local gateway's queue
	for i := l - 1; i >= 0; i-- {
		if i > k+configStorage.numReplicas-1 || i < k {
			q.deleteBid(i)
		}
	}
	// logical (computed) bid, with the left and right boundaries
	// the gateway will actually use
	l = len(q.pending)
	assert(l == configStorage.numReplicas)
	log(LogV, "gwy-best-bids", q.r.String(), chunk.String(), q.StringBids(false))

	tioparent := q.pending[0].tio.parent
	assert(tioparent.cid == q.pending[0].tio.cid)
	computedbid := &PutBid{
		crtime: Now,
		win:    TimWin{begin, end},
		tio:    tioparent,
		state:  bidStateAccepted,
	}
	left := begin.Sub(time.Time{})
	right := end.Sub(time.Time{})
	s := fmt.Sprintf("[computed-bid (chunk#%d):(%11.10v,%11.10v),gwy=%v]", tioparent.chunksid, left, right, q.r.String())
	log(s)
	return computedbid
}

// FIXME: initial version
//        a) maxgap must be separately configured
//        b) bid0 with no alternatives
func (q *GatewayBidQueue) filterBestSequence(chunk *Chunk, maxnum int) {
	//
	// take the bid #0 and trim it right away
	//
	bid0 := q.pending[0]
	end0 := bid0.win.left.Add(configReplicast.minduration)
	assert(!bid0.win.right.Before(end0))
	if bid0.win.right.After(end0) {
		bid0.win.right = end0
	}

	maxgap := configReplicast.minduration / 3
	if maxgap < configNetwork.netdurationFrame {
		maxgap = configNetwork.netdurationFrame
	}

	// try to make sequence..
	var bid1, bid2 *PutBid
	if maxnum > 1 {
		bid1 = q.findNextAdjacent(end0, maxgap, configReplicast.minduration)
	}
	if maxnum > 2 && bid1 != nil {
		assert(bid1.win.right.Sub(bid1.win.left) >= configReplicast.minduration)
		end1 := bid1.win.left.Add(configReplicast.minduration)
		assert(!bid1.win.right.Before(end1))
		bid2 = q.findNextAdjacent(end1, maxgap, configReplicast.minduration)
	}
	if bid2 != nil {
		assert(bid2.win.right.Sub(bid2.win.left) >= configReplicast.minduration)
		end2 := bid2.win.left.Add(configReplicast.minduration)
		assert(!bid2.win.right.Before(end2))
	}

	// prepre filtered result queue
	q.cleanup()
	q.insertBid(bid0)
	if bid1 != nil {
		q.insertBid(bid1)
		if bid2 != nil {
			q.insertBid(bid2)
		}
		log("gwy-bid-sequence", q.r.String(), q.StringBids(false))
	} else {
		log(LogV, "gwy-bid-single", q.r.String(), bid0.String())
	}
}

func (q *GatewayBidQueue) findNextAdjacent(endprev time.Time, maxgap time.Duration, minduration time.Duration) *PutBid {
	l := len(q.pending)
	for k := 1; k < l; k++ {
		bid := q.pending[k]
		// too distant in the future?
		if bid.win.left.Sub(endprev) > maxgap {
			break
		}
		// too close for the next chunk?
		start := bid.win.right.Add(-minduration)
		if start.Before(endprev) {
			continue
		}
		//
		// adjust, trim and yank from the queue
		//
		if bid.win.left.Before(endprev) {
			assert(bid.win.right.After(endprev))
			bid.win.left = endprev
		}
		end := bid.win.left.Add(minduration)
		assert(!bid.win.right.Before(end))
		if bid.win.right.After(end) {
			bid.win.right = end
		}
		q.deleteBid(k)
		return bid
	}
	return nil
}

func (q *GatewayBidQueue) cleanup() {
	l := len(q.pending)
	for i := l - 1; i >= 0; i-- {
		q.deleteBid(i)
	}
}
