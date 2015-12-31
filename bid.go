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
// a bid starts its life as a "tentative" (bidStateTentative) and over its lifecycle
// undergoes the following transitions:
// 	* tentative ==> accepted
// 	* tentative ==> canceled
// 	* tentative ==> canceled ==> accepted
// (the last transition is marked as "un-canceling" in the log)
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

type PutBid struct {
	crtime time.Time    // this bid creation time
	win    TimWin       // time window reserved for requesting gateway
	tio    *Tio         // associated IO request from the gateway
	state  bidStateEnum // bid state
}

func NewPutBid(io *Tio, begin time.Time, args ...interface{}) *PutBid {
	// FIXME TODO: fixed-size chunk
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

func (bid *PutBid) String() string {
	left := bid.win.left.Sub(time.Time{})
	right := bid.win.right.Sub(time.Time{})
	s := ""
	switch bid.state {
	case bidStateTentative:
		s = "tentative"
	case bidStateCanceled:
		s = "canceled"
	case bidStateAccepted:
		s = "accepted"
	}
	tgt := "<nil>"
	if bid.tio.target != nil {
		tgt = bid.tio.target.String()
	}
	return fmt.Sprintf("[%s(chunk#%d):srv=%v,(%11.10v,%11.10v),gwy=%v]", s, bid.tio.chunksid, tgt, left, right, bid.tio.source.String())
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

//=====================================================================
// type ServerBidQueue
//=====================================================================
// ServerBidQueue is a BidQueue of bids issued by a single given storage
// server. The object embeds BidQueue where each bid (PutBid type) is
// in one of the 3 enumerated states, as per bidStateEnum. In addition
// to its state, each bid contains a time window the server reserves
// for the requesting gateway.
//
// In addition to being a sorted BidQueue, ServerBidQueue tracks the count
// of canceled bids resulting from the server _not_ being selected for the
// transaction.
// A canceled bid may be further re-reserved, fully or partially, by another
// storage gateway.
//
type ServerBidQueue struct {
	BidQueue
	canceled int
}

func NewServerBidQueue(ri RunnerInterface, size int) *ServerBidQueue {
	q := NewBidQueue(ri, size)
	return &ServerBidQueue{*q, 0}
}

// createBid() generates a new PutBid that the server, owner of this
// ServerBidQueue, then sends to the requesting storage gateway.
// The latter collects all bids from the group of servers that includes
// this server, first,
// and selects certain criteria-satisfying configStorage.numReplicas
// number of the bids, second.
// The gateway's logic is coded inside filterBestBids() function
// which can be viewed as a client-side counterpart of this createBid()
//
// createBid() itself walks a thin line, so to speak. On one hand, the
// reserved time window must be wide enough to provide for good chances
// of the subsequent overlap between all collected bids, the "overlap"
// that must further accomodate the request in question.
//
// The corresponding logic is in the gateway's filterBestBids() method, where
// filterBestBids() failure causes chunk abort with subsequent rescheduling.
//
// On another hand, bid overprovisioning leads to cumulative idle time and,
// ultimately, underperforming server and the entire cluster.
//
// More comments inside.
//
func (q *ServerBidQueue) createBid(tio *Tio, diskdelay time.Duration) *PutBid {
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
	if diskdelay < configStorage.dskdurationDataChunk && q.canceled > 0 {
		assert(l > 0)
		for k := l - 1; k >= 0; k-- {
			bid := q.pending[k]
			if bid.state != bidStateCanceled {
				continue
			}
			if earliestnotify.After(bid.win.left) {
				break
			}
			q.canceled--
			s := fmt.Sprintf("(%d/%d)", k, l-1)
			log("un-canceling", bid.String(), s, "from", bid.tio.String(), "to", tio.String())
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
		// super-agressive option:
		// newleft = lastbidright.Add(configReplicast.durationBidGap - config.timeClusterTrip)
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

	// if the server is idle. adjust the right boundary to increase its
	// (the server's own) selection chances
	// TODO: fixed static increase of the idle server's bid-window -
	//       must runtime-adjust based on the chunk abort rate
	//
	if l == 0 && diskdelay < configNetwork.netdurationDataChunk {
		bid.win.right = bid.win.right.Add(configNetwork.netdurationDataChunk * 2)
		log("srv-idle-extending-bid-win", bid.String())
	}

	q.insertBid(bid)
	return bid
}

func (q *ServerBidQueue) insertBid(bid *PutBid) {
	l := len(q.pending)
	if l == cap(q.pending) {
		log(LogVV, "growing bidqueue", q.r.String(), cap(q.pending))
	}
	q.pending = append(q.pending, nil)
	q.pending[l] = bid
}

// cancelBid is called in the server's receive path, to handle
// a non-accepted (canceled) bid
func (q *ServerBidQueue) cancelBid(replytio *Tio) {
	q.expire()

	cid := replytio.cid
	k, bid := q.findBid(bidFindChunk, cid)
	if bid == nil {
		log("WARNING: failed to find (expired) bid", q.r.String(), replytio.String())
		return
	}
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
func (q *ServerBidQueue) acceptBid(replytio *Tio, computedbid *PutBid) {
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
	// extend the next canceled bids if exists
	//
	l := len(q.pending)
	if configReplicast.durationBidGap > 0 && k < l-1 {
		nextbid := q.pending[k+1]
		d := nextbid.win.left.Sub(computedbid.win.right)
		if nextbid.state == bidStateCanceled && d > 0 {
			nextbid.win.left = computedbid.win.right.Add(config.timeIncStep)
			log(LogV, "next-canceled-bid-earlier-by", nextbid.String(), d)
		}
	}
	//
	// trim the accepted bid
	//
	bid.win.left = computedbid.win.left
	bid.win.right = computedbid.win.right

	if configReplicast.durationBidGap > 0 && k > 0 {
		prevbid := q.pending[k-1]
		d := bid.win.left.Sub(prevbid.win.right)
		if prevbid.state == bidStateCanceled && d > 0 {
			prevbid.win.right = bid.win.left.Add(-config.timeIncStep)
			log(LogV, "prev-canceled-bid-extend-by", prevbid.String(), d)
		}
	}
}

// expire canceled bids
// Note that "tentative" bids are normally get canceled first while accepted
// bids do get deleted explicitly upon IO completion, hence:
// two logged warnings below
func (q *ServerBidQueue) expire() {
	earliestnotify := Now.Add(configNetwork.durationControlPDU + config.timeClusterTrip)
	earliestEndReceive := Now.Add(configNetwork.netdurationDataChunk)

	keepwalking := true
	for keepwalking {
		keepwalking = false
		if len(q.pending) == 0 {
			return
		}
		for k := 0; k < len(q.pending); k++ {
			bid := q.pending[k]
			switch bid.state {
			case bidStateCanceled:
				if earliestnotify.After(bid.win.left) {
					q.deleteBid(k)
					log("expired-and-removed", q.r.String(), bid.String(), k)
					q.canceled--
					keepwalking = true
					break
				}
			case bidStateTentative:
				if earliestEndReceive.After(bid.win.right) {
					log(LogBoth, "WARNING: timeout waiting for accept/cancel", q.r.String(), bid.String(), k)
				}
			default:
				assert(bid.state == bidStateAccepted)
				if Now.After(bid.win.right) {
					log(LogBoth, "WARNING: accepted bid LINGER", q.r.String(), bid.String(), k)
				}
			}
		}
	}
}

//=====================================================================
// GatewayBidQueue
//=====================================================================
// GatewayBidQueue is a BidQueue of the bids generated by targeted servers
// in response to the gateway's IO (tio) request.
// Gateway accumulates the bids and, once the group-size count is reached,
// "filters" them through the filterBestBids() method of the GatewayBidQueue
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

func (q *GatewayBidQueue) StringBids() string {
	l := len(q.pending)
	s := ""
	for k := 0; k < l; k++ {
		bid := q.pending[k]
		left := bid.win.left.Sub(time.Time{})
		right := bid.win.right.Sub(time.Time{})
		s += fmt.Sprintf("(%11.10v,%11.10v)", left, right)
	}
	return s
}

// filterBestBids selects the best configStorage.numReplicas bids,
// if possible.
//
func (q *GatewayBidQueue) filterBestBids(chunk *Chunk) *PutBid {
	l := len(q.pending)
	assert(l == configReplicast.sizeNgtGroup)
	var begin, end time.Time
	earliestbegin := Now.Add(configNetwork.durationControlPDU)
	// minduration is the minimal size time window required to execute
	// a given put-chunk
	// TODO option: extra half chunk-duration is maybe a bit conservative:
	//              make it configurable?
	var minduration time.Duration = configNetwork.netdurationDataChunk + (configNetwork.netdurationDataChunk >> 1)

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
		// option: if d < minduration
		// Note (TODO):
		// chunk duration + 1.5 trip time is very close to an absolute minimum
		// that may not work for an overloaded host running hundreds of
		// preemptive goroutines
		//
		if d < configNetwork.netdurationDataChunk+config.timeClusterTrip+(config.timeClusterTrip>>1) {
			continue
		}
		// found! adjust the time window to take only what's required and no more
		if d > minduration {
			end = begin.Add(minduration)
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
		log("WARNING: chunk-abort", q.r.String(), chunk.String(), q.StringBids())
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
	log(LogV, "gwy-best-bids", q.r.String(), chunk.String(), q.StringBids())

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

func (q *GatewayBidQueue) cleanup() {
	l := len(q.pending)
	for i := l - 1; i >= 0; i-- {
		q.deleteBid(i)
	}
}
