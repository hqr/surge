//
// object storage types, Replicast (negotiation/reservation) bids
//
package surge

import (
	"fmt"
	"time"
)

//
// type Chunk
//
type Chunk struct {
	cid     int64
	sid     int64 // short id
	gateway RunnerInterface
	crtime  time.Time // creation time
	sizeb   int       // bytes
}

func NewChunk(gwy RunnerInterface, sizebytes int) *Chunk {
	uqid, printid := uqrandom64(gwy.GetID())
	return &Chunk{cid: uqid, sid: printid, gateway: gwy, crtime: Now, sizeb: sizebytes}
}

func (chunk *Chunk) String() string {
	return fmt.Sprintf("[chunk#%d]", chunk.sid)
}

//
// type PutReplica
//
type PutReplica struct {
	chunk  *Chunk
	crtime time.Time // creation time
	num    int       // 1 .. configStorage.numReplicas
}

func (replica *PutReplica) String() string {
	return fmt.Sprintf("[replica#%d,chunk#%d]", replica.num, replica.chunk.sid)
}

func NewPutReplica(c *Chunk, n int) *PutReplica {
	return &PutReplica{chunk: c, crtime: Now, num: n}
}

//====================================================================================
//
// Reservations -- Bids
//
//====================================================================================
const initialBidQueueSize int = 16

type bidStateEnum int

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

//
// type *Bid
//
type TimWin struct {
	left  time.Time
	right time.Time
}

type PutBid struct {
	crtime time.Time
	win    TimWin
	tio    *Tio
	state  bidStateEnum
}

func NewPutBid(io *Tio, begin time.Time, args ...interface{}) *PutBid {
	// FIXME: fixed-size chunk
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
		s = "TENTATIVE"
	case bidStateCanceled:
		s = "CANCELED"
	case bidStateAccepted:
		s = "ACCEPTED"
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

func (q *BidQueue) insertBid(bid *PutBid) {
	l := len(q.pending)
	if l == cap(q.pending) {
		log(LogV, "growing bidqueue", q.r.String(), cap(q.pending))
	}

	q.pending = append(q.pending, nil)
	t := bid.win.left
	k := 0
	// sort by win.left
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

//
// type ServerBidQueue
//
type ServerBidQueue struct {
	BidQueue
	canceled int
}

func NewServerBidQueue(ri RunnerInterface, size int) *ServerBidQueue {
	q := NewBidQueue(ri, size)
	return &ServerBidQueue{*q, 0}
}

func (q *ServerBidQueue) createBid(tio *Tio, diskdelay time.Duration) *PutBid {
	q.expire()

	earliestnotify := Now.Add(configNetwork.durationControlPDU + config.timeClusterTrip)
	l := len(q.pending)
	if diskdelay == 0 && q.canceled > 0 {
		assert(l > 0)
		for k := l - 1; k >= 0; k-- {
			bid := q.pending[k]
			if bid.state == bidStateAccepted {
				break
			}
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

func (q *ServerBidQueue) cancelBid(replytio *Tio) {
	cid := replytio.cid
	_, bid := q.findBid(bidFindChunk, cid)
	if bid == nil {
		log("WARNING: failed to find (expired) bid", q.r.String(), replytio.String())
		return
	}
	assert(bid.tio == replytio)

	assert(bid.state == bidStateTentative)
	bid.state = bidStateCanceled
	log(bid.String())
	q.canceled++

	q.expire()
}

func (q *ServerBidQueue) acceptBid(replytio *Tio, computedbid *PutBid) {
	cid := replytio.cid
	k, bid := q.findBid(bidFindChunk, cid)
	assert(bid != nil)
	// log(LogBoth, "failed to find bid - expired?", q.r.String(), replytio.String(), state)

	assert(bid.state == bidStateTentative)
	assert(bid.tio == replytio)
	assert(!bid.win.left.After(computedbid.win.left), computedbid.String()+","+bid.String())
	assert(!bid.win.right.Before(computedbid.win.right), computedbid.String()+","+bid.String())

	bid.state = bidStateAccepted
	log("bid-accept-trim", bid.String())

	//
	// extend the next canceled bids if exists
	//
	l := len(q.pending)
	if k < l-1 {
		nextbid := q.pending[k+1]
		assert(nextbid.win.left.After(bid.win.right), bid.String()+","+nextbid.String())
		if nextbid.state == bidStateCanceled {
			d := nextbid.win.left.Sub(computedbid.win.right)
			assert(d > 0, computedbid.String()+","+bid.String()+","+nextbid.String())
			nextbid.win.left = computedbid.win.right.Add(config.timeIncStep)
			log("next-canceled-bid-earlier-by", nextbid.String(), d)
		}
	}
	//
	// trim the accepted bid
	//
	bid.win.left = computedbid.win.left
	bid.win.right = computedbid.win.right

	q.expire()
}

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

//
// type GatewayBidQueue
//
type GatewayBidQueue struct {
	BidQueue
	win TimWin
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

// select the best configStorage.numReplicas bids
func (q *GatewayBidQueue) filterBestBids(chunk *Chunk) *PutBid {
	l := len(q.pending)
	assert(l == configReplicast.sizeNgtGroup)
	var begin, end time.Time
	earliestbegin := Now.Add(configNetwork.durationControlPDU)
	//
	// using the fact that the bids are sorted (ascending)
	// by their left (window) boundary
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
		if configStorage.numReplicas > 2 {
			bid2 := q.pending[k+configStorage.numReplicas-2]
			if end.After(bid2.win.right) {
				end = bid2.win.right
			}
		}
		d := end.Sub(begin)
		if d < configNetwork.netdurationDataChunk+config.timeClusterTrip+(config.timeClusterTrip>>1) {
			continue
		}
		break
	}
	if k == l-configStorage.numReplicas {
		// insiffucient bid overlap
		log("WARNING: chunk abort", q.r.String(), chunk.String(), q.StringBids())
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
