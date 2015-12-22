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
	// wide enough for two full chunks
	d := configNetwork.netdurationDataChunk + config.timeClusterTrip
	d *= configReplicast.bidMultiplier
	end := begin.Add(d)
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

func (q *ServerBidQueue) createBid(tio *Tio, diskIOlast time.Time) *PutBid {
	q.expire()

	var numDiskQueueChunks int
	if diskIOlast.After(Now) {
		diff := diskIOlast.Sub(Now)
		numDiskQueueChunks := int(diff / configStorage.dskdurationDataChunk)
		if numDiskQueueChunks > configStorage.maxDiskQueueChunks {
			log("WARNING: exceded-disk-queue", q.r.String(), configStorage.maxDiskQueueChunks)
			return NewPutBid(tio, Now.Add(time.Hour))
		}
	}

	earliestnotify := Now.Add(configNetwork.durationControlPDU + config.timeClusterTrip*2)
	l := len(q.pending)
	if q.canceled > 0 {
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
			s := fmt.Sprintf("(%d/%d/%d)", k, l-1, numDiskQueueChunks)
			log("un-canceling", bid.String(), s, "from", bid.tio.String(), "to", tio.String())
			bid.state = bidStateTentative
			bid.tio = tio
			return bid
		}
	}
	var newleft time.Time
	if l > 0 {
		lastbidright := q.pending[l-1].win.right
		assert(lastbidright.After(Now))

		// adjust with respect to disk queue
		newleft = lastbidright.Add(configReplicast.durationBidGap + config.timeClusterTrip*2)
		if newleft.Before(earliestnotify) {
			newleft = earliestnotify
		}
	} else {
		newleft = earliestnotify
	}

	bid := NewPutBid(tio, newleft)
	if l == cap(q.pending) {
		log(LogVV, "growing bidqueue", q.r.String(), cap(q.pending))
	}
	q.pending = append(q.pending, nil)
	q.pending[l] = bid
	return bid
}

func (q *ServerBidQueue) cancelBid(replytio *Tio) {
	cid := replytio.cid
	_, bid := q.findBid(bidFindChunk, cid)
	assert(bid != nil)
	assert(bid.tio == replytio)
	// log(LogBoth, "failed to find bid - expired?", q.r.String(), replytio.String(), state)

	assert(bid.state == bidStateTentative)
	bid.state = bidStateCanceled
	log(bid.String())
	q.canceled++
}

func (q *ServerBidQueue) acceptBid(replytio *Tio, computedbid *PutBid) {
	cid := replytio.cid
	k, bid := q.findBid(bidFindChunk, cid)
	assert(bid != nil)
	// log(LogBoth, "failed to find bid - expired?", q.r.String(), replytio.String(), state)

	assert(bid.state == bidStateTentative)
	assert(bid.tio == replytio)
	assert(!bid.win.left.After(computedbid.win.left))
	// assert(!bid.win.right.Before(computedbid.win.right))

	bid.state = bidStateAccepted
	log("bid-accept-trim", bid.String())

	//
	// adjust adjacent canceled bids if any
	//
	if k > 0 {
		prevbid := q.pending[k-1]
		if prevbid.state == bidStateCanceled {
			d := computedbid.win.left.Sub(prevbid.win.right)
			assert(d > 0)
			prevbid.win.right = computedbid.win.left.Add(-config.timeIncStep)
			log("prev-bid-grow-by", prevbid.String(), d)
		}
	}
	l := len(q.pending)
	if k < l-1 {
		nextbid := q.pending[k+1]
		if nextbid.state == bidStateCanceled {
			d := nextbid.win.left.Sub(computedbid.win.right)
			assert(d > 0)
			nextbid.win.left = computedbid.win.right.Add(-config.timeIncStep)
			log("next-bid-grow-by", nextbid.String(), d)
		}
	}
	//
	// trim the accepted bid
	//
	bid.win.left = computedbid.win.left
	bid.win.right = computedbid.win.right
}

func (q *ServerBidQueue) expire() {
	atnet := configNetwork.durationControlPDU + config.timeClusterTrip + (config.timeClusterTrip >> 1)
	earliestGwyNotify := Now.Add(atnet)

	earliestEndReceive := Now.Add(configNetwork.netdurationDataChunk)

	for {
		if len(q.pending) == 0 {
			return
		}
		bid := q.pending[0]
		if bid.state == bidStateCanceled && earliestGwyNotify.After(bid.win.left) {
			q.deleteBid(0)
		} else if bid.state == bidStateTentative && earliestEndReceive.After(bid.win.right) {
			s := fmt.Sprintf("timeout waiting for accept/cancel,%s", bid.String())
			log(s)
			assert(false, s)
			q.deleteBid(0)
		} else if Now.After(bid.win.right) {
			q.deleteBid(0)
		} else {
			return
		}
		log("expired-and-removed", bid.String())
		if bid.state == bidStateCanceled {
			q.canceled--
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
	found := false
	k := 0
	var begin, end time.Time
	//
	// using the fact that the bids are sorted (ascending)
	// by their left (window) boundary
	for ; k < l-configStorage.numReplicas; k++ {
		begin = q.pending[k+configStorage.numReplicas-1].win.left
		if Now.After(begin) {
			begin = Now
		}
		end = q.pending[k].win.right
		assert(!begin.Before(q.pending[k].win.left))
		// assert(!end.After(q.pending[k+configStorage.numReplicas-1].win.right))
		if begin.After(end) {
			continue
		}
		d := end.Sub(begin)
		if d < configNetwork.netdurationDataChunk+config.timeClusterTrip {
			continue
		}
		found = true
		break
	}
	if !found {
		log("ERROR: insufficient bid overlap", q.r.String(), chunk.String(), q.StringBids())
		return nil
	}
	// delete the rest bids from the local queue
	for i := l - 1; i >= 0; i-- {
		if i > k+configStorage.numReplicas-1 || i < k {
			q.deleteBid(i)
		}
	}
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
