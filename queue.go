package surge

import (
	"sync"
	"sync/atomic"
	"time"
)

//
// const
//
const initialQueueSize int = 64

//==================================================================
//
// types: Tx queue
//
//==================================================================
type TxQueue struct {
	fifo []EventInterface
	r    RunnerInterface
}

func NewTxQueue(ri RunnerInterface, size int) *TxQueue {
	if size == 0 {
		size = initialQueueSize
	}
	initialQ := make([]EventInterface, size)
	return &TxQueue{
		fifo: initialQ[0:0],
		r:    ri,
	}
}

func (q *TxQueue) NowIsDone() bool {
	return len(q.fifo) == 0
}

func (q *TxQueue) insertEvent(ev EventInterface) {
	l := len(q.fifo)
	q.fifo = append(q.fifo, nil)
	q.fifo[l] = ev
}

func (q *TxQueue) deleteEvent(k int) {
	l := len(q.fifo)
	if k < l-1 {
		copy(q.fifo[k:], q.fifo[k+1:])
	}
	q.fifo[l-1] = nil
	q.fifo = q.fifo[:l-1]
}

func (q *TxQueue) popEvent() EventInterface {
	if q.depth() == 0 {
		return nil
	}
	ev := q.fifo[0]
	q.deleteEvent(0)
	return ev
}

func (q *TxQueue) depth() int {
	return len(q.fifo)
}

//==================================================================
//
// types: Rx queues
//
//==================================================================
type RxQueue struct {
	pending          []EventInterface
	pendingMutex     *sync.RWMutex
	eventstats       int64
	busycnt          int64 // busy tick counter
	idlecnt          int64
	busyidletick     time.Time
	realpendingdepth int64
	r                RunnerInterface
}

type RxQueueSorted struct {
	RxQueue
}

//==================================================================
//
// c-tors
//
//==================================================================
func NewRxQueue(ri RunnerInterface, size int) *RxQueue {
	if size == 0 {
		size = initialQueueSize
	}
	evsq := make([]EventInterface, size)

	return &RxQueue{
		pending:          evsq[0:0],
		pendingMutex:     &sync.RWMutex{},
		eventstats:       0,
		busycnt:          0,
		idlecnt:          0,
		realpendingdepth: 0,
		busyidletick:     time.Now(), // != Now
		r:                ri,
	}
}

func NewRxQueueSorted(ri RunnerInterface, size int) *RxQueueSorted {
	q := NewRxQueue(ri, size)
	return &RxQueueSorted{*q}
}

//==================================================================
//
// RxQueue methods
//
//==================================================================
func (q *RxQueue) lock() {
	q.pendingMutex.Lock()
}
func (q *RxQueue) rlock() {
	q.pendingMutex.RLock()
}
func (q *RxQueue) unlock() {
	q.pendingMutex.Unlock()
}
func (q *RxQueue) runlock() {
	q.pendingMutex.RUnlock()
}

func (q *RxQueue) NumPendingEvents(exact bool) int64 {
	if !exact {
		return int64(len(q.pending))
	}

	return atomic.LoadInt64(&q.realpendingdepth)
}

// caller takes lock
func (q *RxQueue) insertEvent(ev EventInterface) {
	l := len(q.pending)
	q.pending = append(q.pending, nil)
	q.pending[l] = ev
}

// caller takes lock
func (q *RxQueue) deleteEvent(k int) {
	l := len(q.pending)
	if k < l-1 {
		copy(q.pending[k:], q.pending[k+1:])
	}
	q.pending[l-1] = nil
	q.pending = q.pending[:l-1]

	q.eventstats++
}

//
// handle those that are AT or BEFORE the current time
//
func (q *RxQueue) processPendingEvents(rxcallback processEventCb) int {
	totalsize := 0
	q.lock()
	defer q.unlock()
	for k := 0; k < len(q.pending); {
		ev := q.pending[k]
		t := ev.GetTriggerTime()
		if t.After(Now) {
			diff := t.Sub(Now)
			if diff > config.timeIncStep && diff > time.Nanosecond*10 {
				k++
				continue
			}
			// otherwise consider (approx) on time
		}
		ct := ev.GetCreationTime()
		if Now.Sub(ct) < config.timeClusterTrip {
			k++
			continue
		}
		size := rxcallback(ev)
		if size >= 0 {
			q.deleteEvent(k)
			totalsize += size
		} else {
			k++
		}
		if t.Before(Now) {
			diff := Now.Sub(t)
			if diff > config.timeIncStep && diff > time.Nanosecond*10 {
				eventsPastDeadline++
				if diff >= config.timeClusterTrip && diff >= time.Microsecond {
					log(LogBoth, "WARNING: past trigger time", diff, eventsPastDeadline)
				} else {
					log("WARNING: past trigger time", diff, eventsPastDeadline)
				}
			}
		}
	}
	return totalsize
}

func (q *RxQueue) cleanup() {
	q.lock()
	defer q.unlock()
	for k := 0; k < cap(q.pending); k++ {
		q.pending[k] = nil
	}
	q.pending = q.pending[0:0]
}

func (q *RxQueue) NowIsDone() bool {
	q.rlock()
	done := true
	realpendingdepth := int64(0)
	for k := 0; k < len(q.pending); k++ {
		ev := q.pending[k]

		ct := ev.GetCreationTime()
		// cluster trip time enforced: earlier must be in flight
		if Now.Sub(ct) < config.timeClusterTrip {
			continue
		}
		t := ev.GetTriggerTime()
		if t.Before(Now) || t.Equal(Now) {
			done = false
			realpendingdepth++
			break
		} else {
			diff := Now.Sub(t)
			if diff < config.timeClusterTrip>>1 {
				realpendingdepth++
			}
		}
	}
	q.runlock()

	if !q.busyidletick.Equal(Now) {
		// FIXME: redefine or remove
		if realpendingdepth > 1 {
			atomic.AddInt64(&q.busycnt, 1)
		} else {
			atomic.AddInt64(&q.idlecnt, 1)
		}
		q.busyidletick = Now
	}

	return done
}

//
// generic "event" and "rxchannelbusy" d-tors/stats
//
func (q *RxQueue) GetStats(reset bool) NodeStats {
	var b, i int64
	s := make(map[string]int64, 8)
	if reset {
		s["event"] = atomic.SwapInt64(&q.eventstats, 0)
	} else {
		s["event"] = atomic.LoadInt64(&q.eventstats)
	}
	b = atomic.LoadInt64(&q.busycnt)
	i = atomic.LoadInt64(&q.idlecnt)
	s["rxchannelbusy"] = 0
	if b > 0 {
		s["rxchannelbusy"] = b * 100 / (b + i)
	}

	return s
}

//==================================================================
//
// RxQueueSorted methods
//
//==================================================================
func (q *RxQueueSorted) insertEvent(ev EventInterface) {
	l := len(q.pending)
	q.pending = append(q.pending, nil)
	t := ev.GetTriggerTime()
	k := 0
	for ; k < l; k++ {
		tt := q.pending[k].GetTriggerTime()
		if !t.After(tt) {
			break
		}
	}
	if k == l {
		q.pending[l] = ev
		return
	}
	copy(q.pending[k+1:], q.pending[k:])
	q.pending[k] = ev
}

func (q *RxQueueSorted) processPendingEvents(rxcallback processEventCb) int {
	totalsize := 0
	q.lock()
	defer q.unlock()

	for k := 0; k < len(q.pending); {
		ev := q.pending[k]
		t := ev.GetTriggerTime()
		if t.After(Now) {
			diff := t.Sub(Now)
			if diff > config.timeIncStep {
				return totalsize // is sorted by trigger time
			}
			// otherwise consider (approx) on time
		}
		ct := ev.GetCreationTime()
		if Now.Sub(ct) < config.timeClusterTrip {
			k++
			continue
		}
		size := rxcallback(ev)
		if size >= 0 {
			q.deleteEvent(k)
			totalsize += size
		} else {
			k++
		}
		if t.Before(Now) {
			diff := Now.Sub(t)
			if diff > config.timeIncStep && diff > time.Nanosecond*10 {
				eventsPastDeadline++
				if diff >= config.timeClusterTrip && diff >= time.Microsecond {
					log(LogBoth, "WARNING: past trigger time", diff, ev.String(), eventsPastDeadline)
				} else {
					log("WARNING: past trigger time", diff, ev.String(), eventsPastDeadline)
				}
			}
		}
	}
	return totalsize
}
