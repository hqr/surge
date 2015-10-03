package surge

import (
	"sync"
	"sync/atomic"
	"time"
)

//
// const
//
const INITIAL_QUEUE_SIZE int = 64

//==================================================================
//
// types: queues
//
//==================================================================
type RxQueue struct {
	pending          []EventInterface
	pendingMutex     *sync.RWMutex
	eventstats       int64
	busycnt          int64 // busy
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
		size = INITIAL_QUEUE_SIZE
	}
	initialQ := make([]EventInterface, size)
	return &RxQueue{
		pending:          initialQ[0:0],
		pendingMutex:     &sync.RWMutex{},
		eventstats:       int64(0),
		busycnt:          int64(0),
		idlecnt:          int64(0),
		realpendingdepth: int64(0),
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
	if l == cap(q.pending) {
		log(LOG_V, "growing queue", q.r.String(), cap(q.pending))
	}
	q.pending = append(q.pending, nil)
	q.pending[l] = ev
}

// caller takes lock
func (q *RxQueue) deleteEvent(k int) {
	copy(q.pending[k:], q.pending[k+1:])
	l := len(q.pending)
	q.pending[l-1] = nil
	q.pending = q.pending[:l-1]

	q.eventstats++
}

//
// handle those that are AT or BEFORE the current time
//
func (q *RxQueue) processPendingEvents(rxcallback processEvent) {
	q.lock()
	defer q.unlock()
	for k := 0; k < len(q.pending); {
		ev := q.pending[k]
		t := ev.GetTriggerTime()
		if t.After(Now) {
			diff := t.Sub(Now)
			if diff > config.timeIncStep {
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
		if rxcallback(ev) {
			q.deleteEvent(k)
		} else {
			k++
		}
		if t.Before(Now) {
			diff := Now.Sub(t)
			if diff > config.timeIncStep && diff > time.Nanosecond*10 {
				eventsPastDeadline++
				log(LOG_BOTH, "WARNING: past trigger time", diff, eventsPastDeadline)
			}
		}
	}
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
		realpendingdepth++
		t := ev.GetTriggerTime()
		if t.Before(Now) || t.Equal(Now) {
			done = false
			// keep counting real depth..
		}
	}
	q.runlock()

	if !q.busyidletick.Equal(Now) {
		// nothing *really* pending means idle, otherwise busy
		if realpendingdepth == 0 {
			atomic.AddInt64(&q.idlecnt, int64(1))
		} else {
			atomic.AddInt64(&q.busycnt, int64(1))
		}
		q.busyidletick = Now
	}

	atomic.StoreInt64(&q.realpendingdepth, realpendingdepth)

	return done
}

//
// generic "event" and "busy" d-tors/stats
//
func (q *RxQueue) GetStats(reset bool) NodeStats {
	var b, i int64
	s := map[string]int64{}
	if reset {
		s["event"] = atomic.SwapInt64(&q.eventstats, int64(0))
		b = atomic.SwapInt64(&q.busycnt, int64(0))
		i = atomic.SwapInt64(&q.idlecnt, int64(0))
	} else {
		s["event"] = atomic.LoadInt64(&q.eventstats)
		b = atomic.LoadInt64(&q.busycnt)
		i = atomic.LoadInt64(&q.idlecnt)
	}
	s["busy"] = int64(0)
	if b > 0 {
		s["busy"] = b * 100 / (b + i)
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
	if l == cap(q.pending) {
		log(LOG_V, "growing queue", q.r.String(), cap(q.pending))
	}

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

func (q *RxQueueSorted) processPendingEvents(rxcallback processEvent) {
	q.lock()
	defer q.unlock()

	for k := 0; k < len(q.pending); {
		ev := q.pending[k]
		t := ev.GetTriggerTime()
		if t.After(Now) {
			diff := t.Sub(Now)
			if diff > config.timeIncStep {
				return // is sorted by trigger time
			}
			// otherwise consider (approx) on time
		}
		ct := ev.GetCreationTime()
		if Now.Sub(ct) < config.timeClusterTrip {
			k++
			continue
		}
		if rxcallback(ev) {
			q.deleteEvent(k)
		} else {
			k++
		}
		if t.Before(Now) {
			diff := Now.Sub(t)
			if diff > config.timeIncStep && diff > time.Nanosecond*10 {
				eventsPastDeadline++
				log(LOG_BOTH, "WARNING: past trigger time", diff, eventsPastDeadline)
			}
		}
	}
}
