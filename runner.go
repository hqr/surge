package surge

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
)

type RunnerStateEnum int

// constants
const (
	RstateIniting RunnerStateEnum = iota
	RstateRunning
	RstateRxClosed
	RstateStopped
)

//
// interfaces
//
type RunnerInterface interface {
	setChannels(peer RunnerInterface, txch chan EventInterface, rxch chan EventInterface)
	getChannels(peer RunnerInterface) (chan EventInterface, chan EventInterface)

	Run()
	NowIsDone() bool
	PrepareToStop()

	NumPendingEvents() int

	GetState() RunnerStateEnum
	GetId() int
	GetStats(reset bool) NodeStats

	String() string
}

//==================================================================
//
// base runner with a bunch of private methods that can be used by subclasses
//
//==================================================================
type RunnerBase struct {
	id           int
	state        RunnerStateEnum
	txchans      []chan EventInterface
	rxchans      []chan EventInterface
	eps          []RunnerInterface
	cases        []reflect.SelectCase
	pending      []EventInterface
	pendingMutex *sync.Mutex
	rxcount      int
	reserved     int
	eventstats   int64
	busypct      int64
	strtype      string
}

//
// const
//
const INITIAL_SERVER_QUEUE int = 64

type processEvent func(ev EventInterface) bool

//
// static
//
var eventsPastDeadline = 0

//==================================================================
// RunnerBase interface methods
//==================================================================
func (r *RunnerBase) setChannels(peer RunnerInterface, txch chan EventInterface, rxch chan EventInterface) {
	peerid := peer.GetId()

	assert(0 < peerid && peerid < cap(r.txchans) && peerid < cap(r.rxchans))
	assert(peerid < cap(r.eps))
	assert(r.txchans[peerid] == nil)
	assert(r.rxchans[peerid] == nil)

	r.eps[peerid] = peer
	r.txchans[peerid] = txch
	r.rxchans[peerid] = rxch

	r.cases[peerid-1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(rxch)}
}

func (r *RunnerBase) getChannels(peer RunnerInterface) (chan EventInterface, chan EventInterface) {
	peerid := peer.GetId()
	assert(r.eps[peerid] == peer)
	return r.txchans[peerid], r.rxchans[peerid]
}

// sample Run() for a listening runner
func (r *RunnerBase) Run() {
	assert(false, "abstract method")
}

func (r *RunnerBase) PrepareToStop() {
	assert(r.state <= RstateStopped)
	r.state = RstateStopped
}

func (r *RunnerBase) GetState() RunnerStateEnum { return r.state }
func (r *RunnerBase) GetId() int                { return r.id }

//
// generic stats descriptors: "event" and "busy" stats for all runners
//
func (r *RunnerBase) GetStats(reset bool) NodeStats {
	if !reset {
		return map[string]int64{
			"event": r.eventstats,
			"busy":  r.busypct,
		}
	}
	e := atomic.SwapInt64(&r.eventstats, 0)
	b := atomic.SwapInt64(&r.busypct, 0)
	return map[string]int64{
		"event": e,
		"busy":  b,
	}
}

func (r *RunnerBase) String() string { return fmt.Sprintf("[%s#%v]", r.strtype, r.id) }

//==================================================================
// RunnerBase private methods that can be used by concrete models' runners
//==================================================================
func (r *RunnerBase) init(numPeers int) {
	r.txchans = make([]chan EventInterface, numPeers+1)
	r.rxchans = make([]chan EventInterface, numPeers+1)
	r.eps = make([]RunnerInterface, numPeers+1)

	r.cases = make([]reflect.SelectCase, numPeers+1)
	r.cases[numPeers] = reflect.SelectCase{Dir: reflect.SelectDefault}
	r.rxcount = numPeers

	r.txchans[0] = nil // not used
	r.rxchans[0] = nil
	r.eps[0] = nil

	initialQ := make([]EventInterface, INITIAL_SERVER_QUEUE)
	r.pending = initialQ[0:0]
	r.pendingMutex = &sync.Mutex{}
}

func (r *RunnerBase) selectRandomPeer(maxload int) RunnerInterface {
	numPeers := cap(r.eps) - 1
	if numPeers == 1 {
		return r.eps[1]
	}
	idx := rand.Intn(numPeers)
	cnt := 0
	for {
		peer := r.eps[idx+1]
		if maxload == 0 || peer.NumPendingEvents() <= maxload {
			return peer
		}
		idx++
		cnt++
		if idx >= numPeers {
			idx = 0
		}
		if cnt >= numPeers {
			// is overloaded
			return nil
		}
	}
}

func (r *RunnerBase) receiveAndHandle(rxcallback processEvent) {
	ev, err := r.recvNextEvent()
	if err != nil {
		if r.state != RstateRxClosed {
			log("ERROR", r.String(), err)
		}
	} else if ev != nil {
		log(LOG_VVV, "recv-ed", ev.String())
		r.insertEvent(ev)
		r.processPendingEvents(rxcallback)
	}
}

func (r *RunnerBase) recvNextEvent() (EventInterface, error) {
	for r.rxcount > 0 && r.state == RstateRunning {
		chosen, value, ok := reflect.Select(r.cases)
		if ok {
			event := value.Interface().(EventInterface)
			return event, nil
		}

		// SelectDefault case
		var selectedcase reflect.SelectCase = r.cases[chosen]
		if selectedcase.Dir == reflect.SelectDefault {
			return nil, nil
		}

		// transmitter has closed this one..
		r.cases[chosen].Chan = reflect.ValueOf(nil)
		r.rxchans[chosen] = nil
		r.rxcount--
	}

	r.state = RstateRxClosed
	return nil, errors.New(r.String() + ": all receive channels closed by peers")
}

func (r *RunnerBase) NumPendingEvents() int {
	r.pendingMutex.Lock()
	l := len(r.pending)
	r.pendingMutex.Unlock()
	return l
}

func (r *RunnerBase) insertEvent(ev EventInterface) {
	r.pendingMutex.Lock()
	defer r.pendingMutex.Unlock()

	l := len(r.pending)
	if l == cap(r.pending) {
		log(LOG_V, "growing server queue", cap(r.pending))
	}
	r.pending = append(r.pending, nil)
	r.pending[l] = ev
}

func (r *RunnerBase) deleteEvent(k int) {
	copy(r.pending[k:], r.pending[k+1:])
	l := len(r.pending)
	r.pending[l-1] = nil
	r.pending = r.pending[:l-1]

	r.eventstats++
}

//
// handle only those that are AT or BEFORE the current time = Now
//
func (r *RunnerBase) processPendingEvents(rxcallback processEvent) bool {
	r.pendingMutex.Lock()
	defer r.pendingMutex.Unlock()

	var haveMore bool = false
	for k := 0; k < len(r.pending); k++ {
		ev := r.pending[k]
		t := ev.GetTriggerTime()
		if t.Before(Now) || t.Equal(Now) {
			if rxcallback(ev) {
				r.deleteEvent(k)
			}
			nowMinusStep := Now.Add(-config.timeIncStep)
			if t.Before(nowMinusStep) {
				eventsPastDeadline++
				log(LOG_V, "WARNING: events handled past deadline", eventsPastDeadline)
			}
		} else {
			haveMore = true
		}
	}
	return haveMore
}

func (r *RunnerBase) NowIsDone() bool {
	r.pendingMutex.Lock()
	defer r.pendingMutex.Unlock()

	if r.GetState() > RstateRunning {
		// drop to the floor, empty the queue..
		r.pending = r.pending[0:0]
		return true
	}

	for k := 0; k < len(r.pending); k++ {
		ev := r.pending[k]
		t := ev.GetTriggerTime()
		if t.Before(Now) || t.Equal(Now) {
			return false
		}
	}
	return true
}

func (r *RunnerBase) closeTxChannels() {
	for i := 0; i < cap(r.txchans); i++ {
		txch := r.txchans[i]
		if txch != nil {
			close(txch)
		}
	}
}
