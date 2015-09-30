package surge

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"time"
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

	NumPendingEvents(exact bool) int64

	GetState() RunnerStateEnum
	GetId() int
	GetStats(reset bool) NodeStats

	String() string

	Send(ev EventInterface, wait bool) bool
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
	// stats
	statsMutex       *sync.Mutex
	statsPtrArr      []*int64
	eventstats       int64
	busycnt          int64
	idlecnt          int64
	busyidletick     time.Time
	realpendingdepth int64
	// log
	strtype string
	// num live Rx connections
	rxcount int
}

//
// const
//
const INITIAL_PENDING_CNT int = 64
const INITIAL_STATS_COUNTERS int = 4

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
// generic "event" and "busy" d-tors/stats
//
func (r *RunnerBase) GetStats(reset bool) NodeStats {
	r.statsMutex.Lock()
	defer r.statsMutex.Unlock()

	busypct := int64(0)
	estatcopy := r.eventstats

	if r.busycnt > 0 {
		busypct = r.busycnt * 100 / (r.busycnt + r.idlecnt)
	}
	nodestats := map[string]int64{
		"event": estatcopy,
		"busy":  busypct,
	}
	if reset {
		// if overriding, must save a copy..
		for k := 0; k < len(r.statsPtrArr); k++ {
			cntPtr := r.statsPtrArr[k]
			*cntPtr = int64(0)
		}
	}
	return nodestats
}

func (r *RunnerBase) String() string { return fmt.Sprintf("[%s#%v]", r.strtype, r.id) }

func (r *RunnerBase) Send(ev EventInterface, wait bool) bool {
	peer := ev.GetTarget()
	txch, _ := r.getChannels(peer)
	if wait {
		txch <- ev
		return true
	}
	select {
	case txch <- ev:
		// all good, do nothing
	default:
		log("WARNING: channel full", r.String(), peer.String())
		return false
	}
	return true
}

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

	initialQ := make([]EventInterface, INITIAL_PENDING_CNT)
	r.pending = initialQ[0:0]
	r.pendingMutex = &sync.Mutex{}

	r.eventstats, r.busycnt, r.idlecnt = int64(0), int64(0), int64(0)
	r.busyidletick = time.Now() // != Now
	r.realpendingdepth = int64(0)
	r.statsMutex = &sync.Mutex{}

	statsPtrArr := make([]*int64, INITIAL_STATS_COUNTERS)
	r.statsPtrArr = statsPtrArr[0:0]

	r.addStatsPtr(&r.eventstats)
	r.addStatsPtr(&r.busycnt)
	r.addStatsPtr(&r.idlecnt)
}

func (r *RunnerBase) addStatsPtr(ptr *int64) {
	r.statsPtrArr = append(r.statsPtrArr, ptr)
}

func (r *RunnerBase) selectRandomPeer(maxload int) RunnerInterface {
	numPeers := cap(r.eps) - 1
	if numPeers == 1 {
		return r.eps[1]
	}
	idx := rand.Intn(numPeers)
	if maxload == 0 { // unlimited
		return r.eps[idx+1]
	}
	cnt := 0
	for {
		peer := r.eps[idx+1]
		if peer.NumPendingEvents(false) <= int64(maxload) {
			return peer
		}
		idx++
		if idx >= numPeers {
			idx = 0
		}
		cnt++
		if cnt >= numPeers { // is overloaded
			return nil
		}
	}
}

func (r *RunnerBase) receiveEnqueue() (bool, error) {
	ev, err := r.recvNextEvent()
	newev := false
	if err != nil {
		if r.state != RstateRxClosed {
			log("ERROR", r.String(), err)
		}
	} else if ev != nil {
		log(LOG_VVV, "recv-ed", ev.String())
		r.insertEvent(ev)
		newev = true
	}
	return newev, err
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

func (r *RunnerBase) NumPendingEvents(exact bool) int64 {
	if !exact {
		return int64(len(r.pending))
	}
	// FIXME: read lock
	r.pendingMutex.Lock()
	defer r.pendingMutex.Unlock()
	return r.realpendingdepth
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

	ontime := true
	for k := 0; k < len(r.pending); k++ {
		ev := r.pending[k]
		t := ev.GetTriggerTime()
		if t.After(Now) {
			continue
		}
		if rxcallback(ev) {
			r.deleteEvent(k)
		}
		nowMinusStep := Now.Add(-config.timeIncStep)
		if t.Before(nowMinusStep) {
			ontime = false
			eventsPastDeadline++
			diff := Now.Sub(t)
			log(LOG_BOTH, "WARNING: past trigger time", diff, eventsPastDeadline)
		}
	}
	return ontime
}

func (r *RunnerBase) NowIsDone() bool {
	r.pendingMutex.Lock()
	defer r.pendingMutex.Unlock()

	if r.GetState() > RstateRunning {
		// empty the queue..
		for k := 0; k < cap(r.pending); k++ {
			r.pending[k] = nil
		}
		r.pending = r.pending[0:0]
		return true
	}
	done := true
	realpendingdepth := int64(0)
	for k := 0; k < len(r.pending); k++ {
		ev := r.pending[k]

		ct := ev.GetCreationTime()
		// cluster trip time enforced: earlier must be in flight
		if Now.Sub(ct) < config.timeClusterTrip {
			continue
		}

		realpendingdepth++

		t := ev.GetTriggerTime()
		if t.Before(Now) || t.Equal(Now) {
			done = false
		}
	}
	if !r.busyidletick.Equal(Now) {
		// nothing *really* pending => idle, otherwise busy
		if realpendingdepth == 0 {
			r.idlecnt++
		} else {
			r.busycnt++
		}
		r.busyidletick = Now
		r.realpendingdepth = realpendingdepth
	}

	return done
}

func (r *RunnerBase) closeTxChannels() {
	for i := 0; i < cap(r.txchans); i++ {
		txch := r.txchans[i]
		if txch != nil {
			close(txch)
		}
	}
}
