package surge

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
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

type SendMethodEnum int

const (
	SmethodDontWait SendMethodEnum = iota
	SmethodWait
	SmethodDirectInsert
)

//
// interfaces
//
type BaseRunnerInterface interface {
	Run()
	NowIsDone() bool
	PrepareToStop()

	GetState() RunnerStateEnum
	GetID() int
	GetStats(reset bool) RunnerStats

	String() string
}

type DiskRunnerInterface interface {
	BaseRunnerInterface
}

type NodeRunnerInterface interface {
	BaseRunnerInterface

	setChannels(peer NodeRunnerInterface, txch chan EventInterface, rxch chan EventInterface)
	getChannels(peer NodeRunnerInterface) (chan EventInterface, chan EventInterface)

	setExtraChannels(peer NodeRunnerInterface, atidx int, txch chan EventInterface, rxch chan EventInterface)
	getExtraChannels(peer NodeRunnerInterface) (chan EventInterface, chan EventInterface)


	NumPendingEvents(exact bool) int64

	GetRateBucket() RateBucketInterface

	AddTio(tio TioInterface)
	RemoveTio(tio TioInterface)

	Send(ev EventInterface, how SendMethodEnum) bool
}

type RunnerBase struct {
	id      int
	state   RunnerStateEnum
}

//==================================================================
//
// base node runner with a bunch of private methods that can be used by subclasses
//
//==================================================================
//
// const
//
const initialTioCnt int = 4

type NodeRunnerBase struct {
	RunnerBase
	// channels
	txchans      []chan EventInterface
	rxchans      []chan EventInterface
	eps          []NodeRunnerInterface
	extraTxChans []chan EventInterface
	extraRxChans []chan EventInterface
	extraEps     []NodeRunnerInterface
	// cases
	cases []reflect.SelectCase
	// more state
	tios        map[int64]TioInterface
	rxqueue     *RxQueueSorted
	txqueue     *TxQueue
	strtype     string // log
	txbytestats int64
	rxbytestats int64
	rxcount     int // num live Rx connections
}

//==================================================================
// NodeRunnerBase interface methods
//==================================================================
func (r *NodeRunnerBase) setChannels(peer NodeRunnerInterface, txch chan EventInterface, rxch chan EventInterface) {
	peerid := peer.GetID()

	assert(0 < peerid && peerid < cap(r.txchans) && peerid < cap(r.rxchans))
	assert(peerid < cap(r.eps))
	assert(r.txchans[peerid] == nil)
	assert(r.rxchans[peerid] == nil)

	r.eps[peerid] = peer
	r.txchans[peerid] = txch
	r.rxchans[peerid] = rxch

	r.cases[peerid-1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(rxch)}

	if r.rxqueue == nil {
		r.rxqueue = NewRxQueueSorted(r, 0)
	}
	if r.txqueue == nil {
		r.txqueue = NewTxQueue(r, 0)
	}
}

func (r *NodeRunnerBase) getChannels(peer NodeRunnerInterface) (chan EventInterface, chan EventInterface) {
	peerid := peer.GetID()
	assert(r.eps[peerid].GetID() == peerid)
	return r.txchans[peerid], r.rxchans[peerid]
}

func (r *NodeRunnerBase) setExtraChannels(other NodeRunnerInterface, atidx int, txch chan EventInterface, rxch chan EventInterface) {
	numPeers := cap(r.txchans) - 1

	assert(r.extraEps[atidx] == nil)
	assert(r.txchans[numPeers] != nil)
	assert(r.rxchans[numPeers] != nil)

	r.extraEps[atidx] = other
	r.extraTxChans[atidx] = txch
	r.extraRxChans[atidx] = rxch

	assert(!r.cases[numPeers+atidx].Chan.IsValid())
	r.cases[numPeers+atidx] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(rxch)}
	assert(r.cases[numPeers+atidx].Chan.IsValid())
}

func (r *NodeRunnerBase) getExtraChannels(peer NodeRunnerInterface) (chan EventInterface, chan EventInterface) {
	i := int(0)
	for ; i < cap(r.extraTxChans); i++ {
		if r.extraEps[i] == peer {
			break
		}
		assert(r.extraEps[i].GetID() != peer.GetID())
	}
	assert(i < cap(r.extraTxChans))
	assert(r.extraTxChans[i] != nil)
	assert(r.extraRxChans[i] != nil)
	return r.extraTxChans[i], r.extraRxChans[i]
}

// sample Run() for a listening runner
func (r *RunnerBase) Run() {
	assert(false, "abstract method")
}

func (r *RunnerBase) PrepareToStop() {
	assert(r.state <= RstateStopped)
	r.state = RstateStopped
}

func (r *RunnerBase) NowIsDone() bool {
	return true
}

func (r *RunnerBase) GetState() RunnerStateEnum { return r.state }
func (r *RunnerBase) GetID() int                { return r.id }
func (r *RunnerBase) GetStats(reset bool) RunnerStats { return nil }

func (r *NodeRunnerBase) GetStats(reset bool) RunnerStats {
	s := r.rxqueue.GetStats(reset)
	if reset {
		s["txbytes"] = atomic.SwapInt64(&r.txbytestats, 0)
		s["rxbytes"] = atomic.SwapInt64(&r.rxbytestats, 0)
	} else {
		s["txbytes"] = atomic.LoadInt64(&r.txbytestats)
		s["rxbytes"] = atomic.LoadInt64(&r.rxbytestats)
	}
	return s
}

func (r *NodeRunnerBase) GetRateBucket() RateBucketInterface {
	return nil
}

func (r *NodeRunnerBase) String() string { return fmt.Sprintf("%s#%02d", r.strtype, r.id) }

func (r *NodeRunnerBase) Send(ev EventInterface, how SendMethodEnum) bool {
	peer := ev.GetTarget()
	if how == SmethodDirectInsert {
		if peer.String() != r.String() { // FIXME: better identity checking
			peer.Send(ev, how)
		} else {
			r.rxqueue.lock()
			r.rxqueue.insertEvent(ev)
			atomic.AddInt64(&r.rxbytestats, int64(ev.GetSize()))
			r.rxqueue.unlock()
		}
		r.updateTxBytes(ev)
		return true
	}

	txch, _ := r.getChannels(peer)
	if how == SmethodWait {
		txch <- ev
		r.updateTxBytes(ev)
		return true
	}

	assert(how == SmethodDontWait)
	select {
	case txch <- ev:
		// sent
		r.updateTxBytes(ev)
	default:
		log("WARNING: channel full", r.String(), peer.String())
		return false
	}
	return true
}

func (r *NodeRunnerBase) updateTxBytes(ev EventInterface) {
	// actual mcast transmitters do the accounting instead
	if !ev.IsMcast() {
		atomic.AddInt64(&r.txbytestats, int64(ev.GetSize()))
	}
}

//==================================================================
// NodeRunnerBase private methods that can be used by concrete models' runners
//==================================================================
func (r *NodeRunnerBase) init(numPeers int) {
	r.initPeerChannels(numPeers)
	r.initCases()
}

func (r *NodeRunnerBase) initPeerChannels(numPeers int) {
	r.txchans = make([]chan EventInterface, numPeers+1)
	r.rxchans = make([]chan EventInterface, numPeers+1)
	r.eps = make([]NodeRunnerInterface, numPeers+1)

	// indexed by runner ID, [0] not used
	r.txchans[0] = nil
	r.rxchans[0] = nil
	r.eps[0] = nil
}

func (r *NodeRunnerBase) initExtraChannels(n int) {
	r.extraTxChans = make([]chan EventInterface, n)
	r.extraRxChans = make([]chan EventInterface, n)
	r.extraEps = make([]NodeRunnerInterface, n)
}

func (r *NodeRunnerBase) initCases() {
	numPeers := cap(r.txchans) - 1
	numExtra := int(0)
	if r.extraTxChans != nil {
		numExtra = cap(r.extraTxChans)
		assert(numExtra == cap(r.extraRxChans))
		assert(numExtra == cap(r.extraEps))
	}
	r.cases = make([]reflect.SelectCase, numPeers+1+numExtra)
	r.cases[numPeers+numExtra] = reflect.SelectCase{Dir: reflect.SelectDefault}
	r.rxcount = numPeers + numExtra
}

func (r *NodeRunnerBase) initios(args ...interface{}) {
	cnt := initialTioCnt
	if len(args) > 0 {
		cnt = args[0].(int)
	}
	r.tios = make(map[int64]TioInterface, cnt)
}

func (r *NodeRunnerBase) AddTio(tio TioInterface) {
	_, ok := r.tios[tio.GetID()]
	assert(!ok, r.String()+": tio already exists: "+tio.String())
	r.tios[tio.GetID()] = tio
}

func (r *NodeRunnerBase) RemoveTio(tio TioInterface) {
	_, ok := r.tios[tio.GetID()]
	assert(ok, r.String()+": tio does not exist: "+tio.String())
	delete(r.tios, tio.GetID())
}

func (r *NodeRunnerBase) selectRandomPeer(maxload int) NodeRunnerInterface {
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

func (r *NodeRunnerBase) receiveEnqueue() (bool, error) {
	var err error
	var ev EventInterface
	newcnt := 0
	locked := false
	defer func() {
		if locked {
			r.rxqueue.unlock()
		}
	}()
	for {
		ev, err = r.recvNextEvent()
		if err != nil {
			if r.state != RstateRxClosed {
				log("ERROR", r.String(), err)
			}
		} else if ev != nil {
			if !locked {
				r.rxqueue.lock()
				locked = true
			}
			r.rxqueue.insertEvent(ev)
			atomic.AddInt64(&r.rxbytestats, int64(ev.GetSize()))
			newcnt++
			if newcnt < 2 { // TODO: experiment with more
				continue
			}
		}
		break
	}
	return newcnt > 0, err
}

func (r *NodeRunnerBase) recvNextEvent() (EventInterface, error) {
	for r.rxcount > 0 && r.state == RstateRunning {
		chosen, value, ok := reflect.Select(r.cases)
		if ok {
			event := value.Interface().(EventInterface)
			return event, nil
		}

		// SelectDefault case
		var selectedcase = r.cases[chosen]
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

func (r *NodeRunnerBase) NumPendingEvents(exact bool) int64 {
	return r.rxqueue.NumPendingEvents(exact)
}

func (r *NodeRunnerBase) processPendingEvents(rxcallback processEventCb) int {
	return r.rxqueue.processPendingEvents(rxcallback)
}

func (r *NodeRunnerBase) NowIsDone() bool {
	if r.GetState() > RstateRunning {
		r.rxqueue.cleanup()
		return true
	}

	if r.txqueue.NowIsDone() {
		return r.rxqueue.NowIsDone()
	}
	return false
}

func (r *NodeRunnerBase) closeTxChannels() {
	for i := 0; i < cap(r.txchans); i++ {
		txch := r.txchans[i]
		if txch != nil {
			close(txch)
		}
	}
}
