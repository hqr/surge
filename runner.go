package surge

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
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
	GetID() int
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
	id      int
	state   RunnerStateEnum
	txchans []chan EventInterface
	rxchans []chan EventInterface
	eps     []RunnerInterface
	cases   []reflect.SelectCase
	rxqueue *RxQueueSorted
	txqueue *TxQueue
	strtype string // log
	rxcount int    // num live Rx connections
}

type processEvent func(ev EventInterface) bool

//==================================================================
// RunnerBase interface methods
//==================================================================
func (r *RunnerBase) setChannels(peer RunnerInterface, txch chan EventInterface, rxch chan EventInterface) {
	peerid := peer.GetID()

	assert(0 < peerid && peerid < cap(r.txchans) && peerid < cap(r.rxchans))
	assert(peerid < cap(r.eps))
	assert(r.txchans[peerid] == nil)
	assert(r.rxchans[peerid] == nil)

	r.eps[peerid] = peer
	r.txchans[peerid] = txch
	r.rxchans[peerid] = rxch

	r.cases[peerid-1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(rxch)}

	r.rxqueue = NewRxQueueSorted(r, 0)
	r.txqueue = NewTxQueue(r, 0)
}

func (r *RunnerBase) getChannels(peer RunnerInterface) (chan EventInterface, chan EventInterface) {
	peerid := peer.GetID()
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
func (r *RunnerBase) GetID() int                { return r.id }

func (r *RunnerBase) GetStats(reset bool) NodeStats {
	return r.rxqueue.GetStats(reset)
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
			log(LogVVV, "recv-ed", ev.String())
			if !locked {
				r.rxqueue.lock()
				locked = true
			}
			r.rxqueue.insertEvent(ev)
			newcnt++
			if newcnt < 2 { // TODO: experiment with more
				continue
			}
		}
		break
	}
	return newcnt > 0, err
}

func (r *RunnerBase) recvNextEvent() (EventInterface, error) {
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

func (r *RunnerBase) NumPendingEvents(exact bool) int64 {
	return r.rxqueue.NumPendingEvents(exact)
}

func (r *RunnerBase) processPendingEvents(rxcallback processEvent) {
	r.rxqueue.processPendingEvents(rxcallback)
}

func (r *RunnerBase) NowIsDone() bool {
	if r.GetState() > RstateRunning {
		r.rxqueue.cleanup()
		return true
	}

	if r.txqueue.NowIsDone() {
		return r.rxqueue.NowIsDone()
	}
	return false
}

func (r *RunnerBase) closeTxChannels() {
	for i := 0; i < cap(r.txchans); i++ {
		txch := r.txchans[i]
		if txch != nil {
			close(txch)
		}
	}
}
