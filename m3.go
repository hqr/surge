//
// modelThree (name = "3") is a simple ping-pong with a random server
// selection
// Each event produced by the configured gateways is pseudo-randomly ID-ed
// Server-recipient responds back to the source with a new event that carries
// the ID of the source event
// For a given (gateway, server) pair there is only one event in-flight
// at any given time.
//
// Framework services utilized by this model:
// - time, events and event comm,
// - multitasking, stats reporting, logging
//
package surge

import (
	"math/rand"
	"time"
)

// implements ModelInterface
type modelThree struct {
}

type gatewayThree struct {
	RunnerBase
	waitingResponse []int64
}

type serverThree struct {
	RunnerBase
}

type TimedUniqueEvent struct {
	TimedAnyEvent
	id int64
}

func newTimedUniqueEvent(src RunnerInterface, when time.Duration, tgt RunnerInterface, id int64) *TimedUniqueEvent {
	ev := newTimedAnyEvent(src, when, tgt)
	if id == 0 {
		id, _ = uqrandom64(src.GetID())
	}
	return &TimedUniqueEvent{*ev, id}
}

//
// init
//
func init() {
	d := NewStatsDescriptors("3")
	d.Register("event", StatsKindCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxbusy", StatsKindPercentage, StatsScopeGateway|StatsScopeServer)

	props := make(map[string]interface{}, 1)
	props["description"] = "ping-pong with a random target selection"
	RegisterModel("3", &modelThree{}, props)
}

//==================================================================
//
// gatewayThree methods
//
//==================================================================
//
// generate random event (storm) => random servers
//
func (r *gatewayThree) Run() {
	r.state = RstateRunning

	// event handling is a NOP in this model
	rxcallback := func(ev EventInterface) bool {
		log(LogV, "GWY rxcallback", r.String(), ev.String())

		// validate that we got response from the right target
		id := ev.GetSource().GetID()
		assert(r.waitingResponse[id] != 0)

		// validate that we got response to the right event
		realevent := ev.(*TimedUniqueEvent)
		assert(r.waitingResponse[id] == realevent.id)

		// response (nop-) handled, now can talk to target.id == id again..
		r.waitingResponse[id] = 0
		return true
	}

	go func() {
		for r.state == RstateRunning {
			// send
			for i := 0; i < 10; i++ {
				srv := r.selectTarget()
				if srv != nil {
					tgtid := srv.GetID()
					eventID, _ := uqrandom64(r.GetID())
					at := clusterTripPlusRandom()
					ev := newTimedUniqueEvent(r, at, srv, eventID)
					r.Send(ev, SmethodWait)

					r.waitingResponse[tgtid] = eventID
				}
			}
			// recv
			r.receiveEnqueue()
			time.Sleep(time.Microsecond)
			r.processPendingEvents(rxcallback)
		}

		r.closeTxChannels()
	}()
}

func (r *gatewayThree) selectTarget() RunnerInterface {
	numPeers := cap(r.eps) - 1
	assert(numPeers > 1)
	id := rand.Intn(numPeers) + 1
	cnt := 0
	for {
		peer := r.eps[id]
		if r.waitingResponse[id] == 0 {
			return peer
		}
		id++
		cnt++
		if id >= numPeers {
			id = 1
		}
		if cnt >= numPeers {
			// is overloaded
			return nil
		}
	}
}

//==================================================================
//
// serverThree methods
//
//==================================================================
func (r *serverThree) Run() {
	r.state = RstateRunning

	// event handling is a NOP in this model
	rxcallback := func(ev EventInterface) bool {
		assert(r == ev.GetTarget())
		log(LogV, "SRV rxcallback", r.String(), ev.String())

		realevent, ok := ev.(*TimedUniqueEvent)
		assert(ok)

		// send the response
		gwysrc := ev.GetSource()
		at := clusterTripPlusRandom()
		evresponse := newTimedUniqueEvent(r, at, gwysrc, realevent.id)
		r.Send(evresponse, SmethodWait)
		return true
	}

	go func() {
		for r.state == RstateRunning {
			r.receiveEnqueue()
			time.Sleep(time.Microsecond)
			r.processPendingEvents(rxcallback)
		}
	}()
}

//==================================================================
//
// modelThree methods
//
//==================================================================
//
// modelThree interface methods
//
func (m *modelThree) NewGateway(i int) RunnerInterface {
	gwy := &gatewayThree{
		RunnerBase:      RunnerBase{id: i, strtype: "GWY"},
		waitingResponse: nil,
	}
	gwy.init(config.numServers)
	l := cap(gwy.eps)
	gwy.waitingResponse = make([]int64, l)
	return gwy
}

func (m *modelThree) NewServer(i int) RunnerInterface {
	srv := &serverThree{RunnerBase: RunnerBase{id: i, strtype: "SRV"}}
	srv.init(config.numGateways)
	return srv
}

func (m *modelThree) Configure() {
	config.timeClusterTrip = time.Microsecond * 2
}
