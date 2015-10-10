//
// ModelThree (name = "3") is a simple ping-pong with a random server
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
type ModelThree struct {
}

type GatewayThree struct {
	RunnerBase
	waitingResponse []int64
}

type ServerThree struct {
	RunnerBase
}

type TimedUniqueEvent struct {
	TimedUcastEvent
	id int64
}

func newTimedUniqueEvent(src RunnerInterface, when time.Duration, tgt RunnerInterface, id int64) *TimedUniqueEvent {
	ev := newTimedUcastEvent(src, when, tgt)
	if id == 0 {
		id, _ = uqrandom64(src.GetId())
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
	RegisterModel("3", &ModelThree{}, props)
}

//==================================================================
//
// GatewayThree methods
//
//==================================================================
//
// generate random event (storm) => random servers
//
func (r *GatewayThree) Run() {
	r.state = RstateRunning

	// event handling is a NOP in this model
	rxcallback := func(ev EventInterface) bool {
		log(LOG_V, "GWY rxcallback", r.String(), ev.String())

		// validate that we got response from the right target
		id := ev.GetSource().GetId()
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
					tgtid := srv.GetId()
					eventId, _ := uqrandom64(r.GetId())
					at := clusterTripPlusRandom()
					ev := newTimedUniqueEvent(r, at, srv, eventId)
					r.Send(ev, true)

					r.waitingResponse[tgtid] = eventId
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

func (r *GatewayThree) selectTarget() RunnerInterface {
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
// ServerThree methods
//
//==================================================================
func (r *ServerThree) Run() {
	r.state = RstateRunning

	// event handling is a NOP in this model
	rxcallback := func(ev EventInterface) bool {
		assert(r == ev.GetTarget())
		log(LOG_V, "SRV rxcallback", r.String(), ev.String())

		realevent, ok := ev.(*TimedUniqueEvent)
		assert(ok)

		// send the response
		gwysrc := ev.GetSource()
		at := clusterTripPlusRandom()
		evresponse := newTimedUniqueEvent(r, at, gwysrc, realevent.id)
		r.Send(evresponse, true)
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
// ModelThree methods
//
//==================================================================
//
// ModelThree interface methods
//
func (m *ModelThree) NewGateway(i int) RunnerInterface {
	gwy := &GatewayThree{
		RunnerBase:      RunnerBase{id: i, strtype: "GWY"},
		waitingResponse: nil,
	}
	gwy.init(config.numServers)
	l := cap(gwy.eps)
	gwy.waitingResponse = make([]int64, l)
	return gwy
}

func (m *ModelThree) NewServer(i int) RunnerInterface {
	srv := &ServerThree{RunnerBase: RunnerBase{id: i, strtype: "SRV"}}
	srv.init(config.numGateways)
	return srv
}

func (m *ModelThree) NewDisk(i int) RunnerInterface { return nil }

func (m *ModelThree) Configure() {
	config.timeClusterTrip = time.Microsecond * 2
}
