//
// ModelTwo (m2, "two") implements random send, random receive
// Gateway and Server types are almost indistinguishable in this model
// as they both execute indentical Run()
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
type ModelTwo struct {
}

type GatewayTwo struct {
	RunnerBase
}

type ServerTwo struct {
	RunnerBase
}

//
// init
//
var m2 ModelTwo = ModelTwo{}

func init() {
	d := NewStatsDescriptors("two")
	d.Register("event", StatsKindCount, StatsScopeGateway|StatsScopeServer)

	props := make(map[string]interface{}, 1)
	props["description"] = "identical clustered nodes exchanging random events"
	RegisterModel("two", &m2, props)
}

//==================================================================
//
// GatewayTwo methods
//
//==================================================================
func (r *GatewayTwo) Run() {
	r.state = RstateRunning

	// event handling is a NOP in this model
	rxcallback := func(ev EventInterface) bool {
		log(LOG_V, "GWY rxcallback", r.String(), ev.String())
		return true
	}

	go func() {
		for r.state == RstateRunning {
			m2.sendrecv(&r.RunnerBase, rxcallback)
		}

		r.closeTxChannels()
	}()
}

//==================================================================
//
// ServerTwo methods
//
//==================================================================
func (r *ServerTwo) Run() {
	r.state = RstateRunning

	// event handling is a NOP in this model
	rxcallback := func(ev EventInterface) bool {
		assert(r == ev.GetTarget())
		log(LOG_V, "SRV rxcallback", r.String(), ev.String())
		return true
	}

	go func() {
		for r.state == RstateRunning {
			m2.sendrecv(&r.RunnerBase, rxcallback)
		}

		r.closeTxChannels()
	}()
}

//==================================================================
//
// ModelTwo methods
//
//==================================================================
//
// ModelTwo interface methods
//
func (m *ModelTwo) NewGateway(i int) RunnerInterface {
	gwy := &GatewayTwo{RunnerBase{id: i, strtype: "GWY"}}
	gwy.init(config.numServers)
	return gwy
}

func (m *ModelTwo) NewServer(i int) RunnerInterface {
	srv := &ServerTwo{RunnerBase: RunnerBase{id: i, strtype: "SRV"}}
	srv.init(config.numGateways)
	return srv
}

func (m *ModelTwo) NewDisk(i int) RunnerInterface { return nil }

//
// ModelTwo private methods: common Gateway/Server send/recv
//
func (m *ModelTwo) sendrecv(r *RunnerBase, rxcallback processEvent) {
	r.receiveAndHandle(rxcallback)
	time.Sleep(time.Microsecond)
	r.processPendingEvents(rxcallback)
	time.Sleep(time.Microsecond)

	m.send(r)
}

func (m *ModelTwo) send(r *RunnerBase) bool {
	trip := config.timePhysTrip * 4
	peer := r.selectRandomPeer(64)
	if peer != nil {
		txch, _ := r.getChannels(peer)
		at := rand.Int63n(int64(trip)) + int64(trip)
		txch <- newTimedUcastEvent(r, time.Duration(at), peer)
	}
	return peer != nil
}
