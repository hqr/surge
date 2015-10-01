//
// ModelTwo (m2, "2") implements random send, random receive
// Gateway and Server types are almost indistinguishable in this model
// as they both execute indentical Run()
//
// Framework services utilized by this model:
// - time, events and event comm,
// - multitasking, stats reporting, logging
//
package surge

import (
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
	d := NewStatsDescriptors("2")
	d.Register("event", StatsKindCount, StatsScopeGateway|StatsScopeServer)
	d.Register("busy", StatsKindPercentage, StatsScopeGateway|StatsScopeServer)

	props := make(map[string]interface{}, 1)
	props["description"] = "identical clustered nodes exchanging random events"
	RegisterModel("2", &m2, props)
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

	go m2.run(&r.RunnerBase, rxcallback)
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

	go m2.run(&r.RunnerBase, rxcallback)
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
// ModelTwo private methods: common Gateway/Server send/recv and run()
//
func (m *ModelTwo) run(rb *RunnerBase, rxcallback processEvent) {
	for rb.state == RstateRunning {
		m2.recv(rb, rxcallback)
		if !m2.send(rb) {
			k := 0
			for rb.state == RstateRunning && k < 2 {
				time.Sleep(time.Microsecond * 10)
				m2.recv(rb, rxcallback)
				k++
			}
		}
	}

	rb.closeTxChannels()
}

func (m *ModelTwo) recv(r *RunnerBase, rxcallback processEvent) {
	r.receiveEnqueue()
	time.Sleep(time.Microsecond)
	r.processPendingEvents(rxcallback)
}

func (m *ModelTwo) send(r *RunnerBase) bool {
	r1 := r.selectRandomPeer(64)
	r2 := r.selectRandomPeer(64)
	if r1 == nil || r2 == nil {
		return false
	}
	peer := r1
	if r1.NumPendingEvents(true) > r2.NumPendingEvents(true) {
		peer = r2
	}
	at := clusterTripPlusRandom()
	ev := newTimedUcastEvent(r, at, peer)
	return r.Send(ev, false)
}

func (m *ModelTwo) Configure() {
	config.timeClusterTrip = time.Microsecond * 4
}
