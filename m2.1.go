//
// ModelTwoDotOne ("2.1") = ModelTwo + Rate Control
// where the latter is a simple(st) variation of leaky bucket
// Similar to ModelTwo, gateway and server execute indentical Run()
//
package surge

import (
	"time"
)

// const
const (
	MaxBucket  = 10                   // max back to back burst
	RateRefill = time.Microsecond / 2 // + one event per given duration
)

// implements ModelInterface
type ModelTwoDotOne struct {
}

type GatewayTwoDotOne struct {
	RunnerBase
}

type ServerTwoDotOne struct {
	RunnerBase
}

//
// init
//
var m21 ModelTwoDotOne = ModelTwoDotOne{}

func init() {
	d := NewStatsDescriptors("2.1")
	d.Register("event", StatsKindCount, StatsScopeGateway|StatsScopeServer)
	d.Register("busy", StatsKindPercentage, StatsScopeGateway|StatsScopeServer)

	props := make(map[string]interface{}, 1)
	props["description"] = "identical clustered nodes exchanging rate controlled events"
	RegisterModel("2.1", &m21, props)
}

//==================================================================
//
// GatewayTwoDotOne methods
//
//==================================================================
func (r *GatewayTwoDotOne) Run() {
	r.state = RstateRunning

	// event handling is a NOP in this model
	rxcallback := func(ev EventInterface) bool {
		log(LOG_V, "GWY rxcallback", r.String(), ev.String())
		return true
	}

	go m21.run(&r.RunnerBase, rxcallback)
}

//==================================================================
//
// ServerTwoDotOne methods
//
//==================================================================
func (r *ServerTwoDotOne) Run() {
	r.state = RstateRunning

	// event handling is a NOP in this model
	rxcallback := func(ev EventInterface) bool {
		assert(r == ev.GetTarget())
		log(LOG_V, "SRV rxcallback", r.String(), ev.String())
		return true
	}

	go m21.run(&r.RunnerBase, rxcallback)
}

//==================================================================
//
// ModelTwoDotOne methods
//
//==================================================================
//
// ModelTwoDotOne interface methods
//
func (m *ModelTwoDotOne) NewGateway(i int) RunnerInterface {
	gwy := &GatewayTwoDotOne{RunnerBase{id: i, strtype: "GWY"}}
	gwy.init(config.numServers)
	return gwy
}

func (m *ModelTwoDotOne) NewServer(i int) RunnerInterface {
	srv := &ServerTwoDotOne{RunnerBase: RunnerBase{id: i, strtype: "SRV"}}
	srv.init(config.numGateways)
	return srv
}

func (m *ModelTwoDotOne) NewDisk(i int) RunnerInterface { return nil }

//
// ModelTwoDotOne private methods: common Gateway/Server send/recv and run()
//

func (m *ModelTwoDotOne) run(rb *RunnerBase, rxcallback processEvent) {
	lastRefill := Now
	leakyBucket := float64(MaxBucket)

	for rb.state == RstateRunning {
		m21.recv(rb, rxcallback)
		if leakyBucket <= float64(MaxBucket)-1.0 {
			elapsed := Now.Sub(lastRefill)
			if elapsed > RateRefill {
				leakyBucket += float64(elapsed) / float64(RateRefill)
				lastRefill = Now
			}
		}

		if leakyBucket < 1.0 {
			time.Sleep(time.Microsecond)
			continue
		}
		if m21.send(rb) {
			leakyBucket--
		}
	}

	rb.closeTxChannels()
}

func (m *ModelTwoDotOne) recv(r *RunnerBase, rxcallback processEvent) {
	r.receiveEnqueue()
	r.processPendingEvents(rxcallback)
}

func (m *ModelTwoDotOne) send(r *RunnerBase) bool {
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
