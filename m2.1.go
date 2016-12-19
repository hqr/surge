//
// modelTwoDotOne ("2.1") = modelTwo + Rate Control
// where the latter is a simple(st) variation of leaky bucket
// Similar to modelTwo, gateway and server execute indentical Run()
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
type modelTwoDotOne struct {
	ModelGeneric
}

type gatewayTwoDotOne struct {
	NodeRunnerBase
}

type serverTwoDotOne struct {
	NodeRunnerBase
}

//
// init
//
var m21 = modelTwoDotOne{}

func init() {
	d := NewStatsDescriptors("2.1")
	d.Register("event", StatsKindCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxchannelbusy", StatsKindPercentage, StatsScopeGateway|StatsScopeServer)
	d.Register("txbytes", StatsKindByteCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxbytes", StatsKindByteCount, StatsScopeServer|StatsScopeGateway)

	props := make(map[string]interface{}, 1)
	props["description"] = "identical clustered nodes exchanging rate controlled events"
	RegisterModel("2.1", &m21, props)
}

//==================================================================
//
// gatewayTwoDotOne methods
//
//==================================================================
func (r *gatewayTwoDotOne) Run() {
	r.state = RstateRunning

	// event handling is a NOP in this model
	rxcallback := func(ev EventInterface) int {
		log(LogV, "rxcallback", r.String(), ev.String())
		return 0
	}

	go m21.run(&r.NodeRunnerBase, rxcallback)
}

//==================================================================
//
// serverTwoDotOne methods
//
//==================================================================
func (r *serverTwoDotOne) Run() {
	r.state = RstateRunning

	// event handling is a NOP in this model
	rxcallback := func(ev EventInterface) int {
		assert(r == ev.GetTarget())
		log(LogV, "rxcallback", r.String(), ev.String())
		return 0
	}

	go m21.run(&r.NodeRunnerBase, rxcallback)
}

//==================================================================
//
// modelTwoDotOne methods
//
//==================================================================
//
// modelTwoDotOne interface methods
//
func (m *modelTwoDotOne) NewGateway(i int) NodeRunnerInterface {
	gwy := &gatewayTwoDotOne{NodeRunnerBase{RunnerBase: RunnerBase{id: i}, strtype: GWY}}
	gwy.init(config.numServers)
	return gwy
}

func (m *modelTwoDotOne) NewServer(i int) NodeRunnerInterface {
	srv := &serverTwoDotOne{NodeRunnerBase: NodeRunnerBase{RunnerBase: RunnerBase{id: i}, strtype: SRV}}
	srv.init(config.numGateways)
	return srv
}

//
// modelTwoDotOne private methods: common Gateway/Server send/recv and run()
//

func (m *modelTwoDotOne) run(rb *NodeRunnerBase, rxcallback processEventCb) {
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

func (m *modelTwoDotOne) recv(r *NodeRunnerBase, rxcallback processEventCb) {
	r.receiveEnqueue()
	r.processPendingEvents(rxcallback)
}

func (m *modelTwoDotOne) send(r *NodeRunnerBase) bool {
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
	ev := newTimedAnyEvent(r, at, peer)
	return r.Send(ev, SmethodDontWait)
}

func (m *modelTwoDotOne) Configure() {
	config.timeClusterTrip = time.Microsecond * 4
}
