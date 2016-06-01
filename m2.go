//
// modelTwo (m2, "2") implements random send, random receive
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
type modelTwo struct {
}

type gatewayTwo struct {
	RunnerBase
}

type serverTwo struct {
	RunnerBase
}

//
// init
//
var m2 = modelTwo{}

func init() {
	d := NewStatsDescriptors("2")
	d.Register("event", StatsKindCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxchannelbusy", StatsKindPercentage, StatsScopeGateway|StatsScopeServer)
	d.Register("txbytes", StatsKindByteCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxbytes", StatsKindByteCount, StatsScopeServer|StatsScopeGateway)

	props := make(map[string]interface{}, 1)
	props["description"] = "identical clustered nodes exchanging random events"
	RegisterModel("2", &m2, props)
}

//==================================================================
//
// gatewayTwo methods
//
//==================================================================
func (r *gatewayTwo) Run() {
	r.state = RstateRunning

	// event handling is a NOP in this model
	rxcallback := func(ev EventInterface) int {
		log(LogV, "rxcallback", r.String(), ev.String())
		return 0
	}

	go m2.run(&r.RunnerBase, rxcallback)
}

//==================================================================
//
// serverTwo methods
//
//==================================================================
func (r *serverTwo) Run() {
	r.state = RstateRunning

	// event handling is a NOP in this model
	rxcallback := func(ev EventInterface) int {
		assert(r == ev.GetTarget())
		log(LogV, "rxcallback", r.String(), ev.String())
		return 0
	}

	go m2.run(&r.RunnerBase, rxcallback)
}

//==================================================================
//
// modelTwo methods
//
//==================================================================
//
// modelTwo interface methods
//
func (m *modelTwo) NewGateway(i int) RunnerInterface {
	gwy := &gatewayTwo{RunnerBase{id: i, strtype: GWY}}
	gwy.init(config.numServers)
	return gwy
}

func (m *modelTwo) NewServer(i int) RunnerInterface {
	srv := &serverTwo{RunnerBase: RunnerBase{id: i, strtype: SRV}}
	srv.init(config.numGateways)
	return srv
}

//
// modelTwo private methods: common Gateway/Server send/recv and run()
//
func (m *modelTwo) run(rb *RunnerBase, rxcallback processEventCb) {
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

func (m *modelTwo) recv(r *RunnerBase, rxcallback processEventCb) {
	r.receiveEnqueue()
	time.Sleep(time.Microsecond)
	r.processPendingEvents(rxcallback)
}

func (m *modelTwo) send(r *RunnerBase) bool {
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

func (m *modelTwo) Configure() {}
