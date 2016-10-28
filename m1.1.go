//
// modelOne's variation (m11, "1.1") is the same as m1.go with a single
// primary difference: the senders do a double-take of sorts
// to select the least loaded server out of a pair of two randomly selected.
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
type modelOneDotOne struct {
}

type gatewayOneDotOne struct {
	NodeRunnerBase
}

type serverOneDotOne struct {
	NodeRunnerBase
}

//
// init
//
func init() {
	d := NewStatsDescriptors("1.1")
	d.Register("event", StatsKindCount, StatsScopeServer)
	d.Register("rxchannelbusy", StatsKindPercentage, StatsScopeServer)
	d.Register("txbytes", StatsKindByteCount, StatsScopeGateway)

	props := make(map[string]interface{}, 1)
	props["description"] = "unidirectional storm of random events, with partially random selection"
	RegisterModel("1.1", &modelOneDotOne{}, props)
}

//==================================================================
//
// gatewayOneDotOne methods
//
//==================================================================
//
// generate random event (storm) => random servers
//
func (r *gatewayOneDotOne) Run() {
	r.state = RstateRunning

	go func() {
		for r.state == RstateRunning {
			r.send()
			time.Sleep(time.Microsecond * 100)
		}
		r.closeTxChannels()
	}()
}

func (r *gatewayOneDotOne) send() {
	srv1 := r.selectRandomPeer(64)
	srv2 := r.selectRandomPeer(64)
	if srv1 == nil || srv2 == nil {
		return
	}
	srv := srv1
	if srv1.NumPendingEvents(true) > srv2.NumPendingEvents(true) {
		srv = srv2
	}
	at := clusterTripPlusRandom()
	r.Send(newTimedAnyEvent(r, at, srv), SmethodDirectInsert)
}

//==================================================================
//
// serverOneDotOne methods
//
//==================================================================
func (r *serverOneDotOne) Run() {
	r.state = RstateRunning

	// event handling is a NOP in this model
	rxcallback := func(ev EventInterface) int {
		assert(r == ev.GetTarget())
		log(LogVV, "proc-ed", ev.String())
		return 0
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
// ModelInterface methods
//
//==================================================================
func (m *modelOneDotOne) NewGateway(i int) NodeRunnerInterface {
	gwy := &gatewayOneDotOne{NodeRunnerBase{RunnerBase: RunnerBase{id: i}, strtype: GWY}}
	gwy.init(config.numServers)
	return gwy
}

func (m *modelOneDotOne) NewServer(i int) NodeRunnerInterface {
	srv := &serverOneDotOne{NodeRunnerBase: NodeRunnerBase{RunnerBase: RunnerBase{id: i}, strtype: SRV}}
	srv.init(config.numGateways)
	return srv
}

func (m *modelOneDotOne) Configure() {
	config.timeClusterTrip = time.Microsecond * 4
}
