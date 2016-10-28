//
// modelFour (m4, "4") is a step-up in terms of layering:
// this model uses a simple 4-stages IO pipeline via the farmework's
// TIO service...
//
// Framework services utilized by this model:
// - time, events and event comm,
// - multitasking, stats reporting, logging
// - TIO
//
package surge

import (
	"sync/atomic"
	"time"
)

// implements ModelInterface
type modelFour struct {
	pipeline *Pipeline
}

type gatewayFour struct {
	NodeRunnerBase
	tiostats int64
}

type serverFour struct {
	NodeRunnerBase
}

//
// init
//
var m4 modelFour

func init() {
	p := NewPipeline()
	p.AddStage(&PipelineStage{name: "PUT-REQ", handler: "M4putrequest"})
	p.AddStage(&PipelineStage{name: "PUT-REQ-ACK", handler: "M4putreqack"})
	p.AddStage(&PipelineStage{name: "PUT-XFER", handler: "M4putxfer"})
	p.AddStage(&PipelineStage{name: "PUT-XFER-ACK", handler: "M4putxferack"})

	m4.pipeline = p

	d := NewStatsDescriptors("4")
	d.Register("event", StatsKindCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxchannelbusy", StatsKindPercentage, StatsScopeGateway|StatsScopeServer)
	d.Register("txbytes", StatsKindByteCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxbytes", StatsKindByteCount, StatsScopeServer|StatsScopeGateway)
	d.Register("tio", StatsKindCount, StatsScopeGateway)

	props := make(map[string]interface{}, 1)
	props["description"] = "TIO pipeline in action"
	RegisterModel("4", &m4, props)
}

//==================================================================
//
// gatewayFour methods
//
//==================================================================
func (r *gatewayFour) Run() {
	r.state = RstateRunning
	outstanding := 0

	rxcallback := func(ev EventInterface) int {
		tioevent := ev.(*TimedAnyEvent)
		tio := tioevent.GetTio().(*Tio)
		tio.doStage(r)

		if tio.Done() {
			atomic.AddInt64(&r.tiostats, 1)
			outstanding--
		}

		return 0
	}

	go func() {
		for r.state == RstateRunning {
			// issue TIOs
			for k := 0; k < 10; k++ {
				// somewhat limit outstanding TIOs
				if outstanding >= 10*config.numServers {
					break
				}
				tgt := r.selectRandomPeer(0)
				assert(tgt != nil)

				tio := NewTio(r, m4.pipeline, tgt)
				outstanding++
				at := clusterTripPlusRandom()
				tio.nextAnon(at, tgt)
				time.Sleep(time.Microsecond)
			}

			// recv
			r.receiveEnqueue()
			time.Sleep(time.Microsecond)
			r.processPendingEvents(rxcallback)
		}

		r.closeTxChannels()
	}()
}

func (r *gatewayFour) M4putreqack(ev EventInterface) error {
	log(LogVV, r.String(), "::M4putreqack()", ev.String())

	// create event here and ask tio to follow up
	at := clusterTripPlusRandom()
	nextev := newTimedAnyEvent(r, at, ev.GetSource())

	tioevent := ev.(*TimedAnyEvent)
	tio := tioevent.GetTio()

	tio.next(nextev, SmethodWait)
	return nil
}

func (r *gatewayFour) M4putxferack(ev EventInterface) error {
	log(LogVV, r.String(), "::M4putxferack()", ev.String())

	return nil
}

func (r *gatewayFour) GetStats(reset bool) RunnerStats {
	s := r.NodeRunnerBase.GetStats(true)
	if reset {
		s["tio"] = atomic.SwapInt64(&r.tiostats, 0)
	} else {
		s["tio"] = atomic.LoadInt64(&r.tiostats)
	}
	return s
}

//==================================================================
//
// serverFour methods
//
//==================================================================
func (r *serverFour) Run() {
	r.state = RstateRunning

	rxcallback := func(ev EventInterface) int {
		tioevent := ev.(*TimedAnyEvent)
		tio := tioevent.GetTio()
		tio.doStage(r)

		return 0
	}

	go func() {
		for r.state == RstateRunning {
			// recv
			r.receiveEnqueue()
			time.Sleep(time.Microsecond)
			r.processPendingEvents(rxcallback)
		}

		r.closeTxChannels()
	}()
}

func (r *serverFour) M4putrequest(ev EventInterface) error {
	log(LogVV, r.String(), "::M4putrequest()", ev.String())

	tioevent := ev.(*TimedAnyEvent)
	tio := tioevent.GetTio()

	at := clusterTripPlusRandom()
	tio.nextAnon(at, ev.GetSource())
	return nil
}

func (r *serverFour) M4putxfer(ev EventInterface) error {
	log(LogVV, r.String(), "::M4putxfer()", ev.String())

	at := clusterTripPlusRandom()

	tioevent := ev.(*TimedAnyEvent)
	tio := tioevent.GetTio()
	tio.nextAnon(at, ev.GetSource())
	return nil
}

//==================================================================
//
// modelFour methods
//
//==================================================================
//
// modelFour interface methods
//
func (m *modelFour) NewGateway(i int) NodeRunnerInterface {
	gwy := &gatewayFour{NodeRunnerBase{RunnerBase: RunnerBase{id: i}, strtype: GWY}, 0}
	gwy.init(config.numServers)
	gwy.initios()
	return gwy
}

func (m *modelFour) NewServer(i int) NodeRunnerInterface {
	srv := &serverFour{NodeRunnerBase: NodeRunnerBase{RunnerBase: RunnerBase{id: i}, strtype: SRV}}
	srv.init(config.numGateways)
	return srv
}

func (m *modelFour) Configure() {
	config.timeClusterTrip = time.Microsecond * 2
}
