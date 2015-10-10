//
// ModelFour (m4, "4") is a step-up in terms of layering:
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
type ModelFour struct {
	pipeline *Pipeline
}

type GatewayFour struct {
	RunnerBase
	tiostats int64
}

type ServerFour struct {
	RunnerBase
}

//
// init
//
var m4 ModelFour

func init() {
	p := NewPipeline()
	p.AddStage(&PipelineStage{name: "PUT-REQ", handler: "M4putrequest"})
	p.AddStage(&PipelineStage{name: "PUT-REQ-ACK", handler: "M4putreqack"})
	p.AddStage(&PipelineStage{name: "PUT-XFER", handler: "M4putxfer"})
	p.AddStage(&PipelineStage{name: "PUT-XFER-ACK", handler: "M4putxferack"})

	m4.pipeline = p

	d := NewStatsDescriptors("4")
	d.Register("event", StatsKindCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxbusy", StatsKindPercentage, StatsScopeGateway|StatsScopeServer)
	d.Register("tio", StatsKindCount, StatsScopeGateway)

	props := make(map[string]interface{}, 1)
	props["description"] = "TIO pipeline in action"
	RegisterModel("4", &m4, props)
}

//==================================================================
//
// GatewayFour methods
//
//==================================================================
func (r *GatewayFour) Run() {
	r.state = RstateRunning
	outstanding := 0

	rxcallback := func(ev EventInterface) bool {
		tioevent := ev.(*TimedUcastEvent)
		tio := tioevent.extension.(*Tio)
		log(LOG_VV, "GWY rxcallback", tio.String())
		tio.doStage(r)

		if tio.done {
			atomic.AddInt64(&r.tiostats, int64(1))
			outstanding--
		}

		return true
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

				tio := m4.pipeline.NewTio(r)
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

func (r *GatewayFour) M4putreqack(ev EventInterface) error {
	log(LOG_VV, r.String(), "::M4putreqack()", ev.String())

	// create event here and ask tio to follow up
	at := clusterTripPlusRandom()
	nextev := newTimedUcastEvent(r, at, ev.GetSource())

	tioevent := ev.(*TimedUcastEvent)
	tio := tioevent.extension.(*Tio)

	tio.next(nextev)
	return nil
}

func (r *GatewayFour) M4putxferack(ev EventInterface) error {
	log(LOG_VV, r.String(), "::M4putxferack()", ev.String())

	return nil
}

func (r *GatewayFour) GetStats(reset bool) NodeStats {
	s := r.RunnerBase.GetStats(true)
	if reset {
		s["tio"] = atomic.SwapInt64(&r.tiostats, int64(0))
	} else {
		s["tio"] = atomic.LoadInt64(&r.tiostats)
	}
	return s
}

//==================================================================
//
// ServerFour methods
//
//==================================================================
func (r *ServerFour) Run() {
	r.state = RstateRunning

	rxcallback := func(ev EventInterface) bool {
		tioevent := ev.(*TimedUcastEvent)
		tio := tioevent.extension.(*Tio)
		log(LOG_VV, "SRV rxcallback", tio.String())

		tio.doStage(r)

		return true
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

func (r *ServerFour) M4putrequest(ev EventInterface) error {
	log(LOG_VV, r.String(), "::M4putrequest()", ev.String())

	tioevent := ev.(*TimedUcastEvent)
	tio := tioevent.extension.(*Tio)

	at := clusterTripPlusRandom()
	tio.nextAnon(at, ev.GetSource())
	return nil
}

func (r *ServerFour) M4putxfer(ev EventInterface) error {
	log(LOG_VV, r.String(), "::M4putxfer()", ev.String())

	at := clusterTripPlusRandom()

	tioevent := ev.(*TimedUcastEvent)
	tio := tioevent.extension.(*Tio)
	tio.nextAnon(at, ev.GetSource())
	return nil
}

//==================================================================
//
// ModelFour methods
//
//==================================================================
//
// ModelFour interface methods
//
func (m *ModelFour) NewGateway(i int) RunnerInterface {
	gwy := &GatewayFour{RunnerBase{id: i, strtype: "GWY"}, int64(0)}
	gwy.init(config.numServers)
	return gwy
}

func (m *ModelFour) NewServer(i int) RunnerInterface {
	srv := &ServerFour{RunnerBase: RunnerBase{id: i, strtype: "SRV"}}
	srv.init(config.numGateways)
	return srv
}

func (m *ModelFour) NewDisk(i int) RunnerInterface { return nil }

func (m *ModelFour) Configure() {
	config.timeClusterTrip = time.Microsecond * 2
}
