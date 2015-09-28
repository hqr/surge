//
// ModelFour (m4, "four") is a step-up in terms of layering:
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
	p.AddStage(&PipelineStage{name: "PUT-REQ", handler: "Putrequest"})
	p.AddStage(&PipelineStage{name: "PUT-REQ-ACK", handler: "Putreqack"})
	p.AddStage(&PipelineStage{name: "PUT-XFER", handler: "Putxfer"})
	p.AddStage(&PipelineStage{name: "PUT-XFER-ACK", handler: "Putxferack"})

	m4.pipeline = p

	d := NewStatsDescriptors("four")
	d.Register("event", StatsKindCount, StatsScopeGateway|StatsScopeServer)
	d.Register("busy", StatsKindPercentage, StatsScopeGateway|StatsScopeServer)
	d.Register("tio", StatsKindCount, StatsScopeGateway)

	props := make(map[string]interface{}, 1)
	props["description"] = "TIO pipeline in action"
	RegisterModel("four", &m4, props)
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
		tioevent := ev.(*TimedTioEvent)
		tio := tioevent.tio
		log(LOG_VV, "GWY rxcallback", tio.String())
		tio.doStage(r)

		if tio.done {
			r.tiostats++
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
				tio.next(r, at, tgt)
				time.Sleep(time.Microsecond)
			}

			// recv
			r.receiveAndHandle(rxcallback)
			time.Sleep(time.Microsecond)
			r.processPendingEvents(rxcallback)
			time.Sleep(time.Microsecond)
		}

		r.closeTxChannels()
	}()
}

func (r *GatewayFour) Putreqack(ev EventInterface) error {
	log(LOG_VV, r.String(), "::Putreqack()", ev.String())

	at := clusterTripPlusRandom()
	tioevent := ev.(*TimedTioEvent)
	tioevent.tio.next(r, at, ev.GetSource())
	return nil
}

func (r *GatewayFour) Putxferack(ev EventInterface) error {
	log(LOG_VV, r.String(), "::Putxferack()", ev.String())

	return nil
}

func (r *GatewayFour) GetStats(reset bool) NodeStats {
	tiostatscopy := r.tiostats

	// add and reset..
	s := r.RunnerBase.GetStats(true)
	s["tio"] = tiostatscopy
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
		tioevent := ev.(*TimedTioEvent)
		log(LOG_VV, "SRV rxcallback", tioevent.tio.String())

		tio := tioevent.tio
		tio.doStage(r)

		return true
	}

	go func() {
		for r.state == RstateRunning {
			// recv
			r.receiveAndHandle(rxcallback)

			time.Sleep(time.Microsecond)
			r.processPendingEvents(rxcallback)
		}

		r.closeTxChannels()
	}()
}

func (r *ServerFour) Putrequest(ev EventInterface) error {
	log(LOG_VV, r.String(), "::Putrequest()", ev.String())

	at := clusterTripPlusRandom()
	tioevent := ev.(*TimedTioEvent)
	tioevent.tio.next(r, at, ev.GetSource())
	return nil
}

func (r *ServerFour) Putxfer(ev EventInterface) error {
	log(LOG_VV, r.String(), "::Putxfer()", ev.String())

	at := clusterTripPlusRandom()
	tioevent := ev.(*TimedTioEvent)
	tioevent.tio.next(r, at, ev.GetSource())
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

	// common reset..
	gwy.addStatsPtr(&gwy.tiostats)
	return gwy
}

func (m *ModelFour) NewServer(i int) RunnerInterface {
	srv := &ServerFour{RunnerBase: RunnerBase{id: i, strtype: "SRV"}}
	srv.init(config.numGateways)
	return srv
}

func (m *ModelFour) NewDisk(i int) RunnerInterface { return nil }
