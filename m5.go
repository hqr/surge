//
// ModelFive (m5, "5") is the very first realization of the UDP based
// (unicast) storage-clustering via consistent hashing.
//
// Full legal name of this model:
//   Unicast Consistent Hash distribution using Captive Congestion Point
//   (UCH-CCPi)
//
// The model implements a so called Captive Congestion Point logic,
// to rate-control clients by dividing the servers's bandwidth equally
// between all concurrent flows. The latter are getting added when
// consistent-hasher at a gateway selects a given target, etc.
//
// The gateways are further limited by their own network links. Each
// chunk replica is separately ACK-ed so that 3 (or configured) replica
// ACKs constitute a put-chunk.
//
// The UCH-CCPi pipeline includes 3 control events per each replica of
// each chunk, details in the code.
//
// TODO: configStorage.chunksInFlight
//       add support for multiple chunks in-flight, with gateways starting
//       to transmit without waiting for completions
//
package surge

import (
	"sync/atomic"
	"time"
)

// implements ModelInterface
type ModelFive struct {
	putpipeline *Pipeline
}

//========================================================================
// m5 nodes
//========================================================================
type GatewayFive struct {
	GatewayUch
}

type ServerFive struct {
	ServerUch
}

//
// static & init
//
var m5 ModelFive

func init() {
	p := NewPipeline()
	p.AddStage(&PipelineStage{name: "PUT-REQ", handler: "M5putrequest"})
	p.AddStage(&PipelineStage{name: "RATE-INIT", handler: "M5rateinit"})
	p.AddStage(&PipelineStage{name: "REPLICA-ACK", handler: "M5replicack"})

	m5.putpipeline = p

	d := NewStatsDescriptors("5")
	d.Register("event", StatsKindCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxbusy", StatsKindPercentage, StatsScopeServer)
	d.Register("tio", StatsKindCount, StatsScopeGateway)
	d.Register("chunk", StatsKindCount, StatsScopeGateway)
	d.Register("replica", StatsKindCount, StatsScopeGateway)
	d.Register("txbytes", StatsKindByteCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxbytes", StatsKindByteCount, StatsScopeServer|StatsScopeGateway)
	d.Register("disk-queue-depth", StatsKindSampleCount, StatsScopeServer)

	props := make(map[string]interface{}, 1)
	props["description"] = "UCH-CCPi: Unicast Consistent Hash distribution using Captive Congestion Point"
	RegisterModel("5", &m5, props)
}

//==================================================================
//
// GatewayFive methods
//
//==================================================================
func (r *GatewayFive) Run() {
	r.state = RstateRunning

	rxcallback := func(ev EventInterface) bool {
		atomic.AddInt64(&r.rxbytestats, int64(configNetwork.sizeControlPDU))

		switch ev.(type) {
		case *UchRateSetEvent:
			ratesetev := ev.(*UchRateSetEvent)
			log(LOG_V, "GWY::rxcallback:", ratesetev.String())
			r.rateset(ratesetev)
		default:
			srv := ev.GetSource()
			tio := ev.GetExtension().(*Tio)
			log(LOG_V, "GWY::rxcallback", tio.String())
			tio.doStage(r)
			if tio.done {
				log(LOG_V, "tio-done", tio.String())
				atomic.AddInt64(&r.tiostats, int64(1))
				r.finishStartReplica(srv, true)
			}
		}
		return true
	}

	go func() {
		lastRefill := Now
		for r.state == RstateRunning {
			if r.chunk == nil {
				if r.rb.above(int64(configNetwork.sizeControlPDU * 8)) {
					r.startNewChunk()
				}
			}
			// recv
			r.receiveEnqueue()
			r.processPendingEvents(rxcallback)

			if Now.After(lastRefill) {
				lastRefill = Now
				r.sendata()
			}
		}
		r.closeTxChannels()
	}()
}

func (r *GatewayFive) rateset(ev *UchRateSetEvent) {
	tio := ev.extension.(*Tio)
	assert(tio.source == r)

	if tio.done {
		return
	}
	flow := r.flowsto.get(ev.GetSource(), false)
	assert(flow != nil, "FATAL: gwy-rateset on non-existing flow:"+tio.String()+":"+ev.String())

	if flow.offset >= flow.totalbytes {
		return
	}
	assert(flow.tio == tio, flow.String()+":"+tio.String())
	if !flow.rateini || flow.ratects.Before(ev.GetCreationTime()) {
		flow.ratects = ev.GetCreationTime()
		flow.raterts = Now
		flow.rateini = true

		flow.tobandwidth = ev.tobandwidth
		flow.rb.setrate(flow.tobandwidth)

		log(LOG_V, "gwy-rateset", flow.String())
	}
}

//=========================
// GatewayFive TIO handlers
//=========================
func (r *GatewayFive) M5rateinit(ev EventInterface) error {
	tioevent := ev.(*UchRateInitEvent)
	log(LOG_V, r.String(), "::M5rateinit()", tioevent.String())
	srv := tioevent.GetSource()
	flow := r.flowsto.get(srv, true)
	assert(flow.cid == tioevent.cid)
	assert(flow.num == tioevent.num)
	if !flow.rateini {
		flow.ratects = tioevent.GetCreationTime()
		flow.raterts = Now
		flow.rateini = true

		flow.tobandwidth = tioevent.tobandwidth
		flow.rb.setrate(flow.tobandwidth)

		log(LOG_V, "gwy-rateinit", flow.String(), r.replica.String())
	} else {
		log(LOG_V, "gwy-rate-already-set", flow.String())
	}

	log(LOG_V, "gwy-rateinit", flow.String(), r.replica.String())

	return nil
}

func (r *GatewayFive) M5replicack(ev EventInterface) error {
	return r.replicack(ev)
}

//==================================================================
//
// ServerFive methods
//
//==================================================================
func (r *ServerFive) Run() {
	r.state = RstateRunning

	rxcallback := func(ev EventInterface) bool {
		switch ev.(type) {
		case *UchReplicaDataEvent:
			tioevent := ev.(*UchReplicaDataEvent)
			log(LOG_V, "SRV::rxcallback: replica data", tioevent.String())
			r.receiveReplicaData(tioevent)
		default:
			atomic.AddInt64(&r.rxbytestats, int64(configNetwork.sizeControlPDU))
			tio := ev.GetExtension().(*Tio)
			log(LOG_V, "SRV::rxcallback", tio.String())
			tio.doStage(r)
		}

		return true
	}

	go func() {
		numflows := r.flowsfrom.count()
		for r.state == RstateRunning {
			r.receiveEnqueue()
			r.processPendingEvents(rxcallback)

			if numflows != r.flowsfrom.count() {
				r.rerate()
				// r.rerateInverseProportional()
				numflows = r.flowsfrom.count()
			}
		}

		r.closeTxChannels()
	}()
}

//
// note: two possible implementations: rerate() and rerateInverseProportional()
//
func (r *ServerFive) rerate() {
	nflows := r.flowsfrom.count()
	if nflows == 0 {
		return
	}
	applyCallback := func(gwy RunnerInterface, flow *Flow) {
		bytesinflight := int64(flow.tobandwidth) * int64(config.timeClusterTrip) / int64(time.Second) / 8
		if flow.totalbytes-flow.offset <= configNetwork.sizeFrame+int(bytesinflight) {
			return
		}
		ratesetev := newUchRateSetEvent(r, gwy, configNetwork.linkbps/int64(nflows), flow.cid, flow.num)
		flow.tobandwidth = ratesetev.tobandwidth
		ratesetev.SetExtension(flow.tio)
		flow.ratects = Now
		flow.raterts = Now.Add(config.timeClusterTrip * 2)

		log(LOG_V, "srv-send-rateset", flow.String())
		r.Send(ratesetev, true)
		atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
	}
	r.flowsfrom.apply(applyCallback)
}

func (r *ServerFive) rerateInverseProportional() {
	nflows := r.flowsfrom.count()
	if nflows == 0 {
		return
	}
	totalrem := float64(0)
	fdir := r.flowsfrom
	for _, flow := range fdir.flows {
		rem := flow.totalbytes - flow.offset
		if rem <= configNetwork.sizeFrame {
			continue
		}
		totalrem += 1.0 / float64(rem)
	}
	for gwy, flow := range fdir.flows {
		rem := flow.totalbytes - flow.offset
		if rem <= configNetwork.sizeFrame {
			continue
		}

		newbwf := float64(configNetwork.linkbps) * (1.0 / float64(rem) / totalrem)
		newbw := int64(newbwf)
		ratesetev := newUchRateSetEvent(r, gwy, newbw, flow.cid, flow.num)
		flow.tobandwidth = newbw
		ratesetev.SetExtension(flow.tio)
		flow.ratects = Now
		flow.raterts = Now.Add(config.timeClusterTrip * 2)

		log(LOG_V, "srv-send-rateset-proportional", flow.String())
		r.Send(ratesetev, true)
		atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
	}
}

func (r *ServerFive) M5putrequest(ev EventInterface) error {
	log(LOG_V, r.String(), "::M5putrequest()", ev.String())

	tioevent := ev.(*UchReplicaPutRequestEvent)
	gwy := tioevent.GetSource()
	f := r.flowsfrom.get(gwy, false)
	assert(f == nil)

	tio := tioevent.extension.(*Tio)
	flow := NewFlow(gwy, r, tioevent.cid, tioevent.num, tio)
	flow.totalbytes = tioevent.sizeb
	flow.rateini = true
	flow.ratects = Now
	flow.raterts = Now.Add(config.timeClusterTrip * 2)
	r.flowsfrom.insertFlow(gwy, flow)

	nflows := r.flowsfrom.count()
	rateinitev := newUchRateInitEvent(r, gwy, configNetwork.linkbps/int64(nflows), flow.cid, flow.num)
	flow.tobandwidth = rateinitev.tobandwidth
	log("srv-new-flow", flow.String(), rateinitev.String())

	tio.next(rateinitev)
	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
	return nil
}

//==================================================================
//
// ModelFive interface methods
//
//==================================================================
func (m *ModelFive) NewGateway(i int) RunnerInterface {
	setflowratebucket := func(flow *Flow) { // maxval, rate, value, rateptr
		flow.rb = NewRateBucket(configNetwork.maxratebucketval, int64(0), configNetwork.maxratebucketval)
	}

	gwy := NewGatewayUch(i, m5.putpipeline, setflowratebucket)
	gwy.rb = NewRateBucket(
		configNetwork.maxratebucketval, // maxval
		configNetwork.linkbpsminus,     // rate
		configNetwork.maxratebucketval) // value
	rgwy := &GatewayFive{*gwy}
	rgwy.GatewayUch.rptr = rgwy
	return rgwy
}

func (m *ModelFive) NewServer(i int) RunnerInterface {
	srv := NewServerUch(i, m5.putpipeline)
	rsrv := &ServerFive{*srv}
	rsrv.ServerUch.rptr = rsrv
	return rsrv
}

func (m *ModelFive) NewDisk(i int) RunnerInterface { return nil }

func (m *ModelFive) Configure() {
	configNetwork.sizeControlPDU = 100
}
