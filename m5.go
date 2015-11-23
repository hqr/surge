// Package surge provides a framework for discrete event simulation, as well as
// a number of models for Unsolicited and Reservation Group based Edge-driven
// load balancing. Targeted modeling area includes large and super-large storage
// clusters with multiple access points (referred to as "gateways") and multiple
// storage targets (referred to as "servers").
//
// modelFive (m5, "5") is the very first realization of the UDP based
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
type modelFive struct {
	putpipeline *Pipeline
}

//========================================================================
// m5 nodes
//========================================================================
type gatewayFive struct {
	GatewayUch
}

type serverFive struct {
	ServerUch
}

//
// static & init
//
var m5 modelFive

// init initializes UCH-CCPi model. In particular, model-specific IO pipeline
// will contain 3 named stages paired with their respective stage handlers
// (callbacks) specified below as methods of the model's gateways and servers.
//
// The other part of initialization includes statistic counters this model
// supports; each counter has one of the enumerated "scopes" and "kinds" with
// generic support via stats.go module.
//
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
// gatewayFive methods
//
//==================================================================
// Run contains the gateway's receive callback and its goroutine. Each of the
// gateway instances (the running number of which is configured as
// config.numGateways) has a type gatewayFive and spends all its given runtime
// inside its own goroutine.
//
// As per rxcallback below, the gateway handles rate-changing event
// UchRateSetEvent that is asynchronous as far as the model's pipeline stages.
// The latter are executed as well via generic tio.doStage()
//
func (r *gatewayFive) Run() {
	r.state = RstateRunning

	rxcallback := func(ev EventInterface) bool {
		atomic.AddInt64(&r.rxbytestats, int64(configNetwork.sizeControlPDU))

		switch ev.(type) {
		case *UchRateSetEvent:
			ratesetev := ev.(*UchRateSetEvent)
			log(LogV, "GWY::rxcallback:", ratesetev.String())
			r.rateset(ratesetev)
		default:
			tio := ev.GetTio()
			log(LogV, "GWY::rxcallback", tio.String())
			tio.doStage(r)
			if tio.done {
				log(LogV, "tio-done", tio.String())
				atomic.AddInt64(&r.tiostats, int64(1))
			}
		}
		return true
	}

	go func() {
		lastRefill := Now
		for r.state == RstateRunning {
			if r.chunk == nil {
				// the gateway does currently one chunk at a time;
				// configStorage.chunksInFlight > 1 is not supported yet
				//
				// if there no chunk in flight (r.chunk == nil)
				// we must make sure the gateway's rate bucket has
				// at least sizeControlPDU bits to send the new PUT..
				if r.rb.above(int64(configNetwork.sizeControlPDU * 8)) {
					r.startNewChunk()
				}
			}
			// recv processing is always two steps: collect new events
			// from all the Rx channels of this gateway, and process
			// those which time has arrived, via the gateway's rxcallback
			// (above)
			r.receiveEnqueue()
			r.processPendingEvents(rxcallback)

			// the gateway's transmit side uses common GatewayUch method
			// to send replica data if and when appropriate
			if Now.After(lastRefill) {
				lastRefill = Now
				r.sendata()
			}
		}
		r.closeTxChannels()
	}()
}

// rateset handles the namesake event from a rate-setting server, which for
// the UCH-CCPi model boils down to setting the prescribed rate on the
// corresponding active flow (to this server).
// The new rate is delegated to the flow's own rate bucket, which in response
// will start filling up slower or faster, depending on the rate..
//
func (r *gatewayFive) rateset(tioevent *UchRateSetEvent) {
	tio := tioevent.GetTio()
	assert(tio.source == r)

	if tio.done {
		return
	}
	flow := r.flowsto.get(tioevent.GetSource(), false)
	assert(flow != nil, "FATAL: gwy-rateset on non-existing flow:"+tio.String()+":"+tioevent.String())

	if flow.offset >= flow.totalbytes {
		return
	}
	assert(flow.tio == tio, flow.String()+":"+tio.String())
	if !flow.rateini || flow.ratects.Before(tioevent.GetCreationTime()) {
		flow.ratects = tioevent.GetCreationTime()
		flow.raterts = Now
		flow.rateini = true

		flow.tobandwidth = tioevent.tobandwidth
		flow.rb.setrate(flow.tobandwidth)

		log(LogV, "gwy-rateset", flow.String())
	}
}

//=========================
// gatewayFive TIO handlers
//=========================
func (r *gatewayFive) M5rateinit(ev EventInterface) error {
	tioevent := ev.(*UchRateInitEvent)
	log(LogV, r.String(), "::M5rateinit()", tioevent.String())
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

		log(LogV, "gwy-rateinit", flow.String(), r.replica.String())
	} else {
		log(LogV, "gwy-rate-already-set", flow.String())
	}

	log(LogV, "gwy-rateinit", flow.String(), r.replica.String())

	return nil
}

func (r *gatewayFive) M5replicack(ev EventInterface) error {
	return r.replicack(ev)
}

//
// flow factory interface impl - notice model-specific ratebucket
//
func (r *gatewayFive) newflow(t interface{}, repnum int) *Flow {
	tgt := t.(RunnerInterface)
	tio := r.putpipeline.NewTio(r)
	flow := NewFlow(r, tgt, r.chunk.cid, repnum, tio)
	flow.tobandwidth = int64(0) // transmit upon further notice
	flow.totalbytes = r.chunk.sizeb

	flow.rb = NewRateBucket(configNetwork.maxratebucketval, int64(0), configNetwork.maxratebucketval)

	r.flowsto.insertFlow(tgt, flow)
	return flow
}

//==================================================================
//
// serverFive methods
//
//==================================================================
func (r *serverFive) Run() {
	r.state = RstateRunning

	rxcallback := func(ev EventInterface) bool {
		switch ev.(type) {
		case *UchReplicaDataEvent:
			tioevent := ev.(*UchReplicaDataEvent)
			log(LogV, "SRV::rxcallback: replica data", tioevent.String())
			r.receiveReplicaData(tioevent)
		default:
			atomic.AddInt64(&r.rxbytestats, int64(configNetwork.sizeControlPDU))
			tio := ev.GetTio()
			log(LogV, "SRV::rxcallback", tio.String())
			tio.doStage(r)
		}

		return true
	}

	go func() {
		numflows := r.flowsfrom.count()
		for r.state == RstateRunning {
			r.receiveEnqueue()
			r.processPendingEvents(rxcallback)

			// two alternative CCPi-implementing methods below,
			// one simply dividing the server's bandwidth equally between
			// all incoming flows, another - trying the weighted approach,
			// with weights inverse proportional to the remaining bytes
			// to send..
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
func (r *serverFive) rerate() {
	nflows := r.flowsfrom.count()
	if nflows == 0 {
		return
	}
	applyCallback := func(gwy RunnerInterface, flow *Flow) {
		bytesinflight := int64(flow.tobandwidth) * int64(config.timeClusterTrip) / int64(time.Second) / 8
		if flow.totalbytes-flow.offset <= configNetwork.sizeFrame+int(bytesinflight) {
			return
		}
		ratesetev := newUchRateSetEvent(r, gwy, configNetwork.linkbps/int64(nflows), flow.cid, flow.num, flow.tio)
		flow.tobandwidth = ratesetev.tobandwidth
		flow.ratects = Now
		flow.raterts = Now.Add(config.timeClusterTrip * 2)

		log(LogV, "srv-send-rateset", flow.String())
		r.Send(ratesetev, SmethodWait)
		atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
	}
	r.flowsfrom.apply(applyCallback)
}

func (r *serverFive) rerateInverseProportional() {
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
		ratesetev := newUchRateSetEvent(r, gwy, newbw, flow.cid, flow.num, flow.tio)
		flow.tobandwidth = newbw
		flow.ratects = Now
		flow.raterts = Now.Add(config.timeClusterTrip * 2)

		log(LogV, "srv-send-rateset-proportional", flow.String())
		r.Send(ratesetev, SmethodWait)
		atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
	}
}

func (r *serverFive) M5putrequest(ev EventInterface) error {
	log(LogV, r.String(), "::M5putrequest()", ev.String())

	tioevent := ev.(*UchReplicaPutRequestEvent)
	gwy := tioevent.GetSource()
	f := r.flowsfrom.get(gwy, false)
	assert(f == nil)

	//new server's flow
	tio := tioevent.GetTio()
	flow := NewFlow(gwy, r, tioevent.cid, tioevent.num, tio)
	flow.totalbytes = tioevent.sizeb
	flow.rateini = true
	flow.ratects = Now
	flow.raterts = Now.Add(config.timeClusterTrip * 2)
	r.flowsfrom.insertFlow(gwy, flow)

	// respond to the put witn RateInit
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
// modelFive interface methods
//
//==================================================================
func (m *modelFive) NewGateway(i int) RunnerInterface {
	gwy := NewGatewayUch(i, m5.putpipeline)
	gwy.rb = NewRateBucket(
		configNetwork.maxratebucketval, // maxval
		configNetwork.linkbpsminus,     // rate
		configNetwork.maxratebucketval) // value
	rgwy := &gatewayFive{*gwy}
	rgwy.rptr = rgwy
	rgwy.ffi = rgwy
	rgwy.flowsto = NewFlowDir(rgwy, config.numServers)
	return rgwy
}

func (m *modelFive) NewServer(i int) RunnerInterface {
	srv := NewServerUch(i, m5.putpipeline)
	rsrv := &serverFive{*srv}
	rsrv.ServerUch.rptr = rsrv
	return rsrv
}

func (m *modelFive) Configure() {
	configNetwork.sizeControlPDU = 100
}
