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
// The model implements a so called Captive Congestion Point (CCP) logic,
// to rate-control clients by dividing (edge) servers's bandwidth
// equally between all concurrent flows. CCP by definition is
// a network congestion point with a single salient property:
// it is known to be the one and only source of congestion in an
// end-to-end flow.
// Thus, UCH-CCPi, as the name implies, models a storage cluster of
// arbitrary (configured) size where each server tries to manage its
// own CCP by equally dividing its receive bandwidth.
//
// The gateways are further limited by their own network links. Each
// replica of each chunk is separately ACK-ed, so that 3 (or configured)
// replica ACKs constitute a fully stored chunk.
//
// The UCH-CCPi pipeline includes 3 control events per each replica of
// each chunk, details in the code.
//
package surge

import (
	"fmt"
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
	d.registerCommonProtoStats()

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

	rxcallback := func(ev EventInterface) int {
		switch ev.(type) {
		case *UchRateSetEvent:
			ratesetev := ev.(*UchRateSetEvent)
			log(LogV, "rxcallback:", r.String(), ratesetev.String())
			r.rateset(ratesetev)
		default:
			tio := ev.GetTio().(*Tio)
			log(LogV, "rxcallback", r.String(), tio.String())
			tio.doStage(r)
			if tio.Done() {
				log(LogV, "tio-done", tio.String())
				atomic.AddInt64(&r.tiostats, 1)
			}
		}
		return ev.GetSize()
	}

	go func() {
		lastRefill := Now
		for r.state == RstateRunning {
			if r.chunk == nil {
				// make sure the gateway's rate bucket has
				// at least sizeControlPDU bits to send
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
	tio := tioevent.GetTio().(*Tio)
	assert(tio.GetSource() == r)

	if tio.Done() {
		return
	}
	assert(tio.cid == r.chunk.cid)
	flow := tio.GetFlow().(*Flow)
	flex := flow.extension.(*m5FlowExtension)
	assert(flow != nil, "FATAL: gwy-rateset on non-existing flow:"+tio.String()+":"+tioevent.String())

	if flow.getoffset() >= flow.totalbytes {
		return
	}
	assert(flow.GetTio() == tio, flow.String()+":"+tio.String())
	if !flex.rateini {
		assert(flow.getbw() == 0)
		flex.tobandwidth = tioevent.tobandwidth
		log("gwy-rateset-delayed", tioevent.String())
	} else {
		flow.setbw(tioevent.tobandwidth)
		flow.GetRb().setrate(flow.getbw())
		log(LogV, "gwy-rateset", flow.String())
	}
}

//=========================
// gatewayFive TIO handlers
//=========================
func (r *gatewayFive) M5rateinit(ev EventInterface) error {
	tioevent := ev.(*UchRateInitEvent)
	log(LogV, r.String(), "::M5rateinit()", tioevent.String())

	tio := tioevent.GetTio().(*Tio)
	flow := tio.GetFlow().(*Flow)
	assert(tio.GetSource() == r)
	assert(tio.GetCid() == r.chunk.cid)
	assert(flow.GetCid() == tioevent.cid)
	assert(flow.GetRepnum() == tioevent.num)
	assert(flow.getbw() == 0)

	flex := flow.extension.(*m5FlowExtension)
	assert(!flex.rateini)
	flex.rateini = true

	flow.setbw(tioevent.tobandwidth)
	if flex.tobandwidth > 0 {
		flow.setbw(flex.tobandwidth)
		t := tioevent.GetTriggerTime().Sub(time.Time{})
		log("gwy-rateinit-delayed", t, flow.String(), r.replica.String())
	} else {
		log(LogV, "gwy-rateinit", flow.String(), r.replica.String())
	}
	flow.GetRb().setrate(flow.getbw())

	return nil
}

func (r *gatewayFive) M5replicack(ev EventInterface) error {
	return r.replicack(ev)
}

//
// flow factory interface impl - notice model-specific ratebucket
//
func (r *gatewayFive) newflow(t interface{}, args ...interface{}) *Flow {
	tgt := t.(NodeRunnerInterface)
	repnum := args[0].(int)
	assert(repnum == r.replica.num)

	tio := NewTio(r, r.putpipeline, r.replica, tgt)
	flow := NewFlow(r, r.chunk.cid, tgt, repnum, tio)
	flow.extension = &m5FlowExtension{false, 0}

	flow.setbw(0) // transmit upon further notice
	flow.totalbytes = r.chunk.sizeb
	flow.rb = NewRateBucket(configNetwork.maxratebucketval, 0, configNetwork.maxratebucketval)

	return flow
}

//==================================================================
//
// serverFive methods
//
//==================================================================
func (r *serverFive) Run() {
	r.state = RstateRunning

	rxcallback := func(ev EventInterface) int {
		switch ev.(type) {
		case *ReplicaDataEvent:
			tioevent := ev.(*ReplicaDataEvent)
			log(LogVV, "rxcallback: replica data", r.String(), tioevent.String())
			r.receiveReplicaData(tioevent)
		default:
			tio := ev.GetTio()
			log(LogV, "rxcallback", r.String(), tio.String())
			// ev.GetSize() == configNetwork.sizeControlPDU
			r.addBusyDuration(configNetwork.sizeControlPDU, configNetwork.linkbpsControl, NetControlBusy)
			tio.doStage(r)
		}

		return ev.GetSize()
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
			nflows := r.flowsfrom.count()
			if numflows != nflows && nflows > 0 {
				r.rerate(nflows)
				// r.rerateInverseProportional()
			}
			numflows = nflows
		}

		r.closeTxChannels()
	}()
}

//
// note: two possible implementations: rerate() and rerateInverseProportional()
//
func (r *serverFive) rerate(nflows int) {
	applyCallback := func(gwy NodeRunnerInterface, flowint FlowInterface) {
		flow := flowint.(*Flow)
		bytesinflight := flow.getbw() * int64(config.timeClusterTrip) / int64(time.Second) / 8
		if flow.totalbytes-flow.getoffset() <= int64(configNetwork.sizeFrame+int(bytesinflight)) {
			return
		}
		ratesetev := newUchRateSetEvent(r, gwy, configNetwork.linkbps/int64(nflows), flow, flow.GetTio())
		flow.setbw(ratesetev.tobandwidth)

		log(LogV, "srv-send-rateset", flow.String())
		r.Send(ratesetev, SmethodWait)
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
	for _, flowint := range fdir.flows {
		flow := flowint.(*Flow)
		rem := flow.totalbytes - flow.getoffset()
		if rem <= int64(configNetwork.sizeFrame) {
			continue
		}
		totalrem += 1.0 / float64(rem)
	}
	for gwy, flowint := range fdir.flows {
		flow := flowint.(*Flow)
		rem := flow.totalbytes - flow.getoffset()
		if rem <= int64(configNetwork.sizeFrame) {
			continue
		}

		newbwf := float64(configNetwork.linkbps) * (1.0 / float64(rem) / totalrem)
		newbw := int64(newbwf)
		ratesetev := newUchRateSetEvent(r, gwy, newbw, flow, flow.GetTio())
		flow.setbw(newbw)

		log(LogV, "srv-send-rateset-proportional", flow.String())
		r.Send(ratesetev, SmethodWait)
	}
}

func (r *serverFive) M5putrequest(ev EventInterface) error {
	tioevent := ev.(*ReplicaPutRequestEvent)
	log(LogV, r.String(), "::M5putrequest()", tioevent.String())

	tio := tioevent.GetTio()
	gwy := tioevent.GetSource()

	// assuming (! FIXME) the new chunk has already arrived,
	// compute disk queue delay with respect to the configured maxDiskQueueChunks
	diskIOdone := r.disk.lastIOdone
	delay := diskdelay(Now, diskIOdone)

	f := r.flowsfrom.get(gwy, false)
	assert(f == nil, fmt.Sprintf("flow %s => %s already exists", gwy.String(), r.String()))

	//new server's flow
	flow := NewFlow(gwy, tioevent.cid, r, tioevent.num, tio)
	flow.extension = &m5FlowExtension{true, 0}
	flow.totalbytes = tioevent.sizeb
	r.flowsfrom.insertFlow(flow)

	// respond to the put witn RateInit
	nflows := r.flowsfrom.count()
	rateinitev := newUchRateInitEvent(r, gwy, configNetwork.linkbps/int64(nflows), flow, tio, delay)
	flow.setbw(rateinitev.tobandwidth)
	if delay > 0 {
		log("srv-new-delayed-flow", flow.String(), rateinitev.String(), delay)
	} else {
		log("srv-new-flow", flow.String(), rateinitev.String())
	}

	tio.next(rateinitev, SmethodWait)
	return nil
}

//==================================================================
//
// modelFive interface methods
//
//==================================================================
func (m *modelFive) NewGateway(i int) NodeRunnerInterface {
	gwy := NewGatewayUch(i, m5.putpipeline)
	gwy.rb = NewRateBucket(
		configNetwork.maxratebucketval, // maxval
		configNetwork.linkbps,          // rate
		configNetwork.maxratebucketval) // value
	rgwy := &gatewayFive{*gwy}
	rgwy.rptr = rgwy
	rgwy.ffi = rgwy
	return rgwy
}

func (m *modelFive) NewServer(i int) NodeRunnerInterface {
	srv := NewServerUchRegChannels(i, m5.putpipeline)

	// receive side ratebucket use()-d directly by remote senders
	srv.rb = NewRateBucketProtected(
		configNetwork.maxratebucketval, // maxval
		configNetwork.linkbps,          // rate
		configNetwork.maxratebucketval) // value

	rsrv := &serverFive{*srv}
	rsrv.ServerUch.rptr = rsrv
	rsrv.flowsfrom = NewFlowDir(rsrv, config.numGateways)
	return rsrv
}

func (m *modelFive) Configure() {
	configNetwork.sizeControlPDU = 100
	configNetwork.durationControlPDU = time.Duration(configNetwork.sizeControlPDU*8) * time.Second / time.Duration(configNetwork.linkbps)
}

//==================================================================
//
// model-specific events
//
//==================================================================
type UchRateSetEvent struct {
	zControlEvent
	tobandwidth int64 // bits/sec
	num         int   // replica num
}

func newUchRateSetEvent(srv NodeRunnerInterface, gwy NodeRunnerInterface, rate int64, flow FlowInterface, tio TioInterface, args ...interface{}) *UchRateSetEvent {
	var diskdelay time.Duration
	if len(args) > 0 {
		diskdelay = args[0].(time.Duration)
	}
	at := configNetwork.durationControlPDU + config.timeClusterTrip + diskdelay
	timedev := newTimedAnyEvent(srv, at, gwy, tio, configNetwork.sizeControlPDU)

	return &UchRateSetEvent{zControlEvent{zEvent{*timedev}, flow.GetCid()}, rate, flow.GetRepnum()}
}

func (e *UchRateSetEvent) String() string {
	printid := uqrand(e.cid)
	bwstr := fmt.Sprintf("%.2fGbps", float64(e.tobandwidth)/1000.0/1000.0/1000.0)
	return fmt.Sprintf("[RSE %v=>%v,c#%d(%d),%s]", e.source.String(), e.target.String(), printid, e.num, bwstr)
}

type UchRateInitEvent struct {
	UchRateSetEvent
}

func newUchRateInitEvent(srv NodeRunnerInterface, gwy NodeRunnerInterface, rate int64, flow FlowInterface, tio TioInterface, diskdelay time.Duration) *UchRateInitEvent {
	ev := newUchRateSetEvent(srv, gwy, rate, flow, tio, diskdelay)
	return &UchRateInitEvent{*ev}
}

func (e *UchRateInitEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[RIE %v=>%v,c#%d(%d)]", e.source.String(), e.target.String(), printid, e.num)
}

//==================================================================
//
// Flow extension
//
//==================================================================
type m5FlowExtension struct {
	rateini     bool // rateset inited
	tobandwidth int64
}
