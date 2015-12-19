// Package surge provides a framework for discrete event simulation, as well as
// a number of models for Unsolicited and Reservation Group based Edge-driven
// load balancing. Targeted modeling area includes large and super-large storage
// clusters with multiple access points (referred to as "gateways") and multiple
// storage targets (referred to as "servers").
//
// modelSix (m6, "6"), aka UCH-AIMD:
// Unicast Consistent Hash distribution using AIMD
//
// AIMD or Additive Increase/Multiplicative Decrease
// is a well known and extremely well reseached congestion control
// mechanism that has got its wide-spread popularity the most part
// due to TCP:
//
// https://en.wikipedia.org/wiki/Additive_increase/multiplicative_decrease
//
//
package surge

import (
	"fmt"
	"sync/atomic"
	"time"
)

type modelSix struct {
	putpipeline *Pipeline
}

//========================================================================
// m6 nodes
//========================================================================
type gatewaySix struct {
	GatewayUch
}

type serverSix struct {
	ServerUch
}

//
// static & init
//
var m6 modelSix
var timeFrameFullLink time.Duration // time to receive a data frame

// init initializes UCH-AIMD model. In particular, model-specific IO pipeline
// will contain 3 named stages paired with their respective stage handlers
// (callbacks) specified below as methods of the model's gateways and servers.
//
// The other part of initialization includes statistic counters this model
// supports; each counter has one of the enumerated "scopes" and "kinds" with
// generic support via stats.go module.
//
func init() {
	p := NewPipeline()
	p.AddStage(&PipelineStage{name: "PUT-REQ", handler: "M6putrequest"})
	p.AddStage(&PipelineStage{name: "PUT-REQ-ACK", handler: "M6putreqack"})
	p.AddStage(&PipelineStage{name: "REPLICA-ACK", handler: "M6replicack"})

	m6.putpipeline = p

	d := NewStatsDescriptors("6")
	d.Register("event", StatsKindCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxidle", StatsKindPercentage, StatsScopeServer)
	d.Register("tio", StatsKindCount, StatsScopeGateway)
	d.Register("chunk", StatsKindCount, StatsScopeGateway)
	d.Register("replica", StatsKindCount, StatsScopeGateway)
	d.Register("txbytes", StatsKindByteCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxbytes", StatsKindByteCount, StatsScopeServer|StatsScopeGateway)
	d.Register("disk-queue-depth", StatsKindSampleCount, StatsScopeServer)

	props := make(map[string]interface{}, 1)
	props["description"] = "UCH-AIMD: Unicast Consistent Hash distribution using AIMD congestion control"
	RegisterModel("6", &m6, props)

	timeFrameFullLink = time.Duration(configNetwork.sizeFrame*8) * time.Second / time.Duration(configNetwork.linkbps)
}

//==================================================================
//
// gatewaySix methods
//
//==================================================================
// Run contains the gateway's receive callback and its goroutine. Each of the
// gateway instances (the running number of which is configured as
// config.numGateways) has a type gatewayFile and spends all its given runtime
// inside its own goroutine.
//
// As per rxcallback below, the gateway handles congestion notification "ding"
// that is asynchronous as far as the model's pipeline stages.
// The latter are executed as well via generic tio.doStage()
//
func (r *gatewaySix) Run() {
	r.state = RstateRunning

	rxcallback := func(ev EventInterface) int {
		switch ev.(type) {
		case *UchDingAimdEvent:
			dingev := ev.(*UchDingAimdEvent)
			log(LogV, "GWY::rxcallback", dingev.String())
			r.ding(dingev)
		default:
			tio := ev.GetTio()
			log(LogV, "GWY::rxcallback", tio.String())
			tio.doStage(r)
			if tio.done {
				log(LogV, "tio-done", tio.String())
				atomic.AddInt64(&r.tiostats, int64(1))
			}
		}
		return ev.GetSize()
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

// ding handles (reception of the) UchDingAimdEvent. which boils down to
// "dinging" the corresponding rate bucket, which in turn performs the
// multiplicative-decrease step on itself.
// Note that RateBucketAIMD is used here for the gateways flows..
//
func (r *gatewaySix) ding(dingev *UchDingAimdEvent) {
	tio := dingev.GetTio()
	assert(tio.source == r)

	flow := tio.flow
	assert(flow != nil, "FATAL: gwy-ding non-existing flow:"+tio.String()+":"+dingev.String())
	if flow.offset >= flow.totalbytes {
		return
	}
	log("gwy-got-dinged", flow.String())
	rb := flow.rb.(*RateBucketAIMD)
	rb.ding()
	flow.tobandwidth = rb.getrate()
}

//=========================
// gatewaySix TIO handlers - as per the pipeline declared above
//=========================
func (r *gatewaySix) M6putreqack(ev EventInterface) error {
	tioevent := ev.(*ReplicaPutRequestAckEvent)
	log(LogV, r.String(), "::M6putreqack()", tioevent.String())

	tio := tioevent.GetTio()
	assert(tio.source == r)

	flow := tio.flow
	assert(flow != nil, "FATAL: M6putreqack non-existing flow:"+tio.String()+":"+tioevent.String())
	assert(flow.cid == tio.cid)

	flow.tobandwidth = flow.rb.getrate()
	return nil
}

func (r *gatewaySix) M6replicack(ev EventInterface) error {
	return r.replicack(ev)
}

//
// flow factory interface impl - notice model-specific ratebucket
//
func (r *gatewaySix) newflow(t interface{}, args ...interface{}) *Flow {
	tgt := t.(RunnerInterface)
	repnum := args[0].(int)
	assert(repnum == r.replica.num)

	tio := r.putpipeline.NewTio(r, r.replica, tgt)
	flow := NewFlow(r, r.chunk.cid, tgt, repnum, tio)

	flow.tobandwidth = 0 // transmit upon further notice
	flow.totalbytes = r.chunk.sizeb

	flow.rb = NewRateBucketAIMD(
		configAIMD.bwMinInitialAdd,     // minrate
		configNetwork.linkbps,          // maxrate
		configNetwork.maxratebucketval, // maxval
		configAIMD.bwDiv)               // Multiplicative divisor
	return flow
}

//==================================================================
//
// serverSix methods
//
//==================================================================
// Run contains the server's receive callback and the server's goroutine.
// Each of the servers (the running number of which is configured as
// config.numServers) has a type serverSix and spends all its given runtime
// inside its own goroutine.
//
// As per rxcallback below, the UCH-AIMD server handles replica data and
// control PDUs from the client gateways.
//
func (r *serverSix) Run() {
	r.state = RstateRunning

	rxcallback := func(ev EventInterface) int {
		switch ev.(type) {
		case *ReplicaDataEvent:
			dataev := ev.(*ReplicaDataEvent)
			log(LogV, "SRV::rxcallback: replica data", dataev.String())
			gwy := ev.GetSource()
			flow := r.flowsfrom.get(gwy, true)
			flow.tobandwidth = dataev.tobandwidth
			r.receiveReplicaData(dataev)
		default:
			tio := ev.GetTio()
			log(LogV, "SRV::rxcallback", tio.String())
			tio.doStage(r)
		}

		return ev.GetSize()
	}

	go func() {
		lastAimdCheck := Now
		for r.state == RstateRunning {
			r.receiveEnqueue()
			r.processPendingEvents(rxcallback)

			// two alternative AIMD-implementing methods below
			// one based on walking the receive queue, another taking
			// into account gateway-reported bandwidths
			// We space in time the ding-generating checks by at least
			// the time to receive
			// a full data frame plus 1.5 roundtrip times
			if Now.Sub(lastAimdCheck) > (timeFrameFullLink + config.timeClusterTrip*3) {
				r.aimdCheckRxQueueFuture()
				// r.aimdCheckTotalBandwidth()
				lastAimdCheck = Now
			}
		}

		r.closeTxChannels()
	}()
}

func (r *serverSix) aimdCheckRxQueueFuture() {
	var gwy RunnerInterface
	linktime := TimeNil
	linkoverage := 0
	dingall := false
	frsize := configNetwork.sizeFrame
	if r.disk.queue.NumPending() > int64(configAIMD.diskoverage) {
		dingall = true
		log("srv-dingall", r.String())
	}

	r.rxqueue.lock()
	for k := 0; k < len(r.rxqueue.pending); k++ {
		ev := r.rxqueue.pending[k]
		_, ok := ev.(*ReplicaDataEvent)
		if !ok {
			continue
		}
		if dingall {
			gwy = ev.GetSource()
			flow := r.flowsfrom.get(gwy, true)
			r.dingOne(gwy)
			flow.extension = flow.offset + frsize // prevoffset
			continue
		}
		t := ev.GetTriggerTime()
		if linktime.Equal(TimeNil) {
			linktime = t
			gwy = ev.GetSource()
			continue
		}
		if t.Sub(linktime) <= timeFrameFullLink {
			linkoverage++
		}
		linktime = t
	}
	r.rxqueue.unlock()

	if dingall {
		return
	}
	if linkoverage >= configAIMD.linkoverage {
		flow := r.flowsfrom.get(gwy, true)
		prevoffset := flow.extension.(int)
		if flow.offset+3*frsize < flow.totalbytes && prevoffset+frsize < flow.offset {
			r.dingOne(gwy)
			flow.extension = flow.offset + frsize // prevoffset
		}
	}
}

func (r *serverSix) aimdCheckTotalBandwidth() {
	nflows := 0
	totalcurbw := int64(0)
	fdir := r.flowsfrom
	dingall := false
	frsize := configNetwork.sizeFrame

	if r.disk.queue.NumPending() > int64(configAIMD.diskoverage) {
		dingall = true
		log("srv-dingall", r.String())
	}

	for gwy, flow := range fdir.flows {
		if flow.totalbytes-flow.offset < 2*configNetwork.sizeFrame {
			continue
		}
		if dingall {
			r.dingOne(gwy)
			flow.extension = flow.offset + frsize // prevoffset
			continue
		}
		totalcurbw += flow.tobandwidth
		nflows++
	}
	if dingall || nflows <= 1 {
		return
	}

	totalfutbw := totalcurbw + configAIMD.bwMinInitialAdd*int64(nflows)
	if totalfutbw <= configNetwork.linkbps {
		return
	}
	// do some dinging and keep computing the resulting bw while doing so
	for gwy, flow := range fdir.flows {
		if flow.totalbytes-flow.offset < 2*configNetwork.sizeFrame {
			continue
		}
		prevoffset := flow.extension.(int)
		if flow.offset+3*frsize < flow.totalbytes && prevoffset+frsize < flow.offset {
			r.dingOne(gwy)
			flow.extension = flow.offset + frsize // prevoffset
			totalfutbw -= flow.tobandwidth / int64(configAIMD.bwDiv)
			if totalfutbw <= configNetwork.linkbps {
				break
			}
		}
	}
}

func (r *serverSix) dingOne(gwy RunnerInterface) {
	flow := r.flowsfrom.get(gwy, true)
	dingev := newUchDingAimdEvent(r, gwy, flow, flow.tio)

	log("srv-send-ding", flow.String())
	r.Send(dingev, SmethodWait)
}

func (r *serverSix) M6putrequest(ev EventInterface) error {
	log(LogV, r.String(), "::M6putrequest()", ev.String())

	tioevent := ev.(*ReplicaPutRequestEvent)
	gwy := tioevent.GetSource()
	f := r.flowsfrom.get(gwy, false)
	assert(f == nil)

	//new server's flow
	tio := tioevent.GetTio()
	flow := NewFlow(gwy, tioevent.cid, r, tioevent.num, tio)
	flow.extension = int(0) // prevoffset
	flow.totalbytes = tioevent.sizeb
	r.flowsfrom.insertFlow(flow)

	// respond to the put-request
	putreqackev := newReplicaPutRequestAckEvent(r, gwy, flow, tio)
	log("srv-new-flow", flow.String(), putreqackev.String())

	tio.next(putreqackev)
	return nil
}

//==================================================================
//
// modelSix interface methods
//
//==================================================================
func (m *modelSix) NewGateway(i int) RunnerInterface {
	gwy := NewGatewayUch(i, m6.putpipeline)
	gwy.rb = NewRateBucket(
		configNetwork.maxratebucketval, // maxval
		configNetwork.linkbps,          // rate
		configNetwork.maxratebucketval) // value
	rgwy := &gatewaySix{*gwy}
	rgwy.GatewayUch.rptr = rgwy
	rgwy.ffi = rgwy
	return rgwy
}

func (m *modelSix) NewServer(i int) RunnerInterface {
	srv := NewServerUch(i, m6.putpipeline)

	// receive side ratebucket use()-d directly by remote senders
	srv.rb = NewRateBucketProtected(
		configNetwork.maxratebucketval, // maxval
		configNetwork.linkbps,          // rate
		configNetwork.maxratebucketval) // value

	rsrv := &serverSix{*srv}
	rsrv.ServerUch.rptr = rsrv
	rsrv.flowsfrom = NewFlowDir(rsrv, config.numGateways)
	return rsrv
}

func (m *modelSix) Configure() {
	configNetwork.sizeControlPDU = 100
	configNetwork.durationControlPDU = time.Duration(configNetwork.sizeControlPDU*8) * time.Second / time.Duration(configNetwork.linkbps)
}

//==================================================================
//
// modelSix events
//
//==================================================================
type UchDingAimdEvent struct {
	zControlEvent
	num int // replica num
}

func newUchDingAimdEvent(srv RunnerInterface, gwy RunnerInterface, flow *Flow, tio *Tio) *UchDingAimdEvent {
	at := configNetwork.durationControlPDU + config.timeClusterTrip
	timedev := newTimedAnyEvent(srv, at, gwy, tio, configNetwork.sizeControlPDU)

	return &UchDingAimdEvent{zControlEvent{zEvent{*timedev}, flow.cid}, flow.repnum}
}

func (e *UchDingAimdEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[DingAimdEvent src=%v,tgt=%v,chunk#%d,num=%d]", e.source.String(), e.target.String(), printid, e.num)
}
