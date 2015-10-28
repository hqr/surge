//
// ModelSix (m6, "6"), aka UCH-AIMD:
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
	"sync/atomic"
	"time"
)

// implements ModelInterface
type ModelSix struct {
	putpipeline *Pipeline
}

//========================================================================
// m6 nodes
//========================================================================
type GatewaySix struct {
	GatewayUch
}

type ServerSix struct {
	ServerUch
}

//
// static & init
//
var m6 ModelSix
var timeFrame time.Duration // time to receive a data frame

func init() {
	p := NewPipeline()
	p.AddStage(&PipelineStage{name: "PUT-REQ", handler: "M6putrequest"})
	p.AddStage(&PipelineStage{name: "PUT-REQ-ACK", handler: "M6putreqack"})
	p.AddStage(&PipelineStage{name: "REPLICA-ACK", handler: "M6replicack"})

	m6.putpipeline = p

	d := NewStatsDescriptors("6")
	d.Register("event", StatsKindCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxbusy", StatsKindPercentage, StatsScopeServer)
	d.Register("chunk", StatsKindCount, StatsScopeGateway)
	d.Register("replica", StatsKindCount, StatsScopeGateway)
	d.Register("txbytes", StatsKindByteCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxbytes", StatsKindByteCount, StatsScopeServer|StatsScopeGateway)
	d.Register("disk-queue-depth", StatsKindSampleCount, StatsScopeServer)

	props := make(map[string]interface{}, 1)
	props["description"] = "UCH-AIMD: Unicast Consistent Hash distribution using AIMD congestion control"
	RegisterModel("6", &m6, props)

	timeFrame = time.Duration(configNetwork.sizeFrame) * time.Second / time.Duration(configNetwork.linkbpsminus)
}

//==================================================================
//
// GatewaySix methods
//
//==================================================================
func (r *GatewaySix) Run() {
	r.state = RstateRunning

	rxcallback := func(ev EventInterface) bool {
		atomic.AddInt64(&r.rxbytestats, int64(configNetwork.sizeControlPDU))

		switch ev.(type) {
		case *UchDingAimdEvent:
			dingev := ev.(*UchDingAimdEvent)
			log(LOG_V, "GWY::rxcallback", dingev.String())
			r.ding(dingev)
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

func (r *GatewaySix) ding(dingev *UchDingAimdEvent) {
	tio := dingev.extension.(*Tio)
	assert(tio.source == r)

	flow := r.flowsto.get(dingev.GetSource(), false)
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
// GatewaySix TIO handlers
//=========================
func (r *GatewaySix) M6putreqack(ev EventInterface) error {
	tioevent := ev.(*UchReplicaPutRequestAckEvent)
	log(LOG_V, r.String(), "::M6putreqack()", tioevent.String())

	tio := tioevent.extension.(*Tio)
	assert(tio.source == r)

	flow := r.flowsto.get(ev.GetSource(), false)
	assert(flow != nil, "FATAL: M6putreqack non-existing flow:"+tio.String()+":"+tioevent.String())
	assert(flow.tio == tio, flow.String()+":"+tio.String())
	assert(flow.tobandwidth == int64(0))

	flow.tobandwidth = flow.rb.getrate()
	return nil
}

func (r *GatewaySix) M6replicack(ev EventInterface) error {
	return r.replicack(ev)
}

//==================================================================
//
// ServerSix methods
//
//==================================================================
func (r *ServerSix) Run() {
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
		lastAimdCheck := Now
		for r.state == RstateRunning {
			r.receiveEnqueue()
			r.processPendingEvents(rxcallback)

			if Now.Sub(lastAimdCheck) > (timeFrame + config.timeClusterTrip) {
				r.aimdCheck()
				lastAimdCheck = Now
			}
		}

		r.closeTxChannels()
	}()
}

// FIXME: take into account disk queue as well..
func (r *ServerSix) aimdCheck() {
	q := r.rxqueue
	dingcnt := 0
	linktime := Now

	q.lock()
	defer q.unlock()

	for k := 0; k < len(q.pending); k++ {
		ev := q.pending[k]
		_, ok := ev.(*UchReplicaDataEvent)
		if !ok {
			continue
		}
		t := ev.GetTriggerTime()
		if t.Before(linktime) || t.Sub(linktime) < timeFrame {
			r.dingOne(ev.GetSource())
			dingcnt++
			if dingcnt > 1 { // ding upto ... in one go
				break
			}
		}
		linktime = linktime.Add(timeFrame)
	}
}

func (r *ServerSix) dingOne(gwy RunnerInterface) {
	flow := r.flowsfrom.get(gwy, true)
	dingev := newUchDingAimdEvent(r, gwy, flow.cid, flow.num)
	dingev.SetExtension(flow.tio)

	log(LOG_V, "srv-send-ding", flow.String())
	r.Send(dingev, true)
	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
}

func (r *ServerSix) M6putrequest(ev EventInterface) error {
	log(LOG_V, r.String(), "::M6putrequest()", ev.String())

	tioevent := ev.(*UchReplicaPutRequestEvent)
	gwy := tioevent.GetSource()
	f := r.flowsfrom.get(gwy, false)
	assert(f == nil)

	tio := tioevent.extension.(*Tio)
	flow := NewFlow(gwy, r, tioevent.cid, tioevent.num, tio)
	flow.totalbytes = tioevent.sizeb
	r.flowsfrom.insertFlow(gwy, flow)

	putreqackev := newUchReplicaPutRequestAckEvent(r, gwy, flow.cid, flow.num)
	log("srv-new-flow", flow.String(), putreqackev.String())

	tio.next(putreqackev)
	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
	return nil
}

//==================================================================
//
// ModelSix interface methods
//
//==================================================================
func (m *ModelSix) NewGateway(i int) RunnerInterface {
	setflowratebucket := func(flow *Flow) {
		flow.rb = NewRateBucketAIMD(
			configAIMD.bwMinInitialAdd,     // minrate
			configNetwork.linkbpsminus,     // maxrate
			configNetwork.maxratebucketval, // maxval
			configAIMD.timeAdd,             // Additive time interval
			configAIMD.bwDiv)               // Multiplicative divisor
	}

	gwy := NewGatewayUch(i, m6.putpipeline, setflowratebucket)
	gwy.rb = NewRateBucket(
		configNetwork.maxratebucketval, // maxval
		configNetwork.linkbpsminus,     // rate
		configNetwork.maxratebucketval) // value
	rgwy := &GatewaySix{*gwy}
	rgwy.GatewayUch.rptr = rgwy
	return rgwy
}

func (m *ModelSix) NewServer(i int) RunnerInterface {
	srv := NewServerUch(i, m6.putpipeline)
	rsrv := &ServerSix{*srv}
	rsrv.ServerUch.rptr = rsrv
	return rsrv
}

func (m *ModelSix) NewDisk(i int) RunnerInterface { return nil }

func (m *ModelSix) Configure() {
	configNetwork.sizeControlPDU = 100
}
