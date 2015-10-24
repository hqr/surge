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
// The pipeline includes 3 control events per each replica of each chunk,
// more details in the code.
//
package surge

import (
	"fmt"
	"math/rand"
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
	RunnerBase
	tiostats     int64
	chunkstats   int64
	replicastats int64
	txbytestats  int64
	rxbytestats  int64
	chunk        *Chunk
	replica      *PutReplica
	flowsto      *FlowDir
	rb           *RateBucket
}

type ServerFive struct {
	RunnerBase
	txbytestats int64
	rxbytestats int64
	flowsfrom   *FlowDir
	disk        *Disk
}

//
// static & init
//
var m5 ModelFive
var maxleakybucket, linkbpsminus int64

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

	maxleakybucket = int64(configNetwork.sizeFrame*8) + int64(configNetwork.sizeControlPDU*8)
	linkbpsminus = configNetwork.linkbps - configNetwork.linkbps*int64(configNetwork.overheadpct)/int64(100)
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
				if r.rb.enough(int64(configNetwork.sizeControlPDU * 8)) {
					r.startNewChunk()
				}
			}
			// recv
			r.receiveEnqueue()
			r.processPendingEvents(rxcallback)

			if Now.After(lastRefill) {
				r.refill()
				lastRefill = Now
				r.sendata()
			}
		}
		r.closeTxChannels()
	}()
}

func (r *GatewayFive) refill() {
	r.rb.addtime()

	applyCallback := func(srv RunnerInterface, flow *Flow) {
		if flow.tobandwidth == 0 || flow.offset >= flow.totalbytes {
			return
		}
		flow.rb.addtime()
	}
	r.flowsto.apply(applyCallback)
}

// send data frames
func (r *GatewayFive) sendata() {
	q := r.txqueue
	for k := 0; k < len(q.fifo); k++ {
		ev := q.fifo[k]
		if r.Send(ev, false) {
			q.deleteEvent(k)
			atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeFrame))
		}
	}
	applyCallback := func(srv RunnerInterface, flow *Flow) {
		if flow.tobandwidth == 0 || flow.offset >= r.chunk.sizeb {
			return
		}
		frsize := configNetwork.sizeFrame
		if flow.offset+frsize > r.chunk.sizeb {
			frsize = r.chunk.sizeb - flow.offset
		}
		newbits := int64(frsize * 8)
		if !r.rb.enough(newbits) {
			return
		}
		if flow.sendnexts.After(Now) {
			return
		}
		if !flow.rb.enough(newbits) {
			return
		}
		flow.offset += frsize
		ev := newUchReplicaDataEvent(r, srv, r.replica, flow, frsize)
		ev.SetExtension(flow.tio)
		if r.Send(ev, true) {
			flow.rb.use(newbits)
			r.rb.use(newbits)
			atomic.AddInt64(&r.txbytestats, int64(frsize))
			flow.sendnexts = Now.Add(time.Duration(newbits) * time.Second / time.Duration(flow.tobandwidth))
			// opt: start next replica without waiting for the current one's completion
			if flow.offset >= flow.totalbytes {
				r.finishStartReplica(flow.to, false)
			}
		} else {
			q.insertEvent(ev)
		}
	}
	r.flowsto.apply(applyCallback)
}

func (r *GatewayFive) startNewChunk() {
	assert(r.chunk == nil)
	r.chunk = NewChunk(r, configStorage.sizeDataChunk*1024)
	r.startNewReplica(1)
}

func (r *GatewayFive) startNewReplica(num int) {
	r.replica = NewPutReplica(r.chunk, num)

	tgt := r.selectTarget()
	assert(tgt != nil)

	tio := m5.putpipeline.NewTio(r)
	flow := NewFlow(r, tgt, r.chunk.cid, num, tio)
	flow.tobandwidth = int64(0)
	flow.rb = NewRateBucket(maxleakybucket, int64(0), int64(0), &flow.tobandwidth)

	flow.totalbytes = r.chunk.sizeb
	r.flowsto.insertFlow(tgt, flow)
	log("gwy-new-flow", flow.String())

	ev := newUchReplicaPutRequestEvent(r, tgt, r.replica)
	ev.SetExtension(tio)

	tio.next(ev)
	r.rb.use(int64(configNetwork.sizeControlPDU * 8))
	flow.rb.use(int64(configNetwork.sizeControlPDU * 8))
	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
}

func (r *GatewayFive) finishStartReplica(srv RunnerInterface, tiodone bool) {
	flow := r.flowsto.get(srv, true)
	if tiodone {
		log("replica-acked-flow-gone", flow.String())
		if flow.num == configStorage.numReplicas {
			log("chunk-done", r.chunk.String())
			atomic.AddInt64(&r.chunkstats, int64(1))
			r.chunk = nil
		}
		r.flowsto.deleteFlow(srv)
		return
	}

	log("replica-fully-transmitted", flow.String(), r.replica.String())
	flow.tobandwidth = 0
	if flow.num < configStorage.numReplicas {
		r.startNewReplica(flow.num + 1)
	}
}

func (r *GatewayFive) rateset(ev *UchRateSetEvent) {
	tio := ev.extension.(*Tio)
	assert(tio.source == r)

	if tio.done {
		return
	}
	flow := r.flowsto.get(ev.GetSource(), false)
	if flow == nil {
		log("WARNING: gwy-rateset on non-existing flow", tio.String(), ev.String())
		assert(false, "WARNING: gwy-rateset on non-existing flow:"+tio.String()+":"+ev.String())
		return
	}
	if flow.offset >= flow.totalbytes {
		return
	}
	assert(flow.tio == tio, flow.String()+":"+tio.String())
	if !flow.rateini || flow.ratects.Before(ev.GetCreationTime()) {
		flow.ratects = ev.GetCreationTime()
		flow.raterts = Now
		flow.rateini = true
		flow.tobandwidth = ev.tobandwidth
		log(LOG_V, "gwy-rateset", flow.String())
	}
}

func (r *GatewayFive) selectTarget() RunnerInterface {
	numPeers := cap(r.eps) - 1
	assert(numPeers > 1)
	id := rand.Intn(numPeers) + 1
	cnt := 0
	for {
		peer := r.eps[id]
		flow := r.flowsto.get(peer, false)
		if flow == nil {
			return peer
		}
		id++
		cnt++
		if id >= numPeers {
			id = 1
		}
		if cnt >= numPeers {
			return nil
		}
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
		flow.rb.maximize()
		flow.tobandwidth = tioevent.tobandwidth
		log(LOG_V, "gwy-rateinit", flow.String(), r.replica.String())
	} else {
		log(LOG_V, "gwy-rate-already-set", flow.String())
	}

	log(LOG_V, "gwy-rateinit", flow.String(), r.replica.String())

	return nil
}

func (r *GatewayFive) M5replicack(ev EventInterface) error {
	tioevent := ev.(*UchReplicaPutAckEvent)
	srv := tioevent.GetSource()
	flow := r.flowsto.get(srv, true)
	assert(flow.cid == tioevent.cid)
	assert(flow.num == tioevent.num)
	log(LOG_V, "::M5replicack()", flow.String(), tioevent.String())
	atomic.AddInt64(&r.replicastats, int64(1))
	return nil
}

//=========================
// GatewayFive stats
//=========================
func (r *GatewayFive) GetStats(reset bool) NodeStats {
	s := r.RunnerBase.GetStats(true)
	if reset {
		s["tio"] = atomic.SwapInt64(&r.tiostats, int64(0))
		s["chunk"] = atomic.SwapInt64(&r.chunkstats, int64(0))
		s["replica"] = atomic.SwapInt64(&r.replicastats, int64(0))
		s["txbytes"] = atomic.SwapInt64(&r.txbytestats, int64(0))
		s["rxbytes"] = atomic.SwapInt64(&r.rxbytestats, int64(0))
	} else {
		s["tio"] = atomic.LoadInt64(&r.tiostats)
		s["chunk"] = atomic.LoadInt64(&r.chunkstats)
		s["replica"] = atomic.LoadInt64(&r.replicastats)
		s["txbytes"] = atomic.LoadInt64(&r.txbytestats)
		s["rxbytes"] = atomic.LoadInt64(&r.rxbytestats)
	}
	return s
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
			// recv
			r.receiveEnqueue()
			time.Sleep(time.Microsecond)
			r.processPendingEvents(rxcallback)

			if numflows != r.flowsfrom.count() {
				r.rerate()
				numflows = r.flowsfrom.count()
			}
		}

		r.closeTxChannels()
	}()
}

func (r *ServerFive) rerate() {
	nflows := r.flowsfrom.count()
	if nflows == 0 {
		return
	}
	applyCallback := func(gwy RunnerInterface, flow *Flow) {
		assert(flow.to == r)
		assert(flow.rateini)
		if flow.totalbytes-flow.offset <= configNetwork.sizeFrame {
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

func (r *ServerFive) receiveReplicaData(ev *UchReplicaDataEvent) {
	gwy := ev.GetSource()
	flow := r.flowsfrom.get(gwy, true)
	log(LOG_V, "srv-recv-data", flow.String(), ev.String())
	assert(flow.cid == ev.cid)
	assert(flow.num == ev.num)

	x := ev.offset - flow.offset
	assert(x <= configNetwork.sizeFrame, fmt.Sprintf("WARNING: out of order:%d:%s", ev.offset, flow.String()))

	atomic.AddInt64(&r.rxbytestats, int64(x))

	flow.offset = ev.offset
	tio := ev.extension.(*Tio)

	if flow.offset >= flow.totalbytes {
		// postpone the ack until after the replica (chunk.sizeb) is written to disk
		diskdoneintime := r.disk.scheduleWrite(flow.totalbytes)
		r.disk.queue.insertTime(diskdoneintime)

		putackev := newUchReplicaPutAckEvent(r, gwy, flow.cid, flow.num, diskdoneintime)
		tio.next(putackev)
		atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))

		log("srv-replica-done-and-gone", flow.String())
		r.flowsfrom.deleteFlow(gwy)
		return
	}
}

//=========================
// ServerFive stats
//=========================
func (r *ServerFive) GetStats(reset bool) NodeStats {
	s := r.RunnerBase.GetStats(true)
	if reset {
		s["txbytes"] = atomic.SwapInt64(&r.txbytestats, int64(0))
		s["rxbytes"] = atomic.SwapInt64(&r.rxbytestats, int64(0))
	} else {
		s["txbytes"] = atomic.LoadInt64(&r.txbytestats)
		s["rxbytes"] = atomic.LoadInt64(&r.rxbytestats)
	}
	s["disk-queue-depth"] = r.disk.queue.NumPending()
	return s
}

//==================================================================
//
// ModelFive interface methods
//
//==================================================================
func (m *ModelFive) NewGateway(i int) RunnerInterface {
	gwy := &GatewayFive{RunnerBase{id: i, strtype: "GWY"}, int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, nil}
	gwy.init(config.numServers)

	gwy.flowsto = NewFlowDir(gwy, config.numServers)

	gwy.rb = NewRateBucket(maxleakybucket, linkbpsminus, maxleakybucket, nil)
	return gwy
}

func (m *ModelFive) NewServer(i int) RunnerInterface {
	srv := &ServerFive{RunnerBase: RunnerBase{id: i, strtype: "SRV"}, txbytestats: int64(0), rxbytestats: int64(0), flowsfrom: nil}
	srv.init(config.numGateways)

	srv.flowsfrom = NewFlowDir(srv, config.numGateways)
	srv.disk = NewDisk(srv, configStorage.diskMBps)
	return srv
}

func (m *ModelFive) NewDisk(i int) RunnerInterface { return nil }

func (m *ModelFive) Configure() {
	configNetwork.sizeControlPDU = 100
}
