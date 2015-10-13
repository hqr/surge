//
// ModelFive (m5, "5") is the very first approximation to UDP based
// (unicast) storage-clustering via consistent hashing
//
// The pipeline includes 3 control events per each chunk-replica
// Congestion control by the targets tries to divide target's bandwidth
// equally between client gateways
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
// new types & constructors
//========================================================================
type Chunk struct {
	cid     int64
	sid     int64 // short id
	gateway RunnerInterface
	crtime  time.Time // creation time
	sizeb   int       // KB
}

func NewChunk(gwy RunnerInterface, sizebytes int) *Chunk {
	assert(gwy.(*GatewayFive) != nil)
	uqid, printid := uqrandom64(gwy.GetId())
	return &Chunk{cid: uqid, sid: printid, gateway: gwy, crtime: Now, sizeb: sizebytes}
}

func (chunk *Chunk) String() string {
	return fmt.Sprintf("[chunk#%d]", chunk.sid)
}

type PutReplica struct {
	chunk  *Chunk
	crtime time.Time // creation time
	num    int       // 1 .. configStorage.numReplicas
}

func (replica *PutReplica) String() string {
	return fmt.Sprintf("[replica#%d,chunk#%d]", replica.num, replica.chunk.sid)
}

func NewPutReplica(c *Chunk, n int) *PutReplica {
	return &PutReplica{chunk: c, crtime: Now, num: n}
}

//========================================================================
// types: events
//========================================================================
type M5Event struct {
	TimedUcastEvent
}

type M5ControlEvent struct {
	M5Event
	cid int64
}

type M5ReplicaPutRequestEvent struct {
	M5ControlEvent
	num   int // replica num
	sizeb int // size in bytes
}

func newM5ReplicaPutRequestEvent(gwy RunnerInterface, srv RunnerInterface, rep *PutReplica) *M5ReplicaPutRequestEvent {
	at := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	timedev := newTimedUcastEvent(gwy, at, srv)
	assert(timedev.GetSource().(*GatewayFive) != nil)
	return &M5ReplicaPutRequestEvent{M5ControlEvent{M5Event{*timedev}, rep.chunk.cid}, rep.num, rep.chunk.sizeb}
}

type M5ReplicaPutAckEvent struct {
	M5ControlEvent
	num int // replica num
}

func (e *M5ReplicaPutAckEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[PutAckEvent src=%v,tgt=%v,chunk#%d,num=%d]", e.source.String(), e.target.String(), printid, e.num)
}

func newM5ReplicaPutAckEvent(srv RunnerInterface, gwy RunnerInterface, chunkid int64, repnum int, atdisk time.Duration) *M5ReplicaPutAckEvent {
	atnet := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	at := atnet + atdisk
	timedev := newTimedUcastEvent(srv, at, gwy)
	assert(timedev.GetSource().(*ServerFive) != nil)
	return &M5ReplicaPutAckEvent{M5ControlEvent{M5Event{*timedev}, chunkid}, repnum}
}

type M5RateSetEvent struct {
	M5ControlEvent
	tobandwidth int64 // bits/sec
	num         int   // replica num
}

type M5RateInitEvent struct {
	M5RateSetEvent
}

func (e *M5RateInitEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[RateInitEvent src=%v,tgt=%v,chunk#%d,num=%d]", e.source.String(), e.target.String(), printid, e.num)
}

func newM5RateSetEvent(srv RunnerInterface, gwy RunnerInterface, rate int64, chunkid int64, repnum int) *M5RateSetEvent {
	at := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	timedev := newTimedUcastEvent(srv, at, gwy)
	assert(timedev.GetSource().(*ServerFive) != nil)
	// factor in the L2+L3+L4 headers overhead + ARP, etc.
	rate -= rate * int64(configNetwork.overheadpct) / int64(100)
	return &M5RateSetEvent{M5ControlEvent{M5Event{*timedev}, chunkid}, rate, repnum}
}

func (e *M5RateSetEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[RateSetEvent src=%v,tgt=%v,chunk#%d,num=%d]", e.source.String(), e.target.String(), printid, e.num)
}

func newM5RateInitEvent(srv RunnerInterface, gwy RunnerInterface, rate int64, chunkid int64, repnum int) *M5RateInitEvent {
	ev := newM5RateSetEvent(srv, gwy, rate, chunkid, repnum)
	return &M5RateInitEvent{*ev}
}

type M5DataEvent struct {
	M5Event
	cid    int64
	num    int
	offset int
}

type M5ReplicaDataEvent struct {
	M5DataEvent
}

func newM5ReplicaDataEvent(gwy RunnerInterface, srv RunnerInterface, rep *PutReplica, flow *Flow, frsize int) *M5ReplicaDataEvent {
	at := sizeToDuration(frsize, "B", flow.tobandwidth, "b") + config.timeClusterTrip
	timedev := newTimedUcastEvent(gwy, at, srv)
	assert(timedev.GetSource().(*GatewayFive) != nil)
	return &M5ReplicaDataEvent{M5DataEvent{M5Event{*timedev}, rep.chunk.cid, rep.num, flow.offset}}
}

func (e *M5ReplicaDataEvent) String() string {
	printid := uqrand(e.cid)
	dcreated := e.crtime.Sub(time.Time{})
	dtriggered := e.thtime.Sub(time.Time{})
	return fmt.Sprintf("[ReplicaDataEvent src=%v,tgt=%v,chunk#%d,num=%d,offset=%d,(%11.10v,%11.10v)]",
		e.source.String(), e.target.String(), printid, e.num, e.offset, dcreated, dtriggered)
}

//========================================================================
// types: nodes
//========================================================================
type GatewayFive struct {
	RunnerBase
	tiostats     int64
	chunkstats   int64
	replicastats int64
	txbytestats  int64
	rxbytestats  int64
	chunk        *Chunk // FIXME: one chunk at a time for now
	replica      *PutReplica
	flowsto      *FlowDir
	refillbits   int64 // leaky-bucket on the client side
}

type ServerFive struct {
	RunnerBase
	txbytestats int64
	rxbytestats int64
	flowsfrom   *FlowDir
	disk        *Disk
}

//
// init
//
var m5 ModelFive
var maxleakybucket int64
var linkbpsminus int64

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

	props := make(map[string]interface{}, 1)
	props["description"] = "UCH-CCPi: Unicast Consistent Hash distribution using Captive Congestion Point"
	RegisterModel("5", &m5, props)

	maxleakybucket = int64(configNetwork.sizeFrame * 8)
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
		case *M5RateSetEvent:
			ratesetev := ev.(*M5RateSetEvent)
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
				r.finishStartReplica(srv)
			}
		}
		return true
	}

	go func() {
		lastRefill := Now
		for r.state == RstateRunning {
			if r.chunk == nil {
				if r.refillbits > int64(configNetwork.sizeControlPDU*8) {
					r.startNewChunkReplica()
				}
			}
			// recv
			r.receiveEnqueue()
			r.processPendingEvents(rxcallback)

			if Now.After(lastRefill) {
				r.refill(Now.Sub(lastRefill))
				lastRefill = Now
				r.sendata()
			}
		}
		r.closeTxChannels()
	}()
}

func (r *GatewayFive) refill(d time.Duration) {
	if r.refillbits < maxleakybucket {
		r.refillbits += linkbpsminus * int64(d) / int64(time.Second)
		if r.refillbits > maxleakybucket {
			r.refillbits = maxleakybucket
		}
	}

	applyCallback := func(srv RunnerInterface, flow *Flow) {
		if flow.tobandwidth == 0 || flow.offset >= r.chunk.sizeb {
			return
		}
		if flow.refillbits >= maxleakybucket {
			return
		}
		flow.refillbits += flow.tobandwidth * int64(d) / int64(time.Second)
		if flow.refillbits > maxleakybucket {
			flow.refillbits = maxleakybucket
		}
	}
	r.flowsto.apply(applyCallback)
}

// send data frames
func (r *GatewayFive) sendata() {
	q := r.txqueue
	// FIXME: fifo per target, sizeFrame
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
		if r.refillbits < newbits {
			return
		}
		if flow.sendnexts.After(Now) {
			return
		}
		if flow.refillbits < newbits {
			return
		}
		flow.offset += frsize
		ev := newM5ReplicaDataEvent(r, srv, r.replica, flow, frsize)
		ev.SetExtension(flow.tio)
		if r.Send(ev, true) {
			flow.refillbits -= newbits
			r.refillbits -= newbits
			atomic.AddInt64(&r.txbytestats, int64(frsize))
			flow.sendnexts = Now.Add(time.Duration(newbits) * time.Second / time.Duration(flow.tobandwidth))
		} else {
			q.insertEvent(ev)
		}
	}
	r.flowsto.apply(applyCallback)
}

func (r *GatewayFive) startNewChunkReplica() {
	var num int
	if r.chunk == nil {
		r.chunk = NewChunk(r, configStorage.sizeDataChunk*1024)
		num = 1
	} else {
		num = r.replica.num + 1
		assert(num <= configStorage.numReplicas)
	}
	r.replica = NewPutReplica(r.chunk, num)

	tgt := r.selectTarget()
	assert(tgt != nil)

	tio := m5.putpipeline.NewTio(r)
	flow := NewFlow(r, tgt, r.chunk.cid, num, tio)
	flow.tobandwidth = int64(0)
	flow.totalbytes = r.chunk.sizeb
	r.flowsto.insertFlow(tgt, flow)
	log("gwy-new-flow", flow.String())

	ev := newM5ReplicaPutRequestEvent(r, tgt, r.replica)
	ev.SetExtension(tio)

	tio.next(ev)
	r.refillbits -= int64(configNetwork.sizeControlPDU * 8)
	flow.refillbits -= int64(configNetwork.sizeControlPDU * 8)
	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
}

func (r *GatewayFive) finishStartReplica(srv RunnerInterface) {
	flow := r.flowsto.get(srv, true)
	log("gwy-flow-and-replica-done-and-gone", flow.String(), r.replica.String())
	r.flowsto.deleteFlow(srv)
	if r.replica.num == configStorage.numReplicas {
		log("chunk-done", r.chunk.String())
		atomic.AddInt64(&r.chunkstats, int64(1))
		r.chunk = nil
		return
	}
	r.startNewChunkReplica()
}

func (r *GatewayFive) rateset(ev *M5RateSetEvent) {
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
	tioevent := ev.(*M5RateInitEvent)
	log(LOG_V, r.String(), "::M5rateinit()", tioevent.String())
	srv := tioevent.GetSource()
	flow := r.flowsto.get(srv, true)
	assert(flow.cid == tioevent.cid)
	assert(flow.num == tioevent.num)
	if !flow.rateini {
		flow.ratects = tioevent.GetCreationTime()
		flow.raterts = Now
		flow.rateini = true
		flow.refillbits = maxleakybucket
		flow.tobandwidth = tioevent.tobandwidth
		log(LOG_V, "gwy-rateinit", flow.String(), r.replica.String())
	} else {
		log(LOG_V, "gwy-rate-already-set", flow.String())
	}

	log(LOG_V, "gwy-rateinit", flow.String(), r.replica.String())

	return nil
}

func (r *GatewayFive) M5replicack(ev EventInterface) error {
	tioevent := ev.(*M5ReplicaPutAckEvent)
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
		case *M5ReplicaDataEvent:
			tioevent := ev.(*M5ReplicaDataEvent)
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
		ratesetev := newM5RateSetEvent(r, gwy, configNetwork.linkbps/int64(nflows), flow.cid, flow.num)
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

	tioevent := ev.(*M5ReplicaPutRequestEvent)
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
	rateinitev := newM5RateInitEvent(r, gwy, configNetwork.linkbps/int64(nflows), flow.cid, flow.num)
	flow.tobandwidth = rateinitev.tobandwidth
	log("srv-new-flow", flow.String(), rateinitev.String())

	tio.next(rateinitev)
	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
	return nil
}

func (r *ServerFive) receiveReplicaData(ev *M5ReplicaDataEvent) {
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
		putackev := newM5ReplicaPutAckEvent(r, gwy, flow.cid, flow.num, diskdoneintime)
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
	return s
}

//==================================================================
//
// ModelFive interface methods
//
//==================================================================
func (m *ModelFive) NewGateway(i int) RunnerInterface {
	gwy := &GatewayFive{RunnerBase{id: i, strtype: "GWY"}, int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, int64(maxleakybucket)}
	gwy.init(config.numServers)

	gwy.flowsto = NewFlowDir(gwy, config.numServers)
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
