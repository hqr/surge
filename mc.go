package surge

import (
	"fmt"
	"sync/atomic"
	"time"
)

//========================================================================
// const
//========================================================================
const cssdID = 1 // node id of the ssd target in this model

//========================================================================
// interfaces
//========================================================================
type replicaReceiverInterface interface {
	NodeRunnerInterface
	receiveReplicaData(ev *ReplicaDataEvent) bool
}

//========================================================================
// types
//========================================================================
// implements ModelInterface
type modelC struct {
	ModelGeneric
	putpipeline *Pipeline
}

// gets inited in Configure()
type modelC_config struct {
	// computed
	newwritebits int64
	// internal I <=> T <=> T bandwidth
	linkbps int64
	// ssd
	ssdcmdwin          int
	ssdThroughputMBps  int
	ssdCapacityMB      int
	ssdLowmark         int
	ssdHighmark        int
	timeShareNumChunks int
	migcmdwin          int
	// hdd
	hddcmdwin         int
	hddThroughputMBps int
}

var c_config = modelC_config{}

//========================================================================
// nodes
//========================================================================
// initiator-side target's runtime state
type tgtrt struct {
	target    NodeRunnerInterface
	replica   []*PutReplica
	ctrlevent map[int64]*ReplicaPutRequestEvent
	dataevent map[int64]*ReplicaDataEvent
	tios      map[int64]TioInterface
	ack       map[int64]bool
	flow      *FlowLong
	cmdwin    int
}

type gatewayC struct {
	GatewayCommon
	tgtrts map[int]*tgtrt
}

type targetCcommon struct {
	ServerUch
}

type targetChdd struct {
	targetCcommon
	tssd *targetCssd
}

type targetCssd struct {
	targetCcommon
	usedKB               int64 // <= ssdCapacityMB * 1000
	migration            map[int]*FlowLong
	wqueue               *TxQueue
	hddidx               int
	numReceiveDelta      int
	numMigrateDelta      int
	receivingPercentage  int
	numMigrateAckPending int
	hysteresis           bool
}

//======================================================================
// static & init
//======================================================================
var mC modelC

func init() {
	p := NewPipeline()
	p.AddStage(&PipelineStage{name: "WRITE-BEGIN", handler: "MBwritebegin"})
	p.AddStage(&PipelineStage{name: "WRITE-ACK", handler: "MBwriteack"})

	mC.putpipeline = p

	d := NewStatsDescriptors("c")
	d.registerCommonProtoStats()

	props := make(map[string]interface{}, 1)
	props["description"] = "Storage server with SSD and HDD tiers, limited SSD capacity, and SSD to HDD auto-migration (watermarked)"
	RegisterModel("c", &mC, props)
}

//==================================================================
//
// gatewayC methods
//
//==================================================================
func (r *gatewayC) postBuildInit() {
	// because (local) initiator's memory bw >> any disk bw
	// r.rb = NewRateBucket(c_config.newwritebits, c_config.linkbps, 0)
	r.rb = &DummyRateBucket{}
	var tssd *targetCssd
	r.tgtrts = make(map[int]*tgtrt, config.numServers)
	// init per target runtime
	for i := 0; i < config.numServers; i++ {
		t := tgtrt{}
		t.target = allServers[i]
		t.flow = &FlowLong{from: r, to: t.target, rb: r.rb, tobandwidth: c_config.linkbps}
		w := c_config.hddcmdwin
		if t.target.GetID() == cssdID {
			w = c_config.ssdcmdwin
			tssd = t.target.(*targetCssd)
		}
		t.replica = make([]*PutReplica, w)
		t.ctrlevent = make(map[int64]*ReplicaPutRequestEvent, w)
		t.dataevent = make(map[int64]*ReplicaDataEvent, w)
		t.ack = make(map[int64]bool, w)
		t.tios = make(map[int64]TioInterface, w)
		// init cmd window state
		for j := 0; j < w; j++ {
			chunk := NewChunk(r, configStorage.sizeDataChunk*1024)
			t.replica[j] = NewPutReplica(chunk, 1) // one replica only
			tio := NewTioOffset(r, r.putpipeline, t.replica[j], t.target, t.flow, false)
			tid := tio.GetID()
			t.tios[tid] = tio
			t.ctrlevent[tid] = newReplicaPutRequestEvent(r, t.target, t.replica[j], tio)
			// carries the entire chunk
			t.dataevent[tid] = newRepDataEvent(r, t.target, tio, 0, configStorage.sizeDataChunk*1024)
			t.ack[tid] = true
		}
		r.tgtrts[t.target.GetID()] = &t
	}
	// init targets (NOTE: grouping/raiding not supported)
	tssd.migration = make(map[int]*FlowLong, config.numServers-1)
	for i := 0; i < config.numServers; i++ {
		tin := allServers[i]
		targetid := tin.GetID()
		if targetid == cssdID {
			continue
		}
		thdd := tin.(*targetChdd)
		thdd.tssd = tssd
		tssd.migration[targetid] = &FlowLong{from: tssd, to: thdd, rb: tssd.rb, tobandwidth: c_config.linkbps}
	}
}

func (r *gatewayC) rxcallbackC(ev EventInterface) int {
	tio := ev.GetTio()
	tio.doStage(r, ev)
	if tio.Done() {
		log("tio-done", tio.String())
		atomic.AddInt64(&r.tiostats, 1)
	}
	return ev.GetSize()
}

func (r *gatewayC) Run() {
	r.state = RstateRunning
	go func() {
		for r.state == RstateRunning {
			r.receiveEnqueue()
			r.processPendingEvents(r.rxcallbackC)
			for _, tgtrt := range r.tgtrts {
				if r.rb.below(c_config.newwritebits) {
					break
				}
				// TODO: ML load balance here..
				r.writeto(tgtrt)
			}
		}
		r.closeTxChannels()
	}()
}

func (r *gatewayC) writeto(t *tgtrt) {
	if t.flow.timeTxDone.After(Now) {
		return
	}
	for _, tioint := range t.tios {
		tio := tioint.(*TioOffset)
		assert(tio.GetTarget() == t.flow.to)
		tid := tio.GetID()
		if !t.ack[tid] {
			continue
		}
		// control then payload, back to back
		putreqev, _ := t.ctrlevent[tid]
		putreqev.crtime = Now
		putreqev.thtime = Now.Add(configNetwork.durationControlPDU + config.timeClusterTrip)
		putreqev.tiostage = "WRITE-BEGIN" // force/reuse tio to execute this event's stage
		t.ack[tid] = false

		// execute pipeline step
		tio.offset = 0
		_, printid := uqrandom64(r.GetID())
		tio.chunksid = printid
		tio.next(putreqev, SmethodWait)

		// payload
		dataev, _ := t.dataevent[tid]
		dataev.crtime = putreqev.thtime
		dataev.thtime = dataev.crtime.Add(configNetwork.netdurationDataChunk + config.timeClusterTrip)
		ok := r.Send(dataev, SmethodWait)
		assert(ok)

		tio.offset += int64(configStorage.sizeDataChunk) * 1024
		r.rb.use(c_config.newwritebits)
		t.flow.timeTxDone = Now.Add(configNetwork.durationControlPDU + configNetwork.netdurationDataChunk)

		log("write-sent", tio.String(), dataev.String())

		break
	}
}

func (r *gatewayC) MBwriteack(ev EventInterface) error {
	tio := ev.GetTio()
	tid := tio.GetID()
	targetid := tio.GetTarget().GetID()
	t := r.tgtrts[targetid]

	t.ack[tid] = true

	atomic.AddInt64(&r.replicastats, 1)
	atomic.AddInt64(&r.chunkstats, 1)

	putreqev, _ := t.ctrlevent[tid]
	chunklatency := Now.Sub(putreqev.GetCreationTime())
	x := int64(chunklatency) / 1000
	if targetid == cssdID {
		log("write-done-ssd", tio.String(), "latency", chunklatency, x)
	} else {
		log("write-done-hdd", tio.String(), "latency", chunklatency, x)
	}

	return nil
}

//==================================================================
//
// targetCcommon
//
//==================================================================
func (r *targetCcommon) rxcallbackC(ev EventInterface) int {
	switch ev.(type) {
	case *ReplicaPutRequestEvent:
		tio := ev.GetTio()
		tio.doStage(r.realobject())
	case *ReplicaDataEvent:
		tioevent := ev.(*ReplicaDataEvent)
		//
		// cast to the right i-face and call the right target
		//
		rr := r.realobject().(replicaReceiverInterface)
		rr.receiveReplicaData(tioevent)
	case *DataMigrationEvent:
		migev := ev.(*DataMigrationEvent)
		thdd := r.realobject().(*targetChdd)
		thdd.receiveMigrationData(migev)
	case *DataMigrationAck:
		migack := ev.(*DataMigrationAck)
		tssd := r.realobject().(*targetCssd)
		tssd.receiveMigrationAck(migack)
	default:
		assert(false)
	}

	return ev.GetSize()
}

func (r *targetCcommon) MBwritebegin(ev EventInterface) error {
	tioevent := ev.(*ReplicaPutRequestEvent)
	log(r.String(), "::MBwritebegin()", tioevent.GetTio().String(), tioevent.String())
	return nil
}

//==================================================================
//
// targetChdd
//
//==================================================================
func (r *targetChdd) Run() {
	r.state = RstateRunning
	go func() {
		for r.state == RstateRunning {
			r.receiveEnqueue()
			r.processPendingEvents(r.rxcallbackC)
		}
		r.closeTxChannels()
	}()
}

func (r *targetChdd) receiveReplicaData(ev *ReplicaDataEvent) bool {
	// queue to disk and ACK right away
	atdisk := r.disk.scheduleWrite(ev.GetSize())
	gwy := ev.GetSource()
	tio := ev.GetTio()
	putackev := newReplicaPutAckEvent(r, gwy, tio, atdisk)
	tio.next(putackev, SmethodWait)

	log("hdd-write-received", tio.String(), atdisk)
	return true
}

func (r *targetChdd) receiveMigrationData(migev *DataMigrationEvent) {
	// this path is assumed sequential and therefore faster
	atdisk := r.disk.scheduleWrite(migev.GetSize() >> 4)
	tssd := migev.GetSource()
	assert(tssd == r.tssd)

	migack := newDataMigrationAck(r, tssd, atdisk)
	txch, _ := r.getExtraChannels(tssd)
	select {
	case txch <- migack:
		log("migration-ack-sent", migack.String())
	default:
		log("warn:hdd-migrate-ack", migack.String())
	}
}

//==================================================================
//
// targetCssd
//
//==================================================================
func (r *targetCssd) Run() {
	r.state = RstateRunning
	r.receivingPercentage = 100
	r.hysteresis = false
	go func() {
		for r.state == RstateRunning {
			toreceive := (c_config.timeShareNumChunks*r.receivingPercentage + 50) / 100
			tomigrate := c_config.timeShareNumChunks - toreceive
			migrateok := true
			fromqueue := 0
			for r.state == RstateRunning && (r.numReceiveDelta < toreceive || r.numMigrateDelta < tomigrate) {
				r.receiveEnqueue()
				r.processPendingEvents(r.rxcallbackC)

				ok := r.migrate()
				migrateok = migrateok && ok
				// handle queued (delayed) writes
				if r.wqueue.depth() > 0 {
					x64 := float64(r.usedKB) * 100 / float64(c_config.ssdCapacityMB) / 1000
					x := int(x64)
					if r.numReceiveDelta < toreceive ||
						// the second condition takes care of the case when the entire ssdcmdwin
						// gets queued up, which would mean no TIOs between initiator and SSD target
						// hence, we here make sure to have at least one at all times
						// unless above high watermark of course
						(x < c_config.ssdHighmark && r.wqueue.depth() >= c_config.ssdcmdwin) {
						firstev := r.wqueue.popEvent()
						dataev := firstev.(*ReplicaDataEvent)
						fromqueue++
						r.write(dataev)
					}
				}
			}
			log("ssd-run-cycle", "%", r.receivingPercentage, "#r", r.numReceiveDelta, "#m", r.numMigrateDelta)
			log("ssd-run-cycle", "used", r.usedKB, "depth", r.wqueue.depth(), "from-wqueue", fromqueue)
			if !migrateok || r.hysteresis {
				log("ssd-run-cycle", "mok", migrateok, "hysteresis", r.hysteresis)
			}
			r.recomputeRP()
			r.numReceiveDelta, r.numMigrateDelta = 0, 0
		}
		r.closeTxChannels()
	}()
}

// recompute receive/migrate ratio r.receivingPercentage
func (r *targetCssd) recomputeRP() {
	x64 := float64(r.usedKB) * 100 / float64(c_config.ssdCapacityMB) / 1000
	x := int(x64)
	switch {
	case x <= c_config.ssdLowmark:
		r.receivingPercentage = 100
		r.hysteresis = false
	case r.hysteresis:
		assert(x <= 100)
		r.receivingPercentage = 0
	case x < c_config.ssdHighmark:
		p := float64(x-c_config.ssdLowmark) / float64(c_config.ssdHighmark-c_config.ssdLowmark) * 100
		r.receivingPercentage = 100 - int(p)
		if r.receivingPercentage < 60 {
			r.receivingPercentage += 10 // biased to write
		}
	case x >= c_config.ssdHighmark && x <= 100:
		r.receivingPercentage = 0
		r.hysteresis = true
		log("warn:hysteresis")
	default:
		assert(false)
	}
}

func (r *targetCssd) receiveReplicaData(ev *ReplicaDataEvent) bool {
	toreceive := (c_config.timeShareNumChunks*r.receivingPercentage + 50) / 100
	if r.numReceiveDelta >= toreceive {
		r.wqueue.insertEvent(ev)
		return false
	}
	var dataev *ReplicaDataEvent
	if r.wqueue.depth() > 0 {
		firstev := r.wqueue.popEvent()
		dataev = firstev.(*ReplicaDataEvent)
		r.wqueue.insertEvent(ev)
	} else {
		dataev = ev
	}
	r.write(dataev)
	return true
}

func (r *targetCssd) write(dataev *ReplicaDataEvent) {
	tio := dataev.GetTio()
	// queue to disk and ACK when done writing
	atdisk := r.disk.scheduleWrite(dataev.GetSize())
	gwy := dataev.GetSource()
	putackev := newReplicaPutAckEvent(r, gwy, tio, atdisk)
	tio.next(putackev, SmethodWait)
	log("ssd-write-received", tio.String(), atdisk)

	r.usedKB += int64(dataev.GetSize()) / 1024
	r.numReceiveDelta++
}

func (r *targetCssd) migrate() bool {
	toreceive := (c_config.timeShareNumChunks*r.receivingPercentage + 50) / 100
	tomigrate := c_config.timeShareNumChunks - toreceive
	if r.numMigrateDelta >= tomigrate {
		return true
	}
	r.hddidx = (r.hddidx + 1) % config.numServers // round robin
	if allServers[r.hddidx].GetID() == cssdID {
		r.hddidx = (r.hddidx + 1) % config.numServers
	}
	target := allServers[r.hddidx]
	targetid := target.GetID()
	flow := r.migration[targetid]
	if flow.timeTxDone.After(Now) {
		return true
	}
	if r.numMigrateAckPending >= c_config.migcmdwin {
		return false
	}
	// FIXME: encapsulate as r.Send(migev, SmethodWait), see ma as well
	txch, _ := r.getExtraChannels(target)
	migev := newDataMigrationEvent(r, target, flow, configStorage.sizeDataChunk*1024)
	select {
	case txch <- migev:
		r.rb.use(c_config.newwritebits)
		flow.timeTxDone = Now.Add(configNetwork.netdurationDataChunk)

		r.usedKB -= int64(configStorage.sizeDataChunk)
		r.numMigrateDelta++      // in range [0, c_config.timeShareNumChunks]
		r.numMigrateAckPending++ // in range [0, c_config.migcmdwin]
		log("ssd-migrate", flow.String())
		return true
	default:
		log("warn:ssd-migrate", flow.String())
		return false
	}
}

func (r *targetCssd) receiveMigrationAck(migack *DataMigrationAck) {
	r.numMigrateAckPending--
	log("migration-ack-received", migack.String())
}

//==================================================================
//
// modelC interface methods
//
//==================================================================
func (m *modelC) NewGateway(i int) NodeRunnerInterface {
	gwy := NewGatewayCommon(i, mC.putpipeline)
	rgwy := &gatewayC{GatewayCommon: *gwy}
	rgwy.rptr = rgwy

	return rgwy
}

func (m *modelC) NewServer(runnerid int) NodeRunnerInterface {
	switch runnerid {
	case cssdID:
		return m.newServerSsd(runnerid)
	default:
		return m.newServerHdd(runnerid)
	}
}

func (m *modelC) newServerSsd(runnerid int) NodeRunnerInterface {
	srv := NewServerUchExtraChannels(runnerid, mC.putpipeline, DtypeVarLatency, c_config.ssdThroughputMBps)
	srv.rb = &DummyRateBucket{}

	csrv := &targetCcommon{*srv}

	rsrv := &targetCssd{targetCcommon: *csrv}
	rsrv.ServerUch.rptr = rsrv
	rsrv.flowsfrom = NewFlowDir(rsrv, config.numGateways)

	rsrv.wqueue = NewTxQueue(rsrv, c_config.ssdcmdwin*4)

	rsrv.initExtraChannels(config.numServers - 1) // plus ssd => hdd migration
	rsrv.initCases()
	return rsrv
}

func (m *modelC) newServerHdd(runnerid int) NodeRunnerInterface {
	srv := NewServerUchExtraChannels(runnerid, mC.putpipeline, DtypeVarLatency, c_config.hddThroughputMBps)
	srv.rb = &DummyRateBucket{}

	csrv := &targetCcommon{*srv}

	rsrv := &targetChdd{targetCcommon: *csrv} // postbuild will finalize
	rsrv.ServerUch.rptr = rsrv
	rsrv.flowsfrom = NewFlowDir(rsrv, config.numGateways+1)

	rsrv.initExtraChannels(1) // plus ssd => hdd migration
	rsrv.initCases()
	return rsrv
}

// model specific config - TODO: harcoded, use flags
func (m *modelC) Configure() {
	c_config.linkbps = 100 * 1000 * 1000 * 1000
	c_config.ssdcmdwin = 32
	c_config.ssdThroughputMBps = 1400
	c_config.ssdCapacityMB = 100
	c_config.ssdLowmark = 40
	c_config.ssdHighmark = 70
	c_config.hddcmdwin = 8
	c_config.hddThroughputMBps = 180

	// max number of un-acked pending migration chunks in flight
	c_config.migcmdwin = 32

	// ssd target divides its time between a) receiving/writing chunks
	// and b) migrating older chunks to hdd target(s);
	// the corresponding time sharing is expressed as a total number
	// of both kinds of chunks, while the ratio between these two numbers
	// is controlled at runtime by the low and high watermarks as far as
	// the remaining capacity
	c_config.timeShareNumChunks = 10

	// common config
	config.timeClusterTrip = time.Nanosecond * 10
	if config.timeClusterTrip < config.timeIncStep {
		config.timeClusterTrip = config.timeIncStep
	}
	configNetwork.linkbps = c_config.linkbps
	configNetwork.sizeControlPDU = 100
	// sub-lun migration: one-shot transfer - no network, no framing
	configNetwork.sizeFrame = configStorage.sizeDataChunk * 1024
	configNetwork.overheadpct = 0
	configStorage.maxDiskQueue = 0

	// NOTE: do not allow smaller chunk sizes
	if configStorage.sizeDataChunk < 1024 {
		configStorage.sizeDataChunk = 1024
	}
}

func (m *modelC) PostBuild() {
	assert(config.numGateways == 1, "mc: multiple initiators not supported yet")
	for i := 0; i < config.numGateways; i++ {
		gwy := allGateways[i].(*gatewayC)
		gwy.postBuildInit()

		tssd := gwy.tgtrts[cssdID].target
		// interconnect ssd target to hdd(s)
		k := 0
		for j := 0; j < config.numServers; j++ {
			thdd := allServers[j]
			if thdd.GetID() == cssdID {
				assert(thdd == tssd)
				continue
			}
			txch := make(chan EventInterface, config.channelBuffer)
			rxch := make(chan EventInterface, config.channelBuffer)
			tssd.setExtraChannels(thdd, k, txch, rxch)
			k++
			thdd.setExtraChannels(tssd, 0, rxch, txch)
		}
	}
}

//==================================================================
//
// events
//
//==================================================================
type DataMigrationEvent struct {
	zDataEvent
}

func newDataMigrationEvent(from NodeRunnerInterface, to NodeRunnerInterface, flow FlowInterface, size int) *DataMigrationEvent {
	at := configNetwork.netdurationDataChunk + config.timeClusterTrip
	timedev := newTimedAnyEvent(from, at, to, size)
	return &DataMigrationEvent{zDataEvent{zEvent{*timedev}, 0, 1, flow.getoffset(), flow.getbw()}}
}

func (e *DataMigrationEvent) String() string {
	return fmt.Sprintf("[DME %v=>%v]", e.source.String(), e.target.String())
}

type DataMigrationAck struct {
	zControlEvent
}

func (e *DataMigrationAck) String() string {
	dtriggered := e.thtime.Sub(time.Time{})
	return fmt.Sprintf("[DMA %v=>%v,%11.10v]", e.source.String(), e.target.String(), dtriggered)
}

func newDataMigrationAck(from NodeRunnerInterface, to NodeRunnerInterface, at time.Duration) *DataMigrationAck {
	timedev := newTimedAnyEvent(from, at, to)
	return &DataMigrationAck{zControlEvent{zEvent{*timedev}, 0}}
}
