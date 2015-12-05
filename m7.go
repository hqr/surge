package surge

import (
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

type modelSeven struct {
	putpipeline *Pipeline
}

//========================================================================
// m7 nodes
//========================================================================
type gatewaySeven struct {
	GatewayMcast
}

type serverSeven struct {
	ServerUch
}

//
// static & init
//
var m7 modelSeven

func init() {
	p := NewPipeline()
	p.AddStage(&PipelineStage{name: "REQUEST-NG", handler: "M7requestng"})
	p.AddStage(&PipelineStage{name: "BID", handler: "M7bid"})
	p.AddStage(&PipelineStage{name: "ACCEPT-NG", handler: "M7acceptng"})
	p.AddStage(&PipelineStage{name: "REPLICA-ACK", handler: "M7replicack"})

	m7.putpipeline = p

	d := NewStatsDescriptors("7")
	d.Register("event", StatsKindCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxbusy", StatsKindPercentage, StatsScopeServer)
	d.Register("chunk", StatsKindCount, StatsScopeGateway)
	d.Register("replica", StatsKindCount, StatsScopeGateway)
	d.Register("txbytes", StatsKindByteCount, StatsScopeGateway|StatsScopeServer)
	d.Register("rxbytes", StatsKindByteCount, StatsScopeServer|StatsScopeGateway)
	d.Register("disk-queue-depth", StatsKindSampleCount, StatsScopeServer)

	props := make(map[string]interface{}, 1)
	props["description"] = "Simplified Replicast with random bid selection"
	RegisterModel("7", &m7, props)
}

//==================================================================
//
// gatewaySeven methods
//
//==================================================================
// Run contains the gateway's receive callback and its goroutine. Each of the
// gateway instances (the running number of which is configured as
// config.numGateways) has a type gatewayFive and spends all its given runtime
// inside its own goroutine.
//
// As per rxcallback below, the gateway handles the model's pipeline stages
// (via generic doStage)
//
func (r *gatewaySeven) Run() {
	r.state = RstateRunning

	rxcallback := func(ev EventInterface) bool {
		atomic.AddInt64(&r.rxbytestats, int64(configNetwork.sizeControlPDU))

		tio := ev.GetTio()
		log(LogV, "GWY::rxcallback", tio.String())
		tio.doStage(r, ev)
		if tio.done {
			log(LogV, "tio-done", tio.String())
			atomic.AddInt64(&r.tiostats, int64(1))
		}
		return true
	}

	go func() {
		for r.state == RstateRunning {
			if r.chunk == nil {
				if r.rb.above(int64(configNetwork.sizeControlPDU * 8)) {
					r.startNewChunk()
				}
			}
			// recv
			r.receiveEnqueue()
			r.processPendingEvents(rxcallback)

			if r.chunk != nil && r.rzvgroup.getCount() == configStorage.numReplicas {
				r.sendata()
			}
		}
		r.closeTxChannels()
	}()
}

//==========================
// gatewaySeven TIO handlers
//==========================
func (r *gatewaySeven) M7bid(ev EventInterface) error {
	tioevent := ev.(*BidEvent)
	tiochild := tioevent.GetTio()
	tioparent := tiochild.parent
	assert(tioparent.haschild(tiochild))
	log(r.String(), "::M7bid()", tioevent.String())
	srv := tioevent.GetSource()
	group := tioevent.GetGroup()
	ngobj, ok := group.(*NgtGroup)
	assert(ok)

	assert(ngobj.hasmember(srv))
	assert(r.expectacks > 0)

	r.expectacks--
	if r.expectacks < configStorage.numReplicas {
		r.rzvgroup.servers[r.expectacks] = srv // FIXME
	}
	if r.expectacks > 0 {
		return nil
	}

	r.rzvgroup.init(ngobj.getID(), false)
	assert(r.rzvgroup.getCount() == configStorage.numReplicas)

	r.accept(ngobj, tioparent)
	return nil
}

// accept-ng control/stage
func (r *gatewaySeven) accept(ngobj *NgtGroup, tioparent *Tio) {
	assert(r.rzvgroup.getCount() == configStorage.numReplicas)
	assert(len(tioparent.children) == ngobj.getCount())

	flow := tioparent.flow
	targets := ngobj.getmembers()
	for _, srv := range targets {
		tio := tioparent.children[srv]
		acceptev := newMcastChunkPutAcceptEvent(r, ngobj, r.chunk, r.rzvgroup, tio)
		tio.next(acceptev)
	}
	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))

	time.Sleep(time.Microsecond) // FIXME

	for _, srv := range targets {
		// FIXME: terminate child tios that weren't accepted
		if !r.rzvgroup.hasmember(srv) {
			delete(tioparent.children, srv)
		}
	}
	assert(len(tioparent.children) == r.rzvgroup.getCount())

	flow.tobandwidth = configNetwork.linkbps >> 1 // FIXME FIXME
	flow.totalbytes = r.chunk.sizeb

}

//
// ReplicaPutAck handler
// FIXME: partial copy-paste
//
func (r *gatewaySeven) M7replicack(ev EventInterface) error {
	tioevent := ev.(*ReplicaPutAckEvent)
	tiochild := ev.GetTio()
	tioparent := tiochild.parent
	assert(tioparent.haschild(tiochild))
	group := tioevent.GetGroup()
	flow := tiochild.flow
	assert(flow.cid == tioevent.cid)
	assert(group == r.rzvgroup)

	// FIXME
	assert(r.chunk != nil, "ERROR: acking chunk nil,"+tioevent.String()+","+r.rzvgroup.String())
	assert(r.rzvgroup.getCount() == configStorage.numReplicas, "ERROR: acks on incomplete group,"+r.String()+","+r.rzvgroup.String())

	log(LogV, "::replicack()", flow.String(), tioevent.String())
	atomic.AddInt64(&r.replicastats, int64(1))

	r.numreplicas++
	log("replica-acked", flow.String(), "num-acked", r.numreplicas)
	if r.numreplicas < configStorage.numReplicas {
		return nil
	}

	log("chunk-done", r.String(), r.chunk.String())
	atomic.AddInt64(&r.chunkstats, int64(1))
	r.chunk = nil
	r.numreplicas = 0

	r.rzvgroup.init(0, true) // cleanup
	return nil
}

//=================
// gatewaySeven aux
//=================
func (r *gatewaySeven) startNewChunk() {
	assert(r.chunk == nil)
	r.chunk = NewChunk(r, configStorage.sizeDataChunk*1024)

	ngid := r.selectNgtGroup()
	ngobj := NewNgtGroup(ngid)
	r.expectacks = ngobj.getCount()

	// create flow and tios
	targets := ngobj.getmembers()
	assert(len(targets) == configReplicast.sizeNgtGroup)

	tioparent := r.putpipeline.NewTio(r, r.chunk)
	flow := NewFlow(r, nil, r.chunk.cid, 0, tioparent)
	flow.togroup = ngobj // FIXME

	// children tios
	for _, srv := range targets {
		r.putpipeline.NewTio(r, tioparent, r.chunk, flow, srv)
	}
	assert(len(tioparent.children) == ngobj.getCount(), fmt.Sprintf("%d != %d", len(tioparent.children), ngobj.getCount()))

	flow.tobandwidth = int64(0)
	flow.totalbytes = r.chunk.sizeb
	flow.rb = &DummyRateBucket{}

	log("gwy-new-flow-tio", flow.String(), tioparent.String())

	// start negotiating
	for _, srv := range targets {
		tio := tioparent.children[srv]
		ev := newMcastChunkPutRequestEvent(r, ngobj, r.chunk, srv, tio)
		tio.next(ev)
	}
	r.rb.use(int64(configNetwork.sizeControlPDU * 8))
	flow.rb.use(int64(configNetwork.sizeControlPDU * 8))
	flow.sendnexts = Now.Add(time.Duration(configNetwork.sizeControlPDU) * time.Second / time.Duration(configNetwork.linkbps))
	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
}

func (r *gatewaySeven) sendata() {
	frsize := configNetwork.sizeFrame
	for _, tioparent := range r.tios {
		flow := tioparent.flow
		if flow.sendnexts.After(Now) {
			return
		}
		if flow.tobandwidth == 0 || flow.offset >= r.chunk.sizeb {
			return
		}
		if flow.offset+frsize > r.chunk.sizeb {
			frsize = r.chunk.sizeb - flow.offset
		}
		flow.offset += frsize
		newbits := int64(frsize * 8)

		targets := r.rzvgroup.getmembers()
		for _, srv := range targets {
			tio := tioparent.children[srv]
			ev := newMcastChunkDataEvent(r, r.rzvgroup, r.chunk, flow, frsize, tio)
			srv.Send(ev, SmethodDirectInsert)
		}

		// time for the bits to get fully transmitted given the current flow's bandwidth
		flow.sendnexts = Now.Add(time.Duration(newbits) * time.Second / time.Duration(flow.tobandwidth))
		atomic.AddInt64(&r.txbytestats, int64(frsize))
	}
}

//==================================================================
//
// serverSeven methods
//
//==================================================================
// Run provides the servers's receive callback that executes both control path
// (via doStage()) and receives chunk/replica data, via DataEvent
//
func (r *serverSeven) Run() {
	r.state = RstateRunning

	rxcallback := func(ev EventInterface) bool {
		switch ev.(type) {
		case *ReplicaDataEvent:
			tioevent := ev.(*ReplicaDataEvent)
			log(LogV, "SRV::rxcallback: chunk data", tioevent.String())
			r.receiveReplicaData(tioevent)
		default:
			atomic.AddInt64(&r.rxbytestats, int64(configNetwork.sizeControlPDU))
			tio := ev.GetTio()
			log(LogV, "SRV::rxcallback", ev.String(), r.String())
			tio.doStage(r, ev)
		}

		return true
	}

	go func() {
		for r.state == RstateRunning {
			r.receiveEnqueue()
			r.processPendingEvents(rxcallback)
		}

		r.closeTxChannels()
	}()
}

//==========================
// serverSeven TIO handlers
//==========================
func (r *serverSeven) M7requestng(ev EventInterface) error {
	log(r.String(), "::M7requestng()", ev.String())

	tioevent := ev.(*McastChunkPutRequestEvent)
	tio := tioevent.GetTio()
	gwy := tioevent.GetSource()
	ngobj := tioevent.GetGroup()
	assert(ngobj.hasmember(r))

	// respond to the put-request with a bid
	bidev := newBidEvent(r, gwy, ngobj, tioevent.cid, rand.Intn(configReplicast.sizeNgtGroup), tio)
	log("srv-bid", bidev.String())

	tio.next(bidev)
	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
	return nil
}

func (r *serverSeven) M7acceptng(ev EventInterface) error {
	tioevent := ev.(*McastChunkPutAcceptEvent)
	log(r.String(), "::M7acceptng()", tioevent.String())
	gwy := tioevent.GetSource()
	ngobj := tioevent.GetGroup()
	assert(ngobj.hasmember(r))

	rzvgroup := tioevent.rzvgroup
	if rzvgroup.hasmember(r) {
		log(r.String(), "has been ACCEPTED")
		//new server's flow
		tio := tioevent.GetTio()
		flow := NewFlow(gwy, r, tioevent.cid, 0, tio)
		flow.totalbytes = tioevent.sizeb
		r.flowsfrom.insertFlow(flow)
	}

	return nil
}

//==================================================================
//
// modelSeven interface methods
//
//==================================================================
func (m *modelSeven) NewGateway(i int) RunnerInterface {
	gwy := NewGatewayMcast(i, m7.putpipeline)
	gwy.rb = NewRateBucket(
		configNetwork.maxratebucketval, // maxval
		configNetwork.linkbps,          // rate
		configNetwork.maxratebucketval) // value
	rgwy := &gatewaySeven{*gwy}
	rgwy.rptr = rgwy
	return rgwy
}

func (m *modelSeven) NewServer(i int) RunnerInterface {
	srv := NewServerUch(i, m5.putpipeline)
	rsrv := &serverSeven{*srv}
	rsrv.ServerUch.rptr = rsrv
	rsrv.flowsfrom = NewFlowDir(rsrv, config.numGateways)
	return rsrv
}

func (m *modelSeven) Configure() {
	configNetwork.sizeControlPDU = 100
	rem := config.numServers % configReplicast.sizeNgtGroup
	if rem > 0 {
		// FIXME: model.go to support late numServers setting
		config.numServers -= rem
		if config.numServers == configReplicast.sizeNgtGroup {
			log(LogBoth, "Cannot execute the model with a single negotiating group configured, exiting..")
			os.Exit(1)
		}
		log(LogBoth, "NOTE: adjusting the number of servers down, to divide by size of the negotiating group:", config.numServers)
	}
}
