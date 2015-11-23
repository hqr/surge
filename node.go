// Package surge provides a framework for discrete event simulation, as well as
// a number of models for Unsolicited and Reservation Group based Edge-driven
// load balancing. Targeted modeling area includes large and super-large storage
// clusters with multiple access points (referred to as "gateways") and multiple
// storage targets (referred to as "servers").
//
// GatewayUch and ServerUch implement common Unicast-Consistent-Hashing
// gateway and server, respectively.
package surge

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

//===============================================================
//
// FlowFactoryInterface
//
//===============================================================
type FlowFactoryInterface interface {
	newflow(target interface{}, repnum int) *Flow
}

//===============================================================
//
// GatewayUch
//
//===============================================================
type GatewayUch struct {
	RunnerBase
	ffi          FlowFactoryInterface
	tiostats     int64
	chunkstats   int64
	replicastats int64
	txbytestats  int64
	rxbytestats  int64
	chunk        *Chunk
	replica      *PutReplica
	flowsto      *FlowDir
	rb           RateBucketInterface
	rptr         RunnerInterface // real object
	putpipeline  *Pipeline
	numreplicas  int
}

// NewGatewayUch constructs GatewayUch. The latter embeds RunnerBase and must in turn
// be embedded by a concrete and ultimate modeled gateway.
func NewGatewayUch(i int, p *Pipeline) *GatewayUch {
	rbase := RunnerBase{id: i, strtype: "GWY"}
	gwy := &GatewayUch{RunnerBase: rbase}

	gwy.putpipeline = p
	gwy.init(config.numServers)

	return gwy
}

// realobject stores to pointer the the gateway's instance that embeds this
// instance of GatewayUch
func (r *GatewayUch) realobject() RunnerInterface {
	return r.rptr
}

// selectTarget randomly selects target server that is not yet talking to this
// particular gateway.
func (r *GatewayUch) selectTarget() RunnerInterface {
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

//===
// Tx
//===
func (r *GatewayUch) startNewChunk() {
	assert(r.chunk == nil)
	r.chunk = NewChunk(r.realobject(), configStorage.sizeDataChunk*1024)
	r.numreplicas = 0
	r.startNewReplica(1)
}

// startNewReplica allocates new replica and tio to drive the former
// through the pipeline. Flow peer-to-peer object is created as well
// here, subject to the concrete modeled protocol
func (r *GatewayUch) startNewReplica(num int) {
	r.replica = NewPutReplica(r.chunk, num)

	tgt := r.selectTarget()
	assert(tgt != nil)

	flow := r.ffi.newflow(tgt, num)

	log("gwy-new-flow", flow.String())

	ev := newUchReplicaPutRequestEvent(r.realobject(), tgt, r.replica, flow.tio)

	flow.tio.next(ev)
	r.rb.use(int64(configNetwork.sizeControlPDU * 8))
	flow.rb.use(int64(configNetwork.sizeControlPDU * 8))
	flow.sendnexts = Now.Add(time.Duration(configNetwork.sizeControlPDU) * time.Second / time.Duration(configNetwork.linkbpsminus))
	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
}

// sendata sends data packets (frames) in the form of UchReplicaDataEvent,
// each carrying configNetwork.sizeFrame bytes (9000 jumbo by default)
// The sending is throttled both by the gateway's own egress link (r.rb)
// and the end-to-end flow (flow,rb)
func (r *GatewayUch) sendata() {
	q := r.txqueue
	for k := 0; k < len(q.fifo); k++ {
		ev := q.fifo[k]
		if r.Send(ev, SmethodDontWait) {
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
		// check with the gateway's egress link implemented as ratebucket
		if r.rb.below(newbits) {
			return
		}
		// must ensure previous send is fully done
		if flow.sendnexts.After(Now) {
			return
		}
		// check with the flow's own ratebucket
		if flow.rb.below(newbits) {
			return
		}
		flow.offset += frsize
		ev := newUchReplicaDataEvent(r.realobject(), srv, r.replica, flow, frsize, flow.tio)
		// time for the bits to get fully transmitted given the current flow's bandwidth
		flow.sendnexts = Now.Add(time.Duration(newbits) * time.Second / time.Duration(flow.tobandwidth))
		if r.Send(ev, SmethodWait) {
			atomic.AddInt64(&r.txbytestats, int64(frsize))
			// starting next replica without waiting for the current one's completion
			if flow.offset >= flow.totalbytes {
				r.finishStartReplica(flow.to)
			} else {
				flow.rb.use(newbits)
				flow.tobandwidth = flow.rb.getrate()
				r.rb.use(newbits)
			}
		} else {
			q.insertEvent(ev)
			assert(false, "async send not supported yet")
		}
	}
	r.flowsto.apply(applyCallback)
}

// finishStartReplica finishes the replica or the entire chunk, the latter
// when all the required configStorage.numReplicas copies are fully stored
func (r *GatewayUch) finishStartReplica(srv RunnerInterface) {
	flow := r.flowsto.get(srv, true)

	log("replica-fully-transmitted", flow.String(), r.replica.String())
	flow.tobandwidth = 0
	if flow.num < configStorage.numReplicas {
		r.startNewReplica(flow.num + 1)
	}
}

//
// ReplicaPutAck handler
//
func (r *GatewayUch) replicack(ev EventInterface) error {
	tioevent := ev.(*UchReplicaPutAckEvent)
	srv := tioevent.GetSource()
	flow := r.flowsto.get(srv, true)
	assert(flow.cid == tioevent.cid)
	assert(flow.num == tioevent.num)
	log(LogV, "::replicack()", flow.String(), tioevent.String())
	atomic.AddInt64(&r.replicastats, int64(1))

	log("replica-acked-flow-gone", flow.String())
	r.numreplicas++
	if r.numreplicas == configStorage.numReplicas {
		log("chunk-done", r.chunk.String())
		atomic.AddInt64(&r.chunkstats, int64(1))
		r.chunk = nil
	}
	r.flowsto.deleteFlow(srv)
	return nil
}

//
// stats
//
// GetStats implements the corresponding RunnerInterface method for the
// GatewayUch common counters. Some of them are inc-ed inside this module,
// others - elsewhere, for instance in the concrete gateway's instance
// that embeds this GatewayUch
// The caller (such as, e.g., stats.go) will typically collect all the
// atomic counters and reset them to zeros to collect new values with the
// next iteration..
func (r *GatewayUch) GetStats(reset bool) NodeStats {
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

//===============================================================
//
// GatewayMcast
//
//===============================================================
type GatewayMcast struct {
	GatewayUch
	flowsto *FlowDirMcast
}

func NewGatewayMcast(i int, p *Pipeline) *GatewayMcast {
	gwy := NewGatewayUch(i, p)
	return &GatewayMcast{*gwy, nil}
}

//===
// Tx
//===
func (r *GatewayMcast) startNewChunk() {
	assert(r.chunk == nil)
	r.chunk = NewChunk(r.realobject(), configStorage.sizeDataChunk*1024)
	r.numreplicas = 0
	r.startNewReplica(1)
}

// startNewReplica allocates new replica and tio to drive the former
// through the pipeline. Flow peer-to-peer object is created as well
// here, subject to the concrete modeled protocol
func (r *GatewayMcast) startNewReplica(repnum int) {
	r.replica = NewPutReplica(r.chunk, repnum)
	ngid := r.selectNgtGroup()
	ngobj := NewNgtGroup(ngid)
	flow := r.ffi.newflow(ngobj, repnum)

	log("gwy-new-flow", flow.String())

	ev := newMcastReplicaPutRequestEvent(r.realobject(), ngobj, r.replica, flow.tio)

	flow.tio.next(ev)
	r.rb.use(int64(configNetwork.sizeControlPDU * 8))
	flow.rb.use(int64(configNetwork.sizeControlPDU * 8))
	flow.sendnexts = Now.Add(time.Duration(configNetwork.sizeControlPDU) * time.Second / time.Duration(configNetwork.linkbpsminus))
	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
}

// selectNgtGroup randomly selects target negotiating group and returns its ID
func (r *GatewayMcast) selectNgtGroup() int {
	numGroups := config.numServers / configReplicast.sizeNgtGroup
	assert(numGroups > 1)
	return rand.Intn(numGroups) + 1
}

//===============================================================
//
// ServerUch
//
//===============================================================
type ServerUch struct {
	RunnerBase
	txbytestats int64
	rxbytestats int64
	flowsfrom   *FlowDir
	disk        *Disk
	rptr        RunnerInterface // real object
	putpipeline *Pipeline
}

// NewServerUch constructs ServerUch. The latter embeds RunnerBase and must in turn
// be embedded
func NewServerUch(i int, p *Pipeline) *ServerUch {
	rbase := RunnerBase{id: i, strtype: "SRV"}
	srv := &ServerUch{RunnerBase: rbase}

	srv.putpipeline = p
	srv.init(config.numGateways)
	srv.flowsfrom = NewFlowDir(srv, config.numGateways)
	srv.disk = NewDisk(srv, configStorage.diskMBps)

	return srv
}

func (r *ServerUch) realobject() RunnerInterface {
	return r.rptr
}

// receiveReplicaData is executed in the UCH server's receive data path for
// each received event of the type (*UchReplicaPutAckEvent). The event itself
// carries (or rather, is modeled to carry) a full or partial frame, that is,
// up to configNetwork.sizeFrame bytes.  In addition
// to taking care of the flow.offset and receive side statistics, the routine
// may generate an ACK acknowledging receiption of the full replica.
// Delivery of this ACK is accomplished via the corresponding tio that controls
// passage of the stages in accordance with the modeled IO pipeline.
// (putting a single replica is a single transaction that will typpically include
// 3 or more IO stages)
//
func (r *ServerUch) receiveReplicaData(ev *UchReplicaDataEvent) {
	gwy := ev.GetSource()
	flow := r.flowsfrom.get(gwy, true)

	log(LogV, "srv-recv-data", flow.String(), ev.String())
	assert(flow.cid == ev.cid)
	assert(flow.num == ev.num)

	x := ev.offset - flow.offset
	assert(x <= configNetwork.sizeFrame, fmt.Sprintf("FATAL: out of order:%d:%s", ev.offset, flow.String()))
	atomic.AddInt64(&r.rxbytestats, int64(x))

	flow.offset = ev.offset
	tio := ev.GetTio()

	if flow.offset >= flow.totalbytes {
		// postpone the ack until after the replica (chunk.sizeb) is written to disk
		diskdoneintime := r.disk.scheduleWrite(flow.totalbytes)
		r.disk.queue.insertTime(diskdoneintime)

		putackev := newUchReplicaPutAckEvent(r.realobject(), gwy, flow.cid, flow.num, diskdoneintime)
		ts := fmt.Sprintf("%-12.10v", putackev.GetTriggerTime().Sub(time.Time{}))
		tio.next(putackev)
		atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))

		log("srv-replica-done-and-gone", flow.String(), ts)
		r.flowsfrom.deleteFlow(gwy)
	}
}

//
// stats
//
// GetStats implements the corresponding RunnerInterface method for the
// ServerUch common counters. Some of them may be inc-ed inside this module,
// others - elsewhere, for instance in the concrete server's instance
// that embeds this GatewayUch
// The caller (such as, e.g., stats.go) will typically collect all the
// atomic counters and reset them to zeros to collect new values with the
// next iteration..
func (r *ServerUch) GetStats(reset bool) NodeStats {
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

//===============================================================
//
// Node Group: interface and specific wrappers
//
//===============================================================
type GroupInterface interface {
	getID() int // CH, random selection
	getID64() int64
	getCount() int

	SendGroup(ev EventInterface, how SendMethodEnum) bool
	String() string
}

//
// Negotiating Group
//
type NgtGroup struct {
	id int
}

func NewNgtGroup(id int) *NgtGroup {
	assert(id*configReplicast.sizeNgtGroup <= config.numServers)
	return &NgtGroup{}
}

func (g *NgtGroup) getID() int {
	return g.id
}

func (g *NgtGroup) getID64() int64 {
	return int64(g.id)
}

func (g *NgtGroup) getCount() int {
	return configReplicast.sizeNgtGroup
}

func (g *NgtGroup) SendGroup(ev EventInterface, how SendMethodEnum) bool {
	assert(how == SmethodDirectInsert)
	firstidx := (g.id - 1) * configReplicast.sizeNgtGroup
	for idx := 0; idx < configReplicast.sizeNgtGroup; idx++ {
		srv := allServers[firstidx+idx]
		ev.setOneArg(srv)
		srv.Send(ev, SmethodDirectInsert)
	}
	return true
}

func (g *NgtGroup) String() string {
	firstidx := (g.id - 1) * configReplicast.sizeNgtGroup
	s1 := allServers[firstidx].String()
	s2 := allServers[firstidx+configReplicast.sizeNgtGroup-1].String()
	return fmt.Sprintf("[ngt-group %s..%s]", s1, s2)
}

//
// Rendezvous Group
//
type RzvGroup struct {
	id      int64
	servers []RunnerInterface
}

func NewRzvGroup(srv []RunnerInterface) *RzvGroup {
	id := int64(0)
	for _, srv := range srv {
		assert(srv.GetID() < 1000)
		id = int64(1000)*id + int64(srv.GetID())
	}
	return &RzvGroup{id, srv}
}

func (g *RzvGroup) getID() int {
	return int(g.id)
}

func (g *RzvGroup) getID64() int64 {
	return g.id
}

func (g *RzvGroup) getCount() int {
	return len(g.servers)
}

func (g *RzvGroup) SendGroup(ev EventInterface, how SendMethodEnum) bool {
	assert(how == SmethodDirectInsert)
	for _, srv := range g.servers {
		ev.setOneArg(srv)
		srv.Send(ev, SmethodDirectInsert)
	}
	return true
}

func (g *RzvGroup) String() string {
	s := ""
	for idx := range g.servers {
		s += g.servers[idx].String()
		if idx < len(g.servers)-1 {
			s += ","
		}
	}
	return fmt.Sprintf("[rzv-group %s]", s)
}
