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
	newflow(target interface{}, args ...interface{}) *Flow
}

const (
	ReplicaNotDoneYet int = iota
	ReplicaDone
)

const (
	ChunkNotDoneYet int = iota
	ChunkDone
)

//===============================================================
//
// GatewayCommon
// common for (inherited by) both unicast and multicast gateway types
//
//===============================================================
type GatewayCommon struct {
	RunnerBase
	ffi          FlowFactoryInterface
	tiostats     int64
	chunkstats   int64
	replicastats int64
	chunk        *Chunk
	rb           RateBucketInterface
	rptr         RunnerInterface // real object
	putpipeline  *Pipeline
	numreplicas  int
}

func NewGatewayCommon(i int, p *Pipeline) *GatewayCommon {
	rbase := RunnerBase{id: i, strtype: "GWY"}
	gwy := &GatewayCommon{RunnerBase: rbase}

	gwy.putpipeline = p
	gwy.init(config.numServers)
	gwy.initios()

	return gwy
}

// realobject stores to pointer the the gateway's instance that embeds this
// instance of GatewayUch
func (r *GatewayCommon) realobject() RunnerInterface {
	return r.rptr
}

func (r *GatewayCommon) replicackCommon(tioevent *ReplicaPutAckEvent) int {
	tio := tioevent.GetTio()
	flow := tio.flow

	log(LogV, "::replicack()", flow.String(), tioevent.String())
	atomic.AddInt64(&r.replicastats, int64(1))

	r.numreplicas++
	log("replica-acked", flow.String(), "num-acked", r.numreplicas, "by", tioevent.GetSource().String())
	if r.numreplicas < configStorage.numReplicas {
		return ChunkNotDoneYet
	}
	chunklatency := Now.Sub(r.chunk.crtime)
	log("chunk-done", r.chunk.String(), "latency", chunklatency)
	atomic.AddInt64(&r.chunkstats, int64(1))
	r.chunk = nil
	return ChunkDone
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
func (r *GatewayCommon) GetStats(reset bool) NodeStats {
	s := r.RunnerBase.GetStats(true)
	if reset {
		s["tio"] = atomic.SwapInt64(&r.tiostats, 0)
		s["chunk"] = atomic.SwapInt64(&r.chunkstats, 0)
		s["replica"] = atomic.SwapInt64(&r.replicastats, 0)
	} else {
		s["tio"] = atomic.LoadInt64(&r.tiostats)
		s["chunk"] = atomic.LoadInt64(&r.chunkstats)
		s["replica"] = atomic.LoadInt64(&r.replicastats)
	}
	return s
}

//===============================================================
//
// GatewayUch
// generic unicast CH gateway providing common Tx, ACK and server selection
// to specific unicast "flavors"
//
//===============================================================
type GatewayUch struct {
	GatewayCommon
	chunktargets []RunnerInterface
	replica      *PutReplica
}

// NewGatewayUch constructs GatewayUch. The latter embeds RunnerBase and must in turn
// be embedded by a concrete and ultimate modeled gateway.
func NewGatewayUch(i int, p *Pipeline) *GatewayUch {
	g := NewGatewayCommon(i, p)
	gwy := &GatewayUch{GatewayCommon: *g}
	gwy.chunktargets = make([]RunnerInterface, configStorage.numReplicas)
	return gwy
}

// selectTargets randomly selects numReplicas targets for a new chunk
func (r *GatewayUch) selectTargets() {
	arr := make([]RunnerInterface, configStorage.numReplicas)
	numPeers := cap(r.eps) - 1
	idx := 0
	cnt := 0
	for {
		peeridx := rand.Intn(numPeers) + 1
		peer := r.eps[peeridx]
		found := false
		for _, srv := range r.chunktargets {
			if srv != nil && peer.GetID() == srv.GetID() {
				found = true
				break
			}
		}
		if found {
			continue
		}
		for _, srv := range arr {
			if srv != nil && peer.GetID() == srv.GetID() {
				found = true
				break
			}
		}
		if found {
			continue
		}
		arr[idx] = peer
		idx++
		if idx >= configStorage.numReplicas {
			break
		}
		cnt++
		assert(cnt < numPeers*2, "failed to select targets")
	}
	for idx = range r.chunktargets {
		r.chunktargets[idx] = arr[idx]
	}
	log(r.String(), " => ", fmt.Sprintf("%v", r.chunktargets))
}

//===
// Tx
//===
func (r *GatewayUch) startNewChunk() {
	assert(r.chunk == nil)
	r.chunk = NewChunk(r.realobject(), configStorage.sizeDataChunk*1024)
	r.numreplicas = 0
	r.selectTargets()
	r.startNewReplica(1)
}

// startNewReplica allocates new replica and tio to drive the former
// through the pipeline. Flow peer-to-peer object is created as well
// here, subject to the concrete modeled protocol
func (r *GatewayUch) startNewReplica(num int) {
	r.replica = NewPutReplica(r.chunk, num)

	tgt := r.chunktargets[num-1]
	assert(tgt != nil)

	flow := r.ffi.newflow(tgt, num)

	log("gwy-new-flow", flow.String())

	ev := newReplicaPutRequestEvent(r.realobject(), tgt, r.replica, flow.tio)

	flow.tio.next(ev, SmethodWait)
	r.rb.use(int64(configNetwork.sizeControlPDU * 8))
	flow.rb.use(int64(configNetwork.sizeControlPDU * 8))
	flow.timeTxDone = Now.Add(configNetwork.durationControlPDU) // FIXME: assuming 10GE
}

// sendata sends data packets (frames) in the form of ReplicaDataEvent,
// each carrying configNetwork.sizeFrame bytes (9000 jumbo by default)
// The sending is throttled both by the gateway's own egress link (r.rb)
// and the end-to-end flow (flow,rb)
func (r *GatewayUch) sendata() {
	for _, tio := range r.tios {
		flow := tio.flow
		srv := tio.target
		assert(flow.to == srv)
		if flow.tobandwidth == 0 || flow.offset >= r.chunk.sizeb {
			continue
		}
		frsize := configNetwork.sizeFrame
		if flow.offset+frsize > r.chunk.sizeb {
			frsize = r.chunk.sizeb - flow.offset
		}
		newbits := int64(frsize * 8)
		// check with the gateway's egress link implemented as ratebucket
		if r.rb.below(newbits) {
			continue
		}
		// must ensure previous send is fully done
		if flow.timeTxDone.After(Now) {
			continue
		}
		// check with the flow's own ratebucket
		if flow.rb.below(newbits) {
			continue
		}

		// check with the receiver's ratebucket
		rb := srv.GetRateBucket()
		if rb != nil {
			rbprotected := rb.(*RateBucketProtected)
			if !rbprotected.use(newbits) {
				continue
			}
		}

		flow.offset += frsize
		ev := newReplicaDataEvent(r.realobject(), srv, r.replica, flow, frsize, flow.tio)

		// transmit given the current flow's bandwidth
		d := time.Duration(newbits) * time.Second / time.Duration(flow.tobandwidth)
		flow.timeTxDone = Now.Add(d)
		ok := r.Send(ev, SmethodWait)
		assert(ok)
		// starting next replica without waiting for the current one's completion
		if flow.offset >= flow.totalbytes {
			r.finishStartReplica(flow)
		} else {
			flow.rb.use(newbits)
			flow.tobandwidth = flow.rb.getrate()
			r.rb.use(newbits)
		}
	}
}

// finishStartReplica finishes the replica or the entire chunk, the latter
// when all the required configStorage.numReplicas copies are fully stored
func (r *GatewayUch) finishStartReplica(flow *Flow) {
	log("replica-fully-transmitted", flow.String(), r.replica.String())
	flow.tobandwidth = 0
	tio := flow.tio
	assert(flow.repnum == tio.repnum)
	if tio.repnum < configStorage.numReplicas {
		r.startNewReplica(tio.repnum + 1)
	}
}

//
// ReplicaPutAck handler
//
func (r *GatewayUch) replicack(ev EventInterface) error {
	tioevent := ev.(*ReplicaPutAckEvent)
	tio := tioevent.GetTio()
	flow := tio.flow
	assert(flow.cid == tioevent.cid)
	assert(flow.cid == tio.cid)
	assert(flow.repnum == tioevent.num)

	r.replicackCommon(tioevent)
	return nil
}

//===============================================================
//
// GatewayMcast
//
//===============================================================
type GatewayMcast struct {
	GatewayCommon
	rzvgroup *RzvGroup
}

func NewGatewayMcast(i int, p *Pipeline) *GatewayMcast {
	gwy := NewGatewayCommon(i, p)
	srvrs := make([]RunnerInterface, configStorage.numReplicas)
	return &GatewayMcast{GatewayCommon: *gwy, rzvgroup: &RzvGroup{servers: srvrs}}
}

//===
// Tx
//===
// selectNgtGroup randomly selects target negotiating group and returns its ID
// for better pseudo-randomness try to select a different group for this gateway
func (r *GatewayMcast) selectNgtGroup(cid int64, prevgroupid int) int {
	id := int(cid%int64(configReplicast.numNgtGroups)) + 1
	if id != prevgroupid {
		return id
	}
	id = rand.Intn(configReplicast.numNgtGroups) + 1
	if id != prevgroupid {
		return id
	}
	id = prevgroupid + 1
	if id > configReplicast.numNgtGroups {
		id = 1
	}
	return id
}

//===============================================================
//
// ServerUch
//
//===============================================================
type ServerUch struct {
	RunnerBase
	flowsfrom      *FlowDir
	disk           *Disk
	rptr           RunnerInterface // real object
	putpipeline    *Pipeline       // tio pipeline
	busyduration   int64           // time.Duration receive-link busy (alternative)
	timeResetStats time.Time
	rb             RateBucketInterface
}

// NewServerUch constructs ServerUch. The latter embeds RunnerBase and must in turn
// be embedded
func NewServerUch(i int, p *Pipeline) *ServerUch {
	rbase := RunnerBase{id: i, strtype: "SRV"}
	srv := &ServerUch{RunnerBase: rbase}

	srv.putpipeline = p
	srv.init(config.numGateways)
	srv.disk = NewDisk(srv, configStorage.diskMBps)
	srv.timeResetStats = Now

	return srv
}

func (r *ServerUch) realobject() RunnerInterface {
	return r.rptr
}

func (r *ServerUch) GetRateBucket() RateBucketInterface {
	return r.rb
}

// receiveReplicaData is executed in the UCH server's receive data path for
// each received event of the type (*ReplicaPutAckEvent). The event itself
// carries (or rather, is modeled to carry) a full or partial frame, that is,
// up to configNetwork.sizeFrame bytes.  In addition
// to taking care of the flow.offset and receive side statistics, the routine
// may generate an ACK acknowledging receiption of the full replica.
// Delivery of this ACK is accomplished via the corresponding tio that controls
// passage of the stages in accordance with the modeled IO pipeline.
// (putting a single replica is a single transaction that will typpically include
// 3 or more IO stages)
//
func (r *ServerUch) receiveReplicaData(ev *ReplicaDataEvent) int {
	gwy := ev.GetSource()
	flow := r.flowsfrom.get(gwy, true)
	group := ev.GetGroup()

	log(LogV, "srv-recv-data", flow.String(), ev.String())
	assert(flow.cid == ev.cid)
	assert(group != nil || flow.repnum == ev.num)

	r.addBusyDuration(ev.GetSize(), flow.tobandwidth)

	x := ev.offset - flow.offset
	assert(x <= configNetwork.sizeFrame, fmt.Sprintf("FATAL: out of order:%d:%s", ev.offset, flow.String()))

	flow.offset = ev.offset

	if flow.offset < flow.totalbytes {
		return ReplicaNotDoneYet
	}
	//
	// postpone the ack until after the replica (chunk.sizeb) is written to disk
	//
	atdisk := r.disk.scheduleWrite(flow.totalbytes)

	tio := ev.GetTio()
	putackev := newReplicaPutAckEvent(r.realobject(), gwy, flow, tio, atdisk)
	if group != nil {
		// pass the targetgroup back, to validate by the mcasting gwy
		putackev.setOneArg(group)
	}
	gwyacktime := fmt.Sprintf("%-12.10v", putackev.GetTriggerTime().Sub(time.Time{}))
	tio.next(putackev, SmethodWait)

	cstr := ""
	if flow.repnum != 0 {
		cstr = fmt.Sprintf("chunk#%d(%d)", flow.sid, flow.repnum)
	} else {
		cstr = fmt.Sprintf("chunk#%d", flow.sid)
	}
	log("srv-replica-received", r.String(), cstr, "replica-ack-scheduled", gwyacktime)
	r.flowsfrom.deleteFlow(gwy)
	return ReplicaDone
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
	num, _ := r.disk.queueDepth(DqdBuffers)
	s["disk-queue-depth"] = int64(num)

	var d int64
	if reset {
		d = atomic.SwapInt64(&r.busyduration, 0)
	} else {
		d = atomic.LoadInt64(&r.busyduration)
	}
	elapsed := int64(Now.Sub(r.timeResetStats))
	if d >= elapsed {
		s["rxidle"] = 0
	} else {
		s["rxidle"] = (elapsed - d) * 100 / elapsed
	}
	if reset {
		r.timeResetStats = Now
	}

	return s
}

// link busy time based on the link's full bandwidth
// e.g., when a 10GE link is constantly used to send/receive at 5Gpbs,
// the link's busy percentage will compute as 50%
func (r *ServerUch) addBusyDuration(sizeb int, bandwidth int64) {
	d := time.Duration(sizeb*8) * time.Second / time.Duration(configNetwork.linkbps)
	atomic.AddInt64(&r.busyduration, int64(d))
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
	hasmember(r RunnerInterface) bool
	getmembers() []RunnerInterface

	// FIXME: not used
	SendGroup(ev EventInterface, how SendMethodEnum, cloner EventClonerInterface) bool

	String() string
}

//
// Negotiating Group
//
type NgtGroup struct {
	id int
}

func NewNgtGroup(id int) *NgtGroup {
	assert(id > 0)
	assert(id*configReplicast.sizeNgtGroup <= config.numServers)
	return &NgtGroup{id}
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

func (g *NgtGroup) hasmember(r RunnerInterface) bool {
	firstidx := (g.id - 1) * configReplicast.sizeNgtGroup
	for idx := 0; idx < configReplicast.sizeNgtGroup; idx++ {
		srv := allServers[firstidx+idx]
		if r == srv {
			return true
		}
	}
	return false
}

func (g *NgtGroup) getmembers() []RunnerInterface {
	firstidx := (g.id - 1) * configReplicast.sizeNgtGroup
	return allServers[firstidx : firstidx+configReplicast.sizeNgtGroup]
}

func (g *NgtGroup) SendGroup(origev EventInterface, how SendMethodEnum, cloner EventClonerInterface) bool {
	assert(how == SmethodDirectInsert)
	firstidx := (g.id - 1) * configReplicast.sizeNgtGroup
	for idx := 0; idx < configReplicast.sizeNgtGroup; idx++ {
		srv := allServers[firstidx+idx]
		ev := origev
		if idx > 0 {
			ev = cloner.clone(origev)
		}
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
	ngtid   int
}

func (g *RzvGroup) init(ngtid int, cleanup bool) {
	if cleanup {
		for idx := range g.servers {
			g.servers[idx] = nil
		}
		return
	}
	g.ngtid = ngtid
	g.id = 0
	for _, srv := range g.servers {
		if srv == nil {
			g.id = 0
			return
		}
		assert(srv.GetID() < 1000)
		g.id = int64(1000)*g.id + int64(srv.GetID())
	}
}

func (g *RzvGroup) getID() int {
	return int(g.id)
}

func (g *RzvGroup) getID64() int64 {
	return g.id
}

func (g *RzvGroup) getCount() int {
	for _, srv := range g.servers {
		if srv == nil {
			return 0
		}
	}
	return len(g.servers)
}

func (g *RzvGroup) hasmember(r RunnerInterface) bool {
	for _, srv := range g.servers {
		if srv != nil && r == srv {
			return true
		}
	}
	return false
}

func (g *RzvGroup) getmembers() []RunnerInterface {
	return g.servers
}

func (g *RzvGroup) SendGroup(origev EventInterface, how SendMethodEnum, cloner EventClonerInterface) bool {
	assert(how == SmethodDirectInsert)
	for idx, srv := range g.servers {
		ev := origev
		if idx > 0 {
			ev = cloner.clone(origev)
		}
		ev.setOneArg(srv)
		srv.Send(ev, SmethodDirectInsert)
	}
	return true
}

func (g *RzvGroup) String() string {
	s := ""
	for idx := range g.servers {
		if g.servers[idx] == nil {
			s += "<nil>"
		} else {
			s += g.servers[idx].String()
		}
		if idx < len(g.servers)-1 {
			s += ","
		}
	}
	return fmt.Sprintf("[rzv-group %s]", s)
}
