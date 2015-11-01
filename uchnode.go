package surge

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

//
// callback type
//
type setFlowRateBucketCallback func(flow *Flow)

//===============================================================
// GatewayUch
//===============================================================
type GatewayUch struct {
	RunnerBase
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
	setflowrbcb  setFlowRateBucketCallback
}

func NewGatewayUch(i int, p *Pipeline, cb setFlowRateBucketCallback) *GatewayUch {
	rbase := RunnerBase{id: i, strtype: "GWY"}
	gwy := &GatewayUch{RunnerBase: rbase}

	gwy.putpipeline = p
	gwy.setflowrbcb = cb
	gwy.init(config.numServers)
	gwy.flowsto = NewFlowDir(gwy, config.numServers)

	return gwy
}

func (r *GatewayUch) realobject() RunnerInterface {
	return r.rptr
}

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

//======================
// Tx
//======================
func (r *GatewayUch) startNewChunk() {
	assert(r.chunk == nil)
	r.chunk = NewChunk(r.realobject(), configStorage.sizeDataChunk*1024)
	r.startNewReplica(1)
}

// send data frames
func (r *GatewayUch) sendata() {
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
		if r.rb.below(newbits) {
			return
		}
		if flow.sendnexts.After(Now) {
			return
		}
		if flow.rb.below(newbits) {
			return
		}
		flow.offset += frsize
		ev := newUchReplicaDataEvent(r.realobject(), srv, r.replica, flow, frsize)
		ev.SetExtension(flow.tio)
		flow.sendnexts = Now.Add(time.Duration(newbits) * time.Second / time.Duration(flow.tobandwidth))
		if r.Send(ev, true) {
			atomic.AddInt64(&r.txbytestats, int64(frsize))
			// starting next replica without waiting for the current one's completion
			if flow.offset >= flow.totalbytes {
				r.finishStartReplica(flow.to, false)
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

func (r *GatewayUch) finishStartReplica(srv RunnerInterface, tiodone bool) {
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

func (r *GatewayUch) startNewReplica(num int) {
	r.replica = NewPutReplica(r.chunk, num)

	tgt := r.selectTarget()
	assert(tgt != nil)

	tio := r.putpipeline.NewTio(r.realobject())
	flow := NewFlow(r.realobject(), tgt, r.chunk.cid, num, tio)
	flow.tobandwidth = int64(0) // cannot transmit until further notice

	r.setflowrbcb(flow)

	flow.totalbytes = r.chunk.sizeb
	r.flowsto.insertFlow(tgt, flow)
	log("gwy-new-flow", flow.String())

	ev := newUchReplicaPutRequestEvent(r.realobject(), tgt, r.replica)
	ev.SetExtension(tio)

	tio.next(ev)
	r.rb.use(int64(configNetwork.sizeControlPDU * 8))
	flow.rb.use(int64(configNetwork.sizeControlPDU * 8))
	flow.sendnexts = Now.Add(time.Duration(configNetwork.sizeControlPDU) * time.Second / time.Duration(configNetwork.linkbpsminus))
	atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
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
	log(LOG_V, "::replicack()", flow.String(), tioevent.String())
	atomic.AddInt64(&r.replicastats, int64(1))
	return nil
}

//
// stats
//
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
// ServerUch
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

func (r *ServerUch) receiveReplicaData(ev *UchReplicaDataEvent) {
	gwy := ev.GetSource()
	flow := r.flowsfrom.get(gwy, true)

	log(LOG_V, "srv-recv-data", flow.String(), ev.String())
	assert(flow.cid == ev.cid)
	assert(flow.num == ev.num)

	x := ev.offset - flow.offset
	assert(x <= configNetwork.sizeFrame, fmt.Sprintf("FATAL: out of order:%d:%s", ev.offset, flow.String()))
	atomic.AddInt64(&r.rxbytestats, int64(x))

	flow.offset = ev.offset
	tio := ev.extension.(*Tio)

	if flow.offset >= flow.totalbytes {
		// postpone the ack until after the replica (chunk.sizeb) is written to disk
		diskdoneintime := r.disk.scheduleWrite(flow.totalbytes)
		r.disk.queue.insertTime(diskdoneintime)

		putackev := newUchReplicaPutAckEvent(r.realobject(), gwy, flow.cid, flow.num, diskdoneintime)
		tio.next(putackev)
		atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))

		log("srv-replica-done-and-gone", flow.String())
		r.flowsfrom.deleteFlow(gwy)
	}
}

//
// stats
//
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
