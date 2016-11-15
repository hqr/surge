package surge

import (
	"fmt"
	"sync/atomic"
	"time"
)

// implements ModelInterface
type modelB struct {
	putpipeline *Pipeline
}

//========================================================================
// mB nodes
//========================================================================
type gatewayB struct {
	GatewayCommon
	target     NodeRunnerInterface
	replica    []*PutReplica
	event      map[int64]*ReplicaPutRequestEvent
	ack        map[int64]bool
	flow       *FlowLong
	cmdwinsize int
}

type serverB struct {
	ServerUch
}

//
// static & init
//
var mB modelB

func init() {
	p := NewPipeline()
	p.AddStage(&PipelineStage{name: "WRITE-BEGIN", handler: "MBwritebegin"})
	p.AddStage(&PipelineStage{name: "WRITE-ACK", handler: "MBwriteack"})

	mB.putpipeline = p

	d := NewStatsDescriptors("b")
	d.registerCommonProtoStats()

	props := make(map[string]interface{}, 1)
	props["description"] = "I => T writing at different burst (window) sizes"
	RegisterModel("b", &mB, props)
}

//==================================================================
//
// gatewayB methods
//
//==================================================================
func (r *gatewayB) finishInit() {
	target := allServers[r.GetID()-1]
	r.flow = &FlowLong{from: r, to: target, rb: r.rb, tobandwidth: configNetwork.linkbps}

	w := configNetwork.cmdWindowSz
	r.cmdwinsize = w

	// preallocate, to avoid runtime allocations & GC
	chunk := make([]*Chunk, w)
	r.replica = make([]*PutReplica, w)
	r.event = make(map[int64]*ReplicaPutRequestEvent, w)
	r.ack = make(map[int64]bool, w)
	for i := 0; i < w; i++ {
		chunk[i] = NewChunk(r, configStorage.sizeDataChunk*1024)
		r.replica[i] = NewPutReplica(chunk[i], 1)

		tio := NewTioOffset(r, r.putpipeline, r.replica[i], target, r.flow, false)
		r.event[tio.GetID()] = newReplicaPutRequestEvent(r, target, r.replica[i], tio)
		r.ack[tio.GetID()] = true
	}
}

func (r *gatewayB) rxcallbackB(ev EventInterface) int {
	tio := ev.GetTio()
	tio.doStage(r.realobject(), ev)
	if tio.Done() {
		log("tio-done", tio.String())
		atomic.AddInt64(&r.tiostats, 1)
	}
	return ev.GetSize()
}

func (r *gatewayB) Run() {
	r.finishInit()
	r.state = RstateRunning

	go func() {
		for r.state == RstateRunning {
			r.receiveEnqueue()
			r.processPendingEvents(r.rxcallbackB)

			if r.rb.below(int64(configNetwork.sizeControlPDU * 8)) {
				continue
			}
			if r.flow.timeTxDone.After(Now) {
				continue
			}
			for _, tioint := range r.tios {
				tio := tioint.(*TioOffset)
				tid := tio.GetID()
				if !r.ack[tid] {
					continue
				}
				// putreqev renewal
				putreqev, ok := r.event[tid]
				assert(ok)
				putreqev.crtime = Now
				putreqev.thtime = Now.Add(configNetwork.durationControlPDU + config.timeClusterTrip)
				putreqev.tiostage = "WRITE-BEGIN" // force tio to execute this event's stage
				r.ack[tid] = false

				// tio renewal
				tio.offset = 0
				_, printid := uqrandom64(r.GetID())
				tio.chunksid = printid
				tio.next(putreqev, SmethodWait)
				r.rb.use(int64(configNetwork.sizeControlPDU * 8))
				r.flow.timeTxDone = Now.Add(configNetwork.durationControlPDU)
				break
			}

			r.sendata()
		}
		r.closeTxChannels()
	}()
}

func (r *gatewayB) MBwriteack(ev EventInterface) error {
	tio := ev.GetTio()
	tid := tio.GetID()
	r.ack[tid] = true

	atomic.AddInt64(&r.replicastats, 1)
	atomic.AddInt64(&r.chunkstats, 1)

	putreqev, _ := r.event[tid]
	chunklatency := Now.Sub(putreqev.GetCreationTime())
	x := int64(chunklatency) / 1000
	log("chunk-done", tio.String(), "latency", chunklatency, x)

	return nil
}

func (r *gatewayB) sendata() {
	if r.flow.timeTxDone.After(Now) {
		return
	}
	for _, tioint := range r.tios {
		tio := tioint.(*TioOffset)
		tid := tio.GetID()
		if r.ack[tid] {
			continue
		}
		if tio.offset >= tio.totalbytes {
			continue
		}
		frsize := int64(configNetwork.sizeFrame)
		if tio.offset+frsize > tio.totalbytes {
			frsize = tio.totalbytes - tio.offset
		}
		newbits := frsize * 8
		if r.rb.below(newbits) {
			continue
		}
		assert(tio.GetTarget() == r.flow.to)
		ev := newRepDataEvent(r.realobject(), r.flow.to, tio, tio.offset, int(frsize))

		// transmit given the current flow's bandwidth
		d := time.Duration(newbits) * time.Second / time.Duration(r.flow.getbw())
		ok := r.Send(ev, SmethodWait)
		assert(ok)

		tio.offset += frsize
		r.flow.timeTxDone = Now.Add(d)
		r.rb.use(newbits)
		break
	}
}

//==================================================================
//
// serverB methods
//
//==================================================================
func (r *serverB) rxcallbackB(ev EventInterface) int {
	tio := ev.GetTio()
	switch ev.(type) {
	case *ReplicaPutRequestEvent:
		tioevent := ev.(*ReplicaPutRequestEvent)
		log(LogVV, "rxcallback: new write", tioevent.String(), tio.String())
		r.addBusyDuration(configNetwork.sizeControlPDU, configNetwork.linkbpsControl, NetControlBusy)
		tio.doStage(r)
	case *ReplicaDataEvent:
		tioevent := ev.(*ReplicaDataEvent)
		log(LogVV, "rxcallback: replica data", tioevent.String(), tio.String())
		r.receiveReplicaData(tioevent)
	default:
		assert(false)
	}

	return ev.GetSize()
}

func (r *serverB) Run() {
	r.state = RstateRunning
	r.disk.SetDiskLatencySimulatorByname(configStorage.diskLatencySim)

	go func() {
		for r.state == RstateRunning {
			r.receiveEnqueue()
			r.processPendingEvents(r.rxcallbackB)
		}

		r.closeTxChannels()
	}()
}

func (r *serverB) MBwritebegin(ev EventInterface) error {
	tioevent := ev.(*ReplicaPutRequestEvent)
	log(r.String(), "::MBwritebegin()", tioevent.GetTio().String(), tioevent.String())

	// TODO
	return nil
}

func (r *serverB) receiveReplicaData(ev *ReplicaDataEvent) int {
	r.addBusyDuration(ev.GetSize(), configNetwork.linkbps, NetDataBusy)

	tio := ev.GetTio().(*TioOffset)
	if ev.offset < tio.totalbytes-int64(configNetwork.sizeFrame) {
		return ReplicaNotDoneYet
	}
	// queue to disk and ACK right away
	atdisk := r.disk.scheduleWrite(int(tio.totalbytes))
	r.addBusyDuration(int(tio.totalbytes), configStorage.diskbps, DiskBusy)

	gwy := ev.GetSource()
	putackev := newReplicaPutAckEvent(r.realobject(), gwy, tio, atdisk)
	tio.next(putackev, SmethodWait)

	log("write-received", tio.String(), atdisk)
	return ReplicaDone
}

//==================================================================
//
// modelB interface methods
//
//==================================================================
func (m *modelB) NewGateway(i int) NodeRunnerInterface {
	gwy := NewGatewayCommon(i, mB.putpipeline)
	maxval := int64(configNetwork.sizeFrame*8) + int64(configNetwork.sizeControlPDU*8)
	gwy.rb = NewRateBucket(maxval, configNetwork.linkbps, maxval)
	rgwy := &gatewayB{GatewayCommon: *gwy}
	rgwy.rptr = rgwy

	return rgwy
}

func (m *modelB) NewServer(i int) NodeRunnerInterface {
	srv := NewServerUchRegChannels(i, mB.putpipeline)

	srv.rb = &DummyRateBucket{}

	rsrv := &serverB{*srv}
	rsrv.ServerUch.rptr = rsrv
	rsrv.flowsfrom = NewFlowDir(rsrv, config.numGateways)
	return rsrv
}

func (m *modelB) Configure() {}

func (m *modelB) PreConfig() {
	RegisterDiskLatencySimulator(&latencyParabola)
}

func (m *modelB) PostConfig() {
}
