package surge

import (
	"fmt"
	"sync/atomic"
	"time"
)

type modelNine struct {
	putpipeline *Pipeline
}

//========================================================================
// m9 nodes
//========================================================================
type gatewayNine struct {
	gatewayEight
}

type serverNine struct {
	serverEight
}

//
// static & init
//
var m9 modelNine

func init() {
	p := NewPipeline()
	p.AddStage(&PipelineStage{name: "REQUEST-NG", handler: "M8requestng"})
	p.AddStage(&PipelineStage{name: "BID", handler: "M8receivebid"})
	p.AddStage(&PipelineStage{name: "ACCEPT-NG", handler: "M9acceptng"})
	p.AddStage(&PipelineStage{name: "REPLICA-ACK", handler: "M8replicack"})

	m9.putpipeline = p

	d := NewStatsDescriptors("9")
	d.registerCommonProtoStats()

	props := make(map[string]interface{}, 1)
	props["description"] = "Replicast-H: multicast control with unicast data delivery, double-booking and late rejection"
	RegisterModel("9", &m9, props)
}

//==================================================================
//
// gatewayNine methods
//
//==================================================================
func (r *gatewayNine) rxcallback(ev EventInterface) int {
	switch ev.(type) {
	case *LateRejectBidEvent:
		rjev := ev.(*LateRejectBidEvent)
		log("GWY::rxcallback", rjev.String())
		r.rejectBid(rjev)
	default:
		// execute generic tio.doStage() pipeline
		r.gatewayEight.rxcallback(ev)
	}
	return ev.GetSize()
}

// FIXME: copy-paste - fix passing type-specific rxcallback (above)
//
func (r *gatewayNine) Run() {
	r.state = RstateRunning

	go func() {
		for r.state == RstateRunning {
			// previous chunk done? Tx link is available? If yes,
			// start a new put-chunk operation
			if r.chunk == nil {
				if r.rb.above(int64(configNetwork.sizeControlPDU * 8)) {
					r.startNewChunk()
				}
			}
			// recv
			r.receiveEnqueue()
			r.processPendingEvents(r.rxcallback)

			// the second condition (rzvgroup.getCount() ...)
			// corresponds to the post-negotiation phase when
			// all the bids are already received, "filtered" via findBestIntersection()
			// and the corresponding servers selected for the chunk transfer.
			//
			if r.chunk != nil {
				r.sendata()
			}
			if r.requestat.Equal(TimeNil) {
				continue
			}
			if Now.Sub(r.requestat) >= 0 {
				r.requestMoreReplicas(r.ngobj, r.tioparent, r.lastright)
			}
		}
		r.closeTxChannels()
	}()
}

func (r *gatewayNine) rejectBid(rjev *LateRejectBidEvent) {
	tio := rjev.GetTio()
	tioparent := tio.parent
	mcastflow := tioparent.flow
	srv := rjev.GetSource()
	bid := rjev.bid

	assert(tioparent == r.tioparent)
	assert(bid.state == bidStateRejected)
	assert(tioparent.haschild(tio))
	assert(srv == tio.target)
	assert(bid.win.left.After(Now), "rejected bid is already in progress"+bid.String())

	r.bids.rejectBid(bid, srv)
	if !r.rzvgroup.hasmember(srv) {
		return
	}

	r.remrzv(r.rzvgroup, srv)
	cstr := fmt.Sprintf("chunk#%d", r.chunk.sid)
	log("gwy-late-reject", r.rzvgroup.String(), cstr, "removed", srv.String())
	r.requestat = Now
	r.lastright = Now
	l := len(r.bids.pending)
	if l > 0 {
		r.lastright = r.bids.pending[l-1].win.right
	}

	t := mcastflow.timeTxDone.Add(config.timeClusterTrip + configNetwork.durationControlPDU/2)
	if r.requestat.Before(t) {
		r.requestat = t
	}
	x := r.requestat.Sub(time.Time{})
	log("gwy-post-reject-more", r.String(), x)
}

func (r *gatewayNine) remrzv(rzvgroup *RzvGroup, srv RunnerInterface) {
	cnt := rzvgroup.getCount()
	for k := 0; k < cnt; k++ {
		if rzvgroup.servers[k] == srv {
			if k == cnt-1 {
				rzvgroup.servers[k] = nil
			} else {
				copy(rzvgroup.servers[k:], rzvgroup.servers[k+1:])
			}
			return
		}
	}
	assert(false, "failed to remove from rzv "+rzvgroup.String()+","+srv.String())
}

//==================================================================
//
// serverNine methods
//
//==================================================================

// M9acceptng is invoked by the generic tio-processing code when
// the latter executes pipeline stage named "REQUEST-NG"
// This callback processes put-accept message from the gateway
// and further calls acceptBid() or cancelBid(), depending on whether
// the gateway has selected this server or not.
//
// The pipeline itself is declared at the top of this model.
func (r *serverNine) M9acceptng(ev EventInterface) error {
	tioevent := ev.(*McastChunkPutAcceptEvent)
	log(r.String(), "::M9acceptng()", tioevent.String())
	gwy := tioevent.GetSource()
	ngobj := tioevent.GetGroup()
	assert(ngobj.hasmember(r))
	tio := tioevent.GetTio()

	rzvgroup := tioevent.rzvgroup
	if !rzvgroup.hasmember(r) {
		r.bids.cancelBid(tio)
		return nil
	}

	// accepted?
	gwyflow := tio.flow
	assert(gwyflow.cid == tioevent.cid)
	mybid := gwyflow.extension.(*PutBid)
	// FIXME: gwyflow.extension == tioevent.bid redundant
	assert(mybid == tioevent.bid, mybid.String()+","+tioevent.String())
	rjbid, state := r.bids.acceptBid(tio, mybid)
	if rjbid != nil {
		rjev := newLateRejectBidEvent(r, rjbid)
		r.Send(rjev, SmethodWait)
		log("srv-accept-send", rjev.String())
		atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
	}
	// NOTE: the bid was previously rejected, currently removed -
	//       LateRejectBidEvent must be in flight, or else
	if state == bidStateRejected {
		assert(mybid.state == bidStateRejected)
		log("srv-accept-rejected-in-flight", mybid.String())
		return nil
	}

	//new server's flow
	flow := NewFlow(gwy, tioevent.cid, r, tio)
	flow.totalbytes = tioevent.sizeb
	flow.tobandwidth = configNetwork.linkbpsData
	log("srv-new-flow", flow.String(), mybid.String())
	r.flowsfrom.insertFlow(flow)

	return nil
}

//==================================================================
//
// modelNine interface methods
//
//==================================================================
func (m *modelNine) NewGateway(i int) RunnerInterface {
	gwy := m8.newGatewayEight(i)
	rgwy := &gatewayNine{gatewayEight: *gwy}
	rgwy.putpipeline = m9.putpipeline
	rgwy.rptr = rgwy // realobject
	bids := NewGatewayBidQueue(rgwy)
	rgwy.bids = bids
	return rgwy
}

func (m *modelNine) NewServer(i int) RunnerInterface {
	srv := m8.newServerEight(i)
	srv.putpipeline = m9.putpipeline

	rsrv := &serverNine{serverEight: *srv}
	rsrv.flowsfrom = NewFlowDir(rsrv, config.numGateways)

	bids := NewServerSparseDblrBidQueue(rsrv, 0)
	rsrv.bids = bids
	rsrv.rptr = rsrv
	return rsrv
}

func (m *modelNine) Configure() {
	m8.Configure()

}

//==================================================================
//
// modelNine events
//
//==================================================================
type LateRejectBidEvent struct {
	zControlEvent
	bid *PutBid
}

func newLateRejectBidEvent(srv RunnerInterface, bid *PutBid) *LateRejectBidEvent {
	at := configNetwork.durationControlPDU + config.timeClusterTrip
	timedev := newTimedAnyEvent(srv, at, bid.tio.source, bid.tio, configNetwork.sizeControlPDU)

	return &LateRejectBidEvent{zControlEvent{zEvent{*timedev}, bid.tio.cid}, bid}
}

func (e *LateRejectBidEvent) String() string {
	return fmt.Sprintf("[LRBE %v=>%v,%s]", e.source.String(), e.target.String(), e.bid.String())
}
