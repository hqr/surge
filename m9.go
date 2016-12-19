package surge

import (
	"fmt"
	"sync/atomic"
	"time"
)

type modelNine struct {
	ModelGeneric
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
func (r *gatewayNine) rxcallbackNine(ev EventInterface) int {
	switch ev.(type) {
	case *LateRejectBidEvent:
		rjev := ev.(*LateRejectBidEvent)
		log("rxcallback", r.String(), rjev.String())
		r.rejectBid(rjev)
	default:
		// execute generic tio.doStage() pipeline
		r.gatewayEight.rxcallbackMcast(ev)
	}
	return ev.GetSize()
}

func (r *gatewayNine) rejectBid(rjev *LateRejectBidEvent) {
	tio := rjev.GetTio().(*TioRr)
	tioparent := tio.GetParent().(*TioRr)
	mcastflow := tioparent.GetFlow().(*Flow)
	srv := rjev.GetSource()
	bid := rjev.bid

	assert(tioparent == r.tioparent)
	assert(bid.state == bidStateRejected)
	assert(tioparent.haschild(tio))
	assert(srv == tio.GetTarget())
	assert(bid.win.left.After(Now), "rejected bid is already in progress"+bid.String())

	r.bids.rejectBid(bid, srv)
	if !r.rzvgroup.hasmember(srv) {
		log("gwy-reject-not-selected", bid.String())
		return
	}
	assert(tio.bid == bid)

	r.remrzv(r.rzvgroup, srv)
	cstr := fmt.Sprintf("c#%d", r.chunk.sid)
	log("gwy-late-reject", r.String(), r.rzvgroup.String(), cstr, "removed", srv.String())

	// scheduled soon enough?
	if r.requestat.Before(Now) || r.requestat.Sub(Now) < configNetwork.durationControlPDU {
		return
	}

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

func (r *gatewayNine) remrzv(rzvgroup *RzvGroup, srv NodeRunnerInterface) {
	cnt := rzvgroup.getCount()
	for k := 0; k < cnt; k++ {
		if rzvgroup.servers[k] == srv {
			if k < cnt-1 {
				copy(rzvgroup.servers[k:], rzvgroup.servers[k+1:])
			}
			rzvgroup.servers[cnt-1] = nil
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
	gwy := tioevent.GetSource()
	ngobj := tioevent.GetGroup()
	assert(ngobj.hasmember(r))
	tio := tioevent.GetTio().(*TioRr)

	rzvgroup := tioevent.rzvgroup
	if !rzvgroup.hasmember(r) {
		log("srv-accept=cancel", tioevent.String(), tio.String())
		r.bids.cancelBid(tio)
		return nil
	}

	// accepted?
	gwyflow := tio.GetFlow()
	assert(gwyflow.GetCid() == tioevent.cid)
	assert(tio.bid == tioevent.bid, tioevent.String()+","+tioevent.bid.String())
	log("srv-accept=accept", tioevent.String(), tio.bid.String())
	rjbid, state := r.bids.acceptBid(tio, tio.bid)
	if rjbid != nil {
		rjev := newLateRejectBidEvent(r, rjbid)
		r.Send(rjev, SmethodWait)
		log("srv-accept-send", rjev.String())
		atomic.AddInt64(&r.txbytestats, int64(configNetwork.sizeControlPDU))
	}
	// NOTE: the bid was previously rejected, currently removed -
	//       LateRejectBidEvent must be in flight
	if state == bidStateRejected {
		assert(tio.bid.state == bidStateRejected)
		log("srv-accept-rejected-in-flight", tio.bid.String())
		return nil
	}

	// new server flow
	flow := NewFlow(gwy, tioevent.cid, r, tio)
	flow.totalbytes = tioevent.sizeb
	flow.setbw(configNetwork.linkbpsData)
	log("srv-new-flow", flow.String(), tio.bid.String())
	r.flowsfrom.insertFlow(flow)

	return nil
}

//==================================================================
//
// modelNine interface methods
//
//==================================================================
func (m *modelNine) NewGateway(i int) NodeRunnerInterface {
	gwy := m8.newGatewayEight(i)
	rgwy := &gatewayNine{gatewayEight: *gwy}
	rgwy.putpipeline = m9.putpipeline
	rgwy.rptr = rgwy // realobject
	bids := NewGatewayBidQueue(rgwy)
	rgwy.bids = bids
	rgwy.rxcb = rgwy.rxcallbackNine
	return rgwy
}

func (m *modelNine) NewServer(i int) NodeRunnerInterface {
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

func newLateRejectBidEvent(srv NodeRunnerInterface, bid *PutBid) *LateRejectBidEvent {
	at := configNetwork.durationControlPDU + config.timeClusterTrip
	timedev := newTimedAnyEvent(srv, at, bid.tio.GetSource(), bid.tio, configNetwork.sizeControlPDU)

	return &LateRejectBidEvent{zControlEvent{zEvent{*timedev}, bid.tio.cid}, bid}
}

func (e *LateRejectBidEvent) String() string {
	return fmt.Sprintf("[LRBE %v=>%v,%s]", e.source.String(), e.target.String(), e.bid.String())
}
