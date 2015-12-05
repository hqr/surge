package surge

import (
	"fmt"
	"time"
)

type applyCallback func(gwy RunnerInterface, flow *Flow)

//========================================================================
//
// FIXME: FAT
//
//========================================================================
type Flow struct {
	from        RunnerInterface
	to          RunnerInterface
	togroup     GroupInterface
	cid         int64
	sid         int64
	tio         *Tio
	rb          RateBucketInterface // refill at the tobandwidth rate
	tobandwidth int64               // bits/sec
	sendnexts   time.Time           // earliest can send the next frame
	ratects     time.Time           // rateset creation time
	raterts     time.Time           // rateset effective time
	rateini     bool                // rateset inited
	repnum      int                 // replica num
	offset      int
	totalbytes  int
	prevoffset  int
}

// (container) unidirectional unicast flows between this node and multiple other nodes
type FlowDir struct {
	node  RunnerInterface
	flows map[RunnerInterface]*Flow
}

//========================================================================
// c-tors and helpers
//========================================================================
func NewFlow(f RunnerInterface, t RunnerInterface, chunkid int64, num int, io *Tio) *Flow {
	printid := uqrand(chunkid)
	flow := &Flow{
		from:    f,
		to:      t,
		cid:     chunkid,
		sid:     printid,
		tio:     io,
		rb:      nil,
		rateini: false,
		repnum:  num}

	// must be flow-initiating tio
	if flow.tio.flow == nil {
		flow.tio.flow = flow
	}
	return flow
}

func (flow *Flow) unicast() bool {
	return flow.to != nil && flow.togroup == nil
}

func (flow *Flow) String() string {
	f := flow.from.String()
	bwstr := fmt.Sprintf("%.2f", float64(flow.tobandwidth)/1000.0/1000.0/1000.0)
	if flow.unicast() {
		t := flow.to.String()
		return fmt.Sprintf("[flow %s=>%s[chunk#%d(%d)],offset=%d,bw=%sGbps]", f, t, flow.sid, flow.repnum, flow.offset, bwstr)
	}
	t := flow.togroup.String()
	return fmt.Sprintf("[flow %s=>%s[chunk#%d(%d)],offset=%d,bw=%sGbps]", f, t, flow.sid, flow.repnum, flow.offset, bwstr)
}

//
// FlowDir
//
func NewFlowDir(r RunnerInterface, num int) *FlowDir {
	flows := make(map[RunnerInterface]*Flow, num)
	return &FlowDir{r, flows}
}

func (fdir *FlowDir) insertFlow(flow *Flow) {
	assert(flow.unicast())
	if fdir.node == flow.from {
		fdir.flows[flow.to] = flow
	} else {
		assert(fdir.node == flow.to)
		fdir.flows[flow.from] = flow
	}
}

func (fdir *FlowDir) deleteFlow(r RunnerInterface) {
	delete(fdir.flows, r)
}

func (fdir *FlowDir) count() int {
	return len(fdir.flows)
}

func (fdir *FlowDir) get(r RunnerInterface, mustexist bool) *Flow {
	flow, ok := fdir.flows[r]
	if ok {
		return flow
	}
	if mustexist {
		n := fdir.node.String()
		other := r.String()
		assertstr := fmt.Sprintf("flow %s<...>%s does not exist", n, other)
		assert(false, assertstr)
	}
	return nil
}

func (fdir *FlowDir) apply(f applyCallback) {
	for r, flow := range fdir.flows {
		f(r, flow)
	}
}
