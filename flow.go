package surge

import (
	"fmt"
	"time"
)

type applyCallback func(gwy RunnerInterface, flow *Flow)

//========================================================================
//
// fat Flow type
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
	num         int                 // replica num
	offset      int
	totalbytes  int
	prevoffset  int
}

// (container) unidirectional unicast flows between this node and multiple other nodes
type FlowDir struct {
	node  RunnerInterface
	flows map[RunnerInterface]*Flow
}

// (container) multicast flows from this node to a given group
type FlowDirMcast struct {
	node       RunnerInterface
	mcastflows map[int64]*Flow
}

//========================================================================
// c-tors and helpers
//========================================================================
func NewFlow(f RunnerInterface, t RunnerInterface, chunkid int64, repnum int, io *Tio) *Flow {
	printid := uqrand(chunkid)
	return &Flow{
		from:    f,
		to:      t,
		cid:     chunkid,
		sid:     printid,
		tio:     io,
		rb:      nil,
		rateini: false,
		num:     repnum}
}

func NewFlowMcast(f RunnerInterface, t GroupInterface, chunkid int64, repnum int, io *Tio) *Flow {
	printid := uqrand(chunkid)
	return &Flow{
		from:    f,
		to:      nil,
		togroup: t,
		cid:     chunkid,
		sid:     printid,
		tio:     io,
		rb:      nil,
		rateini: false,
		num:     repnum}
}

func (flow *Flow) unicast() bool {
	return flow.to != nil && flow.togroup == nil
}

func (flow *Flow) String() string {
	f := flow.from.String()
	bwstr := fmt.Sprintf("%.2f", float64(flow.tobandwidth)/1000.0/1000.0/1000.0)
	if flow.unicast() {
		t := flow.to.String()
		return fmt.Sprintf("[flow %s=>%s[chunk#%d(%d)],offset=%d,bw=%sGbps]", f, t, flow.sid, flow.num, flow.offset, bwstr)
	}
	t := flow.togroup.String()
	return fmt.Sprintf("[flow %s=>%s[chunk#%d(%d)],offset=%d,bw=%sGbps]", f, t, flow.sid, flow.num, flow.offset, bwstr)
}

//
// FlowDir
//
func NewFlowDir(r RunnerInterface, num int) *FlowDir {
	flows := make(map[RunnerInterface]*Flow, num)
	return &FlowDir{r, flows}
}

func (fdir *FlowDir) insertFlow(r RunnerInterface, flow *Flow) {
	assert(flow.unicast())
	fdir.flows[r] = flow
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

//
// FlowDirMcast
//
func NewFlowDirMcast(r RunnerInterface, num int) *FlowDirMcast {
	mcastflows := make(map[int64]*Flow, num)
	return &FlowDirMcast{r, mcastflows}
}

func (fdir *FlowDirMcast) insertFlow(g GroupInterface, flow *Flow) {
	assert(!flow.unicast())
	fdir.mcastflows[g.getID64()] = flow
}

func (fdir *FlowDirMcast) deleteFlow(g GroupInterface) {
	delete(fdir.mcastflows, g.getID64())
}

func (fdir *FlowDirMcast) count() int {
	return len(fdir.mcastflows)
}

func (fdir *FlowDirMcast) get(g GroupInterface, mustexist bool) *Flow {
	flow, ok := fdir.mcastflows[g.getID64()]
	if ok {
		return flow
	}
	if mustexist {
		n := fdir.node.String()
		other := g.String()
		assertstr := fmt.Sprintf("multicast flow %s=>%s does not exist", n, other)
		assert(false, assertstr)
	}
	return nil
}
