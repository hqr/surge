package surge

import (
	"fmt"
	"time"
)

type applyCallback func(gwy RunnerInterface, flow *Flow)

//========================================================================
// types: flow and flow container
//========================================================================
type Flow struct {
	from        RunnerInterface
	to          RunnerInterface
	cid         int64
	sid         int64
	tio         *Tio
	tobandwidth int64     // bits/sec
	refillbits  int64     // refill at the tobandwidth rate
	sendnexts   time.Time // earliest can send the next frame
	ratects     time.Time // rateset creation time
	raterts     time.Time // rateset effective time
	rateini     bool      // rateset inited
	num         int       // replica num
	offset      int
	totalbytes  int
}

// flows to or from a given node
type FlowDir struct {
	node  RunnerInterface
	flows map[RunnerInterface]*Flow
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
		rateini: false,
		num:     repnum}
}

func (flow *Flow) String() string {
	f := flow.from.String()
	t := flow.to.String()
	bwstr := fmt.Sprintf("%.2f", float64(flow.tobandwidth)/1000.0/1000.0/1000.0)
	return fmt.Sprintf("[flow %s=>%s[chunk#%d(%d)],offset=%d,bw=%sGbps]", f, t, flow.sid, flow.num, flow.offset, bwstr)
}

func NewFlowDir(r RunnerInterface, num int) *FlowDir {
	x := make(map[RunnerInterface]*Flow, num)
	return &FlowDir{r, x}
}

func (fdir *FlowDir) insertFlow(r RunnerInterface, flow *Flow) {
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
