package surge

import (
	"fmt"
	"time"
)

type applyCallback func(gwy NodeRunnerInterface, flow FlowInterface)

//========================================================================
//
// type FlowInterface
//
//========================================================================
type FlowInterface interface {
	String() string
	setOneArg(a interface{})
	unicast() bool

	// more accessors
	GetCid() int64
	GetSid() int64
	GetTio() TioInterface
	GetRb() RateBucketInterface
	GetRepnum() int

	getbw() int64
	setbw(bw int64)
	getoffset() int64
	setoffset(ev *ReplicaDataEvent)
	incoffset(add int)
	bytesToWrite(ev *ReplicaDataEvent) int
}

//========================================================================
//
// type Flow (short-lived: duration = <control> <one chunk> <ack>)
//
//========================================================================
type Flow struct {
	from        NodeRunnerInterface
	to          NodeRunnerInterface
	togroup     GroupInterface
	cid         int64
	sid         int64
	tio         TioInterface
	rb          RateBucketInterface // refill at the tobandwidth rate
	tobandwidth int64               // bits/sec
	timeTxDone  time.Time           // time the last byte of the current packet is sent
	extension   interface{}         // protocol-specific flow extension
	offset      int64
	totalbytes  int64
	repnum      int // replica num
}

//========================================================================
// c-tors and helpers
//========================================================================
func NewFlow(f NodeRunnerInterface, chunkid int64, args ...interface{}) *Flow {
	printid := uqrand(chunkid)
	flow := &Flow{
		from: f,
		cid:  chunkid,
		sid:  printid}

	for i := 0; i < len(args); i++ {
		flow.setOneArg(args[i])
	}
	// must be the flow initiating tio
	if flow.tio.GetFlow() == nil {
		flow.tio.SetFlow(flow)
	}
	return flow
}

func (flow *Flow) setOneArg(a interface{}) {
	switch a.(type) {
	case int:
		flow.repnum = a.(int)
	case TioInterface:
		flow.tio = a.(TioInterface)
	case NodeRunnerInterface:
		flow.to = a.(NodeRunnerInterface)
	case GroupInterface:
		flow.togroup = a.(GroupInterface)
	default:
		assert(false, fmt.Sprintf("unexpected type: %#v", a))
	}
}

func (flow *Flow) unicast() bool {
	return flow.to != nil && flow.togroup == nil
}

func (flow *Flow) String() string {
	f := flow.from.String()
	bwstr := fmt.Sprintf("%.2f", float64(flow.tobandwidth)/1000.0/1000.0/1000.0)
	var cstr string
	if flow.repnum != 0 {
		cstr = fmt.Sprintf("c#%d(%d)", flow.sid, flow.repnum)
	} else {
		cstr = fmt.Sprintf("c#%d", flow.sid)
	}
	if flow.unicast() {
		t := flow.to.String()
		return fmt.Sprintf("[flow %s=>%s[%s],offset=%d,bw=%sGbps]", f, t, cstr, flow.offset, bwstr)
	}
	t := flow.togroup.String()
	return fmt.Sprintf("[flow %s=>%s[%s],offset=%d,bw=%sGbps]", f, t, cstr, flow.offset, bwstr)
}

func (flow *Flow) getbw() int64                   { return flow.tobandwidth }
func (flow *Flow) setbw(bw int64)                 { flow.tobandwidth = bw }
func (flow *Flow) getoffset() int64               { return flow.offset }
func (flow *Flow) setoffset(ev *ReplicaDataEvent) { flow.offset = ev.offset }
func (flow *Flow) incoffset(add int)              { flow.offset += int64(add) }
func (flow *Flow) GetCid() int64                  { return flow.cid }
func (flow *Flow) GetSid() int64                  { return flow.sid }
func (flow *Flow) GetTio() TioInterface           { return flow.tio.GetTio() }
func (flow *Flow) GetRb() RateBucketInterface     { return flow.rb }
func (flow *Flow) GetRepnum() int                 { return flow.repnum }

func (flow *Flow) bytesToWrite(ev *ReplicaDataEvent) int {
	if flow.offset < flow.totalbytes {
		return 0
	}
	return int(flow.totalbytes)
}

//========================================================================
//
// type FlowLong (long-lived unicast flow)
//
//========================================================================
type FlowLong struct {
	from        NodeRunnerInterface
	to          NodeRunnerInterface
	rb          RateBucketInterface // refill at the tobandwidth rate
	tobandwidth int64               // bits/sec
	offset      int64               // transmitted bytes
	timeTxDone  time.Time           // time the last byte of the current packet is sent
}

func (flow *FlowLong) setOneArg(a interface{}) {
}

func (flow *FlowLong) unicast() bool {
	return true
}

func (flow *FlowLong) String() string {
	f := flow.from.String()
	bwstr := fmt.Sprintf("%.2f", float64(flow.tobandwidth)/1000.0/1000.0/1000.0)
	t := flow.to.String()
	return fmt.Sprintf("[flow %s=>%s,bw=%sGbps]", f, t, bwstr)
}

func (flow *FlowLong) getbw() int64               { return flow.tobandwidth }
func (flow *FlowLong) setbw(bw int64)             { flow.tobandwidth = bw }
func (flow *FlowLong) GetRb() RateBucketInterface { return flow.rb }
func (flow *FlowLong) incoffset(add int)          { flow.offset += int64(add) }

// FIXME: remove from interface
func (flow *FlowLong) getoffset() int64                      { return 0 }
func (flow *FlowLong) setoffset(ev *ReplicaDataEvent)        { assert(false) }
func (flow *FlowLong) GetCid() int64                         { return 0 }
func (flow *FlowLong) GetSid() int64                         { return 0 }
func (flow *FlowLong) GetTio() TioInterface                  { return nil }
func (flow *FlowLong) GetRepnum() int                        { return 0 }
func (flow *FlowLong) bytesToWrite(ev *ReplicaDataEvent) int { return 0 }

//========================================================================
//
// FlowDir - container: unidirectional flows many-others => myself
//
//========================================================================
type FlowDir struct {
	node  NodeRunnerInterface
	flows map[NodeRunnerInterface]FlowInterface
}

func NewFlowDir(r NodeRunnerInterface, num int) *FlowDir {
	flows := make(map[NodeRunnerInterface]FlowInterface, num)
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

func (fdir *FlowDir) deleteFlow(r NodeRunnerInterface) {
	delete(fdir.flows, r)
}

func (fdir *FlowDir) count() int {
	return len(fdir.flows)
}

func (fdir *FlowDir) get(r NodeRunnerInterface, mustexist bool) FlowInterface {
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
