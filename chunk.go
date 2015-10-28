//
// object storage types, constructors & events
//
package surge

import (
	"fmt"
	"time"
)

//========================================================================
// chunk & replica
//========================================================================
type Chunk struct {
	cid     int64
	sid     int64 // short id
	gateway RunnerInterface
	crtime  time.Time // creation time
	sizeb   int       // KB
}

func NewChunk(gwy RunnerInterface, sizebytes int) *Chunk {
	uqid, printid := uqrandom64(gwy.GetId())
	return &Chunk{cid: uqid, sid: printid, gateway: gwy, crtime: Now, sizeb: sizebytes}
}

func (chunk *Chunk) String() string {
	return fmt.Sprintf("[chunk#%d]", chunk.sid)
}

type PutReplica struct {
	chunk  *Chunk
	crtime time.Time // creation time
	num    int       // 1 .. configStorage.numReplicas
}

func (replica *PutReplica) String() string {
	return fmt.Sprintf("[replica#%d,chunk#%d]", replica.num, replica.chunk.sid)
}

func NewPutReplica(c *Chunk, n int) *PutReplica {
	return &PutReplica{chunk: c, crtime: Now, num: n}
}

//========================================================================
// events
//========================================================================
type UchEvent struct {
	TimedUcastEvent
}

type UchControlEvent struct {
	UchEvent
	cid int64
}

type UchReplicaPutRequestEvent struct {
	UchControlEvent
	num   int // replica num
	sizeb int // size in bytes
}

func newUchReplicaPutRequestEvent(gwy RunnerInterface, srv RunnerInterface, rep *PutReplica) *UchReplicaPutRequestEvent {
	at := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	timedev := newTimedUcastEvent(gwy, at, srv)
	return &UchReplicaPutRequestEvent{UchControlEvent{UchEvent{*timedev}, rep.chunk.cid}, rep.num, rep.chunk.sizeb}
}

type UchReplicaPutRequestAckEvent struct {
	UchControlEvent
	num int // replica num
}

func newUchReplicaPutRequestAckEvent(srv RunnerInterface, gwy RunnerInterface, chunkid int64, repnum int) *UchReplicaPutRequestAckEvent {
	atnet := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	timedev := newTimedUcastEvent(srv, atnet, gwy)
	return &UchReplicaPutRequestAckEvent{UchControlEvent{UchEvent{*timedev}, chunkid}, repnum}
}

type UchReplicaPutAckEvent struct {
	UchControlEvent
	num int // replica num
}

func (e *UchReplicaPutAckEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[PutAckEvent src=%v,tgt=%v,chunk#%d,num=%d]", e.source.String(), e.target.String(), printid, e.num)
}

func newUchReplicaPutAckEvent(srv RunnerInterface, gwy RunnerInterface, chunkid int64, repnum int, atdisk time.Duration) *UchReplicaPutAckEvent {
	atnet := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	at := atnet + atdisk
	timedev := newTimedUcastEvent(srv, at, gwy)
	return &UchReplicaPutAckEvent{UchControlEvent{UchEvent{*timedev}, chunkid}, repnum}
}

type UchRateSetEvent struct {
	UchControlEvent
	tobandwidth int64 // bits/sec
	num         int   // replica num
}

type UchRateInitEvent struct {
	UchRateSetEvent
}

func (e *UchRateInitEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[RateInitEvent src=%v,tgt=%v,chunk#%d,num=%d]", e.source.String(), e.target.String(), printid, e.num)
}

func newUchRateSetEvent(srv RunnerInterface, gwy RunnerInterface, rate int64, chunkid int64, repnum int) *UchRateSetEvent {
	at := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	timedev := newTimedUcastEvent(srv, at, gwy)
	// factor in the L2+L3+L4 headers overhead + ARP, etc.
	rate -= rate * int64(configNetwork.overheadpct) / int64(100)
	return &UchRateSetEvent{UchControlEvent{UchEvent{*timedev}, chunkid}, rate, repnum}
}

func (e *UchRateSetEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[RateSetEvent src=%v,tgt=%v,chunk#%d,num=%d]", e.source.String(), e.target.String(), printid, e.num)
}

func newUchRateInitEvent(srv RunnerInterface, gwy RunnerInterface, rate int64, chunkid int64, repnum int) *UchRateInitEvent {
	ev := newUchRateSetEvent(srv, gwy, rate, chunkid, repnum)
	return &UchRateInitEvent{*ev}
}

type UchDingAimdEvent struct {
	UchControlEvent
	num int // replica num
}

func newUchDingAimdEvent(srv RunnerInterface, gwy RunnerInterface, chunkid int64, repnum int) *UchDingAimdEvent {
	at := sizeToDuration(configNetwork.sizeControlPDU, "B", configNetwork.linkbps, "b") + config.timeClusterTrip
	timedev := newTimedUcastEvent(srv, at, gwy)
	return &UchDingAimdEvent{UchControlEvent{UchEvent{*timedev}, chunkid}, repnum}
}
func (e *UchDingAimdEvent) String() string {
	printid := uqrand(e.cid)
	return fmt.Sprintf("[DingAimdEvent src=%v,tgt=%v,chunk#%d,num=%d]", e.source.String(), e.target.String(), printid, e.num)
}

type UchDataEvent struct {
	UchEvent
	cid    int64
	num    int
	offset int
}

type UchReplicaDataEvent struct {
	UchDataEvent
}

func newUchReplicaDataEvent(gwy RunnerInterface, srv RunnerInterface, rep *PutReplica, flow *Flow, frsize int) *UchReplicaDataEvent {
	at := sizeToDuration(frsize, "B", flow.tobandwidth, "b") + config.timeClusterTrip
	timedev := newTimedUcastEvent(gwy, at, srv)
	return &UchReplicaDataEvent{UchDataEvent{UchEvent{*timedev}, rep.chunk.cid, rep.num, flow.offset}}
}

func (e *UchReplicaDataEvent) String() string {
	printid := uqrand(e.cid)
	dcreated := e.crtime.Sub(time.Time{})
	dtriggered := e.thtime.Sub(time.Time{})
	return fmt.Sprintf("[ReplicaDataEvent src=%v,tgt=%v,chunk#%d,num=%d,offset=%d,(%11.10v,%11.10v)]",
		e.source.String(), e.target.String(), printid, e.num, e.offset, dcreated, dtriggered)
}
