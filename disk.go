package surge

import (
	"fmt"
	"time"
)

type DiskQueueDepthEnum int
type DiskTypeEnum int

//
// constants
//
const (
	DtypeConstLatency DiskTypeEnum = iota
	DtypeVarLatency
)

const (
	DqdBuffers DiskQueueDepthEnum = iota
	DqdChunks
)

//
// interfaces
//
type DiskInterface interface {
	String() string
	GetMBps() int
	SetMBps(mbps int)
	scheduleWrite(sizebytes int) time.Duration
	scheduleRead(sizebytes int)
	queueDepth(in DiskQueueDepthEnum) (int, time.Duration)
	lastIOdone() time.Time
}

//
// factory
//
func NewDisk(r NodeRunnerInterface, mbps int, dtype DiskTypeEnum) DiskInterface {
	var d DiskInterface
	chduration := sizeToDuration(configStorage.sizeDataChunk, "KB", int64(mbps), "MB")
	switch dtype {
	case DtypeVarLatency:
		dc := &DiskConstLatency{node: r, MBps: mbps, iodone: Now, dskdurationDataChunk: chduration}
		d = &DiskVarLatency{*dc, Now}
	default:
		assert(dtype == DtypeConstLatency)
		d = &DiskConstLatency{node: r, MBps: mbps, iodone: Now, dskdurationDataChunk: chduration}
	}
	return d
}

//==================================================================
//
// type: DiskConstLatency
//
//==================================================================
type DiskConstLatency struct {
	node       NodeRunnerInterface // host
	iodone     time.Time
	writes     int64
	reads      int64 // niy
	writebytes int64
	readbytes  int64 // niy
	MBps       int   // disk throughput
	// computed
	dskdurationDataChunk time.Duration
}

func (d *DiskConstLatency) String() string {
	numchunks, _ := d.queueDepth(DqdChunks)
	return fmt.Sprintf("diskc-%s,w#%d,r#%d,queue#%d", d.node.String(), d.writes, d.reads, numchunks)
}

func (d *DiskConstLatency) GetMBps() int {
	return d.MBps
}

func (d *DiskConstLatency) SetMBps(mbps int) {
	d.MBps = mbps
}

func (d *DiskConstLatency) scheduleWrite(sizebytes int) time.Duration {
	at := sizeToDuration(sizebytes, "B", int64(d.MBps), "MB")
	w := d.writes

	d.writes++
	d.writebytes += int64(sizebytes)
	if w > 0 && Now.Before(d.iodone) {
		d.iodone = d.iodone.Add(at)
		at1 := d.iodone.Sub(Now)
		return at1
	}

	d.iodone = Now.Add(at)
	return at
}

// niy, temp hack used in models m7 and ma
func (d *DiskConstLatency) scheduleRead(sizebytes int) {
	d.reads++
	d.readbytes += int64(sizebytes)
	d.iodone = d.iodone.Add(d.dskdurationDataChunk)
}

func (d *DiskConstLatency) queueDepth(in DiskQueueDepthEnum) (int, time.Duration) {
	if !Now.Before(d.iodone) {
		return 0, 0
	}
	diff := d.iodone.Sub(Now)
	// round up as well
	if in == DqdChunks {
		numDiskQueueChunks := (int64(diff) + int64(d.dskdurationDataChunk/2)) / int64(d.dskdurationDataChunk)
		return int(numDiskQueueChunks), diff
	}
	assert(in == DqdBuffers)
	numDiskQueueBuffers := (int64(diff) + int64(configStorage.dskdurationFrame/2)) / int64(configStorage.dskdurationFrame)
	return int(numDiskQueueBuffers), diff
}

func (d *DiskConstLatency) lastIOdone() time.Time {
	return d.iodone
}

//==================================================================
//
// type: DiskVarLatency
//
//==================================================================
type DiskVarLatency struct {
	DiskConstLatency
	iodoneVar time.Time
}

func (d *DiskVarLatency) String() string {
	numchunks, _ := d.queueDepth(DqdChunks)
	extraLatency := d.extraLatency(numchunks)
	return fmt.Sprintf("diskv-%s,w#%d,r#%d,queue#%d,exlat=%v", d.node.String(), d.writes, d.reads, numchunks, extraLatency)
}

func (d *DiskVarLatency) scheduleWrite(sizebytes int) time.Duration {
	d.DiskConstLatency.scheduleWrite(sizebytes)
	numchunks, _ := d.queueDepth(DqdChunks)

	extraLatency := d.extraLatency(numchunks)
	d.iodoneVar = d.iodone.Add(extraLatency)
	return d.iodoneVar.Sub(Now)
}

//
// FIXME: configurable latency formula goes here
//
func (d *DiskVarLatency) extraLatency(numchunks int) time.Duration {
	switch {
	case numchunks < 4:
		return 0
	case numchunks < 8:
		return d.dskdurationDataChunk
	case numchunks < 12:
		return 2 * d.dskdurationDataChunk
	case numchunks < 16:
		return 4 * d.dskdurationDataChunk
	}
	return 8 * d.dskdurationDataChunk
}

func (d *DiskVarLatency) lastIOdone() time.Time {
	return d.iodoneVar
}
