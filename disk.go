package surge

import (
	"fmt"
	"sync"
	"time"
)

type DiskQueueDepthEnum int

// constants
const (
	DqdBuffers DiskQueueDepthEnum = iota
	DqdChunks
)

var idCounter int
//==================================================================
//
// type: Disk
//
//==================================================================

// DiskRunnerBase implementing DiskRunnerInterface
// It is same as RunnerBase for now. More methods
// will be added as the simulation evolves.
type DiskRunnerBase struct {
	RunnerBase
}

type Disk struct {
	DiskRunnerBase
	node           NodeRunnerInterface
	MBps           int
	reserved       int
	lastIOdone     time.Time
	writes         int64
	reads          int64
	writebytes     int64
	readbytes      int64
	pendingwbytes  int64
	pendingrbytes  int64
	nextWrDone     time.Time
	qMutex         sync.Mutex
}

func NewDisk(r NodeRunnerInterface, mbps int) *Disk {
	idCounter++
	d := &Disk{
		DiskRunnerBase: DiskRunnerBase{RunnerBase{id: idCounter}},
		node: r, MBps: mbps, reserved: 0, lastIOdone: Now}
	return d
}

func (d *Disk) String() string {
	return fmt.Sprintf("disk-%s,%d,%d,%d,%d", d.node.String(), d.writes, d.reads, d.writebytes, d.readbytes)
}

// Dummy Run function to satisfy DiskRunnerInterface
func (d *Disk) Run() {
}

//
// This method keeps track of i/o completion. As of now
// it tracks only write io completion.
// FIXME: Track read i/o completion.
// returns true always for now.
//
func (d *Disk) NowIsDone() bool {
	if d.nextWrDone.After(Now) {
		return true
	}

	// Simulate the write of one chunk
	d.qMutex.Lock()
	pendingwbytes := d.pendingwbytes
	pendingwbytes -= int64(configStorage.sizeDataChunk * 1024)
	if pendingwbytes >= 0 {
		d.pendingwbytes = pendingwbytes
	}
	d.qMutex.Unlock()
	d.nextWrDone = Now.Add(configStorage.dskdurationDataChunk)

	return true
}

func (d *Disk) scheduleWrite(sizebytes int) time.Duration {
	at := sizeToDuration(sizebytes, "B", int64(d.MBps), "MB")
	w := d.writes

	// Simulate adding i/o to the queue.
	d.qMutex.Lock()

	// Queue size is in units of chunks. Ideally it should be in units of i/o
	// In the current implementation, we don't differentiate i/o and all of the
	// existing models does one chunk per i/o, we assume queu size in units of chunks.
	// FIXME: Quesize should be in units of i/o
	qSz := float64(d.pendingwbytes) / float64(configStorage.sizeDataChunk * 1024)
	d.pendingwbytes += int64(sizebytes)
	d.qMutex.Unlock()

	// Simulate delay based on the queue depth
	// We do this by a parabola equation of queue depth.
	// parabola equation :  (x - 32)^2 = 4 * 10 * y
	// This equation is currently a hardcoded equation where
	// minimum delay is hard coded to be at queue size of 32.
	// FIXME: This equation should not be hard coded and should
	//        be made configurable.
	latencyF := ((qSz * qSz) - 64.0 * qSz + 1024.0) / 40.0;

	// Delay is in microseconds.
	latency := time.Duration(latencyF) * time.Microsecond
	log(LogVV, fmt.Sprintf("Scheduling chunk write current QDepth:%v, Induced delay: %v", qSz, latency))
	at += latency

	d.writes++
	d.writebytes += int64(sizebytes)
	if w > 0 && Now.Before(d.lastIOdone) {
		d.lastIOdone = d.lastIOdone.Add(at)
		at1 := d.lastIOdone.Sub(Now)
		return at1
	}

	d.lastIOdone = Now.Add(at)
	return at
}

func (d *Disk) queueDepth(in DiskQueueDepthEnum) (int, time.Duration) {
	if !Now.Before(d.lastIOdone) {
		return 0, 0
	}
	diff := d.lastIOdone.Sub(Now)
	// round up as well
	if in == DqdChunks {
		numDiskQueueChunks := (int64(diff) + int64(configStorage.dskdurationDataChunk/2)) / int64(configStorage.dskdurationDataChunk)
		return int(numDiskQueueChunks), diff
	}
	assert(in == DqdBuffers)
	numDiskQueueBuffers := (int64(diff) + int64(configStorage.dskdurationFrame/2)) / int64(configStorage.dskdurationFrame)
	return int(numDiskQueueBuffers), diff
}
