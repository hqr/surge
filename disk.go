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

//==================================================================
//
// type: Disk
//
//==================================================================
type Disk struct {
	node           RunnerInterface
	MBps           int
	reserved       int
	lastIOdone     time.Time
	writes         int64
	reads          int64
	writebytes     int64
	readbytes      int64
	stopHandler    bool
	pendingwbytes  int64
	pendingrbytes  int64
	qMutex         sync.Mutex
}

func NewDisk(r RunnerInterface, mbps int) *Disk {
	d := &Disk{node: r, MBps: mbps, reserved: 0, lastIOdone: Now}
	return d
}

//
// This method is to simulate any disk controller actions.
// The minimal handler here simulates the writes. Wakes up
// after every unit of time needed to write a chunk and then
// updates the pendingwbytes which is used to derive queue depth.
//
func (d *Disk) handler() {
	// time required to write one chunk.
	sleepTime := configStorage.dskdurationDataChunk

	nextWakeup := Now.Add(sleepTime)
	for d.stopHandler == false {
		// Sleep for one chunk write time
		time.Sleep(sleepTime)
		if nextWakeup.After(Now) {
			continue
		}

		// Smulate the write of one chunk
		d.qMutex.Lock()
		pendingwbytes := d.pendingwbytes
		pendingwbytes -= int64(configStorage.sizeDataChunk * 1024)
		if pendingwbytes >= 0 {
			d.pendingwbytes = pendingwbytes
		}
		d.qMutex.Unlock()
		nextWakeup = Now.Add(sleepTime)
	}
}

// Start the disk functionality.
// Model should call this when it is
// ready to consume chunks
func (d *Disk) Start() {
	d.stopHandler = false
	go d.handler()
}

// Stops the disk functionality.
// Model should call this when the
// i/o is complete and simulation is ending.
func (d *Disk) Stop() {
	d.stopHandler = true
}

func (d *Disk) String() string {
	return fmt.Sprintf("disk-%s,%d,%d,%d,%d", d.node.String(), d.writes, d.reads, d.writebytes, d.readbytes)
}

func (d *Disk) scheduleWrite(sizebytes int) time.Duration {
	at := sizeToDuration(sizebytes, "B", int64(d.MBps), "MB")
	w := d.writes

	// Simulate adding i/o to the queue.
	d.qMutex.Lock()
	d.pendingwbytes += int64(sizebytes)
	qSz := float64(d.pendingwbytes) / float64(configStorage.sizeDataChunk * 1024)
	d.qMutex.Unlock()

	d.writes++
	d.writebytes += int64(sizebytes)

	// Simulate delay based on the queue depth
	// We do this by a parabola equation of queue depth.
	// parabola equation :  (x - 32)^2 = 4 * 10 * y
        latencyF := ((qSz * qSz) - 64.0 * qSz + 1024.0) / 40.0;
	latency := int64(latencyF * 1000.0)

	at += time.Duration(latency)
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
