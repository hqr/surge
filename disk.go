package surge

import (
	"fmt"
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

func (d *Disk) scheduleWrite(sizebytes int) time.Duration {
	at := sizeToDuration(sizebytes, "B", int64(d.MBps), "MB")
	w := d.writes

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
