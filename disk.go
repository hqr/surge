package surge

import (
	"fmt"
	"time"
)

type Disk struct {
	node       RunnerInterface
	MBps       int
	reserved   int
	lastIOdone time.Time
	writes     int64
	reads      int64
	writebytes int64
	readbytes  int64
	queue      *DiskQueue
}

func NewDisk(r RunnerInterface, mbps int) *Disk {
	d := &Disk{node: r, MBps: mbps, reserved: 0, lastIOdone: Now}
	d.queue = NewDiskQueue(d, 0)
	return d
}

func (d *Disk) String() string {
	return fmt.Sprintf("disk-%s,%d,%d,%d,%d", d.node.String(), d.writes, d.reads, d.writebytes, d.readbytes)
}

func (d *Disk) scheduleWrite(sizebytes int) time.Duration {
	at := sizeToDuration(sizebytes, "B", int64(d.MBps), "MB")
	w := d.writes

	d.writes++
	d.writebytes += int64(sizebytes)
	if w > 0 && Now.Before(d.lastIOdone) {
		d.lastIOdone = d.lastIOdone.Add(at)
		at1 := d.lastIOdone.Sub(Now)
		d.queue.insertTime(at1)
		return at1
	}

	d.lastIOdone = Now.Add(at)
	d.queue.insertTime(at)
	return at
}
