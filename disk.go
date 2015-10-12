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
}

func NewDisk(r RunnerInterface, mbps int) *Disk {
	return &Disk{node: r, MBps: mbps, reserved: 0, lastIOdone: Now}
}

func (d *Disk) String() {
	fmt.Sprintf("disk-%s,%d,%d,%d,%d", d.node.String(), d.writes, d.reads, d.writebytes, d.readbytes)
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
