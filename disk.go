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
	latencySim     DiskLatencySimulator
}

func NewDisk(r NodeRunnerInterface, mbps int) *Disk {
	idCounter++
	d := &Disk{
		DiskRunnerBase: DiskRunnerBase{RunnerBase{id: idCounter}},
		node: r, MBps: mbps, reserved: 0, lastIOdone: Now, latencySim: &latencyNil}
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
	if d.writes <= 1 || d.nextWrDone.After(Now) {
		return true
	}

	// Simulate the write of one chunk
	d.qMutex.Lock()
	pendingwbytes := d.pendingwbytes
	pendingwbytes -= int64(configStorage.sizeDataChunk * 1024)
	if pendingwbytes > 0 {
		d.pendingwbytes = pendingwbytes
	} else {
		d.pendingwbytes = 0
	}
	qSz := float64(d.pendingwbytes) / float64(configStorage.sizeDataChunk * 1024)
	d.qMutex.Unlock()
	log(LogVVV, fmt.Sprintf("one-chunk write complete, new QDepth :%v", qSz))

	// TODO: The delay is not taken into account here. We need to account for
	//       the delay as well.
	//       Temporary workaround is to add a 12 microsecond delay which is an average
	//       delay for window sizes between 1 and 32.
	d.nextWrDone = Now.Add(configStorage.dskdurationDataChunk + (12 * time.Microsecond))

	return true
}

//==========================================================================
// Customizable Latency Simulation framework for Disk
//==========================================================================

// This structure defines the possible parameters that influences the latency
// function for Disk. Its only queue size now, but will grow in future.
type DiskLatencyParams struct {
	qSize	float64
}

// Interface which implements the latency function
type DiskLatencySimulator interface {
	GetName() string
	Latency(params DiskLatencyParams) time.Duration
}

var latencySimulators map[string]DiskLatencySimulator

func RegisterDiskLatencySimulator(sim DiskLatencySimulator) {
	assert(latencySimulators[sim.GetName()] == nil)
	latencySimulators[sim.GetName()] = sim
}

func (d *Disk) SetDiskLatencySimulator(sim DiskLatencySimulator) {
	if latencySimulators[sim.GetName()] == nil {
		latencySimulators[sim.GetName()] = sim
	}
	d.latencySim = sim
}

func (d *Disk) SetDiskLatencySimulatorByname(name string) {
	if latencySimulators[name] != nil {
		d.latencySim = latencySimulators[name]
	}
}

//---------------------------------------------------------
// Implementation for DiskLatencySimulator
//---------------------------------------------------------

// Abstract base

type LatencyBase struct {
	name	string
}

var latencyBase = LatencyBase { name: "base" }

func (l *LatencyBase) GetName() string {
	return l.name
}

// Abstract method
func (l *LatencyBase) Latency(params DiskLatencyParams) time.Duration {
	assert(false)
	return 0
}

// Zero induced latency

type LatencyNil struct {
	LatencyBase
}

var latencyNil = LatencyNil{LatencyBase{name: "latency-nil"}}

func (l *LatencyNil) Latency(params DiskLatencyParams) time.Duration {
	return 0
}

// Latency function using parabola equation

type LatencyParabola struct {
	LatencyBase
}
// Parabola equation (x -h)^2 = 4p(y-k) where h = 64, p = 10, k = 0
var latencyParabola = LatencyParabola{LatencyBase{name: "latency-parabola-h64-p10-k0"}}

func (l *LatencyParabola) Latency(params DiskLatencyParams) time.Duration {
	// Parabola equation to simulate latency based on queue depth.
	// parabola equation :  (x - 64)^2 = 4 * 10 * y

	latencyF := ((params.qSize * params.qSize) - 64.0 * params.qSize + 1024.0) / 40.0;

	// Delay is in microseconds.
	return time.Duration(latencyF) * time.Microsecond
}

func (d *Disk) scheduleWrite(sizebytes int) time.Duration {
	at := sizeToDuration(sizebytes, "B", int64(d.MBps), "MB")
	d.writes++
	d.writebytes += int64(sizebytes)

	// Simulate adding i/o to the queue.
	d.qMutex.Lock()

	// Queue size is in units of chunks. Ideally it should be in units of i/o
	// In the current implementation, we don't differentiate i/o and all of the
	// existing models does one chunk per i/o, we assume queu size in units of chunks.
	// FIXME: Quesize should be in units of i/o
	d.pendingwbytes += int64(sizebytes)
	qSz := float64(d.pendingwbytes) / float64(configStorage.sizeDataChunk * 1024)
	d.qMutex.Unlock()

	latency := d.latencySim.Latency(DiskLatencyParams{qSize: qSz})
	log(LogVV, fmt.Sprintf("Scheduling chunk write current QDepth:%v, Induced delay: %v", qSz, latency))
	at += latency

	if d.writes == 1 {
		d.nextWrDone = Now.Add(at)
	}
	if Now.Before(d.lastIOdone) {
		d.lastIOdone = d.lastIOdone.Add(at)
		log(LogVVV, fmt.Sprintf("next-chunk complete in :%v", d.lastIOdone.Sub(time.Time{})))
		at1 := d.lastIOdone.Sub(Now)
		return at1
	}

	d.lastIOdone = Now.Add(at)
	log(LogVVV, fmt.Sprintf("next-chunk complete in :%v", d.lastIOdone.Sub(time.Time{})))
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

func init() {
	latencySimulators = make(map[string]DiskLatencySimulator, 8)
	latencySimulators["latency-nil"] = &latencyNil
}
