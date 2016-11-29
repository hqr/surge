// Package surge provides a framework for discrete event simulation, as well as
// a number of models for Unsolicited and Reservation Group based Edge-driven
// load balancing. Targeted modeling area includes large and super-large storage
// clusters with multiple access points (referred to as "gateways") and multiple
// storage targets (referred to as "servers").
package surge

import (
	"flag"
	"fmt"
	"os"
	"time"
)

var build string

// TODO: Redesign the verbosity command line option
//       This is just to have the old behaviour with the current framework
//       This is a bit ugly and needs to be revisted
var q, v, vv, vvv, vvvv bool

const (
	transportTypeDefault   = "default" // default for the model (or, the only supported)
	transportTypeUnicast   = "unicast"
	transportTypeMulticast = "multicast"
)

//
// config: common and miscellaneous
//
type Config struct {
	numGateways, numServers, numDisksPer int
	mprefix                              string
	timeIncStep                          time.Duration
	timeClusterTrip                      time.Duration
	timeStatsIval                        time.Duration
	timeTrackIval                        time.Duration
	timeToRun                            time.Duration
	LogLevel                             string
	realtimeLogStats                     time.Duration
	LogFile                              string
	DEBUG                                bool
	ValidateConfig                       bool	// Validate configuration and error out for invalid conifgs
	channelBuffer                        int
	srand                                int
	learner				     string
	// derived
	LogFileOrig string
}

var config = Config{
	numGateways: 10,
	numServers:  10,
	numDisksPer: 1,

	mprefix: "",

	timeIncStep:     time.Nanosecond, // the Tick
	timeClusterTrip: time.Microsecond,

	timeStatsIval: time.Millisecond / 100,
	timeTrackIval: time.Millisecond / 10,
	timeToRun:     time.Millisecond, // ttr: total time to run

	LogLevel:         "", // quiet
	realtimeLogStats: time.Second * 60,
	LogFile:          "/tmp/log.csv",
	DEBUG:            true,
	ValidateConfig:   false,

	channelBuffer: 16,
	srand:         1,
	learner:	"simple-learner",
}

//
// config: storage
//
type ConfigStorage struct {
	numReplicas                  int
	sizeDataChunk, sizeMetaChunk int
	diskMBps                     int
	maxDiskQueue                 int
	read                         bool // false => 100% write | true => 50% read, 50% write
	// derived from other config, for convenience
	dskdurationDataChunk time.Duration
	dskdurationFrame     time.Duration
	diskbps              int64
	diskLatencySim       string
	maxDiskQueueChunks   int
}

var configStorage = ConfigStorage{
	numReplicas:   3,
	sizeDataChunk: 128, // KB
	sizeMetaChunk: 1,   // KB
	diskMBps:      400, // MB/sec
	maxDiskQueue:  256, // KB
	read:          false,
	diskLatencySim:	"latency-nil",
}

//
// config: network
//
type ConfigNetwork struct {
	linkbps        int64
	sizeFrame      int
	sizeControlPDU int
	overheadpct    int
	reserved       int
	transportType  string
	// derived from other config, for convenience
	linkbpsorig          int64
	maxratebucketval     int64
	durationControlPDU   time.Duration
	linkbpsControl       int64
	linkbpsData          int64
	cmdWindowSz          int   // cmd Window size in number of chunks.
	netdurationDataChunk time.Duration
	netdurationFrame     time.Duration
}

// NOTE:
// one way to get rid of "past deadline"/"past trigger" warnings (if any)
// would be to bump sizeControlPDU up to, e.g. 9000
var configNetwork = ConfigNetwork{
	linkbps: 10 * 1000 * 1000 * 1000, // bits/sec

	sizeFrame:      9000, // L2 frame size (bytes)
	sizeControlPDU: 300,  // control PDU size (bytes); note that unicast use 100B default; see note above
	overheadpct:    1,    // L2 + L3 + L4 + L5 + arp, etc. overhead (%)

	cmdWindowSz:	2,

	transportType: transportTypeDefault, // transportType* const above
}

//
// AIMD
//
type ConfigAIMD struct {
	// additive increase (step) in absence of dings, as well as
	bwMinInitialAdd int64 // (client) initial and minimum bandwidth, bits/sec
	sizeAddBits     int64 // (client) number of bits to transmit without dings prior to += bwAdd
	bwDiv           int   // (client) multiplicative decrease
	linkoverage     int   // (target) ding when the network queue gets too deep
}

var configAIMD = ConfigAIMD{
	bwMinInitialAdd: configNetwork.linkbps / int64(10),
	sizeAddBits:     int64(configNetwork.sizeFrame*8) + int64(configNetwork.sizeControlPDU*8),
	bwDiv:           2,
	linkoverage:     2,
}

//
// Replicast
//
type ConfigReplicast struct {
	sizeNgtGroup     int
	bidMultiplierPct int
	bidGapBytes      int
	solicitedLinkPct int
	maxBidWait       time.Duration
	// derived from other config, for convenience
	durationBidGap    time.Duration
	durationBidWindow time.Duration
	numNgtGroups      int
	minduration       time.Duration
}

var configReplicast = ConfigReplicast{
	sizeNgtGroup:     9,
	bidMultiplierPct: 140,
	bidGapBytes:      0, // configNetwork.sizeControlPDU,
	solicitedLinkPct: 90,
	maxBidWait:       (configNetwork.durationControlPDU + config.timeClusterTrip) * 6, // 3*RTT
}

func PreConfig() {
	flag.IntVar(&config.numGateways, "gateways", config.numGateways, "number of gateways")
	flag.IntVar(&config.numServers, "servers", config.numServers, "number of servers")

	flag.StringVar(&config.mprefix, "m", config.mprefix, "prefix that defines which models to run, use \"\" to run all")

	flag.DurationVar(&config.timeToRun, "ttr", config.timeToRun,
		"time to run, e.g. 1500us, 350ms, 15s (depending on the model and RAM/CPU, a 10ms run may take 30min and beyond)")

	flag.StringVar(&config.LogFile, "log", config.LogFile, "log file, use -log=\"\" for stdout")

	flag.BoolVar(&q, "q", false, "quiet mode, minimal logging")
	flag.BoolVar(&v, "v", false, "verbose")
	flag.BoolVar(&vv, "vv", false, "verbose-verbose")
	flag.BoolVar(&vvv, "vvv", false, "super-verbose")
	flag.BoolVar(&vvvv, "vvvv", false, "extra-super-verbose")

	flag.BoolVar(&config.DEBUG, "d", config.DEBUG, "debug=true|false")
	flag.BoolVar(&config.ValidateConfig, "validateconfig", config.ValidateConfig, "true|false. Error out on invalid config if true, else Reset to default sane values.")
	flag.IntVar(&config.srand, "srand", config.srand, "random seed, use 0 (zero) for random seed selection")
	flag.StringVar(&config.learner, "learner", config.learner, "Macine learning module to be used for the model")

	flag.IntVar(&configStorage.numReplicas, "replicas", configStorage.numReplicas, "number of replicas")
	flag.IntVar(&configStorage.sizeDataChunk, "chunksize", configStorage.sizeDataChunk, "chunk size (KB)")
	flag.IntVar(&configStorage.diskMBps, "diskthroughput", configStorage.diskMBps, "disk throughput (MB/sec)")
	flag.StringVar(&configStorage.diskLatencySim, "disklatencysim", configStorage.diskLatencySim, "disk Latency Simulator function")

	flag.BoolVar(&configStorage.read, "r", configStorage.read, "read=false(100% write) | true(50/50% read/write)")

	flag.IntVar(&configStorage.maxDiskQueue, "diskqueue", configStorage.maxDiskQueue, "disk queue size (KB)")

	flag.IntVar(&configNetwork.sizeFrame, "l2frame", configNetwork.sizeFrame, "L2 frame size (bytes)")
	flag.Int64Var(&configNetwork.linkbps, "linkbps", configNetwork.linkbps, "Network Link Bandwidth (bits/sec)")
	flag.StringVar(&configNetwork.transportType, "transport", configNetwork.transportType,
		"transport type: [default | unicast | multicast]")

	flag.IntVar(&configNetwork.cmdWindowSz, "cmdwindowsz", configNetwork.cmdWindowSz, "Command window size in unit of chunks")

	flag.IntVar(&configReplicast.sizeNgtGroup, "groupsize", configReplicast.sizeNgtGroup,
		"group size: number of servers in a multicast group")

	flag.StringVar(&build, "build", build,
		"build ID (as in: 'git rev-parse'), or any user-defined string to be used as a logfile name suffix")
}

func ParseCommandLine() {
	//
	// parse command line
	//
	flag.Parse()
}

func PostConfig() {

	config.timeStatsIval = config.timeToRun / 100
	switch {
	case config.timeToRun >= time.Second:
		config.timeStatsIval = config.timeToRun / 1000
	default:
		config.timeStatsIval = config.timeToRun / 100
	}

	config.timeTrackIval = config.timeToRun / 10

	config.LogFileOrig = config.LogFile

	if q {
		config.LogLevel = ""
	} else if v {
		config.LogLevel = LogV
	} else if vv {
		config.LogLevel = LogVV
	} else if vvv {
		config.LogLevel = LogVVV
	} else if vvvv {
		config.LogLevel = LogVVVV
	}

	//
	// computed and assigned (here for convenience)
	//
	configNetwork.maxratebucketval = int64(configNetwork.sizeFrame*8) + int64(configNetwork.sizeControlPDU*8)
	configNetwork.linkbpsorig = configNetwork.linkbps
	configNetwork.linkbps = configNetwork.linkbps - configNetwork.linkbps*int64(configNetwork.overheadpct)/int64(100)

	// the 4 confvars defaults below are based on the full linkbps bw
	configNetwork.durationControlPDU =
		time.Duration(configNetwork.sizeControlPDU*8) * time.Second / time.Duration(configNetwork.linkbps)

	//
	// NOTE: these two may be changed by the models that
	//       separately provision network bandwidth for control and data
	//       therefore, depending computed durations must change as well, accordingly
	//
	configNetwork.linkbpsControl = configNetwork.linkbps
	configNetwork.linkbpsData = configNetwork.linkbps

	configNetwork.netdurationDataChunk =
		time.Duration(configStorage.sizeDataChunk*1024*8) * time.Second / time.Duration(configNetwork.linkbps)
	configNetwork.netdurationFrame = time.Duration(configNetwork.sizeFrame*8) * time.Second / time.Duration(configNetwork.linkbps)

	configStorage.maxDiskQueueChunks = configStorage.maxDiskQueue / configStorage.sizeDataChunk
	if configStorage.maxDiskQueueChunks < 2 {
		configStorage.maxDiskQueueChunks = 2
	}
	configStorage.dskdurationDataChunk = sizeToDuration(configStorage.sizeDataChunk, "KB", int64(configStorage.diskMBps), "MB")
	configStorage.dskdurationFrame = sizeToDuration(configNetwork.sizeFrame, "B", int64(configStorage.diskMBps), "MB")
	//
	// Note: MiB effectively, here and in sizeToDuration()
	//
	configStorage.diskbps = int64(configStorage.diskMBps) * 1024 * 1024 * 8

}

// NOTE:
//   1) Multicast control place multiplies amount of the control traffic:
//      each server in the negotiating group sees all control packets sent to this group, etc.
//      That is why, unlike UCH-* unicast models, this model provisions control
//      bandwidth as a separate significant (configurable) percentage of the total link
//
//   2) Recompute network durations accordingly
//   3) TODO: model.go to round up numServers
//
func configureReplicast(unicastBidMultiplier bool) {
	rem := config.numServers % configReplicast.sizeNgtGroup
	if rem > 0 {
		config.numServers -= rem
		log(LogBoth, "NOTE: adjusting the number of servers down, to be a multiple of negotiating group size:", config.numServers)
	}
	if config.numServers <= configReplicast.sizeNgtGroup {
		log(LogBoth, "Cannot execute the model with a single negotiating group configured..")
		if config.ValidateConfig {
			log(LogBoth, "Exiting..")
			os.Exit(1)
		}
		config.numServers = configReplicast.sizeNgtGroup * 2
		log(LogBoth,
			fmt.Sprintf(
				"Resetting to sane default values: groupsize=%d Servers=%d",
				configReplicast.sizeNgtGroup, config.numServers))
	}

	configReplicast.numNgtGroups = config.numServers / configReplicast.sizeNgtGroup

	configNetwork.linkbpsData = configNetwork.linkbps * int64(configReplicast.solicitedLinkPct) / int64(100)
	configNetwork.linkbpsControl = configNetwork.linkbps - configNetwork.linkbpsData

	configNetwork.durationControlPDU = time.Duration(configNetwork.sizeControlPDU*8) * time.Second / time.Duration(configNetwork.linkbpsControl)
	configNetwork.netdurationDataChunk = time.Duration(configStorage.sizeDataChunk*1024*8) * time.Second / time.Duration(configNetwork.linkbpsData)

	if unicastBidMultiplier {
		configReplicast.bidMultiplierPct = configStorage.numReplicas * 110
	}
	x := configNetwork.netdurationDataChunk + config.timeClusterTrip
	configReplicast.durationBidWindow = x * time.Duration(configReplicast.bidMultiplierPct) / time.Duration(100)

	configNetwork.netdurationFrame = time.Duration(configNetwork.sizeFrame*8) * time.Second / time.Duration(configNetwork.linkbpsData)

	configReplicast.minduration = configNetwork.netdurationDataChunk + configNetwork.netdurationFrame
	if unicastBidMultiplier {
		configReplicast.minduration *= time.Duration(configStorage.numReplicas)
	}
	if configReplicast.durationBidWindow < configReplicast.minduration {
		configReplicast.durationBidWindow = configReplicast.minduration
	}
}

// Wrapper methods for flag
func RegisterCmdlineBoolVar(dest *bool, opt string, def bool, desc string) bool {
	if flag.Parsed() {
		return false
	}
	flag.BoolVar(dest, opt, def, desc)

	return true
}

func RegisterCmdlineIntVar(dest *int, opt string, def int, desc string) bool {
	if flag.Parsed() {
		return false
	}
	flag.IntVar(dest, opt, def, desc)

	return true
}

func RegisterCmdlineInt64Var(dest *int64, opt string, def int64, desc string) bool {
	if flag.Parsed() {
		return false
	}
	flag.Int64Var(dest, opt, def, desc)

	return true
}

func RegisterCmdlineStringVar(dest *string, opt string, def string, desc string) bool {
	if flag.Parsed() {
		return false
	}
	flag.StringVar(dest, opt, def, desc)

	return true
}

func RegisterCmdlineDurationVar(dest *time.Duration, opt string, def time.Duration, desc string) bool {
	if flag.Parsed() {
		return false
	}
	flag.DurationVar(dest, opt, def, desc)

	return true
}

func RegisterCmdlineUintVar(dest *uint, opt string, def uint, desc string) bool {
	if flag.Parsed() {
		return false
	}
	flag.UintVar(dest, opt, def, desc)

	return true
}

func RegisterCmdlineUint64Var(dest *uint64, opt string, def uint64, desc string) bool {
	if flag.Parsed() {
		return false
	}
	flag.Uint64Var(dest, opt, def, desc)

	return true
}

func RegisterCmdlineFloat64Var(dest *float64, opt string, def float64, desc string) bool {
	if flag.Parsed() {
		return false
	}
	flag.Float64Var(dest, opt, def, desc)

	return true
}
