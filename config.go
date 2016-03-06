// Package surge provides a framework for discrete event simulation, as well as
// a number of models for Unsolicited and Reservation Group based Edge-driven
// load balancing. Targeted modeling area includes large and super-large storage
// clusters with multiple access points (referred to as "gateways") and multiple
// storage targets (referred to as "servers").
package surge

import (
	"flag"
	"os"
	"time"
)

var build string

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
	channelBuffer                        int
	srand                                int
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

	channelBuffer: 16,
	srand:         1,
}

//
// config: storage
//
type ConfigStorage struct {
	numReplicas                  int
	sizeDataChunk, sizeMetaChunk int
	diskMBps                     int
	maxDiskQueue                 int
	chunksInFlight               int // TODO: UCH-* models to start next chunk without waiting for ACK..
	// derived from other config, for convenience
	dskdurationDataChunk time.Duration
	dskdurationFrame     time.Duration
	diskbps              int64
	maxDiskQueueChunks   int
}

var configStorage = ConfigStorage{
	numReplicas:    3,
	sizeDataChunk:  128, // KB
	sizeMetaChunk:  1,   // KB
	diskMBps:       400, // MB/sec
	maxDiskQueue:   256, // KB
	chunksInFlight: 1,   // TODO: limits total in-flight for a given gateway
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
	netdurationDataChunk time.Duration
	netdurationFrame     time.Duration
}

var configNetwork = ConfigNetwork{
	linkbps: 10 * 1000 * 1000 * 1000, // bits/sec

	sizeFrame:      9000, // L2 frame size (bytes)
	sizeControlPDU: 300,  // control PDU size (bytes); note that unicast use 100bytes instead
	overheadpct:    1,    // L2 + L3 + L4 + L5 + arp, etc. overhead (%)

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

//===============================================================
// init
//===============================================================
func init() {
	gwPtr := flag.Int("gateways", config.numGateways, "number of gateways")
	srPtr := flag.Int("servers", config.numServers, "number of servers")

	moPtr := flag.String("m", config.mprefix, "prefix that defines which models to run, use \"\" to run all")

	trPtr := flag.Duration("ttr", config.timeToRun, "time to run, e.g. 1500us, 350ms, 15s (depending on the model and RAM/CPU, a 10ms run may take 30min and beyond)")

	lfPtr := flag.String("log", config.LogFile, "log file, use -log=\"\" for stdout")

	qPtr := flag.Bool("q", false, "quiet mode, minimal logging")
	vPtr := flag.Bool("v", false, "verbose")
	vvPtr := flag.Bool("vv", false, "verbose-verbose")
	vvvPtr := flag.Bool("vvv", false, "super-verbose")
	vvvvPtr := flag.Bool("vvvv", false, "extra-super-verbose")

	dbPtr := flag.Bool("d", config.DEBUG, "debug=true|false")
	srandPtr := flag.Int("srand", config.srand, "random seed, use 0 (zero) for random seed selection")

	replicasPtr := flag.Int("replicas", configStorage.numReplicas, "number of replicas")
	chunksizePtr := flag.Int("chunksize", configStorage.sizeDataChunk, "chunk size (KB)")
	diskthPtr := flag.Int("diskthroughput", configStorage.diskMBps, "disk throughput (MB/sec)")

	l2framePtr := flag.Int("l2frame", configNetwork.sizeFrame, "L2 frame size (bytes)")
	linkbpsPtr := flag.Int64("linkbps", configNetwork.linkbps, "Network Link Bandwidth (bits/sec)")
	transportPtr := flag.String("transport", configNetwork.transportType, "transport type: [default | unicast | multicast]")

	buildPtr := flag.String("build", build, "build ID (as in: 'git rev-parse'), or any user-defined string to be used as a logfile name suffix")
	//
	// parse command line
	//
	flag.Parse()

	config.numGateways = *gwPtr
	config.numServers = *srPtr

	config.mprefix = *moPtr

	config.timeToRun = time.Duration(*trPtr)
	config.timeStatsIval = config.timeToRun / 100
	switch {
	case config.timeToRun >= time.Second:
		config.timeStatsIval = config.timeToRun / 1000
	default:
		config.timeStatsIval = config.timeToRun / 100
	}

	config.timeTrackIval = config.timeToRun / 10

	config.LogFileOrig = config.LogFile
	config.LogFile = *lfPtr

	if *qPtr {
		config.LogLevel = ""
	} else if *vPtr {
		config.LogLevel = LogV
	} else if *vvPtr {
		config.LogLevel = LogVV
	} else if *vvvPtr {
		config.LogLevel = LogVVV
	} else if *vvvvPtr {
		config.LogLevel = LogVVVV
	}

	config.DEBUG = *dbPtr
	config.srand = *srandPtr

	configStorage.numReplicas = *replicasPtr
	configStorage.sizeDataChunk = *chunksizePtr
	configStorage.diskMBps = *diskthPtr

	configNetwork.sizeFrame = *l2framePtr
	configNetwork.linkbps = *linkbpsPtr
	configNetwork.transportType = *transportPtr

	build = *buildPtr
	//
	// computed and assigned (here for convenience)
	//
	configNetwork.maxratebucketval = int64(configNetwork.sizeFrame*8) + int64(configNetwork.sizeControlPDU*8)
	configNetwork.linkbpsorig = configNetwork.linkbps
	configNetwork.linkbps = configNetwork.linkbps - configNetwork.linkbps*int64(configNetwork.overheadpct)/int64(100)

	// the 4 confvars defaults below are based on the full linkbps bw
	configNetwork.durationControlPDU = time.Duration(configNetwork.sizeControlPDU*8) * time.Second / time.Duration(configNetwork.linkbps)

	//
	// NOTE: these two may be changed by the models that
	//       separately provision network bandwidth for control and data
	//       therefore, depending computed durations must change as well, accordingly
	//
	configNetwork.linkbpsControl = configNetwork.linkbps
	configNetwork.linkbpsData = configNetwork.linkbps

	configNetwork.netdurationDataChunk = time.Duration(configStorage.sizeDataChunk*1024*8) * time.Second / time.Duration(configNetwork.linkbps)
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
		if config.numServers == configReplicast.sizeNgtGroup {
			log(LogBoth, "Cannot execute the model with a single negotiating group configured, exiting..")
			os.Exit(1)
		}
		log(LogBoth, "NOTE: adjusting the number of servers down, to be a multiple of negotiating group size:", config.numServers)
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
		configReplicast.durationBidWindow = configReplicast.minduration + configNetwork.netdurationFrame
	}
}
