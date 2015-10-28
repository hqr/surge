package surge

import (
	"flag"
	"time"
)

//
// config: common and miscellaneous
//
type Config struct {
	numGateways, numServers, numDisksPer int
	mprefix                              string
	timeIncStep                          time.Duration
	timeClusterTrip                      time.Duration
	timeTripMax                          time.Duration // TODO: randomize trip times
	timeStatsIval                        time.Duration
	timeTrackIval                        time.Duration
	timeToRun                            time.Duration
	channelBuffer                        int
	LogLevel                             string
	realtimeLogStats                     time.Duration
	LogFile                              string
	DEBUG                                bool
	srand                                int
}

var config = Config{
	numGateways: 100,
	numServers:  100,
	numDisksPer: 1,

	mprefix: "",

	timeIncStep:     time.Nanosecond, // the Tick
	timeClusterTrip: time.Microsecond,
	timeTripMax:     time.Microsecond * 2, // TODO: must be F(busy%, timeClusterTrip)

	timeStatsIval: time.Millisecond / 100,
	timeTrackIval: time.Millisecond / 10,
	timeToRun:     time.Millisecond, // ttr: total time to run

	channelBuffer: 16,

	LogLevel:         "", // quiet
	LogFile:          "/tmp/log.csv",
	realtimeLogStats: time.Second * 60,
	DEBUG:            true,
	srand:            1,
}

//
// config: storage
//
type ConfigStorage struct {
	numReplicas                  int
	sizeDataChunk, sizeMetaChunk int
	diskMBps                     int
	chunksInFlight               int // TODO: UCH-* models to start next chunk without waiting for ACK..
}

var configStorage = ConfigStorage{
	numReplicas:    3,
	sizeDataChunk:  128, // KB
	sizeMetaChunk:  1,   // KB
	diskMBps:       400, // MB/sec
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
	// computed and assigned below
	linkbpsminus     int64
	maxratebucketval int64
}

var configNetwork = ConfigNetwork{
	linkbps: 10 * 1000 * 1000 * 1000, // bits/sec

	sizeFrame:      9000, // L2 frame size (bytes)
	sizeControlPDU: 1000, // solicited/control PDU size (bytes)
	overheadpct:    1,    // L2 + L3 + L4 + L5 + arp, etc. overhead (%)
}

//
// AIMD
//
type ConfigAIMD struct {
	// additive increase (step) in absence of dings, as well as
	bwMinInitialAdd int64         // (client) initial and minimum bandwidth, bits/sec
	timeAdd         time.Duration // (client) dingless interval of time prior to += bwAdd
	bwDiv           int           // (client) multiplicative decrease
	bwMaxPctToDing  int           // (target) TODO ding when over the percentage of available
}

var configAIMD = ConfigAIMD{
	bwMinInitialAdd: 1 * 1000 * 1000 * 1000, // TODO: add and min together
	timeAdd:         config.timeClusterTrip * 3,
	bwDiv:           2,
	bwMaxPctToDing:  80, // TODO: must instead take into account the speed of growth..
}

//
// init
//
func init() {
	gwPtr := flag.Int("gateways", config.numGateways, "number of gateways")
	srPtr := flag.Int("servers", config.numServers, "number of servers")

	moPtr := flag.String("m", config.mprefix, "prefix that defines which models to run, use \"\" to run all")

	trPtr := flag.Duration("ttr", config.timeToRun, "time to run, e.g. 1500us, 350ms, 15s")

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

	config.LogFile = *lfPtr
	if *qPtr {
		config.LogLevel = ""
	} else if *vPtr {
		config.LogLevel = LOG_V
	} else if *vvPtr {
		config.LogLevel = LOG_VV
	} else if *vvvPtr {
		config.LogLevel = LOG_VVV
	} else if *vvvvPtr {
		config.LogLevel = LOG_VVVV
	}

	config.DEBUG = *dbPtr
	config.srand = *srandPtr

	configStorage.numReplicas = *replicasPtr
	configStorage.sizeDataChunk = *chunksizePtr
	configStorage.diskMBps = *diskthPtr

	configNetwork.sizeFrame = *l2framePtr
	configNetwork.linkbps = *linkbpsPtr

	configNetwork.maxratebucketval = int64(configNetwork.sizeFrame*8) + int64(configNetwork.sizeControlPDU*8)
	configNetwork.linkbpsminus = configNetwork.linkbps - configNetwork.linkbps*int64(configNetwork.overheadpct)/int64(100)
}
