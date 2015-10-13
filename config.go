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
}

var configStorage = ConfigStorage{
	numReplicas:   3,
	sizeDataChunk: 128, // KB
	sizeMetaChunk: 1,   // KB
	diskMBps:      400, // MB/sec
}

//
// config: network
//
type ConfigNetwork struct {
	linkbps        int64
	sizeFrame      int
	sizeControlPDU int
	overheadpct    int
	leakymax       int
}

var configNetwork = ConfigNetwork{
	linkbps: 10 * 1000 * 1000 * 1000, // bits/sec

	sizeFrame:      9000, // L2 frame size (bytes)
	sizeControlPDU: 1000, // solicited/control PDU size (bytes)
	overheadpct:    1,    // L2 + L3 + L4 + L5 + arp, etc. overhead (%)
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
	if config.timeToRun >= time.Second {
		config.timeStatsIval = config.timeToRun / 1000
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
}
