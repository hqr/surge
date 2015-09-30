package surge

import (
	"flag"
	"time"
)

//
// config
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
	LogFile                              string
	DEBUG                                bool
	srand                                int
}

var config = Config{
	numGateways: 20,
	numServers:  20,
	numDisksPer: 1,

	mprefix: "",

	timeIncStep:     time.Nanosecond * 10,
	timeClusterTrip: time.Microsecond * 20,
	timeStatsIval:   time.Microsecond * 100,
	timeTrackIval:   time.Millisecond,
	timeToRun:       time.Millisecond, // total time to run (ms)

	channelBuffer: 128,

	LogLevel: "", // quiet
	LogFile:  "/tmp/log.csv",
	DEBUG:    true,
	srand:    1,
}

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

	flag.Parse()

	config.numGateways = *gwPtr
	config.numServers = *srPtr

	config.mprefix = *moPtr

	config.timeToRun = time.Duration(*trPtr)
	config.timeStatsIval = config.timeToRun / 100
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
}
