// Package surge provides a framework for discrete event simulation.
// The package's own logging facility has a single main function
//  	func log(level string, args ...interface{})
// where the first argument is either a verbosity level (enumerated
// below) or the first value to log. The function accepts variable
// number of arguments and formats the output as a comma-separated
// line (easily parsable for statistics and reports).
// Each call to log() produces a separate line with system time
// printed at its left.
// For example:
//     9.913299ms  :srv-replica-received,[SRV#76],chunk#10860
// translates as follows:
// At the system (simulated, modeled) time that was precisely
// 9.913299ms from the start of the benchmark, the server #76 reports
// receiving full replica of the chunk with ID=10860, etc.

package surge

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	LogBoth = "both" // log to file and print on screen as well
	LogV    = "V"    // verbose
	LogVV   = "VV"   // super-verbose
	LogVVV  = "VVV"
	LogVVVV = "VVVV"
)

const gigb = float64(1000000000)

var logfd *os.File
var logstream *bufio.Writer
var logMutex = &sync.Mutex{}
var logTimestamp = true

func initLog() {
	if len(config.LogFile) > 0 {
		var err error
		logfd, err = os.Create(config.LogFile)
		assert(err == nil, "failed to create logfile", config.LogFile)
		logstream = bufio.NewWriter(logfd)
	}
}

func terminateLog() {
	if logfd != nil {
		logstream.Flush()
		err := logfd.Close()
		if err != nil {
			fmt.Println("error closing logfile", err, config.LogFile)
		}
	}
}

func timestampLog(ts bool) {
	logTimestamp = ts
}

func flushLog() {
	if logfd != nil {
		logMutex.Lock()
		defer logMutex.Unlock()
		logstream.Flush()
	}
}

func configLog(partial bool) {
	s1 := fmt.Sprintf("\t    {numGateways:%d numServers:%d}", config.numGateways, config.numServers)
	s2 := fmt.Sprintf("\t    {timeToRun:%v}", config.timeToRun)
	s3 := fmt.Sprintf("\t    {linkbps:%.1fGbps linkbpsControl:%.1fGbps linkbpsData:%.1fGbps}",
		float64(configNetwork.linkbps)/gigb,
		float64(configNetwork.linkbpsControl)/gigb,
		float64(configNetwork.linkbpsData)/gigb)
	s4 := fmt.Sprintf("\t    {sizeFrame:%vB sizeControlPDU:%vB}", configNetwork.sizeFrame, configNetwork.sizeControlPDU)
	s5 := fmt.Sprintf("\t    {durationControlPDU:%v netdurationDataChunk:%v}", configNetwork.durationControlPDU, configNetwork.netdurationDataChunk)
	s6 := fmt.Sprintf("\t    {sizeDataChunk:%vKB dskdurationDataChunk:%v diskbps:%.1fGbps}",
		configStorage.sizeDataChunk, configStorage.dskdurationDataChunk, float64(configStorage.diskbps)/gigb)

	if partial {
		fmt.Println(s1)
		fmt.Println(s2)
		fmt.Println(s3)
		fmt.Println(s4)
		fmt.Println(s5)
		fmt.Println(s6)
		return
	}
	log("Configuration:")
	log(s1)
	log(s2)
	log(s3)
	log(s4)
	log(s5)
	log(s6)
	log("Detailed raw config:")
	log(fmt.Sprintf("\t    %+v", configStorage))
	log(fmt.Sprintf("\t    %+v", configAIMD))
	log(fmt.Sprintf("\t    %+v", configReplicast))
	log(fmt.Sprintf("\t    {timeIncStep:%v timeClusterTrip:%v timeStatsIval:%v timeTrackIval:%v timeToRun:%v}",
		config.timeIncStep, config.timeClusterTrip, config.timeStatsIval, config.timeTrackIval, config.timeToRun))
}

//
// the logger
//
func log(level string, args ...interface{}) {
	l1 := len(args) - 1
	sincestartup := Now.Sub(time.Time{})
	logboth := false // terminal and log, both

	var message = fmt.Sprintf("%-12.10v:", sincestartup)
	if !logTimestamp {
		message = ""
	}
	if level == "" || level == LogV || strings.HasPrefix(level, LogVV) {
		if len(level) > len(config.LogLevel) {
			return
		}
	} else if level == LogBoth {
		logboth = true
	} else {
		if l1 >= 0 {
			message += fmt.Sprintf("%s,", level)
		} else {
			message += fmt.Sprintf("%s", level)
		}
	}

	for i := 0; i <= l1; i++ {
		if i < l1 {
			message += fmt.Sprintf("%v,", args[i])
		} else {
			message += fmt.Sprintf("%v", args[i])
		}
	}
	message += "\n"

	logMutex.Lock()
	defer logMutex.Unlock()

	if logfd == nil || logboth {
		fmt.Printf("%s", message)
	}
	if logfd != nil {
		logstream.WriteString(message)
	}
}
