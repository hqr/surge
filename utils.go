package surge

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	LogBoth = "both"
	LogV    = "V"
	LogVV   = "VV"
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
	s6 := fmt.Sprintf("\t    {sizeDataChunk:%vKB dskdurationDataChunk:%v}", configStorage.sizeDataChunk, configStorage.dskdurationDataChunk)

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

func assert(cond bool, args ...interface{}) {
	if !config.DEBUG {
		return
	}
	if cond {
		return
	}
	var message = "assertion failed"
	if len(args) > 0 {
		message += ": "
		for i := 0; i < len(args); i++ {
			message += fmt.Sprintf("%#v ", args[i])
		}
	}
	// log(message)
	panic(message)
}

func calcAvgStd(vec []int) (int, float64, float64) {
	total := 0
	l := len(vec)
	for i := 0; i < l; i++ {
		total += vec[i]
	}
	avg := float64(total) / float64(l)

	std := float64(0)
	for i := 0; i < l; i++ {
		x := float64(vec[i]) - avg
		std += x * x
	}
	std = math.Sqrt(std / float64(l-1))
	return total, avg, std
}

//
// random globally-unique 64bit and its logging (short) counterpart
// FIXME: rewrite using crypto
//
func uqrandom64(multiplier int) (int64, int64) {
	uqid := rand.Int63n(time.Now().UTC().UnixNano() / 10007 * int64(multiplier))
	return uqid, uqrand(uqid)
}

func uqrand(uqid int64) int64 {
	return uqid & 0xffff
}

func clusterTripPlusRandom() time.Duration {
	trip := config.timeClusterTrip
	at := rand.Int63n(int64(trip)) + int64(trip) + 1
	return time.Duration(at)
}

func sizeToDuration(size int, sizeunits string, bw int64, bwunits string) time.Duration {
	var sizebits, bwbitss int64
	switch {
	case strings.HasPrefix(sizeunits, "b"):
		sizebits = int64(size)
	case strings.HasPrefix(sizeunits, "B"):
		sizebits = int64(size) * int64(8)
	case strings.HasPrefix(sizeunits, "k") || strings.HasPrefix(sizeunits, "K"):
		sizebits = int64(size) * int64(1024*8)
	case strings.HasPrefix(sizeunits, "m") || strings.HasPrefix(sizeunits, "M"):
		sizebits = int64(size) * int64(1024*1024*8)
	case strings.HasPrefix(sizeunits, "g") || strings.HasPrefix(sizeunits, "G"):
		sizebits = int64(size) * int64(1024*1024*1024*8)
	default:
		assert(false, "invalid sizeunits: "+sizeunits)
	}
	switch {
	case strings.HasPrefix(bwunits, "b"):
		bwbitss = bw
	case strings.HasPrefix(bwunits, "B"):
		bwbitss = bw * int64(8)
	case strings.HasPrefix(bwunits, "KB"):
		bwbitss = bw * int64(1024*8)
	case strings.HasPrefix(bwunits, "Kb"):
		bwbitss = bw * int64(1024)
	case strings.HasPrefix(bwunits, "MB"):
		bwbitss = bw * int64(1024*1024*8)
	case strings.HasPrefix(bwunits, "Mb"):
		bwbitss = bw * int64(1024*1024)
	case strings.HasPrefix(bwunits, "GB"):
		bwbitss = bw * int64(1024*1024*1024*8)
	case strings.HasPrefix(bwunits, "Gb"):
		bwbitss = bw * int64(1024*1024*1024)
	default:
		assert(false, "invalid bwunits: "+bwunits)
	}
	return time.Duration(sizebits) * time.Second / time.Duration(bwbitss)
}

func bytesToKMG(bytes int64) string {
	x := float64(bytes)
	switch {
	case bytes < 1024:
		return fmt.Sprintf("%dB", bytes)
	case bytes < 1024*1024:
		return fmt.Sprintf("%.2fKB", x/1024.0)
	case bytes < 1024*1024*1024:
		return fmt.Sprintf("%.2fMB", x/1024.0/1024.0)
	default:
		return fmt.Sprintf("%.2fGB", x/1024.0/1024.0/1024.0)
	}
}

func bytesMillisToKMGseconds(bytesms float64) string {
	x := bytesms * 1000.0
	switch {
	case x < 1024:
		return fmt.Sprintf("%.0fB/s", x)
	case x < 1024*1024:
		return fmt.Sprintf("%.2fKB/s", x/1024.0)
	case x < 1024*1024*1024:
		return fmt.Sprintf("%.2fMB/s", x/1024.0/1024.0)
	default:
		return fmt.Sprintf("%.2fGB/s", x/1024.0/1024.0/1024.0)
	}
}
