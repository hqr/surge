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
	LOG_BOTH = "both"
	LOG_V    = "V"
	LOG_VV   = "VV"
	LOG_VVV  = "VVV"
	LOG_VVVV = "VVVV"
)

var logfd *os.File = nil
var logstream *bufio.Writer = nil
var lastflush time.Time = time.Time{}
var logMutex *sync.Mutex = &sync.Mutex{}

func initLog() {
	if len(config.LogFile) > 0 {
		var err error = nil
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

func log(level string, args ...interface{}) {
	l1 := len(args) - 1
	sincestartup := Now.Sub(time.Time{})
	logboth := false // terminal and log, both

	var message = fmt.Sprintf("%-12.10v:", sincestartup)
	if level == "" || level == LOG_V || strings.HasPrefix(level, LOG_VV) {
		if len(level) > len(config.LogLevel) {
			return
		}
	} else if level == LOG_BOTH {
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
