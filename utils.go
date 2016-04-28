package surge

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"
)

func assert(cond bool, args ...interface{}) {
	if !config.DEBUG {
		return
	}
	if cond {
		return
	}
	sincestartup := Now.Sub(time.Time{})
	var message = fmt.Sprintf("%-12.10v:%s", "assertion failed", sincestartup)
	if len(args) > 0 {
		message += ": "
		for i := 0; i < len(args); i++ {
			message += fmt.Sprintf("%#v ", args[i])
		}
	}
	log(message)
	flushLog()
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

func bytesMillisToMseconds(bytesms float64) string {
	x := bytesms * 1000.0 / 1024.0 / 1024.0
	return fmt.Sprintf("%.2f", x)
}

//
// given netDelivered time (in the future) when a new complete chunk
// will have been delivered, compute the required disk queue delay
//
func diskdelay(netDelivered time.Time, diskIOdone time.Time) time.Duration {
	elapsed := Now.Sub(netDelivered) // negative
	thenIOdone := diskIOdone.Add(elapsed)
	if thenIOdone.Before(netDelivered) {
		return 0
	}
	diff := thenIOdone.Sub(netDelivered)
	d1 := configStorage.dskdurationDataChunk * time.Duration(configStorage.maxDiskQueueChunks-1)
	if diff <= d1 {
		return 0
	}
	return diff - d1
}
