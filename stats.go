package surge

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

//
// constants
//
type StatsKindEnum int

const (
	StatsKindUndef StatsKindEnum = iota
	StatsKindCount
	StatsKindByteCount
	StatsKindSampleCount
	StatsKindPercentage
)

type StatsScopeEnum int

const (
	StatsScopeUndef StatsScopeEnum = 1 << iota
	StatsScopeGateway
	StatsScopeServer
)

const maxStatsDescriptors int = 10

//
// types
//
type StatsDescriptor struct {
	name  string
	kind  StatsKindEnum
	scope StatsScopeEnum
}

type ModelStatsDescriptors struct {
	x map[string]*StatsDescriptor
}

type NodeStats map[string]int64 // all named stats counters for a given node (server | gateway)

type ModelStats struct {
	totalgwy     map[string]int64
	totalsrv     map[string]int64
	allNodeStats []NodeStats
	iter         int64
	lastUpdateTs time.Time
}

//
// static
//
var allDtors map[ModelName]*ModelStatsDescriptors
var mdtors *ModelStatsDescriptors
var mstats ModelStats
var mdtsortednames []string
var oneIterNodeStats []NodeStats

// FIXME: debug, temp
var boltzcounts []int64
var boltzlogthm []float64
var boltzwmutex *sync.Mutex

//=================================================================================
// init
//=================================================================================
func NewStatsDescriptors(name ModelName) *ModelStatsDescriptors {
	if allDtors == nil || len(allDtors) == 0 {
		allDtors = make(map[ModelName]*ModelStatsDescriptors, maxModels)
	}
	dtors := make(map[string]*StatsDescriptor, maxStatsDescriptors)
	allDtors[name] = &ModelStatsDescriptors{dtors}
	return allDtors[name]
}

func (dtors *ModelStatsDescriptors) Register(sname string, skind StatsKindEnum, scope StatsScopeEnum) {
	dtors.x[sname] = &StatsDescriptor{sname, skind, scope}
}

func (dtors *ModelStatsDescriptors) registerCommonProtoStats() {
	// uncomment if needed:
	//   dtors.Register("rxbusydata", StatsKindPercentage, StatsScopeServer)
	//   dtors.Register("tio", StatsKindCount, StatsScopeGateway)
	dtors.Register("rxbusy", StatsKindPercentage, StatsScopeServer) // data + control
	dtors.Register("diskbusy", StatsKindPercentage, StatsScopeServer)
	dtors.Register("disk-frame-bufs", StatsKindSampleCount, StatsScopeServer)

	dtors.Register("chunk", StatsKindCount, StatsScopeGateway)
	dtors.Register("replica", StatsKindCount, StatsScopeGateway)

	dtors.Register("txbytes", StatsKindByteCount, StatsScopeGateway|StatsScopeServer)
	dtors.Register("rxbytes", StatsKindByteCount, StatsScopeServer|StatsScopeGateway)
}

func (mstats *ModelStats) init(mname ModelName) {
	mdtors = allDtors[mname]
	assert(mdtors != nil)

	mstats.totalgwy = make(map[string]int64, len(mdtors.x))
	mstats.totalsrv = make(map[string]int64, len(mdtors.x))

	for k := range mdtors.x {
		mstats.totalgwy[k] = 0
		mstats.totalsrv[k] = 0
	}

	mstats.allNodeStats = make([]NodeStats, config.numGateways+config.numServers)
	oneIterNodeStats = make([]NodeStats, config.numGateways+config.numServers)

	for ij := 0; ij < config.numGateways+config.numServers; ij++ {
		mstats.allNodeStats[ij] = make(map[string]int64, len(mdtors.x))
		for n := range mdtors.x {
			mstats.allNodeStats[ij][n] = 0
		}
	}
	mstats.iter = 0
	mstats.lastUpdateTs = time.Time{}

	if cap(mdtsortednames) > 0 {
		mdtsortednames = mdtsortednames[0:0]
	}
	for n := range mdtors.x {
		mdtsortednames = append(mdtsortednames, n)
	}
	sort.Strings(mdtsortednames)

	boltzcounts = make([]int64, maxBidQueueSize)
	boltzlogthm = make([]float64, maxBidQueueSize)
	boltzwmutex = &sync.Mutex{}
}

//============================== stats =============================================
//
// ModelStats
//
//============================== stats =============================================
//
// results of the last timeStatsIval iteration folded into => mstats
//
func (mstats *ModelStats) logBoltz() {
	sc := "boltzcounts,"
	sl := "boltzlogthm,"
	maxi := 0
	assert(cap(boltzcounts) == maxBidQueueSize)
	for i := 0; i < maxBidQueueSize; i++ {
		if boltzcounts[i] > 0 {
			maxi = i
		}
	}
	var n int64
	for i := 0; i <= maxi; i++ {
		n += boltzcounts[i]
	}
	for i := 0; i <= maxi; i++ {
		if boltzcounts[i] == 0 {
			boltzlogthm[i] = -100.0
			continue
		}
		boltzlogthm[i] = math.Log(float64(boltzcounts[i]) / float64(n))
	}
	for i := 0; i <= maxi; i++ {
		sc += fmt.Sprintf("%d,", boltzcounts[i])
		sl += fmt.Sprintf("%.2f,", boltzlogthm[i])
	}
	log(strings.TrimSuffix(sc, ","))
	log(strings.TrimSuffix(sl, ","))

	for i := 0; i <= maxi; i++ {
		boltzcounts[i] = 0
	}
}

func (mstats *ModelStats) update(elapsed time.Duration) {
	mstats.iter++
	mstats.lastUpdateTs = Now
	for ij := 0; ij < config.numGateways+config.numServers; ij++ {
		r := allNodes[ij]
		oneIterNodeStats[ij] = r.GetStats(true)
	}
	_, ok := mdtors.x["bidepth"]
	if ok {
		mstats.logBoltz()
	}

	var newrxbytes string
	for n := range mdtors.x {
		if n == "bidepth" {
			continue
		}
		d := mdtors.x[n]
		newgwy, newsrv := int64(0), int64(0)
		for ij := 0; ij < config.numGateways+config.numServers; ij++ {
			scope := inScope(d, ij)
			if scope == StatsScopeUndef {
				continue
			}
			nodestats := oneIterNodeStats[ij]
			_, ok := nodestats[n]
			assert(ok, "missing stats counter: "+n)
			val := nodestats[n]
			if d.kind == StatsKindCount || d.kind == StatsKindByteCount || d.kind == StatsKindSampleCount {
				mstats.allNodeStats[ij][n] += val
				if scope == StatsScopeGateway {
					newgwy += val
					mstats.totalgwy[n] += val
				} else if scope == StatsScopeServer {
					newsrv += val
					mstats.totalsrv[n] += val
					if strings.Contains(n, "rxbytes") {
						newrxbytes += fmt.Sprintf("%d,", val)
					}
				}
			} else if d.kind == StatsKindPercentage {
				// StatsKindPercentage implies averaging done elsewhere, e.g.
				// the busy %% averaging continuously over cumulative time
				mstats.allNodeStats[ij][n] = val
				if scope == StatsScopeGateway {
					newgwy += val
				} else if scope == StatsScopeServer {
					newsrv += val
				}
			}
		}
		if len(newrxbytes) > 0 {
			log(LogV, fmt.Sprintf("new-srv-%s,%s", n, strings.TrimSuffix(newrxbytes, ",")))
		}

		// log one iteration
		if d.scope&StatsScopeGateway > 0 && newgwy != 0 {
			if d.kind == StatsKindCount || d.kind == StatsKindByteCount {
				spgwy := float64(newgwy) * (float64(time.Millisecond) / float64(elapsed))

				if d.kind == StatsKindByteCount {
					log(LogV, fmt.Sprintf("new-gwy-%s,%d,total-gwy-%s,%s,%s/s, %s", n, newgwy, n, bytesToKMG(mstats.totalgwy[n]), n, bytesMillisToKMGseconds(spgwy)))
				} else {
					log(LogV, fmt.Sprintf("new-gwy-%ss,%d,total-gwy-%ss,%d,%ss/ms, %.0f", n, newgwy, n, mstats.totalgwy[n], n, spgwy))
				}
			} else if d.kind == StatsKindSampleCount {
				avesample := float64(newgwy) / float64(config.numGateways)
				log(LogV, fmt.Sprintf("new-gwy-average-%s,%.1f", n, avesample))
			} else if d.kind == StatsKindPercentage {
				busygwy := float64(newgwy) / float64(config.numGateways)
				log(LogV, fmt.Sprintf("gwy-%s(%%),%.0f", n, busygwy))
			}
		}

		if d.scope&StatsScopeServer > 0 && newsrv != 0 {
			if d.kind == StatsKindCount || d.kind == StatsKindByteCount {
				spsrv := float64(newsrv) * (float64(time.Millisecond) / float64(elapsed))

				if d.kind == StatsKindByteCount {
					log(LogV, fmt.Sprintf("new-srv-%s,%d,total-srv-%s,%s,%s/s, %s", n, newsrv, n, bytesToKMG(mstats.totalsrv[n]), n, bytesMillisToKMGseconds(spsrv)))
				} else {
					log(LogV, fmt.Sprintf("new-srv-%ss,%d,total-srv-%ss,%d,%ss/ms, %.0f", n, newsrv, n, mstats.totalsrv[n], n, spsrv))
				}
			} else if d.kind == StatsKindSampleCount {
				avesample := float64(newsrv) / float64(config.numServers)
				log(LogV, fmt.Sprintf("new-srv-average-%s,%.1f", n, avesample))
			} else if d.kind == StatsKindPercentage {
				busysrv := float64(newsrv) / float64(config.numServers)
				log(LogV, fmt.Sprintf("srv-%s(%%),%.0f", n, busysrv))
			}
		}
	}
}

func inScope(d *StatsDescriptor, ij int) StatsScopeEnum {
	if (ij < config.numGateways) && ((d.scope & StatsScopeGateway) > 0) {
		return StatsScopeGateway
	}
	if (ij >= config.numGateways) && ((d.scope & StatsScopeServer) > 0) {
		return StatsScopeServer
	}
	return StatsScopeUndef
}

func (mstats *ModelStats) log(final bool) {
	loglevel := ""
	elapsed := Now.Sub(time.Time{})
	if final {
		loglevel = LogBoth
		elapsed = config.timeToRun
		if Now.Sub(mstats.lastUpdateTs) > config.timeStatsIval*9/10 {
			mstats.iter++
		}
	}
	for _, n := range mdtsortednames {
		d := mdtors.x[n]

		mstats.logTotal(loglevel, d, elapsed)

		if d.kind == StatsKindCount || d.kind == StatsKindByteCount || d.kind == StatsKindSampleCount {
			mstats.logNodeCounters(loglevel, d, elapsed)
		} else if d.kind == StatsKindPercentage {
			mstats.logNodePercentages(loglevel, d)
		}
	}
}

func (mstats *ModelStats) logNodeCounters(loglevel string, d *StatsDescriptor, elapsed time.Duration) {
	n := d.name
	var spsrv, spgwy string
	if d.kind == StatsKindCount {
		spsrv = fmt.Sprintf("    server  [#%02d-#%02d] (%s/s):", 1, config.numServers, n)
		spgwy = fmt.Sprintf("    gateway [#%02d-#%02d] (%s/s):", 1, config.numGateways, n)
	} else if d.kind == StatsKindByteCount {
		spsrv = fmt.Sprintf("    server  [#%02d-#%02d] (MB/s):", 1, config.numServers)
		spgwy = fmt.Sprintf("    gateway [#%02d-#%02d] (MB/s):", 1, config.numGateways)
	} else {
		spsrv = fmt.Sprintf("    server  [#%02d-#%02d]:", 1, config.numServers)
		spgwy = fmt.Sprintf("    gateway [#%02d-#%02d]:", 1, config.numGateways)
	}
	for ij := 0; ij < config.numGateways+config.numServers; ij++ {
		scope := inScope(d, ij)
		if scope == StatsScopeUndef {
			continue
		}
		if d.kind == StatsKindSampleCount {
			sp := float64(mstats.allNodeStats[ij][n]) / float64(mstats.iter)
			if scope == StatsScopeServer {
				spsrv += fmt.Sprintf("%.1f,", sp)
			} else if scope == StatsScopeGateway {
				spgwy += fmt.Sprintf("%.1f,", sp)
			}
		} else {
			sp := float64(mstats.allNodeStats[ij][n]) * (float64(time.Millisecond) / float64(elapsed))
			spSec := float64(mstats.allNodeStats[ij][n]) * (float64(time.Second) / float64(elapsed))
			if scope == StatsScopeServer {
				if d.kind == StatsKindByteCount {
					spsrv += fmt.Sprintf("%s,", bytesMillisToMseconds(sp))
				} else {
					spsrv += fmt.Sprintf("%.0f,", spSec)
				}
			} else if scope == StatsScopeGateway {
				if d.kind == StatsKindByteCount {
					spgwy += fmt.Sprintf("%s,", bytesMillisToMseconds(sp))
				} else {
					spgwy += fmt.Sprintf("%.0f,", spSec)
				}
			}
		}
	}

	if (d.scope & StatsScopeGateway) > 0 {
		log(loglevel, strings.TrimSuffix(spgwy, ","))
	}
	if (d.scope & StatsScopeServer) > 0 {
		log(loglevel, strings.TrimSuffix(spsrv, ","))
	}
}

func (mstats *ModelStats) logNodePercentages(loglevel string, d *StatsDescriptor) {
	n := d.name
	spsrv := fmt.Sprintf("    server  [#%02d-#%02d]:", 1, config.numServers)
	spgwy := fmt.Sprintf("    gateway [#%02d-#%02d]:", 1, config.numGateways)
	for ij := 0; ij < config.numGateways+config.numServers; ij++ {
		scope := inScope(d, ij)
		if scope == StatsScopeUndef {
			continue
		}
		if scope == StatsScopeServer {
			spsrv += fmt.Sprintf("%d,", mstats.allNodeStats[ij][n])
		} else if scope == StatsScopeGateway {
			spgwy += fmt.Sprintf("%d,", mstats.allNodeStats[ij][n])
		}
	}

	if (d.scope & StatsScopeGateway) > 0 {
		log(loglevel, strings.TrimSuffix(spgwy, ","))
	}
	if (d.scope & StatsScopeServer) > 0 {
		log(loglevel, strings.TrimSuffix(spsrv, ","))
	}
}

func (mstats *ModelStats) logTotal(loglevel string, d *StatsDescriptor, elapsed time.Duration) {
	n := d.name
	if d.kind == StatsKindCount || d.kind == StatsKindByteCount {
		if strings.HasSuffix(n, "s") {
			log(loglevel, n+":")
		} else {
			log(loglevel, n+"s:")
		}
		if ((d.scope & StatsScopeGateway) > 0) && ((d.scope & StatsScopeServer) > 0) {
			serverspeed := float64(mstats.totalsrv[n]) * (float64(time.Millisecond) / float64(elapsed))
			serverspeedSec := float64(mstats.totalsrv[n]) * (float64(time.Second) / float64(elapsed))
			gatewayspeed := float64(mstats.totalgwy[n]) * (float64(time.Millisecond) / float64(elapsed))
			gatewayspeedSec := float64(mstats.totalgwy[n]) * (float64(time.Second) / float64(elapsed))
			if d.kind == StatsKindByteCount {
				log(loglevel, fmt.Sprintf("    gateway total: %s, combined throughput: %s", bytesToKMG(mstats.totalgwy[n]), bytesMillisToKMGseconds(gatewayspeed)))
				log(loglevel, fmt.Sprintf("    server  total: %s, combined throughput: %s", bytesToKMG(mstats.totalsrv[n]), bytesMillisToKMGseconds(serverspeed)))
			} else {
				log(loglevel, fmt.Sprintf("    cluster total: (%d, %d)", mstats.totalgwy[n], mstats.totalsrv[n]))
				log(loglevel, fmt.Sprintf("    combined gateway (%s/s): %.0f", n, gatewayspeedSec))
				log(loglevel, fmt.Sprintf("    combined server  (%s/s): %.0f", n, serverspeedSec))
			}
		} else if (d.scope & StatsScopeGateway) > 0 {
			gatewayspeed := float64(mstats.totalgwy[n]) * (float64(time.Millisecond) / float64(elapsed))
			gatewayspeedSec := float64(mstats.totalgwy[n]) * (float64(time.Second) / float64(elapsed))
			if d.kind == StatsKindByteCount {
				log(loglevel, fmt.Sprintf("    gateway total: %s", bytesToKMG(mstats.totalgwy[n])))
				log(loglevel, fmt.Sprintf("    combined gateway throughput (%s/s): %s", n, bytesMillisToKMGseconds(gatewayspeed)))
			} else {
				log(loglevel, fmt.Sprintf("    gateway total: %d", mstats.totalgwy[n]))
				log(loglevel, fmt.Sprintf("    combined gateway (%s/s): %.0f", n, gatewayspeedSec))
			}
		} else if (d.scope & StatsScopeServer) > 0 {
			serverspeed := float64(mstats.totalsrv[n]) * (float64(time.Millisecond) / float64(elapsed))
			serverspeedSec := float64(mstats.totalsrv[n]) * (float64(time.Second) / float64(elapsed))
			if d.kind == StatsKindByteCount {
				log(loglevel, fmt.Sprintf("    server total: %s", bytesToKMG(mstats.totalsrv[n])))
				log(loglevel, fmt.Sprintf("    combined server throughput (%s/s): %s", n, bytesMillisToKMGseconds(serverspeed)))
			} else {
				log(loglevel, fmt.Sprintf("    server total: %d", mstats.totalsrv[n]))
				log(loglevel, fmt.Sprintf("    combined server (%s/s): %.0f", n, serverspeedSec))
			}
		}
	} else if d.kind == StatsKindSampleCount {
		log(loglevel, n+":")
		if (d.scope & StatsScopeGateway) > 0 {
			average := float64(mstats.totalgwy[n]) / float64(config.numGateways) / float64(mstats.iter)
			log(loglevel, fmt.Sprintf("    gateway average: %.1f", average))
		}
		if (d.scope & StatsScopeServer) > 0 {
			average := float64(mstats.totalsrv[n]) / float64(config.numServers) / float64(mstats.iter)
			log(loglevel, fmt.Sprintf("    server average: %.1f", average))
		}
	} else if d.kind == StatsKindPercentage {
		log(loglevel, n+"(%):")
		sumgateway, sumserver := int64(0), int64(0)
		for ij := 0; ij < config.numGateways+config.numServers; ij++ {
			scope := inScope(d, ij)
			if scope == StatsScopeUndef {
				continue
			}
			if scope == StatsScopeServer {
				sumserver += mstats.allNodeStats[ij][n]
			} else if scope == StatsScopeGateway {
				sumgateway += mstats.allNodeStats[ij][n]
			}
		}
		ag := float64(sumgateway) / float64(config.numGateways)
		as := float64(sumserver) / float64(config.numServers)
		if ((d.scope & StatsScopeGateway) > 0) && ((d.scope & StatsScopeServer) > 0) {
			log(loglevel, fmt.Sprintf("    (gateways, servers) average %s(%%): (%.1f, %.1f)", n, ag, as))
		} else if (d.scope & StatsScopeGateway) > 0 {
			log(loglevel, fmt.Sprintf("    gateway average %s(%%): %.1f", n, ag))
		} else if (d.scope & StatsScopeServer) > 0 {
			log(loglevel, fmt.Sprintf("    server average %s(%%): %.1f", n, as))
		}
	}
}
