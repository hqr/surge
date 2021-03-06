package surge

import (
	"fmt"
	"sort"
	"strings"
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
	StatsScopeUndef   StatsScopeEnum = 1 << iota
	StatsScopeGateway                // averaged over all gateways
	StatsScopeServer                 // averaged over all targets
	StatsScopeNode                   // individual node stats (no averaging)
)

const c_maxStatsDescriptors int = 10
const c_runnerStatUndefined int64 = -999999999

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

type RunnerStats map[string]int64 // all named stats counters for a given Runner (server | gateway | Disk ...)

type ModelStats struct {
	totalgwy       map[string]int64
	totalsrv       map[string]int64
	allRunnerStats []RunnerStats
	iter           int64
	lastUpdateTs   time.Time
}

//
// static
//
var allDtors map[ModelName]*ModelStatsDescriptors
var mdtors *ModelStatsDescriptors
var mstats ModelStats
var mdtsortednames []string
var oneIterRunnerStats []RunnerStats

//=================================================================================
// helpers, c-tor
//=================================================================================
func isGwy(ij int) bool {
	return ij < config.numGateways
}

func isSrv(ij int) bool {
	return ij >= config.numGateways
}

func NewStatsDescriptors(name ModelName) *ModelStatsDescriptors {
	if allDtors == nil || len(allDtors) == 0 {
		allDtors = make(map[ModelName]*ModelStatsDescriptors, maxModels)
	}
	dtors := make(map[string]*StatsDescriptor, c_maxStatsDescriptors)
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

//=========================================================================
// StatsDescriptor
//=========================================================================
func inScope(d *StatsDescriptor, ij int) StatsScopeEnum {
	if isGwy(ij) && ((d.scope & StatsScopeGateway) > 0) {
		return StatsScopeGateway
	}
	if isSrv(ij) && ((d.scope & StatsScopeServer) > 0) {
		return StatsScopeServer
	}
	if (d.scope & StatsScopeNode) > 0 {
		return StatsScopeNode
	}
	return StatsScopeUndef
}

//============================== main =============================================
//
// ModelStats
//
//============================== main =============================================
func (mstats *ModelStats) init(mname ModelName) {
	mdtors = allDtors[mname]
	assert(mdtors != nil)

	mstats.totalgwy = make(map[string]int64, len(mdtors.x))
	mstats.totalsrv = make(map[string]int64, len(mdtors.x))

	for k := range mdtors.x {
		mstats.totalgwy[k] = 0
		mstats.totalsrv[k] = 0
	}

	mstats.allRunnerStats = make([]RunnerStats, config.numGateways+config.numServers)
	oneIterRunnerStats = make([]RunnerStats, config.numGateways+config.numServers)

	for ij := 0; ij < config.numGateways+config.numServers; ij++ {
		mstats.allRunnerStats[ij] = make(map[string]int64, len(mdtors.x))
		for n := range mdtors.x {
			mstats.allRunnerStats[ij][n] = 0
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
}

//
// results of the last timeStatsIval iteration folded into => mstats
//
func (mstats *ModelStats) update(elapsed time.Duration) {
	mstats.iter++
	mstats.lastUpdateTs = Now
	for ij := 0; ij < config.numGateways+config.numServers; ij++ {
		r := allNodes[ij]
		oneIterRunnerStats[ij] = r.GetStats(true)
	}
	for n := range mdtors.x {
		d := mdtors.x[n]
		newgwy, newsrv := int64(0), int64(0)
		for ij := 0; ij < config.numGateways+config.numServers; ij++ {
			scope := inScope(d, ij)
			if scope == StatsScopeUndef {
				continue
			}
			nodestats := oneIterRunnerStats[ij]
			_, ok := nodestats[n]
			// node is allowed not to support a given counter
			if !ok && scope == StatsScopeNode && mstats.iter == 1 {
				mstats.allRunnerStats[ij][n] = c_runnerStatUndefined
				continue
			}
			val := nodestats[n]
			if d.kind == StatsKindCount || d.kind == StatsKindByteCount || d.kind == StatsKindSampleCount {
				mstats.allRunnerStats[ij][n] += val
				if isGwy(ij) {
					newgwy += val
					mstats.totalgwy[n] += val
				} else if isSrv(ij) {
					newsrv += val
					mstats.totalsrv[n] += val
				}
			} else if d.kind == StatsKindPercentage {
				// simply remember the more precise value
				//   (StatsKindPercentage implies averaging done *elsewhere*, e.g.
				//    the busy %% averaging continuously over cumulative time..)
				mstats.allRunnerStats[ij][n] = val
				if isGwy(ij) {
					newgwy += val
				} else if isSrv(ij) {
					newsrv += val
				}
			}
		}
		mstats.logUpdate(elapsed, n, newgwy, newsrv)
	}
}

//
// LogV sum/avg(all gateways) and sum/avg(all servers) for this current timeStatsIval iteration
//
func (mstats *ModelStats) logUpdate(elapsed time.Duration, n string, newgwy int64, newsrv int64) {
	d := mdtors.x[n]
	loglevel := LogV
	if newgwy != 0 {
		if d.kind == StatsKindCount || d.kind == StatsKindByteCount {
			spgwy := float64(newgwy) * (float64(time.Millisecond) / float64(elapsed))
			if d.kind == StatsKindByteCount {
				log(loglevel, fmt.Sprintf("new-gwy-%s,%d,total-gwy-%s,%s,%s/s, %s",
					n, newgwy, n, bytesToKMG(mstats.totalgwy[n]), n, bytesMillisToKMGseconds(spgwy)))
			} else {
				log(loglevel, fmt.Sprintf("new-gwy-%ss,%d,total-gwy-%ss,%d,%ss/ms, %.0f",
					n, newgwy, n, mstats.totalgwy[n], n, spgwy))
			}
		} else if d.kind == StatsKindSampleCount {
			avesample := float64(newgwy) / float64(config.numGateways)
			log(loglevel, fmt.Sprintf("new-gwy-average-%s,%.1f", n, avesample))
		} else if d.kind == StatsKindPercentage {
			busygwy := float64(newgwy) / float64(config.numGateways)
			log(loglevel, fmt.Sprintf("gwy-%s(%%),%.0f", n, busygwy))
		}
	}
	if newsrv != 0 {
		if d.kind == StatsKindCount || d.kind == StatsKindByteCount {
			spsrv := float64(newsrv) * (float64(time.Millisecond) / float64(elapsed))
			if d.kind == StatsKindByteCount {
				log(loglevel, fmt.Sprintf("new-srv-%s,%d,total-srv-%s,%s,%s/s, %s",
					n, newsrv, n, bytesToKMG(mstats.totalsrv[n]), n, bytesMillisToKMGseconds(spsrv)))
			} else {
				log(loglevel, fmt.Sprintf("new-srv-%ss,%d,total-srv-%ss,%d,%ss/ms, %.0f",
					n, newsrv, n, mstats.totalsrv[n], n, spsrv))
			}
		} else if d.kind == StatsKindSampleCount {
			avesample := float64(newsrv) / float64(config.numServers)
			log(loglevel, fmt.Sprintf("new-srv-average-%s,%.1f", n, avesample))
		} else if d.kind == StatsKindPercentage {
			busysrv := float64(newsrv) / float64(config.numServers)
			log(loglevel, fmt.Sprintf("srv-%s(%%),%.0f", n, busysrv))
		}
	}
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
	var spsrv, spgwy, s, g string
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
		if mstats.allRunnerStats[ij][n] == c_runnerStatUndefined {
			continue
		}
		if d.kind == StatsKindSampleCount {
			sp := float64(mstats.allRunnerStats[ij][n]) / float64(mstats.iter)
			if isSrv(ij) {
				s += fmt.Sprintf("%.1f,", sp)
			} else if isGwy(ij) {
				g += fmt.Sprintf("%.1f,", sp)
			}
		} else {
			sp := float64(mstats.allRunnerStats[ij][n]) * (float64(time.Millisecond) / float64(elapsed))
			spSec := float64(mstats.allRunnerStats[ij][n]) * (float64(time.Second) / float64(elapsed))
			if isSrv(ij) {
				if d.kind == StatsKindByteCount {
					s += fmt.Sprintf("%s,", bytesMillisToMseconds(sp))
				} else {
					s += fmt.Sprintf("%.0f,", spSec)
				}
			} else if isGwy(ij) {
				if d.kind == StatsKindByteCount {
					g += fmt.Sprintf("%s,", bytesMillisToMseconds(sp))
				} else {
					g += fmt.Sprintf("%.0f,", spSec)
				}
			}
		}
	}
	if len(g) > 0 {
		spgwy += g
		log(loglevel, strings.TrimSuffix(spgwy, ","))
	}
	if len(s) > 0 {
		spsrv += s
		log(loglevel, strings.TrimSuffix(spsrv, ","))
	}
}

func (mstats *ModelStats) logNodePercentages(loglevel string, d *StatsDescriptor) {
	n := d.name
	var spsrv, spgwy, s, g string
	spsrv = fmt.Sprintf("    server  [#%02d-#%02d]:", 1, config.numServers)
	spgwy = fmt.Sprintf("    gateway [#%02d-#%02d]:", 1, config.numGateways)
	for ij := 0; ij < config.numGateways+config.numServers; ij++ {
		scope := inScope(d, ij)
		if scope == StatsScopeUndef {
			continue
		}
		if mstats.allRunnerStats[ij][n] == c_runnerStatUndefined {
			continue
		}
		if isSrv(ij) {
			s += fmt.Sprintf("%d,", mstats.allRunnerStats[ij][n])
		} else if isGwy(ij) {
			g += fmt.Sprintf("%d,", mstats.allRunnerStats[ij][n])
		}
	}
	if len(g) > 0 {
		spgwy += g
		log(loglevel, strings.TrimSuffix(spgwy, ","))
	}
	if len(s) > 0 {
		spsrv += s
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
		if mstats.totalsrv[n] > 0 && mstats.totalgwy[n] > 0 {
			serverspeed := float64(mstats.totalsrv[n]) * (float64(time.Millisecond) / float64(elapsed))
			serverspeedSec := float64(mstats.totalsrv[n]) * (float64(time.Second) / float64(elapsed))
			gatewayspeed := float64(mstats.totalgwy[n]) * (float64(time.Millisecond) / float64(elapsed))
			gatewayspeedSec := float64(mstats.totalgwy[n]) * (float64(time.Second) / float64(elapsed))
			if d.kind == StatsKindByteCount {
				log(loglevel, fmt.Sprintf("    gateway total: %s, combined throughput: %s",
					bytesToKMG(mstats.totalgwy[n]), bytesMillisToKMGseconds(gatewayspeed)))
				log(loglevel, fmt.Sprintf("    server  total: %s, combined throughput: %s",
					bytesToKMG(mstats.totalsrv[n]), bytesMillisToKMGseconds(serverspeed)))
			} else {
				log(loglevel, fmt.Sprintf("    cluster total: (%d, %d)", mstats.totalgwy[n], mstats.totalsrv[n]))
				log(loglevel, fmt.Sprintf("    combined gateway (%s/s): %.0f", n, gatewayspeedSec))
				log(loglevel, fmt.Sprintf("    combined server  (%s/s): %.0f", n, serverspeedSec))
			}
		} else if mstats.totalgwy[n] > 0 {
			gatewayspeed := float64(mstats.totalgwy[n]) * (float64(time.Millisecond) / float64(elapsed))
			gatewayspeedSec := float64(mstats.totalgwy[n]) * (float64(time.Second) / float64(elapsed))
			if d.kind == StatsKindByteCount {
				log(loglevel, fmt.Sprintf("    gateway total: %s", bytesToKMG(mstats.totalgwy[n])))
				log(loglevel, fmt.Sprintf("    combined gateway throughput (%s/s): %s", n, bytesMillisToKMGseconds(gatewayspeed)))
			} else {
				log(loglevel, fmt.Sprintf("    gateway total: %d", mstats.totalgwy[n]))
				log(loglevel, fmt.Sprintf("    combined gateway (%s/s): %.0f", n, gatewayspeedSec))
			}
		} else if mstats.totalsrv[n] > 0 {
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
		if mstats.totalgwy[n] > 0 {
			average := float64(mstats.totalgwy[n]) / float64(config.numGateways) / float64(mstats.iter)
			log(loglevel, fmt.Sprintf("    gateway average: %.1f", average))
		}
		if mstats.totalsrv[n] > 0 {
			average := float64(mstats.totalsrv[n]) / float64(config.numServers) / float64(mstats.iter)
			log(loglevel, fmt.Sprintf("    server average: %.1f", average))
		}
	} else if d.kind == StatsKindPercentage {
		log(loglevel, n+"(%):")
		sumgateway, sumserver := int64(0), int64(0)
		for ij := 0; ij < config.numGateways+config.numServers; ij++ {
			if mstats.allRunnerStats[ij][n] == c_runnerStatUndefined {
				if isGwy(ij) {
					sumgateway = c_runnerStatUndefined
				} else if isSrv(ij) {
					sumserver = c_runnerStatUndefined
				}
				continue
			}
			if isSrv(ij) && sumserver != c_runnerStatUndefined {
				sumserver += mstats.allRunnerStats[ij][n]
			} else if isGwy(ij) && sumgateway != c_runnerStatUndefined {
				sumgateway += mstats.allRunnerStats[ij][n]
			}
		}
		ag := float64(sumgateway) / float64(config.numGateways)
		as := float64(sumserver) / float64(config.numServers)
		if sumgateway != c_runnerStatUndefined && sumserver != c_runnerStatUndefined {
			log(loglevel, fmt.Sprintf("    (gateways, servers) average %s(%%): (%.1f, %.1f)", n, ag, as))
		} else if sumgateway != c_runnerStatUndefined {
			log(loglevel, fmt.Sprintf("    gateway average %s(%%): %.1f", n, ag))
		} else if sumserver != c_runnerStatUndefined {
			log(loglevel, fmt.Sprintf("    server average %s(%%): %.1f", n, as))
		}
	}
}
