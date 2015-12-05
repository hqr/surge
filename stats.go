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
}

//============================== stats =============================================
//
// ModelStats
//
//============================== stats =============================================
//
// results of the last timeStatsIval iteration folded into => mstats
//
func (mstats *ModelStats) update(elapsed time.Duration) {
	mstats.iter++
	mstats.lastUpdateTs = Now
	for ij := 0; ij < config.numGateways+config.numServers; ij++ {
		r := allNodes[ij]
		oneIterNodeStats[ij] = r.GetStats(true)
	}

	for n := range mdtors.x {
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
				}
			} else if d.kind == StatsKindPercentage {
				// adjust the average
				mstats.allNodeStats[ij][n] = (mstats.allNodeStats[ij][n]*(mstats.iter-1) + val + 1) / mstats.iter
				// running average optional & later..
				// mstats.allNodeStats[ij][n] = (mstats.allNodeStats[ij][n]*60 + val*40) / 100
				if scope == StatsScopeGateway {
					newgwy += val
				} else if scope == StatsScopeServer {
					newsrv += val
				}
			}
		}

		// log one iteration
		if d.scope&StatsScopeGateway > 0 && newgwy != 0 {
			if d.kind == StatsKindCount || d.kind == StatsKindByteCount {
				spgwy := float64(newgwy) * (float64(time.Millisecond) / float64(elapsed))

				if d.kind == StatsKindByteCount {
					log(fmt.Sprintf("new-gwy-%s,%d,total-gwy-%s,%s,%s/s, %s", n, newgwy, n, bytesToKMG(mstats.totalgwy[n]), n, bytesMillisToKMGseconds(spgwy)))
				} else {
					log(fmt.Sprintf("new-gwy-%ss,%d,total-gwy-%ss,%d,%ss/ms, %.0f", n, newgwy, n, mstats.totalgwy[n], n, spgwy))
				}
			} else if d.kind == StatsKindSampleCount {
				avesample := float64(newgwy) / float64(config.numGateways)
				log(fmt.Sprintf("new-gwy-average-%s,%.1f", n, avesample))
			} else if d.kind == StatsKindPercentage {
				busygwy := float64(newgwy) / float64(config.numGateways)
				log(fmt.Sprintf("gwy-%s(%%),%.0f", n, busygwy))
			}
		}

		if d.scope&StatsScopeServer > 0 && newsrv != 0 {
			if d.kind == StatsKindCount || d.kind == StatsKindByteCount {
				spsrv := float64(newsrv) * (float64(time.Millisecond) / float64(elapsed))

				if d.kind == StatsKindByteCount {
					log(fmt.Sprintf("new-srv-%s,%d,total-srv-%s,%s,%s/s, %s", n, newsrv, n, bytesToKMG(mstats.totalsrv[n]), n, bytesMillisToKMGseconds(spsrv)))
				} else {
					log(fmt.Sprintf("new-srv-%ss,%d,total-srv-%ss,%d,%ss/ms, %.0f", n, newsrv, n, mstats.totalsrv[n], n, spsrv))
				}
			} else if d.kind == StatsKindSampleCount {
				avesample := float64(newsrv) / float64(config.numServers)
				log(fmt.Sprintf("new-srv-average-%s,%.1f", n, avesample))
			} else if d.kind == StatsKindPercentage {
				busysrv := float64(newsrv) / float64(config.numServers)
				log(fmt.Sprintf("srv-%s(%%),%.0f", n, busysrv))
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
	spsrv, spgwy := "    server,", "    gateway,"
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
			if scope == StatsScopeServer {
				if d.kind == StatsKindByteCount {
					spsrv += fmt.Sprintf("%s,", bytesMillisToKMGseconds(sp))
				} else {
					spsrv += fmt.Sprintf("%.1f,", sp)
				}
			} else if scope == StatsScopeGateway {
				if d.kind == StatsKindByteCount {
					spgwy += fmt.Sprintf("%s,", bytesMillisToKMGseconds(sp))
				} else {
					spgwy += fmt.Sprintf("%.1f,", sp)
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
	spsrv, spgwy := "    server,", "    gateway,"
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
			gatewayspeed := float64(mstats.totalgwy[n]) * (float64(time.Millisecond) / float64(elapsed))
			if d.kind == StatsKindByteCount {
				log(loglevel, fmt.Sprintf("    total (gateways): %s, throughput: %s", bytesToKMG(mstats.totalgwy[n]), bytesMillisToKMGseconds(gatewayspeed)))
				log(loglevel, fmt.Sprintf("    total (servers): %s, throughput: %s", bytesToKMG(mstats.totalsrv[n]), bytesMillisToKMGseconds(serverspeed)))
			} else {
				log(loglevel, fmt.Sprintf("    total (gateways, servers): (%d, %d)", mstats.totalgwy[n], mstats.totalsrv[n]))
				log(loglevel, fmt.Sprintf("    gateway %ss/ms: %.0f", n, gatewayspeed))
				log(loglevel, fmt.Sprintf("    server %ss/ms: %.0f", n, serverspeed))
			}
		} else if (d.scope & StatsScopeGateway) > 0 {
			gatewayspeed := float64(mstats.totalgwy[n]) * (float64(time.Millisecond) / float64(elapsed))
			if d.kind == StatsKindByteCount {
				log(loglevel, fmt.Sprintf("    total: %s", bytesToKMG(mstats.totalgwy[n])))
				log(loglevel, fmt.Sprintf("    throughput %s/s: %s", n, bytesMillisToKMGseconds(gatewayspeed)))
			} else {
				log(loglevel, fmt.Sprintf("    total: %d", mstats.totalgwy[n]))
				log(loglevel, fmt.Sprintf("    gateway %ss/ms: %.0f", n, gatewayspeed))
			}
		} else if (d.scope & StatsScopeServer) > 0 {
			serverspeed := float64(mstats.totalsrv[n]) * (float64(time.Millisecond) / float64(elapsed))
			if d.kind == StatsKindByteCount {
				log(loglevel, fmt.Sprintf("    total: %s", bytesToKMG(mstats.totalsrv[n])))
				log(loglevel, fmt.Sprintf("    throughput %s/s: %s", n, bytesMillisToKMGseconds(serverspeed)))
			} else {
				log(loglevel, fmt.Sprintf("    total: %d", mstats.totalsrv[n]))
				log(loglevel, fmt.Sprintf("    server %ss/ms: %.0f", n, serverspeed))
			}
		}
	} else if d.kind == StatsKindSampleCount {
		log(loglevel, n+":")
		if (d.scope & StatsScopeGateway) > 0 {
			average := float64(mstats.totalgwy[n]) / float64(config.numGateways) / float64(mstats.iter)
			log(loglevel, fmt.Sprintf("    gateways average: %.1f", average))
		}
		if (d.scope & StatsScopeServer) > 0 {
			average := float64(mstats.totalsrv[n]) / float64(config.numServers) / float64(mstats.iter)
			log(loglevel, fmt.Sprintf("    servers average: %.1f", average))
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
			log(loglevel, fmt.Sprintf("    average (gateways, servers) %s(%%): (%.1f, %.1f)", n, ag, as))
		} else if (d.scope & StatsScopeGateway) > 0 {
			log(loglevel, fmt.Sprintf("    average %s(%%): %.1f", n, ag))
		} else if (d.scope & StatsScopeServer) > 0 {
			log(loglevel, fmt.Sprintf("    average %s(%%): %.1f", n, as))
		}
	}
}
