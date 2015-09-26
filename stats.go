package surge

import (
	"fmt"
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
	StatsKindPercentage
)

type StatsScopeEnum int

const (
	StatsScopeUndef StatsScopeEnum = 1 << iota
	StatsScopeGateway
	StatsScopeServer
)

const MAX_STATS_DESCRIPTORS int = 10

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
}

//
// static
//
var allDtors map[ModelName]*ModelStatsDescriptors
var mdtors *ModelStatsDescriptors
var mstats ModelStats

//============================== init =============================================
//
//
//============================== reset ============================================
func NewStatsDescriptors(name ModelName) *ModelStatsDescriptors {
	if allDtors == nil || len(allDtors) == 0 {
		allDtors = make(map[ModelName]*ModelStatsDescriptors, MAX_MODELS)
	}
	dtors := make(map[string]*StatsDescriptor, MAX_STATS_DESCRIPTORS)
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

	for k, _ := range mdtors.x {
		mstats.totalgwy[k] = 0
		mstats.totalsrv[k] = 0
	}

	mstats.allNodeStats = make([]NodeStats, config.numGateways+config.numServers)

	for ij := 0; ij < config.numGateways+config.numServers; ij++ {
		mstats.allNodeStats[ij] = make(map[string]int64, len(mdtors.x))
		for n, _ := range mdtors.x {
			mstats.allNodeStats[ij][n] = 0
		}
	}
	mstats.iter = 0
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
	for n := range mdtors.x {
		d := mdtors.x[n]
		newgwy, newsrv := int64(0), int64(0)
		for ij := 0; ij < config.numGateways+config.numServers; ij++ {
			r := allNodes[ij]
			scope := inScope(d, ij)
			if scope == StatsScopeUndef {
				continue
			}
			nodestats := r.GetStats(true)
			_, ok := nodestats[n]
			if ok {
				val := nodestats[n]
				if d.kind == StatsKindCount {
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
		}

		// log one iter
		if d.kind == StatsKindCount {
			spgwy := float64(newgwy) * (float64(time.Millisecond) / float64(elapsed))
			spsrv := float64(newsrv) * (float64(time.Millisecond) / float64(elapsed))
			if d.scope&StatsScopeGateway > 0 {
				log(fmt.Sprintf("new-gwy-%ss,%d,total-gwy-%ss,%d,%ss/ms, %.0f", n, newgwy, n, mstats.totalgwy[n], n, spgwy))
			}
			if d.scope&StatsScopeServer > 0 {
				log(fmt.Sprintf("new-srv-%ss,%d,total-srv-%ss,%d,%ss/ms, %.0f", n, newsrv, n, mstats.totalsrv[n], n, spsrv))
			}
		} else if d.kind == StatsKindPercentage {
			busygwy := float64(newgwy) / float64(config.numGateways)
			busysrv := float64(newsrv) / float64(config.numServers)
			if (d.scope & StatsScopeGateway) > 0 {
				log(fmt.Sprintf("gwy-%s(%%),%.0f", n, busygwy))
			}
			if (d.scope & StatsScopeServer) > 0 {
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

func (mstats *ModelStats) log() {
	for n := range mdtors.x {
		d := mdtors.x[n]

		mstats.logTotal(d)

		if d.kind == StatsKindCount {
			mstats.logNodeCounters(d)
		} else if d.kind == StatsKindPercentage {
			mstats.logNodePercentages(d)
		}
	}
}

func (mstats *ModelStats) logNodeCounters(d *StatsDescriptor) {
	n := d.name
	spsrv, spgwy := "    per-Srv,", "    per-Gwy,"
	for ij := 0; ij < config.numGateways+config.numServers; ij++ {
		scope := inScope(d, ij)
		if scope == StatsScopeUndef {
			continue
		}
		sp := float64(mstats.allNodeStats[ij][n]) * (float64(time.Millisecond) / float64(config.timeToRun))
		if scope == StatsScopeServer {
			spsrv += fmt.Sprintf("%.0f,", sp)
		} else if scope == StatsScopeGateway {
			spgwy += fmt.Sprintf("%.0f,", sp)
		}
	}

	if (d.scope & StatsScopeGateway) > 0 {
		log(LOG_BOTH, strings.TrimSuffix(spgwy, ","))
	}
	if (d.scope & StatsScopeServer) > 0 {
		log(LOG_BOTH, strings.TrimSuffix(spsrv, ","))
	}
}

func (mstats *ModelStats) logNodePercentages(d *StatsDescriptor) {
	n := d.name
	spsrv, spgwy := "    per-Srv,", "    per-Gwy,"
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
		log(LOG_BOTH, strings.TrimSuffix(spgwy, ","))
	}
	if (d.scope & StatsScopeServer) > 0 {
		log(LOG_BOTH, strings.TrimSuffix(spsrv, ","))
	}
}

func (mstats *ModelStats) logTotal(d *StatsDescriptor) {
	n := d.name
	if d.kind == StatsKindCount {
		log(LOG_BOTH, n+"s:")
		if ((d.scope & StatsScopeGateway) > 0) && ((d.scope & StatsScopeServer) > 0) {
			log(LOG_BOTH, fmt.Sprintf("    total (Srv,Gwy): (%d, %d)", mstats.totalsrv[n], mstats.totalgwy[n]))

			serverspeed := float64(mstats.totalsrv[n]) * (float64(time.Millisecond) / float64(config.timeToRun))
			gatewayspeed := float64(mstats.totalgwy[n]) * (float64(time.Millisecond) / float64(config.timeToRun))
			log(LOG_BOTH, fmt.Sprintf("    average (Srv,Gwy) %ss/ms: (%.0f, %.0f)", n, serverspeed, gatewayspeed))
		} else if (d.scope & StatsScopeGateway) > 0 {
			log(LOG_BOTH, fmt.Sprintf("    total Gateway: %d", mstats.totalgwy[n]))

			gatewayspeed := float64(mstats.totalgwy[n]) * (float64(time.Millisecond) / float64(config.timeToRun))
			log(LOG_BOTH, fmt.Sprintf("    average Gateway %ss/ms: %.0f", n, gatewayspeed))
		} else if (d.scope & StatsScopeServer) > 0 {
			log(LOG_BOTH, fmt.Sprintf("    total Server: %d", mstats.totalsrv[n]))

			serverspeed := float64(mstats.totalsrv[n]) * (float64(time.Millisecond) / float64(config.timeToRun))
			log(LOG_BOTH, fmt.Sprintf("    average Server %ss/ms: %.0f", n, serverspeed))
		}
	} else if d.kind == StatsKindPercentage {
		log(LOG_BOTH, n+"(%):")
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
			log(LOG_BOTH, fmt.Sprintf("    average (Srv,Gwy) %s(%%): (%.1f, %.1f)", n, ag, as))
		} else if (d.scope & StatsScopeGateway) > 0 {
			log(LOG_BOTH, fmt.Sprintf("    average Gateway %s(%%): %.1f", n, ag))
		} else if (d.scope & StatsScopeServer) > 0 {
			log(LOG_BOTH, fmt.Sprintf("    average Server %s(%%): %.1f", n, as))
		}
	}
}
