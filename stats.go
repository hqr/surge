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
	for n := range mdtors.x {
		i, j := 0, 0
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
				mstats.allNodeStats[ij][n] += val

				if scope == StatsScopeGateway {
					newgwy += val
					mstats.totalgwy[n] += val
					i++
				} else if scope == StatsScopeServer {
					newsrv += val
					mstats.totalsrv[n] += val
					j++
				}

			}
		}
		// log one iter
		spgwy := float64(newgwy) * (float64(time.Millisecond) / float64(elapsed))
		spsrv := float64(newsrv) * (float64(time.Millisecond) / float64(elapsed))
		if i > 0 {
			log(fmt.Sprintf("new-gwy-%ss,%d,total-gwy-%ss,%d,%ss/ms, %.0f", n, newgwy, n, mstats.totalgwy[n], n, spgwy))
		}
		if j > 0 {
			log(fmt.Sprintf("new-srv-%ss,%d,total-srv-%ss,%d,%ss/ms, %.0f", n, newsrv, n, mstats.totalsrv[n], n, spsrv))
		}
	}
}

func inScope(d *StatsDescriptor, ij int) StatsScopeEnum {
	assert(d.kind == StatsKindCount, "not implemented yet")

	if ij < config.numGateways && (d.scope&StatsScopeGateway > 0) {
		return StatsScopeGateway
	}
	if ij >= config.numGateways && (d.scope&StatsScopeServer > 0) {
		return StatsScopeServer
	}
	return StatsScopeUndef
}

func (mstats *ModelStats) log() {
	for n := range mdtors.x {
		log(LOG_BOTH, n+"s:")
		d := mdtors.x[n]
		log(LOG_BOTH, fmt.Sprintf("    total (Srv,Gwy): (%d, %d)", mstats.totalsrv[n], mstats.totalgwy[n]))

		serverspeed := float64(mstats.totalsrv[n]) * (float64(time.Millisecond) / float64(config.timeToRun))
		gatewayspeed := float64(mstats.totalgwy[n]) * (float64(time.Millisecond) / float64(config.timeToRun))
		log(LOG_BOTH, fmt.Sprintf("    average (Srv,Gwy) %ss/ms: (%.0f, %.0f)", n, serverspeed, gatewayspeed))

		spsrv, spgwy := "    per-Srv,", "    per-Gwy,"
		loggwy, logsrv := false, false
		for ij := 0; ij < config.numGateways+config.numServers; ij++ {
			scope := inScope(d, ij)
			if scope == StatsScopeUndef {
				continue
			}
			sp := float64(mstats.allNodeStats[ij][n]) * (float64(time.Millisecond) / float64(config.timeToRun))
			if scope == StatsScopeServer {
				spsrv += fmt.Sprintf("%.0f,", sp)
				if mstats.allNodeStats[ij][n] > 0 {
					logsrv = true
				}
			} else if scope == StatsScopeGateway {
				spgwy += fmt.Sprintf("%.0f,", sp)
				if mstats.allNodeStats[ij][n] > 0 {
					loggwy = true
				}
			}
		}

		if logsrv {
			log(LOG_BOTH, strings.TrimSuffix(spsrv, ","))
		}
		if loggwy {
			log(LOG_BOTH, strings.TrimSuffix(spgwy, ","))
		}
	}
}
