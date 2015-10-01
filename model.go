package surge

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"
)

// constants
const MAX_MODELS int = 16

// each model self-registers at startup
type ModelName string

// registration cb
func RegisterModel(name ModelName, model ModelInterface, props ...map[string]interface{}) {
	if !inited {
		__init()
	}
	allModels[name] = model
	allNamesSorted = append(allNamesSorted, string(name))

	if len(props) > 0 {
		allModelProps[name] = props[0]
		// FIXME: either warn len > 1 or merge maps..
	} else {
		allModelProps[name] = make(map[string]interface{}, 0)
	}
}

//
// globals & init
//
var inited bool = false

var allModels map[ModelName]ModelInterface // all registered models
var allModelProps map[ModelName]map[string]interface{}
var allNamesSorted []string

var Now = time.Time{}

var allGateways []RunnerInterface
var allServers []RunnerInterface
var allNodes []RunnerInterface // a union of the previous two, for common ops

func init() {
	if !inited {
		__init()
	}
}

func __init() {
	allModels = make(map[ModelName]ModelInterface, MAX_MODELS)
	allModelProps = make(map[ModelName]map[string]interface{}, MAX_MODELS)

	allGateways = make([]RunnerInterface, config.numGateways)
	allServers = make([]RunnerInterface, config.numServers)
	allNodes = make([]RunnerInterface, config.numGateways+config.numServers)

	if config.srand == 0 {
		rand.Seed(time.Now().UTC().UnixNano())
	} else {
		rand.Seed(int64(config.srand))
	}
	inited = true
}

//
// all models implement the following interfaces
//
type ModelInterface interface {
	NewGateway(id int) RunnerInterface
	NewServer(id int) RunnerInterface
	NewDisk(id int) RunnerInterface
	Configure() // model can optionally change global config and/or prepare to run
}

//============================================================================
// common functions and main loop
//============================================================================

func NowIsDone() bool {
	for ij := 0; ij < config.numGateways+config.numServers; ij++ {
		r := allNodes[ij]
		if !r.NowIsDone() {
			return false
		}
	}
	return true
}

//
// construct and container-ize all model's runners
//
func buildModel(model ModelInterface, name ModelName) {
	ij := 0
	for i := 0; i < config.numGateways; i++ {
		runnerid := i + 1
		allGateways[i] = model.NewGateway(runnerid)
		assert(allGateways[i] != nil)

		allNodes[ij] = allGateways[i]
		ij++
	}
	for j := 0; j < config.numServers; j++ {
		runnerid := j + 1
		allServers[j] = model.NewServer(runnerid)
		assert(allServers[j] != nil)

		allNodes[ij] = allServers[j]
		ij++
	}

	for i := 0; i < config.numGateways; i++ {
		gw := allGateways[i]
		for j := 0; j < config.numServers; j++ {
			sr := allServers[j]

			txch := make(chan EventInterface, config.channelBuffer)
			rxch := make(chan EventInterface, config.channelBuffer)
			gw.setChannels(sr, txch, rxch)
			sr.setChannels(gw, rxch, txch)
		}
	}

	// init stats counters for the named model
	mstats.init(name)
}

//
// graceful termination
//
func prepareToStopModel(model ModelInterface) {
	for ij := 0; ij < config.numGateways+config.numServers; ij++ {
		allNodes[ij].PrepareToStop()
	}
}

//
// MAIN LOOP
//
func RunAllModels() {
	initLog()
	defer terminateLog()
	stdout := bufio.NewWriter(os.Stdout)
	defer stdout.Flush()
	hasprefix := 0

	sort.Strings(allNamesSorted)

	// shallow copy global config, restore prior to each model-run
	configCopy := config

	for _, sname := range allNamesSorted {
		if !strings.HasPrefix(sname, config.mprefix) {
			continue
		}
		if hasprefix > 0 {
			log(LOG_BOTH, "====")
		}
		hasprefix++

		runtime.GC()

		name := ModelName(sname)
		props := allModelProps[name]
		namedesc := "@" + string(name)
		if desc, ok := props["description"]; ok {
			namedesc += " [ " + desc.(string) + " ]"
		}
		log(LOG_BOTH, "Model "+namedesc)

		if maxprocs, ok := props["GOMAXPROCS"]; ok {
			runtime.GOMAXPROCS(maxprocs.(int))
		} else {
			runtime.GOMAXPROCS(runtime.NumCPU())
		}
		model, _ := allModels[name]
		buildModel(model, name)

		eventsPastDeadline = 0
		config = configCopy
		model.Configure()

		//
		// run it servers first (as they typically do not start generating load)
		//
		Now = time.Time{}
		for j := 0; j < config.numServers; j++ {
			srv := allServers[j]
			srv.Run()
		}
		time.Sleep(time.Microsecond)
		Now = time.Time{}

		// now the gateways..
		for i := 0; i < config.numGateways; i++ {
			gwy := allGateways[i]
			gwy.Run()
		}

		log(LOG_V, "Model @"+string(name)+" running now...")

		//
		// ONE MODEL MAIN LOOP
		//
		oneModelTimeLoop(model, stdout)

		fmt.Printf("\r")
		// benchmark results
		mstats.log()
	}
	if hasprefix == 0 {
		fmt.Printf("No registered models matched prefix '%s': nothing to do\n", config.mprefix)
	}
}

func oneModelTimeLoop(model ModelInterface, stdout *bufio.Writer) {
	nextStatsTime := Now.Add(config.timeStatsIval)
	nextTrackTime := Now.Add(config.timeTrackIval)
	endtime := Now.Add(config.timeToRun)
	pct := 0

	// advance the model's TIME and report stats periodically
	for {
		if Now.Equal(nextTrackTime) || Now.After(nextTrackTime) {
			pct += 10
			fmt.Printf("\r====  %2d%% done", pct)
			stdout.Flush()
			nextTrackTime = nextTrackTime.Add(config.timeTrackIval)
		}
		if NowIsDone() {
			if Now.Equal(nextStatsTime) || Now.After(nextStatsTime) {
				// new stats iteration
				mstats.update(config.timeStatsIval)
				nextStatsTime = Now.Add(config.timeStatsIval)
			}
			Now = Now.Add(config.timeIncStep)
		} else {
			time.Sleep(config.timeIncStep)
		}

		// past time-to-run gracefully terminate all model's runners
		if Now.Equal(endtime) || Now.After(endtime) {
			prepareToStopModel(model)
			break
		}
		// Or, the model itself may have decided to stop running
		if finishedRunning() {
			break
		}
	}
	if Now.Before(endtime) && endtime.Sub(Now) < config.timeClusterTrip {
		Now = endtime
	}
}

func finishedRunning() bool {
	for ij := 0; ij < config.numGateways+config.numServers; ij++ {
		r := allNodes[ij]
		if r.GetState() <= RstateRunning {
			return false
		}
	}

	return true
}
