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
const maxModels int = 16
const GWY = "I"
const SRV = "T"

// each model self-registers at startup
type ModelName string

// registration cb
func RegisterModel(name ModelName, model ModelInterface, props ...map[string]interface{}) {
	if !inited {
		__init()
	}
	assert(allModels[name] == nil, "already registered")
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
var inited = false

var allModels map[ModelName]ModelInterface // all registered models
var allModelProps map[ModelName]map[string]interface{}
var allNamesSorted []string

var Now = time.Time{}
var TimeNil = time.Time{}.Add(time.Hour * 10000)

var allGateways []NodeRunnerInterface
var allServers []NodeRunnerInterface
var allNodes []NodeRunnerInterface // a union of the previous two, for common ops

var eventsPastDeadline = 0

func init() {
	if !inited {
		__init()
	}
}

func __init() {
	allModels = make(map[ModelName]ModelInterface, maxModels)
	allModelProps = make(map[ModelName]map[string]interface{}, maxModels)

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
	NewGateway(id int) NodeRunnerInterface
	NewServer(id int) NodeRunnerInterface
	PreConfig()	// model can optionally define model specifc config
	PostConfig()	// model can validate if the config is confroming to model requirements
	PreBuild()	// model can perform custom build steps before the generic build
	PostBuild()	// model can perform custom build steps after the generic build
}

// Model Template
type ModelGeneric struct {
}

// abstract method
func (mg *ModelGeneric) NewGateway(id int) NodeRunnerInterface {
	assert(false)
	return nil
}

// abstract method
func (mg *ModelGeneric) NewServer(id int) NodeRunnerInterface {
	assert(false)
	return nil
}

func (mg *ModelGeneric) PreConfig() {}
func (mg *ModelGeneric) PostConfig() {}
func (mg *ModelGeneric) PreBuild() {}
func (mg *ModelGeneric) PostBuild() {}

//============================================================================
// common functions and main loop
//============================================================================

func NowIsDone() bool {
	runtime.Gosched()
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

	allGateways = make([]NodeRunnerInterface, config.numGateways)
	allServers = make([]NodeRunnerInterface, config.numServers)
	allNodes = make([]NodeRunnerInterface, config.numGateways+config.numServers)

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

	// Configuration Handling
	PreConfig()
	for _, mname := range allNamesSorted {
		model, _ := allModels[ModelName(mname)]
		model.PreConfig()
	}

	ParseCommandLine()

	PostConfig()
	cnt := 0
	onename := ""
	for _, mname := range allNamesSorted {
		if !strings.HasPrefix(mname, config.mprefix) {
			continue
		}
		cnt++
		onename = mname
	}

	if cnt == 1 {
		nameLog(onename)
	}

	initLog()
	defer terminateLog()
	stdout := bufio.NewWriter(os.Stdout)
	defer stdout.Flush()
	hasprefix := 0

	sort.Strings(allNamesSorted)

	// shallow copy global config, restore prior to each model-run
	configCopy := config
	configNetworkCopy := configNetwork
	configStorageCopy := configStorage

	for _, sname := range allNamesSorted {
		if !strings.HasPrefix(sname, config.mprefix) {
			continue
		}
		if hasprefix > 0 {
			log(LogBoth, "====")
		}
		hasprefix++

		runtime.GC()

		name := ModelName(sname)
		props := allModelProps[name]
		namedesc := "@" + string(name)
		if desc, ok := props["description"]; ok {
			namedesc += " [ " + desc.(string) + " ]"
		}
		log(LogBoth, "Model "+namedesc)

		if maxprocs, ok := props["GOMAXPROCS"]; ok {
			runtime.GOMAXPROCS(maxprocs.(int))
		} else {
			runtime.GOMAXPROCS(runtime.NumCPU()) // the default
		}

		eventsPastDeadline = 0
		config = configCopy
		configNetwork = configNetworkCopy
		configStorage = configStorageCopy

		model, _ := allModels[name]
		model.PostConfig() // Model specific configuration

		model.PreBuild() // Model specific build steps before generic build
		buildModel(model, name)
		model.PostBuild() // Model specific build steps after generic build

		//
		// log configuration
		//
		timestampLog(false)
		configLog(true)
		configLog(false)
		timestampLog(true)

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

		//
		// ONE MODEL MAIN LOOP
		//
		oneModelTimeLoop(model, stdout)

		fmt.Printf("\r")
		// benchmark results
		timestampLog(false)
		configLog(false)
		mstats.log(true)
		timestampLog(true)
	}
	if hasprefix == 0 {
		fmt.Printf("No registered models matched prefix '%s': nothing to do\n", config.mprefix)
	}
}

func oneModelTimeLoop(model ModelInterface, stdout *bufio.Writer) {
	nextStatsTime := Now.Add(config.timeStatsIval)
	nextTrackTime := Now.Add(config.timeTrackIval)
	endtime := Now.Add(config.timeToRun)
	realtime := time.Now()
	pct := 0

	timestampLog(false)
	log(realtime.Format(time.RFC822))
	timestampLog(true)
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

				rt := time.Now()
				if rt.Sub(realtime) > config.realtimeLogStats {
					mstats.log(false)
					realtime = rt
					log(realtime.Format(time.RFC822) + " ====")
					flushLog()
				}
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
	fmt.Printf("\r                ")
	stdout.Flush()
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
