package surge

// This file implements the centralized initialization framework for surge.
// The core interface is Initializer. Each file or module that needs to have some
// initialization logic before calling main should create a structure which includes
// BaseInitializer structure. The structure should redefine the Initialize() method
// which should call the function that performs initialization. Each initialization
// can have a dependency list of Initializers. The dependency should be met before
// the initializer is called.
// One example: config should be called before model. So model initialization
// will have a dependency set for config.
//
// With this new framework, no files other than this file will have the init method.
// All files or modules that need initialization will define their own initializer and
// set the dependency correctly and then add it to the global initializers list using
// the AddInitializer() function. This should be called as a variable initialization in
// each file so that they get called befor the init() function here.

import (
	"fmt"
	)


type InitializerState int

const (
	InitializerNotProcessed InitializerState = iota
	InitializerBlocked
	InitializerDeadlocked
	InitializerProcessed
	InitializerFailed
)

type Initializer interface  {
	Initialize() InitializerState
	GetName() string
	GetState() InitializerState
	SetState(InitializerState)
	BlockedOn(dependency Initializer) bool
	SetDependency(initializer Initializer)
	GetDependencies() []Initializer
}

var initializers = make(map[string]Initializer)

func AddInitializer(initializer Initializer) bool {
	name := initializer.GetName()
	assert(initializers[name] == nil)
	initializers[name] = initializer
	fmt.Printf("Added Initializer: %s\n", name)

	return true
}

func initialize(initializer Initializer) InitializerState{
	state := initializer.Initialize()
	initializer.SetState(state)

	return state
}

func init() {
	fmt.Println("init called from init.go")
	count := len(initializers)
	for _, initializer := range initializers {
		if initializer.GetState() != InitializerNotProcessed {
			continue
		}
		for _, dep := range initializer.GetDependencies() {

			// Check if we will deadlock
			if dep.BlockedOn(initializer) {
				initializer.SetState(InitializerDeadlocked)
				// panic here
				return 
			}

			status := initialize(dep)
			if status == InitializerBlocked {
				initializer.SetState(InitializerBlocked)
			}
		}
		if initializer.GetState() != InitializerBlocked {
			initialize(initializer)
			count--
		}
	}
	assert(count == 0)
}

type BaseInitializer struct {
	name	string
	state	InitializerState
	deps	map[string]Initializer
}

func (bi *BaseInitializer) Initialize() InitializerState {
	assert(false)
	return InitializerNotProcessed
}

func (bi *BaseInitializer) GetName() string {
	return bi.name
}

func (bi *BaseInitializer) GetState() InitializerState {
	return bi.state
}

func (bi *BaseInitializer) SetState(state InitializerState) {
	bi.state = state
}

func (bi *BaseInitializer) SetDependency(initializer Initializer) {
	bi.deps[initializer.GetName()] = initializer
}

func (bi *BaseInitializer) GetDependencies() []Initializer {
	var initializers []Initializer

	for _, initializer := range bi.deps {
		initializers = append(initializers, initializer)
	}

	return initializers
}

func (bi *BaseInitializer) BlockedOn(initializer Initializer) bool {
	return (bi.deps[initializer.GetName()] != nil)
}
