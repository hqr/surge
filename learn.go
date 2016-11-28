// Basic Machine Learning framework for surge. The framework provides for
// APIs to train the module and then predict bandwidth given a comannd
// window size.
package surge

import (
	"errors"
)

const (
	minCmdWinsize = 1
	maxCmdWinsize = 256
)

var learners map[string]LearnerInterface

var (
	NotEnoughDataError  = errors.New("not enough training data to predict.")
	NoTrainingDataError = errors.New("you need to Fit() before you can Predict()")
	NotImplementedError = errors.New("Method not implemented")
)

type LearnerInterface interface {
	GetName() string
	Initialize()
	Train(cmdWinSize int32, tput int64)
	Predict(cmdWinSize int32) (int64, error)
	GetOptimalWinsize() (int32, error)
}

// Learner Template
type LearnerTemplate struct {
	name		string
	minTrainValues	int	// Minimum number of training values before the learner can predict
}

func (l *LearnerTemplate) GetName() string {
	return l.name
}

func (l *LearnerTemplate) Initialize() {
	l.minTrainValues = 16
}

// A simple learner which remembers the optimal window sizes
// from all previous window sizes

type SimpleLearner struct {
	LearnerTemplate
	first		int	// minimal cmdwinsize value for this learner
	last		int	// max cmdwinsize value for this learner
	tputs	map[int32]int64
	fitCount	int
	optimalWinSize	int32
	maxTput	int64
}

func (sl *SimpleLearner) Initialize() {

	sl.first = minCmdWinsize
	sl.last = maxCmdWinsize
	sl.minTrainValues = 8

	sl.tputs = make(map[int32]int64, sl.last)
	sl.fitCount = 0
	sl.optimalWinSize = 0

	sl.maxTput = 0
	for i := 0; i < sl.last; i++ {
		sl.tputs[int32(i)] = 0
	}

}

func (sl *SimpleLearner) Train(cmdWinSize int32, tput int64) {
	sl.fitCount++
	sl.tputs[cmdWinSize] = tput
	if tput >= sl.maxTput {
		sl.maxTput = tput
		sl.optimalWinSize = cmdWinSize
	} else if sl.optimalWinSize == cmdWinSize {
		// The previous high value of this cmdWinsize changed.
		// Need to find the next largest tput
		sl.maxTput = 0
		for i := 0; i < sl.last; i++ {
			if sl.tputs[int32(i)] > sl.maxTput {
				sl.maxTput = tput
				sl.optimalWinSize = cmdWinSize
			}
		}
	}
}

// This learner cannot predict. It only recollects from previous
// data.
func (sl *SimpleLearner) Predict(cmdWinSize int32) (int64, error) {
	return 0, NotImplementedError;
}

func (sl *SimpleLearner) GetOptimalWinsize() (int32, error) {
	if sl.fitCount < sl.minTrainValues {
		return 0, NotEnoughDataError
	}

	return sl.optimalWinSize, nil
}

// Public functions

func RegisterLearner(name string, learner LearnerInterface) {
	assert(learners[name] == nil)
	learners[name] = learner;
}

func GetLearner(name string) LearnerInterface {
	return learners[name]
}

var simpleLearner = SimpleLearner{}
func init() {
	learners = make(map[string]LearnerInterface, 4)
	RegisterLearner("simple-learner", &simpleLearner)
}
