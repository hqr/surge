// Implementation of a linear regression based learner
package surge

import (
	"fmt"
	"github.com/sjwhitworth/golearn/base"
	"github.com/sjwhitworth/golearn/linear_models"
	)

const (
	maxRows	= 256	// Max Training Datasets (command window sizes)
	)

type LRLearner struct {
	LearnerTemplate
	inputSz		int		// count of distinct Training data sets.
	data		[]int64		// Training data set
	specs		[]base.AttributeSpec
	attrs		[]base.Attribute
	instances	*base.DenseInstances
	predictions	*base.DenseInstances
}

func (ll *LRLearner) Initialize() {
	ll.minTrainValues = 32

	// Initialize the DenseInstance
	// Two attributes:
	//	- CommandWindowSz: Independent variable
	//	- Throughput: Dependent variable (ClassAttribute)
	ll.attrs = make([]base.Attribute, 2)
	ll.attrs[0] = base.NewFloatAttribute("CommandWindowSz")
	ll.attrs[1] = base.NewFloatAttribute("Throughput")
	ll.specs = make([]base.AttributeSpec, len(ll.attrs))
	ll.instances	= base.NewDenseInstances()
	for i, a := range ll.attrs {
		spec := ll.instances.AddAttribute(a)
		ll.specs[i] = spec
	}
	ll.instances.Extend(maxRows)
	ll.instances.AddClassAttribute(ll.attrs[1])

	// Make a copy in ll.predictions
	ll.predictions = base.NewDenseCopy(ll.instances)
	attrs := ll.predictions.AllAttributes()
	ll.predictions.AddClassAttribute(attrs[1])

	// Initialize the data set to -1 to distinguish updated training data with
	// uninitialized data.
	ll.data = make([]int64, maxRows)
	for i := 0; i < maxRows; i++ {
		ll.data[i] = -1
	}
}

// Train() API just stores the data in the data array to be processed by the
// regression api later in Predict() API
func (ll *LRLearner) Train(cmdWinSize int32, tput int64) {
	if ll.data[cmdWinSize] < 0 {
		ll.inputSz++
	}
	ll.data[cmdWinSize] = tput
}


// This internal method populates the DenseInstances(golearn data structure) from
// the training dataset. There are two DenseInstances:
//	- ll.instances : Training dataset for the Regression module. Attributes initialized
//	                 only for those rows which has training data
//	- ll.predictions : The output prediction dataset. Attribute is initialized for all
//	                   rows, so that we get predictions for all rows.
//
func (ll *LRLearner) populateTrainingData() {
	instance_specs := base.ResolveAllAttributes(ll.instances)
	prediction_specs := base.ResolveAllAttributes(ll.predictions)
	row := 0
	for i := 1; i < maxRows; i++ {
		tput := ll.data[i]
		ll.predictions.Set(prediction_specs[0], row, base.PackFloatToBytes(float64(i)))
		if tput == -1 { continue }

		ll.predictions.Set(prediction_specs[1], row, base.PackFloatToBytes(float64(tput)))

		ll.instances.Set(instance_specs[0], row, base.PackFloatToBytes(float64(i)))
		ll.instances.Set(instance_specs[1], row, base.PackFloatToBytes(float64(tput)))
		row++
	}
}

// Internal method which performs the linear regression.
// Returns: DenseInstance predictions. This DenseInstance has only one attribute and that is
//	    the ClassAtribute - Throughput.
//
func (ll *LRLearner) predict() (base.FixedDataGrid, error) {
	if ll.inputSz < ll.minTrainValues {
		return nil, NotEnoughDataError
	}
	ll.populateTrainingData()

	lr := linear_models.NewLinearRegression()
	err := lr.Fit(ll.instances)
	if err != nil {
		log(LogVVV, fmt.Sprintf("LinearRegression.Fit() failed: %v", err))
		return nil, err
	}

	predictions, err := lr.Predict(ll.predictions)
	if err != nil {
		log(LogVVV, fmt.Sprintf("LinearRegression.Predict() failed: %v", err))
		return nil, err
	}

	return predictions, nil
}

func (ll *LRLearner) Predict(cmdWinSize int32) (int64, error) {
	predictions, err := ll.predict()

	if err != nil { return 0, err }

	specs := base.ResolveAllAttributes(predictions)
	tput := int64(base.UnpackBytesToFloat(predictions.Get(specs[1], int(cmdWinSize))))

	return tput, nil
}

func (ll *LRLearner) GetOptimalWinsize() (int32, error) {
	if ll.inputSz < ll.minTrainValues {
		return 0, NotEnoughDataError
	}

	predictions, err := ll.predict()

	if err != nil { return 0, err }

	_, rows := predictions.Size()

	specs := base.ResolveAllAttributes(predictions)
	max_tput := int64(0)
	opt_winsz := int32(0)
	for i := 1; i < rows; i++ {
		tput := int64(base.UnpackBytesToFloat(predictions.Get(specs[0], i)))
		if max_tput < tput {
			max_tput = tput
			opt_winsz = int32(i)
		}
	}

	if opt_winsz == 0 || max_tput == 0 {
		return 0, NoTrainingDataError
	}

	return opt_winsz, nil
}

// Instantiation
var lrLearner = LRLearner{}

func init() {
	RegisterLearner("lr-learner", &lrLearner)
}
