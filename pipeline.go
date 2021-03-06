// Package surge provides a framework for discrete event simulation, as well as
// a number of models for Unsolicited and Reservation Group based Edge-driven
// load balancing. Targeted modeling area includes large and super-large storage
// clusters with multiple access points (referred to as "gateways") and multiple
// storage targets (referred to as "servers").
//
package surge

//
// types
//
type StageFunc func(EventInterface) error

// PipelineStage is a triplet (name, handler, index) where the handler
// (callback) gets automatically executed for a given named stage.
//
// See tio.go for the generic mechanism to move tio
// (a.k.a transactional IO, an abstraction for a compound multi-step
// networking operation) through its associated pipeline,
// from stage to the next stage and strictly in the order of increasing
// PipelineStage.index.
//
// The pipeline itself is just an array of named stages (and their respective
// callback handlers) that is *declared* at model initialization time and that
// typically never changes.
//
type PipelineStage struct {
	name    string
	handler string // name of the method to process this named stage
	index   int
}

// Pipeline object represents IO pipeline, typically a single (and static)
// one for a given model.
type Pipeline struct {
	pslice []PipelineStage
}

//
// methods
//
func NewPipeline() *Pipeline {
	p := make([]PipelineStage, 8) // notice the max.. will likely be enough
	return &Pipeline{p[0:0]}
}

func (p *Pipeline) AddStage(stage *PipelineStage) {
	stage.index = len(p.pslice)
	p.pslice = append(p.pslice, *stage)
}

func (p *Pipeline) Count() int {
	return len(p.pslice)
}

func (p *Pipeline) GetStage(idx int) *PipelineStage {
	assert(idx < p.Count())
	return &p.pslice[idx]
}

func (p *Pipeline) IndexOf(name string) int {
	for i := 0; i < len(p.pslice); i++ {
		if name == p.pslice[i].name {
			return i
		}
	}
	return -1
}

func (p *Pipeline) String() string {
	if len(p.pslice) == 0 {
		return "<nil>"
	}
	s := p.pslice[0].name
	for i := 1; i < len(p.pslice); i++ {
		s += " => " + p.pslice[i].name
	}
	return s
}
