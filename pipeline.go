package surge

//
// types
//
type StageFunc func(EventInterface) error

type PipelineStage struct {
	name    string
	handler string // name of the method to process this named stage
	index   int
}

type Pipeline struct {
	pslice []PipelineStage
}

//
// methods
//
func NewPipeline() *Pipeline {
	p := make([]PipelineStage, 8)
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

func (p *Pipeline) NewTio(src RunnerInterface) *Tio {
	return NewTio(src, p)
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
