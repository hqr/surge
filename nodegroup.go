package surge

import ()

type GroupInterface interface {
	getID() int // CH, random selection
	getCount() int

	SendGroup(ev EventInterface, how SendMethodEnum) bool
	RecvGroup(ev EventInterface) int
}

//
// types, c-tors
//
type NgtGroup struct {
	id int
}

type RzvGroup struct {
	servers []RunnerInterface
}

func NewNgtGroup(id int) *NgtGroup {
	assert(id*configReplicast.countNgtGroup <= config.numServers)
	return &NgtGroup{id}
}

func NewRzvGroup(srv ...RunnerInterface) *RzvGroup {
	return &RzvGroup{srv}
}

//
// Negotiating Group methods
//
func (g *NgtGroup) getID() int {
	return g.id
}

func (g *NgtGroup) getCount() int {
	return configReplicast.countNgtGroup
}

func (g *NgtGroup) SendGroup(ev EventInterface, how SendMethodEnum) bool {
	firstidx := (g.id - 1) * configReplicast.countNgtGroup
	for idx := 0; idx < configReplicast.countNgtGroup; idx++ {
		srv := allServers[firstidx+idx]
		ev.setOneArg(srv)
		srv.Send(ev, SmethodDirectInsert)
	}
	return true
}
