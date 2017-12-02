package vertices

import "Sava/util"

// SSSPVertex ...
type SSSPVertex struct {
	Vertex
	Source      int
	Destination int
}

// Compute ...
func (sssp *SSSPVertex) Compute(Step int, MsgChan chan util.WorkerMessage) {
	// blablablabla TODO
}

func (sssp *SSSPVertex) isSource(v int) bool {
	return v == sssp.Source
}

func (sssp *SSSPVertex) isDestination(v int) bool {
	return v == sssp.Destination
}
