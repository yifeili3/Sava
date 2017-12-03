package vertices

import (
	"Sava/util"
	"math"
)

// SSSPVertex ...
type SSSPVertex struct {
	BaseVertex
	Source int
}

// Compute ...
func (sssp *SSSPVertex) Compute(Step int, MsgChan chan util.WorkerMessage) {
	sssp.SuperStep = Step
	var mindist int
	if sssp.isSource(sssp.ID) {
		mindist = 0
	} else {
		mindist = math.MaxInt32
	}
	for _, msg := range sssp.IncomingMsgCurrent {
		if mindist > msg.MessageValue.(int) {
			mindist = msg.MessageValue.(int)
		}
	}
	if mindist < sssp.CurrentValue.(int) {
		sssp.CurrentValue = mindist
	}

	for _, edge := range sssp.EdgeList {
		msg := util.WorkerMessage{
			DestVertex:   edge.DestVertex,
			MessageValue: mindist + edge.EdgeValue.(int),
			SuperStep:    Step,
		}
		sssp.SendMessageTo(edge.DestVertex, msg, MsgChan)
	}
	sssp.VoteToHalt()
}

func (sssp *SSSPVertex) isSource(v int) bool {
	return v == sssp.Source
}
