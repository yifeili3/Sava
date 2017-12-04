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
	var mindist float64
	if sssp.isSource(sssp.ID) {
		mindist = 0.0
	} else {
		mindist = math.MaxFloat64
	}
	for _, msg := range sssp.IncomingMsgCurrent {
		if mindist > msg.MessageValue.(float64) {
			mindist = msg.MessageValue.(float64)
		}
	}
	if mindist < sssp.CurrentValue.(float64) {
		sssp.CurrentValue = mindist
	}

	for _, edge := range sssp.EdgeList {
		msg := util.WorkerMessage{
			DestVertex:   edge.DestVertex,
			MessageValue: mindist + edge.EdgeValue.(float64),
			SuperStep:    Step,
		}
		sssp.SendMessageTo(edge.DestVertex, msg, MsgChan)
	}
	sssp.VoteToHalt()
}

func (sssp *SSSPVertex) isSource(v int) bool {
	return v == sssp.Source
}
