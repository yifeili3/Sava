package vertices

import "Sava/util"

//PageRankVertex ...
type PageRankVertex struct {
	BaseVertex
	NumVertices int
}

const vertValue = 0.15
const edgeWeight = 0.85

// Compute ...
func (prv *PageRankVertex) Compute(Step int, MsgCHan chan util.WorkerMessage) {
	prv.SuperStep = Step
	if prv.SuperStep < 30 {
		sum := 0.0
		for _, msg := range prv.IncomingMsgCurrent {
			sum += msg.MessageValue.(float64)
		}
		prv.CurrentValue = vertValue/float64(prv.NumVertices) + (float64(sum) * edgeWeight)

		outgoingPageRank := prv.CurrentValue.(float64) / float64(len(prv.EdgeList))

		for _, edge := range prv.EdgeList {
			msg := util.WorkerMessage{
				DestVertex:   edge.DestVertex,
				MessageValue: outgoingPageRank,
				SuperStep:    prv.SuperStep,
			}
			prv.SendMessageTo(edge.DestVertex, msg, MsgCHan)
		}
	} else {
		prv.VoteToHalt()
	}
}
