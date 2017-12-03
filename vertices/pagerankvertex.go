package vertices

import (
	"Sava/util"
)

//PageRankVertex ...
type PageRankVertex struct {
	BaseVertex
	NumVertices int
}

const vertValue = 0.15
const edgeWeight = 0.85

// Compute ...
func (prv *PageRankVertex) Compute(Step int, MsgCHan chan util.WorkerMessage) {
	//log.Println("-------Vertex" + strconv.Itoa(prv.ID) + "-------")
	prv.SuperStep = Step
	var outgoingPageRank float64
	if prv.SuperStep >= 1 {
		sum := 0.0
		for _, msg := range prv.IncomingMsgCurrent {
			//log.Printf("Incoming Message value: %f\n", msg.MessageValue.(float64))
			sum += msg.MessageValue.(float64)
		}
		prv.CurrentValue = vertValue/float64(prv.NumVertices) + (float64(sum) * edgeWeight)
		outgoingPageRank = prv.CurrentValue.(float64) / float64(len(prv.EdgeList))
		//log.Printf("Sum: %f,CurrentValue: %f, outgoingPageRank:  %f", sum, prv.CurrentValue.(float64), outgoingPageRank)
	}

	if prv.SuperStep <= 10 {
		for _, edge := range prv.EdgeList {
			msg := util.WorkerMessage{
				DestVertex:   edge.DestVertex,
				MessageValue: outgoingPageRank,
				SuperStep:    prv.SuperStep,
			}
			prv.SendMessageTo(edge.DestVertex, msg, MsgCHan)
		}
	} else {
		//log.Printf("Current Value of %d is %f\n", prv.ID, prv.CurrentValue.(float64))
		prv.VoteToHalt()
	}

}
