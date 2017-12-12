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
			//log.Printf("Incoming Message value: %f from vertex %d to %s\n", msg.MessageValue.(float64), msg.FormVertex, strconv.Itoa(prv.ID))
			sum += msg.MessageValue.(float64)
		}
		prv.CurrentValue = vertValue + (float64(sum) * edgeWeight)
		outgoingPageRank = prv.CurrentValue.(float64) / float64(len(prv.EdgeList))
		//log.Printf("V%d: Sum: %f,CurrentValue: %f, outgoingPageRank:  %f", prv.ID, sum, prv.CurrentValue.(float64), outgoingPageRank)
	}

	if prv.SuperStep <= 20 {
		for _, edge := range prv.EdgeList {
			msg := util.WorkerMessage{
				DestVertex:   edge.DestVertex,
				MessageValue: outgoingPageRank,
				SuperStep:    prv.SuperStep,
				FormVertex:   prv.ID,
			}
			//log.Printf("Sending msg to %d, superstep %d\n", msg.DestVertex, msg.SuperStep)
			//log.Printf("Current Value of %d is %f\n", prv.ID, prv.CurrentValue.(float64))
			prv.SendMessageTo(edge.DestVertex, msg, MsgCHan)
			//log.Printf("Message queue %d %d %d", len(prv.IncomingMsgCurrent), len(prv.IncomingMsgNext), len(prv.OutgoingMsg))
		}
	} else {
		//log.Printf("Current Value of %d is %f\n", prv.ID, prv.CurrentValue.(float64))
		prv.VoteToHalt()
	}

}
