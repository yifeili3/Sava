package vertices

import (
	"Sava/util"
	"encoding/json"
	"net"
)

const (
	udpSender      = 4000
	workerListener = 4002
)

//BaseVertex ...
type BaseVertex struct {
	ID                 int
	WorkerID           int
	SuperStep          int
	EdgeList           []Edge
	CurrentValue       interface{}
	IncomingMsgCurrent []util.WorkerMessage
	IncomingMsgNext    []util.WorkerMessage
	OutgoingMsg        []util.WorkerMessage
	Partition          []util.MetaInfo
	IsActive           bool
}

//Edge ...
type Edge struct {
	DestVertex int
	EdgeValue  interface{}
}

//Vertex ...
type Vertex interface {
	Compute(Step int, MsgChan chan util.WorkerMessage)
	GetVertexID() int
	GetValue() interface{}
	GetOutEdgeList() *[]Edge
	UpdateMessageQueue()
	UpdateSuperstep(s int)
	GetActive() bool
	EnqueueMessage(in util.WorkerMessage)
	GetSuperStep() int
	MutableValue(in interface{})
}

//GetVertexID ...
func (v *BaseVertex) GetVertexID() int {
	return v.ID
}

//GetSuperStep ...
func (v *BaseVertex) GetSuperStep() int {
	return v.SuperStep
}

//GetValue ...
func (v *BaseVertex) GetValue() interface{} {
	return v.CurrentValue
}

//GetActive ...
func (v *BaseVertex) GetActive() bool {
	return v.IsActive
}

//MutableValue ...
func (v *BaseVertex) MutableValue(in interface{}) {
	v.CurrentValue = in
}

//GetOutEdgeList ...
func (v *BaseVertex) GetOutEdgeList() *[]Edge {
	return &v.EdgeList
}

//SendMessageTo ...
func (v *BaseVertex) SendMessageTo(destVertex int, msg util.WorkerMessage, MsgChan chan util.WorkerMessage) {
	// check if remote/local
	//log.Println("Sending vertex message from " + strconv.Itoa(v.ID) + "to " + strconv.Itoa(destVertex))
	n := destVertex % len(v.Partition)

	receiverID := v.Partition[n].ID
	/*
		if receiverID == v.WorkerID {
			log.Println("Send to local")
			sendToLocal(msg, MsgChan)
		} else {
			sendToRemote(receiverID, msg)
		}
	*/
	sendToRemote(receiverID, msg)
	//log.Printf("receiverID:%d\n", receiverID)
}

func sendToLocal(msg util.WorkerMessage, MsgChan chan util.WorkerMessage) {
	MsgChan <- msg
}

func sendToRemote(receiverID int, msg util.WorkerMessage) {
	// send over network
	m := util.Message{
		MsgType:   "V2V",
		WorkerMsg: msg,
	}
	buf, _ := json.Marshal(m)
	srcAddr := net.UDPAddr{
		IP:   net.ParseIP(util.WhereAmI()),
		Port: udpSender,
	}
	destAddr := net.UDPAddr{
		IP:   net.ParseIP(util.CalculateIP(receiverID)),
		Port: 4010,
	}
	//log.Println(destAddr.IP.String)
	util.SendMessage(&srcAddr, &destAddr, buf)
}

//VoteToHalt ...
func (v *BaseVertex) VoteToHalt() {
	v.IsActive = false
}

/* Below are functions used by worker */

//UpdateSuperstep ...
func (v *BaseVertex) UpdateSuperstep(s int) {
	v.SuperStep = s
}

//EnqueueMessage ...
func (v *BaseVertex) EnqueueMessage(in util.WorkerMessage) {
	v.IncomingMsgNext = append(v.IncomingMsgNext, in)
	//log.Println(len(v.IncomingMsgNext))
}

// UpdateMessageQueue ...need to update status of each vertex
func (v *BaseVertex) UpdateMessageQueue() {
	// move message from S+1 to S
	v.IncomingMsgCurrent = make([]util.WorkerMessage, len(v.IncomingMsgNext))
	for i := 0; i < len(v.IncomingMsgNext); i++ {
		// need deep copy????
		v.IncomingMsgCurrent[i] = v.IncomingMsgNext[i]
		//log.Println("incoming message from:" + strconv.FormatFloat(v.IncomingMsgCurrent[i].MessageValue.(float64), 'f', 6, 64))
	}
	v.IncomingMsgNext = nil
	// vote to halt, but message comes in
	if v.IsActive == false && len(v.IncomingMsgCurrent) > 0 {
		v.IsActive = true
	}
}
