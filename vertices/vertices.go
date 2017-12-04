package vertices

import (
	"Sava/util"
	"encoding/json"
	"log"
	"net"
	"sync"
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
	mux                sync.Mutex
	muxIncomingNext    sync.Mutex
	muxIncomingCurr    sync.Mutex
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
	UpdateMessageQueue(updateChan chan bool)
	UpdateSuperstep(s int)
	GetActive() bool
	EnqueueMessage(in util.WorkerMessage)
	GetSuperStep() int
	MutableValue(in interface{})
	SendAllMessage()
	GetOutgoingMsg(outMsg [][]util.WorkerMessage) *[][]util.WorkerMessage
}

// GetOutgoingMsg ...
func (v *BaseVertex) GetOutgoingMsg(outMsg [][]util.WorkerMessage) *[][]util.WorkerMessage {
	for i := 0; i < len(v.OutgoingMsg); i++ {
		if v.OutgoingMsg[i].SuperStep != v.SuperStep {
			continue
		}
		n := v.OutgoingMsg[i].DestVertex % len(v.Partition)
		outMsg[n] = append(outMsg[n], v.OutgoingMsg[i])
		//receiverID := v.Partition[n].ID
	}
	return &outMsg
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
	//log.Printf("Vex %d is sending message to worker %d,destVex %d\n", v.ID, receiverID, destVertex)
	/*
		if receiverID == v.WorkerID {
			log.Println("Send to local")
			sendToLocal(msg, MsgChan)
		} else {
			sendToRemote(receiverID, msg)
		}
	*/
	//sendToRemote(receiverID, msg)
	v.sendToQueue(receiverID, msg)
	//log.Printf("receiverID:%d\n", receiverID)
}

func (v *BaseVertex) SendAllMessage() {
	//log.Println(len(v.OutgoingMsg))
	for i := 0; i < len(v.OutgoingMsg); i++ {
		n := v.OutgoingMsg[i].DestVertex % len(v.Partition)
		receiverID := v.Partition[n].ID
		sendToRemote(receiverID, v.OutgoingMsg[i])
	}
	v.OutgoingMsg = make([]util.WorkerMessage, 0)
}

func sendToLocal(msg util.WorkerMessage, MsgChan chan util.WorkerMessage) {
	MsgChan <- msg
}

func (v *BaseVertex) sendToQueue(receiVerID int, msg util.WorkerMessage) {
	//log.Println("ADD LOCK")
	v.mux.Lock()
	v.OutgoingMsg = append(v.OutgoingMsg, msg)
	v.mux.Unlock()
	//log.Println("Release LOCK")
	//log.Printf("Appending msg to v.outgointmsg, value %f\n", v.OutgoingMsg[len(v.OutgoingMsg)-1].MessageValue)
}

func sendToRemote(receiverID int, msg util.WorkerMessage) {
	// send over network
	//log.Printf("ReceiverID is %d, msg dest is %d", receiverID, msg.DestVertex)
	m := util.Message{
		MsgType:   "V2V",
		SuperStep: msg.SuperStep,
		WorkerMsg: msg,
	}
	buf, _ := json.Marshal(m)
	srcAddr := net.UDPAddr{
		IP:   net.ParseIP(util.WhereAmI()),
		Port: 4005,
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
	//log.Println("get incoming message value:%f\n", in.MessageValue)
	//v.muxIncomingNext.Lock()
	v.IncomingMsgNext = append(v.IncomingMsgNext, in)
	//v.muxIncomingNext.Unlock()
	//log.Printf("length of incomingMsgNext is %d\n", len(v.IncomingMsgNext))
}

// UpdateMessageQueue ...need to update status of each vertex
func (v *BaseVertex) UpdateMessageQueue(updateChan chan bool) {
	//log.Println("Print incomingmsgCurrent")

	// move message from S+1 to S
	v.IncomingMsgCurrent = make([]util.WorkerMessage, 0)
	for i := 0; i < len(v.IncomingMsgNext); i++ {
		// need deep copy????
		if v.SuperStep == v.IncomingMsgNext[i].SuperStep {
			//v.muxIncomingNext.Lock()
			v.IncomingMsgCurrent = append(v.IncomingMsgCurrent, v.IncomingMsgNext[i])
			//v.muxIncomingNext.Unlock()
			//log.Println("incoming message from:" + strconv.FormatFloat(v.IncomingMsgCurrent[i].MessageValue.(float64), 'f', 6, 64))
		}

	}
	v.IncomingMsgNext = make([]util.WorkerMessage, 0)
	// vote to halt, but message comes in
	if v.IsActive == false && len(v.IncomingMsgCurrent) > 0 {
		log.Println("Should stop at 2")
		v.IsActive = true
	}
	updateChan <- true
}
