package worker

import (
	"Sava/util"
	"Sava/vertices"
	"bufio"
	"encoding/json"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

const (
	serverBase     = "172.22.154.132"
	udpSender      = 4000
	workerListener = 4002
	masterListener = 4004
)

type master struct {
	ID   int
	Addr net.UDPAddr
}

//Worker ...
type Worker struct {
	SuperStep  int
	ID         int
	VertexMap  map[int]vertices.Vertex
	Filename   string
	Connection *net.UDPConn
	Addr       net.UDPAddr
	HasStart   bool
	MasterList []master
	MsgChan    chan util.WorkerMessage
}

//NewWorker ...
func NewWorker() (worker *Worker, err error) {
	serverID := util.WhoAmI()
	ipAddr := util.WhereAmI()
	log.Println("Starting worker...")

	addr := net.UDPAddr{
		Port: workerListener,
		IP:   net.ParseIP(ipAddr),
	}

	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		log.Println(err)
	}

	worker = &Worker{
		Connection: conn,
		Addr:       addr,
		ID:         serverID,
		SuperStep:  1,
		HasStart:   false,
		MasterList: make([]master, 2),
	}
	for i := 0; i < 2; i++ {
		worker.MasterList[i] = master{
			ID:   i + 1,
			Addr: net.UDPAddr{IP: net.ParseIP(util.CalculateIP(i + 1)), Port: masterListener},
		}
	}
	return worker, err
}

// HandleInput ...Listen to STDIN and take in client's input
func (w *Worker) HandleInput() {
	var input string
	inputReader := bufio.NewReader(os.Stdin)
	for {
		input, _ = inputReader.ReadString('\n')
		in := strings.Replace(input, "\n", "", -1)
		cmd := strings.Split(in, " ")
		switch cmd[0] {
		case "JOIN":
			w.HasStart = true
			w.join()
		case "RUNJOB":
			jobName := cmd[1]
			dataFile := cmd[2]
			w.RequestJob(jobName, dataFile)
		default:
			continue
		}
	}
}

func (w *Worker) join() {
	join := util.Message{MsgType: "JOIN"}
	buf := util.FormatWorkerMessage(join)
	for i := 0; i < 2; i++ {
		srcAddr := net.UDPAddr{IP: w.Addr.IP, Port: udpSender}
		destAddr := w.MasterList[i].Addr
		util.SendMessage(&srcAddr, &destAddr, buf)
	}
}

//RequestJob ...
func (w *Worker) RequestJob(jobName string, filePath string) {
	for i := range w.MasterList {
		MasterID := w.MasterList[i].ID
		//util.RPCPutFile(MasterID, jobName, "job")
		util.RPCPutFile(MasterID, filePath, "data")
		// send message to master and ask to start job
		b := util.FormatMessage(jobName, filePath)
		targetAddr := net.UDPAddr{
			IP:   net.ParseIP(util.CalculateIP(MasterID)),
			Port: masterListener,
		}
		util.UDPSend(&targetAddr, b)

	}
	log.Println("Job requested to master")

}

// HeartBeat ...use as goroutine
func (w *Worker) HeartBeat() {
	for {
		if w.HasStart {
			heartbeat := util.Message{MsgType: "HEARTBEAT"}
			buf := util.FormatWorkerMessage(heartbeat)
			for i := 0; i < 2; i++ {
				srcAddr := net.UDPAddr{IP: w.Addr.IP, Port: udpSender}
				destAddr := w.MasterList[i].Addr
				util.SendMessage(&srcAddr, &destAddr, buf)
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

//WorkerTaskListener ...
func (w *Worker) WorkerTaskListener() {
	buf := make([]byte, 8192)
	for {
		n, _, _ := w.Connection.ReadFromUDP(buf)
		if n != 0 {
			// process message
			var msg util.Message
			err := json.Unmarshal(buf[0:n], &msg)
			if err != nil {
				log.Println(err)
			}

			switch msg.MsgType {
			case "CONFIRMJOIN":
				log.Println("Receive Confirmjoin Message")
			case "V2V": // Vertex message
				log.Println("Receive Vertex Message")
				w.networkMessageReceiver(msg)
			case "JOB": // Job message
				log.Println("Receive Job Message")
				//msg.FileName
				// Read Partition
				// arr:=
				numVertices := 0
				/*
					for i := 0; i < len(msg.Partition); i++ {
						if msg.Partition[i].ID == w.ID {
							part := msg.Partition[i]
						}
					}
				*/
				baseVertices := make(map[int]vertices.BaseVertex)
				// load vertices in map

				switch msg.JobName {
				case "PageRank":
					prvMap := make(map[int]vertices.Vertex, len(baseVertices))
					for id, baseVertex := range baseVertices {
						prvMap[id] = &vertices.PageRankVertex{
							BaseVertex:  baseVertex,
							NumVertices: numVertices,
						}
					}
					w.VertexMap = prvMap
				case "SSSP":
					//w.VertexMap =
				default:
					log.Println("Unknown operation")
				}
				//**********************************
				// TODO:initialize vertices
				go w.localMessageProcessor()
			case "SUPERSTEP": // Superstep msg
				if !w.checkHalt() {
					log.Println("Receive SuperStep Message")
					SuperstepNum := msg.SuperStep
					w.SuperStep = msg.SuperStep
					w.runSuperStep(SuperstepNum, w.MsgChan)
					// at end of superstep, send message to master
					superstep := util.Message{MsgType: "SUPERSTEPDONE", SuperStep: w.SuperStep}
					buf := util.FormatWorkerMessage(superstep)
					for i := 0; i < 2; i++ {
						destAddr := w.MasterList[i].Addr
						util.SendMessage(&w.Addr, &destAddr, buf)
					}
				} else {
					halt := util.Message{MsgType: "HALT", SuperStep: w.SuperStep}
					buf := util.FormatWorkerMessage(halt)
					for i := 0; i < 2; i++ {
						destAddr := w.MasterList[i].Addr
						util.SendMessage(&w.Addr, &destAddr, buf)
					}
				}
			case "DONE":
				//get output from vertices and rpc return file to master
			}
		} else {
			continue
		}
	}
}

func (w *Worker) checkHalt() bool {
	flag := true
	for _, v := range w.VertexMap {
		if v.GetActive() {
			flag = false
			break
		}
	}
	return flag
}

func (w *Worker) runSuperStep(Step int, MsgChan chan util.WorkerMessage) {
	log.Println("Work: Running superstep #:", Step)
	doneChan := make(chan bool)
	for _, v := range w.VertexMap {
		go superStep(Step, v, doneChan, MsgChan)
	}
	for _ = range w.VertexMap {
		<-doneChan
	}
	for _, v := range w.VertexMap {
		v.UpdateMessageQueue()
	}
}

func superStep(Step int, v vertices.Vertex, doneChan chan bool, MsgChan chan util.WorkerMessage) {
	v.UpdateSuperstep(Step)
	v.Compute(Step, MsgChan)
	doneChan <- true
}

// running on worker, receive msg and put into each vertex's message queue
func (w *Worker) localMessageProcessor() {
	for {
		localMsg := <-w.MsgChan
		if v, ok := w.VertexMap[localMsg.DestVertex]; ok {
			// local message
			v.EnqueueMessage(localMsg)
		} else {
			log.Println("Vertex not on this worker!")
		}
	}
}

func (w *Worker) networkMessageReceiver(msg util.Message) {
	workerMsg := msg.WorkerMsg
	if v, ok := w.VertexMap[workerMsg.DestVertex]; ok {
		v.EnqueueMessage(workerMsg)
	} else {
		log.Println("Network Vertex not existed")
	}
}
