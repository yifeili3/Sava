package worker

import (
	"Sava/util"
	"Sava/vertices"
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	serverBase        = "172.22.154.132"
	udpSender         = 4000
	workerListener    = 4002
	workermsgListener = 4010
	masterListener    = 4004
	fileBase          = "/home/yifeili3/sava/"
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
	Connection []*net.UDPConn
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

	msgaddr := net.UDPAddr{
		Port: workermsgListener,
		IP:   net.ParseIP(ipAddr),
	}
	msgconn, err := net.ListenUDP("udp", &msgaddr)
	if err != nil {
		log.Println(err)
	}

	worker = &Worker{
		Connection: make([]*net.UDPConn, 2),
		Addr:       addr,
		ID:         serverID,
		SuperStep:  1,
		HasStart:   false,
		MasterList: make([]master, 2),
	}
	worker.Connection[0] = conn
	worker.Connection[1] = msgconn

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

func (w *Worker) WorkerMessageListener() {
	buf := make([]byte, 8192)
	for {
		n, _, _ := w.Connection[1].ReadFromUDP(buf)
		if n != 0 {
			// process message
			var msg util.Message
			err := json.Unmarshal(buf[0:n], &msg)
			if err != nil {
				log.Println(err)
			}
			switch msg.MsgType {
			case "V2V": // Vertex message
				//log.Println("Receive Vertex Message")
				w.networkMessageReceiver(msg)
			default:
				log.Println("error")
			}
		} else {
			continue
		}
	}
}

//WorkerTaskListener ...
func (w *Worker) WorkerTaskListener() {
	buf := make([]byte, 8192)
	for {
		n, _, _ := w.Connection[0].ReadFromUDP(buf)
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
			/*
				case "V2V": // Vertex message
					log.Println("Receive Vertex Message")
					w.networkMessageReceiver(msg)
			*/
			case "JOB": // Job message
				log.Println("Receive Job Message")
				baseVertices := make(map[int]*vertices.BaseVertex)

				file, err := os.OpenFile(fileBase+"data/"+msg.FileName, os.O_RDONLY, 0666)
				if err != nil {
					log.Println(err)
				}
				defer file.Close()
				br := bufio.NewReader(file)
				for {
					a, _, err := br.ReadLine()
					if err == io.EOF {
						break
					}
					line := string(a)
					src := strings.Split(line, "\t")[0]
					dest := strings.Split(line, "\t")[1]
					srcNode, _ := strconv.Atoi(src)
					destNode, _ := strconv.Atoi(dest)

					if v, ok := baseVertices[srcNode]; ok {
						v.EdgeList = append(v.EdgeList, vertices.Edge{DestVertex: destNode, EdgeValue: 1})
					} else {
						baseVertices[srcNode] = &vertices.BaseVertex{
							ID:                 srcNode,
							WorkerID:           w.ID,
							SuperStep:          0,
							CurrentValue:       1.0,
							EdgeList:           make([]vertices.Edge, 0),
							IncomingMsgCurrent: make([]util.WorkerMessage, 0),
							IncomingMsgNext:    make([]util.WorkerMessage, 0),
							OutgoingMsg:        make([]util.WorkerMessage, 0),
							Partition:          make([]util.MetaInfo, 0),
							IsActive:           true,
						}
						v := baseVertices[srcNode]
						v.Partition = msg.Partition
						v.EdgeList = append(v.EdgeList, vertices.Edge{DestVertex: destNode, EdgeValue: 1})
					}
				}
				log.Printf("Number of vertices: %d\n", msg.NumVertex)
				switch msg.JobName {
				case "PageRank":
					prvMap := make(map[int]vertices.Vertex, len(baseVertices))
					for id, baseVertex := range baseVertices {
						prvMap[id] = &vertices.PageRankVertex{
							BaseVertex:  *baseVertex,
							NumVertices: msg.NumVertex,
						}
					}
					w.VertexMap = prvMap
				case "SSSP":
					ssspMap := make(map[int]vertices.Vertex, len(baseVertices))
					for id, baseVertex := range baseVertices {
						ssspMap[id] = &vertices.SSSPVertex{
							BaseVertex: *baseVertex,
							Source:     msg.NumVertex,
						}
					}
					w.VertexMap = ssspMap

				default:
					log.Println("Unknown operation")
				}

				ack := util.Message{MsgType: "ACK"}
				buf := util.FormatWorkerMessage(ack)
				for i := 0; i < 2; i++ {
					srcAddr := net.UDPAddr{IP: w.Addr.IP, Port: udpSender}
					destAddr := w.MasterList[i].Addr
					util.SendMessage(&srcAddr, &destAddr, buf)
				}

			case "SUPERSTEP": // Superstep msg
				SuperstepNum := msg.SuperStep
				w.SuperStep = msg.SuperStep
				w.MsgChan = make(chan util.WorkerMessage)
				w.runSuperStep(SuperstepNum, w.MsgChan)
				// at end of superstep, send message to master
				if !w.checkHalt() {
					superstep := util.Message{MsgType: "SUPERSTEPDONE", SuperStep: w.SuperStep}
					buf := util.FormatWorkerMessage(superstep)
					srcAddr := net.UDPAddr{IP: w.Addr.IP, Port: udpSender}
					for i := 0; i < 2; i++ {
						destAddr := w.MasterList[i].Addr
						util.SendMessage(&srcAddr, &destAddr, buf)
					}
				} else {
					halt := util.Message{MsgType: "HALT"}
					buf := util.FormatWorkerMessage(halt)
					srcAddr := net.UDPAddr{IP: w.Addr.IP, Port: udpSender}
					for i := 0; i < 2; i++ {
						destAddr := w.MasterList[i].Addr
						util.SendMessage(&srcAddr, &destAddr, buf)
					}
				}
			case "JOBDONE":
				//get output from vertices and rpc return file to master
				w.writeToFile(fileBase + "result/" + strconv.Itoa(w.ID) + ".txt")
				jobdone := util.Message{MsgType: "JOBDONE"}
				buf := util.FormatWorkerMessage(jobdone)
				srcAddr := net.UDPAddr{IP: w.Addr.IP, Port: udpSender}
				for i := 0; i < 2; i++ {
					destAddr := w.MasterList[i].Addr
					util.SendMessage(&srcAddr, &destAddr, buf)
				}
			}
		} else {
			continue
		}
	}
}

type sortarr []sortstruct

type sortstruct struct {
	key   int
	value float64
}

func (s sortarr) Len() int {
	return len(s)
}
func (s sortarr) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortarr) Less(i, j int) bool {
	return s[i].value > s[j].value
}

func (w *Worker) writeToFile(filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Println(err)
	}
	defer file.Close()
	arr := make([]sortstruct, 0)

	for _, v := range w.VertexMap {
		arr = append(arr, sortstruct{key: v.GetVertexID(), value: v.GetValue().(float64)})
	}
	sort.Sort(sortarr(arr))
	for i := 0; i < len(arr); i++ {
		fmt.Fprintln(file, strconv.Itoa(arr[i].key)+"\t"+strconv.FormatFloat(arr[i].value, 'f', 6, 64))
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
	for _, v := range w.VertexMap {
		v.UpdateMessageQueue()
	}
	doneChan := make(chan bool)
	for _, v := range w.VertexMap {
		go superStep(Step, v, doneChan, MsgChan)
	}
	for _ = range w.VertexMap {
		<-doneChan
	}
	for _, v := range w.VertexMap {
		v.SendAllMessage()
	}
}

func superStep(Step int, v vertices.Vertex, doneChan chan bool, MsgChan chan util.WorkerMessage) {
	v.UpdateSuperstep(Step)
	v.Compute(Step, MsgChan)
	doneChan <- true
}

// running on worker, receive msg and put into each vertex's message queue
func (w *Worker) localMessageProcessor() {
	log.Println("start local message processor")
	for {
		localMsg := <-w.MsgChan
		log.Println("Local Message")
		if v, ok := w.VertexMap[localMsg.DestVertex]; ok {
			// local message
			log.Println("Local Message")
			v.EnqueueMessage(localMsg)
		} else {
			log.Println("Vertex not on this worker!")
		}
	}
}

func (w *Worker) networkMessageReceiver(msg util.Message) {
	workerMsg := msg.WorkerMsg
	//log.Printf("%d %f\n ", workerMsg.DestVertex, workerMsg.MessageValue.(float64))
	if v, ok := w.VertexMap[workerMsg.DestVertex]; ok {
		//log.Println("Enqueue message")
		v.EnqueueMessage(workerMsg)
	} else {
		log.Println("Network Vertex not existed")
	}
}
