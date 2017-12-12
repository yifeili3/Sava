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
	tcpport           = 9339
)

type master struct {
	ID   int
	Addr net.UDPAddr
}

//Worker ...
type Worker struct {
	SuperStep   int
	ID          int
	VertexMap   map[int]vertices.Vertex
	Filename    string
	Connection  []*net.UDPConn
	Addr        net.UDPAddr
	HasStart    bool
	MasterList  []master
	MsgChan     chan util.WorkerMessage
	Partition   []util.MetaInfo
	TCPListener net.Listener
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

	l, err := net.Listen("tcp", ":"+strconv.Itoa(tcpport))
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
	worker.TCPListener = l

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
			if jobName == "PageRank" {
				w.RequestJob(jobName, dataFile, "")
			} else if jobName == "SSSP" {
				sourceID := cmd[3]
				w.RequestJob(jobName, dataFile, sourceID)

			} else {
				log.Println("Wrong job, plz try again")
			}

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
func (w *Worker) RequestJob(jobName string, filePath string, sourceID string) {
	for i := range w.MasterList {
		MasterID := w.MasterList[i].ID
		//util.RPCPutFile(MasterID, jobName, "job")
		util.RPCPutFile(MasterID, filePath, "data")
		// send message to master and ask to start job
		b := util.FormatMessage(jobName, filePath, sourceID)
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
		time.Sleep(time.Millisecond * 5000)
	}
}

func (w *Worker) WokerTCPListener() {
	for {
		c, err := w.TCPListener.Accept()
		if err != nil {
			log.Println(err)
		}
		go w.HandleTCPConn(c)

		//time.Sleep(time.Millisecond * 1000)
	}

}

func (w *Worker) HandleTCPConn(c net.Conn) {
	defer c.Close()
	var buf = make([]byte, 12000000)
	count := 0
	var n int
	var err error
	//log.Println("Start to read from conn")
	for {
		n, err = c.Read(buf[count:])
		if n != 0 {
			//log.Printf("Read %d byte from tcp\n", n)
		} else {
			//log.Println("Read finish")
			break
		}
		count += n
		if err != nil {
			//log.Println(err)
			//log.Println("Read finish")
			break
		}
	}

	log.Println(count)
	var msg util.Message
	err = json.Unmarshal(buf[0:count], &msg)
	if err != nil {
		log.Println(err)
	}
	//remoteAddr := c.LocalAddr()
	remoteAddr := c.RemoteAddr()

	if msg.MsgType == "W2W" {
		//log.Println("W2W")
		w.PartWorkerMsg(msg)
		for i := range w.MasterList {
			msg := util.Message{
				MsgType:   "W2WREV",
				SuperStep: w.SuperStep,
				TargetID:  util.CalculateID(remoteAddr.String()),
			}
			b, _ := json.Marshal(msg)
			targetAddr := net.UDPAddr{
				IP:   w.MasterList[i].Addr.IP,
				Port: masterListener,
			}
			srcAddr := net.UDPAddr{
				IP:   w.Addr.IP,
				Port: 1995,
			}
			time.Sleep(time.Millisecond * 30)
			util.SendMessage(&srcAddr, &targetAddr, b)
		}
	} else {
		log.Println("TCP receive some unrecognized msg")
	}
}

/*
func (w *Worker) WorkerMessageListener() {
	buf := make([]byte, 15000000)
	for {
		n, remoteAddr, _ := w.Connection[1].ReadFromUDP(buf)
		//log.Printf("messagerListener in %d\n", n)
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
			case "W2W":
				log.Println("W2W")
				w.PartWorkerMsg(msg)
				for i := range w.MasterList {
					msg := util.Message{
						MsgType:   "W2WREV",
						SuperStep: w.SuperStep,
						TargetID:  util.CalculateID(remoteAddr.IP.String()),
					}
					b, _ := json.Marshal(msg)
					targetAddr := net.UDPAddr{
						IP:   w.MasterList[i].Addr.IP,
						Port: masterListener,
					}
					util.UDPSend(&targetAddr, b)
				}
			default:
				log.Println("error")
			}
		} else {
			continue
		}
	}
}
*/

func (w *Worker) PartWorkerMsg(msg util.Message) {
	for i := range msg.PoolWorkerMsg {
		workmsg := msg.PoolWorkerMsg[i]
		// this is a list of metaInfos of a single vertex of that worker
		if w.SuperStep == msg.SuperStep {
			//log.Printf("The pool message is %d %d %f\n ", workmsg.DestVertex, workmsg.MessageValue.(float64))
			if v, ok := w.VertexMap[workmsg.DestVertex]; ok {
				//log.Println("Enqueue message")
				v.EnqueueMessage(workmsg)
			} else {
				log.Println("Network Vertex not existed")
			}
		}

	}
}

//WorkerTaskListener ...
func (w *Worker) WorkerTaskListener() {
	buf := make([]byte, 8192)
	for {
		n, _, _ := w.Connection[0].ReadFromUDP(buf)
		//log.Printf("WokerTaskListener %d\n", n)
		if n != 0 {
			//log.Println("TaskListener in")
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
				w.Partition = msg.Partition
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
						v.EdgeList = append(v.EdgeList, vertices.Edge{DestVertex: destNode, EdgeValue: 1.0})
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
						v.EdgeList = append(v.EdgeList, vertices.Edge{DestVertex: destNode, EdgeValue: 1.0})
					}
				}
				//log.Printf("Number of vertices: %d\n", msg.NumVertex)
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
					sourceID, _ := strconv.Atoi(msg.SourceID)
					ssspMap := make(map[int]vertices.Vertex, len(baseVertices))
					for id, baseVertex := range baseVertices {
						ssspMap[id] = &vertices.SSSPVertex{
							BaseVertex: *baseVertex,
							Source:     sourceID,
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
				// wait for a while and send msg to master
				//time.Sleep(time.Millisecond * 2000)
				if !w.checkHalt() {
					/*
						superstep := util.Message{MsgType: "SUPERSTEPDONE", SuperStep: w.SuperStep}
						buf := util.FormatWorkerMessage(superstep)
						srcAddr := net.UDPAddr{IP: w.Addr.IP, Port: udpSender}
						for i := 0; i < 2; i++ {
							destAddr := w.MasterList[i].Addr
							util.SendMessage(&srcAddr, &destAddr, buf)
						}*/
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
				arr := w.get25()
				jobdone := util.Message{MsgType: "JOBDONE", Result: arr}
				buf := util.FormatWorkerMessage(jobdone)
				srcAddr := net.UDPAddr{IP: w.Addr.IP, Port: udpSender}
				for i := 0; i < 2; i++ {
					destAddr := w.MasterList[i].Addr
					util.SendMessage(&srcAddr, &destAddr, buf)
				}
				os.Exit(0)
			case "RESULT":
				log.Println("Get the result from master, job done. Result:")
				for i := range msg.Result {
					log.Printf("%d\t%f", msg.Result[i].Key, msg.Result[i].Value)
				}
				os.Exit(0)
			}
		} else {
			log.Println("TaskListener else")
			continue
		}
	}
}

type sortarr []util.Sortstruct

func (s sortarr) Len() int {
	return len(s)
}
func (s sortarr) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortarr) Less(i, j int) bool {
	return s[i].Value > s[j].Value
}

func (w *Worker) get25() []util.Sortstruct {
	arr := make([]util.Sortstruct, 0)

	for _, v := range w.VertexMap {
		arr = append(arr, util.Sortstruct{Key: v.GetVertexID(), Value: v.GetValue().(float64)})
	}
	sort.Sort(sortarr(arr))
	return arr[0:25]
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
	defer func() {
		if err := recover(); err != nil {
			// Handle our error.
			fmt.Println("FIX")
			fmt.Println("ERR", err)
		}
	}()

	log.Println("\nWork: Running superstep #:", Step)
	updateChan := make(chan bool)
	for _, v := range w.VertexMap {
		go v.UpdateMessageQueue(updateChan)
	}
	for _ = range w.VertexMap {
		<-updateChan
	}
	doneChan := make(chan bool)
	for _, v := range w.VertexMap {
		go superStep(Step, v, doneChan, MsgChan)
	}

	for _ = range w.VertexMap {
		<-doneChan
	}

	messageQueue := make([][]util.WorkerMessage, len(w.Partition))
	for _, v := range w.VertexMap {
		messageQueue = *v.GetOutgoingMsg(messageQueue)
	}
	//log.Println("Sending W2W")
	// sending a bag of []util.WorkerMessage to workers
	for i := range messageQueue {
		//log.Println("Yo where the Fuck")
		receiverID := w.Partition[i].ID
		cmd := util.Message{
			MsgType:       "W2W",
			PoolWorkerMsg: messageQueue[i],
			SuperStep:     w.SuperStep,
		}
		b, _ := json.Marshal(cmd)
		//log.Println(len(b))
		//log.Println("begin dial...")
		srcAddr := util.CalculateIP(receiverID) + ":" + strconv.Itoa(tcpport)
		conn, err := net.Dial("tcp", srcAddr)
		if err != nil {
			log.Println(err)
		}

		conn.Write(b)
		//log.Println("TCP dial end.")
		conn.Close()
		/*
			srcAddr := net.UDPAddr{
				IP:   w.Addr.IP,
				Port: 4003,
			}
			targetAddr := net.UDPAddr{
				IP:   net.ParseIP(util.CalculateIP(receiverID)),
				Port: workermsgListener,
			}
			util.SendMessage(&srcAddr, &targetAddr, b)
		*/
		//time.Sleep(time.Millisecond * 1000)
		for i := range w.MasterList {
			msg := util.Message{
				MsgType:   "W2WSEND",
				SuperStep: w.SuperStep,
				TargetID:  receiverID,
			}
			b, _ := json.Marshal(msg)
			srcAddr := net.UDPAddr{
				IP:   w.Addr.IP,
				Port: 4001,
			}
			targetAddr := net.UDPAddr{
				IP:   w.MasterList[i].Addr.IP,
				Port: masterListener,
			}
			util.SendMessage(&srcAddr, &targetAddr, b)
		}
		//log.Println("End sending all messages")
		//time.Sleep(time.Millisecond * 1000)
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
	//log.Printf("worker's supterstep is %d, msg is %d", w.SuperStep, msg.SuperStep)
	if w.SuperStep == msg.SuperStep {
		//log.Printf("%d %f\n ", workerMsg.DestVertex, workerMsg.MessageValue.(float64))
		if v, ok := w.VertexMap[workerMsg.DestVertex]; ok {
			//log.Println("Enqueue message")
			v.EnqueueMessage(workerMsg)
		} else {
			log.Println("Network Vertex not existed")
		}
	}
}
