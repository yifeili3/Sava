package master

import (
	"SDFS/member"
	"Sava/util"
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	masterUDPport   = 4004
	udpport         = 4002
	baseFilepath    = "/home/yifeili3/sava/"
	threshold       = 200
	masterthreshold = 40
)

// what master is doing

type Master struct {
	SuperStep      int
	IsMaster       bool
	MembershipList []member.Node
	Connection     *net.UDPConn
	Addr           net.UDPAddr
	ID             int
	BackupMaster   member.Node
	Partition      []util.MetaInfo
	ClientID       int
	JobName        string
	DataFile       string
	WaitMsg        [][]int
	WWCount        int
	Mux            sync.Mutex
	Flag           bool
	Result         []util.Sortstruct
	SourceID       string
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

func NewMaster() (master *Master, err error) {
	ID := util.WhoAmI()
	ipAddr := util.WhereAmI()

	addr := net.UDPAddr{
		IP:   net.ParseIP(ipAddr),
		Port: masterUDPport,
	}
	conn, err := net.ListenUDP("udp", &addr)
	var isMaster bool
	if ID == 1 {
		isMaster = true
	} else if ID == 2 {
		isMaster = false
	}
	backupMaster := *member.NewMember(3-ID, net.UDPAddr{IP: net.ParseIP(util.CalculateIP(3 - ID)), Port: masterUDPport}, 0)
	master = &Master{
		SuperStep:      -1,
		IsMaster:       isMaster,
		MembershipList: make([]member.Node, 8),
		Connection:     conn,
		Addr:           addr,
		ID:             ID,
		BackupMaster:   backupMaster,
		Flag:           true,
		WWCount:        0,
		Result:         make([]util.Sortstruct, 0),
	}
	// fill the membershiplist for machine 2 to 8
	for i := 2; i < 10; i++ {
		master.MembershipList[i-2] = *member.NewMember(i+1, net.UDPAddr{IP: net.ParseIP(util.CalculateIP(i + 1)), Port: udpport}, 0)
	}
	return master, err

}

// Three kinds of messages
// Heartbeat
// jobrequest
// jobDone from workers
func (m *Master) UDPListener() {
	p := make([]byte, 4096)
	for {
		n, remoteAddr, _ := m.Connection.ReadFromUDP(p)
		if n == 0 {
			continue
		} else {
			var ret util.Message
			err := json.Unmarshal(p[0:n], &ret)
			if err != nil {
				log.Println(err)
			}
			if ret.JobName != "" { // only happens when requesting a job
				remoteID := util.CalculateID(remoteAddr.IP.String())
				log.Printf("Receive job %s\n", ret.JobName)
				m.partitionJob(remoteID, ret.JobName, ret.FileName, ret.SourceID)
			} else if ret.MsgType != "" {
				// JOIN/#LEAVE/HEARTBEAT/HALT/DONE
				if ret.MsgType == "JOIN" {
					//log.Println("Receive some worker join...")
					m.joinNode(remoteAddr)
				} else if ret.MsgType == "HEARTBEAT" {
					m.updateHeartBeat(remoteAddr)
				} else if ret.MsgType == "SUPERSTEPDONE" {
					m.updateSuperStep(remoteAddr, ret)
				} else if ret.MsgType == "HALT" {
					m.processHalt(remoteAddr, ret)
				} else if ret.MsgType == "ACK" { // check the state of all partition worker
					m.checkStart(remoteAddr)
				} else if ret.MsgType == "JOBDONE" {
					m.collectResult(remoteAddr, ret)
				} else if ret.MsgType == "W2WSEND" {
					m.updateSuperStep(remoteAddr, ret)
				} else if ret.MsgType == "W2WREV" {
					m.startNextSupterStep(remoteAddr, ret)
				} else {
					log.Println("Unknown message from:" + remoteAddr.IP.String())
				}
			}

		}

	}

}

// update the membership list for workers
func (m *Master) UpdateMemberShip() {
	// send heartbeat to backup master to let him know alive
	targetAddr := net.UDPAddr{
		IP:   net.ParseIP(util.CalculateIP(m.BackupMaster.ID)),
		Port: masterUDPport,
	}
	cmd := util.Message{
		MsgType: "HEARTBEAT",
	}
	b, _ := json.Marshal(cmd)
	util.UDPSend(&targetAddr, b)
	// add 1 to heartbeat and check threshold
	for i := range m.MembershipList {
		if m.MembershipList[i].Active {
			m.MembershipList[i].UpdateHeartBeat()
			if threshold < m.MembershipList[i].Heartbeat {
				if !m.MembershipList[i].Fail && m.MembershipList[i].Active {
					log.Println("Detect node " + strconv.Itoa(m.MembershipList[i].ID) + " failure")
					m.MembershipList[i].Fail = true
					// repartition job
					if len(m.JobName) != 0 {
						m.partitionJob(m.ClientID, m.JobName, m.DataFile, m.SourceID)
					}

				}
			}
		}
	}
	m.BackupMaster.UpdateHeartBeat()
	if masterthreshold < m.BackupMaster.Heartbeat {
		// Master down
		if !m.BackupMaster.Fail {
			log.Println("Backup master down")
			m.BackupMaster.Fail = true
		}

		if !m.IsMaster {
			m.IsMaster = true
			log.Println("Taking off the job of original master")
		}
	}

}

func (m *Master) intergrateFile() {

	sort.Sort(sortarr(m.Result))
	for i := range m.Result {
		if i >= 25 {
			break
		}
		log.Printf("%d\t%f", m.Result[i].Key, m.Result[i].Value)
	}

	// intergrate the result file into one file and send it back to client
	/*
		foName := baseFilepath + m.JobName + "_result.txt"
		fo, err := os.OpenFile(foName, os.O_CREATE|os.O_WRONLY, 0600)

		if err != nil {
			log.Println(err)
		}

		bw := bufio.NewWriter(fo)
		filepath.Walk(baseFilepath+"result/", func(path string, info os.FileInfo, err error) error {
			fp, err := os.Open(path)
			if err != nil {
				log.Println(err)
			}
			br := bufio.NewReader(fp)
			for {
				buffer := make([]byte, 1024)
				readCount, readErr := br.Read(buffer)
				if readErr == io.EOF {
					break
				} else {
					bw.Write(buffer[:readCount])
				}
			}
			fp.Close()
			os.Remove(path)
			return err

		})
		bw.Flush()
		fo.Close()
		// RPC this file back to client
		util.RPCPutFile(m.ClientID, foName, "result")
	*/
	log.Println("transmit result to client done, job done.")

}

func (m *Master) collectResult(workerAddr *net.UDPAddr, ret util.Message) {

	id := util.CalculateID(workerAddr.IP.String())
	for i := range ret.Result {
		m.Result = append(m.Result, ret.Result[i])
	}

	log.Printf("Node %d response to jobdone\n", id)
	flag := true
	for i := range m.Partition {
		if m.Partition[i].ID == id {
			m.Partition[i].State = -2
			m.Partition[i].Halt = true
		}
		flag = flag && (m.Partition[i].State == -2)
	}
	if flag && m.IsMaster {
		m.intergrateFile()
		// migrate all the result file
	}

}

func (m *Master) jobDone() {
	if !m.IsMaster {
		return
	}
	for i := range m.Partition {
		IP := util.CalculateIP(m.Partition[i].ID)
		sourceAddr := net.UDPAddr{
			IP:   net.ParseIP(IP),
			Port: udpport,
		}
		cmd := util.Message{
			MsgType: "JOBDONE",
		}
		b, _ := json.Marshal(cmd)
		util.UDPSend(&sourceAddr, b)
	}
}

func (m *Master) processHalt(workerAddr *net.UDPAddr, ret util.Message) {

	id := util.CalculateID(workerAddr.IP.String())
	log.Printf("Receive Node %d saying Halt", id)
	flag := true
	for i := range m.Partition {
		log.Printf("check %d\n", m.Partition[i].ID)
		if m.Partition[i].ID == id {
			m.Partition[i].State = ret.SuperStep
			m.Partition[i].Halt = true
		}
		flag = flag && m.Partition[i].Halt
	}
	if flag && m.IsMaster {
		log.Printf("Sending message to all workers that job done.\n")
		m.jobDone()
	}
}

func (m *Master) updateSuperStep(workerAddr *net.UDPAddr, ret util.Message) {
	var x int
	var y int
	id := util.CalculateID(workerAddr.IP.String())
	receiveID := ret.TargetID
	for i := range m.Partition {
		if m.Partition[i].ID == id {
			x = i
		}
		if m.Partition[i].ID == receiveID {
			y = i
		}
	}

	log.Printf("Master receive %d that he's sending m to %d, superstep m %d, w %d\n", id, receiveID, m.SuperStep, ret.SuperStep)
	if m.SuperStep == ret.SuperStep {
		m.Mux.Lock()
		m.WaitMsg[x][y] -= 1
		m.WWCount++
		log.Println(m.WWCount)
		m.Mux.Unlock()
	}

	/*
		flag := true
		for i := range m.Partition {
			log.Printf("check %d\n", m.Partition[i].ID)
			if m.Partition[i].ID == id {
				m.Partition[i].State = ret.SuperStep
				m.Partition[i].Halt = false
			}
			flag = flag && (m.Partition[i].State == m.SuperStep)
		}
		if flag && m.IsMaster {
			log.Printf("All nodes finished current superstep%d, start next.\n", m.SuperStep)
			time.Sleep(time.Millisecond * 2000)
			m.startNextSupterStep()

		}
	*/
}

func (m *Master) startNextSupterStep(workerAddr *net.UDPAddr, ret util.Message) {
	var x int
	var y int

	receiveID := util.CalculateID(workerAddr.IP.String())
	id := ret.TargetID
	log.Printf("Master receive %d that %d's message has sent to him, superstep m %d, w %d\n", receiveID, id, m.SuperStep, ret.SuperStep)
	for i := range m.Partition {
		if m.Partition[i].ID == id {
			x = i
		}
		if m.Partition[i].ID == receiveID {
			m.Partition[i].SSdone = true
			y = i
		}
	}
	if m.SuperStep == ret.SuperStep {
		m.Mux.Lock()
		m.WaitMsg[x][y] += 1
		m.WWCount++
		log.Println(m.WWCount)
		m.Mux.Unlock()
	}
	size := len(m.Partition)
	flag := true
	flag = flag && (m.WWCount == size*size*2)
	if !flag {
		return
	}
	for i := range m.WaitMsg {
		for j := range m.WaitMsg[i] {
			m.Mux.Lock()
			flag = (m.WaitMsg[i][j] == 0) && flag
			m.Mux.Unlock()
			if !flag {
				//log.Printf("i %d, j %d\n", i, j)
				break
			}
		}
		if !flag {
			break
		}
		flag = !m.Partition[i].Halt && flag && m.Partition[i].SSdone
		if !flag {
			log.Printf("Node %d has voted to halt\n", m.Partition[i].ID)
			break
		}

	}

	if !flag {
		return
	}
	/*
		for i := range m.WaitMsg {
			for j := range m.WaitMsg[i] {
				log.Printf("%d \n", m.WaitMsg[i][j])
			}
			log.Println()
		}
	*/

	m.SuperStep++
	m.WWCount = 0
	if !m.IsMaster {
		return
	}
	log.Printf("Master: %d Superstep start\n", m.SuperStep)
	for i := range m.Partition {
		IP := util.CalculateIP(m.Partition[i].ID)
		sourceAddr := net.UDPAddr{
			IP:   net.ParseIP(IP),
			Port: udpport,
		}
		cmd := util.Message{
			MsgType:   "SUPERSTEP",
			SuperStep: m.SuperStep,
		}
		b, _ := json.Marshal(cmd)
		util.UDPSend(&sourceAddr, b)
	}

}

func (m *Master) checkStart(workerAddr *net.UDPAddr) {
	id := util.CalculateID(workerAddr.IP.String())
	flag := true
	for i := range m.Partition {
		if m.Partition[i].ID == id {
			m.Partition[i].State = 0
			m.Partition[i].Halt = false
		}
		flag = flag && (m.Partition[i].State == 0)
	}
	if flag && m.IsMaster {
		// start job!
		m.startJob()
	}

}

func (m *Master) startJob() {
	m.SuperStep = 0
	for i := range m.Partition {
		IP := util.CalculateIP(m.Partition[i].ID)
		sourceAddr := net.UDPAddr{
			IP:   net.ParseIP(IP),
			Port: udpport,
		}
		cmd := util.Message{
			MsgType:   "SUPERSTEP",
			SuperStep: m.SuperStep,
		}
		b, _ := json.Marshal(cmd)
		util.UDPSend(&sourceAddr, b)
	}
}

// note that the data file is already been processed
func (m *Master) partitionJob(clientID int, jobName string, datafile string, sourceID string) {
	//clientID := util.CalculateID(clientAddr.IP.String())
	m.SourceID = sourceID
	m.ClientID = clientID
	m.WWCount = 0
	m.Result = make([]util.Sortstruct, 0)

	// get the alive worker to partition job
	var aliveWorker []int
	for i := range m.MembershipList {
		if m.MembershipList[i].Active && !m.MembershipList[i].Fail {
			if m.MembershipList[i].ID != clientID {
				aliveWorker = append(aliveWorker, m.MembershipList[i].ID)
			}
		}
	}
	size := len(aliveWorker)
	if size == 0 {
		log.Println("No worker available")
		os.Exit(0)
		return
	}
	m.WaitMsg = make([][]int, size)
	for i := range m.WaitMsg {
		m.WaitMsg[i] = make([]int, size)
		for j := range m.WaitMsg[i] {
			m.WaitMsg[i][j] = 0
		}
	}
	//var PartitionInfo []util.MetaInfo
	PartitionInfo := make([]util.MetaInfo, size)
	// partition the original data file into size parts
	// readfile
	fi, err := os.OpenFile(baseFilepath+"data/"+datafile, os.O_RDONLY, 0666)
	if err != nil {
		log.Println("Open datafile error")
	}

	br := bufio.NewReader(fi)
	var nodeCount int
	nodeCount = 0
	//everyPartSize := 0
	var partition [][]string
	partition = make([][]string, size)
	NodeMap := make(map[int]int, 0)
	for {
		a, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		line := string(a)
		//log.Println(line)
		if strings.HasPrefix(line, "#") {
			if !strings.Contains(line, "Nodes:") {
				continue
			} else {
				temp := strings.Split(line, " ")
				nodeCount, _ = strconv.Atoi(temp[2])
				//everyPartSize = nodeCount / size
				//log.Printf("nodecount:%d, everyPartSize:%d\n", nodeCount, everyPartSize)
				continue
			}
		}

		// start to treat real node->node
		node1 := strings.Split(line, "\t")[0]
		num1, _ := strconv.Atoi(node1)
		node2 := strings.Split(line, "\t")[1]
		num2, _ := strconv.Atoi(node2)
		//log.Printf(line+" be put in partition[%d], node ID %d", num1%size, aliveWorker[num1%size])
		partition[num1%size] = append(partition[num1%size], line)
		partition[num2%size] = append(partition[num2%size], node2+"\t"+node1)
		NodeMap[num1] = 0
		NodeMap[num2] = 0

	}
	fi.Close()
	// save these temp data to number of files
	for i := range aliveWorker {
		meta := util.MetaInfo{
			ID:     aliveWorker[i],
			State:  -1,
			Halt:   false,
			SSdone: false,
		}
		//PartitionInfo = append(PartitionInfo, meta)
		PartitionInfo[i] = meta

		fo, err := os.Create(baseFilepath + "data/" + strconv.Itoa(aliveWorker[i]) + ".txt")
		if err != nil {
			log.Println("Create file error")
		}

		wb := bufio.NewWriter(fo)
		for j := range partition[i] {
			wb.WriteString(partition[i][j] + "\n")
		}

		wb.Flush()
		fo.Close()

		// and then RPC this file to target worker
		if m.IsMaster {
			log.Println("RPC " + strconv.Itoa(aliveWorker[i]) + ".txt")
			util.RPCPutFile(aliveWorker[i], baseFilepath+"data/"+strconv.Itoa(aliveWorker[i])+".txt", "data")
		}
		// remove the file since no use after
		//os.Remove(baseFilepath + "data/" + strconv.Itoa(aliveWorker[i]) + ".txt")
	}

	m.Partition = PartitionInfo
	m.JobName = jobName
	m.DataFile = datafile
	if !m.IsMaster {
		m.SuperStep = 0
		return
	}
	// send start work message to all workers
	for i := range aliveWorker {
		msg := util.Message{
			MsgType:   "JOB",
			Partition: m.Partition,
			JobName:   jobName,
			FileName:  strconv.Itoa(aliveWorker[i]) + ".txt",
			NumVertex: nodeCount,
			SourceID:  sourceID,
		}
		b, _ := json.Marshal(msg)
		targetAddr := net.UDPAddr{
			IP:   net.ParseIP(util.CalculateIP(aliveWorker[i])),
			Port: udpport,
		}
		util.UDPSend(&targetAddr, b)
	}

}

func (m *Master) joinNode(sourceAddr *net.UDPAddr) {
	ID := util.CalculateID(sourceAddr.IP.String())
	log.Println("Receive Worker " + strconv.Itoa(ID) + " join...")
	// in m.MembershipList, MembershipList[0] is for ID 3, [7] is for 10
	m.MembershipList[ID-3].Active = true
	m.MembershipList[ID-3].Fail = false
	m.MembershipList[ID-3].Heartbeat = 0
	// send Join ack to that node
	if !m.IsMaster {
		return
	}
	message := &util.Message{
		MsgType: "CONFIRMJOIN",
	}
	b, _ := json.Marshal(message)
	targetAddr := net.UDPAddr{
		IP:   sourceAddr.IP,
		Port: udpport,
	}
	util.UDPSend(&targetAddr, b)
}

func (m *Master) updateHeartBeat(sourceAddr *net.UDPAddr) {
	ID := util.CalculateID(sourceAddr.IP.String())
	if ID < 3 {
		// heartbeat from other master
		m.BackupMaster.Heartbeat = 0
		return
	}
	if m.MembershipList[ID-3].Active == true {
		m.MembershipList[ID-3].Heartbeat = 0
	}
}
