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
	"path/filepath"
	"strconv"
	"strings"
)

const (
	masterUDPport = 5678
	udpport       = 4000
	baseFilepath  = "/home/yifeili3/sava/"
	threshold     = 20
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
}

func newMaster() (master *Master, err error) {
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
				m.partitionJob(remoteID, ret.JobName, ret.FileName)
			} else if ret.MsgType != "" {
				// JOIN/#LEAVE/HEARTBEAT/HALT/DONE
				if ret.MsgType == "JOIN" {
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
					m.collectResult(remoteAddr)
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
					m.partitionJob(m.ClientID, m.JobName, m.DataFile)
				}
			}
		}
	}
	m.BackupMaster.UpdateHeartBeat()
	if threshold < m.BackupMaster.Heartbeat {
		// Master down
		log.Println("Backup master down")
		if !m.IsMaster {
			m.IsMaster = true
		}
	}

}

func (m *Master) intergrateFile() {
	// intergrate the result file into one file and send it back to client
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
	log.Println("transmit result to client done, job done.")

}

func (m *Master) collectResult(workerAddr *net.UDPAddr) {
	id := util.CalculateID(workerAddr.IP.String())
	flag := true
	for i := range m.Partition {
		if m.Partition[i].ID == id {
			m.Partition[i].State = -2
			m.Partition[i].Halt = true
		}
		flag = flag && (m.Partition[i].State == -2)
	}
	if flag && m.IsMaster {
		// migrate all the result file
	}

}

func (m *Master) jobDone() {
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
	flag := true
	for i := range m.Partition {
		if m.Partition[i].ID == id {
			m.Partition[i].State = ret.SuperStep
			m.Partition[i].Halt = true
		}
		flag = flag && m.Partition[i].Halt
	}
	if flag && m.IsMaster {
		m.jobDone()
	}
}

func (m *Master) updateSuperStep(workerAddr *net.UDPAddr, ret util.Message) {
	id := util.CalculateID(workerAddr.IP.String())
	flag := true
	for i := range m.Partition {
		if m.Partition[i].ID == id {
			m.Partition[i].State = ret.SuperStep
			m.Partition[i].Halt = false
		}
		flag = flag && (m.Partition[i].State == m.SuperStep)
	}
	if flag && m.IsMaster {
		m.startNextSupterStep()
	}
}

func (m *Master) startNextSupterStep() {
	m.SuperStep += 1
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
	m.SuperStep = 1
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
func (m *Master) partitionJob(clientID int, jobName string, datafile string) {
	//clientID := util.CalculateID(clientAddr.IP.String())
	m.ClientID = clientID

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
	PartitionInfo := make([]util.MetaInfo, size)
	// partition the original data file into size parts
	// readfile
	fi, err := os.OpenFile(baseFilepath+"data/"+datafile, os.O_RDONLY, 0666)
	if err != nil {
		log.Println("Open datafile error")
	}
	defer fi.Close()
	br := bufio.NewReader(fi)
	var nodeCount int
	nodeCount = 0
	curPart := 0
	count := 0
	lastNode := "-1"
	everyPartSize := 0
	var partition [][]string
	partition = make([][]string, size)
	for {
		a, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		line := string(a)
		if strings.HasPrefix(line, "#") {
			if !strings.Contains(line, "Nodes: ") {
				continue
			} else {
				temp := strings.Split(line, " ")

				nodeCount, _ = strconv.Atoi(temp[2])
				everyPartSize = nodeCount / size
				continue
			}
		}
		// start to treat real node->node
		node := strings.Split(line, " ")[0]
		if node != lastNode {
			count++
		}
		lastNode = node

		if count <= everyPartSize {
			partition[curPart] = append(partition[curPart], line)
		} else {
			curPart++
			count = 0
			partition[curPart] = append(partition[curPart], line)
		}

	}
	// save these temp data to number of files
	for i := range aliveWorker {
		startNode, _ := strconv.Atoi(strings.Split(partition[i][0], "\t")[0])
		endNode, _ := strconv.Atoi(strings.Split(partition[i][len(partition[i])-1], "\t")[0])
		meta := util.MetaInfo{
			ID:        aliveWorker[i],
			StartNode: startNode,
			EndNode:   endNode,
			State:     -1,
			Halt:      false,
		}
		PartitionInfo = append(PartitionInfo, meta)
		fo, err := os.Create(baseFilepath + "data/" + string(aliveWorker[i]) + ".txt")
		if err != nil {
			log.Println("Create file error")
		}

		wb := bufio.NewWriter(fo)
		for j := range partition[i] {
			wb.WriteString(partition[i][j])
		}

		wb.Flush()
		fo.Close()

		// and then RPC this file to target worker
		if m.IsMaster {
			util.RPCPutFile(aliveWorker[i], string(aliveWorker[i])+".txt", "data")
		}
		// remove the file since no use after
		os.Remove(baseFilepath + "data/" + string(aliveWorker[i]) + ".txt")

	}
	m.Partition = PartitionInfo
	m.JobName = jobName
	m.DataFile = datafile
	if !m.IsMaster {
		return
	}
	// send start work message to all workers
	for i := range aliveWorker {
		msg := util.Message{
			MsgType:   "JOB",
			Partition: m.Partition,
			JobName:   jobName,
			FileName:  string(aliveWorker[i]) + ".txt",
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
	// in m.MembershipList, MembershipList[0] is for ID 3, [7] is for 10
	m.MembershipList[ID-3].Active = true
	m.MembershipList[ID-3].Fail = false
	m.MembershipList[ID-3].Heartbeat = 0
	// send Join ack to that node
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
	}
	if m.MembershipList[ID-3].Active == true {
		m.MembershipList[ID-3].Heartbeat = 0
	}
}
