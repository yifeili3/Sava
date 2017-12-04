package util

import (
	"SDFS/shareReadWrite"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"strings"
)

const (
	serverBase    = "172.22.154.132"
	masterRPCport = ":9876"
	udpport       = 4000
	baseFilepath  = "/home/yifeili3/sava/"
	tcpport       = 9339
)

// Message ...
type Message struct {
	MsgType       string // Start/Superstep/W2W/JOB / W2WSEND / W2WREV
	SuperStep     int
	JobName       string
	Partition     []MetaInfo
	FileName      string
	WorkerMsg     WorkerMessage
	PoolWorkerMsg []WorkerMessage
	NumVertex     int
	TargetID      int
	Result        []Sortstruct
	SourceID      string
}
type Sortstruct struct {
	Key   int
	Value float64
}

//WorkerMessage ...
type WorkerMessage struct {
	DestVertex   int
	MessageValue interface{}
	SuperStep    int
	FormVertex   int
}

//MetaInfo ...
type MetaInfo struct {
	ID        int
	StartNode int
	EndNode   int
	State     int
	Halt      bool
	SSdone    bool
}

//ClientMsg ...
type ClientMsg struct {
	MsgType string // JOIN Leave, Alive, RequestJob
}

//FormatWorkerMessage ...
func FormatWorkerMessage(input Message) []byte {
	buf, _ := json.Marshal(input)
	return buf
}

// WhoAmI ...Get self ID based on ip address
func WhoAmI() int {
	ipaddr := WhereAmI()
	return CalculateID(ipaddr)
}

// WhereAmI ...Get current ip address of the machine
func WhereAmI() string {
	addrs, _ := net.InterfaceAddrs()
	var ipaddr string
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ipaddr = ipnet.IP.String()
			}
		}
	}
	return ipaddr
}

//CalculateID ...Map current ip address base off vm1 ip address
func CalculateID(serverAddr string) int {
	addr, err := strconv.Atoi(serverAddr[12:14])
	if err != nil {
		log.Fatal(">Wrong ip Address")
	}
	base, _ := strconv.Atoi(serverBase[12:14])
	return addr - base + 1
}

//UDPSend ...
func UDPSend(dstAddr *net.UDPAddr, info []byte) {
	srcAddr := net.UDPAddr{
		IP:   net.IPv4zero,
		Port: 2468,
	}

	conn, err := net.DialUDP("udp", &srcAddr, dstAddr)
	if err != nil {
		fmt.Println(err)
	}
	defer conn.Close()

	conn.Write(info)
}

//CalculateIP ...Map current id base off vm1 ip address
func CalculateIP(id int) string {
	base, _ := strconv.Atoi(serverBase[12:14])
	return serverBase[0:12] + strconv.Itoa(base+id-1)
}

//SendMessage ...
func SendMessage(srcAddr *net.UDPAddr, dstAddr *net.UDPAddr, info []byte) {
	conn, err := net.DialUDP("udp", srcAddr, dstAddr)
	if err != nil {
		fmt.Println(err)
	}
	conn.Write(info)
	conn.Close()
}

//RPCPutFile ...put file to master
func RPCPutFile(targetID int, localFileName string, filetype string) {
	log.Println("Start putting file")

	rpcObject, err := rpc.DialHTTP("tcp", CalculateIP(targetID)+masterRPCport)
	if err != nil {
		log.Println("RPC dial error")
	}
	n := &shareReadWrite.Node{}
	var reply string
	n.ReadLocalFile(localFileName, &reply)
	// parse the localFileName string

	filename := localFileName[strings.LastIndex(localFileName, "/")+1:]
	log.Println(filename)
	var remotefilename string
	if filetype == "data" {
		remotefilename = baseFilepath + "data/" + filename
	} else if filetype == "job" {
		remotefilename = baseFilepath + "vertex/" + filename
	} else if filetype == "result" {
		remotefilename = baseFilepath + "result/" + filename
	}
	cmd := &shareReadWrite.WriteCmd{File: remotefilename, Input: reply}
	var rep string
	err = rpcObject.Call("Node.WriteLocalFile", cmd, &rep)
}

//FormatMessage ...
func FormatMessage(jobName string, filename string, sourceID string) []byte {
	data := Message{JobName: jobName, FileName: filename[strings.LastIndex(filename, "/"):], MsgType: "Job", SourceID: sourceID}
	b, _ := json.Marshal(data)
	return b
}
